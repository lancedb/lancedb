// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::{cmp::Ordering, collections::HashMap, ops::Range, sync::Arc};

use crate::{
    decoder::DecoderConfig,
    encodings::physical::block::CompressionScheme,
    format::pb21::{
        BufferCompression, CompressiveEncoding, PageLayout, compressive_encoding::Compression,
    },
};

use arrow_array::{Array, StructArray, UInt64Array, make_array};
use arrow_data::transform::{Capacities, MutableArrayData};
use arrow_ord::ord::make_comparator;
use arrow_schema::{DataType, Field, Field as ArrowField, FieldRef, Schema, SortOptions};
use arrow_select::concat::concat;
use bytes::{Bytes, BytesMut};
use futures::{FutureExt, StreamExt, future::BoxFuture};
use log::{debug, info, trace};
use tokio::sync::mpsc::{self, UnboundedSender};

use lance_core::{Error, Result, datatypes::Field as LanceField, utils::bit::pad_bytes};
use lance_datagen::{ArrayGenerator, RowCount, Seed, array, gen_batch};

use crate::{
    EncodingsIo,
    buffer::LanceBuffer,
    compression::{
        BlockCompressor, CompressionStrategy, field_metadata_params, finalize_miniblock_compressor,
        reject_packed_struct_per_value, try_bitpacking_block, try_bitpacking_miniblock,
        try_byte_stream_split_miniblock, try_child_rle_miniblock,
        try_fixed_packed_struct_miniblock, try_fixed_u8_rle_block, try_fixed_u8_rle_miniblock,
        try_general_block, try_raw_block, try_raw_fixed_size_list_miniblock,
        try_raw_fixed_width_miniblock, try_raw_per_value, try_uncompressed_fixed_width_miniblock,
        try_variable_packed_struct_per_value, try_variable_rle_block, try_variable_width_miniblock,
        try_variable_width_per_value,
    },
    compression_config::{CompressionFieldParams, CompressionParams},
    data::DataBlock,
    decoder::{
        ColumnInfo, DecodeBatchScheduler, DecoderMessage, DecoderPlugins, FilterExpression,
        PageInfo, create_decode_stream,
    },
    encoder::{
        ColumnIndexSequence, EncodedColumn, EncodedPage, EncodingOptions, FieldEncoder,
        FieldEncodingContext, FieldEncodingStrategy, MIN_PAGE_BUFFER_ALIGNMENT, OutOfLineBuffers,
        structural::{
            PrimitiveFieldEncoding, PrimitivePageEncoding, try_create_binary_blob, try_create_list,
            try_create_map, try_create_struct, try_create_structural_blob,
            try_create_structural_fixed_size_list,
        },
    },
    encodings::logical::primitive::{fullzip::PerValueCompressor, miniblock::MiniBlockCompressor},
    repdef::RepDefBuilder,
};

const MAX_PAGE_BYTES: u64 = 32 * 1024 * 1024;
const TEST_ALIGNMENT: usize = MIN_PAGE_BUFFER_ALIGNMENT as usize;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TestEncoding {
    Array,
    StructuralU16,
    StructuralU32,
    StructuralSparse,
}

impl TestEncoding {
    fn all() -> impl Iterator<Item = Self> {
        [
            Self::Array,
            Self::StructuralU16,
            Self::StructuralU32,
            Self::StructuralSparse,
        ]
        .into_iter()
    }

    fn is_structural(self) -> bool {
        self != Self::Array
    }
}

impl std::fmt::Display for TestEncoding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Array => write!(f, "array"),
            Self::StructuralU16 => write!(f, "structural-u16"),
            Self::StructuralU32 => write!(f, "structural-u32"),
            Self::StructuralSparse => write!(f, "structural-sparse"),
        }
    }
}

#[derive(Debug, Clone)]
struct TestCompressionStrategy {
    encoding: TestEncoding,
    params: CompressionParams,
}

impl TestCompressionStrategy {
    fn field_params(&self, field: &LanceField) -> CompressionFieldParams {
        let mut params = self
            .params
            .get_field_params(&field.name, &field.data_type());
        let mut metadata = field_metadata_params(field);
        if self.encoding == TestEncoding::StructuralU16
            && metadata
                .minichunk_size
                .is_some_and(|size| size >= 32 * 1024)
        {
            metadata.minichunk_size = None;
        }
        params.merge(&metadata);
        params
    }
}

impl CompressionStrategy for TestCompressionStrategy {
    fn create_miniblock_compressor(
        &self,
        field: &LanceField,
        data: &DataBlock,
    ) -> Result<Box<dyn MiniBlockCompressor>> {
        let params = self.field_params(field);
        let compressor =
            if let Some(compressor) = try_uncompressed_fixed_width_miniblock(data, &params) {
                compressor
            } else if let Some(compressor) = try_byte_stream_split_miniblock(data, &params) {
                compressor
            } else if let Some(compressor) = match self.encoding {
                TestEncoding::StructuralSparse => try_child_rle_miniblock(data, &params),
                TestEncoding::Array | TestEncoding::StructuralU16 | TestEncoding::StructuralU32 => {
                    try_fixed_u8_rle_miniblock(data, &params)
                }
            } {
                compressor
            } else if let Some(compressor) = try_bitpacking_miniblock(data) {
                compressor
            } else if let Some(compressor) = try_raw_fixed_width_miniblock(data) {
                compressor
            } else if let Some(compressor) = try_variable_width_miniblock(field, data, &params)? {
                compressor
            } else if let Some(compressor) = try_fixed_packed_struct_miniblock(data)? {
                compressor
            } else if let Some(compressor) = try_raw_fixed_size_list_miniblock(data) {
                compressor
            } else {
                return Err(lance_core::Error::not_supported_source(
                    format!(
                        "Mini-block compression not yet supported for block type {}",
                        data.name()
                    )
                    .into(),
                ));
            };
        finalize_miniblock_compressor(data, compressor, &params)
    }

    fn create_per_value(
        &self,
        field: &LanceField,
        data: &DataBlock,
    ) -> Result<Box<dyn PerValueCompressor>> {
        let params = self.field_params(field);
        if let Some(compressor) = try_raw_per_value(data) {
            return Ok(compressor);
        }
        let packed = match self.encoding {
            TestEncoding::Array | TestEncoding::StructuralU16 => {
                reject_packed_struct_per_value(field, data)?
            }
            TestEncoding::StructuralU32 | TestEncoding::StructuralSparse => {
                try_variable_packed_struct_per_value(Arc::new(self.clone()), field, data)?
            }
        };
        if let Some(compressor) = packed {
            return Ok(compressor);
        }
        if let Some(compressor) = try_variable_width_per_value(field, data, &params)? {
            return Ok(compressor);
        }
        Err(lance_core::Error::not_supported_source(
            format!(
                "Per-value compression not yet supported for block type {}",
                data.name()
            )
            .into(),
        ))
    }

    fn create_block_compressor(
        &self,
        field: &LanceField,
        data: &DataBlock,
    ) -> Result<(Box<dyn BlockCompressor>, CompressiveEncoding)> {
        let params = self.field_params(field);
        let rle = match self.encoding {
            TestEncoding::Array | TestEncoding::StructuralU16 => None,
            TestEncoding::StructuralU32 => try_fixed_u8_rle_block(data, &params)?,
            TestEncoding::StructuralSparse => try_variable_rle_block(data, &params)?,
        };
        if let Some(compressor) = rle {
            return Ok(compressor);
        }
        if let Some(compressor) = try_bitpacking_block(data) {
            return Ok(compressor);
        }
        if matches!(
            self.encoding,
            TestEncoding::StructuralU32 | TestEncoding::StructuralSparse
        ) && let Some(compressor) = try_general_block(data, &params)?
        {
            return Ok(compressor);
        }
        if let Some(compressor) = try_raw_block(data) {
            return Ok(compressor);
        }
        Err(lance_core::Error::not_supported_source(
            format!(
                "Block compression not yet supported for block type {}",
                data.name()
            )
            .into(),
        ))
    }
}

pub fn test_compression_strategy(
    encoding: TestEncoding,
    params: CompressionParams,
) -> Arc<dyn CompressionStrategy> {
    Arc::new(TestCompressionStrategy { encoding, params })
}

#[derive(Debug)]
struct TestFieldEncodingStrategy {
    encoding: TestEncoding,
    primitive: PrimitiveFieldEncoding,
}

impl FieldEncodingStrategy for TestFieldEncodingStrategy {
    fn create_field_encoder(
        &self,
        field: &LanceField,
        column_index: &mut ColumnIndexSequence,
        context: &FieldEncodingContext<'_>,
    ) -> Result<Box<dyn FieldEncoder>> {
        if let Some(encoder) =
            try_create_binary_blob(&self.primitive, field, column_index, context)?
        {
            return Ok(encoder);
        }
        if self.encoding != TestEncoding::StructuralU16
            && let Some(encoder) =
                try_create_structural_blob(&self.primitive, field, column_index, context)?
        {
            return Ok(encoder);
        }
        if field.is_blob() {
            return Err(Error::invalid_input_source(
                format!(
                    "Blob encoding is not available for field '{}' with data type {}",
                    field.name,
                    field.data_type()
                )
                .into(),
            ));
        }
        if self.encoding != TestEncoding::StructuralU16 {
            if let Some(encoder) = try_create_map(field, column_index, context)? {
                return Ok(encoder);
            }
            if let Some(encoder) =
                try_create_structural_fixed_size_list(field, column_index, context)?
            {
                return Ok(encoder);
            }
        }
        if let Some(encoder) = self.primitive.try_create(field, column_index, context)? {
            return Ok(encoder);
        }
        if self.encoding == TestEncoding::StructuralU16 {
            if matches!(
                field.data_type(),
                DataType::FixedSizeList(item, _)
                    if matches!(item.data_type(), DataType::Struct(_))
            ) {
                return Err(Error::not_supported_source(
                    "FixedSizeList<Struct> is not enabled by the selected file format".into(),
                ));
            }
            if matches!(field.data_type(), DataType::Map(_, _)) {
                return Err(Error::not_supported_source(
                    "Map data type is not enabled by the selected file format".into(),
                ));
            }
        }
        if let Some(encoder) = try_create_list(field, column_index, context)? {
            return Ok(encoder);
        }
        if let Some(encoder) = try_create_struct(field, column_index, context)? {
            return Ok(encoder);
        }
        Err(Error::not_supported_source(
            format!(
                "{} has no field encoding for '{}' with data type {}",
                self.encoding,
                field.name,
                field.data_type()
            )
            .into(),
        ))
    }
}

pub fn test_encoding_strategy(encoding: TestEncoding) -> Box<dyn FieldEncodingStrategy> {
    if encoding == TestEncoding::Array {
        return Box::new(crate::array_encoding::ArrayFieldEncodingStrategy::new());
    }

    let compression = test_compression_strategy(encoding, CompressionParams::default());
    let page_encodings = match encoding {
        TestEncoding::StructuralU16 => vec![
            PrimitivePageEncoding::reject_sparse(),
            PrimitivePageEncoding::dense_u16(compression),
        ],
        TestEncoding::StructuralU32 => vec![
            PrimitivePageEncoding::reject_sparse(),
            PrimitivePageEncoding::constant(),
            PrimitivePageEncoding::dense_u32(compression),
        ],
        TestEncoding::StructuralSparse => vec![
            PrimitivePageEncoding::sparse(compression.clone()),
            PrimitivePageEncoding::constant(),
            PrimitivePageEncoding::dense_u32(compression),
        ],
        TestEncoding::Array => unreachable!(),
    };
    Box::new(TestFieldEncodingStrategy {
        encoding,
        primitive: PrimitiveFieldEncoding::new(page_encodings),
    })
}

pub fn create_test_field_encoder(
    strategy: &dyn FieldEncodingStrategy,
    field: &lance_core::datatypes::Field,
    column_index: &mut ColumnIndexSequence,
    options: &EncodingOptions,
) -> Result<Box<dyn FieldEncoder>> {
    let context = FieldEncodingContext {
        strategy,
        options,
        root_field_metadata: &field.metadata,
    };
    strategy.create_field_encoder(field, column_index, &context)
}

#[derive(Debug)]
pub(crate) struct SimulatedScheduler {
    data: Bytes,
}

impl SimulatedScheduler {
    pub fn new(data: Bytes) -> Self {
        Self { data }
    }
}

impl EncodingsIo for SimulatedScheduler {
    fn submit_request(
        &self,
        ranges: Vec<Range<u64>>,
        priority: u64,
    ) -> BoxFuture<'static, Result<Vec<Bytes>>> {
        let data = ranges
            .into_iter()
            .map(|range| self.data.slice(range.start as usize..range.end as usize))
            .collect();

        log::trace!("Scheduled request with priority {}", priority);
        std::future::ready(data)
            .map(move |data| {
                log::trace!("Decoded request with priority {}", priority);
                Ok(data)
            })
            .boxed()
    }
}

fn column_indices_from_schema_helper(
    fields: &[FieldRef],
    column_indices: &mut Vec<u32>,
    column_counter: &mut u32,
    is_structural_encoding: bool,
) {
    // In the old style, every field except FSL gets its own column.  In the new style only primitive
    // leaf fields get their own column.
    for field in fields {
        if is_structural_encoding && field.metadata().contains_key("lance-encoding:packed") {
            column_indices.push(*column_counter);
            *column_counter += 1;
            continue;
        }

        match field.data_type() {
            DataType::Struct(fields) => {
                if !is_structural_encoding {
                    column_indices.push(*column_counter);
                    *column_counter += 1;
                }
                column_indices_from_schema_helper(
                    fields.as_ref(),
                    column_indices,
                    column_counter,
                    is_structural_encoding,
                );
            }
            DataType::List(inner) => {
                if !is_structural_encoding {
                    column_indices.push(*column_counter);
                    *column_counter += 1;
                }
                column_indices_from_schema_helper(
                    std::slice::from_ref(inner),
                    column_indices,
                    column_counter,
                    is_structural_encoding,
                );
            }
            DataType::LargeList(inner) => {
                if !is_structural_encoding {
                    column_indices.push(*column_counter);
                    *column_counter += 1;
                }
                column_indices_from_schema_helper(
                    std::slice::from_ref(inner),
                    column_indices,
                    column_counter,
                    is_structural_encoding,
                );
            }
            DataType::Map(entries, _) => {
                column_indices_from_schema_helper(
                    std::slice::from_ref(entries),
                    column_indices,
                    column_counter,
                    is_structural_encoding,
                );
            }
            DataType::FixedSizeList(inner, _) => {
                // FSL(primitive) does not get its own column in either approach
                column_indices_from_schema_helper(
                    std::slice::from_ref(inner),
                    column_indices,
                    column_counter,
                    is_structural_encoding,
                );
            }
            _ => {
                column_indices.push(*column_counter);
                *column_counter += 1;

                column_indices_from_schema_helper(
                    &[],
                    column_indices,
                    column_counter,
                    is_structural_encoding,
                );
            }
        }
    }
}

fn column_indices_from_schema(schema: &Schema, is_structural_encoding: bool) -> Vec<u32> {
    let mut column_indices = Vec::new();
    let mut column_counter = 0;
    column_indices_from_schema_helper(
        schema.fields(),
        &mut column_indices,
        &mut column_counter,
        is_structural_encoding,
    );
    column_indices
}

#[allow(clippy::too_many_arguments)]
async fn test_decode(
    num_rows: u64,
    batch_size: u32,
    schema: &Schema,
    column_infos: &[Arc<ColumnInfo>],
    expected: Option<Arc<dyn Array>>,
    io: Arc<dyn EncodingsIo>,
    is_structural_encoding: bool,
    schedule_fn: impl FnOnce(
        DecodeBatchScheduler,
        UnboundedSender<Result<DecoderMessage>>,
    ) -> BoxFuture<'static, ()>,
) {
    let lance_schema = lance_core::datatypes::Schema::try_from(schema).unwrap();
    let cache = Arc::new(lance_core::cache::LanceCache::with_capacity(
        128 * 1024 * 1024,
    ));
    let column_indices = column_indices_from_schema(schema, is_structural_encoding);
    let decode_scheduler = DecodeBatchScheduler::try_new(
        &lance_schema,
        &column_indices,
        column_infos,
        &Vec::new(),
        num_rows,
        Arc::<DecoderPlugins>::default(),
        io,
        cache,
        &FilterExpression::no_filter(),
        &DecoderConfig::default(),
    )
    .await
    .unwrap();

    let (tx, rx) = mpsc::unbounded_channel();

    let scheduler_fut = schedule_fn(decode_scheduler, tx);

    scheduler_fut.await;

    let mut decode_stream = create_decode_stream(
        &lance_schema,
        num_rows,
        batch_size,
        is_structural_encoding,
        /*should_validate=*/ true,
        /*spawn_structural_batch_decode_tasks=*/ is_structural_encoding,
        rx,
        /*batch_size_bytes=*/ None,
    )
    .unwrap();

    let mut offset = 0;
    while let Some(batch) = decode_stream.next().await {
        let batch = batch.task.await.unwrap();
        if let Some(expected) = expected.as_ref() {
            let actual = batch.column(0);
            let expected_size = (batch_size as usize).min(expected.len() - offset);
            let expected = expected.slice(offset, expected_size);
            assert_eq!(expected.data_type(), actual.data_type());
            if expected.len() != actual.len() {
                panic!(
                    "Mismatch in length (at offset={}) expected {} but got {}",
                    offset,
                    expected.len(),
                    actual.len()
                );
            }
            if &expected != actual {
                if let Ok(comparator) = make_comparator(&expected, &actual, SortOptions::default())
                {
                    // We can't just assert_eq! because the error message is not very helpful.  This gives us a bit
                    // more information about where the mismatch is.
                    for i in 0..expected.len() {
                        if !matches!(comparator(i, i), Ordering::Equal) {
                            panic!(
                                "Mismatch at index {} (offset={}) expected {:?} but got {:?} first mismatch is expected {:?} but got {:?}",
                                i,
                                offset,
                                expected,
                                actual,
                                expected.slice(i, 1),
                                actual.slice(i, 1)
                            );
                        }
                    }
                } else {
                    // Some arrays (like the null type) don't have a comparator so we just re-run the normal comparison
                    // and let it assert
                    assert_eq!(&expected, actual);
                }
            }
        }
        offset += batch.num_rows();
    }
    if let Some(expected) = expected.as_ref() {
        assert_eq!(offset, expected.len());
    }
}

pub trait ArrayGeneratorProvider {
    fn provide(&self) -> Box<dyn ArrayGenerator>;
    fn copy(&self) -> Box<dyn ArrayGeneratorProvider>;
}
struct RandomArrayGeneratorProvider {
    field: Field,
}

impl ArrayGeneratorProvider for RandomArrayGeneratorProvider {
    fn provide(&self) -> Box<dyn ArrayGenerator> {
        array::rand_type(self.field.data_type())
    }

    fn copy(&self) -> Box<dyn ArrayGeneratorProvider> {
        Box::new(Self {
            field: self.field.clone(),
        })
    }
}

/// Given a field this will test the round trip encoding and decoding of random data
pub async fn check_basic_random(field: Field) {
    check_specific_random(field, TestCases::basic()).await;
}

pub async fn check_specific_random(field: Field, test_cases: TestCases) {
    let array_generator_provider = RandomArrayGeneratorProvider {
        field: field.clone(),
    };
    check_round_trip_encoding_generated(field, Box::new(array_generator_provider), test_cases)
        .await;
}

pub struct FnArrayGeneratorProvider<F: Fn() -> Box<dyn ArrayGenerator> + Clone + 'static> {
    provider_fn: F,
}

impl<F: Fn() -> Box<dyn ArrayGenerator> + Clone + 'static> FnArrayGeneratorProvider<F> {
    pub fn new(provider_fn: F) -> Self {
        Self { provider_fn }
    }
}

impl<F: Fn() -> Box<dyn ArrayGenerator> + Clone + 'static> ArrayGeneratorProvider
    for FnArrayGeneratorProvider<F>
{
    fn provide(&self) -> Box<dyn ArrayGenerator> {
        (self.provider_fn)()
    }

    fn copy(&self) -> Box<dyn ArrayGeneratorProvider> {
        Box::new(Self {
            provider_fn: self.provider_fn.clone(),
        })
    }
}

pub async fn check_basic_generated(
    field: Field,
    array_generator_provider: Box<dyn ArrayGeneratorProvider>,
) {
    check_round_trip_encoding_generated(field, array_generator_provider, TestCases::basic()).await;
}

pub async fn check_round_trip_encoding_generated(
    field: Field,
    array_generator_provider: Box<dyn ArrayGeneratorProvider>,
    test_cases: TestCases,
) {
    let lance_field = lance_core::datatypes::Field::try_from(&field).unwrap();
    for page_size in test_cases.page_sizes.iter().copied() {
        debug!("Testing random data with a page size of {}", page_size);
        let encoder_factory = |encoding: TestEncoding| {
            let encoding_strategy = test_encoding_strategy(encoding);
            let mut column_index_seq = ColumnIndexSequence::default();
            let encoding_options = EncodingOptions {
                max_page_bytes: MAX_PAGE_BYTES,
                cache_bytes_per_column: page_size,
                keep_original_array: true,
                buffer_alignment: MIN_PAGE_BUFFER_ALIGNMENT,
            };
            create_test_field_encoder(
                encoding_strategy.as_ref(),
                &lance_field,
                &mut column_index_seq,
                &encoding_options,
            )
            .unwrap()
        };

        check_round_trip_random(
            encoder_factory,
            field.clone(),
            array_generator_provider.copy(),
            &test_cases,
        )
        .await
    }
}

fn supports_nulls(data_type: &DataType, encoding: TestEncoding) -> bool {
    if let DataType::Struct(fields) = data_type {
        if encoding == TestEncoding::Array {
            // 2.0 doesn't support nullability for structs
            false
        } else if fields.is_empty() {
            // Even in 2.1 we don't support nulls for struct if there are no children because
            // we have no spot to stick the repdef info (there is no column)
            false
        } else {
            true
        }
    } else {
        true
    }
}

type EncodingVerificationFn = dyn Fn(&[EncodedColumn], &TestEncoding);

// The default will just test the full read
#[derive(Clone)]
pub struct TestCases {
    ranges: Vec<Range<u64>>,
    indices: Vec<Vec<u64>>,
    batch_size: u32,
    skip_validation: bool,
    max_page_size: Option<u64>,
    page_sizes: Vec<u64>,
    encodings: Vec<TestEncoding>,
    verify_encoding: Option<Arc<EncodingVerificationFn>>,
    expected_encoding: Option<Vec<String>>,
}

impl Default for TestCases {
    fn default() -> Self {
        Self {
            batch_size: 100,
            ranges: Vec::new(),
            indices: Vec::new(),
            skip_validation: false,
            max_page_size: None,
            page_sizes: vec![4096, 1024 * 1024],
            encodings: TestEncoding::all().collect(),
            verify_encoding: None,
            expected_encoding: None,
        }
    }
}

impl TestCases {
    pub fn basic() -> Self {
        Self::default()
            .with_range(0..500)
            .with_range(100..1100)
            .with_range(8000..8500)
            .with_indices(vec![100])
            .with_indices(vec![0])
            .with_indices(vec![9999])
            .with_indices(vec![100, 1100, 5000])
            .with_indices(vec![1000, 2000, 3000])
            .with_indices(vec![2000, 2001, 2002, 2003, 2004])
            // Big take that spans multiple pages and generates multiple output batches
            .with_indices((100..500).map(|i| i * 3).collect::<Vec<_>>())
    }

    pub fn with_range(mut self, range: Range<u64>) -> Self {
        self.ranges.push(range);
        self
    }

    pub fn with_indices(mut self, indices: Vec<u64>) -> Self {
        self.indices.push(indices);
        self
    }

    pub fn with_batch_size(mut self, batch_size: u32) -> Self {
        self.batch_size = batch_size;
        self
    }

    pub fn without_validation(mut self) -> Self {
        self.skip_validation = true;
        self
    }

    pub fn with_encoding(mut self, encoding: TestEncoding) -> Self {
        self.encodings = vec![encoding];
        self
    }

    pub fn with_encodings(mut self, encodings: impl IntoIterator<Item = TestEncoding>) -> Self {
        self.encodings = encodings.into_iter().collect();
        self
    }

    pub fn with_structural_encodings(self) -> Self {
        self.with_encodings([
            TestEncoding::StructuralU16,
            TestEncoding::StructuralU32,
            TestEncoding::StructuralSparse,
        ])
    }

    pub fn with_u32_structural_encodings(self) -> Self {
        self.with_encodings([TestEncoding::StructuralU32, TestEncoding::StructuralSparse])
    }

    pub fn with_dense_encodings(self) -> Self {
        self.with_encodings([
            TestEncoding::Array,
            TestEncoding::StructuralU16,
            TestEncoding::StructuralU32,
        ])
    }

    pub fn with_array_and_u16_encodings(self) -> Self {
        self.with_encodings([TestEncoding::Array, TestEncoding::StructuralU16])
    }

    pub fn with_page_sizes(mut self, page_sizes: Vec<u64>) -> Self {
        self.page_sizes = page_sizes;
        self
    }

    pub fn with_max_page_size(mut self, max_page_size: u64) -> Self {
        self.max_page_size = Some(max_page_size);
        self
    }

    fn get_max_page_size(&self) -> u64 {
        self.max_page_size.unwrap_or(MAX_PAGE_BYTES)
    }

    fn encodings(&self) -> impl Iterator<Item = TestEncoding> + '_ {
        self.encodings.iter().copied()
    }

    pub fn with_verify_encoding(mut self, verify_encoding: Arc<EncodingVerificationFn>) -> Self {
        self.verify_encoding = Some(verify_encoding);
        self
    }

    fn verify_encoding(&self, columns: &[EncodedColumn], encoding: &TestEncoding) {
        if let Some(verify_encoding) = self.verify_encoding.as_ref() {
            verify_encoding(columns, encoding);
        }
    }

    pub fn with_expected_encoding(mut self, encoding: impl Into<String>) -> Self {
        self.expected_encoding = Some(vec![encoding.into()]);
        self
    }

    pub fn with_expected_encoding_chain<I: IntoIterator<Item = T>, T: Into<String>>(
        mut self,
        encodings: I,
    ) -> Self {
        self.expected_encoding = Some(encodings.into_iter().map(Into::into).collect());
        self
    }
}

/// Maps encoding enum variant to its string tag
/// Uses exhaustive match to ensure compile-time checking when variants are added/removed
fn tag(e: &Compression) -> &'static str {
    use Compression::*;

    match e {
        Flat(_) => "flat",
        Variable(_) => "variable",
        Constant(_) => "constant",
        OutOfLineBitpacking(_) => "out_of_line_bitpacking",
        InlineBitpacking(_) => "inline_bitpacking",
        Fsst(_) => "fsst",
        Dictionary(_) => "dictionary",
        Rle(_) => "rle",
        ByteStreamSplit(_) => "byte_stream_split",
        General(_) => "general",
        FixedSizeList(_) => "fixed_size_list",
        PackedStruct(_) => "packed_struct",
        VariablePackedStruct(_) => "variable_packed_struct",
    }
}

/// Returns any buffer outputs of this encoding
fn buffer(c: &Compression) -> Option<Vec<&BufferCompression>> {
    use Compression::*;

    match c {
        Flat(f) => f.data.as_ref().map(|b| vec![b]),
        Variable(v) => v.values.as_ref().map(|b| vec![b]),
        InlineBitpacking(i) => i.values.as_ref().map(|b| vec![b]),
        General(g) => g.compression.as_ref().map(|c| vec![c]),
        _ => None,
    }
}

/// Returns the child encoding if this variant contains nested ArrayEncoding
fn child(c: &Compression) -> Option<Vec<&CompressiveEncoding>> {
    use Compression::*;

    match c {
        Variable(v) => v.offsets.as_ref().map(|b| vec![b.as_ref()]),
        OutOfLineBitpacking(o) => o.values.as_ref().map(|b| vec![b.as_ref()]),
        Fsst(f) => f.values.as_ref().map(|b| vec![b.as_ref()]),
        ByteStreamSplit(b) => b.values.as_ref().map(|b| vec![b.as_ref()]),
        General(g) => g.values.as_ref().map(|b| vec![b.as_ref()]),
        Dictionary(d) => {
            let mut children = Vec::new();
            if let Some(values) = d.items.as_ref() {
                children.push(values.as_ref());
            }
            if let Some(indices) = d.indices.as_ref() {
                children.push(indices.as_ref());
            }
            Some(children)
        }
        Rle(r) => {
            let mut children = Vec::new();
            if let Some(values) = r.values.as_ref() {
                children.push(values.as_ref());
            }
            if let Some(run_lengths) = r.run_lengths.as_ref() {
                children.push(run_lengths.as_ref());
            }
            Some(children)
        }
        FixedSizeList(f) => f.values.as_ref().map(|b| vec![b.as_ref()]),
        VariablePackedStruct(v) => {
            let nested: Vec<&CompressiveEncoding> = v
                .fields
                .iter()
                .filter_map(|field| field.value.as_ref())
                .collect();
            if nested.is_empty() {
                None
            } else {
                Some(nested)
            }
        }
        _ => None,
    }
}

/// Extract encoding types from array encoding (helper for nested encodings)
/// Returns the encoding chain including compression schemes for Block variants
pub fn extract_array_encoding_chain(enc: &CompressiveEncoding) -> Vec<String> {
    let mut chain = Vec::with_capacity(8);
    let mut stack = vec![enc];

    while let Some(cur) = stack.pop() {
        if let Some(inner) = &cur.compression {
            // 1. Add current layer's tag
            chain.push(tag(inner).to_string());

            // 2. Extract any buffer output
            if let Some(buffer) = buffer(inner) {
                chain.extend(buffer.into_iter().map(|b| {
                    let scheme = CompressionScheme::try_from(b.scheme()).unwrap();
                    scheme.to_string()
                }));
            }

            // 3. Process child encoding if exists
            if let Some(children) = child(inner) {
                stack.extend(children);
            }
        }
    }
    chain
}

fn collect_page_encoding(layout: &PageLayout, actual_chain: &mut Vec<String>) -> Result<()> {
    // Extract encodings from the page layout
    use crate::format::pb21::page_layout::Layout;
    if let Some(ref layout_type) = layout.layout {
        match layout_type {
            Layout::MiniBlockLayout(mini_block) => {
                if mini_block.dictionary.is_some() {
                    actual_chain.push("dictionary".to_string());
                }
                // Check value compression
                if let Some(ref value_comp) = mini_block.value_compression {
                    let chain = extract_array_encoding_chain(value_comp);
                    actual_chain.extend(chain);
                }
            }
            Layout::FullZipLayout(full_zip) => {
                // Check value compression in full zip layout
                if let Some(ref value_comp) = full_zip.value_compression {
                    let chain = extract_array_encoding_chain(value_comp);
                    actual_chain.extend(chain);
                }
            }
            Layout::ConstantLayout(_) => {
                // Constant layout does not describe a value encoding chain.
            }
            Layout::BlobLayout(blob) => {
                if let Some(inner_layout) = &blob.inner_layout {
                    collect_page_encoding(inner_layout.as_ref(), actual_chain)?
                }
            }
            Layout::SparseLayout(sparse) => {
                if let Some(value_compression) = &sparse.value_compression {
                    actual_chain.extend(extract_array_encoding_chain(value_compression));
                }
            }
        }
    }

    Ok(())
}

/// Verify that a single page contains the expected encoding
fn verify_page_encoding(
    page: &EncodedPage,
    expected_chain: &[String],
    col_idx: usize,
) -> Result<()> {
    use crate::decoder::PageEncoding;
    use lance_core::Error;

    let mut actual_chain = Vec::new();

    match &page.description {
        PageEncoding::Structural(layout) => {
            collect_page_encoding(layout, &mut actual_chain)?;

            // All-null structural pages may legitimately contain no encodings to verify.
            // This can happen even when compression is configured because there is no value data
            // (and rep/def compression is not currently described in the page layout).
            if actual_chain.is_empty()
                && page.data.is_empty()
                && let Some(crate::format::pb21::page_layout::Layout::ConstantLayout(cl)) =
                    layout.layout.as_ref()
                && cl.inline_value.is_none()
            {
                return Ok(());
            }
        }
        PageEncoding::Legacy(_) => {
            // We don't need to care about the v2.0 array encoding.
        }
    }

    // Check that all expected encodings appear in the actual chain
    for expected in expected_chain {
        if !actual_chain.iter().any(|actual| actual.contains(expected)) {
            return Err(Error::invalid_input_source(
                format!(
                    "Column {} expected encoding chain {:?} but got {:?}",
                    col_idx, expected_chain, actual_chain
                )
                .into(),
            ));
        }
    }
    Ok(())
}

/// Given specific data and test cases we check round trip encoding and decoding
///
/// Note that the input `data` is a `Vec` to simulate multiple calls to `maybe_encode`.
/// In other words, these are multiple chunks of one long array and not multiple columns
/// in a record batch.  To feed a "record batch" you should first convert the record batch
/// to a struct array.
pub async fn check_round_trip_encoding_of_data(
    data: Vec<Arc<dyn Array>>,
    test_cases: &TestCases,
    metadata: HashMap<String, String>,
) {
    check_round_trip_encoding_of_data_with_expected(data, None, test_cases, metadata).await
}

pub async fn check_round_trip_encoding_of_data_with_expected(
    data: Vec<Arc<dyn Array>>,
    expected_override: Option<Arc<dyn Array>>,
    test_cases: &TestCases,
    metadata: HashMap<String, String>,
) {
    let example_data = data.first().expect("Data must have at least one array");
    let mut field = Field::new("", example_data.data_type().clone(), true);
    field = field.with_metadata(metadata);
    let lance_field = lance_core::datatypes::Field::try_from(&field).unwrap();
    for encoding in test_cases.encodings() {
        for page_size in test_cases.page_sizes.iter() {
            let encoding_strategy = test_encoding_strategy(encoding);
            let mut column_index_seq = ColumnIndexSequence::default();
            let encoding_options = EncodingOptions {
                cache_bytes_per_column: *page_size,
                max_page_bytes: test_cases.get_max_page_size(),
                keep_original_array: true,
                buffer_alignment: MIN_PAGE_BUFFER_ALIGNMENT,
            };
            let encoder = create_test_field_encoder(
                encoding_strategy.as_ref(),
                &lance_field,
                &mut column_index_seq,
                &encoding_options,
            )
            .unwrap();
            info!(
                "Testing round trip encoding of data with test encoding {} and page size {}",
                encoding, page_size
            );
            check_round_trip_encoding_inner(
                encoder,
                &field,
                data.clone(),
                expected_override.clone(),
                test_cases,
                encoding,
            )
            .await
        }
    }
}

struct SimulatedWriter {
    page_infos: Vec<Vec<PageInfo>>,
    encoded_data: BytesMut,
}

impl SimulatedWriter {
    fn new(num_columns: u32) -> Self {
        let mut page_infos = Vec::with_capacity(num_columns as usize);
        page_infos.resize_with(num_columns as usize, Default::default);
        Self {
            page_infos,
            encoded_data: BytesMut::new(),
        }
    }

    fn write_buffer(&mut self, buffer: LanceBuffer) -> (u64, u64) {
        let offset = self.encoded_data.len() as u64;
        self.encoded_data.extend_from_slice(&buffer);
        let size = self.encoded_data.len() as u64 - offset;
        let pad_bytes = pad_bytes::<TEST_ALIGNMENT>(self.encoded_data.len());
        self.encoded_data.extend(std::iter::repeat_n(0, pad_bytes));
        (offset, size)
    }

    fn write_lance_buffer(&mut self, buffer: LanceBuffer) {
        self.encoded_data.extend_from_slice(&buffer);
        let pad_bytes = pad_bytes::<TEST_ALIGNMENT>(self.encoded_data.len());
        self.encoded_data.extend(std::iter::repeat_n(0, pad_bytes));
    }

    fn write_page(&mut self, encoded_page: EncodedPage) {
        trace!("Encoded page {:?}", encoded_page);
        let page_buffers = encoded_page.data;
        let page_encoding = encoded_page.description;
        let buffer_offsets_and_sizes = page_buffers
            .into_iter()
            .map(|b| {
                let (offset, size) = self.write_buffer(b);
                trace!("Encoded buffer offset={} size={}", offset, size);
                (offset, size)
            })
            .collect::<Vec<_>>();

        let page_info = PageInfo {
            num_rows: encoded_page.num_rows,
            encoding: page_encoding,
            buffer_offsets_and_sizes: Arc::from(buffer_offsets_and_sizes),
            priority: encoded_page.row_number,
        };

        let col_idx = encoded_page.column_idx as usize;
        self.page_infos[col_idx].push(page_info);
    }

    fn new_external_buffers(&self) -> OutOfLineBuffers {
        OutOfLineBuffers::new(self.encoded_data.len() as u64, MIN_PAGE_BUFFER_ALIGNMENT)
    }
}

/// This is the inner-most check function that actually runs the round trip and tests it
async fn check_round_trip_encoding_inner(
    mut encoder: Box<dyn FieldEncoder>,
    field: &Field,
    data: Vec<Arc<dyn Array>>,
    expected_override: Option<Arc<dyn Array>>,
    test_cases: &TestCases,
    encoding: TestEncoding,
) {
    let mut writer = SimulatedWriter::new(encoder.num_columns());

    let log_page = |encoded_page: &EncodedPage| {
        debug!(
            "Encoded page on column {} with {} rows and start row {} and buffer sizes [{}]",
            encoded_page.column_idx,
            encoded_page.num_rows,
            encoded_page.row_number,
            encoded_page
                .data
                .iter()
                .map(|buf| buf.len().to_string())
                .collect::<Vec<_>>()
                .join(", ")
        );
    };

    let mut row_number = 0;
    for arr in &data {
        let mut external_buffers = writer.new_external_buffers();
        let repdef = RepDefBuilder::default();
        let num_rows = arr.len() as u64;
        let encode_tasks = encoder
            .maybe_encode(
                arr.clone(),
                &mut external_buffers,
                repdef,
                row_number,
                num_rows,
            )
            .unwrap();
        for buffer in external_buffers.take_buffers() {
            writer.write_lance_buffer(buffer);
        }
        for encode_task in encode_tasks {
            let encoded_page = encode_task.await.unwrap();
            log_page(&encoded_page);

            // For V2.1, verify encoding in the page if expected
            if encoding.is_structural()
                && let Some(ref expected) = test_cases.expected_encoding
            {
                verify_page_encoding(&encoded_page, expected, encoded_page.column_idx as usize)
                    .unwrap();
            }

            writer.write_page(encoded_page);
        }
        row_number += arr.len() as u64;
    }

    let mut external_buffers = writer.new_external_buffers();
    let encode_tasks = encoder.flush(&mut external_buffers).unwrap();
    for buffer in external_buffers.take_buffers() {
        writer.write_lance_buffer(buffer);
    }
    for task in encode_tasks {
        let encoded_page = task.await.unwrap();
        log_page(&encoded_page);

        // For V2.1, verify encoding in the page if expected
        if encoding.is_structural()
            && let Some(ref expected) = test_cases.expected_encoding
        {
            verify_page_encoding(&encoded_page, expected, encoded_page.column_idx as usize)
                .unwrap();
        }

        writer.write_page(encoded_page);
    }

    let mut external_buffers = writer.new_external_buffers();
    let encoded_columns = encoder.finish(&mut external_buffers).await.unwrap();
    test_cases.verify_encoding(&encoded_columns, &encoding);
    for buffer in external_buffers.take_buffers() {
        writer.write_lance_buffer(buffer);
    }
    let mut column_infos = Vec::new();
    for (col_idx, encoded_column) in encoded_columns.into_iter().enumerate() {
        // Keep track of pages for encoding verification
        for page in encoded_column.final_pages {
            writer.write_page(page);
        }

        let col_buffer_off_and_size = encoded_column
            .column_buffers
            .into_iter()
            .map(|b| writer.write_buffer(b))
            .collect::<Vec<_>>();

        let column_info = ColumnInfo::new(
            col_idx as u32,
            Arc::from(std::mem::take(&mut writer.page_infos[col_idx])),
            col_buffer_off_and_size,
            encoded_column.encoding,
        );

        column_infos.push(Arc::new(column_info));
    }

    let encoded_data = writer.encoded_data.freeze();

    let scheduler = Arc::new(SimulatedScheduler::new(encoded_data)) as Arc<dyn EncodingsIo>;

    let num_rows = data.iter().map(|arr| arr.len() as u64).sum::<u64>();
    let concat_data = if test_cases.skip_validation {
        None
    } else if let Some(DataType::Struct(_)) = data.first().map(|datum| datum.data_type()) {
        // TODO(tsaucer) When arrow upgrades to 56, remove this if statement
        // This is due to a check for concat_struct in arrow-rs. See https://github.com/lance-format/lance/pull/4598
        let capacities = Capacities::Array(num_rows as usize);
        let array_data: Vec<_> = data.iter().map(|a| a.to_data()).collect::<Vec<_>>();
        let array_data = array_data.iter().collect();
        let mut mutable = MutableArrayData::with_capacities(array_data, false, capacities);

        for (i, a) in data.iter().enumerate() {
            mutable.extend(i, 0, a.len())
        }

        Some(make_array(mutable.freeze()))
    } else {
        Some(concat(&data.iter().map(|arr| arr.as_ref()).collect::<Vec<_>>()).unwrap())
    };

    let expected_data = expected_override.clone().or_else(|| concat_data.clone());

    let is_structural_encoding = encoding.is_structural();

    let decode_field = if is_structural_encoding {
        let mut lance_field = lance_core::datatypes::Field::try_from(field).unwrap();
        if lance_field.is_blob() && matches!(lance_field.data_type(), DataType::Struct(_)) {
            lance_field.unloaded_mut();
            let mut arrow_field = ArrowField::from(&lance_field);
            let mut metadata = arrow_field.metadata().clone();
            metadata.insert("lance-encoding:packed".to_string(), "true".to_string());
            arrow_field = arrow_field.with_metadata(metadata);
            arrow_field
        } else {
            field.clone()
        }
    } else {
        field.clone()
    };

    let schema = Schema::new(vec![decode_field]);

    debug!("Testing full decode");
    let scheduler_copy = scheduler.clone();
    test_decode(
        num_rows,
        test_cases.batch_size,
        &schema,
        &column_infos,
        expected_data.clone(),
        scheduler_copy.clone(),
        is_structural_encoding,
        |mut decode_scheduler, tx| {
            async move {
                decode_scheduler.schedule_range(
                    0..num_rows,
                    &FilterExpression::no_filter(),
                    tx,
                    scheduler_copy,
                )
            }
            .boxed()
        },
    )
    .await;

    // Test range scheduling
    for range in &test_cases.ranges {
        debug!("Testing decode of range {:?}", range);
        let num_rows = range.end - range.start;
        let expected = expected_data
            .as_ref()
            .map(|arr| arr.slice(range.start as usize, num_rows as usize));
        let scheduler = scheduler.clone();
        let range = range.clone();
        test_decode(
            num_rows,
            test_cases.batch_size,
            &schema,
            &column_infos,
            expected,
            scheduler.clone(),
            is_structural_encoding,
            |mut decode_scheduler, tx| {
                async move {
                    decode_scheduler.schedule_range(
                        range,
                        &FilterExpression::no_filter(),
                        tx,
                        scheduler,
                    )
                }
                .boxed()
            },
        )
        .await;
    }

    // Test take scheduling
    for indices in &test_cases.indices {
        if indices.len() == 1 {
            debug!("Testing decode of index {}", indices[0]);
        } else {
            debug!(
                "Testing decode of {} indices spread across range [{}..{}]",
                indices.len(),
                indices[0],
                indices[indices.len() - 1]
            );
        }
        let num_rows = indices.len() as u64;
        let indices_arr = UInt64Array::from(indices.clone());

        // There is a bug in arrow_select::take::take that causes it to return empty arrays
        // if the data type is an empty struct.  This is a workaround for that.
        let is_empty_struct = if let DataType::Struct(fields) = field.data_type() {
            fields.is_empty()
        } else {
            false
        };

        let expected = if is_empty_struct {
            Some(Arc::new(StructArray::new_empty_fields(indices_arr.len(), None)) as Arc<dyn Array>)
        } else {
            concat_data.as_ref().map(|concat_data| {
                arrow_select::take::take(&concat_data, &indices_arr, None).unwrap()
            })
        };

        let scheduler = scheduler.clone();
        let indices = indices.clone();
        test_decode(
            num_rows,
            test_cases.batch_size,
            &schema,
            &column_infos,
            expected,
            scheduler.clone(),
            is_structural_encoding,
            |mut decode_scheduler, tx| {
                async move {
                    decode_scheduler.schedule_take(
                        &indices,
                        &FilterExpression::no_filter(),
                        tx,
                        scheduler,
                    )
                }
                .boxed()
            },
        )
        .await;
    }
}

const NUM_RANDOM_ROWS: u32 = 10000;

/// Generates random data (parameterized by null rate, slicing, and # ingest batches)
/// and tests with that against default test cases.
///
/// To test specific test cases use the
async fn check_round_trip_random(
    encoder_factory: impl Fn(TestEncoding) -> Box<dyn FieldEncoder>,
    field: Field,
    array_generator_provider: Box<dyn ArrayGeneratorProvider>,
    test_cases: &TestCases,
) {
    for null_rate in [None, Some(0.5), Some(1.0)] {
        for use_slicing in [false, true] {
            for encoding in test_cases.encodings() {
                if null_rate != Some(1.0) && matches!(field.data_type(), DataType::Null) {
                    continue;
                }

                let field = if null_rate.is_some() {
                    if !supports_nulls(field.data_type(), encoding) {
                        continue;
                    }
                    field.clone().with_nullable(true)
                } else {
                    field.clone().with_nullable(false)
                };

                for num_ingest_batches in [1, 5, 10] {
                    let rows_per_batch = NUM_RANDOM_ROWS / num_ingest_batches;
                    let mut data = Vec::new();

                    // Test both ingesting one big array sliced into smaller arrays and smaller
                    // arrays independently generated.  These behave slightly differently.  For
                    // example, a list array sliced into smaller arrays will have arrays whose
                    // starting offset is not 0.
                    if use_slicing {
                        let mut generator =
                            gen_batch().anon_col(array_generator_provider.provide());
                        if let Some(null_rate) = null_rate {
                            // The null generator is the only generator that already inserts nulls
                            // and attempting to do so again makes arrow-rs grumpy
                            if !matches!(field.data_type(), DataType::Null) {
                                generator.with_random_nulls(null_rate);
                            }
                        }
                        let all_data = generator
                            .into_batch_rows(RowCount::from(10000))
                            .unwrap()
                            .column(0)
                            .clone();
                        let mut offset = 0;
                        for _ in 0..num_ingest_batches {
                            data.push(all_data.slice(offset, rows_per_batch as usize));
                            offset += rows_per_batch as usize;
                        }
                    } else {
                        for i in 0..num_ingest_batches {
                            let mut generator = gen_batch()
                                .with_seed(Seed::from(i as u64))
                                .anon_col(array_generator_provider.provide());
                            if let Some(null_rate) = null_rate {
                                // The null generator is the only generator that already inserts nulls
                                // and attempting to do so again makes arrow-rs grumpy
                                if !matches!(field.data_type(), DataType::Null) {
                                    generator.with_random_nulls(null_rate);
                                }
                            }
                            let arr = generator
                                .into_batch_rows(RowCount::from(rows_per_batch as u64))
                                .unwrap()
                                .column(0)
                                .clone();
                            data.push(arr);
                        }
                    }

                    info!(
                        "Testing encoding {} with {} rows divided across {} batches for {} rows per batch with null_rate={:?} and use_slicing={}",
                        encoding,
                        NUM_RANDOM_ROWS,
                        num_ingest_batches,
                        rows_per_batch,
                        null_rate,
                        use_slicing
                    );
                    check_round_trip_encoding_inner(
                        encoder_factory(encoding),
                        &field,
                        data,
                        None,
                        test_cases,
                        encoding,
                    )
                    .await
                }
            }
        }
    }
}
