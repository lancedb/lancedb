// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::{ops::Range, sync::Arc};

use arrow_array::{Array, ArrayRef, LargeListArray, ListArray, cast::AsArray, make_array};
use arrow_schema::DataType;
use futures::future::BoxFuture;
use lance_arrow::deepcopy::deep_copy_nulls;
use lance_arrow::list::ListArrayExt;
use lance_core::Result;

use crate::{
    decoder::{
        DecodedArray, FilterExpression, ScheduledScanLine, SchedulerContext,
        StructuralDecodeArrayTask, StructuralFieldDecoder, StructuralFieldScheduler,
        StructuralSchedulingJob,
    },
    encoder::{EncodeTask, FieldEncoder, OutOfLineBuffers},
    repdef::RepDefBuilder,
};

/// A structural encoder for list fields
///
/// The list's offsets are added to the rep/def builder
/// and the list array's values are passed to the child encoder
///
/// The values will have any garbage values removed and will be trimmed
/// to only include the values that are actually used.
pub struct ListStructuralEncoder {
    keep_original_array: bool,
    child: Box<dyn FieldEncoder>,
}

impl ListStructuralEncoder {
    pub fn new(keep_original_array: bool, child: Box<dyn FieldEncoder>) -> Self {
        Self {
            keep_original_array,
            child,
        }
    }
}

impl FieldEncoder for ListStructuralEncoder {
    fn maybe_encode(
        &mut self,
        array: ArrayRef,
        external_buffers: &mut OutOfLineBuffers,
        mut repdef: RepDefBuilder,
        row_number: u64,
        num_rows: u64,
    ) -> Result<Vec<EncodeTask>> {
        let values = if let Some(list_arr) = array.as_list_opt::<i32>() {
            let has_garbage_values = if self.keep_original_array {
                repdef.add_offsets(list_arr.offsets().clone(), array.nulls().cloned())
            } else {
                // there is no need to deep copy offsets, because offset buffers will be cast to a common type (i64).
                repdef.add_offsets(list_arr.offsets().clone(), deep_copy_nulls(array.nulls()))
            };
            if has_garbage_values {
                list_arr.filter_garbage_nulls().trimmed_values()
            } else {
                list_arr.trimmed_values()
            }
        } else if let Some(list_arr) = array.as_list_opt::<i64>() {
            let has_garbage_values = if self.keep_original_array {
                repdef.add_offsets(list_arr.offsets().clone(), array.nulls().cloned())
            } else {
                repdef.add_offsets(list_arr.offsets().clone(), deep_copy_nulls(array.nulls()))
            };
            if has_garbage_values {
                list_arr.filter_garbage_nulls().trimmed_values()
            } else {
                list_arr.trimmed_values()
            }
        } else {
            panic!("List encoder used for non-list data")
        };
        self.child
            .maybe_encode(values, external_buffers, repdef, row_number, num_rows)
    }

    fn flush(&mut self, external_buffers: &mut OutOfLineBuffers) -> Result<Vec<EncodeTask>> {
        self.child.flush(external_buffers)
    }

    fn num_columns(&self) -> u32 {
        self.child.num_columns()
    }

    fn finish(
        &mut self,
        external_buffers: &mut OutOfLineBuffers,
    ) -> BoxFuture<'_, Result<Vec<crate::encoder::EncodedColumn>>> {
        self.child.finish(external_buffers)
    }
}

#[derive(Debug)]
pub struct StructuralListScheduler {
    child: Box<dyn StructuralFieldScheduler>,
}

impl StructuralListScheduler {
    pub fn new(child: Box<dyn StructuralFieldScheduler>) -> Self {
        Self { child }
    }
}

impl StructuralFieldScheduler for StructuralListScheduler {
    fn schedule_ranges<'a>(
        &'a self,
        ranges: &[Range<u64>],
        filter: &FilterExpression,
    ) -> Result<Box<dyn StructuralSchedulingJob + 'a>> {
        let child = self.child.schedule_ranges(ranges, filter)?;

        Ok(Box::new(StructuralListSchedulingJob::new(child)))
    }

    fn initialize<'a>(
        &'a mut self,
        filter: &'a FilterExpression,
        context: &'a SchedulerContext,
    ) -> BoxFuture<'a, Result<()>> {
        self.child.initialize(filter, context)
    }
}

/// Scheduling job for list data
///
/// Scheduling is handled by the primitive encoder and nothing special
/// happens here.
#[derive(Debug)]
struct StructuralListSchedulingJob<'a> {
    child: Box<dyn StructuralSchedulingJob + 'a>,
}

impl<'a> StructuralListSchedulingJob<'a> {
    fn new(child: Box<dyn StructuralSchedulingJob + 'a>) -> Self {
        Self { child }
    }
}

impl StructuralSchedulingJob for StructuralListSchedulingJob<'_> {
    fn schedule_next(&mut self, context: &mut SchedulerContext) -> Result<Vec<ScheduledScanLine>> {
        self.child.schedule_next(context)
    }
}

#[derive(Debug)]
pub struct StructuralListDecoder {
    child: Box<dyn StructuralFieldDecoder>,
    data_type: DataType,
}

impl StructuralListDecoder {
    pub fn new(child: Box<dyn StructuralFieldDecoder>, data_type: DataType) -> Self {
        Self { child, data_type }
    }
}

impl StructuralFieldDecoder for StructuralListDecoder {
    fn accept_page(&mut self, child: crate::decoder::LoadedPageShard) -> Result<()> {
        self.child.accept_page(child)
    }

    fn drain(&mut self, num_rows: u64) -> Result<Box<dyn StructuralDecodeArrayTask>> {
        let child_task = self.child.drain(num_rows)?;
        Ok(Box::new(StructuralListDecodeTask::new(
            child_task,
            self.data_type.clone(),
        )))
    }

    fn data_type(&self) -> &DataType {
        &self.data_type
    }
}

#[derive(Debug)]
struct StructuralListDecodeTask {
    child_task: Box<dyn StructuralDecodeArrayTask>,
    data_type: DataType,
}

impl StructuralListDecodeTask {
    fn new(child_task: Box<dyn StructuralDecodeArrayTask>, data_type: DataType) -> Self {
        Self {
            child_task,
            data_type,
        }
    }
}

impl StructuralDecodeArrayTask for StructuralListDecodeTask {
    fn decode(self: Box<Self>) -> Result<DecodedArray> {
        let DecodedArray {
            array,
            mut repdef,
            data_size,
        } = self.child_task.decode()?;
        match &self.data_type {
            DataType::List(child_field) => {
                let (offsets, validity) = repdef.unravel_offsets::<i32>()?;
                let array = if !child_field.is_nullable() && array.null_count() == array.len() {
                    make_array(array.into_data().into_builder().nulls(None).build()?)
                } else {
                    array
                };
                let list_array = ListArray::try_new(child_field.clone(), offsets, array, validity)?;

                Ok(DecodedArray {
                    array: Arc::new(list_array),
                    repdef,
                    data_size,
                })
            }
            DataType::LargeList(child_field) => {
                let (offsets, validity) = repdef.unravel_offsets::<i64>()?;
                let list_array =
                    LargeListArray::try_new(child_field.clone(), offsets, array, validity)?;
                Ok(DecodedArray {
                    array: Arc::new(list_array),
                    repdef,
                    data_size,
                })
            }
            _ => panic!("List decoder did not have a list field"),
        }
    }
}

#[cfg(test)]
mod tests {

    use std::{collections::HashMap, sync::Arc};

    use crate::constants::{
        STRUCTURAL_ENCODING_FULLZIP, STRUCTURAL_ENCODING_META_KEY, STRUCTURAL_ENCODING_MINIBLOCK,
    };
    use arrow_array::{
        Array, ArrayRef, BooleanArray, DictionaryArray, LargeStringArray, ListArray, StructArray,
        UInt8Array, UInt64Array,
        builder::{
            Int32Builder, Int64Builder, LargeListBuilder, ListBuilder, StringBuilder, UInt32Builder,
        },
    };

    use arrow_buffer::{BooleanBuffer, NullBuffer, OffsetBuffer, ScalarBuffer};
    use arrow_schema::{DataType, Field, Fields};
    use rstest::rstest;

    use crate::testing::{
        TestCases, TestEncoding, check_basic_random, check_round_trip_encoding_of_data,
        create_test_field_encoder, test_encoding_strategy,
    };

    fn make_list_type(inner_type: DataType) -> DataType {
        DataType::List(Arc::new(Field::new("item", inner_type, true)))
    }

    fn make_large_list_type(inner_type: DataType) -> DataType {
        DataType::LargeList(Arc::new(Field::new("item", inner_type, true)))
    }

    async fn try_encode_v22_pages(
        array: ArrayRef,
    ) -> lance_core::Result<Vec<crate::encoder::EncodedPage>> {
        try_encode_v22_pages_with_metadata(array, HashMap::new()).await
    }

    async fn try_encode_v22_pages_with_metadata(
        array: ArrayRef,
        field_metadata: HashMap<String, String>,
    ) -> lance_core::Result<Vec<crate::encoder::EncodedPage>> {
        let arrow_field =
            Field::new("", array.data_type().clone(), true).with_metadata(field_metadata);
        let lance_field = lance_core::datatypes::Field::try_from(&arrow_field).unwrap();
        let encoding_strategy = test_encoding_strategy(TestEncoding::StructuralU32);
        let mut column_index_seq = crate::encoder::ColumnIndexSequence::default();
        let encoding_options = crate::encoder::EncodingOptions::default();
        let mut encoder = create_test_field_encoder(
            encoding_strategy.as_ref(),
            &lance_field,
            &mut column_index_seq,
            &encoding_options,
        )
        .unwrap();
        let mut external_buffers =
            crate::encoder::OutOfLineBuffers::new(0, crate::encoder::MIN_PAGE_BUFFER_ALIGNMENT);
        let num_rows = array.len() as u64;
        let mut pages = Vec::new();
        for task in encoder
            .maybe_encode(
                array,
                &mut external_buffers,
                crate::repdef::RepDefBuilder::default(),
                0,
                num_rows,
            )
            .unwrap()
        {
            pages.push(task.await?);
        }
        for task in encoder.flush(&mut external_buffers).unwrap() {
            pages.push(task.await?);
        }
        Ok(pages)
    }

    async fn encode_v22_pages(array: ArrayRef) -> Vec<crate::encoder::EncodedPage> {
        try_encode_v22_pages(array).await.unwrap()
    }

    fn assert_split_miniblock_layout(
        pages: &[crate::encoder::EncodedPage],
        expect_structural_only_page: bool,
    ) {
        let mut miniblock_pages = 0;
        let mut fullzip_pages = 0;
        let mut structural_only_pages = 0;

        for page in pages {
            let crate::decoder::PageEncoding::Structural(layout) = &page.description else {
                continue;
            };
            match layout.layout.as_ref().unwrap() {
                crate::format::pb21::page_layout::Layout::MiniBlockLayout(_) => {
                    miniblock_pages += 1;
                }
                crate::format::pb21::page_layout::Layout::FullZipLayout(_) => {
                    fullzip_pages += 1;
                }
                crate::format::pb21::page_layout::Layout::ConstantLayout(layout) => {
                    if layout.inline_value.is_none()
                        && (layout.num_rep_values > 0 || layout.num_def_values > 0)
                    {
                        structural_only_pages += 1;
                    }
                }
                crate::format::pb21::page_layout::Layout::BlobLayout(_) => {}
                crate::format::pb21::page_layout::Layout::SparseLayout(_) => {}
            }
        }

        assert!(
            miniblock_pages > 0,
            "expected leaf values to remain on mini-block pages"
        );
        assert_eq!(
            fullzip_pages, 0,
            "split list pages should not fall back to full-zip"
        );
        if expect_structural_only_page {
            assert!(
                structural_only_pages > 0,
                "expected at least one structural-only page"
            );
        }
    }

    fn assert_has_fullzip_layout(pages: &[crate::encoder::EncodedPage]) {
        let has_fullzip = pages.iter().any(|page| {
            let crate::decoder::PageEncoding::Structural(layout) = &page.description else {
                return false;
            };
            matches!(
                layout.layout.as_ref().unwrap(),
                crate::format::pb21::page_layout::Layout::FullZipLayout(_)
            )
        });
        assert!(has_fullzip, "expected at least one full-zip page");
    }

    #[rstest]
    #[test_log::test(tokio::test)]
    async fn test_list(
        #[values(STRUCTURAL_ENCODING_MINIBLOCK, STRUCTURAL_ENCODING_FULLZIP)]
        structural_encoding: &str,
    ) {
        let mut field_metadata = HashMap::new();
        field_metadata.insert(
            STRUCTURAL_ENCODING_META_KEY.to_string(),
            structural_encoding.into(),
        );
        let field =
            Field::new("", make_list_type(DataType::Int32), true).with_metadata(field_metadata);
        check_basic_random(field).await;
    }

    #[rstest]
    #[test_log::test(tokio::test)]
    async fn test_deeply_nested_lists(
        #[values(STRUCTURAL_ENCODING_MINIBLOCK, STRUCTURAL_ENCODING_FULLZIP)]
        structural_encoding: &str,
    ) {
        let mut field_metadata = HashMap::new();
        field_metadata.insert(
            STRUCTURAL_ENCODING_META_KEY.to_string(),
            structural_encoding.into(),
        );
        let field = Field::new("item", DataType::Int32, true).with_metadata(field_metadata);
        for _ in 0..5 {
            let field = Field::new("", make_list_type(field.data_type().clone()), true);
            check_basic_random(field).await;
        }
    }

    #[test_log::test(tokio::test)]
    async fn test_large_list() {
        let field = Field::new("", make_large_list_type(DataType::Int32), true);
        check_basic_random(field).await;
    }

    #[test_log::test(tokio::test)]
    async fn test_nested_strings() {
        let field = Field::new("", make_list_type(DataType::Utf8), true);
        check_basic_random(field).await;
    }

    #[test_log::test(tokio::test)]
    async fn test_nested_list() {
        let field = Field::new("", make_list_type(make_list_type(DataType::Int32)), true);
        check_basic_random(field).await;
    }

    /// Regression test: a `List<List<Float32>>` column written as MULTIPLE
    /// batches (chunks) whose flattened leaf values cross a value-page boundary
    /// fails to decode with "Max offset N exceeds length of values M" (Arrow
    /// error raised by `ListArray::try_new` in `StructuralListDecodeTask::decode`).
    ///
    /// The trigger (verified against the production file and pylance 7.0.0b12 /
    /// 7.0.0 / 9.0.0-beta.10) requires ALL of:
    ///   1. >= 2 list layers (`List<List<..>>`),
    ///   2. a leaf large enough to be chunked into multiple value pages,
    ///   3. the column written as more than one batch.
    /// A single batch of the identical data round-trips fine — which is why the
    /// earlier single-chunk version of this test (and the small `test_nested_list`
    /// cases) did not catch it. Found in production on the gaming TransNet
    /// `dino_embedding_per_frame` column (rectangular 3 x 768 float per row).
    ///
    /// Each element of the `vec![..]` passed to `check_round_trip_encoding_of_data`
    /// is encoded as a separate batch (its own `RepDefBuilder`), so we split the
    /// rows into two chunks to exercise the multi-batch repdef accumulation path.
    #[rstest]
    #[test_log::test(tokio::test)]
    async fn test_multipage_nested_float_list(
        #[values(STRUCTURAL_ENCODING_MINIBLOCK, STRUCTURAL_ENCODING_FULLZIP)]
        structural_encoding: &str,
    ) {
        use arrow_array::Float32Array;

        // Production shape: 3 inner lists per row, 768 floats each.
        let inner_per_row: usize = 3;
        let inner_len: usize = 768;
        // Two chunks (batches) -> two pages; a read batch that spans the page
        // boundary is where the multi-page outer-offset bug triggered. A single
        // [2731] chunk (one page) decodes fine, which is why this needs >= 2.
        let chunk_rows: &[usize] = &[1366, 1365];

        let make_chunk = |start_row: usize, num_rows: usize| -> Arc<dyn Array> {
            let total_inner = num_rows * inner_per_row;
            let total_values = total_inner * inner_len;
            let values = Float32Array::from(
                (0..total_values)
                    .map(|i| (start_row + i) as f32)
                    .collect::<Vec<_>>(),
            );
            let inner_offsets = ScalarBuffer::<i32>::from(
                (0..=total_inner)
                    .map(|i| (i * inner_len) as i32)
                    .collect::<Vec<_>>(),
            );
            let inner_list = ListArray::new(
                Arc::new(Field::new("item", DataType::Float32, true)),
                OffsetBuffer::new(inner_offsets),
                Arc::new(values),
                None,
            );
            let outer_offsets = ScalarBuffer::<i32>::from(
                (0..=num_rows)
                    .map(|i| (i * inner_per_row) as i32)
                    .collect::<Vec<_>>(),
            );
            Arc::new(ListArray::new(
                Arc::new(Field::new(
                    "item",
                    DataType::List(Arc::new(Field::new("item", DataType::Float32, true))),
                    true,
                )),
                OffsetBuffer::new(outer_offsets),
                Arc::new(inner_list),
                None,
            ))
        };

        let mut start = 0;
        let chunks: Vec<Arc<dyn Array>> = chunk_rows
            .iter()
            .map(|&n| {
                let c = make_chunk(start, n);
                start += n;
                c
            })
            .collect();

        let mut field_metadata = HashMap::new();
        field_metadata.insert(
            STRUCTURAL_ENCODING_META_KEY.to_string(),
            structural_encoding.into(),
        );

        let test_cases = TestCases::default().with_structural_encodings();
        check_round_trip_encoding_of_data(chunks, &test_cases, field_metadata).await;
    }

    #[test_log::test(tokio::test)]
    async fn test_list_struct_list() {
        let struct_type = DataType::Struct(Fields::from(vec![Field::new(
            "inner_str",
            DataType::Utf8,
            false,
        )]));

        let field = Field::new("", make_list_type(struct_type), true);
        check_basic_random(field).await;
    }

    #[test_log::test(tokio::test)]
    async fn test_list_struct_empty() {
        let fields = Fields::from(vec![Field::new("inner", DataType::UInt64, true)]);
        let items = UInt64Array::from(Vec::<u64>::new());
        let structs = StructArray::new(fields, vec![Arc::new(items)], None);
        let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0; 2 * 1024 * 1024 + 1]));
        let lists = ListArray::new(
            Arc::new(Field::new("item", structs.data_type().clone(), true)),
            offsets,
            Arc::new(structs),
            None,
        );

        check_round_trip_encoding_of_data(
            vec![Arc::new(lists)],
            &TestCases::default(),
            HashMap::new(),
        )
        .await;
    }

    #[rstest]
    #[test_log::test(tokio::test)]
    async fn test_simple_list(
        #[values(STRUCTURAL_ENCODING_MINIBLOCK, STRUCTURAL_ENCODING_FULLZIP)]
        structural_encoding: &str,
    ) {
        let items_builder = Int32Builder::new();
        let mut list_builder = ListBuilder::new(items_builder);
        list_builder.append_value([Some(1), Some(2), Some(3)]);
        list_builder.append_value([Some(4), Some(5)]);
        list_builder.append_null();
        list_builder.append_value([Some(6), Some(7), Some(8)]);
        let list_array = list_builder.finish();

        let mut field_metadata = HashMap::new();
        field_metadata.insert(
            STRUCTURAL_ENCODING_META_KEY.to_string(),
            structural_encoding.into(),
        );

        let test_cases = TestCases::default()
            .with_range(0..2)
            .with_range(0..3)
            .with_range(1..3)
            .with_indices(vec![1, 3])
            .with_indices(vec![2]);
        check_round_trip_encoding_of_data(vec![Arc::new(list_array)], &test_cases, field_metadata)
            .await;
    }

    #[rstest]
    #[test_log::test(tokio::test)]
    async fn test_simple_nested_list_ends_with_null(
        #[values(STRUCTURAL_ENCODING_MINIBLOCK, STRUCTURAL_ENCODING_FULLZIP)]
        structural_encoding: &str,
    ) {
        use arrow_array::Int32Array;

        let values = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let inner_offsets = ScalarBuffer::<i32>::from(vec![0, 1, 2, 3, 4, 5, 5]);
        let inner_validity = BooleanBuffer::from(vec![true, true, true, true, true, false]);
        let outer_offsets = ScalarBuffer::<i32>::from(vec![0, 1, 2, 3, 4, 5, 6, 6]);
        let outer_validity = BooleanBuffer::from(vec![true, true, true, true, true, true, false]);

        let inner_list = ListArray::new(
            Arc::new(Field::new("item", DataType::Int32, true)),
            OffsetBuffer::new(inner_offsets),
            Arc::new(values),
            Some(NullBuffer::new(inner_validity)),
        );
        let outer_list = ListArray::new(
            Arc::new(Field::new(
                "item",
                DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
                true,
            )),
            OffsetBuffer::new(outer_offsets),
            Arc::new(inner_list),
            Some(NullBuffer::new(outer_validity)),
        );

        let mut field_metadata = HashMap::new();
        field_metadata.insert(
            STRUCTURAL_ENCODING_META_KEY.to_string(),
            structural_encoding.into(),
        );

        let test_cases = TestCases::default()
            .with_range(0..2)
            .with_range(0..3)
            .with_range(5..7)
            .with_indices(vec![1, 6])
            .with_indices(vec![6])
            .with_structural_encodings();
        check_round_trip_encoding_of_data(vec![Arc::new(outer_list)], &test_cases, field_metadata)
            .await;
    }

    #[rstest]
    #[test_log::test(tokio::test)]
    async fn test_simple_string_list(
        #[values(STRUCTURAL_ENCODING_MINIBLOCK, STRUCTURAL_ENCODING_FULLZIP)]
        structural_encoding: &str,
    ) {
        let items_builder = StringBuilder::new();
        let mut list_builder = ListBuilder::new(items_builder);
        list_builder.append_value([Some("a"), Some("bc"), Some("def")]);
        list_builder.append_value([Some("gh"), None]);
        list_builder.append_null();
        list_builder.append_value([Some("ijk"), Some("lmnop"), Some("qrs")]);
        let list_array = list_builder.finish();

        let mut field_metadata = HashMap::new();
        field_metadata.insert(
            STRUCTURAL_ENCODING_META_KEY.to_string(),
            structural_encoding.into(),
        );

        let test_cases = TestCases::default()
            .with_range(0..2)
            .with_range(0..3)
            .with_range(1..3)
            .with_indices(vec![1, 3])
            .with_indices(vec![2])
            .with_structural_encodings();
        check_round_trip_encoding_of_data(vec![Arc::new(list_array)], &test_cases, field_metadata)
            .await;
    }

    #[rstest]
    #[test_log::test(tokio::test)]
    async fn test_simple_string_list_no_null(
        #[values(STRUCTURAL_ENCODING_MINIBLOCK, STRUCTURAL_ENCODING_FULLZIP)]
        structural_encoding: &str,
    ) {
        let items_builder = StringBuilder::new();
        let mut list_builder = ListBuilder::new(items_builder);
        list_builder.append_value([Some("a"), Some("bc"), Some("def")]);
        list_builder.append_value([Some("gh"), Some("zxy")]);
        list_builder.append_value([Some("gh"), Some("z")]);
        list_builder.append_value([Some("ijk"), Some("lmnop"), Some("qrs")]);
        let list_array = list_builder.finish();

        let mut field_metadata = HashMap::new();
        field_metadata.insert(
            STRUCTURAL_ENCODING_META_KEY.to_string(),
            structural_encoding.into(),
        );

        let test_cases = TestCases::default()
            .with_range(0..2)
            .with_range(0..3)
            .with_range(1..3)
            .with_indices(vec![1, 3])
            .with_indices(vec![2])
            .with_structural_encodings();
        check_round_trip_encoding_of_data(vec![Arc::new(list_array)], &test_cases, field_metadata)
            .await;
    }

    #[rstest]
    #[test_log::test(tokio::test)]
    async fn test_simple_sliced_list(
        #[values(STRUCTURAL_ENCODING_MINIBLOCK, STRUCTURAL_ENCODING_FULLZIP)]
        structural_encoding: &str,
    ) {
        let items_builder = Int32Builder::new();
        let mut list_builder = ListBuilder::new(items_builder);
        list_builder.append_value([Some(1), Some(2), Some(3)]);
        list_builder.append_value([Some(4), Some(5)]);
        list_builder.append_null();
        list_builder.append_value([Some(6), Some(7), Some(8)]);
        let list_array = list_builder.finish();

        let list_array = list_array.slice(1, 2);

        let mut field_metadata = HashMap::new();
        field_metadata.insert(
            STRUCTURAL_ENCODING_META_KEY.to_string(),
            structural_encoding.into(),
        );

        let test_cases = TestCases::default()
            .with_range(0..2)
            .with_range(1..2)
            .with_indices(vec![0])
            .with_indices(vec![1])
            .with_structural_encodings();
        check_round_trip_encoding_of_data(vec![Arc::new(list_array)], &test_cases, field_metadata)
            .await;
    }

    #[test_log::test(tokio::test)]
    async fn test_simple_list_dict() {
        let values = LargeStringArray::from_iter_values(["a", "bb", "ccc"]);
        let indices = UInt8Array::from(vec![0, 1, 2, 0, 1, 2, 0, 1, 2]);
        let dict_array = DictionaryArray::new(indices, Arc::new(values));
        let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0, 3, 5, 6, 9]));
        let list_array = ListArray::new(
            Arc::new(Field::new("item", dict_array.data_type().clone(), true)),
            offsets,
            Arc::new(dict_array),
            None,
        );

        let test_cases = TestCases::default()
            .with_range(0..2)
            .with_range(1..3)
            .with_range(2..4)
            .with_indices(vec![1])
            .with_indices(vec![2]);
        check_round_trip_encoding_of_data(
            vec![Arc::new(list_array)],
            &test_cases,
            HashMap::default(),
        )
        .await;
    }

    #[test_log::test(tokio::test)]
    async fn test_simple_list_all_null() {
        let items = UInt64Array::from(vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        let offsets = ScalarBuffer::<i32>::from(vec![0, 5, 8, 10]);
        let offsets = OffsetBuffer::new(offsets);
        let list_validity = NullBuffer::new(BooleanBuffer::from(vec![false, false, false]));

        // The list array is nullable but the items are not.  Then, all lists are null.
        let list_arr = ListArray::new(
            Arc::new(Field::new("item", DataType::UInt64, false)),
            offsets,
            Arc::new(items),
            Some(list_validity),
        );

        let test_cases = TestCases::default()
            .with_range(0..3)
            .with_range(1..2)
            .with_indices(vec![1])
            .with_indices(vec![2])
            .with_structural_encodings();
        check_round_trip_encoding_of_data(
            vec![Arc::new(list_arr)],
            &test_cases,
            HashMap::default(),
        )
        .await;
    }

    #[rstest]
    #[test_log::test(tokio::test)]
    async fn test_list_with_garbage_nulls(
        #[values(STRUCTURAL_ENCODING_MINIBLOCK, STRUCTURAL_ENCODING_FULLZIP)]
        structural_encoding: &str,
    ) {
        // In Arrow, list nulls are allowed to be non-empty, with masked garbage values
        // Here we make a list with a null row in the middle with 3 garbage values
        let items = UInt64Array::from(vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        let offsets = ScalarBuffer::<i32>::from(vec![0, 5, 8, 10]);
        let offsets = OffsetBuffer::new(offsets);
        let list_validity = NullBuffer::new(BooleanBuffer::from(vec![true, false, true]));
        let list_arr = ListArray::new(
            Arc::new(Field::new("item", DataType::UInt64, true)),
            offsets,
            Arc::new(items),
            Some(list_validity),
        );

        let mut field_metadata = HashMap::new();
        field_metadata.insert(
            STRUCTURAL_ENCODING_META_KEY.to_string(),
            structural_encoding.into(),
        );

        let test_cases = TestCases::default()
            .with_range(0..3)
            .with_range(1..2)
            .with_indices(vec![1])
            .with_indices(vec![2])
            .with_structural_encodings();
        check_round_trip_encoding_of_data(vec![Arc::new(list_arr)], &test_cases, field_metadata)
            .await;
    }

    #[rstest]
    #[test_log::test(tokio::test)]
    async fn test_simple_two_page_list(
        #[values(STRUCTURAL_ENCODING_MINIBLOCK, STRUCTURAL_ENCODING_FULLZIP)]
        structural_encoding: &str,
    ) {
        // This is a simple pre-defined list that spans two pages.  This test is useful for
        // debugging the repetition index

        let items_builder = Int64Builder::new();
        let mut list_builder = ListBuilder::new(items_builder);
        for i in 0..512 {
            list_builder.append_value([Some(i), Some(i * 2)]);
        }
        let list_array_1 = list_builder.finish();

        let items_builder = Int64Builder::new();
        let mut list_builder = ListBuilder::new(items_builder);
        for i in 0..512 {
            let i = i + 512;
            list_builder.append_value([Some(i), Some(i * 2)]);
        }
        let list_array_2 = list_builder.finish();

        let mut metadata = HashMap::new();
        metadata.insert(
            STRUCTURAL_ENCODING_META_KEY.to_string(),
            structural_encoding.into(),
        );

        let test_cases = TestCases::default()
            .with_structural_encodings()
            .with_page_sizes(vec![100])
            .with_range(800..900);
        check_round_trip_encoding_of_data(
            vec![Arc::new(list_array_1), Arc::new(list_array_2)],
            &test_cases,
            metadata,
        )
        .await;
    }

    #[test_log::test(tokio::test)]
    async fn test_simple_large_list() {
        let items_builder = Int32Builder::new();
        let mut list_builder = LargeListBuilder::new(items_builder);
        list_builder.append_value([Some(1), Some(2), Some(3)]);
        list_builder.append_value([Some(4), Some(5)]);
        list_builder.append_null();
        list_builder.append_value([Some(6), Some(7), Some(8)]);
        let list_array = list_builder.finish();

        let test_cases = TestCases::default()
            .with_range(0..2)
            .with_range(0..3)
            .with_range(1..3)
            .with_indices(vec![1, 3]);
        check_round_trip_encoding_of_data(vec![Arc::new(list_array)], &test_cases, HashMap::new())
            .await;
    }

    #[rstest]
    #[test_log::test(tokio::test)]
    async fn test_empty_lists(
        #[values(STRUCTURAL_ENCODING_MINIBLOCK, STRUCTURAL_ENCODING_FULLZIP)]
        structural_encoding: &str,
    ) {
        let mut field_metadata = HashMap::new();
        field_metadata.insert(
            STRUCTURAL_ENCODING_META_KEY.to_string(),
            structural_encoding.into(),
        );

        // Scenario 1: Some lists are empty

        let values = [vec![Some(1), Some(2), Some(3)], vec![], vec![None]];
        // Test empty list at beginning, middle, and end
        for order in [[0, 1, 2], [1, 0, 2], [2, 0, 1]] {
            let items_builder = Int32Builder::new();
            let mut list_builder = ListBuilder::new(items_builder);
            for idx in order {
                list_builder.append_value(values[idx].clone());
            }
            let list_array = Arc::new(list_builder.finish());
            let test_cases = TestCases::default()
                .with_indices(vec![1])
                .with_indices(vec![0])
                .with_indices(vec![2])
                .with_indices(vec![0, 1]);
            check_round_trip_encoding_of_data(
                vec![list_array.clone()],
                &test_cases,
                field_metadata.clone(),
            )
            .await;
            let test_cases = test_cases.with_batch_size(1);
            check_round_trip_encoding_of_data(
                vec![list_array],
                &test_cases,
                field_metadata.clone(),
            )
            .await;
        }

        // Scenario 2: All lists are empty

        // When encoding a list of empty lists there are no items to encode
        // which is strange and we want to ensure we handle it
        let items_builder = Int32Builder::new();
        let mut list_builder = ListBuilder::new(items_builder);
        list_builder.append(true);
        list_builder.append_null();
        list_builder.append(true);
        let list_array = Arc::new(list_builder.finish());

        let test_cases = TestCases::default().with_range(0..2).with_indices(vec![1]);
        check_round_trip_encoding_of_data(
            vec![list_array.clone()],
            &test_cases,
            field_metadata.clone(),
        )
        .await;
        let test_cases = test_cases.with_batch_size(1);
        check_round_trip_encoding_of_data(vec![list_array], &test_cases, field_metadata.clone())
            .await;

        // Scenario 2B: All lists are empty (but now with strings)

        // When encoding a list of empty lists there are no items to encode
        // which is strange and we want to ensure we handle it
        let items_builder = StringBuilder::new();
        let mut list_builder = ListBuilder::new(items_builder);
        list_builder.append(true);
        list_builder.append_null();
        list_builder.append(true);
        let list_array = Arc::new(list_builder.finish());

        let test_cases = TestCases::default().with_range(0..2).with_indices(vec![1]);
        check_round_trip_encoding_of_data(
            vec![list_array.clone()],
            &test_cases,
            field_metadata.clone(),
        )
        .await;
        let test_cases = test_cases.with_batch_size(1);
        check_round_trip_encoding_of_data(vec![list_array], &test_cases, field_metadata.clone())
            .await;

        // Scenario 3: All lists are null

        let items_builder = Int32Builder::new();
        let mut list_builder = ListBuilder::new(items_builder);
        list_builder.append_null();
        list_builder.append_null();
        list_builder.append_null();
        let list_array = Arc::new(list_builder.finish());

        let test_cases = TestCases::default().with_range(0..2).with_indices(vec![1]);
        check_round_trip_encoding_of_data(
            vec![list_array.clone()],
            &test_cases,
            field_metadata.clone(),
        )
        .await;
        let test_cases = test_cases.with_batch_size(1);
        check_round_trip_encoding_of_data(vec![list_array], &test_cases, field_metadata.clone())
            .await;

        // Scenario 4: All lists are null and inside a struct (only valid for 2.1 since 2.0 doesn't
        // support null structs)
        let items_builder = Int32Builder::new();
        let mut list_builder = ListBuilder::new(items_builder);
        list_builder.append_null();
        list_builder.append_null();
        list_builder.append_null();
        let list_array = Arc::new(list_builder.finish());

        let struct_validity = NullBuffer::new(BooleanBuffer::from(vec![true, false, true]));
        let struct_array = Arc::new(StructArray::new(
            Fields::from(vec![Field::new(
                "lists",
                list_array.data_type().clone(),
                true,
            )]),
            vec![list_array],
            Some(struct_validity),
        ));

        let test_cases = TestCases::default()
            .with_range(0..2)
            .with_indices(vec![1])
            .with_structural_encodings();
        check_round_trip_encoding_of_data(
            vec![struct_array.clone()],
            &test_cases,
            field_metadata.clone(),
        )
        .await;
        let test_cases = test_cases.with_batch_size(1);
        check_round_trip_encoding_of_data(vec![struct_array], &test_cases, field_metadata.clone())
            .await;
    }

    #[test_log::test(tokio::test)]
    async fn test_empty_list_list() {
        let items_builder = Int32Builder::new();
        let list_builder = ListBuilder::new(items_builder);
        let mut outer_list_builder = ListBuilder::new(list_builder);
        outer_list_builder.append_null();
        outer_list_builder.append_null();
        outer_list_builder.append_null();
        let list_array = Arc::new(outer_list_builder.finish());

        let test_cases = TestCases::default().with_structural_encodings();
        check_round_trip_encoding_of_data(vec![list_array], &test_cases, HashMap::new()).await;
    }

    #[test_log::test(tokio::test)]
    #[ignore] // This test is quite slow in debug mode
    async fn test_jumbo_list() {
        // This is an overflow test.  We have a list of lists where each list
        // has 1Mi items.  We encode 5000 of these lists and so we have over 4Gi in the
        // offsets range
        let items = BooleanArray::new_null(1024 * 1024);
        let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0, 1024 * 1024]));
        let list_arr = Arc::new(ListArray::new(
            Arc::new(Field::new("item", DataType::Boolean, true)),
            offsets,
            Arc::new(items),
            None,
        )) as ArrayRef;
        let arrs = vec![list_arr; 5000];

        // We can't validate because our validation relies on concatenating all input arrays
        let test_cases = TestCases::default().without_validation();
        check_round_trip_encoding_of_data(arrs, &test_cases, HashMap::new()).await;
    }

    // Regression test for issue with ListArray encoding when crossing 1024 value boundary
    // This test reproduces the bug where rows_avail assertion fails in schedule_instructions
    // when encoding a ListArray with specific size patterns that cross the 1024 value boundary
    #[tokio::test]
    async fn test_fuzz_issue_4466() {
        // This specific pattern of list sizes triggers the bug when total values cross 1024
        // 94 lists total 1009 values (passes), 95 lists total 1025 values (fails)
        let list_sizes = vec![
            13, 18, 12, 7, 14, 12, 6, 13, 18, 8, // 0-9: 119 values
            6, 11, 17, 12, 8, 19, 5, 6, 10, 13, // 10-19: 107 values
            8, 6, 10, 4, 8, 16, 14, 12, 18, 9, // 20-29: 105 values
            17, 8, 14, 18, 15, 3, 2, 4, 5, 1, // 30-39: 82 values
            3, 13, 1, 2, 10, 4, 10, 18, 7, 14, // 40-49: 75 values
            18, 13, 9, 17, 3, 13, 10, 14, 8, 19, // 50-59: 125 values
            17, 10, 5, 11, 6, 15, 10, 18, 18, 20, // 60-69: 130 values
            16, 11, 12, 15, 7, 9, 3, 10, 20, 5, // 70-79: 102 values
            2, 3, 17, 4, 8, 12, 15, 6, 3, 20, // 80-89: 90 values
            15, 20, 1, 19, 16, // 90-94: 71 values
        ];

        // Build the ListArray
        let mut list_builder = ListBuilder::new(Int32Builder::new());
        let mut total_values = 0;

        for size in &list_sizes {
            for i in 0..*size {
                list_builder.values().append_value(i);
            }
            list_builder.append(true);
            total_values += size;
        }

        let list_array = Arc::new(list_builder.finish());

        // Verify we have the expected number of values
        assert_eq!(list_array.len(), 95);
        assert_eq!(total_values, 1025);

        // This should trigger the assertion failure at primitive.rs:1362
        // debug_assert!(rows_avail > 0)
        let test_cases = TestCases::default().with_structural_encodings();

        // The bug manifests when encoding this specific pattern
        // Expected: successful round-trip encoding
        // Actual: panic at primitive.rs:1362 - assertion failed: rows_avail > 0
        check_round_trip_encoding_of_data(vec![list_array], &test_cases, HashMap::new()).await;
    }

    #[rstest]
    #[test_log::test(tokio::test)]
    async fn test_sparse_large_string_list(
        #[values(STRUCTURAL_ENCODING_MINIBLOCK, STRUCTURAL_ENCODING_FULLZIP)]
        structural_encoding: &str,
    ) {
        // 2.5 million rows, mostly empty lists. ~100 lists have 10 short strings each.
        let num_rows = 2_500_000u32;
        let num_non_empty = 100u32;
        let strings_per_list = 10;

        let items_builder = StringBuilder::new();
        let mut list_builder = ListBuilder::new(items_builder);

        // Spread non-empty lists evenly across the range
        let step = num_rows / num_non_empty;
        let mut next_non_empty = step / 2;

        for i in 0..num_rows {
            if i == next_non_empty {
                let vals: Vec<Option<&str>> = (0..strings_per_list)
                    .map(|j| match j % 4 {
                        0 => Some("a"),
                        1 => Some("bb"),
                        2 => Some("ccc"),
                        _ => Some("d"),
                    })
                    .collect();
                list_builder.append_value(vals);
                next_non_empty = next_non_empty.saturating_add(step);
            } else {
                list_builder.append_value([] as [Option<&str>; 0]);
            }
        }
        let list_array = list_builder.finish();

        let mut field_metadata = HashMap::new();
        field_metadata.insert(
            STRUCTURAL_ENCODING_META_KEY.to_string(),
            structural_encoding.into(),
        );

        let test_cases = TestCases::default()
            .with_range(0..1000)
            .with_range(0..num_rows as u64)
            .with_indices(vec![0, (step / 2) as u64, num_rows as u64 - 1])
            .with_dense_encodings();
        check_round_trip_encoding_of_data(vec![Arc::new(list_array)], &test_cases, field_metadata)
            .await;
    }

    #[test_log::test(tokio::test)]
    async fn test_sparse_boolean_list_uses_miniblock() {
        // Redacted reproduction from a production schema shape containing ARRAY(BOOLEAN).
        // The field names are not relevant; the failure requires sparse list structure
        // with a 1-bit Boolean leaf value.
        let num_rows = 200_000usize;
        let num_non_empty = 10usize;
        let booleans_per_list = 8usize;
        let step = num_rows / num_non_empty;

        let mut offsets = Vec::with_capacity(num_rows + 1);
        let mut values = Vec::with_capacity(num_non_empty * booleans_per_list);
        offsets.push(0i32);

        let mut next_non_empty = step / 2;
        for row in 0..num_rows {
            if row == next_non_empty {
                values.extend((0..booleans_per_list).map(|idx| idx % 2 == 0));
                next_non_empty += step;
            }
            offsets.push(values.len() as i32);
        }

        let items = BooleanArray::from(values);
        let list_array = ListArray::new(
            Arc::new(Field::new("item", DataType::Boolean, true)),
            OffsetBuffer::new(ScalarBuffer::from(offsets)),
            Arc::new(items),
            None,
        );

        let test_cases = TestCases::default()
            .with_range(0..1000)
            .with_range(0..num_rows as u64)
            .with_indices(vec![0, (step / 2) as u64, num_rows as u64 - 1])
            .with_dense_encodings();
        let list_array = Arc::new(list_array) as ArrayRef;
        let pages = encode_v22_pages(list_array.clone()).await;
        assert_split_miniblock_layout(&pages, false);
        check_round_trip_encoding_of_data(vec![list_array], &test_cases, HashMap::new()).await;
    }

    #[test_log::test(tokio::test)]
    async fn test_sparse_boolean_list_with_long_empty_prefix() {
        let empty_prefix_rows = 70_000usize;
        let trailing_empty_rows = 9usize;
        let booleans_per_list = 8usize;
        let num_rows = empty_prefix_rows + 1 + trailing_empty_rows;

        let mut offsets = Vec::with_capacity(num_rows + 1);
        offsets.extend(std::iter::repeat_n(0i32, empty_prefix_rows + 1));
        let values = (0..booleans_per_list)
            .map(|idx| idx % 2 == 0)
            .collect::<Vec<_>>();
        offsets.push(values.len() as i32);
        offsets.extend(std::iter::repeat_n(
            values.len() as i32,
            trailing_empty_rows,
        ));

        let items = BooleanArray::from(values);
        let list_array = ListArray::new(
            Arc::new(Field::new("item", DataType::Boolean, true)),
            OffsetBuffer::new(ScalarBuffer::from(offsets)),
            Arc::new(items),
            None,
        );

        let test_cases = TestCases::default()
            .with_range(0..num_rows as u64)
            .with_indices(vec![0, empty_prefix_rows as u64, num_rows as u64 - 1])
            .with_dense_encodings();
        let list_array = Arc::new(list_array) as ArrayRef;
        let pages = encode_v22_pages(list_array.clone()).await;
        assert_split_miniblock_layout(&pages, true);
        check_round_trip_encoding_of_data(vec![list_array], &test_cases, HashMap::new()).await;
    }

    #[test_log::test(tokio::test)]
    async fn test_sparse_boolean_list_with_long_null_prefix() {
        let null_prefix_rows = 70_000usize;
        let trailing_empty_rows = 9usize;
        let booleans_per_list = 8usize;
        let num_rows = null_prefix_rows + 1 + trailing_empty_rows;

        let mut offsets = Vec::with_capacity(num_rows + 1);
        offsets.extend(std::iter::repeat_n(0i32, null_prefix_rows + 1));
        let values = (0..booleans_per_list)
            .map(|idx| idx % 2 == 0)
            .collect::<Vec<_>>();
        offsets.push(values.len() as i32);
        offsets.extend(std::iter::repeat_n(
            values.len() as i32,
            trailing_empty_rows,
        ));
        let validity = BooleanBuffer::from_iter((0..num_rows).map(|row| row >= null_prefix_rows));

        let items = BooleanArray::from(values);
        let list_array = ListArray::new(
            Arc::new(Field::new("item", DataType::Boolean, true)),
            OffsetBuffer::new(ScalarBuffer::from(offsets)),
            Arc::new(items),
            Some(NullBuffer::new(validity)),
        );

        let test_cases = TestCases::default()
            .with_range(0..num_rows as u64)
            .with_indices(vec![0, null_prefix_rows as u64, num_rows as u64 - 1])
            .with_dense_encodings();
        let list_array = Arc::new(list_array) as ArrayRef;
        let pages = encode_v22_pages(list_array.clone()).await;
        assert_split_miniblock_layout(&pages, true);
        check_round_trip_encoding_of_data(vec![list_array], &test_cases, HashMap::new()).await;
    }

    #[test_log::test(tokio::test)]
    async fn test_sparse_boolean_list_with_amortized_long_empty_prefix() {
        let empty_prefix_rows = 62_000usize;
        let booleans_per_list = 8_192usize;
        let num_rows = empty_prefix_rows + 1;

        let mut offsets = Vec::with_capacity(num_rows + 1);
        offsets.extend(std::iter::repeat_n(0i32, empty_prefix_rows + 1));
        let values = (0..booleans_per_list)
            .map(|idx| idx % 2 == 0)
            .collect::<Vec<_>>();
        offsets.push(values.len() as i32);

        let items = BooleanArray::from(values);
        let list_array = ListArray::new(
            Arc::new(Field::new("item", DataType::Boolean, true)),
            OffsetBuffer::new(ScalarBuffer::from(offsets)),
            Arc::new(items),
            None,
        );

        let test_cases = TestCases::default()
            .with_range(0..num_rows as u64)
            .with_indices(vec![0, empty_prefix_rows as u64])
            .with_dense_encodings();
        let list_array = Arc::new(list_array) as ArrayRef;
        let pages = encode_v22_pages(list_array.clone()).await;
        assert_split_miniblock_layout(&pages, true);
        check_round_trip_encoding_of_data(vec![list_array], &test_cases, HashMap::new()).await;
    }

    #[test_log::test(tokio::test)]
    async fn test_nested_sparse_boolean_list_fails_without_panic() {
        let empty_inner_lists = 70_000usize;
        let booleans_per_list = 8usize;

        let mut inner_offsets = vec![0i32; empty_inner_lists + 1];
        let values = (0..booleans_per_list)
            .map(|idx| idx % 2 == 0)
            .collect::<Vec<_>>();
        inner_offsets.push(values.len() as i32);

        let inner_items = BooleanArray::from(values);
        let inner_list = ListArray::new(
            Arc::new(Field::new("item", DataType::Boolean, true)),
            OffsetBuffer::new(ScalarBuffer::from(inner_offsets)),
            Arc::new(inner_items),
            None,
        );
        let outer_list = ListArray::new(
            Arc::new(Field::new("item", inner_list.data_type().clone(), true)),
            OffsetBuffer::new(ScalarBuffer::from(vec![0i32, empty_inner_lists as i32 + 1])),
            Arc::new(inner_list),
            None,
        );

        let err = try_encode_v22_pages(Arc::new(outer_list))
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("Mini-block cannot encode"),
            "unexpected error: {err}"
        );
    }

    #[test_log::test(tokio::test)]
    async fn test_nested_sparse_string_single_row_falls_back_to_fullzip() {
        let empty_inner_lists = 70_000usize;

        let mut inner_offsets = vec![0i32; empty_inner_lists + 1];
        inner_offsets.push(1);
        inner_offsets.push(2);

        let mut strings = StringBuilder::new();
        strings.append_value("value");
        strings.append_value("other");
        let inner_items = strings.finish();
        let inner_list = ListArray::new(
            Arc::new(Field::new("item", DataType::Utf8, true)),
            OffsetBuffer::new(ScalarBuffer::from(inner_offsets)),
            Arc::new(inner_items),
            None,
        );
        let outer_list = ListArray::new(
            Arc::new(Field::new("item", inner_list.data_type().clone(), true)),
            OffsetBuffer::new(ScalarBuffer::from(vec![0i32, empty_inner_lists as i32 + 2])),
            Arc::new(inner_list),
            None,
        );

        let outer_list = Arc::new(outer_list) as ArrayRef;
        let pages = encode_v22_pages(outer_list.clone()).await;
        assert_has_fullzip_layout(&pages);

        let test_cases = TestCases::default()
            .with_range(0..1)
            .with_indices(vec![0])
            .with_dense_encodings();
        check_round_trip_encoding_of_data(vec![outer_list], &test_cases, HashMap::new()).await;
    }

    /// Builds the HNSW-flush repro shape: a dense prefix where every row has
    /// `NEIGHBORS_PER_ROW` distinct values, followed by a long tail of empty
    /// lists. Mirrors `HNSW::schema()` `__neighbors` / `__dists` columns:
    /// dense level-0 lists, then ~6x as many mostly-empty higher-level rows.
    fn make_hnsw_shaped_list_u32() -> ListArray {
        const DENSE_ROWS: u32 = 40_000;
        const NEIGHBORS_PER_ROW: u32 = 32;
        const EMPTY_TAIL_ROWS: u32 = 240_000;

        let mut list_builder = ListBuilder::new(UInt32Builder::new());
        let mut next_val: u32 = 0;
        for _ in 0..DENSE_ROWS {
            for _ in 0..NEIGHBORS_PER_ROW {
                list_builder.values().append_value(next_val);
                next_val = next_val.wrapping_add(1);
            }
            list_builder.append(true);
        }
        for _ in 0..EMPTY_TAIL_ROWS {
            list_builder.append(true);
        }
        list_builder.finish()
    }

    /// Reproduces the HNSW-flush shape at v2.2 on the auto path (no
    /// `STRUCTURAL_ENCODING` metadata): a dense level-0 prefix followed by a
    /// long tail of empty lists. The global levels/values ratio looks dense,
    /// so this used to encode as a single mini-block page whose final chunk
    /// absorbed every trailing empty list and overflowed the per-chunk `u16`
    /// `num_levels`, corrupting the read. The structural page planner now
    /// splits on top-level row boundaries: the dense prefix stays on
    /// mini-block pages and the empty tail becomes structural-only pages, so
    /// the round-trip is lossless without falling back to full-zip.
    #[test_log::test(tokio::test)]
    async fn test_list_hnsw_shape_splits_to_miniblock_v2_2() {
        let list_array = make_hnsw_shaped_list_u32();
        let dense_rows: u64 = 40_000;
        let total_rows = list_array.len() as u64;

        let test_cases = TestCases::default()
            .with_range(0..1000)
            .with_range(dense_rows.saturating_sub(8)..(dense_rows + 8))
            .with_range(0..total_rows)
            .with_indices(vec![0, dense_rows - 1, dense_rows, total_rows - 1])
            .with_encoding(TestEncoding::StructuralU32);
        let list_array = Arc::new(list_array) as ArrayRef;
        let pages = encode_v22_pages(list_array.clone()).await;
        assert_split_miniblock_layout(&pages, true);
        check_round_trip_encoding_of_data(vec![list_array], &test_cases, HashMap::new()).await;
    }

    /// Companion to the auto-path test: even when the user explicitly requests
    /// `STRUCTURAL_ENCODING_MINIBLOCK`, the structural page planner splits the
    /// HNSW shape so every emitted page fits the mini-block per-chunk budget.
    /// The request is honored (the dense prefix stays on mini-block pages
    /// rather than being forced to full-zip) and the round-trip is lossless.
    #[test_log::test(tokio::test)]
    async fn test_forced_miniblock_hnsw_shape_splits_to_miniblock_v2_2() {
        let list_array = make_hnsw_shaped_list_u32();
        let total_rows = list_array.len() as u64;

        let mut field_metadata = HashMap::new();
        field_metadata.insert(
            STRUCTURAL_ENCODING_META_KEY.to_string(),
            STRUCTURAL_ENCODING_MINIBLOCK.into(),
        );

        let test_cases = TestCases::default()
            .with_range(0..total_rows)
            .with_encoding(TestEncoding::StructuralU32);
        let list_array = Arc::new(list_array) as ArrayRef;
        let pages = try_encode_v22_pages_with_metadata(list_array.clone(), field_metadata.clone())
            .await
            .unwrap();
        assert_split_miniblock_layout(&pages, true);
        check_round_trip_encoding_of_data(vec![list_array], &test_cases, field_metadata).await;
    }
}
