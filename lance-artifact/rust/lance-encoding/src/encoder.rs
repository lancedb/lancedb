// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! The top-level encoding module for Lance files.
//!
//! Lance files are encoded using a [`FieldEncodingStrategy`] which choose
//! what encoder to use for each field.
//!
//! Structural strategies build a tree of encoders for each field from the
//! version-free builders in [`structural`]. Struct and list encoders collect
//! validity and offsets; primitive leaf encoders accumulate values and emit
//! miniblock or full-zip pages.

use std::{collections::HashMap, sync::Arc};

use arrow_array::{Array, ArrayRef, RecordBatch};
use bytes::{Bytes, BytesMut};
use futures::future::BoxFuture;
use lance_core::datatypes::{Field, Schema};
use lance_core::utils::bit::{is_pwr_two, pad_bytes_to};
use lance_core::{Error, Result};

use crate::buffer::LanceBuffer;
use crate::data::DataBlock;
use crate::decoder::PageEncoding;
use crate::repdef::RepDefBuilder;
use crate::{
    decoder::{ColumnInfo, PageInfo},
    format::pb,
};

pub use crate::array_encoding::ArrayFieldEncodingStrategy;

pub mod structural;

/// The minimum alignment for a page buffer.  Writers must respect this.
pub const MIN_PAGE_BUFFER_ALIGNMENT: u64 = 8;

/// An array encoded with the `pb::ArrayEncoding` grammar.
#[derive(Debug)]
pub struct EncodedArray {
    pub data: DataBlock,
    pub encoding: pb::ArrayEncoding,
}

impl EncodedArray {
    pub fn new(data: DataBlock, encoding: pb::ArrayEncoding) -> Self {
        Self { data, encoding }
    }

    pub fn into_buffers(self) -> (Vec<LanceBuffer>, pb::ArrayEncoding) {
        (self.data.into_buffers(), self.encoding)
    }
}

/// Encodes one data block and describes it with `pb::ArrayEncoding`.
pub trait ArrayEncoder: std::fmt::Debug + Send + Sync {
    fn encode(
        &self,
        data: DataBlock,
        data_type: &arrow_schema::DataType,
        buffer_index: &mut u32,
    ) -> Result<EncodedArray>;
}

/// Selects an `ArrayEncoder` for one page.
pub trait ArrayEncodingStrategy: Send + Sync + std::fmt::Debug {
    fn create_array_encoder(
        &self,
        arrays: &[ArrayRef],
        field: &Field,
    ) -> Result<Box<dyn ArrayEncoder>>;
}

/// An encoded page of data
///
/// Maps to a top-level array
///
/// For example, `FixedSizeList<Int32>` will have two EncodedArray instances and one EncodedPage
#[derive(Debug)]
pub struct EncodedPage {
    // The encoded page buffers
    pub data: Vec<LanceBuffer>,
    // A description of the encoding used to encode the page
    pub description: PageEncoding,
    /// The number of rows in the encoded page
    pub num_rows: u64,
    /// The top-level row number of the first row in the page
    ///
    /// Generally the number of "top-level" rows and the number of rows are the same.  However,
    /// when there is repetition (list/fixed-size-list) there will be more or less items than rows.
    ///
    /// A top-level row can never be split across a page boundary.
    pub row_number: u64,
    /// The index of the column
    pub column_idx: u32,
}

pub struct EncodedColumn {
    pub column_buffers: Vec<LanceBuffer>,
    pub encoding: pb::ColumnEncoding,
    pub final_pages: Vec<EncodedPage>,
}

impl Default for EncodedColumn {
    fn default() -> Self {
        Self {
            column_buffers: Default::default(),
            encoding: pb::ColumnEncoding {
                column_encoding: Some(pb::column_encoding::ColumnEncoding::Values(())),
            },
            final_pages: Default::default(),
        }
    }
}

/// A tool to reserve space for buffers that are not in-line with the data
///
/// In most cases, buffers are stored in the page and referred to in the encoding
/// metadata by their index in the page.  This keeps all buffers within a page together.
/// As a result, most encoders should not need to use this structure.
///
/// In some cases (currently only the large binary encoding) there is a need to access
/// buffers that are not in the page (because storing the position / offset of every page
/// in the page metadata would be too expensive).
///
/// To do this you can add a buffer with `add_buffer` and then use the returned position
/// in some way (in the large binary encoding the returned position is stored in the page
/// data as a position / size array).
pub struct OutOfLineBuffers {
    position: u64,
    buffer_alignment: u64,
    buffers: Vec<LanceBuffer>,
}

impl OutOfLineBuffers {
    pub fn new(base_position: u64, buffer_alignment: u64) -> Self {
        Self {
            position: base_position,
            buffer_alignment,
            buffers: Vec::new(),
        }
    }

    pub fn add_buffer(&mut self, buffer: LanceBuffer) -> u64 {
        let position = self.position;
        self.position += buffer.len() as u64;
        self.position += pad_bytes_to(buffer.len(), self.buffer_alignment as usize) as u64;
        self.buffers.push(buffer);
        position
    }

    pub fn take_buffers(self) -> Vec<LanceBuffer> {
        self.buffers
    }

    pub fn reset_position(&mut self, position: u64) {
        self.position = position;
    }
}

/// A task to create a page of data
pub type EncodeTask = BoxFuture<'static, Result<EncodedPage>>;

/// Top level encoding trait to code any Arrow array type into one or more pages.
///
/// The field encoder implements buffering and encoding of a single input column
/// but it may map to multiple output columns.  For example, a list array or struct
/// array will be encoded into multiple columns.
///
/// Also, fields may be encoded at different speeds.  For example, given a struct
/// column with three fields (a boolean field, an int32 field, and a 4096-dimension
/// tensor field) the tensor field is likely to emit encoded pages much more frequently
/// than the boolean field.
pub trait FieldEncoder: Send {
    /// Buffer the data and, if there is enough data in the buffer to form a page, return
    /// an encoding task to encode the data.
    ///
    /// This may return more than one task because a single column may be mapped to multiple
    /// output columns.  For example, if encoding a struct column with three children then
    /// up to three tasks may be returned from each call to maybe_encode.
    ///
    /// It may also return multiple tasks for a single column if the input array is larger
    /// than a single disk page.
    ///
    /// It could also return an empty Vec if there is not enough data yet to encode any pages.
    ///
    /// The `row_number` must be passed which is the top-level row number currently being encoded
    /// This is stored in any pages produced by this call so that we can know the priority of the
    /// page.
    ///
    /// The `num_rows` is the number of top level rows.  It is initially the same as `array.len()`
    /// however it is passed seprately because array will become flattened over time (if there is
    /// repetition) and we need to know the original number of rows for various purposes.
    fn maybe_encode(
        &mut self,
        array: ArrayRef,
        external_buffers: &mut OutOfLineBuffers,
        repdef: RepDefBuilder,
        row_number: u64,
        num_rows: u64,
    ) -> Result<Vec<EncodeTask>>;
    /// Flush any remaining data from the buffers into encoding tasks
    ///
    /// Each encode task produces a single page.  The order of these pages will be maintained
    /// in the file (we do not worry about order between columns but all pages in the same
    /// column should maintain order)
    ///
    /// This may be called intermittently throughout encoding but will always be called
    /// once at the end of encoding just before calling finish
    fn flush(&mut self, external_buffers: &mut OutOfLineBuffers) -> Result<Vec<EncodeTask>>;
    /// Finish encoding and return column metadata
    ///
    /// This is called only once, after all encode tasks have completed
    ///
    /// This returns a Vec because a single field may have created multiple columns
    fn finish(
        &mut self,
        external_buffers: &mut OutOfLineBuffers,
    ) -> BoxFuture<'_, Result<Vec<EncodedColumn>>>;

    /// The number of output columns this encoding will create
    fn num_columns(&self) -> u32;
}

/// Keeps track of the current column index and makes a mapping
/// from field id to column index
#[derive(Debug, Default)]
pub struct ColumnIndexSequence {
    current_index: u32,
    mapping: Vec<(u32, u32)>,
}

impl ColumnIndexSequence {
    pub fn next_column_index(&mut self, field_id: u32) -> u32 {
        let idx = self.current_index;
        self.current_index += 1;
        self.mapping.push((field_id, idx));
        idx
    }

    pub fn skip(&mut self) {
        self.current_index += 1;
    }
}

/// Options that control the encoding process
pub struct EncodingOptions {
    /// How much data (in bytes) to cache in-memory before writing a page
    ///
    /// This cache is applied on a per-column basis
    pub cache_bytes_per_column: u64,
    /// The maximum size of a page in bytes, if a single array would create
    /// a page larger than this then it will be split into multiple pages
    pub max_page_bytes: u64,
    /// If false (the default) then arrays will be copied (deeply) before
    /// being cached.  This ensures any data kept alive by the array can
    /// be discarded safely and helps avoid writer accumulation.  However,
    /// there is an associated cost.
    pub keep_original_array: bool,
    /// The alignment that the writer is applying to buffers
    ///
    /// The encoder needs to know this so it figures the position of out-of-line
    /// buffers correctly
    pub buffer_alignment: u64,
}

impl Default for EncodingOptions {
    fn default() -> Self {
        Self {
            cache_bytes_per_column: 8 * 1024 * 1024,
            max_page_bytes: 32 * 1024 * 1024,
            keep_original_array: true,
            buffer_alignment: 64,
        }
    }
}

/// A trait to pick which kind of field encoding to use for a field
///
/// Unlike the ArrayEncodingStrategy, the field encoding strategy is
/// chosen before any data is generated and the same field encoder is
/// used for all data in the field.
pub trait FieldEncodingStrategy: Send + Sync + std::fmt::Debug {
    /// Choose and create an appropriate field encoder for the given
    /// field.
    ///
    /// The field encoder can be chosen on the data type as well as
    /// any metadata that is attached to the field.
    ///
    fn create_field_encoder(
        &self,
        field: &Field,
        column_index: &mut ColumnIndexSequence,
        context: &FieldEncodingContext<'_>,
    ) -> Result<Box<dyn FieldEncoder>>;
}

/// Context shared while one top-level field and all of its children are mapped
/// to concrete field encoders.
pub struct FieldEncodingContext<'a> {
    /// The complete strategy composition used for recursive child fields.
    pub strategy: &'a dyn FieldEncodingStrategy,
    /// Runtime-only writer options.
    pub options: &'a EncodingOptions,
    /// Metadata inherited from the top-level field.
    pub root_field_metadata: &'a HashMap<String, String>,
}

/// A batch encoder that encodes RecordBatch objects by delegating
/// to field encoders for each top-level field in the batch.
pub struct BatchEncoder {
    pub field_encoders: Vec<Box<dyn FieldEncoder>>,
    pub field_id_to_column_index: Vec<(u32, u32)>,
}

impl BatchEncoder {
    pub fn try_new(
        schema: &Schema,
        strategy: &dyn FieldEncodingStrategy,
        options: &EncodingOptions,
    ) -> Result<Self> {
        let mut col_idx = 0;
        let mut col_idx_sequence = ColumnIndexSequence::default();
        let field_encoders = schema
            .fields
            .iter()
            .map(|field| {
                let context = FieldEncodingContext {
                    strategy,
                    options,
                    root_field_metadata: &field.metadata,
                };
                let encoder =
                    strategy.create_field_encoder(field, &mut col_idx_sequence, &context)?;
                col_idx += encoder.as_ref().num_columns();
                Ok(encoder)
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            field_encoders,
            field_id_to_column_index: col_idx_sequence.mapping,
        })
    }

    pub fn num_columns(&self) -> u32 {
        self.field_encoders
            .iter()
            .map(|field_encoder| field_encoder.num_columns())
            .sum::<u32>()
    }
}

/// An encoded batch of data and a page table describing it
///
/// This is returned by [`crate::encoder::encode_batch`]
#[derive(Debug)]
pub struct EncodedBatch {
    pub data: Bytes,
    pub page_table: Vec<Arc<ColumnInfo>>,
    pub schema: Arc<Schema>,
    pub top_level_columns: Vec<u32>,
    pub num_rows: u64,
}

fn write_page_to_data_buffer(page: EncodedPage, data_buffer: &mut BytesMut) -> PageInfo {
    let buffers = page.data;
    let mut buffer_offsets_and_sizes = Vec::with_capacity(buffers.len());
    for buffer in buffers {
        let buffer_offset = data_buffer.len() as u64;
        data_buffer.extend_from_slice(&buffer);
        let size = data_buffer.len() as u64 - buffer_offset;
        buffer_offsets_and_sizes.push((buffer_offset, size));
    }

    PageInfo {
        buffer_offsets_and_sizes: Arc::from(buffer_offsets_and_sizes),
        encoding: page.description,
        num_rows: page.num_rows,
        priority: page.row_number,
    }
}

/// Helper method to encode a batch of data into memory
///
/// This is primarily for testing and benchmarking but could be useful in other
/// niche situations like IPC.
pub async fn encode_batch(
    batch: &RecordBatch,
    schema: Arc<Schema>,
    encoding_strategy: &dyn FieldEncodingStrategy,
    options: &EncodingOptions,
) -> Result<EncodedBatch> {
    if !is_pwr_two(options.buffer_alignment) || options.buffer_alignment < MIN_PAGE_BUFFER_ALIGNMENT
    {
        return Err(Error::invalid_input_source(
            format!(
                "buffer_alignment must be a power of two and at least {}",
                MIN_PAGE_BUFFER_ALIGNMENT
            )
            .into(),
        ));
    }

    let mut data_buffer = BytesMut::new();
    let lance_schema = Schema::try_from(batch.schema().as_ref())?;
    let options = EncodingOptions {
        keep_original_array: true,
        ..*options
    };
    let batch_encoder = BatchEncoder::try_new(&lance_schema, encoding_strategy, &options)?;
    let mut page_table = Vec::new();
    let mut col_idx_offset = 0;
    for (arr, mut encoder) in batch.columns().iter().zip(batch_encoder.field_encoders) {
        let mut external_buffers =
            OutOfLineBuffers::new(data_buffer.len() as u64, options.buffer_alignment);
        let repdef = RepDefBuilder::default();
        let encoder = encoder.as_mut();
        let num_rows = arr.len() as u64;
        let mut tasks =
            encoder.maybe_encode(arr.clone(), &mut external_buffers, repdef, 0, num_rows)?;
        tasks.extend(encoder.flush(&mut external_buffers)?);
        for buffer in external_buffers.take_buffers() {
            data_buffer.extend_from_slice(&buffer);
        }
        let mut pages = HashMap::<u32, Vec<PageInfo>>::new();
        for task in tasks {
            let encoded_page = task.await?;
            // Write external buffers first
            pages
                .entry(encoded_page.column_idx)
                .or_default()
                .push(write_page_to_data_buffer(encoded_page, &mut data_buffer));
        }
        let mut external_buffers =
            OutOfLineBuffers::new(data_buffer.len() as u64, options.buffer_alignment);
        let encoded_columns = encoder.finish(&mut external_buffers).await?;
        for buffer in external_buffers.take_buffers() {
            data_buffer.extend_from_slice(&buffer);
        }
        let num_columns = encoded_columns.len();
        for (col_idx, encoded_column) in encoded_columns.into_iter().enumerate() {
            let col_idx = col_idx + col_idx_offset;
            let mut col_buffer_offsets_and_sizes = Vec::new();
            for buffer in encoded_column.column_buffers {
                let buffer_offset = data_buffer.len() as u64;
                data_buffer.extend_from_slice(&buffer);
                let size = data_buffer.len() as u64 - buffer_offset;
                col_buffer_offsets_and_sizes.push((buffer_offset, size));
            }
            for page in encoded_column.final_pages {
                pages
                    .entry(page.column_idx)
                    .or_default()
                    .push(write_page_to_data_buffer(page, &mut data_buffer));
            }
            let col_pages = std::mem::take(pages.entry(col_idx as u32).or_default());
            page_table.push(Arc::new(ColumnInfo {
                index: col_idx as u32,
                buffer_offsets_and_sizes: Arc::from(
                    col_buffer_offsets_and_sizes.into_boxed_slice(),
                ),
                page_infos: Arc::from(col_pages.into_boxed_slice()),
                encoding: encoded_column.encoding,
            }))
        }
        col_idx_offset += num_columns;
    }
    let top_level_columns = batch_encoder
        .field_id_to_column_index
        .iter()
        .map(|(_, idx)| *idx)
        .collect();
    Ok(EncodedBatch {
        data: data_buffer.freeze(),
        top_level_columns,
        page_table,
        schema,
        num_rows: batch.num_rows() as u64,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::{TestEncoding, create_test_field_encoder, test_encoding_strategy};
    use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Fields as ArrowFields};

    #[test]
    fn test_fixed_size_list_struct_requires_v2_2() {
        let list_item = ArrowField::new(
            "item",
            ArrowDataType::Struct(ArrowFields::from(vec![ArrowField::new(
                "x",
                ArrowDataType::Int32,
                true,
            )])),
            true,
        );
        let arrow_field = ArrowField::new(
            "list_struct",
            ArrowDataType::FixedSizeList(Arc::new(list_item), 2),
            true,
        );
        let field = Field::try_from(&arrow_field).unwrap();

        let strategy = test_encoding_strategy(TestEncoding::StructuralU16);
        let mut column_index = ColumnIndexSequence::default();
        let options = EncodingOptions::default();

        let result =
            create_test_field_encoder(strategy.as_ref(), &field, &mut column_index, &options);
        assert!(
            result.is_err(),
            "FixedSizeList<Struct> should be rejected for file version 2.1"
        );
        let err = result.err().unwrap();

        assert!(
            err.to_string()
                .contains("FixedSizeList<Struct> is not enabled by the selected file format")
        );
    }
}
