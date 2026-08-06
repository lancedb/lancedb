// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Lance v2.0 file composition.

use std::{collections::BTreeMap, sync::Arc};

use bytes::Bytes;
use lance_core::{
    Result,
    datatypes::{Field, Schema},
};
use lance_encoding::{
    decoder::PageInfo,
    encoder::{ArrayFieldEncodingStrategy, EncodedBatch, FieldEncodingStrategy},
};
use lance_io::traits::Writer as ObjectWriter;

use crate::{reader::ReadProjection, writer::FileWriterOptions};

mod reader;
mod writer;

#[cfg(test)]
pub(crate) use reader::test_projection_length;
pub(crate) use reader::{
    decode_column_metadata, finish_metadata, finish_metadata_index, validate_global_buffers,
};
pub use reader::{
    projection_from_column_names, projection_from_field_ids, projection_from_whole_schema,
};
pub use writer::Writer;

pub(crate) fn read_projection() -> Arc<dyn ReadProjection> {
    reader::read_projection()
}

/// Count physical columns represented by a field in a v2.0 footer.
pub fn physical_column_count(field: &Field) -> usize {
    if field.is_blob() || field.is_packed_struct() {
        1
    } else {
        1 + field
            .children
            .iter()
            .map(physical_column_count)
            .sum::<usize>()
    }
}

/// Build persisted field-to-column entries for a v2.0 data file.
pub fn data_file_columns(schema: &Schema) -> (Vec<i32>, Vec<i32>) {
    let mut field_ids = Vec::new();
    let mut column_indices = Vec::new();
    append_physical_fields(&schema.fields, &mut field_ids, &mut column_indices, &mut 0);
    (field_ids, column_indices)
}

pub(super) fn field_id_to_column_index(schema: &Schema) -> BTreeMap<u32, u32> {
    let (field_ids, column_indices) = data_file_columns(schema);
    field_ids
        .into_iter()
        .zip(column_indices)
        .map(|(field_id, column_index)| (field_id as u32, column_index as u32))
        .collect()
}

fn append_physical_fields(
    fields: &[Field],
    field_ids: &mut Vec<i32>,
    column_indices: &mut Vec<i32>,
    next_column: &mut i32,
) {
    for field in fields {
        field_ids.push(field.id);
        column_indices.push(*next_column);
        *next_column += 1;
        if !field.is_blob() && !field.is_packed_struct() {
            append_physical_fields(&field.children, field_ids, column_indices, next_column);
        }
    }
}

fn is_external_metadata_structural_header(
    fields: &[Field],
    target_column: usize,
    next_column: &mut usize,
) -> Option<bool> {
    for field in fields {
        if *next_column == target_column {
            return Some(field.logical_type.is_struct() && !field.is_packed_struct());
        }
        *next_column += 1;
        if !field.is_blob()
            && !field.is_packed_struct()
            && let Some(is_header) =
                is_external_metadata_structural_header(&field.children, target_column, next_column)
        {
            return Some(is_header);
        }
    }
    None
}

pub(super) fn should_copy_external_metadata_column(
    schema: &Schema,
    column_index: usize,
    has_existing_pages: bool,
) -> bool {
    let mut next_column = 0;
    let is_header =
        is_external_metadata_structural_header(&schema.fields, column_index, &mut next_column)
            .unwrap_or(false);
    !is_header || !has_existing_pages
}

pub(super) fn finalize_external_metadata_column(
    schema: &Schema,
    column_index: usize,
    pages: &mut Vec<PageInfo>,
    num_rows: u64,
) {
    let mut next_column = 0;
    let is_header =
        is_external_metadata_structural_header(&schema.fields, column_index, &mut next_column)
            .unwrap_or(false);
    if is_header && !pages.is_empty() {
        pages[0].num_rows = num_rows;
        pages[0].priority = 0;
        pages.truncate(1);
    }
}

/// Compose the v2.0 field encoding mechanisms.
pub fn encoding_strategy() -> Arc<dyn FieldEncodingStrategy> {
    Arc::new(ArrayFieldEncodingStrategy::new())
}

/// Create a v2.0 writer with an explicit schema.
pub fn create_writer(
    object_writer: Box<dyn ObjectWriter>,
    schema: Schema,
    options: FileWriterOptions,
) -> Result<Writer> {
    Writer::try_new(object_writer, schema, options)
}

/// Create a v2.0 writer whose schema is inferred from the first batch.
pub fn create_lazy_writer(
    object_writer: Box<dyn ObjectWriter>,
    options: FileWriterOptions,
) -> Writer {
    Writer::new_lazy(object_writer, options)
}

/// Encode a self-described v2.0 batch.
pub fn encode_self_described_batch(batch: &EncodedBatch) -> Result<Bytes> {
    writer::concat_lance_footer(batch, true)
}

/// Encode a mini-lance v2.0 batch.
pub fn encode_mini_batch(batch: &EncodedBatch) -> Result<Bytes> {
    writer::concat_lance_footer(batch, false)
}
