// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Lance v1 file implementation.
//!
//! This module is the canonical home of the v1 reader, writer, metadata, and
//! page-table grammar. V1 accepts footer versions `(0, 0)` through `(0, 2)` and
//! writes the `(0, 2)` identity used by [`writer::FileWriter`].

pub mod encoding;
pub mod format;
pub mod page_table;
pub mod reader;
pub mod writer;

use std::{collections::BTreeMap, sync::Arc};

use lance_core::{
    Result,
    datatypes::{Field, Schema},
};

use crate::reader::ReaderProjection;

fn append_field_ids(
    fields: &[Field],
    field_id_to_column_index: &BTreeMap<u32, u32>,
    column_indices: &mut Vec<u32>,
) {
    for field in fields {
        if let Some(column_index) = field_id_to_column_index.get(&(field.id as u32)).copied() {
            column_indices.push(column_index);
        }
        if !field.is_blob() && !field.is_packed_struct() {
            append_field_ids(&field.children, field_id_to_column_index, column_indices);
        }
    }
}

pub fn projection_from_field_ids(
    schema: &Schema,
    field_id_to_column_index: &BTreeMap<u32, u32>,
) -> ReaderProjection {
    let mut column_indices = Vec::new();
    append_field_ids(
        &schema.fields,
        field_id_to_column_index,
        &mut column_indices,
    );
    ReaderProjection {
        schema: Arc::new(schema.clone()),
        column_indices,
    }
}

pub fn projection_from_whole_schema(schema: &Schema) -> ReaderProjection {
    projection_from_field_ids(schema, &field_id_to_column_index(schema))
}

pub fn projection_from_column_names(
    schema: &Schema,
    column_names: &[&str],
) -> Result<ReaderProjection> {
    let field_id_to_column_index = field_id_to_column_index(schema);
    let projected = schema.project(column_names)?;
    Ok(projection_from_field_ids(
        &projected,
        &field_id_to_column_index,
    ))
}

/// Count physical columns represented by a field in a v1 footer.
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

/// Build persisted field-to-column entries for a v1 data file.
pub fn data_file_columns(schema: &Schema) -> (Vec<i32>, Vec<i32>) {
    let mut field_ids = Vec::new();
    let mut column_indices = Vec::new();
    append_physical_fields(&schema.fields, &mut field_ids, &mut column_indices, &mut 0);
    (field_ids, column_indices)
}

fn field_id_to_column_index(schema: &Schema) -> BTreeMap<u32, u32> {
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
