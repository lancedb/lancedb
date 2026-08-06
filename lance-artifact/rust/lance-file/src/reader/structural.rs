// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::{collections::BTreeMap, sync::Arc};

use async_trait::async_trait;
use bytes::Bytes;
use lance_core::{
    Error, Result,
    cache::LanceCache,
    datatypes::{Field, Schema},
};
use lance_encoding::{
    EncodingsIo,
    decoder::{ColumnInfo, PageEncoding, PageInfo},
    format::{pb, pb21},
};
use prost::{Message, Name};

use crate::{
    format::pbfile,
    reader::{
        BufferDescriptor, FileMetadataIndex, FileMetadataProvider, FileReader, PreparedProjection,
        ReadProjection, ReaderProjection, normalized_column_num_rows, verify_uniform_lengths,
    },
    writer::PAGE_BUFFER_ALIGNMENT,
};

fn fetch_encoding<M: Default + Name + Sized>(encoding: &pbfile::Encoding) -> Result<M> {
    match &encoding.location {
        Some(pbfile::encoding::Location::Indirect(_)) => Err(Error::invalid_input_source(
            "Indirect file encodings are not supported".into(),
        )),
        Some(pbfile::encoding::Location::Direct(encoding)) => {
            let envelope = prost_types::Any::decode(Bytes::from(encoding.encoding.clone()))
                .map_err(|error| {
                    Error::invalid_input_source(
                        format!("Invalid direct {} encoding envelope: {error}", M::NAME).into(),
                    )
                })?;
            envelope.to_msg::<M>().map_err(|error| {
                Error::invalid_input_source(
                    format!("Invalid direct {} encoding: {error}", M::NAME).into(),
                )
            })
        }
        Some(pbfile::encoding::Location::None(_)) => Err(Error::invalid_input_source(
            format!("Missing {} encoding description", M::NAME).into(),
        )),
        None => Err(Error::invalid_input_source(
            format!("Missing {} encoding location", M::NAME).into(),
        )),
    }
}

/// Decode structural page syntax before an exact reader validates its grammar.
pub fn decode_page_layout(
    column_index: u32,
    page_index: usize,
    page: &pbfile::column_metadata::Page,
) -> Result<pb21::PageLayout> {
    fetch_encoding(page.encoding.as_ref().ok_or_else(|| {
        Error::invalid_input_source(
            format!(
                "Column {} page {} is missing its encoding",
                column_index, page_index
            )
            .into(),
        )
    })?)
}

/// Build normalized page metadata after exact grammar validation.
pub fn build_page_info(
    column_index: u32,
    page_index: usize,
    page: &pbfile::column_metadata::Page,
    page_layout: pb21::PageLayout,
) -> Result<PageInfo> {
    if page.buffer_offsets.len() != page.buffer_sizes.len() {
        return Err(Error::invalid_input_source(
            format!(
                "Column {} page {} has {} buffer offsets but {} buffer sizes",
                column_index,
                page_index,
                page.buffer_offsets.len(),
                page.buffer_sizes.len()
            )
            .into(),
        ));
    }
    let buffer_offsets_and_sizes = Arc::from(
        page.buffer_offsets
            .iter()
            .zip(&page.buffer_sizes)
            .map(|(offset, size)| {
                if offset % PAGE_BUFFER_ALIGNMENT as u64 != 0 {
                    return Err(Error::invalid_input_source(
                        format!(
                            "Column {} page {} buffer offset {} is not aligned to {} bytes",
                            column_index, page_index, offset, PAGE_BUFFER_ALIGNMENT
                        )
                        .into(),
                    ));
                }
                Ok((*offset, *size))
            })
            .collect::<Result<Vec<_>>>()?,
    );
    Ok(PageInfo {
        buffer_offsets_and_sizes,
        encoding: PageEncoding::Structural(page_layout),
        num_rows: page.length,
        priority: page.priority,
    })
}

/// Finish normalized column metadata after all pages have been validated.
pub fn build_column_info(
    column_index: u32,
    metadata: &pbfile::ColumnMetadata,
    page_infos: Vec<PageInfo>,
) -> Result<Arc<ColumnInfo>> {
    if metadata.buffer_offsets.len() != metadata.buffer_sizes.len() {
        return Err(Error::invalid_input_source(
            format!(
                "Column {} has {} buffer offsets but {} buffer sizes",
                column_index,
                metadata.buffer_offsets.len(),
                metadata.buffer_sizes.len()
            )
            .into(),
        ));
    }
    let buffer_offsets_and_sizes = Arc::from(
        metadata
            .buffer_offsets
            .iter()
            .zip(&metadata.buffer_sizes)
            .map(|(offset, size)| (*offset, *size))
            .collect::<Vec<_>>(),
    );
    let encoding: pb::ColumnEncoding =
        fetch_encoding(metadata.encoding.as_ref().ok_or_else(|| {
            Error::invalid_input_source(
                format!("Column {} is missing its encoding", column_index).into(),
            )
        })?)?;
    Ok(Arc::new(ColumnInfo {
        index: column_index,
        page_infos: Arc::from(page_infos),
        buffer_offsets_and_sizes,
        encoding,
    }))
}

/// Validate the structural global-buffer alignment contract.
pub fn validate_global_buffers(buffers: &[BufferDescriptor]) -> Result<()> {
    for (buffer_index, buffer) in buffers.iter().enumerate() {
        if buffer.position % PAGE_BUFFER_ALIGNMENT as u64 != 0 {
            return Err(Error::invalid_input_source(
                format!(
                    "Global buffer {} position {} is not aligned to {} bytes",
                    buffer_index, buffer.position, PAGE_BUFFER_ALIGNMENT
                )
                .into(),
            ));
        }
    }
    Ok(())
}

fn field_column_shape(field: &Field) -> (bool, bool) {
    if field.is_blob() || field.is_packed_struct() {
        return (true, false);
    }
    (field.children.is_empty(), !field.children.is_empty())
}

/// Count physical columns in the leaf-column layout used by v2.1+.
pub fn physical_column_count(field: &Field) -> usize {
    if field.children.is_empty() || field.is_blob() || field.is_packed_struct() {
        1
    } else {
        field.children.iter().map(physical_column_count).sum()
    }
}

fn append_physical_fields(
    fields: &[Field],
    field_ids: &mut Vec<i32>,
    column_indices: &mut Vec<i32>,
    next_column: &mut i32,
) {
    for field in fields {
        if field.children.is_empty() || field.is_blob() || field.is_packed_struct() {
            field_ids.push(field.id);
            column_indices.push(*next_column);
            *next_column += 1;
        } else {
            append_physical_fields(&field.children, field_ids, column_indices, next_column);
        }
    }
}

/// Build persisted field-to-column entries for the v2.1+ leaf layout.
pub fn data_file_columns(schema: &Schema) -> (Vec<i32>, Vec<i32>) {
    let mut field_ids = Vec::new();
    let mut column_indices = Vec::new();
    append_physical_fields(&schema.fields, &mut field_ids, &mut column_indices, &mut 0);
    (field_ids, column_indices)
}

/// Build the field-id lookup for the v2.1+ leaf layout.
pub fn field_id_to_column_index(schema: &Schema) -> BTreeMap<u32, u32> {
    let (field_ids, column_indices) = data_file_columns(schema);
    field_ids
        .into_iter()
        .zip(column_indices)
        .filter_map(|(field_id, column_index)| {
            (column_index >= 0).then_some((field_id as u32, column_index as u32))
        })
        .collect()
}

fn append_field_ids(
    fields: &[Field],
    field_id_to_column_index: &BTreeMap<u32, u32>,
    column_indices: &mut Vec<u32>,
) {
    for field in fields {
        let (contributes, recurse) = field_column_shape(field);
        if contributes
            && let Some(column_index) = field_id_to_column_index.get(&(field.id as u32)).copied()
        {
            column_indices.push(column_index);
        }
        if recurse {
            append_field_ids(&field.children, field_id_to_column_index, column_indices);
        }
    }
}

/// Build the leaf-column projection selected by a v2.1+ exact reader.
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

/// Project names using a caller-selected leaf-column mapping.
pub fn projection_from_column_names(
    schema: &Schema,
    column_names: &[&str],
    field_id_to_column_index: &BTreeMap<u32, u32>,
) -> Result<ReaderProjection> {
    let projected = schema.project(column_names)?;
    Ok(projection_from_field_ids(
        &projected,
        field_id_to_column_index,
    ))
}

fn children_share_parent_length(field: &Field) -> bool {
    field.logical_type.is_struct()
}

fn validate_field_length<F: Fn(usize) -> Result<u64>>(
    field: &Field,
    comparable: bool,
    column_indices: &[u32],
    cursor: &mut usize,
    column_len: &F,
) -> Result<u64> {
    let (contributes, recurse) = field_column_shape(field);
    let mut field_rows = None;
    if contributes {
        let column = *column_indices.get(*cursor).ok_or_else(|| {
            Error::invalid_input(format!(
                "projection supplied fewer column indices than its fields require (ran out at field '{}')",
                field.name
            ))
        })?;
        *cursor += 1;
        field_rows = Some(column_len(column as usize)?);
    }
    if recurse {
        let enforce_children = comparable && children_share_parent_length(field);
        for child in &field.children {
            let child_rows =
                validate_field_length(child, enforce_children, column_indices, cursor, column_len)?;
            let expected = *field_rows.get_or_insert(child_rows);
            if enforce_children && child_rows != expected {
                return Err(Error::invalid_input(format!(
                    "cannot read field '{}': its children have differing lengths (child '{}' has {} rows, but the field has {}); a struct's children must all have the same length",
                    field.name, child.name, child_rows, expected
                )));
            }
        }
    }
    field_rows.ok_or_else(|| {
        Error::invalid_input(format!(
            "projected field '{}' maps to no columns",
            field.name
        ))
    })
}

/// Determine the normalized logical read length for a structural projection.
pub fn prepared_read_length(prepared: &PreparedProjection) -> Result<u64> {
    let column_len = |column: usize| {
        let info = prepared.column_infos.get(column).ok_or_else(|| {
            Error::invalid_input(format!(
                "projection references column index {} but only {} columns are available",
                column,
                prepared.column_infos.len()
            ))
        })?;
        normalized_column_num_rows(info)
    };
    projection_length(
        &prepared.decoder_projection.schema,
        &prepared.decoder_projection.column_indices,
        &column_len,
    )
}

fn projection_length<F: Fn(usize) -> Result<u64>>(
    schema: &Schema,
    column_indices: &[u32],
    column_len: &F,
) -> Result<u64> {
    let mut cursor = 0;
    let mut field_lengths = Vec::with_capacity(schema.fields.len());
    for field in &schema.fields {
        let rows = validate_field_length(field, true, column_indices, &mut cursor, column_len)?;
        field_lengths.push((field.name.as_str(), rows));
    }
    if cursor != column_indices.len() {
        return Err(Error::invalid_input(format!(
            "projection supplied {} column indices but its fields require {}",
            column_indices.len(),
            cursor
        )));
    }
    verify_uniform_lengths(&field_lengths)
}

pub type DecodeColumn = fn(u32, &pbfile::ColumnMetadata) -> Result<Arc<ColumnInfo>>;

#[derive(Debug)]
struct StructuralReadProjection {
    decode_column: DecodeColumn,
}

/// Compose structural projection execution with an exact column grammar.
pub fn read_projection(decode_column: DecodeColumn) -> Arc<dyn ReadProjection> {
    Arc::new(StructuralReadProjection { decode_column })
}

#[async_trait]
impl ReadProjection for StructuralReadProjection {
    fn validate_indexed(
        &self,
        projection: &ReaderProjection,
        metadata_index: &FileMetadataIndex,
    ) -> Result<()> {
        FileMetadataProvider::validate_indexed_projection_structure(projection, metadata_index)?;
        if FileMetadataProvider::projection_matches_indexed_metadata(projection) {
            Ok(())
        } else {
            Err(FileMetadataProvider::indexed_projection_error(
                projection,
                metadata_index,
            ))
        }
    }

    fn read_length(&self, prepared: &PreparedProjection) -> Result<u64> {
        prepared_read_length(prepared)
    }

    async fn prepare(
        &self,
        metadata_provider: &FileMetadataProvider,
        projection: &ReaderProjection,
        io: &Arc<dyn EncodingsIo>,
        cache: &Arc<LanceCache>,
    ) -> Result<(PreparedProjection, u64)> {
        let prepared = match metadata_provider {
            FileMetadataProvider::Full(metadata) => {
                FileReader::validate_projection(projection, metadata)?;
                PreparedProjection {
                    column_infos: metadata.column_infos.clone(),
                    decoder_projection: projection.clone(),
                }
            }
            FileMetadataProvider::Indexed(metadata_index) => {
                self.validate_indexed(projection, metadata_index)?;
                let column_infos = FileMetadataProvider::load_indexed_column_infos(
                    metadata_index,
                    io,
                    cache,
                    &projection.column_indices,
                    self.decode_column,
                )
                .await?;
                PreparedProjection {
                    column_infos,
                    decoder_projection: ReaderProjection {
                        schema: projection.schema.clone(),
                        column_indices: (0..projection.column_indices.len())
                            .map(|index| index as u32)
                            .collect(),
                    },
                }
            }
        };
        let read_len = self.read_length(&prepared)?;
        Ok((prepared, read_len))
    }
}

#[cfg(test)]
pub fn test_projection_length(
    schema: &Schema,
    column_indices: &[u32],
    column_lengths: &[u64],
) -> Result<u64> {
    projection_length(schema, column_indices, &|column| {
        column_lengths.get(column).copied().ok_or_else(|| {
            Error::invalid_input(format!("missing synthetic length for column {column}"))
        })
    })
}
