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
    format::pb,
};
use prost::{Message, Name};

use crate::{
    format::pbfile,
    reader::{
        BufferDescriptor, CachedFileMetadata, FileMetadataIndex, FileMetadataProvider, FileReader,
        PreparedProjection, RawFileMetadata, ReadProjection, ReaderProjection,
        normalized_column_num_rows, verify_uniform_lengths,
    },
    version::ConcreteFileVersion,
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

pub fn decode_column(
    column_index: u32,
    metadata: &pbfile::ColumnMetadata,
) -> Result<Arc<ColumnInfo>> {
    let page_infos = metadata
        .pages
        .iter()
        .enumerate()
        .map(|(page_index, page)| {
            let array_encoding =
                fetch_encoding::<pb::ArrayEncoding>(page.encoding.as_ref().ok_or_else(|| {
                    Error::invalid_input_source(
                        format!(
                            "Column {} page {} is missing its encoding",
                            column_index, page_index
                        )
                        .into(),
                    )
                })?)?;
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
                    .map(|(offset, size)| (*offset, *size))
                    .collect::<Vec<_>>(),
            );
            Ok(PageInfo {
                buffer_offsets_and_sizes,
                encoding: PageEncoding::Legacy(array_encoding),
                num_rows: page.length,
                priority: page.priority,
            })
        })
        .collect::<Result<Vec<_>>>()?;

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
    Ok(Arc::new(ColumnInfo {
        index: column_index,
        page_infos: Arc::from(page_infos),
        buffer_offsets_and_sizes,
        encoding: fetch_encoding(metadata.encoding.as_ref().ok_or_else(|| {
            Error::invalid_input_source(
                format!("Column {} is missing its encoding", column_index).into(),
            )
        })?)?,
    }))
}

pub fn decode_column_metadata(
    column_metadatas: &[pbfile::ColumnMetadata],
) -> Result<Vec<Arc<ColumnInfo>>> {
    column_metadatas
        .iter()
        .enumerate()
        .map(|(column_index, metadata)| {
            let column_index = u32::try_from(column_index).map_err(|_| {
                Error::invalid_input_source("File has more than u32::MAX columns".into())
            })?;
            decode_column(column_index, metadata)
        })
        .collect()
}

pub async fn prepare_projection(
    metadata_provider: &FileMetadataProvider,
    projection: &ReaderProjection,
    _io: &Arc<dyn EncodingsIo>,
    _cache: &Arc<LanceCache>,
) -> Result<PreparedProjection> {
    match metadata_provider {
        FileMetadataProvider::Full(metadata) => {
            FileReader::validate_projection(projection, metadata)?;
            Ok(PreparedProjection {
                column_infos: metadata.column_infos.clone(),
                decoder_projection: projection.clone(),
            })
        }
        FileMetadataProvider::Indexed(metadata_index) => {
            FileMetadataProvider::validate_indexed_projection_structure(
                projection,
                metadata_index,
            )?;
            Err(FileMetadataProvider::indexed_projection_error(
                projection,
                metadata_index,
            ))
        }
    }
}

fn field_column_shape(field: &Field) -> (bool, bool) {
    if field.is_blob() || field.is_packed_struct() {
        return (true, false);
    }
    (true, !field.children.is_empty())
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
    projection_from_field_ids(schema, &super::field_id_to_column_index(schema))
}

pub fn projection_from_column_names(
    schema: &Schema,
    column_names: &[&str],
) -> Result<ReaderProjection> {
    let field_id_to_column_index = super::field_id_to_column_index(schema);
    let projected = schema.project(column_names)?;
    Ok(projection_from_field_ids(
        &projected,
        &field_id_to_column_index,
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
    let mut cursor = 0;
    let mut field_lengths = Vec::with_capacity(prepared.decoder_projection.schema.fields.len());
    for field in &prepared.decoder_projection.schema.fields {
        let rows = validate_field_length(
            field,
            true,
            &prepared.decoder_projection.column_indices,
            &mut cursor,
            &column_len,
        )?;
        field_lengths.push((field.name.as_str(), rows));
    }
    if cursor != prepared.decoder_projection.column_indices.len() {
        return Err(Error::invalid_input(format!(
            "projection supplied {} column indices but its fields require {}",
            prepared.decoder_projection.column_indices.len(),
            cursor
        )));
    }
    verify_uniform_lengths(&field_lengths)
}

#[derive(Debug)]
struct V20ReadProjection;

pub(super) fn read_projection() -> Arc<dyn ReadProjection> {
    Arc::new(V20ReadProjection)
}

#[async_trait]
impl ReadProjection for V20ReadProjection {
    fn validate_indexed(
        &self,
        projection: &ReaderProjection,
        metadata_index: &FileMetadataIndex,
    ) -> Result<()> {
        FileMetadataProvider::validate_indexed_projection_structure(projection, metadata_index)?;
        Err(FileMetadataProvider::indexed_projection_error(
            projection,
            metadata_index,
        ))
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
        let prepared = prepare_projection(metadata_provider, projection, io, cache).await?;
        let read_len = self.read_length(&prepared)?;
        Ok((prepared, read_len))
    }
}

pub fn finish_metadata(raw: RawFileMetadata) -> Result<CachedFileMetadata> {
    if !matches!(
        (raw.footer.major_version, raw.footer.minor_version),
        (0, 3) | (2, 0)
    ) {
        return Err(Error::version_conflict(
            "Attempt to use the Lance v2.0 reader for a different file version".to_string(),
            raw.footer.major_version,
            raw.footer.minor_version,
        ));
    }
    let column_infos = decode_column_metadata(&raw.column_metadatas)?;
    Ok(CachedFileMetadata {
        file_schema: raw.file_schema,
        column_metadatas: raw.column_metadatas,
        column_infos,
        num_rows: raw.num_rows,
        file_buffers: raw.file_buffers,
        num_data_bytes: raw.num_data_bytes,
        num_column_metadata_bytes: raw.num_column_metadata_bytes,
        num_global_buffer_bytes: raw.num_global_buffer_bytes,
        num_footer_bytes: raw.num_footer_bytes,
        major_version: raw.footer.major_version,
        minor_version: raw.footer.minor_version,
        version: ConcreteFileVersion::V2_0,
        file_size_bytes: raw.file_size_bytes,
        retained_global_buffers: raw.retained_global_buffers,
    })
}

pub fn validate_global_buffers(_buffers: &[BufferDescriptor]) -> Result<()> {
    Ok(())
}

pub fn finish_metadata_index(index: FileMetadataIndex) -> Result<FileMetadataIndex> {
    if index.version == ConcreteFileVersion::V2_0 {
        Ok(index)
    } else {
        let (major, minor) = index.version.to_standard_footer_numbers();
        Err(Error::version_conflict(
            "Attempt to use the Lance v2.0 reader for a different metadata index".to_string(),
            major,
            minor,
        ))
    }
}

#[cfg(test)]
pub fn test_projection_length(
    schema: &Schema,
    column_indices: &[u32],
    column_lengths: &[u64],
) -> Result<u64> {
    let column_len = |column: usize| {
        column_lengths.get(column).copied().ok_or_else(|| {
            Error::invalid_input(format!("missing synthetic length for column {column}"))
        })
    };
    let mut cursor = 0;
    let mut field_lengths = Vec::with_capacity(schema.fields.len());
    for field in &schema.fields {
        let rows = validate_field_length(field, true, column_indices, &mut cursor, &column_len)?;
        field_lengths.push((field.name.as_str(), rows));
    }
    verify_uniform_lengths(&field_lengths)
}
