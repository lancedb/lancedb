// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Exact file-format composition roots.
//!
//! Each version module lists the mechanisms used by that file format. Callers
//! resolve release selectors before entering this module. Prefer APIs under a
//! concrete module such as [`v2_1`] when the version is statically known. The
//! root functions perform the single exhaustive dispatch for runtime versions.

use bytes::Bytes;
use lance_core::{Error, Result, datatypes::Schema};
use lance_encoding::{
    decoder::{ColumnInfo, DecoderPlugins, PageInfo},
    encoder::EncodedBatch,
};
use lance_io::{scheduler::FileScheduler, traits::Writer};
use std::{collections::BTreeMap, future::Future, sync::Arc};

use crate::{
    format::pbfile,
    reader::{
        BufferDescriptor, CachedFileMetadata, FileMetadataIndex, FileMetadataProvider, FileReader,
        FileReaderOptions, ProjectedFileReader, RawFileMetadata, ReadProjection, ReaderProjection,
    },
    version::ConcreteFileVersion,
    writer::{FileWriter, FileWriterOptions},
};
use lance_core::cache::LanceCache;

pub mod v1;
pub mod v2_0;
pub mod v2_1;
pub mod v2_2;
pub mod v2_3;

pub(crate) fn read_projection(version: ConcreteFileVersion) -> Result<Arc<dyn ReadProjection>> {
    match version {
        ConcreteFileVersion::V1 => Err(Error::internal(
            "current reader composition received Lance v1".to_string(),
        )),
        ConcreteFileVersion::V2_0 => Ok(v2_0::read_projection()),
        ConcreteFileVersion::V2_1 => Ok(v2_1::read_projection()),
        ConcreteFileVersion::V2_2 => Ok(v2_2::read_projection()),
        ConcreteFileVersion::V2_3 => Ok(v2_3::read_projection()),
    }
}

/// A self-described file reader selected by the exact footer version.
pub enum OpenedFileReader {
    /// A v1 file. The persisted footer numbers are retained for diagnostics.
    V1 {
        /// The major version stored in the footer.
        major_version: u16,
        /// The minor version stored in the footer.
        minor_version: u16,
    },
    /// A current-format reader selected from the exact footer identity.
    Current(FileReader),
}

pub(crate) fn finish_metadata(
    version: ConcreteFileVersion,
    metadata: RawFileMetadata,
) -> Result<CachedFileMetadata> {
    match version {
        ConcreteFileVersion::V1 => Err(Error::internal(
            "current metadata dispatch received a Lance v1 file".to_string(),
        )),
        ConcreteFileVersion::V2_0 => v2_0::finish_metadata(metadata),
        ConcreteFileVersion::V2_1 => v2_1::finish_metadata(metadata),
        ConcreteFileVersion::V2_2 => v2_2::finish_metadata(metadata),
        ConcreteFileVersion::V2_3 => v2_3::finish_metadata(metadata),
    }
}

pub(crate) fn finish_metadata_index(index: FileMetadataIndex) -> Result<FileMetadataIndex> {
    match index.version {
        ConcreteFileVersion::V1 => Err(Error::version_conflict(
            "Attempt to use the Lance current-format reader with a v1 metadata index".to_string(),
            0,
            2,
        )),
        ConcreteFileVersion::V2_0 => v2_0::finish_metadata_index(index),
        ConcreteFileVersion::V2_1 => v2_1::finish_metadata_index(index),
        ConcreteFileVersion::V2_2 => v2_2::finish_metadata_index(index),
        ConcreteFileVersion::V2_3 => v2_3::finish_metadata_index(index),
    }
}

pub(crate) fn decode_column_metadata(
    version: ConcreteFileVersion,
    column_metadatas: &[pbfile::ColumnMetadata],
) -> Result<Vec<Arc<ColumnInfo>>> {
    match version {
        ConcreteFileVersion::V1 => Err(Error::not_supported(
            "self-described batches are not part of the Lance v1 grammar".to_string(),
        )),
        ConcreteFileVersion::V2_0 => v2_0::decode_column_metadata(column_metadatas),
        ConcreteFileVersion::V2_1 => v2_1::decode_column_metadata(column_metadatas),
        ConcreteFileVersion::V2_2 => v2_2::decode_column_metadata(column_metadatas),
        ConcreteFileVersion::V2_3 => v2_3::decode_column_metadata(column_metadatas),
    }
}

pub(crate) fn validate_global_buffers(
    version: ConcreteFileVersion,
    buffers: &[BufferDescriptor],
) -> Result<()> {
    match version {
        ConcreteFileVersion::V1 => Ok(()),
        ConcreteFileVersion::V2_0 => v2_0::validate_global_buffers(buffers),
        ConcreteFileVersion::V2_1 => v2_1::validate_global_buffers(buffers),
        ConcreteFileVersion::V2_2 => v2_2::validate_global_buffers(buffers),
        ConcreteFileVersion::V2_3 => v2_3::validate_global_buffers(buffers),
    }
}

pub fn reader_projection_from_field_ids(
    version: ConcreteFileVersion,
    schema: &Schema,
    field_id_to_column_index: &BTreeMap<u32, u32>,
) -> Result<ReaderProjection> {
    Ok(match version {
        ConcreteFileVersion::V1 => v1::projection_from_field_ids(schema, field_id_to_column_index),
        ConcreteFileVersion::V2_0 => {
            v2_0::projection_from_field_ids(schema, field_id_to_column_index)
        }
        ConcreteFileVersion::V2_1 => {
            v2_1::projection_from_field_ids(schema, field_id_to_column_index)
        }
        ConcreteFileVersion::V2_2 => {
            v2_2::projection_from_field_ids(schema, field_id_to_column_index)
        }
        ConcreteFileVersion::V2_3 => {
            v2_3::projection_from_field_ids(schema, field_id_to_column_index)
        }
    })
}

pub fn reader_projection_from_whole_schema(
    schema: &Schema,
    version: ConcreteFileVersion,
) -> ReaderProjection {
    match version {
        ConcreteFileVersion::V1 => v1::projection_from_whole_schema(schema),
        ConcreteFileVersion::V2_0 => v2_0::projection_from_whole_schema(schema),
        ConcreteFileVersion::V2_1 => v2_1::projection_from_whole_schema(schema),
        ConcreteFileVersion::V2_2 => v2_2::projection_from_whole_schema(schema),
        ConcreteFileVersion::V2_3 => v2_3::projection_from_whole_schema(schema),
    }
}

pub fn reader_projection_from_column_names(
    version: ConcreteFileVersion,
    schema: &Schema,
    column_names: &[&str],
) -> Result<ReaderProjection> {
    match version {
        ConcreteFileVersion::V1 => v1::projection_from_column_names(schema, column_names),
        ConcreteFileVersion::V2_0 => v2_0::projection_from_column_names(schema, column_names),
        ConcreteFileVersion::V2_1 => v2_1::projection_from_column_names(schema, column_names),
        ConcreteFileVersion::V2_2 => v2_2::projection_from_column_names(schema, column_names),
        ConcreteFileVersion::V2_3 => v2_3::projection_from_column_names(schema, column_names),
    }
}

/// Count the physical columns represented by one field in an exact grammar.
pub fn physical_column_count(
    version: ConcreteFileVersion,
    field: &lance_core::datatypes::Field,
) -> usize {
    match version {
        ConcreteFileVersion::V1 => v1::physical_column_count(field),
        ConcreteFileVersion::V2_0 => v2_0::physical_column_count(field),
        ConcreteFileVersion::V2_1 => v2_1::physical_column_count(field),
        ConcreteFileVersion::V2_2 => v2_2::physical_column_count(field),
        ConcreteFileVersion::V2_3 => v2_3::physical_column_count(field),
    }
}

/// Build persisted field-to-column entries for an exact grammar.
pub fn data_file_columns(version: ConcreteFileVersion, schema: &Schema) -> (Vec<i32>, Vec<i32>) {
    match version {
        ConcreteFileVersion::V1 => v1::data_file_columns(schema),
        ConcreteFileVersion::V2_0 => v2_0::data_file_columns(schema),
        ConcreteFileVersion::V2_1 => v2_1::data_file_columns(schema),
        ConcreteFileVersion::V2_2 => v2_2::data_file_columns(schema),
        ConcreteFileVersion::V2_3 => v2_3::data_file_columns(schema),
    }
}

/// Copy one column's external metadata and buffers according to the exact file
/// grammar.
///
/// The caller supplies the version-free I/O operation. V2.0 may suppress that
/// operation when a structural header page has already been copied.
pub async fn copy_external_metadata_column<Copy, CopyFuture>(
    version: ConcreteFileVersion,
    schema: &Schema,
    column_index: usize,
    has_existing_pages: bool,
    copy: Copy,
) -> Result<()>
where
    Copy: FnOnce() -> CopyFuture + Send,
    CopyFuture: Future<Output = Result<()>> + Send,
{
    match version {
        ConcreteFileVersion::V1 => Err(Error::not_supported(
            "binary-copy metadata operations are not supported for Lance v1".to_string(),
        )),
        ConcreteFileVersion::V2_0 => {
            if v2_0::should_copy_external_metadata_column(schema, column_index, has_existing_pages)
            {
                copy().await
            } else {
                Ok(())
            }
        }
        ConcreteFileVersion::V2_1 | ConcreteFileVersion::V2_2 | ConcreteFileVersion::V2_3 => {
            copy().await
        }
    }
}

/// Normalize one copied column before an exact-version footer is written.
pub fn finalize_external_metadata_column(
    version: ConcreteFileVersion,
    schema: &Schema,
    column_index: usize,
    pages: &mut Vec<PageInfo>,
    num_rows: u64,
) -> Result<()> {
    match version {
        ConcreteFileVersion::V1 => Err(Error::not_supported(
            "binary-copy metadata operations are not supported for Lance v1".to_string(),
        )),
        ConcreteFileVersion::V2_0 => {
            v2_0::finalize_external_metadata_column(schema, column_index, pages, num_rows);
            Ok(())
        }
        ConcreteFileVersion::V2_1 | ConcreteFileVersion::V2_2 | ConcreteFileVersion::V2_3 => Ok(()),
    }
}

/// Open a projected reader while keeping exact metadata-form selection in the
/// file layer.
///
/// `open_indexed` returns `None` when the loaded index is not selective enough
/// to justify a projected reader. V2.0 never invokes it because indexed
/// metadata is not part of that reader's accepted grammar.
pub async fn open_projected_reader<OpenIndexed, IndexedFuture, OpenFull, FullFuture>(
    version: ConcreteFileVersion,
    projection: &ReaderProjection,
    prefer_indexed: bool,
    open_indexed: OpenIndexed,
    open_full: OpenFull,
) -> Result<ProjectedFileReader>
where
    OpenIndexed: FnOnce() -> IndexedFuture + Send,
    IndexedFuture: Future<Output = Result<Option<ProjectedFileReader>>> + Send,
    OpenFull: FnOnce() -> FullFuture + Send,
    FullFuture: Future<Output = Result<ProjectedFileReader>> + Send,
{
    match version {
        ConcreteFileVersion::V1 => Err(Error::not_supported(
            "projected current-format readers cannot open Lance v1 files".to_string(),
        )),
        ConcreteFileVersion::V2_0 => open_full().await,
        ConcreteFileVersion::V2_1 | ConcreteFileVersion::V2_2 | ConcreteFileVersion::V2_3 => {
            if prefer_indexed
                && FileMetadataProvider::projection_matches_indexed_metadata(projection)
                && let Some(reader) = open_indexed().await?
            {
                return Ok(reader);
            }
            open_full().await
        }
    }
}

/// Open a self-described file and dispatch to the matching reader.
///
/// The current-format reader's optimistic tail read is also used for exact
/// version detection, so current files do not pay for a separate footer probe.
pub async fn open_self_described_reader(
    scheduler: FileScheduler,
    decoder_plugins: Arc<DecoderPlugins>,
    cache: &LanceCache,
    options: FileReaderOptions,
) -> Result<OpenedFileReader> {
    FileReader::try_open_for_dispatch(scheduler, None, decoder_plugins, cache, options).await
}

/// Create a current-format writer for an exact file version.
///
/// V1 uses [`v1::writer::FileWriter`] directly because its manifest provider is
/// part of the writer type.
pub fn create_writer(
    version: ConcreteFileVersion,
    object_writer: Box<dyn Writer>,
    schema: Schema,
    options: FileWriterOptions,
) -> Result<FileWriter> {
    match version {
        ConcreteFileVersion::V1 => Err(Error::not_supported(
            "Lance v1 files must be created with versions::v1::writer::FileWriter".to_string(),
        )),
        ConcreteFileVersion::V2_0 => {
            v2_0::create_writer(object_writer, schema, options).map(Into::into)
        }
        ConcreteFileVersion::V2_1 => {
            v2_1::create_writer(object_writer, schema, options).map(Into::into)
        }
        ConcreteFileVersion::V2_2 => {
            v2_2::create_writer(object_writer, schema, options).map(Into::into)
        }
        ConcreteFileVersion::V2_3 => {
            v2_3::create_writer(object_writer, schema, options).map(Into::into)
        }
    }
}

/// Create a lazy current-format writer for an exact file version.
pub fn create_lazy_writer(
    version: ConcreteFileVersion,
    object_writer: Box<dyn Writer>,
    options: FileWriterOptions,
) -> Result<FileWriter> {
    match version {
        ConcreteFileVersion::V1 => Err(Error::not_supported(
            "legacy v1 files require an explicit schema and manifest provider".to_string(),
        )),
        ConcreteFileVersion::V2_0 => Ok(v2_0::create_lazy_writer(object_writer, options).into()),
        ConcreteFileVersion::V2_1 => Ok(v2_1::create_lazy_writer(object_writer, options).into()),
        ConcreteFileVersion::V2_2 => Ok(v2_2::create_lazy_writer(object_writer, options).into()),
        ConcreteFileVersion::V2_3 => Ok(v2_3::create_lazy_writer(object_writer, options).into()),
    }
}

/// Encode a self-described batch for an exact file version.
pub fn encode_self_described_batch(
    version: ConcreteFileVersion,
    batch: &EncodedBatch,
) -> Result<Bytes> {
    match version {
        ConcreteFileVersion::V1 => Err(Error::not_supported(
            "Lance v1 does not support self-described current-format batches".to_string(),
        )),
        ConcreteFileVersion::V2_0 => v2_0::encode_self_described_batch(batch),
        ConcreteFileVersion::V2_1 => v2_1::encode_self_described_batch(batch),
        ConcreteFileVersion::V2_2 => v2_2::encode_self_described_batch(batch),
        ConcreteFileVersion::V2_3 => v2_3::encode_self_described_batch(batch),
    }
}

/// Encode a mini-lance batch for an exact file version.
pub fn encode_mini_batch(version: ConcreteFileVersion, batch: &EncodedBatch) -> Result<Bytes> {
    match version {
        ConcreteFileVersion::V1 => Err(Error::not_supported(
            "Lance v1 does not support mini-lance current-format batches".to_string(),
        )),
        ConcreteFileVersion::V2_0 => v2_0::encode_mini_batch(batch),
        ConcreteFileVersion::V2_1 => v2_1::encode_mini_batch(batch),
        ConcreteFileVersion::V2_2 => v2_2::encode_mini_batch(batch),
        ConcreteFileVersion::V2_3 => v2_3::encode_mini_batch(batch),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    #[test]
    fn v1_rejects_current_format_embedded_batches() {
        let batch = EncodedBatch {
            data: Bytes::new(),
            page_table: Vec::new(),
            schema: Arc::new(Schema::default()),
            top_level_columns: Vec::new(),
            num_rows: 0,
        };

        assert!(matches!(
            encode_self_described_batch(ConcreteFileVersion::V1, &batch),
            Err(Error::NotSupported { .. })
        ));
        assert!(matches!(
            encode_mini_batch(ConcreteFileVersion::V1, &batch),
            Err(Error::NotSupported { .. })
        ));
    }
}
