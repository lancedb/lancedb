// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch};
use bytes::Bytes;
use lance_core::{Result, datatypes::Schema};
use lance_encoding::decoder::{ColumnInfo, PageEncoding};
use lance_io::object_store::ObjectStore;
use object_store::path::Path;
use prost::Message;
use prost_types::Any;

use crate::{format::pbfile, versions};

pub(crate) mod structural;

/// Page buffers in current Lance files are aligned to 64 bytes.
pub(crate) const PAGE_BUFFER_ALIGNMENT: usize = 64;
pub(crate) const ENV_LANCE_FILE_WRITER_MAX_PAGE_BYTES: &str = "LANCE_FILE_WRITER_MAX_PAGE_BYTES";

/// Summary of a completed Lance file write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FileWriteSummary {
    /// The number of rows written to the file.
    pub num_rows: u64,
    /// The final size of the file in bytes.
    pub size_bytes: u64,
}

/// Runtime options shared by all current-format writers.
///
/// These options control buffering and execution only. Select the file grammar
/// by constructing a writer under [`crate::versions`].
#[derive(Debug, Clone, Default)]
pub struct FileWriterOptions {
    /// How many bytes to use for buffering column data.
    ///
    /// The budget is divided evenly across columns. The default is 8 MiB per
    /// column.
    pub data_cache_bytes: Option<u64>,
    /// A best-effort maximum encoded page size.
    pub max_page_bytes: Option<u64>,
    /// Keep input arrays instead of copying buffered slices.
    ///
    /// Do not enable this for arrays arriving through the Arrow C data
    /// interface because a small child array can keep an entire batch alive.
    pub keep_original_array: Option<bool>,
}

/// A type-erased current-format file writer.
///
/// This enum exists for callers that select a concrete file version at
/// runtime. Each variant owns the complete implementation for exactly one file
/// grammar; this type only forwards operations without adding format policy.
pub enum FileWriter {
    V2_0(Box<versions::v2_0::Writer>),
    V2_1(Box<versions::v2_1::Writer>),
    V2_2(Box<versions::v2_2::Writer>),
    V2_3(Box<versions::v2_3::Writer>),
}

fn column_info_to_metadata(column: &ColumnInfo) -> Result<pbfile::ColumnMetadata> {
    let pages = column
        .page_infos
        .iter()
        .map(|page| {
            let encoding = match &page.encoding {
                PageEncoding::Legacy(encoding) => Any::from_msg(encoding)?.encode_to_vec(),
                PageEncoding::Structural(encoding) => Any::from_msg(encoding)?.encode_to_vec(),
            };
            let (buffer_offsets, buffer_sizes) =
                page.buffer_offsets_and_sizes.iter().copied().unzip();
            Ok(pbfile::column_metadata::Page {
                buffer_offsets,
                buffer_sizes,
                encoding: Some(pbfile::Encoding {
                    location: Some(pbfile::encoding::Location::Direct(pbfile::DirectEncoding {
                        encoding,
                    })),
                }),
                length: page.num_rows,
                priority: page.priority,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let (buffer_offsets, buffer_sizes) = column.buffer_offsets_and_sizes.iter().copied().unzip();
    let encoding = Any::from_msg(&column.encoding)?.encode_to_vec();
    Ok(pbfile::ColumnMetadata {
        pages,
        buffer_offsets,
        buffer_sizes,
        encoding: Some(pbfile::Encoding {
            location: Some(pbfile::encoding::Location::Direct(pbfile::DirectEncoding {
                encoding,
            })),
        }),
    })
}

impl From<versions::v2_0::Writer> for FileWriter {
    fn from(writer: versions::v2_0::Writer) -> Self {
        Self::V2_0(Box::new(writer))
    }
}

impl From<versions::v2_1::Writer> for FileWriter {
    fn from(writer: versions::v2_1::Writer) -> Self {
        Self::V2_1(Box::new(writer))
    }
}

impl From<versions::v2_2::Writer> for FileWriter {
    fn from(writer: versions::v2_2::Writer) -> Self {
        Self::V2_2(Box::new(writer))
    }
}

impl From<versions::v2_3::Writer> for FileWriter {
    fn from(writer: versions::v2_3::Writer) -> Self {
        Self::V2_3(Box::new(writer))
    }
}

impl FileWriter {
    /// Spill page metadata to a sidecar file.
    pub fn with_page_metadata_spill(self, object_store: Arc<ObjectStore>, path: Path) -> Self {
        match self {
            Self::V2_0(writer) => Self::V2_0(Box::new(
                (*writer).with_page_metadata_spill(object_store, path),
            )),
            Self::V2_1(writer) => Self::V2_1(Box::new(
                (*writer).with_page_metadata_spill(object_store, path),
            )),
            Self::V2_2(writer) => Self::V2_2(Box::new(
                (*writer).with_page_metadata_spill(object_store, path),
            )),
            Self::V2_3(writer) => Self::V2_3(Box::new(
                (*writer).with_page_metadata_spill(object_store, path),
            )),
        }
    }

    /// Schedule batches of data to be written to the file.
    pub async fn write_batches(
        &mut self,
        batches: impl Iterator<Item = &RecordBatch>,
    ) -> Result<()> {
        for batch in batches {
            self.write_batch(batch).await?;
        }
        Ok(())
    }

    /// Schedule a batch of data to be written to the file.
    pub async fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        match self {
            Self::V2_0(writer) => writer.write_batch(batch).await,
            Self::V2_1(writer) => writer.write_batch(batch).await,
            Self::V2_2(writer) => writer.write_batch(batch).await,
            Self::V2_3(writer) => writer.write_batch(batch).await,
        }
    }

    /// Write one top-level column.
    pub async fn write_column(&mut self, column_index: usize, array: ArrayRef) -> Result<()> {
        match self {
            Self::V2_0(writer) => writer.write_column(column_index, array).await,
            Self::V2_1(writer) => writer.write_column(column_index, array).await,
            Self::V2_2(writer) => writer.write_column(column_index, array).await,
            Self::V2_3(writer) => writer.write_column(column_index, array).await,
        }
    }

    /// Append a buffer whose page or column metadata is supplied externally.
    pub async fn write_external_buffer(&mut self, bytes: &[u8]) -> Result<(u64, u64)> {
        match self {
            Self::V2_0(writer) => writer.write_external_buffer(bytes).await,
            Self::V2_1(writer) => writer.write_external_buffer(bytes).await,
            Self::V2_2(writer) => writer.write_external_buffer(bytes).await,
            Self::V2_3(writer) => writer.write_external_buffer(bytes).await,
        }
    }

    /// Add a metadata entry to the schema.
    pub fn add_schema_metadata(&mut self, key: impl Into<String>, value: impl Into<String>) {
        let key = key.into();
        let value = value.into();
        match self {
            Self::V2_0(writer) => writer.add_schema_metadata(key, value),
            Self::V2_1(writer) => writer.add_schema_metadata(key, value),
            Self::V2_2(writer) => writer.add_schema_metadata(key, value),
            Self::V2_3(writer) => writer.add_schema_metadata(key, value),
        }
    }

    /// Prepare a writer from encoded columns whose buffers were produced externally.
    pub fn initialize_with_external_columns(
        &mut self,
        schema: Schema,
        columns: &[Arc<ColumnInfo>],
        rows_written: u64,
    ) -> Result<()> {
        let column_metadata = columns
            .iter()
            .map(|column| column_info_to_metadata(column))
            .collect::<Result<Vec<_>>>()?;
        match self {
            Self::V2_0(writer) => {
                writer.initialize_with_external_metadata(schema, column_metadata, rows_written)
            }
            Self::V2_1(writer) => {
                writer.initialize_with_external_metadata(schema, column_metadata, rows_written)
            }
            Self::V2_2(writer) => {
                writer.initialize_with_external_metadata(schema, column_metadata, rows_written)
            }
            Self::V2_3(writer) => {
                writer.initialize_with_external_metadata(schema, column_metadata, rows_written)
            }
        }
        Ok(())
    }

    /// Add an arbitrary global buffer and return its one-based index.
    pub async fn add_global_buffer(&mut self, buffer: Bytes) -> Result<u32> {
        match self {
            Self::V2_0(writer) => writer.add_global_buffer(buffer).await,
            Self::V2_1(writer) => writer.add_global_buffer(buffer).await,
            Self::V2_2(writer) => writer.add_global_buffer(buffer).await,
            Self::V2_3(writer) => writer.add_global_buffer(buffer).await,
        }
    }

    /// Finish the file and close its object writer.
    pub async fn finish(&mut self) -> Result<FileWriteSummary> {
        match self {
            Self::V2_0(writer) => writer.finish().await,
            Self::V2_1(writer) => writer.finish().await,
            Self::V2_2(writer) => writer.finish().await,
            Self::V2_3(writer) => writer.finish().await,
        }
    }

    /// Abandon the file write.
    pub async fn abort(&mut self) {
        match self {
            Self::V2_0(writer) => writer.abort().await,
            Self::V2_1(writer) => writer.abort().await,
            Self::V2_2(writer) => writer.abort().await,
            Self::V2_3(writer) => writer.abort().await,
        }
    }

    /// Return the current object-writer position.
    pub async fn tell(&mut self) -> Result<u64> {
        match self {
            Self::V2_0(writer) => writer.tell().await,
            Self::V2_1(writer) => writer.tell().await,
            Self::V2_2(writer) => writer.tell().await,
            Self::V2_3(writer) => writer.tell().await,
        }
    }

    /// Return the field-id to physical-column mapping.
    pub fn field_id_to_column_indices(&self) -> &[(u32, u32)] {
        match self {
            Self::V2_0(writer) => writer.field_id_to_column_indices(),
            Self::V2_1(writer) => writer.field_id_to_column_indices(),
            Self::V2_2(writer) => writer.field_id_to_column_indices(),
            Self::V2_3(writer) => writer.field_id_to_column_indices(),
        }
    }
}

#[cfg(test)]
#[path = "writer_tests.rs"]
mod writer_tests;
