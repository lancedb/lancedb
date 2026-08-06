// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch};
use bytes::{BufMut, Bytes};
use lance_core::{Result, datatypes::Schema};
use lance_encoding::{
    compression_config::CompressionParams,
    encoder::{BatchEncoder, EncodedBatch},
};
use lance_io::{object_store::ObjectStore, traits::Writer as ObjectWriter};
use object_store::path::Path;
use tokio::io::AsyncWriteExt;

use crate::{
    format::{MAGIC, pbfile},
    writer::{
        FileWriteSummary, FileWriterOptions,
        structural::{EncodedBatchBody, EncodingPipeline, StructuralFileSink, encode_batch_body},
    },
};

use super::encoding_strategy;

/// A writer for the Lance v2.2 file grammar.
///
/// The concrete writer owns the v2.2 encoding composition, finish ordering,
/// and exact footer identity. Shared components only execute the structural
/// encoding and I/O mechanisms selected here.
pub struct Writer {
    sink: StructuralFileSink,
    encoding: EncodingPipeline,
    compression: CompressionParams,
}

impl Writer {
    /// Create a v2.2 writer with an explicit schema.
    pub fn try_new(
        object_writer: Box<dyn ObjectWriter>,
        schema: Schema,
        options: FileWriterOptions,
    ) -> Result<Self> {
        Self::try_new_with_compression(object_writer, schema, options, Default::default())
    }

    /// Create a v2.2 writer with explicit compression tuning.
    pub fn try_new_with_compression(
        object_writer: Box<dyn ObjectWriter>,
        schema: Schema,
        options: FileWriterOptions,
        compression: CompressionParams,
    ) -> Result<Self> {
        let mut writer = Self::new_lazy_with_compression(object_writer, options, compression);
        writer.initialize(schema)?;
        Ok(writer)
    }

    /// Create a v2.2 writer whose schema is inferred from the first batch.
    pub fn new_lazy(object_writer: Box<dyn ObjectWriter>, options: FileWriterOptions) -> Self {
        Self::new_lazy_with_compression(object_writer, options, Default::default())
    }

    /// Create a lazy v2.2 writer with explicit compression tuning.
    pub fn new_lazy_with_compression(
        object_writer: Box<dyn ObjectWriter>,
        options: FileWriterOptions,
        compression: CompressionParams,
    ) -> Self {
        Self {
            sink: StructuralFileSink::new(object_writer),
            encoding: EncodingPipeline::new(options),
            compression,
        }
    }

    fn initialize(&mut self, schema: Schema) -> Result<()> {
        let encoding_options = self.encoding.encoding_options(&schema);
        schema.validate()?;
        let strategy = encoding_strategy(self.compression.clone());
        let encoder = BatchEncoder::try_new(&schema, strategy.as_ref(), &encoding_options)?;
        self.encoding.initialize(schema, encoder, &mut self.sink);
        Ok(())
    }

    fn ensure_initialized(&mut self, batch: &RecordBatch) -> Result<()> {
        if !self.encoding.is_initialized() {
            self.initialize(Schema::try_from(batch.schema().as_ref())?)?;
        }
        Ok(())
    }

    /// Spill page metadata to a sidecar file instead of retaining it in memory.
    pub fn with_page_metadata_spill(mut self, object_store: Arc<ObjectStore>, path: Path) -> Self {
        self.sink.with_page_metadata_spill(object_store, path);
        self
    }

    /// Schedule batches to be written in iteration order.
    pub async fn write_batches(
        &mut self,
        batches: impl Iterator<Item = &RecordBatch>,
    ) -> Result<()> {
        for batch in batches {
            self.write_batch(batch).await?;
        }
        Ok(())
    }

    /// Schedule one record batch for writing.
    pub async fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        self.ensure_initialized(batch)?;
        self.encoding.write_batch(batch, &mut self.sink).await
    }

    /// Write one top-level field, advancing only that field's row count.
    pub async fn write_column(&mut self, column_index: usize, array: ArrayRef) -> Result<()> {
        self.encoding
            .write_column(column_index, array, &mut self.sink)
            .await
    }

    /// Append a buffer whose page or column metadata is supplied externally.
    pub async fn write_external_buffer(&mut self, bytes: &[u8]) -> Result<(u64, u64)> {
        self.sink.write_external_buffer(bytes).await
    }

    /// Add an entry to the schema metadata written in the file descriptor.
    pub fn add_schema_metadata(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.encoding.add_schema_metadata(key, value);
    }

    /// Prepare the writer for encoded column data produced externally.
    pub fn initialize_with_external_metadata(
        &mut self,
        schema: Schema,
        column_metadata: Vec<pbfile::ColumnMetadata>,
        rows_written: u64,
    ) {
        self.encoding
            .initialize_with_external_metadata(schema, rows_written);
        self.sink.initialize_with_external_metadata(column_metadata);
    }

    /// Add an arbitrary global buffer and return its one-based index.
    pub async fn add_global_buffer(&mut self, buffer: Bytes) -> Result<u32> {
        self.sink.add_global_buffer(buffer).await
    }

    /// Finish the v2.2 file and close its object writer.
    pub async fn finish(&mut self) -> Result<FileWriteSummary> {
        // The order below is the v2.2 wire contract.
        self.encoding.flush(&mut self.sink).await?;
        self.encoding.finish_encoders(&mut self.sink).await?;

        let descriptor = self.encoding.make_file_descriptor()?;
        let global_buffer_offsets = self.sink.write_global_buffers(descriptor).await?;
        let num_global_buffers = global_buffer_offsets.len() as u32;

        let column_metadata_start = self.sink.tell().await?;
        let column_metadata_offsets = self.sink.write_column_metadatas().await?;
        let column_metadata_offsets_start = self
            .sink
            .write_offset_table(&column_metadata_offsets)
            .await?;
        let global_buffer_offsets_start =
            self.sink.write_offset_table(&global_buffer_offsets).await?;
        let num_columns = self.sink.num_columns();

        let output = self.sink.output_mut();
        output.write_u64_le(column_metadata_start).await?;
        output.write_u64_le(column_metadata_offsets_start).await?;
        output.write_u64_le(global_buffer_offsets_start).await?;
        output.write_u32_le(num_global_buffers).await?;
        output.write_u32_le(num_columns).await?;
        output.write_u16_le(2).await?;
        output.write_u16_le(2).await?;
        output.write_all(MAGIC).await?;

        Ok(FileWriteSummary {
            num_rows: self.encoding.rows_written(),
            size_bytes: self.sink.shutdown().await?,
        })
    }

    /// Abandon this write.
    pub async fn abort(&mut self) {
        // Dropping a multipart ObjectWriter aborts the upload.
    }

    /// Return the current object-writer position.
    pub async fn tell(&mut self) -> Result<u64> {
        self.sink.tell().await
    }

    /// Return the field-id to physical-column mapping selected by v2.2.
    pub fn field_id_to_column_indices(&self) -> &[(u32, u32)] {
        self.encoding.field_id_to_column_indices()
    }
}

/// Append a self-described or mini-lance v2.2 footer to an encoded batch.
pub fn concat_lance_footer(batch: &EncodedBatch, write_schema: bool) -> Result<Bytes> {
    let EncodedBatchBody {
        mut data,
        column_metadata_start,
        column_metadata_offsets_start,
        global_buffer_offsets_start,
        num_global_buffers,
        num_columns,
    } = encode_batch_body(batch, write_schema)?;

    data.put_u64_le(column_metadata_start);
    data.put_u64_le(column_metadata_offsets_start);
    data.put_u64_le(global_buffer_offsets_start);
    data.put_u32_le(num_global_buffers);
    data.put_u32_le(num_columns);
    data.put_u16_le(2);
    data.put_u16_le(2);
    data.extend_from_slice(MAGIC);
    Ok(data.freeze())
}
