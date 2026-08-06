// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use core::panic;
use std::{collections::HashMap, sync::Arc};

use arrow_array::{ArrayRef, RecordBatch};
use arrow_data::ArrayData;
use bytes::{Buf, Bytes, BytesMut};
use futures::{StreamExt, stream::FuturesOrdered};
use lance_core::{
    Error, Result,
    datatypes::{Field, Schema},
    utils::bit::pad_bytes,
};
use lance_encoding::{
    decoder::PageEncoding,
    encoder::{
        BatchEncoder, EncodeTask, EncodedBatch, EncodedPage, EncodingOptions, FieldEncoder,
        OutOfLineBuffers,
    },
    repdef::RepDefBuilder,
};
use lance_io::{object_store::ObjectStore, traits::Writer as ObjectWriter};
use log::{debug, warn};
use object_store::path::Path;
use prost::Message;
use prost_types::Any;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tracing::instrument;

use crate::{
    datatypes::FieldsWithMeta,
    format::{pb, pbfile},
    writer::{ENV_LANCE_FILE_WRITER_MAX_PAGE_BYTES, FileWriterOptions, PAGE_BUFFER_ALIGNMENT},
};

const PAD_BUFFER: [u8; PAGE_BUFFER_ALIGNMENT] = [72; PAGE_BUFFER_ALIGNMENT];

// V2.1+ splits large pages on read instead of write. This write-time limit is
// retained as a best-effort memory bound and is not a grammar decision.
const MAX_PAGE_BYTES: usize = 32 * 1024 * 1024;

// Total in-memory budget for serialized page metadata before spill.
const DEFAULT_SPILL_BUFFER_LIMIT: usize = 256 * 1024;

struct PageMetadataSpill {
    writer: Box<dyn ObjectWriter>,
    object_store: Arc<ObjectStore>,
    path: Path,
    position: u64,
    column_buffers: Vec<Vec<u8>>,
    column_chunks: Vec<Vec<(u64, u32)>>,
    per_column_limit: usize,
}

impl PageMetadataSpill {
    async fn new(object_store: Arc<ObjectStore>, path: Path, num_columns: usize) -> Result<Self> {
        let writer = object_store.create(&path).await?;
        let per_column_limit = (DEFAULT_SPILL_BUFFER_LIMIT / num_columns.max(1)).max(64);
        Ok(Self {
            writer,
            object_store,
            path,
            position: 0,
            column_buffers: vec![Vec::new(); num_columns],
            column_chunks: vec![Vec::new(); num_columns],
            per_column_limit,
        })
    }

    async fn append_page(
        &mut self,
        column_index: usize,
        page: &pbfile::column_metadata::Page,
    ) -> Result<()> {
        page.encode_length_delimited(&mut self.column_buffers[column_index])
            .map_err(|error| {
                Error::io_source(Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    error,
                )))
            })?;
        if self.column_buffers[column_index].len() >= self.per_column_limit {
            self.flush_column(column_index).await?;
        }
        Ok(())
    }

    async fn flush_column(&mut self, column_index: usize) -> Result<()> {
        let buffer = &self.column_buffers[column_index];
        if buffer.is_empty() {
            return Ok(());
        }
        let len = buffer.len();
        self.writer.write_all(buffer).await?;
        self.column_chunks[column_index].push((self.position, len as u32));
        self.position += len as u64;
        self.column_buffers[column_index].clear();
        Ok(())
    }

    async fn shutdown_writer(&mut self) -> Result<()> {
        for column_index in 0..self.column_buffers.len() {
            self.flush_column(column_index).await?;
        }
        ObjectWriter::shutdown(self.writer.as_mut()).await?;
        Ok(())
    }
}

fn decode_spilled_chunk(data: &Bytes) -> Result<Vec<pbfile::column_metadata::Page>> {
    let mut pages = Vec::new();
    let mut cursor = data.clone();
    while cursor.has_remaining() {
        let page = pbfile::column_metadata::Page::decode_length_delimited(&mut cursor).map_err(
            |error| {
                Error::io_source(Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    error,
                )))
            },
        )?;
        pages.push(page);
    }
    Ok(pages)
}

enum PageSpillState {
    Pending(Arc<ObjectStore>, Path),
    Active(PageMetadataSpill),
}

fn initial_column_metadata() -> pbfile::ColumnMetadata {
    pbfile::ColumnMetadata {
        pages: Vec::new(),
        buffer_offsets: Vec::new(),
        buffer_sizes: Vec::new(),
        encoding: None,
    }
}

/// Writes the structural page and metadata representation used by v2.1+.
///
/// This component does not choose a file version, field encoding strategy, or
/// footer identity. Exact version writers opt into this representation and own
/// the order in which its operations are invoked.
pub struct StructuralFileSink {
    writer: Box<dyn ObjectWriter>,
    column_metadata: Vec<pbfile::ColumnMetadata>,
    num_columns: u32,
    global_buffers: Vec<(u64, u64)>,
    page_spill: Option<PageSpillState>,
}

impl StructuralFileSink {
    pub fn new(writer: Box<dyn ObjectWriter>) -> Self {
        Self {
            writer,
            column_metadata: Vec::new(),
            num_columns: 0,
            global_buffers: Vec::new(),
            page_spill: None,
        }
    }

    pub fn with_page_metadata_spill(&mut self, object_store: Arc<ObjectStore>, path: Path) {
        self.page_spill = Some(PageSpillState::Pending(object_store, path));
    }

    pub fn initialize_columns(&mut self, num_columns: u32) {
        self.num_columns = num_columns;
        self.column_metadata = vec![initial_column_metadata(); num_columns as usize];
    }

    pub fn initialize_with_external_metadata(
        &mut self,
        column_metadata: Vec<pbfile::ColumnMetadata>,
    ) {
        self.num_columns = column_metadata.len() as u32;
        self.column_metadata = column_metadata;
    }

    async fn write_aligned_buffer_to(
        writer: &mut (impl AsyncWrite + Unpin),
        buffer: &[u8],
    ) -> Result<()> {
        writer.write_all(buffer).await?;
        let padding = pad_bytes::<PAGE_BUFFER_ALIGNMENT>(buffer.len());
        writer.write_all(&PAD_BUFFER[..padding]).await?;
        Ok(())
    }

    pub async fn write_aligned_buffer(&mut self, buffer: &[u8]) -> Result<()> {
        Self::write_aligned_buffer_to(&mut self.writer, buffer).await
    }

    pub async fn write_raw(&mut self, buffer: &[u8]) -> Result<()> {
        self.writer.write_all(buffer).await?;
        Ok(())
    }

    pub async fn write_page(&mut self, encoded_page: EncodedPage) -> Result<()> {
        let buffers = encoded_page.data;
        let mut buffer_offsets = Vec::with_capacity(buffers.len());
        let mut buffer_sizes = Vec::with_capacity(buffers.len());
        for buffer in buffers {
            buffer_offsets.push(self.tell().await?);
            buffer_sizes.push(buffer.len() as u64);
            self.write_aligned_buffer(&buffer).await?;
        }
        let encoded_encoding = match encoded_page.description {
            PageEncoding::Legacy(array_encoding) => Any::from_msg(&array_encoding)?.encode_to_vec(),
            PageEncoding::Structural(page_layout) => Any::from_msg(&page_layout)?.encode_to_vec(),
        };
        let page = pbfile::column_metadata::Page {
            buffer_offsets,
            buffer_sizes,
            encoding: Some(pbfile::Encoding {
                location: Some(pbfile::encoding::Location::Direct(pbfile::DirectEncoding {
                    encoding: encoded_encoding,
                })),
            }),
            length: encoded_page.num_rows,
            priority: encoded_page.row_number,
        };
        let column_index = encoded_page.column_idx as usize;
        if matches!(&self.page_spill, Some(PageSpillState::Pending(..))) {
            let Some(PageSpillState::Pending(store, path)) = self.page_spill.take() else {
                unreachable!()
            };
            self.page_spill = Some(PageSpillState::Active(
                PageMetadataSpill::new(store, path, self.num_columns as usize).await?,
            ));
        }
        match &mut self.page_spill {
            Some(PageSpillState::Active(spill)) => spill.append_page(column_index, &page).await?,
            None => self.column_metadata[column_index].pages.push(page),
            Some(PageSpillState::Pending(..)) => unreachable!(),
        }
        Ok(())
    }

    #[instrument(skip_all, level = "debug")]
    pub async fn write_pages(
        &mut self,
        mut encoding_tasks: FuturesOrdered<EncodeTask>,
    ) -> Result<()> {
        while let Some(encoding_task) = encoding_tasks.next().await {
            self.write_page(encoding_task?).await?;
        }
        self.writer.flush().await?;
        Ok(())
    }

    pub async fn write_column_buffer_at(
        &mut self,
        column_index: usize,
        position: u64,
        buffer: &[u8],
    ) -> Result<()> {
        self.write_aligned_buffer(buffer).await?;
        let metadata = &mut self.column_metadata[column_index];
        metadata.buffer_offsets.push(position);
        metadata.buffer_sizes.push(buffer.len() as u64);
        Ok(())
    }

    pub fn set_column_encoding(&mut self, column_index: usize, encoding: pbfile::Encoding) {
        self.column_metadata[column_index].encoding = Some(encoding);
    }

    pub async fn add_global_buffer(&mut self, buffer: Bytes) -> Result<u32> {
        let position = self.tell().await?;
        let len = buffer.len() as u64;
        self.write_aligned_buffer(&buffer).await?;
        self.global_buffers.push((position, len));
        Ok(self.global_buffers.len() as u32)
    }

    pub async fn write_global_buffers(
        &mut self,
        descriptor: pb::FileDescriptor,
    ) -> Result<Vec<(u64, u64)>> {
        let descriptor_bytes = descriptor.encode_to_vec();
        let descriptor_len = descriptor_bytes.len() as u64;
        let descriptor_position = self.tell().await?;
        self.writer.write_all(&descriptor_bytes).await?;
        let mut offsets = Vec::with_capacity(1 + self.global_buffers.len());
        offsets.push((descriptor_position, descriptor_len));
        offsets.append(&mut self.global_buffers);
        Ok(offsets)
    }

    async fn write_column_metadata(
        &mut self,
        metadata: pbfile::ColumnMetadata,
    ) -> Result<(u64, u64)> {
        let metadata_bytes = metadata.encode_to_vec();
        let position = self.tell().await?;
        let len = metadata_bytes.len() as u64;
        self.writer.write_all(&metadata_bytes).await?;
        Ok((position, len))
    }

    pub async fn write_column_metadatas(&mut self) -> Result<Vec<(u64, u64)>> {
        let metadatas = std::mem::take(&mut self.column_metadata);
        let spill_state = self.page_spill.take();
        let (spill_chunks, spill_reader) =
            if let Some(PageSpillState::Active(mut spill)) = spill_state {
                spill.shutdown_writer().await?;
                let reader = spill.object_store.open(&spill.path).await?;
                let chunks = std::mem::take(&mut spill.column_chunks);
                (chunks, Some(reader))
            } else {
                (Vec::new(), None)
            };

        let mut metadata_positions = Vec::with_capacity(metadatas.len());
        for (column_index, mut metadata) in metadatas.into_iter().enumerate() {
            if let Some(reader) = &spill_reader {
                let mut pages = Vec::new();
                for &(offset, len) in &spill_chunks[column_index] {
                    let data = reader
                        .get_range(offset as usize..(offset as usize + len as usize))
                        .await
                        .map_err(|error| Error::io_source(Box::new(error)))?;
                    pages.extend(decode_spilled_chunk(&data)?);
                }
                metadata.pages = pages;
            }
            metadata_positions.push(self.write_column_metadata(metadata).await?);
        }
        Ok(metadata_positions)
    }

    pub async fn write_offset_table(&mut self, offsets: &[(u64, u64)]) -> Result<u64> {
        let start = self.tell().await?;
        for (position, len) in offsets {
            self.writer.write_u64_le(*position).await?;
            self.writer.write_u64_le(*len).await?;
        }
        Ok(start)
    }

    pub async fn write_external_buffer(&mut self, bytes: &[u8]) -> Result<(u64, u64)> {
        const ZERO_PADDING: [u8; PAGE_BUFFER_ALIGNMENT] = [0; PAGE_BUFFER_ALIGNMENT];
        let position = self.tell().await?;
        let padding = (PAGE_BUFFER_ALIGNMENT - position as usize % PAGE_BUFFER_ALIGNMENT)
            % PAGE_BUFFER_ALIGNMENT;
        self.writer.write_all(&ZERO_PADDING[..padding]).await?;
        let start = position + padding as u64;
        self.writer.write_all(bytes).await?;
        Ok((start, bytes.len() as u64))
    }

    pub fn output_mut(&mut self) -> &mut dyn ObjectWriter {
        self.writer.as_mut()
    }

    pub async fn tell(&mut self) -> Result<u64> {
        Ok(self.writer.tell().await? as u64)
    }

    pub fn num_columns(&self) -> u32 {
        self.num_columns
    }

    pub async fn shutdown(&mut self) -> Result<u64> {
        let result = ObjectWriter::shutdown(self.writer.as_mut()).await?;
        Ok(result.size as u64)
    }
}

/// Runs field encoders and row accounting without selecting a file grammar.
///
/// The exact version writer constructs a [`BatchEncoder`] and supplies it when
/// a schema is initialized. This component only executes that decision.
pub struct EncodingPipeline {
    schema: Option<Schema>,
    field_encoders: Vec<Box<dyn FieldEncoder>>,
    field_id_to_column_indices: Vec<(u32, u32)>,
    rows_written: u64,
    field_rows_written: Vec<u64>,
    schema_metadata: HashMap<String, String>,
    options: FileWriterOptions,
}

impl EncodingPipeline {
    pub fn new(options: FileWriterOptions) -> Self {
        Self {
            schema: None,
            field_encoders: Vec::new(),
            field_id_to_column_indices: Vec::new(),
            rows_written: 0,
            field_rows_written: Vec::new(),
            schema_metadata: HashMap::new(),
            options,
        }
    }

    pub fn encoding_options(&self, schema: &Schema) -> EncodingOptions {
        let cache_bytes_per_column = if let Some(data_cache_bytes) = self.options.data_cache_bytes {
            data_cache_bytes / schema.fields.len() as u64
        } else {
            8 * 1024 * 1024
        };
        let max_page_bytes = self.options.max_page_bytes.unwrap_or_else(|| {
            std::env::var(ENV_LANCE_FILE_WRITER_MAX_PAGE_BYTES)
                .map(|value| {
                    value.parse::<u64>().unwrap_or_else(|error| {
                        warn!(
                            "Failed to parse {}: {}, using default",
                            ENV_LANCE_FILE_WRITER_MAX_PAGE_BYTES, error
                        );
                        MAX_PAGE_BYTES as u64
                    })
                })
                .unwrap_or(MAX_PAGE_BYTES as u64)
        });

        EncodingOptions {
            cache_bytes_per_column,
            max_page_bytes,
            keep_original_array: self.options.keep_original_array.unwrap_or(false),
            buffer_alignment: PAGE_BUFFER_ALIGNMENT as u64,
        }
    }

    pub fn initialize(
        &mut self,
        mut schema: Schema,
        encoder: BatchEncoder,
        sink: &mut StructuralFileSink,
    ) {
        sink.initialize_columns(encoder.num_columns());
        self.field_rows_written = vec![0; encoder.field_encoders.len()];
        self.field_encoders = encoder.field_encoders;
        self.field_id_to_column_indices = encoder.field_id_to_column_index;
        self.schema_metadata
            .extend(std::mem::take(&mut schema.metadata));
        self.schema = Some(schema);
    }

    pub fn is_initialized(&self) -> bool {
        self.schema.is_some()
    }

    fn verify_field_nullability(array: &ArrayData, field: &Field) -> Result<()> {
        if !field.nullable && array.null_count() > 0 {
            return Err(Error::invalid_input(format!(
                "The field `{}` contained null values even though the field is marked non-null in the schema",
                field.name
            )));
        }
        for (child_field, child_array) in field.children.iter().zip(array.child_data()) {
            Self::verify_field_nullability(child_array, child_field)?;
        }
        Ok(())
    }

    fn verify_nullability_constraints(&self, batch: &RecordBatch) -> Result<()> {
        for (column, field) in batch
            .columns()
            .iter()
            .zip(self.schema.as_ref().unwrap().fields.iter())
        {
            Self::verify_field_nullability(&column.to_data(), field)?;
        }
        Ok(())
    }

    fn encode_columns(
        &mut self,
        fields: &[(usize, ArrayRef)],
        external_buffers: &mut OutOfLineBuffers,
    ) -> Result<Vec<Vec<EncodeTask>>> {
        let row_numbers = fields
            .iter()
            .map(|(field_index, _)| self.field_rows_written[*field_index])
            .collect::<Vec<_>>();
        fields
            .iter()
            .zip(row_numbers)
            .map(|((field_index, array), row_number)| {
                self.field_encoders[*field_index].maybe_encode(
                    array.clone(),
                    external_buffers,
                    RepDefBuilder::default(),
                    row_number,
                    array.len() as u64,
                )
            })
            .collect()
    }

    fn encode_batch(
        &mut self,
        batch: &RecordBatch,
        external_buffers: &mut OutOfLineBuffers,
    ) -> Result<Vec<Vec<EncodeTask>>> {
        let field_arrays = self
            .schema
            .as_ref()
            .unwrap()
            .fields
            .iter()
            .enumerate()
            .map(|(field_index, field)| {
                let array = batch.column_by_name(&field.name).ok_or_else(|| {
                    Error::invalid_input_source(
                        format!(
                            "Cannot write batch.  The batch was missing the column `{}`",
                            field.name
                        )
                        .into(),
                    )
                })?;
                Ok((field_index, array.clone()))
            })
            .collect::<Result<Vec<_>>>()?;
        self.encode_columns(&field_arrays, external_buffers)
    }

    #[instrument(skip_all, level = "debug")]
    pub async fn write_batch(
        &mut self,
        batch: &RecordBatch,
        sink: &mut StructuralFileSink,
    ) -> Result<()> {
        debug!(
            "write_batch called with {} rows, {} columns, and {} bytes of data",
            batch.num_rows(),
            batch.num_columns(),
            batch.get_array_memory_size()
        );
        self.verify_nullability_constraints(batch)?;
        let num_rows = batch.num_rows() as u64;
        if num_rows == 0 {
            return Ok(());
        }
        if num_rows > u32::MAX as u64 {
            return Err(Error::invalid_input_source(
                "cannot write Lance files with more than 2^32 rows".into(),
            ));
        }

        let mut external_buffers =
            OutOfLineBuffers::new(sink.tell().await?, PAGE_BUFFER_ALIGNMENT as u64);
        let encoding_tasks = self.encode_batch(batch, &mut external_buffers)?;
        for external_buffer in external_buffers.take_buffers() {
            sink.write_aligned_buffer(&external_buffer).await?;
        }
        let encoding_tasks = encoding_tasks
            .into_iter()
            .flatten()
            .collect::<FuturesOrdered<_>>();

        if self.rows_written.checked_add(num_rows).is_none() {
            return Err(Error::invalid_input_source(format!(
                "cannot write batch with {} rows because {} rows have already been written and Lance files cannot contain more than 2^64 rows",
                num_rows, self.rows_written
            ).into()));
        }
        for field_rows in &mut self.field_rows_written {
            *field_rows += num_rows;
        }
        self.rows_written += num_rows;
        sink.write_pages(encoding_tasks).await
    }

    pub async fn write_column(
        &mut self,
        column_index: usize,
        array: ArrayRef,
        sink: &mut StructuralFileSink,
    ) -> Result<()> {
        let schema = self.schema.as_ref().ok_or_else(|| {
            Error::invalid_input_source(
                "write_column requires the writer to be created with an explicit schema".into(),
            )
        })?;
        let field = schema.fields.get(column_index).ok_or_else(|| {
            Error::invalid_input_source(
                format!(
                    "write_column: field index {} is out of bounds (schema has {} fields)",
                    column_index,
                    schema.fields.len()
                )
                .into(),
            )
        })?;
        if array.len() as u64 > u32::MAX as u64 {
            return Err(Error::invalid_input_source(
                "cannot write Lance files with more than 2^32 rows".into(),
            ));
        }
        Self::verify_field_nullability(&array.to_data(), field)?;
        if array.is_empty() {
            return Ok(());
        }

        let fields = [(column_index, array)];
        let mut external_buffers =
            OutOfLineBuffers::new(sink.tell().await?, PAGE_BUFFER_ALIGNMENT as u64);
        let encoding_tasks = self.encode_columns(&fields, &mut external_buffers)?;
        for external_buffer in external_buffers.take_buffers() {
            sink.write_aligned_buffer(&external_buffer).await?;
        }
        let encoding_tasks = encoding_tasks
            .into_iter()
            .flatten()
            .collect::<FuturesOrdered<_>>();
        for (field_index, array) in &fields {
            let new_total = self.field_rows_written[*field_index] + array.len() as u64;
            self.field_rows_written[*field_index] = new_total;
            self.rows_written = self.rows_written.max(new_total);
        }
        sink.write_pages(encoding_tasks).await
    }

    pub async fn flush(&mut self, sink: &mut StructuralFileSink) -> Result<()> {
        let mut external_buffers =
            OutOfLineBuffers::new(sink.tell().await?, PAGE_BUFFER_ALIGNMENT as u64);
        let encoding_tasks = self
            .field_encoders
            .iter_mut()
            .map(|writer| writer.flush(&mut external_buffers))
            .collect::<Result<Vec<_>>>()?;
        for external_buffer in external_buffers.take_buffers() {
            sink.write_aligned_buffer(&external_buffer).await?;
        }
        sink.write_pages(
            encoding_tasks
                .into_iter()
                .flatten()
                .collect::<FuturesOrdered<_>>(),
        )
        .await
    }

    pub async fn finish_encoders(&mut self, sink: &mut StructuralFileSink) -> Result<()> {
        if self.field_encoders.is_empty() {
            return Ok(());
        }
        let mut column_index = 0;
        for mut writer in std::mem::take(&mut self.field_encoders) {
            let mut external_buffers =
                OutOfLineBuffers::new(sink.tell().await?, PAGE_BUFFER_ALIGNMENT as u64);
            let columns = writer.finish(&mut external_buffers).await?;
            for buffer in external_buffers.take_buffers() {
                sink.write_raw(&buffer).await?;
            }
            debug_assert_eq!(
                columns.len(),
                writer.num_columns() as usize,
                "Expected {} columns from column at index {} and got {}",
                writer.num_columns(),
                column_index,
                columns.len()
            );
            for column in columns {
                for page in column.final_pages {
                    sink.write_page(page).await?;
                }
                let mut buffer_position = sink.tell().await?;
                for buffer in column.column_buffers {
                    sink.write_column_buffer_at(column_index, buffer_position, &buffer)
                        .await?;
                    buffer_position += buffer.len() as u64;
                }
                let encoded_encoding = Any::from_msg(&column.encoding)?.encode_to_vec();
                sink.set_column_encoding(
                    column_index,
                    pbfile::Encoding {
                        location: Some(pbfile::encoding::Location::Direct(
                            pbfile::DirectEncoding {
                                encoding: encoded_encoding,
                            },
                        )),
                    },
                );
                column_index += 1;
            }
        }
        if column_index != sink.num_columns() as usize {
            panic!(
                "Column writers finished with {} columns but we expected {}",
                column_index,
                sink.num_columns()
            );
        }
        Ok(())
    }

    pub fn add_schema_metadata(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.schema_metadata.insert(key.into(), value.into());
    }

    pub fn initialize_with_external_metadata(&mut self, schema: Schema, rows_written: u64) {
        self.schema = Some(schema);
        self.rows_written = rows_written;
    }

    pub fn make_file_descriptor(&mut self) -> Result<pb::FileDescriptor> {
        let schema = self.schema.as_mut().ok_or_else(|| {
            Error::invalid_input(
                "No schema provided on writer open and no data provided.  Schema is unknown and file cannot be created",
            )
        })?;
        schema.metadata = std::mem::take(&mut self.schema_metadata);
        schema
            .fields
            .iter_mut()
            .for_each(|field| field.unload_blobs_recursive());
        make_file_descriptor(schema, self.rows_written)
    }

    pub fn rows_written(&self) -> u64 {
        self.rows_written
    }

    pub fn field_id_to_column_indices(&self) -> &[(u32, u32)] {
        &self.field_id_to_column_indices
    }
}

fn make_file_descriptor(schema: &Schema, num_rows: u64) -> Result<pb::FileDescriptor> {
    let fields_with_meta = FieldsWithMeta::from(schema);
    Ok(pb::FileDescriptor {
        schema: Some(pb::Schema {
            fields: fields_with_meta.fields.0,
            metadata: fields_with_meta.metadata,
        }),
        length: num_rows,
    })
}

/// Structural file body produced before an exact version appends its footer.
pub struct EncodedBatchBody {
    pub data: BytesMut,
    pub column_metadata_start: u64,
    pub column_metadata_offsets_start: u64,
    pub global_buffer_offsets_start: u64,
    pub num_global_buffers: u32,
    pub num_columns: u32,
}

pub fn encode_batch_body(batch: &EncodedBatch, write_schema: bool) -> Result<EncodedBatchBody> {
    use bytes::BufMut;

    let mut data = BytesMut::with_capacity(batch.data.len() + 1024 * 1024);
    data.extend_from_slice(&batch.data);
    let global_buffers = if write_schema {
        let schema_start = data.len() as u64;
        let schema = Schema::try_from(batch.schema.as_ref())?;
        let descriptor = make_file_descriptor(&schema, batch.num_rows)?;
        let descriptor_bytes = descriptor.encode_to_vec();
        let descriptor_len = descriptor_bytes.len() as u64;
        data.extend_from_slice(&descriptor_bytes);
        vec![(schema_start, descriptor_len)]
    } else {
        Vec::new()
    };
    let column_metadata_start = data.len() as u64;

    let mut column_metadata_positions = Vec::with_capacity(batch.page_table.len());
    for column in &batch.page_table {
        let position = data.len() as u64;
        let pages = column
            .page_infos
            .iter()
            .map(|page_info| {
                let encoded_encoding = match &page_info.encoding {
                    PageEncoding::Legacy(array_encoding) => {
                        Any::from_msg(array_encoding)?.encode_to_vec()
                    }
                    PageEncoding::Structural(page_layout) => {
                        Any::from_msg(page_layout)?.encode_to_vec()
                    }
                };
                let (buffer_offsets, buffer_sizes): (Vec<_>, Vec<_>) =
                    page_info.buffer_offsets_and_sizes.iter().copied().unzip();
                Ok(pbfile::column_metadata::Page {
                    buffer_offsets,
                    buffer_sizes,
                    encoding: Some(pbfile::Encoding {
                        location: Some(pbfile::encoding::Location::Direct(
                            pbfile::DirectEncoding {
                                encoding: encoded_encoding,
                            },
                        )),
                    }),
                    length: page_info.num_rows,
                    priority: page_info.priority,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let (buffer_offsets, buffer_sizes): (Vec<_>, Vec<_>) =
            column.buffer_offsets_and_sizes.iter().copied().unzip();
        let encoded_column_encoding = Any::from_msg(&column.encoding)?.encode_to_vec();
        let metadata = pbfile::ColumnMetadata {
            pages,
            buffer_offsets,
            buffer_sizes,
            encoding: Some(pbfile::Encoding {
                location: Some(pbfile::encoding::Location::Direct(pbfile::DirectEncoding {
                    encoding: encoded_column_encoding,
                })),
            }),
        };
        let metadata_bytes = metadata.encode_to_vec();
        column_metadata_positions.push((position, metadata_bytes.len() as u64));
        data.extend_from_slice(&metadata_bytes);
    }

    let column_metadata_offsets_start = data.len() as u64;
    for (position, len) in column_metadata_positions {
        data.put_u64_le(position);
        data.put_u64_le(len);
    }
    let global_buffer_offsets_start = data.len() as u64;
    let num_global_buffers = global_buffers.len() as u32;
    for (position, len) in global_buffers {
        data.put_u64_le(position);
        data.put_u64_le(len);
    }

    Ok(EncodedBatchBody {
        data,
        column_metadata_start,
        column_metadata_offsets_start,
        global_buffer_offsets_start,
        num_global_buffers,
        num_columns: batch.page_table.len() as u32,
    })
}
