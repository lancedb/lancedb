// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use core::panic;
use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch};

use arrow_data::ArrayData;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use futures::StreamExt;
use futures::stream::FuturesOrdered;
use lance_core::datatypes::{Field, Schema as LanceSchema};
use lance_core::utils::bit::pad_bytes;
use lance_core::{Error, Result};
use lance_encoding::decoder::PageEncoding;
use lance_encoding::encoder::{
    ArrayFieldEncodingStrategy, BatchEncoder, EncodeTask, EncodedBatch, EncodedPage,
    EncodingOptions, FieldEncoder, FieldEncodingStrategy, OutOfLineBuffers,
};
use lance_encoding::repdef::RepDefBuilder;
use lance_io::object_store::ObjectStore;
use lance_io::traits::Writer as ObjectWriter;
use log::{debug, warn};
use object_store::path::Path;
use prost::Message;
use prost_types::Any;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;
use tracing::instrument;

use crate::datatypes::FieldsWithMeta;
use crate::format::MAGIC;
use crate::format::pb;
use crate::format::pbfile;
use crate::format::pbfile::DirectEncoding;
use crate::writer::{
    ENV_LANCE_FILE_WRITER_MAX_PAGE_BYTES, FileWriteSummary, FileWriterOptions,
    PAGE_BUFFER_ALIGNMENT,
};

const PAD_BUFFER: [u8; PAGE_BUFFER_ALIGNMENT] = [72; PAGE_BUFFER_ALIGNMENT];
// In 2.1+, we split large pages on read instead of write to avoid empty pages
// and small pages issues. However, we keep the write-time limit at 32MB to avoid
// potential regressions in 2.0 format readers.
//
// This limit is not applied in the 2.1 writer
const MAX_PAGE_BYTES: usize = 32 * 1024 * 1024;
// Total in-memory budget for buffering serialized page metadata before flushing
// to the spill file. Divided evenly across columns (with a floor of 64 bytes).
const DEFAULT_SPILL_BUFFER_LIMIT: usize = 256 * 1024;

/// Spills serialized page metadata to a temporary file to bound memory usage.
///
/// The spill file is an unstructured sequence of "chunks". Each chunk is a
/// contiguous run of length-delimited protobuf `Page` messages belonging to a
/// single column. Chunks from different columns are interleaved in the order
/// they are flushed (i.e. whenever a column's in-memory buffer exceeds
/// `per_column_limit`). The `column_chunks` index records the (offset, length)
/// of every chunk so each column's pages can be read back and reassembled in
/// order.
struct PageMetadataSpill {
    writer: Box<dyn ObjectWriter>,
    object_store: Arc<ObjectStore>,
    path: Path,
    /// Current write position in the spill file.
    position: u64,
    /// Per-column buffer of serialized (length-delimited protobuf) page metadata
    /// that has not yet been flushed to the spill file.
    column_buffers: Vec<Vec<u8>>,
    /// Per-column list of chunks that have been flushed to the spill file.
    /// Each entry is (offset, length) pointing into the spill file.
    column_chunks: Vec<Vec<(u64, u32)>>,
    /// Maximum bytes to buffer per column before flushing to the spill file.
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
        column_idx: usize,
        page: &pbfile::column_metadata::Page,
    ) -> Result<()> {
        page.encode_length_delimited(&mut self.column_buffers[column_idx])
            .map_err(|e| {
                Error::io_source(Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    e,
                )))
            })?;
        if self.column_buffers[column_idx].len() >= self.per_column_limit {
            self.flush_column(column_idx).await?;
        }
        Ok(())
    }

    async fn flush_column(&mut self, column_idx: usize) -> Result<()> {
        let buf = &self.column_buffers[column_idx];
        if buf.is_empty() {
            return Ok(());
        }
        let len = buf.len();
        self.writer.write_all(buf).await?;
        self.column_chunks[column_idx].push((self.position, len as u32));
        self.position += len as u64;
        self.column_buffers[column_idx].clear();
        Ok(())
    }

    async fn shutdown_writer(&mut self) -> Result<()> {
        for col_idx in 0..self.column_buffers.len() {
            self.flush_column(col_idx).await?;
        }
        ObjectWriter::shutdown(self.writer.as_mut()).await?;
        Ok(())
    }
}

fn decode_spilled_chunk(data: &Bytes) -> Result<Vec<pbfile::column_metadata::Page>> {
    let mut pages = Vec::new();
    let mut cursor = data.clone();
    while cursor.has_remaining() {
        let page =
            pbfile::column_metadata::Page::decode_length_delimited(&mut cursor).map_err(|e| {
                Error::io_source(Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    e,
                )))
            })?;
        pages.push(page);
    }
    Ok(pages)
}

enum PageSpillState {
    Pending(Arc<ObjectStore>, Path),
    Active(PageMetadataSpill),
}

/// A writer for the Lance v2.0 file grammar.
pub struct Writer {
    writer: Box<dyn ObjectWriter>,
    schema: Option<LanceSchema>,
    column_writers: Vec<Box<dyn FieldEncoder>>,
    column_metadata: Vec<pbfile::ColumnMetadata>,
    field_id_to_column_indices: Vec<(u32, u32)>,
    num_columns: u32,
    rows_written: u64,
    // The number of rows written for each top-level field (i.e. each entry in
    // `column_writers`). With `write_batch` every field advances together and
    // these are all equal, but `write_column` advances one field at a time, so
    // a single file may end up with columns of differing item counts.
    field_rows_written: Vec<u64>,
    global_buffers: Vec<(u64, u64)>,
    schema_metadata: HashMap<String, String>,
    encoding_strategy: Box<dyn FieldEncodingStrategy>,
    options: FileWriterOptions,
    page_spill: Option<PageSpillState>,
}

fn initial_column_metadata() -> pbfile::ColumnMetadata {
    pbfile::ColumnMetadata {
        pages: Vec::new(),
        buffer_offsets: Vec::new(),
        buffer_sizes: Vec::new(),
        encoding: None,
    }
}

impl Writer {
    /// Create a new v2.0 writer with a desired output schema.
    pub fn try_new(
        object_writer: Box<dyn ObjectWriter>,
        schema: LanceSchema,
        options: FileWriterOptions,
    ) -> Result<Self> {
        let mut writer = Self::new_lazy(object_writer, options);
        writer.initialize(schema)?;
        Ok(writer)
    }

    /// Create a new v2.0 writer without a desired output schema.
    ///
    /// The output schema will be set based on the first batch of data to arrive.
    /// If no data arrives and the writer is finished then the write will fail.
    pub fn new_lazy(object_writer: Box<dyn ObjectWriter>, options: FileWriterOptions) -> Self {
        Self {
            writer: object_writer,
            schema: None,
            column_writers: Vec::new(),
            column_metadata: Vec::new(),
            num_columns: 0,
            rows_written: 0,
            field_rows_written: Vec::new(),
            field_id_to_column_indices: Vec::new(),
            global_buffers: Vec::new(),
            schema_metadata: HashMap::new(),
            page_spill: None,
            encoding_strategy: Box::new(ArrayFieldEncodingStrategy::new()),
            options,
        }
    }

    /// Spill page metadata to a sidecar file instead of accumulating in memory.
    ///
    /// This can dramatically reduce memory usage when many writers are open
    /// concurrently (e.g. IVF shuffle with thousands of partition writers).
    /// The sidecar file is created lazily on the first page write. The caller
    /// is responsible for cleaning up `path` (e.g. by placing it in a temp
    /// directory that is removed via RAII).
    pub fn with_page_metadata_spill(mut self, object_store: Arc<ObjectStore>, path: Path) -> Self {
        self.page_spill = Some(PageSpillState::Pending(object_store, path));
        self
    }

    async fn do_write_buffer(writer: &mut (impl AsyncWrite + Unpin), buf: &[u8]) -> Result<()> {
        writer.write_all(buf).await?;
        let pad_bytes = pad_bytes::<PAGE_BUFFER_ALIGNMENT>(buf.len());
        writer.write_all(&PAD_BUFFER[..pad_bytes]).await?;
        Ok(())
    }

    async fn write_page(&mut self, encoded_page: EncodedPage) -> Result<()> {
        let buffers = encoded_page.data;
        let mut buffer_offsets = Vec::with_capacity(buffers.len());
        let mut buffer_sizes = Vec::with_capacity(buffers.len());
        for buffer in buffers {
            buffer_offsets.push(self.writer.tell().await? as u64);
            buffer_sizes.push(buffer.len() as u64);
            Self::do_write_buffer(&mut self.writer, &buffer).await?;
        }
        let encoded_encoding = match encoded_page.description {
            PageEncoding::Legacy(array_encoding) => Any::from_msg(&array_encoding)?.encode_to_vec(),
            PageEncoding::Structural(page_layout) => Any::from_msg(&page_layout)?.encode_to_vec(),
        };
        let page = pbfile::column_metadata::Page {
            buffer_offsets,
            buffer_sizes,
            encoding: Some(pbfile::Encoding {
                location: Some(pbfile::encoding::Location::Direct(DirectEncoding {
                    encoding: encoded_encoding,
                })),
            }),
            length: encoded_page.num_rows,
            priority: encoded_page.row_number,
        };
        let col_idx = encoded_page.column_idx as usize;
        if matches!(&self.page_spill, Some(PageSpillState::Pending(..))) {
            let Some(PageSpillState::Pending(store, path)) = self.page_spill.take() else {
                unreachable!()
            };
            self.page_spill = Some(PageSpillState::Active(
                PageMetadataSpill::new(store, path, self.num_columns as usize).await?,
            ));
        }
        match &mut self.page_spill {
            Some(PageSpillState::Active(spill)) => spill.append_page(col_idx, &page).await?,
            None => self.column_metadata[col_idx].pages.push(page),
            Some(PageSpillState::Pending(..)) => unreachable!(),
        }
        Ok(())
    }

    #[instrument(skip_all, level = "debug")]
    async fn write_pages(&mut self, mut encoding_tasks: FuturesOrdered<EncodeTask>) -> Result<()> {
        // As soon as an encoding task is done we write it.  There is no parallelism
        // needed here because "writing" is really just submitting the buffer to the
        // underlying write scheduler (either the OS or object_store's scheduler for
        // cloud writes).  The only time we might truly await on write_page is if the
        // scheduler's write queue is full.
        //
        // Also, there is no point in trying to make write_page parallel anyways
        // because we wouldn't want buffers getting mixed up across pages.
        while let Some(encoding_task) = encoding_tasks.next().await {
            let encoded_page = encoding_task?;
            self.write_page(encoded_page).await?;
        }
        // It's important to flush here, we don't know when the next batch will arrive
        // and the underlying cloud store could have writes in progress that won't advance
        // until we interact with the writer again.  These in-progress writes will time out
        // if we don't flush.
        self.writer.flush().await?;
        Ok(())
    }

    /// Schedule batches of data to be written to the file
    pub async fn write_batches(
        &mut self,
        batches: impl Iterator<Item = &RecordBatch>,
    ) -> Result<()> {
        for batch in batches {
            self.write_batch(batch).await?;
        }
        Ok(())
    }

    fn verify_field_nullability(arr: &ArrayData, field: &Field) -> Result<()> {
        if !field.nullable && arr.null_count() > 0 {
            return Err(Error::invalid_input(format!(
                "The field `{}` contained null values even though the field is marked non-null in the schema",
                field.name
            )));
        }

        for (child_field, child_arr) in field.children.iter().zip(arr.child_data()) {
            Self::verify_field_nullability(child_arr, child_field)?;
        }

        Ok(())
    }

    fn verify_nullability_constraints(&self, batch: &RecordBatch) -> Result<()> {
        for (col, field) in batch
            .columns()
            .iter()
            .zip(self.schema.as_ref().unwrap().fields.iter())
        {
            Self::verify_field_nullability(&col.to_data(), field)?;
        }
        Ok(())
    }

    fn initialize(&mut self, mut schema: LanceSchema) -> Result<()> {
        let cache_bytes_per_column = if let Some(data_cache_bytes) = self.options.data_cache_bytes {
            data_cache_bytes / schema.fields.len() as u64
        } else {
            8 * 1024 * 1024
        };

        let max_page_bytes = self.options.max_page_bytes.unwrap_or_else(|| {
            std::env::var(ENV_LANCE_FILE_WRITER_MAX_PAGE_BYTES)
                .map(|s| {
                    s.parse::<u64>().unwrap_or_else(|e| {
                        warn!(
                            "Failed to parse {}: {}, using default",
                            ENV_LANCE_FILE_WRITER_MAX_PAGE_BYTES, e
                        );
                        MAX_PAGE_BYTES as u64
                    })
                })
                .unwrap_or(MAX_PAGE_BYTES as u64)
        });

        schema.validate()?;

        let keep_original_array = self.options.keep_original_array.unwrap_or(false);
        let encoding_options = EncodingOptions {
            cache_bytes_per_column,
            max_page_bytes,
            keep_original_array,
            buffer_alignment: PAGE_BUFFER_ALIGNMENT as u64,
        };
        let encoder =
            BatchEncoder::try_new(&schema, self.encoding_strategy.as_ref(), &encoding_options)?;
        self.num_columns = encoder.num_columns();

        self.field_rows_written = vec![0; encoder.field_encoders.len()];
        self.column_writers = encoder.field_encoders;
        self.column_metadata = vec![initial_column_metadata(); self.num_columns as usize];
        self.field_id_to_column_indices = encoder.field_id_to_column_index;
        self.schema_metadata
            .extend(std::mem::take(&mut schema.metadata));
        self.schema = Some(schema);
        Ok(())
    }

    fn ensure_initialized(&mut self, batch: &RecordBatch) -> Result<&LanceSchema> {
        if self.schema.is_none() {
            let schema = LanceSchema::try_from(batch.schema().as_ref())?;
            self.initialize(schema)?;
        }
        Ok(self.schema.as_ref().unwrap())
    }

    #[instrument(skip_all, level = "debug")]
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
            .map(|(field_idx, field)| {
                let array =
                    batch
                        .column_by_name(&field.name)
                        .ok_or(Error::invalid_input_source(
                            format!(
                                "Cannot write batch.  The batch was missing the column `{}`",
                                field.name
                            )
                            .into(),
                        ))?;
                Ok((field_idx, array.clone()))
            })
            .collect::<Result<Vec<_>>>()?;
        self.encode_columns(&field_arrays, external_buffers)
    }

    // Encode a set of `(field index, array)` pairs, each advancing only its own
    // column. Each task captures its field's current row offset at encode time,
    // so `advance_columns` must run after this call (never before); the order of
    // the returned tasks relative to `write_pages` does not matter.
    fn encode_columns(
        &mut self,
        field_arrays: &[(usize, ArrayRef)],
        external_buffers: &mut OutOfLineBuffers,
    ) -> Result<Vec<Vec<EncodeTask>>> {
        // Snapshot the starting row number of each field before borrowing the
        // column writers mutably below.
        let row_numbers = field_arrays
            .iter()
            .map(|(field_idx, _)| self.field_rows_written[*field_idx])
            .collect::<Vec<_>>();
        field_arrays
            .iter()
            .zip(row_numbers)
            .map(|((field_idx, array), row_number)| {
                let repdef = RepDefBuilder::default();
                let num_rows = array.len() as u64;
                self.column_writers[*field_idx].maybe_encode(
                    array.clone(),
                    external_buffers,
                    repdef,
                    row_number,
                    num_rows,
                )
            })
            .collect::<Result<Vec<_>>>()
    }

    // Advance the per-field row counters after a set of columns has been
    // written, keeping `rows_written` (the file's logical length) in sync as the
    // longest column. Only the written fields move, so their new totals fold into
    // `rows_written` directly without rescanning every field. (`write_batch`
    // advances every field uniformly and tracks this inline instead.)
    fn advance_columns(&mut self, field_arrays: &[(usize, ArrayRef)]) {
        for (field_idx, array) in field_arrays {
            let new_total = self.field_rows_written[*field_idx] + array.len() as u64;
            self.field_rows_written[*field_idx] = new_total;
            self.rows_written = self.rows_written.max(new_total);
        }
    }

    /// Schedule a batch of data to be written to the file
    ///
    /// Note: the future returned by this method may complete before the data has been fully
    /// flushed to the file (some data may be in the data cache or the I/O cache)
    pub async fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        debug!(
            "write_batch called with {} rows, {} columns, and {} bytes of data",
            batch.num_rows(),
            batch.num_columns(),
            batch.get_array_memory_size()
        );
        self.ensure_initialized(batch)?;
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
        // First we push each array into its column writer.  This may or may not generate enough
        // data to trigger an encoding task.  We collect any encoding tasks into a queue.
        let mut external_buffers =
            OutOfLineBuffers::new(self.tell().await?, PAGE_BUFFER_ALIGNMENT as u64);
        let encoding_tasks = self.encode_batch(batch, &mut external_buffers)?;
        // Next, write external buffers
        for external_buffer in external_buffers.take_buffers() {
            Self::do_write_buffer(&mut self.writer, &external_buffer).await?;
        }

        let encoding_tasks = encoding_tasks
            .into_iter()
            .flatten()
            .collect::<FuturesOrdered<_>>();

        // `write_batch` advances every field by the same amount, so the longest
        // column simply grows by `num_rows`. Guard against overflowing the row
        // counter.
        if self.rows_written.checked_add(num_rows).is_none() {
            return Err(Error::invalid_input_source(format!("cannot write batch with {} rows because {} rows have already been written and Lance files cannot contain more than 2^64 rows", num_rows, self.rows_written).into()));
        }
        for field_rows in self.field_rows_written.iter_mut() {
            *field_rows += num_rows;
        }
        self.rows_written += num_rows;

        self.write_pages(encoding_tasks).await?;

        Ok(())
    }

    /// Write a single column, advancing only that column's row counter.
    ///
    /// Unlike [`write_batch`](Self::write_batch), which advances every column
    /// from a single shared row counter, this method advances one column
    /// independently. Used across calls it produces a single file whose columns
    /// may have different item counts.
    ///
    /// `column_index` refers to a top-level field in the writer's schema (the
    /// same order as the schema's fields); a nested child cannot be targeted on
    /// its own. Because each call writes the whole field from a single array, the
    /// children of a struct field always advance together and stay equal-length;
    /// only different top-level fields can diverge in length. A column may be
    /// written across multiple calls; its values are appended. A field that is
    /// never written ends up as a zero-length column. The writer must have been
    /// created with an explicit schema (via [`try_new`](Self::try_new)); a lazy
    /// schema cannot be inferred here because individual calls need not cover
    /// every field.
    ///
    /// ```
    /// # use arrow_array::{ArrayRef, Int32Array};
    /// # use std::sync::Arc;
    /// # use lance_file::writer::FileWriter;
    /// # async fn example(writer: &mut FileWriter) -> lance_core::Result<()> {
    /// // Field 0 gets three values, field 1 gets one — a non-rectangular file.
    /// writer.write_column(0, Arc::new(Int32Array::from(vec![1, 2, 3]))).await?;
    /// writer.write_column(1, Arc::new(Int32Array::from(vec![10]))).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn write_column(&mut self, column_index: usize, array: ArrayRef) -> Result<()> {
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

        // A never-advanced field simply remains a zero-length column, which the
        // encoders handle at `finish` time.
        if array.is_empty() {
            return Ok(());
        }

        let columns = [(column_index, array)];
        let mut external_buffers =
            OutOfLineBuffers::new(self.tell().await?, PAGE_BUFFER_ALIGNMENT as u64);
        let encoding_tasks = self.encode_columns(&columns, &mut external_buffers)?;
        for external_buffer in external_buffers.take_buffers() {
            Self::do_write_buffer(&mut self.writer, &external_buffer).await?;
        }
        let encoding_tasks = encoding_tasks
            .into_iter()
            .flatten()
            .collect::<FuturesOrdered<_>>();

        self.advance_columns(&columns);
        self.write_pages(encoding_tasks).await?;
        Ok(())
    }

    async fn write_column_metadata(
        &mut self,
        metadata: pbfile::ColumnMetadata,
    ) -> Result<(u64, u64)> {
        let metadata_bytes = metadata.encode_to_vec();
        let position = self.writer.tell().await? as u64;
        let len = metadata_bytes.len() as u64;
        self.writer.write_all(&metadata_bytes).await?;
        Ok((position, len))
    }

    async fn write_column_metadatas(&mut self) -> Result<Vec<(u64, u64)>> {
        let metadatas = std::mem::take(&mut self.column_metadata);

        // If spilling, finalize the spill writer and reopen for reading.
        // The spill file itself is cleaned up by the caller (it lives in a
        // temp directory managed by the caller's RAII guard).
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
        for (col_idx, mut metadata) in metadatas.into_iter().enumerate() {
            if let Some(reader) = &spill_reader {
                let mut pages = Vec::new();
                for &(offset, len) in &spill_chunks[col_idx] {
                    let data = reader
                        .get_range(offset as usize..(offset as usize + len as usize))
                        .await
                        .map_err(|e| Error::io_source(Box::new(e)))?;
                    pages.extend(decode_spilled_chunk(&data)?);
                }
                metadata.pages = pages;
            }
            metadata_positions.push(self.write_column_metadata(metadata).await?);
        }

        Ok(metadata_positions)
    }

    fn make_file_descriptor(
        schema: &lance_core::datatypes::Schema,
        num_rows: u64,
    ) -> Result<pb::FileDescriptor> {
        let fields_with_meta = FieldsWithMeta::from(schema);
        Ok(pb::FileDescriptor {
            schema: Some(pb::Schema {
                fields: fields_with_meta.fields.0,
                metadata: fields_with_meta.metadata,
            }),
            length: num_rows,
        })
    }

    async fn write_global_buffers(&mut self) -> Result<Vec<(u64, u64)>> {
        let schema = self.schema.as_mut().ok_or(Error::invalid_input("No schema provided on writer open and no data provided.  Schema is unknown and file cannot be created"))?;
        schema.metadata = std::mem::take(&mut self.schema_metadata);
        // Use descriptor layout for blob v2 fields in the footer to avoid exposing logical child fields.
        schema
            .fields
            .iter_mut()
            .for_each(|f| f.unload_blobs_recursive());

        let file_descriptor = Self::make_file_descriptor(schema, self.rows_written)?;
        let file_descriptor_bytes = file_descriptor.encode_to_vec();
        let file_descriptor_len = file_descriptor_bytes.len() as u64;
        let file_descriptor_position = self.writer.tell().await? as u64;
        self.writer.write_all(&file_descriptor_bytes).await?;
        let mut gbo_table = Vec::with_capacity(1 + self.global_buffers.len());
        gbo_table.push((file_descriptor_position, file_descriptor_len));
        gbo_table.append(&mut self.global_buffers);
        Ok(gbo_table)
    }

    /// Add a metadata entry to the schema
    ///
    /// This method is useful because sometimes the metadata is not known until after the
    /// data has been written.  This method allows you to alter the schema metadata.  It
    /// must be called before `finish` is called.
    pub fn add_schema_metadata(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.schema_metadata.insert(key.into(), value.into());
    }

    /// Prepare the writer when column data and metadata were produced externally.
    ///
    /// This is useful for flows that copy already-encoded pages (e.g., binary copy
    /// during compaction) where the column buffers have been written directly and we
    /// only need to write the footer and schema metadata. The provided
    /// `column_metadata` must describe the buffers already persisted by the
    /// underlying `ObjectWriter`, and `rows_written` should reflect the total number
    /// of rows in those buffers.
    pub fn initialize_with_external_metadata(
        &mut self,
        schema: lance_core::datatypes::Schema,
        column_metadata: Vec<pbfile::ColumnMetadata>,
        rows_written: u64,
    ) {
        self.schema = Some(schema);
        self.num_columns = column_metadata.len() as u32;
        self.column_metadata = column_metadata;
        self.rows_written = rows_written;
    }

    /// Adds a global buffer to the file
    ///
    /// The global buffer can contain any arbitrary bytes.  It will be written to the disk
    /// immediately.  This method returns the index of the global buffer (this will always
    /// start at 1 and increment by 1 each time this method is called)
    pub async fn add_global_buffer(&mut self, buffer: Bytes) -> Result<u32> {
        let position = self.writer.tell().await? as u64;
        let len = buffer.len() as u64;
        Self::do_write_buffer(&mut self.writer, &buffer).await?;
        self.global_buffers.push((position, len));
        Ok(self.global_buffers.len() as u32)
    }

    async fn finish_writers(&mut self) -> Result<()> {
        let mut col_idx = 0;
        for mut writer in std::mem::take(&mut self.column_writers) {
            let mut external_buffers =
                OutOfLineBuffers::new(self.tell().await?, PAGE_BUFFER_ALIGNMENT as u64);
            let columns = writer.finish(&mut external_buffers).await?;
            for buffer in external_buffers.take_buffers() {
                self.writer.write_all(&buffer).await?;
            }
            debug_assert_eq!(
                columns.len(),
                writer.num_columns() as usize,
                "Expected {} columns from column at index {} and got {}",
                writer.num_columns(),
                col_idx,
                columns.len()
            );
            for column in columns {
                for page in column.final_pages {
                    self.write_page(page).await?;
                }
                let column_metadata = &mut self.column_metadata[col_idx];
                let mut buffer_pos = self.writer.tell().await? as u64;
                for buffer in column.column_buffers {
                    column_metadata.buffer_offsets.push(buffer_pos);
                    let mut size = 0;
                    Self::do_write_buffer(&mut self.writer, &buffer).await?;
                    size += buffer.len() as u64;
                    buffer_pos += size;
                    column_metadata.buffer_sizes.push(size);
                }
                let encoded_encoding = Any::from_msg(&column.encoding)?.encode_to_vec();
                column_metadata.encoding = Some(pbfile::Encoding {
                    location: Some(pbfile::encoding::Location::Direct(pbfile::DirectEncoding {
                        encoding: encoded_encoding,
                    })),
                });
                col_idx += 1;
            }
        }
        if col_idx != self.column_metadata.len() {
            panic!(
                "Column writers finished with {} columns but we expected {}",
                col_idx,
                self.column_metadata.len()
            );
        }
        Ok(())
    }

    /// Finishes writing the file
    ///
    /// This method will wait until all data has been flushed to the file.  Then it
    /// will write the file metadata and the footer.  It will not return until all
    /// data has been flushed and the file has been closed.
    ///
    /// Returns a summary of the completed file write.
    pub async fn finish(&mut self) -> Result<FileWriteSummary> {
        // 1. flush any remaining data and write out those pages
        let mut external_buffers =
            OutOfLineBuffers::new(self.tell().await?, PAGE_BUFFER_ALIGNMENT as u64);
        let encoding_tasks = self
            .column_writers
            .iter_mut()
            .map(|writer| writer.flush(&mut external_buffers))
            .collect::<Result<Vec<_>>>()?;
        for external_buffer in external_buffers.take_buffers() {
            Self::do_write_buffer(&mut self.writer, &external_buffer).await?;
        }
        let encoding_tasks = encoding_tasks
            .into_iter()
            .flatten()
            .collect::<FuturesOrdered<_>>();
        self.write_pages(encoding_tasks).await?;

        if !self.column_writers.is_empty() {
            self.finish_writers().await?;
        }

        // 3. write global buffers (we write the schema here)
        let global_buffer_offsets = self.write_global_buffers().await?;
        let num_global_buffers = global_buffer_offsets.len() as u32;

        // 4. write the column metadatas
        let column_metadata_start = self.writer.tell().await? as u64;
        let metadata_positions = self.write_column_metadatas().await?;

        // 5. write the column metadata offset table
        let cmo_table_start = self.writer.tell().await? as u64;
        for (meta_pos, meta_len) in metadata_positions {
            self.writer.write_u64_le(meta_pos).await?;
            self.writer.write_u64_le(meta_len).await?;
        }

        // 6. write global buffers offset table
        let gbo_table_start = self.writer.tell().await? as u64;
        for (gbo_pos, gbo_len) in global_buffer_offsets {
            self.writer.write_u64_le(gbo_pos).await?;
            self.writer.write_u64_le(gbo_len).await?;
        }

        // 7. write the footer
        self.writer.write_u64_le(column_metadata_start).await?;
        self.writer.write_u64_le(cmo_table_start).await?;
        self.writer.write_u64_le(gbo_table_start).await?;
        self.writer.write_u32_le(num_global_buffers).await?;
        self.writer.write_u32_le(self.num_columns).await?;
        self.writer.write_u16_le(0).await?;
        self.writer.write_u16_le(3).await?;
        self.writer.write_all(MAGIC).await?;

        // 7. close the writer
        let write_result = ObjectWriter::shutdown(self.writer.as_mut()).await?;

        Ok(FileWriteSummary {
            num_rows: self.rows_written,
            size_bytes: write_result.size as u64,
        })
    }

    pub async fn abort(&mut self) {
        // For multipart uploads, ObjectWriter's Drop impl will abort
        // the upload when the writer is dropped.
    }

    pub async fn tell(&mut self) -> Result<u64> {
        Ok(self.writer.tell().await? as u64)
    }

    /// Append a buffer whose metadata is supplied by the caller.
    pub async fn write_external_buffer(&mut self, bytes: &[u8]) -> Result<(u64, u64)> {
        let start = self.tell().await?;
        self.writer.write_all(bytes).await?;
        Ok((start, bytes.len() as u64))
    }

    pub fn field_id_to_column_indices(&self) -> &[(u32, u32)] {
        &self.field_id_to_column_indices
    }
}

// Creates a lance footer and appends it to the encoded data
//
// The logic here is very similar to logic in the FileWriter except we
// are using BufMut (put_xyz) instead of AsyncWrite (write_xyz).
pub fn concat_lance_footer(batch: &EncodedBatch, write_schema: bool) -> Result<Bytes> {
    // Estimating 1MiB for file footer
    let mut data = BytesMut::with_capacity(batch.data.len() + 1024 * 1024);
    data.put(batch.data.clone());
    // write global buffers (we write the schema here)
    let global_buffers = if write_schema {
        let schema_start = data.len() as u64;
        let lance_schema = lance_core::datatypes::Schema::try_from(batch.schema.as_ref())?;
        let descriptor = Writer::make_file_descriptor(&lance_schema, batch.num_rows)?;
        let descriptor_bytes = descriptor.encode_to_vec();
        let descriptor_len = descriptor_bytes.len() as u64;
        data.put(descriptor_bytes.as_slice());

        vec![(schema_start, descriptor_len)]
    } else {
        vec![]
    };
    let col_metadata_start = data.len() as u64;

    let mut col_metadata_positions = Vec::new();
    // Write column metadata
    for col in &batch.page_table {
        let position = data.len() as u64;
        let pages = col
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
                let (buffer_offsets, buffer_sizes): (Vec<_>, Vec<_>) = page_info
                    .buffer_offsets_and_sizes
                    .as_ref()
                    .iter()
                    .cloned()
                    .unzip();
                Ok(pbfile::column_metadata::Page {
                    buffer_offsets,
                    buffer_sizes,
                    encoding: Some(pbfile::Encoding {
                        location: Some(pbfile::encoding::Location::Direct(DirectEncoding {
                            encoding: encoded_encoding,
                        })),
                    }),
                    length: page_info.num_rows,
                    priority: page_info.priority,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let (buffer_offsets, buffer_sizes): (Vec<_>, Vec<_>) =
            col.buffer_offsets_and_sizes.iter().cloned().unzip();
        let encoded_col_encoding = Any::from_msg(&col.encoding)?.encode_to_vec();
        let column = pbfile::ColumnMetadata {
            pages,
            buffer_offsets,
            buffer_sizes,
            encoding: Some(pbfile::Encoding {
                location: Some(pbfile::encoding::Location::Direct(pbfile::DirectEncoding {
                    encoding: encoded_col_encoding,
                })),
            }),
        };
        let column_bytes = column.encode_to_vec();
        col_metadata_positions.push((position, column_bytes.len() as u64));
        data.put(column_bytes.as_slice());
    }
    // Write column metadata offsets table
    let cmo_table_start = data.len() as u64;
    for (meta_pos, meta_len) in col_metadata_positions {
        data.put_u64_le(meta_pos);
        data.put_u64_le(meta_len);
    }
    // Write global buffers offsets table
    let gbo_table_start = data.len() as u64;
    let num_global_buffers = global_buffers.len() as u32;
    for (gbo_pos, gbo_len) in global_buffers {
        data.put_u64_le(gbo_pos);
        data.put_u64_le(gbo_len);
    }

    // write the footer
    data.put_u64_le(col_metadata_start);
    data.put_u64_le(cmo_table_start);
    data.put_u64_le(gbo_table_start);
    data.put_u32_le(num_global_buffers);
    data.put_u32_le(batch.page_table.len() as u32);
    data.put_u16_le(2);
    data.put_u16_le(0);
    data.put(MAGIC.as_slice());

    Ok(data.freeze())
}
