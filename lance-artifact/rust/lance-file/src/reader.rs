// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::{
    borrow::Cow,
    collections::{BTreeMap, BTreeSet},
    fmt::Debug,
    io::Cursor,
    ops::Range,
    pin::Pin,
    sync::Arc,
};

use arrow_array::RecordBatchReader;
use arrow_schema::Schema as ArrowSchema;
use async_trait::async_trait;
use byteorder::{ByteOrder, LittleEndian, ReadBytesExt};
use bytes::{Bytes, BytesMut};
use futures::{Stream, StreamExt, stream::BoxStream};
use lance_core::deepsize::{Context, DeepSizeOf};
use lance_encoding::{
    EncodingsIo,
    decoder::{
        ColumnInfo, DecoderConfig, DecoderPlugins, FilterExpression, PageEncoding, ReadBatchTask,
        RequestedRows, SchedulerDecoderConfig, schedule_and_decode, schedule_and_decode_blocking,
    },
    encoder::EncodedBatch,
};
use log::debug;
use object_store::path::Path;
use prost::Message;

use lance_core::{
    Error, Result,
    cache::{CacheKey, CacheKeySchema, KeyBuilder, LanceCache},
    datatypes::{Field, Schema},
};
use lance_encoding::format::pb as pbenc;
use lance_encoding::format::pb21 as pbenc21;
use lance_io::{
    ReadBatchParams,
    scheduler::FileScheduler,
    stream::{RecordBatchStream, RecordBatchStreamAdapter},
};

use crate::{
    datatypes::{Fields, FieldsWithMeta},
    format::{MAGIC, pb, pbfile},
    io::LanceEncodingsIo,
    version::ConcreteFileVersion,
    versions,
};

pub(crate) mod structural;

/// Default chunk size for reading large pages (8MiB)
/// Pages larger than this will be split into multiple chunks during read
pub const DEFAULT_READ_CHUNK_SIZE: u64 = 8 * 1024 * 1024;

// For now, we don't use global buffers for anything other than schema.  If we
// use these later we should make them lazily loaded and then cached once loaded.
//
// We store their position / length for debugging purposes
#[derive(Debug, DeepSizeOf)]
pub struct BufferDescriptor {
    pub position: u64,
    pub size: u64,
}

impl BufferDescriptor {
    fn checked_range(&self, buffer_index: usize, file_len: u64) -> Result<Range<u64>> {
        let end = self.position.checked_add(self.size).ok_or_else(|| {
            Error::invalid_input_source(
                format!(
                    "Global buffer {} range overflows: position={}, size={}",
                    buffer_index, self.position, self.size
                )
                .into(),
            )
        })?;
        if self.position > file_len {
            return Err(Error::invalid_input_source(
                format!(
                    "Global buffer {} position {} is outside file of size {}",
                    buffer_index, self.position, file_len
                )
                .into(),
            ));
        }
        if end > file_len {
            return Err(Error::invalid_input_source(
                format!(
                    "Global buffer {} range {}..{} is outside file of size {}",
                    buffer_index, self.position, end, file_len
                )
                .into(),
            ));
        }
        Ok(self.position..end)
    }
}

/// Statistics summarize some of the file metadata for quick summary info
#[derive(Debug)]
pub struct FileStatistics {
    /// Statistics about each of the columns in the file
    pub columns: Vec<ColumnStatistics>,
}

/// Summary information describing a column
#[derive(Debug)]
pub struct ColumnStatistics {
    /// The number of pages in the column
    pub num_pages: usize,
    /// The total number of data & metadata bytes in the column
    ///
    /// This is the compressed on-disk size
    pub size_bytes: u64,
}

// TODO: Caching
#[derive(Debug)]
pub struct CachedFileMetadata {
    /// The schema of the file
    pub file_schema: Arc<Schema>,
    /// The column metadatas
    pub column_metadatas: Vec<pbfile::ColumnMetadata>,
    pub column_infos: Vec<Arc<ColumnInfo>>,
    /// The number of rows in the file
    pub num_rows: u64,
    pub file_buffers: Vec<BufferDescriptor>,
    /// The number of bytes contained in the data page section of the file
    pub num_data_bytes: u64,
    /// The number of bytes contained in the column metadata (not including buffers
    /// referenced by the metadata)
    pub num_column_metadata_bytes: u64,
    /// The number of bytes contained in global buffers
    pub num_global_buffer_bytes: u64,
    /// The number of bytes contained in the CMO and GBO tables
    pub num_footer_bytes: u64,
    /// The major version number stored in the file footer.
    pub major_version: u16,
    /// The minor version number stored in the file footer.
    pub minor_version: u16,
    pub version: ConcreteFileVersion,
    /// The actual total file size in bytes, as reported by the object store.
    pub file_size_bytes: u64,
    /// User global buffers (index >= 1) whose bytes were already captured by the
    /// tail read that `read_all_metadata` performs at open, keyed by buffer index.
    ///
    /// All global buffers are laid out contiguously starting at the schema, so on
    /// small/medium files they land inside the captured tail window. Retaining
    /// those bytes lets `read_global_buffer` serve them with zero additional I/O.
    /// The bytes are copied out of the tail (rather than sliced) so the much
    /// larger tail allocation can be dropped — we only hold what we will serve.
    ///
    /// The schema (buffer 0) is excluded: it is already decoded at open and is
    /// not fetched through `read_global_buffer`. Buffers that fall outside the
    /// window (large files) are absent here and fall back to a dedicated read.
    pub retained_global_buffers: BTreeMap<u32, Bytes>,
}

impl CachedFileMetadata {
    /// Total file size in bytes.
    pub fn file_size(&self) -> u64 {
        self.file_size_bytes
    }
}

fn column_metadata_deep_size(column_metadatas: &[pbfile::ColumnMetadata]) -> usize {
    column_metadatas
        .iter()
        .map(|cm| cm.encoded_len() * 4)
        .sum::<usize>()
        + std::mem::size_of_val(column_metadatas)
}

impl DeepSizeOf for CachedFileMetadata {
    fn deep_size_of_children(&self, context: &mut Context) -> usize {
        let schema_size = self.file_schema.deep_size_of_children(context);

        let buffers_size: usize = self
            .file_buffers
            .iter()
            .map(|fb| fb.deep_size_of_children(context))
            .sum();

        // column_metadatas is Vec<pbfile::ColumnMetadata> (protobuf generated,
        // does not implement DeepSizeOf). We use prost::Message::encoded_len()
        // as a proxy for in-memory size. The decoded representation is typically
        // several times larger than the wire format due to heap-allocated
        // repeated/string/bytes fields, so we apply a 4x multiplier.
        let column_metadatas_size = column_metadata_deep_size(self.column_metadatas.as_slice());

        // column_infos is Vec<Arc<ColumnInfo>>. Each ColumnInfo contains
        // page_infos (with protobuf PageEncoding), buffer offsets, and a
        // column-level ColumnEncoding protobuf.
        let column_infos_size = self.column_infos.deep_size_of_children(context);

        // Global buffer bytes retained for zero-IO reads (copied out of the tail).
        let retained_buffers_size = self.retained_global_buffers.deep_size_of_children(context);

        schema_size
            + buffers_size
            + column_metadatas_size
            + column_infos_size
            + retained_buffers_size
    }
}

/// Lightweight file metadata used to locate per-column metadata on demand.
///
/// This contains the file-level schema, row count, global buffer descriptors,
/// and column metadata offset table. Unlike [`CachedFileMetadata`], it does not
/// hold decoded metadata for every column.
#[derive(Debug, DeepSizeOf)]
pub struct FileMetadataIndex {
    pub(crate) file_schema: Arc<Schema>,
    pub(crate) num_rows: u64,
    pub(crate) file_buffers: Vec<BufferDescriptor>,
    pub(crate) column_metadata_offsets: Arc<[(u64, u64)]>,
    pub(crate) num_columns: u32,
    pub(crate) version: ConcreteFileVersion,
    pub(crate) file_size_bytes: u64,
    pub(crate) retained_global_buffers: BTreeMap<u32, Bytes>,
}

impl FileMetadataIndex {
    /// Returns the total size of the file in bytes.
    pub fn file_size(&self) -> u64 {
        self.file_size_bytes
    }

    /// Returns the number of physical columns in the file.
    pub fn num_columns(&self) -> u32 {
        self.num_columns
    }
}

#[derive(Debug)]
struct CachedColumnMetadata {
    column_metadata: pbfile::ColumnMetadata,
    column_info: Arc<ColumnInfo>,
}

impl DeepSizeOf for CachedColumnMetadata {
    fn deep_size_of_children(&self, context: &mut Context) -> usize {
        column_metadata_deep_size(std::slice::from_ref(&self.column_metadata))
            + self.column_info.deep_size_of_children(context)
    }
}

#[derive(Debug, Clone)]
struct ColumnMetadataCacheKey {
    column_index: u32,
}

impl CacheKey for ColumnMetadataCacheKey {
    type ValueType = CachedColumnMetadata;

    fn key(&self) -> Cow<'_, str> {
        Cow::Owned(format!("column_metadata/{}", self.column_index))
    }

    fn type_name() -> &'static str {
        "ColumnMetadata"
    }

    fn schema() -> CacheKeySchema {
        CacheKeySchema::new("lance.file.column-metadata-key", 1)
    }

    fn write_key(&self, builder: &mut KeyBuilder) {
        builder.write_u32(self.column_index);
    }
}

impl CachedFileMetadata {
    pub fn version(&self) -> ConcreteFileVersion {
        self.version
    }
}

/// Selecting columns from a lance file requires specifying both the
/// index of the column and the data type of the column
///
/// Partly, this is because it is not strictly required that columns
/// be read into the same type.  For example, a string column may be
/// read as a string, large_string or string_view type.
///
/// A read will only succeed if the decoder for a column is capable
/// of decoding into the requested type.
///
/// Note that this should generally be limited to different in-memory
/// representations of the same semantic type.  An encoding could
/// theoretically support "casting" (e.g. int to string, etc.) but
/// there is little advantage in doing so here.
///
/// Note: in order to specify a projection the user will need some way
/// to figure out the column indices.  In the table format we do this
/// using field IDs and keeping track of the field id->column index mapping.
///
/// If users are not using the table format then they will need to figure
/// out some way to do this themselves.
#[derive(Debug, Clone)]
pub struct ReaderProjection {
    /// The data types (schema) of the selected columns.  The names
    /// of the schema are arbitrary and ignored.
    pub schema: Arc<Schema>,
    /// The indices of the columns to load.
    ///
    /// The content of this vector depends on the file version.
    ///
    /// In Lance File Version 2.0 we need ids for structural fields as
    /// well as leaf fields:
    ///
    ///   - Primitive: the index of the column in the schema
    ///   - List: the index of the list column in the schema
    ///     followed by the column indices of the children
    ///   - FixedSizeList (of primitive): the index of the column in the schema
    ///     (this case is not nested)
    ///   - FixedSizeList (of non-primitive): not yet implemented
    ///   - Dictionary: same as primitive
    ///   - Struct: the index of the struct column in the schema
    ///     followed by the column indices of the children
    ///
    ///   In other words, this should be a DFS listing of the desired schema.
    ///
    /// In Lance File Version 2.1 we only need ids for leaf fields.  Any structural
    /// fields are completely transparent.
    ///
    /// For example, if the goal is to load:
    ///
    ///   x: int32
    ///   y: `struct<z: int32, w: string>`
    ///   z: `list<int32>`
    ///
    /// and the schema originally used to store the data was:
    ///
    ///   a: `struct<x: int32>`
    ///   b: int64
    ///   y: `struct<z: int32, c: int64, w: string>`
    ///   z: `list<int32>`
    ///
    /// Then the column_indices should be:
    ///
    /// - 2.0: [1, 3, 4, 6, 7, 8]
    /// - 2.1: [0, 2, 4, 5]
    pub column_indices: Vec<u32>,
}

impl ReaderProjection {
    /// Returns whether this projection is selective enough to benefit from
    /// loading column metadata through the file's metadata index.
    ///
    /// The caller must already have selected a file format that supports indexed
    /// metadata. This method only evaluates the projection shape and selectivity.
    pub fn prefers_indexed_metadata(&self, total_columns: usize) -> bool {
        FileMetadataProvider::projection_matches_indexed_metadata(self)
            && self.column_indices.len().saturating_mul(4) < total_columns
    }
}

/// File Reader Options that can control reading behaviors, such as whether to enable caching on repetition indices
#[derive(Clone, Debug)]
pub struct FileReaderOptions {
    pub decoder_config: DecoderConfig,
    /// Size of chunks when reading large pages. Pages larger than this
    /// will be read in multiple chunks to control memory usage.
    /// Default: 8MB (DEFAULT_READ_CHUNK_SIZE)
    pub read_chunk_size: u64,
    /// If set, the reader will produce batches whose total size in bytes
    /// is approximately this value. The row-based `batch_size` remains an
    /// independent upper bound, and the limit reached first determines the batch size.
    ///
    /// This can be set at the dataset level (via `ReadParams::file_reader_options`)
    /// to provide a default for all scans, or at the scanner level (via
    /// `Scanner::batch_size_bytes`) to override per scan.
    pub batch_size_bytes: Option<u64>,
}

impl Default for FileReaderOptions {
    fn default() -> Self {
        Self {
            decoder_config: DecoderConfig::default(),
            read_chunk_size: DEFAULT_READ_CHUNK_SIZE,
            batch_size_bytes: None,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct PreparedProjection {
    pub column_infos: Vec<Arc<ColumnInfo>>,
    pub decoder_projection: ReaderProjection,
}

#[derive(Debug, Clone)]
pub(crate) enum FileMetadataProvider {
    Full(Arc<CachedFileMetadata>),
    Indexed(Arc<FileMetadataIndex>),
}

/// Executable projection behavior selected by an exact file-version module.
///
/// The shared reader invokes this behavior but never interprets a version or
/// accepted-grammar profile.
#[async_trait]
pub(crate) trait ReadProjection: Debug + Send + Sync {
    fn validate_indexed(
        &self,
        projection: &ReaderProjection,
        metadata_index: &FileMetadataIndex,
    ) -> Result<()>;

    fn read_length(&self, prepared: &PreparedProjection) -> Result<u64>;

    async fn prepare(
        &self,
        metadata_provider: &FileMetadataProvider,
        projection: &ReaderProjection,
        io: &Arc<dyn EncodingsIo>,
        cache: &Arc<LanceCache>,
    ) -> Result<(PreparedProjection, u64)>;
}

#[derive(Debug, Clone)]
pub(crate) struct DecodeEngine {
    pub scheduler: Arc<dyn EncodingsIo>,
    pub base_projection: ReaderProjection,
    pub metadata_provider: FileMetadataProvider,
    pub read_projection: Arc<dyn ReadProjection>,
    pub decoder_plugins: Arc<DecoderPlugins>,
    pub cache: Arc<LanceCache>,
    pub options: FileReaderOptions,
}

/// A projection-scoped reader for a current-format Lance file.
///
/// This reader fixes a base projection at construction time. All later reads
/// must stay within that projection, which lets the reader load only the column
/// metadata needed by the base projection when opening from a [`FileMetadataIndex`].
/// It intentionally does not expose APIs that require synchronous access to full
/// file metadata.
#[derive(Debug, Clone)]
pub struct ProjectedFileReader {
    core: DecodeEngine,
}

/// A current-format Lance file reader backed by fully decoded metadata.
#[derive(Debug, Clone)]
pub struct FileReader {
    pub(crate) core: DecodeEngine,
    pub(crate) metadata: Arc<CachedFileMetadata>,
}

pub(crate) fn tasks_to_record_batch_stream(
    schema: Arc<Schema>,
    tasks: Pin<Box<dyn Stream<Item = ReadBatchTask> + Send>>,
    batch_readahead: u32,
) -> Pin<Box<dyn RecordBatchStream>> {
    let arrow_schema = Arc::new(ArrowSchema::from(schema.as_ref()));
    let batches = tasks
        .map(|task| task.task)
        .buffered(batch_readahead as usize)
        .boxed();
    Box::pin(RecordBatchStreamAdapter::new(arrow_schema, batches))
}

pub(crate) enum RawFileMetadataOpen {
    Legacy {
        major_version: u16,
        minor_version: u16,
    },
    Current {
        version: ConcreteFileVersion,
        metadata: RawFileMetadata,
    },
}

pub(crate) struct RawFileMetadata {
    pub file_schema: Arc<Schema>,
    pub column_metadatas: Vec<pbfile::ColumnMetadata>,
    pub num_rows: u64,
    pub file_buffers: Vec<BufferDescriptor>,
    pub num_data_bytes: u64,
    pub num_column_metadata_bytes: u64,
    pub num_global_buffer_bytes: u64,
    pub num_footer_bytes: u64,
    pub footer: Footer,
    pub file_size_bytes: u64,
    pub retained_global_buffers: BTreeMap<u32, Bytes>,
}

#[derive(Debug)]
pub(crate) struct Footer {
    #[allow(dead_code)]
    pub column_meta_start: u64,
    // We don't use this today because we always load metadata for every column
    // and don't yet support "metadata projection"
    #[allow(dead_code)]
    pub column_meta_offsets_start: u64,
    pub global_buff_offsets_start: u64,
    pub num_global_buffers: u32,
    pub num_columns: u32,
    pub major_version: u16,
    pub minor_version: u16,
}

const FOOTER_LEN: usize = 40;

// Count the V2.1 physical columns required to reconstruct a projected field.
// This is the same DFS shape consumed by `ColumnInfoIter`: ordinary structural
// nodes are transparent and leaves contribute columns. Indexed metadata loading
// can therefore compact any ordinary structural projection into 0..N while
// preserving this order.
//
// Blob and packed-struct fields remain unsupported by indexed projection. Their
// opaque decode semantics are handled by the existing full-metadata reader.
fn indexed_projection_column_count(field: &Field) -> Option<usize> {
    if field.is_blob() || field.is_packed_struct() {
        return None;
    }

    if field.children.is_empty() {
        return Some(1);
    }

    field.children.iter().try_fold(0usize, |count, child| {
        count.checked_add(indexed_projection_column_count(child)?)
    })
}

// The reader combines a projection's columns into rectangular batches, so they
// must all have the same length.  Returns that common length, or a descriptive
// error (naming each column's length) when they differ. Ordinary files always
// pass; only files written with `FileWriter::write_column` whose columns ended up
// unequal can fail, and those must be read separately.
pub(crate) fn normalized_column_num_rows(info: &ColumnInfo) -> Result<u64> {
    info.page_infos.iter().try_fold(0_u64, |rows, page| {
        let page_rows = match &page.encoding {
            PageEncoding::Structural(layout) => match &layout.layout {
                Some(pbenc21::page_layout::Layout::SparseLayout(sparse)) => sparse
                    .structural_layers
                    .first()
                    .and_then(|layer| layer.layer.as_ref())
                    .map_or(page.num_rows, |layer| match layer {
                        pbenc21::sparse_structural_layer::Layer::Validity(layer) => layer.num_slots,
                        pbenc21::sparse_structural_layer::Layer::List(layer) => layer.num_slots,
                        pbenc21::sparse_structural_layer::Layer::FixedSizeList(layer) => {
                            layer.num_slots
                        }
                    }),
                _ => page.num_rows,
            },
            _ => page.num_rows,
        };
        rows.checked_add(page_rows)
            .ok_or_else(|| Error::invalid_input_source("Column row count overflows u64".into()))
    })
}

pub(crate) fn verify_uniform_lengths(field_lengths: &[(&str, u64)]) -> Result<u64> {
    let first = field_lengths.first().map_or(0, |&(_, len)| len);
    if field_lengths.iter().all(|&(_, len)| len == first) {
        return Ok(first);
    }
    let columns = field_lengths
        .iter()
        .map(|(name, len)| format!("{name}={len}"))
        .collect::<Vec<_>>()
        .join(", ");
    Err(Error::invalid_input(format!(
        "cannot read columns of differing lengths together ({columns}); \
         read each column (or equal-length group) separately"
    )))
}

impl FileReader {
    pub(crate) fn base_projection(&self) -> &ReaderProjection {
        &self.core.base_projection
    }

    pub(crate) fn full_projection(&self, projection: ReaderProjection) -> PreparedProjection {
        PreparedProjection {
            column_infos: self.metadata.column_infos.clone(),
            decoder_projection: projection,
        }
    }

    pub(crate) async fn read_prepared_tasks(
        &self,
        params: ReadBatchParams,
        batch_size: u32,
        prepared: PreparedProjection,
        read_len: u64,
        filter: FilterExpression,
    ) -> Result<Pin<Box<dyn Stream<Item = ReadBatchTask> + Send>>> {
        self.core
            .read_prepared_tasks(params, batch_size, prepared, read_len, filter)
            .await
    }

    pub fn with_scheduler(&self, scheduler: Arc<dyn EncodingsIo>) -> Self {
        Self {
            core: self.core.with_scheduler(scheduler),
            metadata: self.metadata.clone(),
        }
    }

    /// Returns a clone of this reader whose I/O is additionally recorded into
    /// `stats`, on top of the scheduler's global accounting.
    ///
    /// All cached metadata is shared with `self`, so no file is re-opened and
    /// only a few `Arc` clones are performed.  If the underlying I/O service
    /// does not support per-scope statistics (e.g. an in-memory scheduler), the
    /// returned reader is an ordinary, uninstrumented clone.
    pub fn with_io_stats(
        &self,
        stats: Arc<dyn lance_core::utils::io_stats::IoStatsRecorder>,
    ) -> Self {
        match self.core.scheduler.with_io_stats(stats) {
            Some(scheduler) => self.with_scheduler(scheduler),
            None => self.clone(),
        }
    }

    pub fn num_rows(&self) -> u64 {
        self.core.num_rows()
    }

    /// The number of rows stored in a single physical column.
    ///
    /// For ordinary (rectangular) files every column has the same length, equal
    /// to [`num_rows`](Self::num_rows). Files written with
    /// [`FileWriter::write_column`](crate::writer::FileWriter::write_column)
    /// may have columns of differing lengths; this returns the length of one
    /// such column, derived by summing its pages' row counts. Errors if
    /// `column_index` is out of bounds.
    pub fn column_num_rows(&self, column_index: usize) -> Result<u64> {
        let column = self
            .metadata
            .column_metadatas
            .get(column_index)
            .ok_or_else(|| {
                Error::invalid_input(format!(
                    "column index {} is out of bounds (file has {} columns)",
                    column_index,
                    self.metadata.column_metadatas.len()
                ))
            })?;
        Ok(column.pages.iter().map(|page| page.length).sum())
    }

    pub fn metadata(&self) -> &Arc<CachedFileMetadata> {
        &self.metadata
    }

    fn statistics_from_column_metadata(
        column_metadatas: &[pbfile::ColumnMetadata],
    ) -> FileStatistics {
        let column_stats = column_metadatas
            .iter()
            .map(|col_metadata| {
                let num_pages = col_metadata.pages.len();
                let size_bytes = col_metadata
                    .pages
                    .iter()
                    .map(|page| page.buffer_sizes.iter().sum::<u64>())
                    .sum::<u64>();
                ColumnStatistics {
                    num_pages,
                    size_bytes,
                }
            })
            .collect();

        FileStatistics {
            columns: column_stats,
        }
    }

    pub fn file_statistics(&self) -> FileStatistics {
        Self::statistics_from_column_metadata(&self.metadata().column_metadatas)
    }

    pub async fn read_global_buffer(&self, index: u32) -> Result<Bytes> {
        self.core.read_global_buffer(index).await
    }

    async fn read_tail(scheduler: &FileScheduler) -> Result<(Bytes, u64)> {
        let file_size = scheduler.reader().size().await? as u64;
        let begin = if file_size < scheduler.reader().block_size() as u64 {
            0
        } else {
            file_size - scheduler.reader().block_size() as u64
        };
        let tail_bytes = scheduler.submit_single(begin..file_size, 0).await?;
        Ok((tail_bytes, file_size))
    }

    async fn read_range_from_tail_or_scheduler(
        tail_bytes: &Bytes,
        tail_offset: u64,
        scheduler: &FileScheduler,
        range: Range<u64>,
    ) -> Result<Bytes> {
        let tail_end = tail_offset + tail_bytes.len() as u64;
        if range.start >= tail_offset && range.end <= tail_end {
            let rel_start = (range.start - tail_offset) as usize;
            let rel_end = (range.end - tail_offset) as usize;
            Ok(tail_bytes.slice(rel_start..rel_end))
        } else {
            scheduler.submit_single(range, 0).await
        }
    }

    fn retained_global_buffers_from_tail(
        gbo_table: &[BufferDescriptor],
        tail_bytes: &Bytes,
        tail_offset: u64,
        file_len: u64,
    ) -> Result<BTreeMap<u32, Bytes>> {
        let tail_end = tail_offset
            .checked_add(tail_bytes.len() as u64)
            .ok_or_else(|| Error::invalid_input_source("Tail byte range overflows".into()))?;
        let mut retained_buffers = BTreeMap::new();
        for (index, buffer) in gbo_table.iter().enumerate().skip(1) {
            let range = buffer.checked_range(index, file_len)?;
            if range.start >= tail_offset && range.end <= tail_end {
                let rel_start = (range.start - tail_offset) as usize;
                let rel_end = (range.end - tail_offset) as usize;
                let bytes = Bytes::copy_from_slice(&tail_bytes[rel_start..rel_end]);
                retained_buffers.insert(index as u32, bytes);
            }
        }
        Ok(retained_buffers)
    }

    // Checks to make sure the footer is written correctly and returns the
    // position of the file descriptor (which comes from the footer)
    fn decode_footer(footer_bytes: &Bytes) -> Result<Footer> {
        let len = footer_bytes.len();
        if len < FOOTER_LEN {
            return Err(Error::invalid_input(format!(
                "does not have sufficient data, len: {}, bytes: {:?}",
                len, footer_bytes
            )));
        }
        let mut cursor = Cursor::new(footer_bytes.slice(len - FOOTER_LEN..));

        let column_meta_start = cursor.read_u64::<LittleEndian>()?;
        let column_meta_offsets_start = cursor.read_u64::<LittleEndian>()?;
        let global_buff_offsets_start = cursor.read_u64::<LittleEndian>()?;
        let num_global_buffers = cursor.read_u32::<LittleEndian>()?;
        let num_columns = cursor.read_u32::<LittleEndian>()?;
        let major_version = cursor.read_u16::<LittleEndian>()?;
        let minor_version = cursor.read_u16::<LittleEndian>()?;

        let magic_bytes = footer_bytes.slice(len - 4..);
        if magic_bytes.as_ref() != MAGIC {
            return Err(Error::invalid_input(format!(
                "file does not appear to be a Lance file (invalid magic: {:?})",
                MAGIC
            )));
        }
        Ok(Footer {
            column_meta_start,
            column_meta_offsets_start,
            global_buff_offsets_start,
            num_global_buffers,
            num_columns,
            major_version,
            minor_version,
        })
    }

    fn current_file_version(footer: &Footer) -> Result<ConcreteFileVersion> {
        let version =
            ConcreteFileVersion::from_footer_numbers(footer.major_version, footer.minor_version)?;
        match version {
            ConcreteFileVersion::V1 => Err(Error::version_conflict(
                "Attempt to use the lance v2 reader to read a legacy file".to_string(),
                footer.major_version,
                footer.minor_version,
            )),
            ConcreteFileVersion::V2_0
            | ConcreteFileVersion::V2_1
            | ConcreteFileVersion::V2_2
            | ConcreteFileVersion::V2_3 => Ok(version),
        }
    }

    // TODO: Once we have coalesced I/O we should only read the column metadatas that we need
    fn read_all_column_metadata(
        column_metadata_bytes: Bytes,
        footer: &Footer,
    ) -> Result<Vec<pbfile::ColumnMetadata>> {
        let column_metadata_start = footer.column_meta_start;
        // cmo == column_metadata_offsets
        let cmo_table_size = 16 * footer.num_columns as usize;
        if column_metadata_bytes.len() < cmo_table_size {
            return Err(Error::invalid_input(format!(
                "column metadata region has {} bytes but CMO table needs {} bytes for {} columns",
                column_metadata_bytes.len(),
                cmo_table_size,
                footer.num_columns
            )));
        }
        let cmo_table = column_metadata_bytes.slice(column_metadata_bytes.len() - cmo_table_size..);
        let column_metadata_offsets = Self::decode_cmo_table(cmo_table, footer)?;

        column_metadata_offsets
            .iter()
            .map(|(position, length)| {
                let normalized_position = (*position - column_metadata_start) as usize;
                let normalized_end = normalized_position + (*length as usize);
                Ok(pbfile::ColumnMetadata::decode(
                    &column_metadata_bytes[normalized_position..normalized_end],
                )?)
            })
            .collect::<Result<Vec<_>>>()
    }

    fn decode_cmo_table(cmo_table: Bytes, footer: &Footer) -> Result<Arc<[(u64, u64)]>> {
        let expected_size = 16 * footer.num_columns as usize;
        if cmo_table.len() != expected_size {
            return Err(Error::invalid_input(format!(
                "column metadata offset table has {} bytes but expected {} bytes for {} columns",
                cmo_table.len(),
                expected_size,
                footer.num_columns
            )));
        }

        let mut offsets = Vec::with_capacity(footer.num_columns as usize);
        for col_idx in 0..footer.num_columns {
            let offset = (col_idx * 16) as usize;
            let position = LittleEndian::read_u64(&cmo_table[offset..offset + 8]);
            let length = LittleEndian::read_u64(&cmo_table[offset + 8..offset + 16]);
            let end = position.checked_add(length).ok_or_else(|| {
                Error::invalid_input(format!(
                    "column metadata range overflows for column index {}, position={}, length={}",
                    col_idx, position, length
                ))
            })?;
            if position < footer.column_meta_start || end > footer.column_meta_offsets_start {
                return Err(Error::invalid_input(format!(
                    "column metadata range for column index {} is outside metadata region: position={}, length={}, metadata_start={}, cmo_start={}",
                    col_idx,
                    position,
                    length,
                    footer.column_meta_start,
                    footer.column_meta_offsets_start
                )));
            }
            offsets.push((position, length));
        }

        Ok(Arc::from(offsets))
    }

    async fn optimistic_tail_read(
        data: &Bytes,
        start_pos: u64,
        scheduler: &FileScheduler,
        file_len: u64,
    ) -> Result<Bytes> {
        let num_bytes_needed = file_len.checked_sub(start_pos).ok_or_else(|| {
            Error::invalid_input_source(
                format!(
                    "Tail read position {} is outside file of size {}",
                    start_pos, file_len
                )
                .into(),
            )
        })? as usize;
        if data.len() >= num_bytes_needed {
            Ok(data.slice((data.len() - num_bytes_needed)..))
        } else {
            let num_bytes_missing = (num_bytes_needed - data.len()) as u64;
            let start = file_len - num_bytes_needed as u64;
            let missing_bytes = scheduler
                .submit_single(start..start + num_bytes_missing, 0)
                .await?;
            let mut combined = BytesMut::with_capacity(data.len() + num_bytes_missing as usize);
            combined.extend(missing_bytes);
            combined.extend(data);
            Ok(combined.freeze())
        }
    }

    fn do_decode_gbo_table(gbo_bytes: &Bytes, footer: &Footer) -> Result<Vec<BufferDescriptor>> {
        let mut global_bufs_cursor = Cursor::new(gbo_bytes);

        let mut global_buffers = Vec::with_capacity(footer.num_global_buffers as usize);
        for _ in 0..footer.num_global_buffers {
            let buf_pos = global_bufs_cursor.read_u64::<LittleEndian>()?;
            let buf_size = global_bufs_cursor.read_u64::<LittleEndian>()?;
            global_buffers.push(BufferDescriptor {
                position: buf_pos,
                size: buf_size,
            });
        }

        Ok(global_buffers)
    }

    fn validate_gbo_table(
        gbo_table: &[BufferDescriptor],
        file_len: u64,
        version: ConcreteFileVersion,
    ) -> Result<()> {
        versions::validate_global_buffers(version, gbo_table)?;
        for (buffer_index, buffer) in gbo_table.iter().enumerate() {
            buffer.checked_range(buffer_index, file_len)?;
        }
        Ok(())
    }

    async fn decode_gbo_table(
        tail_bytes: &Bytes,
        file_len: u64,
        scheduler: &FileScheduler,
        footer: &Footer,
        version: ConcreteFileVersion,
    ) -> Result<Vec<BufferDescriptor>> {
        // This could, in theory, trigger another IOP but the GBO table should never be large
        // enough for that to happen
        let gbo_bytes = Self::optimistic_tail_read(
            tail_bytes,
            footer.global_buff_offsets_start,
            scheduler,
            file_len,
        )
        .await?;
        let gbo_table = Self::do_decode_gbo_table(&gbo_bytes, footer)?;
        Self::validate_gbo_table(&gbo_table, file_len, version)?;
        Ok(gbo_table)
    }

    fn decode_schema(schema_bytes: Bytes) -> Result<(u64, lance_core::datatypes::Schema)> {
        let file_descriptor = pb::FileDescriptor::decode(schema_bytes)?;
        let pb_schema = file_descriptor.schema.unwrap();
        let num_rows = file_descriptor.length;
        let fields_with_meta = FieldsWithMeta {
            fields: Fields(pb_schema.fields),
            metadata: pb_schema.metadata,
        };
        let schema = Schema::try_from(fields_with_meta)?;
        Ok((num_rows, schema))
    }

    pub(crate) async fn read_raw_metadata_for_dispatch(
        scheduler: &FileScheduler,
    ) -> Result<RawFileMetadataOpen> {
        let (tail_bytes, file_len) = Self::read_tail(scheduler).await?;
        let tail_offset = file_len - tail_bytes.len() as u64;
        let footer = Self::decode_footer(&tail_bytes)?;
        let version =
            ConcreteFileVersion::from_footer_numbers(footer.major_version, footer.minor_version)?;
        if version == ConcreteFileVersion::V1 {
            return Ok(RawFileMetadataOpen::Legacy {
                major_version: footer.major_version,
                minor_version: footer.minor_version,
            });
        }

        let gbo_table =
            Self::decode_gbo_table(&tail_bytes, file_len, scheduler, &footer, version).await?;
        if gbo_table.is_empty() {
            return Err(Error::internal(
                "File did not contain any global buffers, schema expected".to_string(),
            ));
        }
        let schema_start = gbo_table[0].position;
        let schema_size = gbo_table[0].size;
        let num_footer_bytes = file_len.checked_sub(schema_start).ok_or_else(|| {
            Error::invalid_input_source(
                format!(
                    "Schema position {} is outside file of size {}",
                    schema_start, file_len
                )
                .into(),
            )
        })?;
        let all_metadata_bytes =
            Self::optimistic_tail_read(&tail_bytes, schema_start, scheduler, file_len).await?;
        let schema_bytes = all_metadata_bytes.slice(0..schema_size as usize);
        let (num_rows, schema) = Self::decode_schema(schema_bytes)?;

        let column_metadata_start = (footer.column_meta_start - schema_start) as usize;
        let column_metadata_end = (footer.global_buff_offsets_start - schema_start) as usize;
        let column_metadata_bytes =
            all_metadata_bytes.slice(column_metadata_start..column_metadata_end);
        let column_metadatas = Self::read_all_column_metadata(column_metadata_bytes, &footer)?;

        let num_global_buffer_bytes = gbo_table.iter().map(|buf| buf.size).sum::<u64>();
        let num_data_bytes = footer.column_meta_start - num_global_buffer_bytes;
        let num_column_metadata_bytes = footer.global_buff_offsets_start - footer.column_meta_start;
        // The tail read above already pulled in any global buffer that lives within
        // the captured window. Copy those user buffers (index >= 1; the schema at 0
        // is decoded above and never fetched via read_global_buffer) out of the tail
        // so read_global_buffer can serve them without I/O. We copy rather than slice
        // so the much larger tail allocation can be released once decoding is done.
        let retained_global_buffers = Self::retained_global_buffers_from_tail(
            &gbo_table,
            &tail_bytes,
            tail_offset,
            file_len,
        )?;

        Ok(RawFileMetadataOpen::Current {
            version,
            metadata: RawFileMetadata {
                file_schema: Arc::new(schema),
                column_metadatas,
                num_rows,
                file_buffers: gbo_table,
                num_data_bytes,
                num_column_metadata_bytes,
                num_global_buffer_bytes,
                num_footer_bytes,
                footer,
                file_size_bytes: file_len,
                retained_global_buffers,
            },
        })
    }

    async fn read_raw_metadata_index_with_known_schema(
        scheduler: &FileScheduler,
        known_schema: Option<(Arc<Schema>, u64)>,
    ) -> Result<FileMetadataIndex> {
        let (tail_bytes, file_len) = Self::read_tail(scheduler).await?;
        let tail_offset = file_len - tail_bytes.len() as u64;
        let footer = Self::decode_footer(&tail_bytes)?;

        let file_version = Self::current_file_version(&footer)?;

        let gbo_table =
            Self::decode_gbo_table(&tail_bytes, file_len, scheduler, &footer, file_version).await?;
        if gbo_table.is_empty() {
            return Err(Error::internal(
                "File did not contain any global buffers, schema expected".to_string(),
            ));
        }
        let (file_schema, num_rows) = match known_schema {
            Some((file_schema, num_rows)) => (file_schema, num_rows),
            None => {
                let schema_buffer = &gbo_table[0];
                let schema_range = schema_buffer.checked_range(0, file_len)?;
                let schema_bytes = Self::read_range_from_tail_or_scheduler(
                    &tail_bytes,
                    tail_offset,
                    scheduler,
                    schema_range,
                )
                .await?;
                let (num_rows, schema) = Self::decode_schema(schema_bytes)?;
                (Arc::new(schema), num_rows)
            }
        };

        let cmo_table = Self::read_range_from_tail_or_scheduler(
            &tail_bytes,
            tail_offset,
            scheduler,
            footer.column_meta_offsets_start..footer.global_buff_offsets_start,
        )
        .await?;
        let column_metadata_offsets = Self::decode_cmo_table(cmo_table, &footer)?;

        let retained_global_buffers = Self::retained_global_buffers_from_tail(
            &gbo_table,
            &tail_bytes,
            tail_offset,
            file_len,
        )?;

        Ok(FileMetadataIndex {
            file_schema,
            num_rows,
            file_buffers: gbo_table,
            column_metadata_offsets,
            num_columns: footer.num_columns,
            version: file_version,
            file_size_bytes: file_len,
            retained_global_buffers,
        })
    }

    /// Reads the lightweight metadata index from a file.
    ///
    /// This reads the file schema from the schema global buffer. Use
    /// [`Self::read_metadata_index_with_schema`] when the caller already has
    /// the schema and row count from a higher-level metadata source.
    pub(crate) async fn read_raw_metadata_index(
        scheduler: &FileScheduler,
    ) -> Result<FileMetadataIndex> {
        Self::read_raw_metadata_index_with_known_schema(scheduler, None).await
    }

    /// Reads the metadata index without fetching the schema global buffer.
    ///
    /// Use this when the caller already has the file schema and physical row
    /// count from an enclosing metadata layer, such as a dataset manifest.
    pub(crate) async fn read_raw_metadata_index_with_schema(
        scheduler: &FileScheduler,
        file_schema: Arc<Schema>,
        num_rows: u64,
    ) -> Result<FileMetadataIndex> {
        Self::read_raw_metadata_index_with_known_schema(scheduler, Some((file_schema, num_rows)))
            .await
    }

    pub(crate) fn validate_projection(
        projection: &ReaderProjection,
        metadata: &CachedFileMetadata,
    ) -> Result<()> {
        if projection.schema.fields.is_empty() {
            return Err(Error::invalid_input(
                "Attempt to read zero columns from the file, at least one column must be specified"
                    .to_string(),
            ));
        }
        let mut column_indices_seen = BTreeSet::new();
        for column_index in &projection.column_indices {
            if !column_indices_seen.insert(*column_index) {
                return Err(Error::invalid_input(format!(
                    "The projection specified the column index {} more than once",
                    column_index
                )));
            }
            if *column_index >= metadata.column_infos.len() as u32 {
                return Err(Error::invalid_input(format!(
                    "The projection specified the column index {} but there are only {} columns in the file",
                    column_index,
                    metadata.column_infos.len()
                )));
            }
        }
        Ok(())
    }

    // The actual decoder needs all the column infos that make up a type.  In other words, if
    // the first type in the schema is Struct<i32, i32> then the decoder will need 3 column infos.
    //
    // This is a file reader concern because the file reader needs to support late projection of columns
    // and so it will need to figure this out anyways.
    //
    // It's a bit of a tricky process though because the number of column infos may depend on the
    // encoding.  Considering the above example, if we wrote it with a packed encoding, then there would
    // only be a single column in the file (and not 3).
    //
    // At the moment this method words because our rules are simple and we just repeat them here.  See
    // Self::default_projection for a similar problem.  In the future this is something the encodings
    // registry will need to figure out.
    fn collect_columns_from_projection(
        &self,
        _projection: &ReaderProjection,
    ) -> Result<Vec<Arc<ColumnInfo>>> {
        Ok(self.metadata.column_infos.clone())
    }

    #[allow(clippy::too_many_arguments)]
    async fn do_read_range(
        column_infos: Vec<Arc<ColumnInfo>>,
        io: Arc<dyn EncodingsIo>,
        cache: Arc<LanceCache>,
        num_rows: u64,
        decoder_plugins: Arc<DecoderPlugins>,
        range: Range<u64>,
        batch_size: u32,
        projection: ReaderProjection,
        filter: FilterExpression,
        decoder_config: DecoderConfig,
        batch_size_bytes: Option<u64>,
    ) -> Result<BoxStream<'static, ReadBatchTask>> {
        debug!(
            "Reading range {:?} with batch_size {} from file with {} rows and {} columns into schema with {} columns",
            range,
            batch_size,
            num_rows,
            column_infos.len(),
            projection.schema.fields.len(),
        );

        let config = SchedulerDecoderConfig {
            batch_size,
            cache,
            decoder_plugins,
            io,
            decoder_config,
            batch_size_bytes,
        };

        let requested_rows = RequestedRows::Ranges(vec![range]);

        schedule_and_decode(
            column_infos,
            requested_rows,
            filter,
            projection.column_indices,
            projection.schema,
            config,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn do_take_rows(
        column_infos: Vec<Arc<ColumnInfo>>,
        io: Arc<dyn EncodingsIo>,
        cache: Arc<LanceCache>,
        decoder_plugins: Arc<DecoderPlugins>,
        indices: Vec<u64>,
        batch_size: u32,
        projection: ReaderProjection,
        filter: FilterExpression,
        decoder_config: DecoderConfig,
        batch_size_bytes: Option<u64>,
    ) -> Result<BoxStream<'static, ReadBatchTask>> {
        debug!(
            "Taking {} rows spread across range {}..{} with batch_size {} from columns {:?}",
            indices.len(),
            indices[0],
            indices[indices.len() - 1],
            batch_size,
            column_infos.iter().map(|ci| ci.index).collect::<Vec<_>>()
        );

        let config = SchedulerDecoderConfig {
            batch_size,
            cache,
            decoder_plugins,
            io,
            decoder_config,
            batch_size_bytes,
        };

        let requested_rows = RequestedRows::Indices(indices);

        schedule_and_decode(
            column_infos,
            requested_rows,
            filter,
            projection.column_indices,
            projection.schema,
            config,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn do_read_ranges(
        column_infos: Vec<Arc<ColumnInfo>>,
        io: Arc<dyn EncodingsIo>,
        cache: Arc<LanceCache>,
        decoder_plugins: Arc<DecoderPlugins>,
        ranges: Vec<Range<u64>>,
        batch_size: u32,
        projection: ReaderProjection,
        filter: FilterExpression,
        decoder_config: DecoderConfig,
        batch_size_bytes: Option<u64>,
    ) -> Result<BoxStream<'static, ReadBatchTask>> {
        let num_rows = ranges.iter().map(|r| r.end - r.start).sum::<u64>();
        debug!(
            "Taking {} ranges ({} rows) spread across range {}..{} with batch_size {} from columns {:?}",
            ranges.len(),
            num_rows,
            ranges[0].start,
            ranges[ranges.len() - 1].end,
            batch_size,
            column_infos.iter().map(|ci| ci.index).collect::<Vec<_>>()
        );

        let config = SchedulerDecoderConfig {
            batch_size,
            cache,
            decoder_plugins,
            io,
            decoder_config,
            batch_size_bytes,
        };

        let requested_rows = RequestedRows::Ranges(ranges);

        schedule_and_decode(
            column_infos,
            requested_rows,
            filter,
            projection.column_indices,
            projection.schema,
            config,
        )
        .await
    }

    fn take_rows_blocking(
        &self,
        indices: Vec<u64>,
        batch_size: u32,
        projection: ReaderProjection,
        filter: FilterExpression,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        let column_infos = self.collect_columns_from_projection(&projection)?;
        debug!(
            "Taking {} rows spread across range {}..{} with batch_size {} from columns {:?}",
            indices.len(),
            indices[0],
            indices[indices.len() - 1],
            batch_size,
            column_infos.iter().map(|ci| ci.index).collect::<Vec<_>>()
        );

        let config = SchedulerDecoderConfig {
            batch_size,
            cache: self.core.cache.clone(),
            decoder_plugins: self.core.decoder_plugins.clone(),
            io: self.core.scheduler.clone(),
            decoder_config: self.core.options.decoder_config.clone(),
            batch_size_bytes: self.core.options.batch_size_bytes,
        };

        let requested_rows = RequestedRows::Indices(indices);

        schedule_and_decode_blocking(
            column_infos,
            requested_rows,
            filter,
            projection.column_indices,
            projection.schema,
            config,
        )
    }

    fn read_ranges_blocking(
        &self,
        ranges: Vec<Range<u64>>,
        batch_size: u32,
        projection: ReaderProjection,
        filter: FilterExpression,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        let column_infos = self.collect_columns_from_projection(&projection)?;
        let num_rows = ranges.iter().map(|r| r.end - r.start).sum::<u64>();
        debug!(
            "Taking {} ranges ({} rows) spread across range {}..{} with batch_size {} from columns {:?}",
            ranges.len(),
            num_rows,
            ranges[0].start,
            ranges[ranges.len() - 1].end,
            batch_size,
            column_infos.iter().map(|ci| ci.index).collect::<Vec<_>>()
        );

        let config = SchedulerDecoderConfig {
            batch_size,
            cache: self.core.cache.clone(),
            decoder_plugins: self.core.decoder_plugins.clone(),
            io: self.core.scheduler.clone(),
            decoder_config: self.core.options.decoder_config.clone(),
            batch_size_bytes: self.core.options.batch_size_bytes,
        };

        let requested_rows = RequestedRows::Ranges(ranges);

        schedule_and_decode_blocking(
            column_infos,
            requested_rows,
            filter,
            projection.column_indices,
            projection.schema,
            config,
        )
    }

    fn read_range_blocking(
        &self,
        range: Range<u64>,
        batch_size: u32,
        projection: ReaderProjection,
        filter: FilterExpression,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        let column_infos = self.collect_columns_from_projection(&projection)?;
        let num_rows = self.core.num_rows();

        debug!(
            "Reading range {:?} with batch_size {} from file with {} rows and {} columns into schema with {} columns",
            range,
            batch_size,
            num_rows,
            column_infos.len(),
            projection.schema.fields.len(),
        );

        let config = SchedulerDecoderConfig {
            batch_size,
            cache: self.core.cache.clone(),
            decoder_plugins: self.core.decoder_plugins.clone(),
            io: self.core.scheduler.clone(),
            decoder_config: self.core.options.decoder_config.clone(),
            batch_size_bytes: self.core.options.batch_size_bytes,
        };

        let requested_rows = RequestedRows::Ranges(vec![range]);

        schedule_and_decode_blocking(
            column_infos,
            requested_rows,
            filter,
            projection.column_indices,
            projection.schema,
            config,
        )
    }

    pub(crate) fn read_prepared_blocking(
        &self,
        params: ReadBatchParams,
        batch_size: u32,
        prepared: PreparedProjection,
        read_len: u64,
        filter: FilterExpression,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        let projection = prepared.decoder_projection;
        let verify_bound = |params: &ReadBatchParams, bound: u64, inclusive: bool| {
            if bound > read_len || (bound == read_len && inclusive) {
                Err(Error::invalid_input(format!(
                    "cannot read {params:?} from columns with {read_len} rows"
                )))
            } else {
                Ok(())
            }
        };
        match &params {
            ReadBatchParams::Indices(indices) => {
                for index in indices {
                    match index {
                        None => return Err(Error::invalid_input("Null value in indices array")),
                        Some(index) => verify_bound(&params, index as u64, true)?,
                    }
                }
                let indices = indices.iter().map(|index| index.unwrap() as u64).collect();
                self.take_rows_blocking(indices, batch_size, projection, filter)
            }
            ReadBatchParams::Range(range) => {
                verify_bound(&params, range.end as u64, false)?;
                self.read_range_blocking(
                    range.start as u64..range.end as u64,
                    batch_size,
                    projection,
                    filter,
                )
            }
            ReadBatchParams::Ranges(ranges) => {
                let mut ranges_u64 = Vec::with_capacity(ranges.len());
                for range in ranges.as_ref() {
                    verify_bound(&params, range.end, false)?;
                    ranges_u64.push(range.start..range.end);
                }
                self.read_ranges_blocking(ranges_u64, batch_size, projection, filter)
            }
            ReadBatchParams::RangeFrom(range) => {
                verify_bound(&params, range.start as u64, true)?;
                self.read_range_blocking(
                    range.start as u64..read_len,
                    batch_size,
                    projection,
                    filter,
                )
            }
            ReadBatchParams::RangeTo(range) => {
                verify_bound(&params, range.end as u64, false)?;
                self.read_range_blocking(0..range.end as u64, batch_size, projection, filter)
            }
            ReadBatchParams::RangeFull => {
                self.read_range_blocking(0..read_len, batch_size, projection, filter)
            }
        }
    }

    pub fn schema(&self) -> &Arc<Schema> {
        self.core.schema()
    }
}

impl FileMetadataProvider {
    pub(crate) fn version(&self) -> ConcreteFileVersion {
        match self {
            Self::Full(metadata) => metadata.version,
            Self::Indexed(metadata_index) => metadata_index.version,
        }
    }

    pub(crate) fn num_rows(&self) -> u64 {
        match self {
            Self::Full(metadata) => metadata.num_rows,
            Self::Indexed(metadata_index) => metadata_index.num_rows,
        }
    }

    pub(crate) fn schema(&self) -> &Arc<Schema> {
        match self {
            Self::Full(metadata) => &metadata.file_schema,
            Self::Indexed(metadata_index) => &metadata_index.file_schema,
        }
    }

    pub(crate) fn file_buffers(&self) -> &Vec<BufferDescriptor> {
        match self {
            Self::Full(metadata) => &metadata.file_buffers,
            Self::Indexed(metadata_index) => &metadata_index.file_buffers,
        }
    }

    pub(crate) fn retained_global_buffers(&self) -> &BTreeMap<u32, Bytes> {
        match self {
            Self::Full(metadata) => &metadata.retained_global_buffers,
            Self::Indexed(metadata_index) => &metadata_index.retained_global_buffers,
        }
    }

    fn file_size(&self) -> u64 {
        match self {
            Self::Full(metadata) => metadata.file_size_bytes,
            Self::Indexed(metadata_index) => metadata_index.file_size_bytes,
        }
    }

    pub(crate) fn file_statistics(&self) -> Option<FileStatistics> {
        let metadata = match self {
            Self::Full(metadata) => metadata,
            Self::Indexed(_) => return None,
        };
        Some(FileReader::statistics_from_column_metadata(
            &metadata.column_metadatas,
        ))
    }

    pub(crate) fn projection_matches_indexed_metadata(projection: &ReaderProjection) -> bool {
        if projection.schema.fields.is_empty() {
            return false;
        }

        projection
            .schema
            .fields
            .iter()
            .try_fold(0usize, |count, field| {
                count.checked_add(indexed_projection_column_count(field)?)
            })
            == Some(projection.column_indices.len())
    }

    pub(crate) fn validate_indexed_projection_structure(
        projection: &ReaderProjection,
        metadata_index: &FileMetadataIndex,
    ) -> Result<()> {
        if projection.schema.fields.is_empty() {
            return Err(Error::invalid_input(
                "Attempt to read zero columns from the file, at least one column must be specified"
                    .to_string(),
            ));
        }
        let mut column_indices_seen = BTreeSet::new();
        for column_index in &projection.column_indices {
            if !column_indices_seen.insert(*column_index) {
                return Err(Error::invalid_input(format!(
                    "The projection specified the column index {} more than once",
                    column_index
                )));
            }
            if *column_index >= metadata_index.num_columns {
                return Err(Error::invalid_input(format!(
                    "The projection specified the column index {} but there are only {} columns in the file",
                    column_index, metadata_index.num_columns
                )));
            }
        }
        Ok(())
    }

    pub(crate) fn indexed_projection_error(
        projection: &ReaderProjection,
        metadata_index: &FileMetadataIndex,
    ) -> Error {
        Error::not_supported(format!(
            "lazy column metadata loading requires a V2.1+ ordinary structural projection without blob or packed-struct fields whose physical-column count matches the projection; got file version {:?}, {} schema fields, and {} column indices",
            metadata_index.version,
            projection.schema.fields.len(),
            projection.column_indices.len()
        ))
    }

    fn column_metadata_range(
        metadata_index: &FileMetadataIndex,
        column_index: u32,
    ) -> Result<Range<u64>> {
        let (position, length) = metadata_index
            .column_metadata_offsets
            .get(column_index as usize)
            .copied()
            .ok_or_else(|| {
                Error::invalid_input(format!(
                    "The projection specified the column index {} but there are only {} columns in the file",
                    column_index, metadata_index.num_columns
                ))
            })?;
        let end = position.checked_add(length).ok_or_else(|| {
            Error::invalid_input(format!(
                "column metadata range overflows for column index {}, position={}, length={}",
                column_index, position, length
            ))
        })?;
        Ok(position..end)
    }

    pub(crate) async fn load_indexed_column_infos<F>(
        metadata_index: &FileMetadataIndex,
        io: &Arc<dyn EncodingsIo>,
        cache: &Arc<LanceCache>,
        column_indices: &[u32],
        decode_column: F,
    ) -> Result<Vec<Arc<ColumnInfo>>>
    where
        F: Fn(u32, &pbfile::ColumnMetadata) -> Result<Arc<ColumnInfo>>,
    {
        let mut column_infos = vec![None; column_indices.len()];
        let mut missing_columns = Vec::new();

        for (result_index, column_index) in column_indices.iter().copied().enumerate() {
            let cache_key = ColumnMetadataCacheKey { column_index };
            if let Some(cached) = cache.get_with_key(&cache_key).await {
                column_infos[result_index] = Some(cached.column_info.clone());
            } else {
                let range = Self::column_metadata_range(metadata_index, column_index)?;
                missing_columns.push((result_index, column_index, range));
            }
        }

        missing_columns.sort_by_key(|(_, _, range)| range.start);
        if !missing_columns.is_empty() {
            let ranges = missing_columns
                .iter()
                .map(|(_, _, range)| range.clone())
                .collect::<Vec<_>>();
            let metadata_bytes = io.submit_request(ranges, 0).await?;
            for ((result_index, column_index, _), bytes) in
                missing_columns.into_iter().zip(metadata_bytes)
            {
                let column_metadata = pbfile::ColumnMetadata::decode(bytes)?;
                let column_info = decode_column(column_index, &column_metadata)?;
                let cached = Arc::new(CachedColumnMetadata {
                    column_metadata,
                    column_info: column_info.clone(),
                });
                let cache_key = ColumnMetadataCacheKey { column_index };
                cache.insert_with_key(&cache_key, cached).await;
                column_infos[result_index] = Some(column_info);
            }
        }

        column_infos
            .into_iter()
            .enumerate()
            .map(|(idx, column_info)| {
                column_info.ok_or_else(|| {
                    Error::internal(format!(
                        "lazy metadata loader did not load requested projection column at position {}",
                        idx
                    ))
                })
            })
            .collect()
    }
}

impl DecodeEngine {
    pub(crate) fn try_new(
        scheduler: Arc<dyn EncodingsIo>,
        base_projection: ReaderProjection,
        decoder_plugins: Arc<DecoderPlugins>,
        metadata_provider: FileMetadataProvider,
        read_projection: Arc<dyn ReadProjection>,
        cache: Arc<LanceCache>,
        options: FileReaderOptions,
    ) -> Result<Self> {
        Ok(Self {
            scheduler,
            base_projection,
            metadata_provider,
            read_projection,
            decoder_plugins,
            cache,
            options,
        })
    }

    pub(crate) fn with_scheduler(&self, scheduler: Arc<dyn EncodingsIo>) -> Self {
        Self {
            scheduler,
            base_projection: self.base_projection.clone(),
            metadata_provider: self.metadata_provider.clone(),
            read_projection: self.read_projection.clone(),
            decoder_plugins: self.decoder_plugins.clone(),
            cache: self.cache.clone(),
            options: self.options.clone(),
        }
    }

    fn num_rows(&self) -> u64 {
        self.metadata_provider.num_rows()
    }

    fn schema(&self) -> &Arc<Schema> {
        self.metadata_provider.schema()
    }

    pub(crate) async fn read_global_buffer(&self, index: u32) -> Result<Bytes> {
        let file_buffers = self.metadata_provider.file_buffers();
        let buffer_desc = file_buffers.get(index as usize).ok_or_else(|| {
            Error::invalid_input(format!(
                "request for global buffer at index {} but there were only {} global buffers in the file",
                index,
                file_buffers.len()
            ))
        })?;

        if let Some(bytes) = self.metadata_provider.retained_global_buffers().get(&index) {
            return Ok(bytes.clone());
        }

        let bytes = self
            .scheduler
            .submit_request(
                vec![
                    buffer_desc
                        .checked_range(index as usize, self.metadata_provider.file_size())?,
                ],
                0,
            )
            .await?;
        bytes.into_iter().next().ok_or_else(|| {
            Error::internal(format!(
                "global buffer read for index {} returned no bytes",
                index
            ))
        })
    }

    async fn read_range(
        &self,
        range: Range<u64>,
        batch_size: u32,
        prepared: PreparedProjection,
        filter: FilterExpression,
    ) -> Result<BoxStream<'static, ReadBatchTask>> {
        FileReader::do_read_range(
            prepared.column_infos,
            self.scheduler.clone(),
            self.cache.clone(),
            self.num_rows(),
            self.decoder_plugins.clone(),
            range,
            batch_size,
            prepared.decoder_projection,
            filter,
            self.options.decoder_config.clone(),
            self.options.batch_size_bytes,
        )
        .await
    }

    async fn take_rows(
        &self,
        indices: Vec<u64>,
        batch_size: u32,
        prepared: PreparedProjection,
    ) -> Result<BoxStream<'static, ReadBatchTask>> {
        FileReader::do_take_rows(
            prepared.column_infos,
            self.scheduler.clone(),
            self.cache.clone(),
            self.decoder_plugins.clone(),
            indices,
            batch_size,
            prepared.decoder_projection,
            FilterExpression::no_filter(),
            self.options.decoder_config.clone(),
            self.options.batch_size_bytes,
        )
        .await
    }

    async fn read_ranges(
        &self,
        ranges: Vec<Range<u64>>,
        batch_size: u32,
        prepared: PreparedProjection,
        filter: FilterExpression,
    ) -> Result<BoxStream<'static, ReadBatchTask>> {
        FileReader::do_read_ranges(
            prepared.column_infos,
            self.scheduler.clone(),
            self.cache.clone(),
            self.decoder_plugins.clone(),
            ranges,
            batch_size,
            prepared.decoder_projection,
            filter,
            self.options.decoder_config.clone(),
            self.options.batch_size_bytes,
        )
        .await
    }

    pub(crate) async fn read_prepared_tasks(
        &self,
        params: ReadBatchParams,
        batch_size: u32,
        prepared: PreparedProjection,
        read_len: u64,
        filter: FilterExpression,
    ) -> Result<Pin<Box<dyn Stream<Item = ReadBatchTask> + Send>>> {
        let verify_bound = |params: &ReadBatchParams, bound: u64, inclusive: bool| {
            if bound > read_len || (bound == read_len && inclusive) {
                Err(Error::invalid_input(format!(
                    "cannot read {params:?} from columns with {read_len} rows"
                )))
            } else {
                Ok(())
            }
        };
        match &params {
            ReadBatchParams::Indices(indices) => {
                for idx in indices {
                    match idx {
                        None => {
                            return Err(Error::invalid_input("Null value in indices array"));
                        }
                        Some(idx) => {
                            verify_bound(&params, idx as u64, true)?;
                        }
                    }
                }
                let indices = indices.iter().map(|idx| idx.unwrap() as u64).collect();
                self.take_rows(indices, batch_size, prepared).await
            }
            ReadBatchParams::Range(range) => {
                verify_bound(&params, range.end as u64, false)?;
                self.read_range(
                    range.start as u64..range.end as u64,
                    batch_size,
                    prepared,
                    filter,
                )
                .await
            }
            ReadBatchParams::Ranges(ranges) => {
                let mut ranges_u64 = Vec::with_capacity(ranges.len());
                for range in ranges.as_ref() {
                    verify_bound(&params, range.end, false)?;
                    ranges_u64.push(range.start..range.end);
                }
                self.read_ranges(ranges_u64, batch_size, prepared, filter)
                    .await
            }
            ReadBatchParams::RangeFrom(range) => {
                verify_bound(&params, range.start as u64, true)?;
                self.read_range(range.start as u64..read_len, batch_size, prepared, filter)
                    .await
            }
            ReadBatchParams::RangeTo(range) => {
                verify_bound(&params, range.end as u64, false)?;
                self.read_range(0..range.end as u64, batch_size, prepared, filter)
                    .await
            }
            ReadBatchParams::RangeFull => {
                self.read_range(0..read_len, batch_size, prepared, filter)
                    .await
            }
        }
    }
}

impl ProjectedFileReader {
    pub(crate) fn base_projection(&self) -> &ReaderProjection {
        &self.core.base_projection
    }

    pub(crate) async fn read_prepared_tasks(
        &self,
        params: ReadBatchParams,
        batch_size: u32,
        prepared: PreparedProjection,
        read_len: u64,
        filter: FilterExpression,
    ) -> Result<Pin<Box<dyn Stream<Item = ReadBatchTask> + Send>>> {
        self.core
            .read_prepared_tasks(params, batch_size, prepared, read_len, filter)
            .await
    }

    /// Returns a clone of this reader using a different scheduler.
    pub fn with_scheduler(&self, scheduler: Arc<dyn EncodingsIo>) -> Self {
        Self {
            core: self.core.with_scheduler(scheduler),
        }
    }

    /// Returns the number of rows in the file.
    pub fn num_rows(&self) -> u64 {
        self.core.num_rows()
    }

    /// Returns the file schema visible to this reader.
    pub fn schema(&self) -> &Arc<Schema> {
        self.core.schema()
    }

    /// Returns file statistics when this reader has full file metadata.
    pub fn file_statistics(&self) -> Option<FileStatistics> {
        self.core.metadata_provider.file_statistics()
    }

    #[cfg(test)]
    pub(crate) fn metadata_index(&self) -> Option<&Arc<FileMetadataIndex>> {
        match &self.core.metadata_provider {
            FileMetadataProvider::Indexed(metadata_index) => Some(metadata_index),
            FileMetadataProvider::Full(_) => None,
        }
    }

    /// Reads a global buffer by index.
    pub async fn read_global_buffer(&self, index: u32) -> Result<Bytes> {
        self.core.read_global_buffer(index).await
    }
}

impl FileReader {
    #[cfg(test)]
    fn scheduler(&self) -> Arc<dyn EncodingsIo> {
        self.core.scheduler.clone()
    }

    pub async fn try_open(
        scheduler: FileScheduler,
        base_projection: Option<ReaderProjection>,
        decoder_plugins: Arc<DecoderPlugins>,
        cache: &LanceCache,
        options: FileReaderOptions,
    ) -> Result<Self> {
        match Self::try_open_for_dispatch(
            scheduler,
            base_projection,
            decoder_plugins,
            cache,
            options,
        )
        .await?
        {
            versions::OpenedFileReader::V1 {
                major_version,
                minor_version,
            } => Err(Error::version_conflict(
                "Attempt to use the Lance current-format reader to read a v1 file".to_string(),
                major_version,
                minor_version,
            )),
            versions::OpenedFileReader::Current(reader) => Ok(reader),
        }
    }

    pub(crate) async fn try_open_for_dispatch(
        scheduler: FileScheduler,
        base_projection: Option<ReaderProjection>,
        decoder_plugins: Arc<DecoderPlugins>,
        cache: &LanceCache,
        options: FileReaderOptions,
    ) -> Result<versions::OpenedFileReader> {
        let metadata = match Self::read_raw_metadata_for_dispatch(&scheduler).await? {
            RawFileMetadataOpen::Legacy {
                major_version,
                minor_version,
            } => {
                return Ok(versions::OpenedFileReader::V1 {
                    major_version,
                    minor_version,
                });
            }
            RawFileMetadataOpen::Current { version, metadata } => {
                Arc::new(versions::finish_metadata(version, metadata)?)
            }
        };
        let path = scheduler.reader().path().clone();
        let io = Arc::new(
            LanceEncodingsIo::new(scheduler).with_read_chunk_size(options.read_chunk_size),
        );
        Self::try_open_with_file_metadata(
            io,
            path,
            base_projection,
            decoder_plugins,
            metadata,
            cache,
            options,
        )
        .await
        .map(versions::OpenedFileReader::Current)
    }

    pub async fn try_open_with_file_metadata(
        scheduler: Arc<dyn EncodingsIo>,
        path: Path,
        base_projection: Option<ReaderProjection>,
        decoder_plugins: Arc<DecoderPlugins>,
        metadata: Arc<CachedFileMetadata>,
        cache: &LanceCache,
        options: FileReaderOptions,
    ) -> Result<Self> {
        if metadata.version == ConcreteFileVersion::V1 {
            return Err(Error::version_conflict(
                "Attempt to use the Lance current-format reader with v1 metadata".to_string(),
                metadata.major_version,
                metadata.minor_version,
            ));
        }
        let read_projection = versions::read_projection(metadata.version)?;
        let has_explicit_projection = base_projection.is_some();
        let base_projection = base_projection.unwrap_or_else(|| {
            versions::reader_projection_from_whole_schema(&metadata.file_schema, metadata.version)
        });
        if has_explicit_projection {
            Self::validate_projection(&base_projection, &metadata)?;
        }
        let cache = Arc::new(cache.with_key_prefix(path.as_ref()));
        let core = DecodeEngine::try_new(
            scheduler,
            base_projection,
            decoder_plugins,
            FileMetadataProvider::Full(metadata.clone()),
            read_projection,
            cache,
            options,
        )?;
        Ok(Self { core, metadata })
    }

    pub async fn read_all_metadata(scheduler: &FileScheduler) -> Result<CachedFileMetadata> {
        match Self::read_raw_metadata_for_dispatch(scheduler).await? {
            RawFileMetadataOpen::Legacy {
                major_version,
                minor_version,
            } => Err(Error::version_conflict(
                "Attempt to use the Lance current-format reader to read v1 metadata".to_string(),
                major_version,
                minor_version,
            )),
            RawFileMetadataOpen::Current { version, metadata } => {
                versions::finish_metadata(version, metadata)
            }
        }
    }

    pub async fn read_metadata_index(scheduler: &FileScheduler) -> Result<FileMetadataIndex> {
        let index = Self::read_raw_metadata_index(scheduler).await?;
        versions::finish_metadata_index(index)
    }

    pub async fn read_metadata_index_with_schema(
        scheduler: &FileScheduler,
        file_schema: Arc<Schema>,
        num_rows: u64,
    ) -> Result<FileMetadataIndex> {
        let index =
            Self::read_raw_metadata_index_with_schema(scheduler, file_schema, num_rows).await?;
        versions::finish_metadata_index(index)
    }

    pub fn version(&self) -> ConcreteFileVersion {
        self.metadata.version
    }

    async fn prepare(&self, projection: ReaderProjection) -> Result<(PreparedProjection, u64)> {
        self.core
            .read_projection
            .prepare(
                &self.core.metadata_provider,
                &projection,
                &self.core.scheduler,
                &self.core.cache,
            )
            .await
    }

    pub async fn read_tasks(
        &self,
        params: ReadBatchParams,
        batch_size: u32,
        projection: Option<ReaderProjection>,
        filter: FilterExpression,
    ) -> Result<Pin<Box<dyn Stream<Item = ReadBatchTask> + Send>>> {
        let projection = projection.unwrap_or_else(|| self.base_projection().clone());
        let (prepared, read_len) = self.prepare(projection).await?;
        self.read_prepared_tasks(params, batch_size, prepared, read_len, filter)
            .await
    }

    pub async fn read_stream_projected(
        &self,
        params: ReadBatchParams,
        batch_size: u32,
        batch_readahead: u32,
        projection: ReaderProjection,
        filter: FilterExpression,
    ) -> Result<Pin<Box<dyn RecordBatchStream>>> {
        let schema = projection.schema.clone();
        let tasks = self
            .read_tasks(params, batch_size, Some(projection), filter)
            .await?;
        Ok(tasks_to_record_batch_stream(schema, tasks, batch_readahead))
    }

    pub fn read_stream_projected_blocking(
        &self,
        params: ReadBatchParams,
        batch_size: u32,
        projection: Option<ReaderProjection>,
        filter: FilterExpression,
    ) -> Result<Box<dyn RecordBatchReader + Send + 'static>> {
        let projection = projection.unwrap_or_else(|| self.base_projection().clone());
        Self::validate_projection(&projection, self.metadata())?;
        let prepared = self.full_projection(projection);
        let read_len = self.core.read_projection.read_length(&prepared)?;
        self.read_prepared_blocking(params, batch_size, prepared, read_len, filter)
    }

    pub async fn read_stream(
        &self,
        params: ReadBatchParams,
        batch_size: u32,
        batch_readahead: u32,
        filter: FilterExpression,
    ) -> Result<Pin<Box<dyn RecordBatchStream>>> {
        self.read_stream_projected(
            params,
            batch_size,
            batch_readahead,
            self.base_projection().clone(),
            filter,
        )
        .await
    }
}

impl ProjectedFileReader {
    pub async fn try_open(
        scheduler: FileScheduler,
        base_projection: Option<ReaderProjection>,
        decoder_plugins: Arc<DecoderPlugins>,
        cache: &LanceCache,
        options: FileReaderOptions,
    ) -> Result<Self> {
        let base_projection = base_projection.ok_or_else(|| {
            Error::invalid_input("ProjectedReader requires an explicit base projection")
        })?;
        let metadata_index = Arc::new(FileReader::read_metadata_index(&scheduler).await?);
        let path = scheduler.reader().path().clone();
        let io = Arc::new(
            LanceEncodingsIo::new(scheduler).with_read_chunk_size(options.read_chunk_size),
        );
        Self::try_open_with_metadata_index(
            io,
            path,
            Some(base_projection),
            decoder_plugins,
            metadata_index,
            cache,
            options,
        )
        .await
    }

    pub async fn try_open_with_metadata_index(
        scheduler: Arc<dyn EncodingsIo>,
        path: Path,
        base_projection: Option<ReaderProjection>,
        decoder_plugins: Arc<DecoderPlugins>,
        metadata_index: Arc<FileMetadataIndex>,
        cache: &LanceCache,
        options: FileReaderOptions,
    ) -> Result<Self> {
        if metadata_index.version == ConcreteFileVersion::V1 {
            return Err(Error::version_conflict(
                "Attempt to use the Lance projected current-format reader with v1 metadata"
                    .to_string(),
                0,
                2,
            ));
        }
        let base_projection = base_projection.ok_or_else(|| {
            Error::invalid_input("ProjectedReader requires an explicit base projection")
        })?;
        let read_projection = versions::read_projection(metadata_index.version)?;
        read_projection.validate_indexed(&base_projection, &metadata_index)?;
        let cache = Arc::new(cache.with_key_prefix(path.as_ref()));
        let core = DecodeEngine::try_new(
            scheduler,
            base_projection,
            decoder_plugins,
            FileMetadataProvider::Indexed(metadata_index),
            read_projection,
            cache,
            options,
        )?;
        Ok(Self { core })
    }

    pub async fn try_open_with_file_metadata(
        scheduler: Arc<dyn EncodingsIo>,
        path: Path,
        base_projection: Option<ReaderProjection>,
        decoder_plugins: Arc<DecoderPlugins>,
        metadata: Arc<CachedFileMetadata>,
        cache: &LanceCache,
        options: FileReaderOptions,
    ) -> Result<Self> {
        if metadata.version == ConcreteFileVersion::V1 {
            return Err(Error::version_conflict(
                "Attempt to use the Lance projected current-format reader with v1 metadata"
                    .to_string(),
                metadata.major_version,
                metadata.minor_version,
            ));
        }
        let read_projection = versions::read_projection(metadata.version)?;
        let has_explicit_projection = base_projection.is_some();
        let base_projection = base_projection.unwrap_or_else(|| {
            versions::reader_projection_from_whole_schema(&metadata.file_schema, metadata.version)
        });
        if has_explicit_projection {
            FileReader::validate_projection(&base_projection, &metadata)?;
        }
        let cache = Arc::new(cache.with_key_prefix(path.as_ref()));
        let core = DecodeEngine::try_new(
            scheduler,
            base_projection,
            decoder_plugins,
            FileMetadataProvider::Full(metadata),
            read_projection,
            cache,
            options,
        )?;
        Ok(Self { core })
    }

    pub fn version(&self) -> ConcreteFileVersion {
        self.core.metadata_provider.version()
    }

    pub async fn read_tasks(
        &self,
        params: ReadBatchParams,
        batch_size: u32,
        projection: Option<ReaderProjection>,
        filter: FilterExpression,
    ) -> Result<Pin<Box<dyn Stream<Item = ReadBatchTask> + Send>>> {
        let projection = projection.unwrap_or_else(|| self.base_projection().clone());
        let (prepared, read_len) = self
            .core
            .read_projection
            .prepare(
                &self.core.metadata_provider,
                &projection,
                &self.core.scheduler,
                &self.core.cache,
            )
            .await?;
        self.read_prepared_tasks(params, batch_size, prepared, read_len, filter)
            .await
    }
}

/// Inspects a page and returns a String describing the page's encoding
pub fn describe_encoding(page: &pbfile::column_metadata::Page) -> String {
    if let Some(encoding) = &page.encoding {
        if let Some(style) = &encoding.location {
            match style {
                pbfile::encoding::Location::Indirect(indirect) => {
                    format!(
                        "IndirectEncoding(pos={},size={})",
                        indirect.buffer_location, indirect.buffer_length
                    )
                }
                pbfile::encoding::Location::Direct(direct) => {
                    let encoding_any =
                        prost_types::Any::decode(Bytes::from(direct.encoding.clone()))
                            .expect("failed to deserialize encoding as protobuf");
                    if encoding_any.type_url == "/lance.encodings.ArrayEncoding" {
                        let encoding = encoding_any.to_msg::<pbenc::ArrayEncoding>();
                        match encoding {
                            Ok(encoding) => {
                                format!("{:#?}", encoding)
                            }
                            Err(err) => {
                                format!("Unsupported(decode_err={})", err)
                            }
                        }
                    } else if encoding_any.type_url == "/lance.encodings21.PageLayout" {
                        let encoding = encoding_any.to_msg::<pbenc21::PageLayout>();
                        match encoding {
                            Ok(encoding) => {
                                format!("{:#?}", encoding)
                            }
                            Err(err) => {
                                format!("Unsupported(decode_err={})", err)
                            }
                        }
                    } else {
                        format!("Unrecognized(type_url={})", encoding_any.type_url)
                    }
                }
                pbfile::encoding::Location::None(_) => "NoEncodingDescription".to_string(),
            }
        } else {
            "MISSING STYLE".to_string()
        }
    } else {
        "MISSING".to_string()
    }
}

pub trait EncodedBatchReaderExt {
    fn try_from_mini_lance(bytes: Bytes, schema: &Schema) -> Result<Self>
    where
        Self: Sized;
    fn try_from_self_described_lance(bytes: Bytes) -> Result<Self>
    where
        Self: Sized;
}

impl EncodedBatchReaderExt for EncodedBatch {
    fn try_from_mini_lance(bytes: Bytes, schema: &Schema) -> Result<Self>
    where
        Self: Sized,
    {
        let footer = FileReader::decode_footer(&bytes)?;
        let file_version = FileReader::current_file_version(&footer)?;
        let projection = versions::reader_projection_from_whole_schema(schema, file_version);

        // Next, read the metadata for the columns
        // This is both the column metadata and the CMO table
        let column_metadata_start = footer.column_meta_start as usize;
        let column_metadata_end = footer.global_buff_offsets_start as usize;
        let column_metadata_bytes = bytes.slice(column_metadata_start..column_metadata_end);
        let column_metadatas =
            FileReader::read_all_column_metadata(column_metadata_bytes, &footer)?;

        let page_table = versions::decode_column_metadata(file_version, &column_metadatas)?;

        Ok(Self {
            data: bytes,
            num_rows: page_table
                .first()
                .map(|col| col.page_infos.iter().map(|page| page.num_rows).sum::<u64>())
                .unwrap_or(0),
            page_table,
            top_level_columns: projection.column_indices,
            schema: Arc::new(schema.clone()),
        })
    }

    fn try_from_self_described_lance(bytes: Bytes) -> Result<Self>
    where
        Self: Sized,
    {
        let footer = FileReader::decode_footer(&bytes)?;
        let file_version = FileReader::current_file_version(&footer)?;

        let file_len = bytes.len() as u64;
        let gbo_table = FileReader::do_decode_gbo_table(
            &bytes.slice(footer.global_buff_offsets_start as usize..),
            &footer,
        )?;
        FileReader::validate_gbo_table(&gbo_table, file_len, file_version)?;
        if gbo_table.is_empty() {
            return Err(Error::internal(
                "File did not contain any global buffers, schema expected".to_string(),
            ));
        }
        let schema_range = gbo_table[0].checked_range(0, file_len)?;
        let schema_start = schema_range.start as usize;
        let schema_end = schema_range.end as usize;

        let schema_bytes = bytes.slice(schema_start..schema_end);
        let (_, schema) = FileReader::decode_schema(schema_bytes)?;
        let projection = versions::reader_projection_from_whole_schema(&schema, file_version);

        // Next, read the metadata for the columns
        // This is both the column metadata and the CMO table
        let column_metadata_start = footer.column_meta_start as usize;
        let column_metadata_end = footer.global_buff_offsets_start as usize;
        let column_metadata_bytes = bytes.slice(column_metadata_start..column_metadata_end);
        let column_metadatas =
            FileReader::read_all_column_metadata(column_metadata_bytes, &footer)?;

        let page_table = versions::decode_column_metadata(file_version, &column_metadatas)?;

        Ok(Self {
            data: bytes,
            num_rows: page_table
                .first()
                .map(|col| col.page_infos.iter().map(|page| page.num_rows).sum::<u64>())
                .unwrap_or(0),
            page_table,
            top_level_columns: projection.column_indices,
            schema: Arc::new(schema),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, HashMap},
        pin::Pin,
        sync::Arc,
    };

    use arrow_array::{
        Int32Array, ListArray, RecordBatch, RecordBatchIterator, UInt32Array,
        types::{Float64Type, Int32Type},
    };
    use arrow_buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
    use arrow_schema::{DataType, Field, Fields, Schema as ArrowSchema};
    use bytes::Bytes;
    use futures::{StreamExt, prelude::stream::TryStreamExt};
    use lance_arrow::{BLOB_META_KEY, RecordBatchExt};
    use lance_core::{ArrowResult, datatypes::Schema};
    use lance_datagen::{ArrayGeneratorExt, BatchCount, ByteCount, RowCount, array, gen_batch};
    use lance_encoding::{
        constants::{STRUCTURAL_ENCODING_META_KEY, STRUCTURAL_ENCODING_SPARSE},
        decoder::{
            DecodeBatchScheduler, DecoderPlugins, EncodedBatchLayout, FilterExpression,
            PageEncoding, ReadBatchTask, decode_batch,
        },
        encoder::{EncodedBatch, EncodingOptions, encode_batch},
        format::pb21,
    };
    use lance_io::{stream::RecordBatchStream, utils::CachedFileSize};
    use log::debug;
    use rstest::rstest;
    use tokio::sync::mpsc;

    use crate::reader::{
        EncodedBatchReaderExt, FileReader, FileReaderOptions, ProjectedFileReader, ReaderProjection,
    };
    use crate::testing::{FsFixture, WrittenFile, test_cache, write_lance_file};
    use crate::version::{ConcreteFileVersion, LanceFileVersion};
    use crate::versions;
    use crate::writer::{FileWriterOptions, PAGE_BUFFER_ALIGNMENT};
    use lance_encoding::decoder::DecoderConfig;

    fn footer_version(bytes: &[u8]) -> (u16, u16) {
        let version_start = bytes.len() - 8;
        (
            u16::from_le_bytes([bytes[version_start], bytes[version_start + 1]]),
            u16::from_le_bytes([bytes[version_start + 2], bytes[version_start + 3]]),
        )
    }

    #[tokio::test]
    async fn sparse_file_writer_reader_scan_range_and_take_roundtrip() {
        let fs = FsFixture::default();
        let sparse_metadata = HashMap::from([(
            STRUCTURAL_ENCODING_META_KEY.to_string(),
            STRUCTURAL_ENCODING_SPARSE.to_string(),
        )]);
        let value_field =
            Field::new("values", DataType::Int32, true).with_metadata(sparse_metadata.clone());
        let item_field = Arc::new(Field::new("item", DataType::Int32, true));
        let list_field = Field::new("items", DataType::List(item_field.clone()), true)
            .with_metadata(sparse_metadata);
        let arrow_schema = Arc::new(ArrowSchema::new(vec![value_field, list_field]));
        let list = ListArray::try_new(
            item_field,
            OffsetBuffer::new(ScalarBuffer::from(vec![0_i32, 2, 2, 2, 3, 3, 5])),
            Arc::new(Int32Array::from(vec![
                Some(1),
                None,
                Some(3),
                Some(4),
                Some(5),
            ])),
            Some(NullBuffer::from(vec![true, false, true, true, true, true])),
        )
        .unwrap();
        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![
                    Some(10),
                    None,
                    Some(30),
                    Some(40),
                    None,
                    Some(60),
                ])),
                Arc::new(list),
            ],
        )
        .unwrap();
        let input = RecordBatchIterator::new(vec![Ok(batch.clone())], arrow_schema);
        write_lance_file(
            input,
            &fs,
            ConcreteFileVersion::V2_3,
            FileWriterOptions::default(),
        )
        .await;

        let file_scheduler = fs
            .scheduler
            .open_file(&fs.tmp_path, &CachedFileSize::unknown())
            .await
            .unwrap();
        let file_reader = FileReader::try_open(
            file_scheduler,
            None,
            Arc::<DecoderPlugins>::default(),
            &test_cache(),
            FileReaderOptions::default(),
        )
        .await
        .unwrap();
        assert_eq!(file_reader.metadata().column_infos.len(), 2);
        assert!(
            file_reader
                .metadata()
                .column_infos
                .iter()
                .flat_map(|column| column.page_infos.iter())
                .all(|page| {
                    matches!(
                        &page.encoding,
                        PageEncoding::Structural(layout)
                            if matches!(
                                layout.layout,
                                Some(pb21::page_layout::Layout::SparseLayout(_))
                            )
                    )
                })
        );

        let scan = file_reader
            .read_stream(
                lance_io::ReadBatchParams::RangeFull,
                1024,
                1,
                FilterExpression::no_filter(),
            )
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(scan, vec![batch.clone()]);

        let range = file_reader
            .read_stream(
                lance_io::ReadBatchParams::Range(1..5),
                1024,
                1,
                FilterExpression::no_filter(),
            )
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(range, vec![batch.slice(1, 4)]);

        let indices = UInt32Array::from(vec![0, 3, 5]);
        let take = file_reader
            .read_stream(
                lance_io::ReadBatchParams::Indices(indices.clone()),
                1024,
                1,
                FilterExpression::no_filter(),
            )
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(take, vec![batch.take(&indices).unwrap()]);
    }

    async fn create_some_file(fs: &FsFixture, version: ConcreteFileVersion) -> WrittenFile {
        let location_type = DataType::Struct(Fields::from(vec![
            Field::new("x", DataType::Float64, true),
            Field::new("y", DataType::Float64, true),
        ]));
        let categories_type = DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)));

        let mut reader = gen_batch()
            .col("score", array::rand::<Float64Type>())
            .col("location", array::rand_type(&location_type))
            .col("categories", array::rand_type(&categories_type))
            .col("binary", array::rand_type(&DataType::Binary));
        if version == ConcreteFileVersion::V2_0 {
            reader = reader.col("large_bin", array::rand_type(&DataType::LargeBinary));
        }
        let reader = reader.into_reader_rows(RowCount::from(1000), BatchCount::from(100));

        write_lance_file(reader, fs, version, FileWriterOptions::default()).await
    }

    async fn create_wide_direct_file(fs: &FsFixture, num_columns: usize) -> WrittenFile {
        let mut reader = gen_batch();
        for column_idx in 0..num_columns {
            reader = reader.col(format!("c{column_idx}"), array::step::<Int32Type>());
        }
        let reader = reader.into_reader_rows(RowCount::from(1000), BatchCount::from(100));

        write_lance_file(
            reader,
            fs,
            ConcreteFileVersion::V2_1,
            FileWriterOptions::default(),
        )
        .await
    }

    async fn create_wide_fixed_size_list_file(fs: &FsFixture, num_columns: usize) -> WrittenFile {
        let data_type =
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 4);
        let mut reader = gen_batch();
        for column_idx in 0..num_columns {
            reader = reader.col(
                format!("c{column_idx}"),
                array::rand_type(&data_type).with_random_nulls(0.1),
            );
        }
        let reader = reader.into_reader_rows(RowCount::from(64), BatchCount::from(4));

        write_lance_file(
            reader,
            fs,
            ConcreteFileVersion::V2_1,
            FileWriterOptions::default(),
        )
        .await
    }

    async fn create_wide_structural_file(fs: &FsFixture, num_groups: usize) -> WrittenFile {
        let struct_type = DataType::Struct(Fields::from(vec![
            Field::new("x", DataType::Int32, true),
            Field::new("y", DataType::Int32, true),
        ]));
        let list_type = DataType::List(Arc::new(Field::new("item", DataType::Int32, true)));
        let mut reader = gen_batch();
        for group_idx in 0..num_groups {
            reader = reader
                .col(
                    format!("s{group_idx}"),
                    array::rand_type(&struct_type).with_random_nulls(0.5),
                )
                .col(
                    format!("l{group_idx}"),
                    array::rand_type(&list_type).with_random_nulls(0.5),
                );
        }
        let reader = reader.into_reader_rows(RowCount::from(64), BatchCount::from(4));

        write_lance_file(
            reader,
            fs,
            ConcreteFileVersion::V2_1,
            FileWriterOptions::default(),
        )
        .await
    }

    type Transformer = Box<dyn Fn(&RecordBatch) -> RecordBatch>;

    async fn verify_expected(
        expected: &[RecordBatch],
        mut actual: Pin<Box<dyn RecordBatchStream>>,
        read_size: u32,
        transform: Option<Transformer>,
    ) {
        let mut remaining = expected.iter().map(|batch| batch.num_rows()).sum::<usize>() as u32;
        let mut expected_iter = expected.iter().map(|batch| {
            if let Some(transform) = &transform {
                transform(batch)
            } else {
                batch.clone()
            }
        });
        let mut next_expected = expected_iter.next().unwrap().clone();
        while let Some(actual) = actual.next().await {
            let mut actual = actual.unwrap();
            let mut rows_to_verify = actual.num_rows() as u32;
            let expected_length = remaining.min(read_size);
            assert_eq!(expected_length, rows_to_verify);

            while rows_to_verify > 0 {
                let next_slice_len = (next_expected.num_rows() as u32).min(rows_to_verify);
                assert_eq!(
                    next_expected.slice(0, next_slice_len as usize),
                    actual.slice(0, next_slice_len as usize)
                );
                remaining -= next_slice_len;
                rows_to_verify -= next_slice_len;
                if remaining > 0 {
                    if next_slice_len == next_expected.num_rows() as u32 {
                        next_expected = expected_iter.next().unwrap().clone();
                    } else {
                        next_expected = next_expected.slice(
                            next_slice_len as usize,
                            next_expected.num_rows() - next_slice_len as usize,
                        );
                    }
                }
                if rows_to_verify > 0 {
                    actual = actual.slice(
                        next_slice_len as usize,
                        actual.num_rows() - next_slice_len as usize,
                    );
                }
            }
        }
        assert_eq!(remaining, 0);
    }

    async fn collect_read_tasks(
        tasks: Pin<Box<dyn futures::Stream<Item = ReadBatchTask> + Send>>,
        readahead: usize,
    ) -> Vec<RecordBatch> {
        tasks
            .map(|task| task.task)
            .buffered(readahead)
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
    }

    /// Writes `batch` to a fresh file, overwrites `patch` bytes at `patch_offset`
    /// into the single occurrence of `pattern`, and reads the file back with the
    /// default reader configuration.
    async fn read_file_with_mutated_bytes(
        version: LanceFileVersion,
        batch: RecordBatch,
        pattern: &[u8],
        patch_offset: usize,
        patch: &[u8],
    ) -> lance_core::Result<Vec<RecordBatch>> {
        let fs = FsFixture::default();
        let schema = batch.schema();
        write_lance_file(
            RecordBatchIterator::new(vec![Ok(batch)], schema),
            &fs,
            ConcreteFileVersion::from(version),
            FileWriterOptions::default(),
        )
        .await;

        let mut bytes = fs
            .object_store
            .read_one_all(&fs.tmp_path)
            .await
            .unwrap()
            .to_vec();
        let matches = bytes
            .windows(pattern.len())
            .enumerate()
            .filter_map(|(position, window)| (window == pattern).then_some(position))
            .collect::<Vec<_>>();
        assert_eq!(
            matches.len(),
            1,
            "expected the byte pattern to appear exactly once in the file"
        );
        let patch_start = matches[0] + patch_offset;
        bytes[patch_start..patch_start + patch.len()].copy_from_slice(patch);
        fs.object_store.put(&fs.tmp_path, &bytes).await.unwrap();

        let file_scheduler = fs
            .scheduler
            .open_file(&fs.tmp_path, &CachedFileSize::unknown())
            .await
            .unwrap();
        let file_reader = FileReader::try_open(
            file_scheduler,
            None,
            Arc::<DecoderPlugins>::default(),
            &test_cache(),
            FileReaderOptions::default(),
        )
        .await
        .unwrap();
        file_reader
            .read_stream(
                lance_io::ReadBatchParams::RangeFull,
                1024,
                16,
                FilterExpression::no_filter(),
            )
            .await?
            .try_collect::<Vec<_>>()
            .await
    }

    #[tokio::test]
    async fn test_reader_rejects_excess_miniblock_row_counts() {
        let batch =
            arrow_array::record_batch!(("id", UInt64, (0..2048_u64).collect::<Vec<_>>())).unwrap();
        let fs = FsFixture::default();
        write_lance_file(
            RecordBatchIterator::new(vec![Ok(batch.clone())], batch.schema()),
            &fs,
            ConcreteFileVersion::V2_1,
            FileWriterOptions::default(),
        )
        .await;

        let mut bytes = fs
            .object_store
            .read_one_all(&fs.tmp_path)
            .await
            .unwrap()
            .to_vec();
        // V2.1 places this column's first mini-block metadata word at byte zero.
        // The issue's mutation makes its non-final item count exceed the page total.
        bytes[0] ^= 0xf7;
        fs.object_store.put(&fs.tmp_path, &bytes).await.unwrap();

        let file_scheduler = fs
            .scheduler
            .open_file(&fs.tmp_path, &CachedFileSize::unknown())
            .await
            .unwrap();
        let file_reader = FileReader::try_open(
            file_scheduler,
            None,
            Arc::<DecoderPlugins>::default(),
            &test_cache(),
            FileReaderOptions::default(),
        )
        .await
        .unwrap();
        let result = file_reader
            .read_stream(
                lance_io::ReadBatchParams::RangeFull,
                1024,
                16,
                FilterExpression::no_filter(),
            )
            .await;
        let error = match result {
            Ok(stream) => stream
                .try_collect::<Vec<_>>()
                .await
                .expect_err("excess mini-block row counts must fail the read"),
            Err(error) => error,
        };
        assert!(
            matches!(error, lance_core::Error::CorruptFile { .. }),
            "expected CorruptFile, got: {error}"
        );
        assert!(
            error.to_string().contains("exceeding items_in_page"),
            "unexpected message: {error}"
        );
    }

    /// A corrupt file whose variable-width offsets point outside the value bytes
    /// must fail with a typed error under the default reader configuration
    /// (`validate_on_decode` disabled) instead of materializing values outside
    /// the data buffer.
    ///
    /// Uses a dictionary-encoded string column because its values page stores
    /// the offsets verbatim, so flipping the tail offset in the file reaches the
    /// Arrow conversion boundary without being rejected by an intermediate
    /// decompressor.
    #[rstest]
    #[tokio::test]
    async fn test_default_reader_rejects_out_of_bounds_variable_width_offsets(
        #[values(LanceFileVersion::V2_1, LanceFileVersion::V2_2, LanceFileVersion::V2_3)]
        version: LanceFileVersion,
    ) {
        use arrow_array::{Array, DictionaryArray, Int32Array, StringArray};

        let values = StringArray::from(vec!["alpha", "beta", "gamma"]);
        let indices = Int32Array::from((0..300).map(|i| i % 3).collect::<Vec<i32>>());
        let dictionary = DictionaryArray::new(indices, Arc::new(values));
        let arrow_schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "category",
            dictionary.data_type().clone(),
            false,
        )]));
        let batch = RecordBatch::try_new(arrow_schema, vec![Arc::new(dictionary)]).unwrap();

        // The dictionary values page stores the value offsets as plain
        // little-endian i32s ending with [5, 9, 14] (2.1 also stores the leading
        // zero, 2.2+ omits it).  If a future encoding change stops storing these
        // offsets verbatim this lookup fails loudly and the test needs a new
        // byte pattern.  The patch rewrites the tail offset so it points far
        // beyond the value bytes.
        let offsets_tail_pattern = [5_i32, 9, 14]
            .iter()
            .flat_map(|value| value.to_le_bytes())
            .collect::<Vec<u8>>();
        let error = read_file_with_mutated_bytes(
            version,
            batch,
            &offsets_tail_pattern,
            8,
            &100_000_i32.to_le_bytes(),
        )
        .await
        .expect_err("out-of-bounds offsets must fail the read");
        assert!(
            matches!(error, lance_core::Error::CorruptFile { .. }),
            "expected CorruptFile, got: {error}"
        );
        assert!(
            error.to_string().contains("out of bounds"),
            "unexpected message: {error}"
        );
    }

    /// Storage dictionaries expand their values through `DataBlockBuilder`
    /// before the final Arrow layout validation.  Corrupt dictionary offsets
    /// must therefore fail at the append boundary instead of reaching a slice
    /// operation with a decreasing range.
    #[rstest]
    #[tokio::test]
    async fn test_default_reader_rejects_non_monotonic_storage_dictionary_offsets(
        #[values(LanceFileVersion::V2_1, LanceFileVersion::V2_2, LanceFileVersion::V2_3)]
        version: LanceFileVersion,
    ) {
        use arrow_array::StringArray;

        let metadata = HashMap::from([
            (
                "lance-encoding:dict-size-ratio".to_string(),
                "0.99".to_string(),
            ),
            (
                "lance-encoding:dict-values-compression".to_string(),
                "none".to_string(),
            ),
        ]);
        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("category", DataType::Utf8, false).with_metadata(metadata),
        ]));
        let values = (0..300)
            .map(|index| match index % 3 {
                0 => "alpha",
                1 => "beta",
                _ => "gamma",
            })
            .collect::<Vec<_>>();
        let batch =
            RecordBatch::try_new(arrow_schema, vec![Arc::new(StringArray::from(values))]).unwrap();

        let offsets_tail_pattern = [5_i32, 9, 14]
            .iter()
            .flat_map(|value| value.to_le_bytes())
            .collect::<Vec<u8>>();
        let error = read_file_with_mutated_bytes(
            version,
            batch,
            &offsets_tail_pattern,
            4,
            &2_i32.to_le_bytes(),
        )
        .await
        .expect_err("non-monotonic dictionary offsets must fail the read");
        assert!(
            matches!(error, lance_core::Error::CorruptFile { .. }),
            "expected CorruptFile, got: {error}"
        );
        assert!(
            error.to_string().contains("decreases"),
            "unexpected message: {error}"
        );
    }

    /// Same contract as the test above, but for a plain (non-dictionary) string
    /// column: the mini-block chunk stores chunk-relative value offsets that are
    /// used to slice the chunk, so a corrupt tail offset must surface as a typed
    /// error from the chunk decompressor instead of a panic in the decode task.
    #[rstest]
    #[tokio::test]
    async fn test_default_reader_rejects_out_of_bounds_miniblock_offsets(
        #[values(LanceFileVersion::V2_1, LanceFileVersion::V2_2, LanceFileVersion::V2_3)]
        version: LanceFileVersion,
    ) {
        use arrow_array::StringArray;

        let arrow_schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "strings",
            DataType::Utf8,
            false,
        )]));
        let batch = RecordBatch::try_new(
            arrow_schema,
            vec![Arc::new(StringArray::from(vec!["alpha", "beta", "gamma"]))],
        )
        .unwrap();

        // For ["alpha", "beta", "gamma"] the chunk stores LE i32 offsets
        // [16, 21, 25, 30] (chunk-relative: a 16-byte offsets region precedes
        // the value bytes).  The patch rewrites the tail offset to point far
        // past the chunk.
        let chunk_offsets_pattern = [16_i32, 21, 25, 30]
            .iter()
            .flat_map(|value| value.to_le_bytes())
            .collect::<Vec<u8>>();
        let error = read_file_with_mutated_bytes(
            version,
            batch,
            &chunk_offsets_pattern,
            12,
            &100_000_i32.to_le_bytes(),
        )
        .await
        .expect_err("an out-of-bounds chunk offset must fail the read");
        assert!(
            matches!(error, lance_core::Error::CorruptFile { .. }),
            "expected CorruptFile, got: {error}"
        );
        assert!(
            error.to_string().contains("out of bounds"),
            "unexpected message: {error}"
        );
    }

    #[tokio::test]
    async fn test_round_trip() {
        let fs = FsFixture::default();

        let WrittenFile { data, .. } = create_some_file(&fs, ConcreteFileVersion::V2_0).await;

        let file_size = fs.object_store.size(&fs.tmp_path).await.unwrap() as usize;
        let footer = fs
            .object_store
            .open(&fs.tmp_path)
            .await
            .unwrap()
            .get_range(file_size - 8..file_size)
            .await
            .unwrap();
        assert_eq!(footer_version(&footer), (0, 3));
        assert_eq!(
            crate::determine_file_version(&fs.object_store, &fs.tmp_path, Some(file_size))
                .await
                .unwrap(),
            ConcreteFileVersion::V2_0
        );

        for read_size in [32, 1024, 1024 * 1024] {
            let file_scheduler = fs
                .scheduler
                .open_file(&fs.tmp_path, &CachedFileSize::unknown())
                .await
                .unwrap();
            let file_reader = FileReader::try_open(
                file_scheduler,
                None,
                Arc::<DecoderPlugins>::default(),
                &test_cache(),
                FileReaderOptions::default(),
            )
            .await
            .unwrap();

            assert_eq!(
                (
                    file_reader.metadata().major_version,
                    file_reader.metadata().minor_version
                ),
                (0, 3)
            );
            let schema = file_reader.schema();
            assert_eq!(schema.metadata.get("foo").unwrap(), "bar");

            let batch_stream = file_reader
                .read_stream(
                    lance_io::ReadBatchParams::RangeFull,
                    read_size,
                    16,
                    FilterExpression::no_filter(),
                )
                .await
                .unwrap();

            verify_expected(&data, batch_stream, read_size, None).await;
        }
    }

    #[rstest]
    #[test_log::test(tokio::test)]
    async fn test_encoded_batch_round_trip(
        // TODO: Add V2_1 (currently fails)
        #[values(ConcreteFileVersion::V2_0)] version: ConcreteFileVersion,
    ) {
        let data = gen_batch()
            .col("x", array::rand::<Int32Type>())
            .col("y", array::rand_utf8(ByteCount::from(16), false))
            .into_batch_rows(RowCount::from(10000))
            .unwrap();

        let lance_schema = Arc::new(Schema::try_from(data.schema().as_ref()).unwrap());

        let encoding_options = EncodingOptions {
            cache_bytes_per_column: 4096,
            max_page_bytes: 32 * 1024 * 1024,
            keep_original_array: true,
            buffer_alignment: 64,
        };

        let encoding_strategy = crate::versions::v2_0::encoding_strategy();

        let encoded_batch = encode_batch(
            &data,
            lance_schema.clone(),
            encoding_strategy.as_ref(),
            &encoding_options,
        )
        .await
        .unwrap();

        // Test self described
        let bytes = versions::encode_self_described_batch(version, &encoded_batch).unwrap();
        assert_eq!(footer_version(&bytes), (2, 0));

        let decoded_batch = EncodedBatch::try_from_self_described_lance(bytes).unwrap();

        let decoded = decode_batch(
            &decoded_batch,
            &FilterExpression::no_filter(),
            Arc::<DecoderPlugins>::default(),
            false,
            EncodedBatchLayout::Array,
            None,
        )
        .await
        .unwrap();

        assert_eq!(data, decoded);

        // Test mini
        let bytes = versions::encode_mini_batch(version, &encoded_batch).unwrap();
        assert_eq!(footer_version(&bytes), (2, 0));
        let decoded_batch =
            EncodedBatch::try_from_mini_lance(bytes, lance_schema.as_ref()).unwrap();
        let decoded = decode_batch(
            &decoded_batch,
            &FilterExpression::no_filter(),
            Arc::<DecoderPlugins>::default(),
            false,
            EncodedBatchLayout::Array,
            None,
        )
        .await
        .unwrap();

        assert_eq!(data, decoded);
    }

    #[rstest]
    #[test_log::test(tokio::test)]
    async fn test_projection(
        #[values(
            ConcreteFileVersion::V2_0,
            ConcreteFileVersion::V2_1,
            ConcreteFileVersion::V2_2
        )]
        version: ConcreteFileVersion,
    ) {
        let fs = FsFixture::default();

        let written_file = create_some_file(&fs, version).await;
        let file_scheduler = fs
            .scheduler
            .open_file(&fs.tmp_path, &CachedFileSize::unknown())
            .await
            .unwrap();

        let field_id_mapping = written_file
            .field_id_mapping
            .iter()
            .copied()
            .collect::<BTreeMap<_, _>>();

        let empty_projection = ReaderProjection {
            column_indices: Vec::default(),
            schema: Arc::new(Schema::default()),
        };

        for columns in [
            vec!["score"],
            vec!["location"],
            vec!["categories"],
            vec!["score.x"],
            vec!["score", "categories"],
            vec!["score", "location"],
            vec!["location", "categories"],
            vec!["score.y", "location", "categories"],
        ] {
            debug!("Testing round trip with projection {:?}", columns);
            for use_field_ids in [true, false] {
                // We can specify the projection as part of the read operation via read_stream_projected
                let file_reader = FileReader::try_open(
                    file_scheduler.clone(),
                    None,
                    Arc::<DecoderPlugins>::default(),
                    &test_cache(),
                    FileReaderOptions::default(),
                )
                .await
                .unwrap();

                let projected_schema = written_file.schema.project(&columns).unwrap();
                let projection = if use_field_ids {
                    versions::reader_projection_from_field_ids(
                        file_reader.metadata().version(),
                        &projected_schema,
                        &field_id_mapping,
                    )
                    .unwrap()
                } else {
                    versions::reader_projection_from_column_names(
                        file_reader.metadata().version(),
                        &written_file.schema,
                        &columns,
                    )
                    .unwrap()
                };

                let batch_stream = file_reader
                    .read_stream_projected(
                        lance_io::ReadBatchParams::RangeFull,
                        1024,
                        16,
                        projection.clone(),
                        FilterExpression::no_filter(),
                    )
                    .await
                    .unwrap();

                let projection_arrow = ArrowSchema::from(projection.schema.as_ref());
                verify_expected(
                    &written_file.data,
                    batch_stream,
                    1024,
                    Some(Box::new(move |batch: &RecordBatch| {
                        batch.project_by_schema(&projection_arrow).unwrap()
                    })),
                )
                .await;

                // We can also specify the projection as a base projection when we open the file
                let file_reader = FileReader::try_open(
                    file_scheduler.clone(),
                    Some(projection.clone()),
                    Arc::<DecoderPlugins>::default(),
                    &test_cache(),
                    FileReaderOptions::default(),
                )
                .await
                .unwrap();

                let batch_stream = file_reader
                    .read_stream(
                        lance_io::ReadBatchParams::RangeFull,
                        1024,
                        16,
                        FilterExpression::no_filter(),
                    )
                    .await
                    .unwrap();

                let projection_arrow = ArrowSchema::from(projection.schema.as_ref());
                verify_expected(
                    &written_file.data,
                    batch_stream,
                    1024,
                    Some(Box::new(move |batch: &RecordBatch| {
                        batch.project_by_schema(&projection_arrow).unwrap()
                    })),
                )
                .await;

                assert!(
                    file_reader
                        .read_stream_projected(
                            lance_io::ReadBatchParams::RangeFull,
                            1024,
                            16,
                            empty_projection.clone(),
                            FilterExpression::no_filter(),
                        )
                        .await
                        .is_err()
                );
            }
        }

        assert!(
            FileReader::try_open(
                file_scheduler.clone(),
                Some(empty_projection),
                Arc::<DecoderPlugins>::default(),
                &test_cache(),
                FileReaderOptions::default(),
            )
            .await
            .is_err()
        );

        let arrow_schema = ArrowSchema::new(vec![
            Field::new("x", DataType::Int32, true),
            Field::new("y", DataType::Int32, true),
        ]);
        let schema = Schema::try_from(&arrow_schema).unwrap();

        let projection_with_dupes = ReaderProjection {
            column_indices: vec![0, 0],
            schema: Arc::new(schema),
        };

        assert!(
            FileReader::try_open(
                file_scheduler.clone(),
                Some(projection_with_dupes),
                Arc::<DecoderPlugins>::default(),
                &test_cache(),
                FileReaderOptions::default(),
            )
            .await
            .is_err()
        );
    }

    #[tokio::test]
    async fn test_lazy_reader_direct_projection_matches_eager_reader() {
        let fs = FsFixture::default();
        let written_file = create_wide_direct_file(&fs, 16).await;

        let file_scheduler = fs
            .scheduler
            .open_file(&fs.tmp_path, &CachedFileSize::unknown())
            .await
            .unwrap();
        let projection = versions::reader_projection_from_column_names(
            ConcreteFileVersion::V2_1,
            &written_file.schema,
            &["c10"],
        )
        .unwrap();

        let eager_reader = FileReader::try_open(
            file_scheduler.clone(),
            None,
            Arc::<DecoderPlugins>::default(),
            &test_cache(),
            FileReaderOptions::default(),
        )
        .await
        .unwrap();
        let expected = eager_reader
            .read_stream_projected(
                lance_io::ReadBatchParams::RangeFull,
                127,
                16,
                projection.clone(),
                FilterExpression::no_filter(),
            )
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        let cache = test_cache();
        let lazy_reader = ProjectedFileReader::try_open(
            file_scheduler,
            Some(projection.clone()),
            Arc::<DecoderPlugins>::default(),
            &cache,
            FileReaderOptions::default(),
        )
        .await
        .unwrap();
        let tasks = lazy_reader
            .read_tasks(
                lance_io::ReadBatchParams::RangeFull,
                127,
                None,
                FilterExpression::no_filter(),
            )
            .await
            .unwrap();
        let actual = collect_read_tasks(tasks, 16).await;

        assert_eq!(expected, actual);
    }

    #[tokio::test]
    async fn test_lazy_reader_loads_only_requested_column_metadata() {
        let fs = FsFixture::default();
        let written_file = create_wide_direct_file(&fs, 512).await;

        let projection = versions::reader_projection_from_column_names(
            ConcreteFileVersion::V2_1,
            &written_file.schema,
            &["c0"],
        )
        .unwrap();
        let file_scheduler = fs
            .scheduler
            .open_file(&fs.tmp_path, &CachedFileSize::unknown())
            .await
            .unwrap();
        let lazy_reader = ProjectedFileReader::try_open(
            file_scheduler,
            Some(projection.clone()),
            Arc::<DecoderPlugins>::default(),
            &test_cache(),
            FileReaderOptions::default(),
        )
        .await
        .unwrap();
        let selected_column = projection.column_indices[0] as usize;
        let requested_metadata_bytes = lazy_reader
            .metadata_index()
            .unwrap()
            .column_metadata_offsets[selected_column]
            .1;
        let total_metadata_bytes = lazy_reader
            .metadata_index()
            .unwrap()
            .column_metadata_offsets
            .iter()
            .map(|(_, length)| *length)
            .sum::<u64>();
        assert!(
            total_metadata_bytes > 8 * fs.object_store.block_size() as u64,
            "test file metadata is too small to prove lazy loading: {total_metadata_bytes} bytes"
        );

        fs.object_store.io_stats_incremental();
        let tasks = lazy_reader
            .read_tasks(
                lance_io::ReadBatchParams::Range(0..0),
                1024,
                Some(projection.clone()),
                FilterExpression::no_filter(),
            )
            .await
            .unwrap();
        let batches = collect_read_tasks(tasks, 1).await;
        assert!(batches.is_empty());

        let stats = fs.object_store.io_stats_incremental();
        assert!(
            stats.read_bytes < total_metadata_bytes / 2,
            "lazy read fetched too much metadata: read {} bytes, requested column metadata is {} bytes, total column metadata is {} bytes",
            stats.read_bytes,
            requested_metadata_bytes,
            total_metadata_bytes
        );

        fs.object_store.io_stats_incremental();
        let tasks = lazy_reader
            .read_tasks(
                lance_io::ReadBatchParams::Range(0..0),
                1024,
                Some(projection),
                FilterExpression::no_filter(),
            )
            .await
            .unwrap();
        let batches = collect_read_tasks(tasks, 1).await;
        assert!(batches.is_empty());

        let stats = fs.object_store.io_stats_incremental();
        assert_eq!(
            stats.read_iops, 0,
            "cached column metadata should avoid repeat metadata I/O"
        );
        assert_eq!(
            stats.read_bytes, 0,
            "cached column metadata should avoid repeat metadata reads"
        );
    }

    async fn assert_lazy_projection_matches_eager_and_reads_metadata_subset(
        fs: &FsFixture,
        projection: ReaderProjection,
        shape: &str,
    ) -> Vec<RecordBatch> {
        let file_scheduler = fs
            .scheduler
            .open_file(&fs.tmp_path, &CachedFileSize::unknown())
            .await
            .unwrap();
        let eager_reader = FileReader::try_open(
            file_scheduler.clone(),
            None,
            Arc::<DecoderPlugins>::default(),
            &test_cache(),
            FileReaderOptions::default(),
        )
        .await
        .unwrap();
        let expected = eager_reader
            .read_stream_projected(
                lance_io::ReadBatchParams::RangeFull,
                127,
                16,
                projection.clone(),
                FilterExpression::no_filter(),
            )
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        let cache = test_cache();
        let lazy_reader = ProjectedFileReader::try_open(
            file_scheduler,
            Some(projection.clone()),
            Arc::<DecoderPlugins>::default(),
            &cache,
            FileReaderOptions::default(),
        )
        .await
        .unwrap();
        let metadata_index = lazy_reader.metadata_index().unwrap();
        let requested_metadata_bytes = projection
            .column_indices
            .iter()
            .map(|column_index| metadata_index.column_metadata_offsets[*column_index as usize].1)
            .sum::<u64>();
        let total_metadata_bytes = metadata_index
            .column_metadata_offsets
            .iter()
            .map(|(_, length)| *length)
            .sum::<u64>();
        assert!(total_metadata_bytes > requested_metadata_bytes * 8);

        fs.object_store.io_stats_incremental();
        let tasks = lazy_reader
            .read_tasks(
                lance_io::ReadBatchParams::Range(0..0),
                127,
                None,
                FilterExpression::no_filter(),
            )
            .await
            .unwrap();
        assert!(collect_read_tasks(tasks, 1).await.is_empty());
        let metadata_stats = fs.object_store.io_stats_incremental();
        assert!(
            metadata_stats.read_bytes < total_metadata_bytes / 2,
            "lazy {shape} read fetched too much metadata: read {} bytes, requested column metadata is {} bytes, total column metadata is {} bytes",
            metadata_stats.read_bytes,
            requested_metadata_bytes,
            total_metadata_bytes
        );

        let tasks = lazy_reader
            .read_tasks(
                lance_io::ReadBatchParams::RangeFull,
                127,
                None,
                FilterExpression::no_filter(),
            )
            .await
            .unwrap();
        let actual = collect_read_tasks(tasks, 16).await;
        assert_eq!(expected, actual);
        actual
    }

    #[tokio::test]
    async fn test_lazy_reader_fixed_size_list_projection_matches_eager_reader() {
        let fs = FsFixture::default();
        let written_file = create_wide_fixed_size_list_file(&fs, 512).await;
        let projection = versions::reader_projection_from_column_names(
            ConcreteFileVersion::V2_1,
            &written_file.schema,
            &["c17", "c509"],
        )
        .unwrap();
        assert!(projection.prefers_indexed_metadata(512));
        assert_lazy_projection_matches_eager_and_reads_metadata_subset(
            &fs,
            projection,
            "fixed-size-list",
        )
        .await;
    }

    #[tokio::test]
    async fn test_v2_0_rejects_indexed_metadata_reader() {
        let fs = FsFixture::default();
        let written_file = create_some_file(&fs, ConcreteFileVersion::V2_0).await;
        let projection = versions::reader_projection_from_column_names(
            ConcreteFileVersion::V2_0,
            &written_file.schema,
            &["score"],
        )
        .unwrap();
        assert!(projection.prefers_indexed_metadata(100));

        let file_scheduler = fs
            .scheduler
            .open_file(&fs.tmp_path, &CachedFileSize::unknown())
            .await
            .unwrap();
        let err = ProjectedFileReader::try_open(
            file_scheduler,
            Some(projection),
            Arc::<DecoderPlugins>::default(),
            &test_cache(),
            FileReaderOptions::default(),
        )
        .await
        .unwrap_err();
        assert!(
            matches!(err, lance_core::Error::NotSupported { .. }),
            "expected V2.0 indexed metadata open to fail, got {err:?}"
        );
    }

    #[tokio::test]
    async fn test_lazy_reader_nested_projection_compacts_physical_columns() {
        let fs = FsFixture::default();
        let written_file = create_wide_structural_file(&fs, 128).await;
        let projection = versions::reader_projection_from_column_names(
            ConcreteFileVersion::V2_1,
            &written_file.schema,
            &["s97.y", "l4", "s3"],
        )
        .unwrap();

        assert_eq!(
            projection
                .schema
                .fields
                .iter()
                .map(|field| field.name.as_str())
                .collect::<Vec<_>>(),
            vec!["s97", "l4", "s3"]
        );
        assert_eq!(projection.schema.fields[0].children.len(), 1);
        assert_eq!(projection.schema.fields[0].children[0].name, "y");
        assert_eq!(projection.schema.fields[2].children.len(), 2);
        assert_eq!(projection.column_indices.len(), 4);
        assert!(
            projection
                .column_indices
                .windows(2)
                .any(|indices| indices[0] > indices[1]),
            "the projection must reorder physical columns to exercise compact remapping"
        );
        assert!(projection.prefers_indexed_metadata(128 * 4));
        let actual = assert_lazy_projection_matches_eager_and_reads_metadata_subset(
            &fs, projection, "nested",
        )
        .await;
        assert!(
            actual
                .iter()
                .flat_map(|batch| batch.columns())
                .any(|column| column.null_count() > 0),
            "the structural projection must exercise nullable arrays"
        );
    }

    #[rstest]
    #[case::before_metadata_region(90, 5)]
    #[case::after_metadata_region(190, 20)]
    fn test_decode_cmo_table_rejects_out_of_range_offsets(
        #[case] position: u64,
        #[case] length: u64,
    ) {
        let mut cmo_table = [0; 16];
        cmo_table[0..8].copy_from_slice(&position.to_le_bytes());
        cmo_table[8..16].copy_from_slice(&length.to_le_bytes());
        let footer = super::Footer {
            column_meta_start: 100,
            column_meta_offsets_start: 200,
            global_buff_offsets_start: 200,
            num_global_buffers: 0,
            num_columns: 1,
            major_version: 2,
            minor_version: 1,
        };

        let err = FileReader::decode_cmo_table(Bytes::copy_from_slice(&cmo_table), &footer)
            .expect_err("out-of-range CMO entries must be rejected");
        assert!(
            matches!(err, lance_core::Error::InvalidInput { .. }),
            "expected InvalidInput, got {err:?}"
        );
    }

    #[rstest]
    #[case::blob(BLOB_META_KEY)]
    #[case::packed_struct("lance-encoding:packed")]
    #[tokio::test]
    async fn test_lazy_reader_rejects_opaque_projection(#[case] metadata_key: &str) {
        let fs = FsFixture::default();
        let written_file = create_some_file(&fs, ConcreteFileVersion::V2_1).await;

        let ordinary_projection = versions::reader_projection_from_column_names(
            ConcreteFileVersion::V2_1,
            &written_file.schema,
            &["location.x"],
        )
        .unwrap();
        assert_eq!(ordinary_projection.schema.fields[0].children.len(), 1);
        assert!(ordinary_projection.prefers_indexed_metadata(100));

        let file_scheduler = fs
            .scheduler
            .open_file(&fs.tmp_path, &CachedFileSize::unknown())
            .await
            .unwrap();
        let err = ProjectedFileReader::try_open(
            file_scheduler.clone(),
            None,
            Arc::<DecoderPlugins>::default(),
            &test_cache(),
            FileReaderOptions::default(),
        )
        .await
        .unwrap_err();
        assert!(
            matches!(err, lance_core::Error::InvalidInput { .. }),
            "expected InvalidInput, got {err:?}"
        );

        let mut projection = ordinary_projection;
        Arc::make_mut(&mut projection.schema).fields[0]
            .metadata
            .insert(metadata_key.to_string(), "true".to_string());
        assert!(!projection.prefers_indexed_metadata(100));

        let err = ProjectedFileReader::try_open(
            file_scheduler,
            Some(projection),
            Arc::<DecoderPlugins>::default(),
            &test_cache(),
            FileReaderOptions::default(),
        )
        .await
        .unwrap_err();
        assert!(
            matches!(err, lance_core::Error::NotSupported { .. }),
            "expected NotSupported for {metadata_key}, got {err:?}"
        );
    }

    // The projection-length validation lives in `DecodeEngine`, shared by the
    // eager and the lazy (indexed) metadata providers. The indexed provider loads
    // only the projected columns and renumbers them 0..N, so this checks that the
    // renumbered `column_infos`/`column_indices` still line up for the length
    // check: a mismatched-length projection is rejected through
    // `ProjectedFileReader`, and a single short column resolves to its own length.
    #[tokio::test]
    async fn test_lazy_reader_validates_unequal_length_projection() {
        use arrow_array::Int32Array;
        use lance_io::ReadBatchParams;

        let arrow_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("c", DataType::Int32, true),
        ]));
        let lance_schema = Schema::try_from(arrow_schema.as_ref()).unwrap();

        let fs = FsFixture::default();
        let mut writer = versions::v2_1::create_writer(
            fs.object_store.create(&fs.tmp_path).await.unwrap(),
            lance_schema.clone(),
            FileWriterOptions::default(),
        )
        .unwrap();
        // "a" has 5 rows, "c" has 1 -- an unequal-length file.
        writer
            .write_column(0, Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])))
            .await
            .unwrap();
        writer
            .write_column(1, Arc::new(Int32Array::from(vec![100])))
            .await
            .unwrap();
        writer.finish().await.unwrap();

        let file_scheduler = fs
            .scheduler
            .open_file(&fs.tmp_path, &CachedFileSize::unknown())
            .await
            .unwrap();
        let cache = test_cache();
        let open_indexed = |names: &[&str]| {
            let projection = versions::reader_projection_from_column_names(
                ConcreteFileVersion::V2_1,
                &lance_schema,
                names,
            )
            .unwrap();
            ProjectedFileReader::try_open(
                file_scheduler.clone(),
                Some(projection),
                Arc::<DecoderPlugins>::default(),
                &cache,
                FileReaderOptions::default(),
            )
        };

        // A mismatched-length projection [a, c] (5 vs 1) is rejected at read time,
        // through the indexed provider's renumbered column infos.
        let lazy = open_indexed(&["a", "c"]).await.unwrap();
        // (`read_tasks` yields a stream, which is not `Debug`, so match rather
        // than `unwrap_err`.)
        let err = match lazy
            .read_tasks(
                ReadBatchParams::RangeFull,
                1024,
                None,
                FilterExpression::no_filter(),
            )
            .await
        {
            Ok(_) => panic!("expected the mismatched-length projection to be rejected"),
            Err(e) => e.to_string(),
        };
        assert!(
            err.contains("a=5") && err.contains("c=1"),
            "error should name each column's length, got: {err}"
        );

        // A single short column resolves to its own length (1), not the file's
        // longest column.
        let lazy = open_indexed(&["c"]).await.unwrap();
        let tasks = lazy
            .read_tasks(
                ReadBatchParams::RangeFull,
                1024,
                None,
                FilterExpression::no_filter(),
            )
            .await
            .unwrap();
        let batches = collect_read_tasks(tasks, 16).await;
        let values: Vec<Option<i32>> = batches
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(values, vec![Some(100)]);
    }

    #[test_log::test(tokio::test)]
    async fn test_compressing_buffer() {
        let fs = FsFixture::default();

        let written_file = create_some_file(&fs, ConcreteFileVersion::V2_0).await;
        let file_scheduler = fs
            .scheduler
            .open_file(&fs.tmp_path, &CachedFileSize::unknown())
            .await
            .unwrap();

        // We can specify the projection as part of the read operation via read_stream_projected
        let file_reader = FileReader::try_open(
            file_scheduler.clone(),
            None,
            Arc::<DecoderPlugins>::default(),
            &test_cache(),
            FileReaderOptions::default(),
        )
        .await
        .unwrap();

        let mut projection = written_file.schema.project(&["score"]).unwrap();
        for field in projection.fields.iter_mut() {
            field
                .metadata
                .insert("lance:compression".to_string(), "zstd".to_string());
        }
        let projection = ReaderProjection {
            column_indices: projection.fields.iter().map(|f| f.id as u32).collect(),
            schema: Arc::new(projection),
        };

        let batch_stream = file_reader
            .read_stream_projected(
                lance_io::ReadBatchParams::RangeFull,
                1024,
                16,
                projection.clone(),
                FilterExpression::no_filter(),
            )
            .await
            .unwrap();

        let projection_arrow = Arc::new(ArrowSchema::from(projection.schema.as_ref()));
        verify_expected(
            &written_file.data,
            batch_stream,
            1024,
            Some(Box::new(move |batch: &RecordBatch| {
                batch.project_by_schema(&projection_arrow).unwrap()
            })),
        )
        .await;
    }

    #[tokio::test]
    async fn test_read_all() {
        let fs = FsFixture::default();
        let WrittenFile { data, .. } = create_some_file(&fs, ConcreteFileVersion::V2_0).await;
        let total_rows = data.iter().map(|batch| batch.num_rows()).sum::<usize>();

        let file_scheduler = fs
            .scheduler
            .open_file(&fs.tmp_path, &CachedFileSize::unknown())
            .await
            .unwrap();
        let file_reader = FileReader::try_open(
            file_scheduler.clone(),
            None,
            Arc::<DecoderPlugins>::default(),
            &test_cache(),
            FileReaderOptions::default(),
        )
        .await
        .unwrap();

        let batches = file_reader
            .read_stream(
                lance_io::ReadBatchParams::RangeFull,
                total_rows as u32,
                16,
                FilterExpression::no_filter(),
            )
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), total_rows);
    }

    #[rstest]
    #[tokio::test]
    async fn test_blocking_take(
        #[values(
            ConcreteFileVersion::V2_0,
            ConcreteFileVersion::V2_1,
            ConcreteFileVersion::V2_2
        )]
        version: ConcreteFileVersion,
    ) {
        let fs = FsFixture::default();
        let WrittenFile { data, schema, .. } = create_some_file(&fs, version).await;
        let total_rows = data.iter().map(|batch| batch.num_rows()).sum::<usize>();

        let file_scheduler = fs
            .scheduler
            .open_file(&fs.tmp_path, &CachedFileSize::unknown())
            .await
            .unwrap();
        let file_reader = FileReader::try_open(
            file_scheduler.clone(),
            Some(
                versions::reader_projection_from_column_names(version, &schema, &["score"])
                    .unwrap(),
            ),
            Arc::<DecoderPlugins>::default(),
            &test_cache(),
            FileReaderOptions::default(),
        )
        .await
        .unwrap();

        let batches = tokio::task::spawn_blocking(move || {
            file_reader
                .read_stream_projected_blocking(
                    lance_io::ReadBatchParams::Indices(UInt32Array::from(vec![0, 1, 2, 3, 4])),
                    total_rows as u32,
                    None,
                    FilterExpression::no_filter(),
                )
                .unwrap()
                .collect::<ArrowResult<Vec<_>>>()
                .unwrap()
        })
        .await
        .unwrap();

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 5);
        assert_eq!(batches[0].num_columns(), 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_drop_in_progress() {
        let fs = FsFixture::default();
        let WrittenFile { data, .. } = create_some_file(&fs, ConcreteFileVersion::V2_0).await;
        let total_rows = data.iter().map(|batch| batch.num_rows()).sum::<usize>();

        let file_scheduler = fs
            .scheduler
            .open_file(&fs.tmp_path, &CachedFileSize::unknown())
            .await
            .unwrap();
        let file_reader = FileReader::try_open(
            file_scheduler.clone(),
            None,
            Arc::<DecoderPlugins>::default(),
            &test_cache(),
            FileReaderOptions::default(),
        )
        .await
        .unwrap();

        let mut batches = file_reader
            .read_stream(
                lance_io::ReadBatchParams::RangeFull,
                (total_rows / 10) as u32,
                16,
                FilterExpression::no_filter(),
            )
            .await
            .unwrap();

        drop(file_reader);

        let batch = batches.next().await.unwrap().unwrap();
        assert!(batch.num_rows() > 0);

        // Drop in-progress scan
        drop(batches);
    }

    #[tokio::test]
    async fn drop_while_scheduling() {
        // This is a bit of a white-box test, pokes at the internals.  We want to
        // test the case where the read stream is dropped before the scheduling
        // thread finishes.  We can't do that in a black-box fashion because the
        // scheduling thread runs in the background and there is no easy way to
        // pause / gate it.

        // It's a regression for a bug where the scheduling thread would panic
        // if the stream was dropped before it finished.

        let fs = FsFixture::default();
        let written_file = create_some_file(&fs, ConcreteFileVersion::V2_0).await;
        let total_rows = written_file
            .data
            .iter()
            .map(|batch| batch.num_rows())
            .sum::<usize>();

        let file_scheduler = fs
            .scheduler
            .open_file(&fs.tmp_path, &CachedFileSize::unknown())
            .await
            .unwrap();
        let file_reader = FileReader::try_open(
            file_scheduler.clone(),
            None,
            Arc::<DecoderPlugins>::default(),
            &test_cache(),
            FileReaderOptions::default(),
        )
        .await
        .unwrap();

        let projection = versions::reader_projection_from_whole_schema(
            &written_file.schema,
            ConcreteFileVersion::V2_0,
        );
        let column_infos = file_reader.metadata().column_infos.clone();
        let mut decode_scheduler = DecodeBatchScheduler::try_new(
            &projection.schema,
            &projection.column_indices,
            &column_infos,
            &vec![],
            total_rows as u64,
            Arc::<DecoderPlugins>::default(),
            file_reader.scheduler(),
            test_cache(),
            &FilterExpression::no_filter(),
            &DecoderConfig::default(),
        )
        .await
        .unwrap();

        let range = 0..total_rows as u64;

        let (tx, rx) = mpsc::unbounded_channel();

        // Simulate the stream / decoder being dropped
        drop(rx);

        // Scheduling should not panic
        decode_scheduler.schedule_range(
            range,
            &FilterExpression::no_filter(),
            tx,
            file_reader.scheduler(),
        )
    }

    #[tokio::test]
    async fn test_read_empty_range() {
        let fs = FsFixture::default();
        create_some_file(&fs, ConcreteFileVersion::V2_0).await;

        let file_scheduler = fs
            .scheduler
            .open_file(&fs.tmp_path, &CachedFileSize::unknown())
            .await
            .unwrap();
        let file_reader = FileReader::try_open(
            file_scheduler.clone(),
            None,
            Arc::<DecoderPlugins>::default(),
            &test_cache(),
            FileReaderOptions::default(),
        )
        .await
        .unwrap();

        // All ranges empty, no data
        let batches = file_reader
            .read_stream(
                lance_io::ReadBatchParams::Range(0..0),
                1024,
                16,
                FilterExpression::no_filter(),
            )
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert_eq!(batches.len(), 0);

        // Some ranges empty
        let batches = file_reader
            .read_stream(
                lance_io::ReadBatchParams::Ranges(Arc::new([0..1, 2..2])),
                1024,
                16,
                FilterExpression::no_filter(),
            )
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(batches.len(), 1);
    }

    async fn write_file_with_global_buffer(fs: &FsFixture, buffer: Bytes) {
        let lance_schema =
            lance_core::datatypes::Schema::try_from(&ArrowSchema::new(vec![Field::new(
                "foo",
                DataType::Int32,
                true,
            )]))
            .unwrap();

        let mut file_writer = versions::v2_1::create_writer(
            fs.object_store.create(&fs.tmp_path).await.unwrap(),
            lance_schema,
            FileWriterOptions::default(),
        )
        .unwrap();

        let buf_index = file_writer.add_global_buffer(buffer).await.unwrap();
        assert_eq!(buf_index, 1);

        file_writer.finish().await.unwrap();
    }

    #[derive(Clone, Copy, Debug)]
    enum MetadataReadPath {
        Full,
        Indexed,
    }

    #[derive(Clone, Copy, Debug)]
    enum InvalidGboDescriptor {
        Unaligned,
        PastEof,
        Overflowing,
    }

    #[rstest]
    #[case::full_unaligned(MetadataReadPath::Full, InvalidGboDescriptor::Unaligned, "not aligned")]
    #[case::full_past_eof(MetadataReadPath::Full, InvalidGboDescriptor::PastEof, "outside file")]
    #[case::full_overflowing(
        MetadataReadPath::Full,
        InvalidGboDescriptor::Overflowing,
        "overflows"
    )]
    #[case::indexed_unaligned(
        MetadataReadPath::Indexed,
        InvalidGboDescriptor::Unaligned,
        "not aligned"
    )]
    #[case::indexed_past_eof(
        MetadataReadPath::Indexed,
        InvalidGboDescriptor::PastEof,
        "outside file"
    )]
    #[case::indexed_overflowing(
        MetadataReadPath::Indexed,
        InvalidGboDescriptor::Overflowing,
        "overflows"
    )]
    #[tokio::test]
    async fn test_metadata_rejects_invalid_gbo_descriptor(
        #[case] read_path: MetadataReadPath,
        #[case] invalid_descriptor: InvalidGboDescriptor,
        #[case] expected_message: &str,
    ) {
        let fs = FsFixture::default();
        write_file_with_global_buffer(&fs, Bytes::from_static(b"hello")).await;

        let mut file_bytes = fs
            .object_store
            .read_one_all(&fs.tmp_path)
            .await
            .unwrap()
            .to_vec();
        let file_len = file_bytes.len() as u64;
        let footer = FileReader::decode_footer(&Bytes::copy_from_slice(&file_bytes)).unwrap();
        let gbo_table_start = usize::try_from(footer.global_buff_offsets_start).unwrap();
        let alignment = PAGE_BUFFER_ALIGNMENT as u64;
        let (position, size) = match invalid_descriptor {
            InvalidGboDescriptor::Unaligned => (1, 0),
            InvalidGboDescriptor::PastEof => (((file_len + alignment) / alignment) * alignment, 0),
            InvalidGboDescriptor::Overflowing => (u64::MAX - (u64::MAX % alignment), alignment),
        };
        file_bytes[gbo_table_start..gbo_table_start + 8].copy_from_slice(&position.to_le_bytes());
        file_bytes[gbo_table_start + 8..gbo_table_start + 16].copy_from_slice(&size.to_le_bytes());
        fs.object_store
            .put(&fs.tmp_path, &file_bytes)
            .await
            .unwrap();

        let scheduler = fs
            .scheduler
            .open_file(&fs.tmp_path, &CachedFileSize::unknown())
            .await
            .unwrap();
        let error = match read_path {
            MetadataReadPath::Full => FileReader::read_all_metadata(&scheduler).await.map(|_| ()),
            MetadataReadPath::Indexed => FileReader::read_metadata_index(&scheduler)
                .await
                .map(|_| ()),
        }
        .expect_err("invalid GBO descriptor must fail before metadata I/O");

        assert!(
            matches!(error, lance_core::Error::InvalidInput { .. }),
            "expected InvalidInput, got {error:?}"
        );
        assert!(
            error.to_string().contains(expected_message),
            "unexpected error: {error}"
        );
    }

    /// A global buffer that fits inside the tail region captured at open is served
    /// from memory with no additional I/O.  A buffer larger than that window cannot
    /// fit and falls back to a dedicated read.  Both must round-trip correctly.
    #[rstest]
    #[case::within_tail_window(true)]
    #[case::outside_tail_window(false)]
    #[tokio::test]
    async fn test_read_global_buffer(#[case] within_window: bool) {
        let fs = FsFixture::default();

        let block_size = fs.object_store.block_size();
        let buffer = if within_window {
            Bytes::from_static(b"hello")
        } else {
            Bytes::from(vec![7u8; 2 * block_size])
        };
        let expected_read_iops = if within_window { 0 } else { 1 };

        write_file_with_global_buffer(&fs, buffer.clone()).await;

        let file_scheduler = fs
            .scheduler
            .open_file(&fs.tmp_path, &CachedFileSize::unknown())
            .await
            .unwrap();
        let file_reader = FileReader::try_open(
            file_scheduler,
            None,
            Arc::<DecoderPlugins>::default(),
            &test_cache(),
            FileReaderOptions::default(),
        )
        .await
        .unwrap();

        // The user buffer should be retained only when it fits the tail window, and
        // the schema (buffer 0) is never retained.
        let retained = &file_reader.metadata().retained_global_buffers;
        assert!(!retained.contains_key(&0), "schema must not be retained");
        assert_eq!(retained.contains_key(&1), within_window);

        // Reset the IO counters so we only measure the read_global_buffer call.
        fs.object_store.io_stats_incremental();

        let buf = file_reader.read_global_buffer(1).await.unwrap();
        assert_eq!(buf, buffer);

        let stats = fs.object_store.io_stats_incremental();
        assert_eq!(stats.read_iops, expected_read_iops);
    }

    /// A file whose only global buffer is the schema (i.e. a plain data file, the
    /// common case) must retain nothing — there is no user buffer to serve.
    #[tokio::test]
    async fn test_read_global_buffer_no_user_buffers() {
        let fs = FsFixture::default();
        create_some_file(&fs, ConcreteFileVersion::V2_1).await;

        let file_scheduler = fs
            .scheduler
            .open_file(&fs.tmp_path, &CachedFileSize::unknown())
            .await
            .unwrap();
        let file_reader = FileReader::try_open(
            file_scheduler,
            None,
            Arc::<DecoderPlugins>::default(),
            &test_cache(),
            FileReaderOptions::default(),
        )
        .await
        .unwrap();

        let metadata = file_reader.metadata();
        assert_eq!(metadata.file_buffers.len(), 1, "expected only the schema");
        assert!(
            metadata.retained_global_buffers.is_empty(),
            "a file with no user global buffers must retain nothing"
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_deep_size_of_includes_column_metadata(
        #[values(
            ConcreteFileVersion::V2_0,
            ConcreteFileVersion::V2_1,
            ConcreteFileVersion::V2_2,
            ConcreteFileVersion::V2_3
        )]
        version: ConcreteFileVersion,
    ) {
        // Regression test: CachedFileMetadata::deep_size_of must account for
        // column_metadatas and column_infos, otherwise the moka cache weigher
        // dramatically underestimates entry sizes and never evicts, causing
        // unbounded memory growth on random-access workloads.
        use lance_core::deepsize::DeepSizeOf;

        let fs = FsFixture::default();
        let _written = create_some_file(&fs, version).await;
        let cache = test_cache();
        let file_scheduler = fs
            .scheduler
            .open_file(&fs.tmp_path, &CachedFileSize::unknown())
            .await
            .unwrap();
        let file_reader = FileReader::try_open(
            file_scheduler,
            None,
            Arc::<DecoderPlugins>::default(),
            &cache,
            FileReaderOptions::default(),
        )
        .await
        .unwrap();

        let metadata = file_reader.metadata();
        let deep_size = metadata.deep_size_of();

        // The file has multiple columns (score, location, categories, binary,
        // maybe large_bin). The deep_size_of must be substantially more than
        // just the schema — it should include column_metadatas + column_infos.
        // A naive implementation that ignores these fields reports < 1 KB;
        // a correct one should report at least several KB for this test file.
        assert!(
            deep_size > 1024,
            "deep_size_of ({deep_size}) is suspiciously small — \
             column_metadatas and column_infos may not be accounted for"
        );

        // Verify column_metadatas is non-empty (sanity check).
        assert!(
            !metadata.column_metadatas.is_empty(),
            "Expected non-empty column_metadatas"
        );

        // Verify the size scales with the number of columns: a file with more
        // columns should have a larger deep_size_of.
        let num_columns = metadata.column_metadatas.len();
        assert!(
            deep_size > num_columns * 50,
            "deep_size_of ({deep_size}) should scale with column count ({num_columns})"
        );
    }

    #[tokio::test]
    async fn test_read_global_buffer_out_of_range() {
        let fs = FsFixture::default();

        write_file_with_global_buffer(&fs, Bytes::from_static(b"hello")).await;

        let file_scheduler = fs
            .scheduler
            .open_file(&fs.tmp_path, &CachedFileSize::unknown())
            .await
            .unwrap();
        let file_reader = FileReader::try_open(
            file_scheduler,
            None,
            Arc::<DecoderPlugins>::default(),
            &test_cache(),
            FileReaderOptions::default(),
        )
        .await
        .unwrap();

        // The file has two global buffers (schema at 0, "hello" at 1); index 2 is
        // out of range and must surface a descriptive error rather than panicking.
        let err = file_reader.read_global_buffer(2).await.unwrap_err();
        assert!(
            matches!(err, lance_core::Error::InvalidInput { .. }),
            "expected InvalidInput, got: {err:?}"
        );
        let msg = err.to_string();
        assert!(msg.contains('2'), "error should mention the index: {msg}");
    }

    // Exercises the projection length-validation walk in isolation, feeding
    // synthetic per-column lengths so we can reach cases no current writer can
    // actually produce -- in particular a struct whose children diverge in
    // length, which the decoders would otherwise panic on or misread.
    #[rstest]
    fn test_validate_struct_child_lengths(#[values(false, true)] is_structural: bool) {
        let run = |dt: DataType, indices: &[u32], lengths: Vec<u64>| -> lance_core::Result<u64> {
            let arrow = ArrowSchema::new(vec![Field::new("s", dt, true)]);
            let schema = Schema::try_from(&arrow).unwrap();
            if is_structural {
                versions::v2_1::test_projection_length(&schema, indices, &lengths)
            } else {
                versions::v2_0::test_projection_length(&schema, indices, &lengths)
            }
        };

        let struct_ty = || {
            DataType::Struct(Fields::from(vec![
                Field::new("a", DataType::Int32, true),
                Field::new("b", DataType::Int32, true),
            ]))
        };

        // In 2.1 a struct contributes no column of its own (just its two leaves);
        // in 2.0 it also has its own column first.
        let (indices, equal, unequal): (&[u32], Vec<u64>, Vec<u64>) = if is_structural {
            (&[0, 1], vec![5, 5], vec![5, 3])
        } else {
            (&[0, 1, 2], vec![5, 5, 5], vec![5, 5, 3])
        };

        assert_eq!(run(struct_ty(), indices, equal).unwrap(), 5);

        let err = run(struct_ty(), indices, unequal).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("differing lengths") && msg.contains('b'),
            "expected a child-length error naming 'b', got: {msg}"
        );
    }

    #[test]
    fn test_validate_v2_0_unloaded_blob_projection_is_opaque() {
        let metadata = HashMap::from([(BLOB_META_KEY.to_string(), "true".to_string())]);
        let arrow = ArrowSchema::new(vec![
            Field::new("blob", DataType::LargeBinary, true).with_metadata(metadata),
        ]);
        let mut schema = Schema::try_from(&arrow).unwrap();
        schema.fields[0].unloaded_mut();
        let projection = ReaderProjection {
            schema: Arc::new(schema),
            column_indices: vec![0],
        };
        let rows = versions::v2_0::test_projection_length(
            &projection.schema,
            &projection.column_indices,
            &[3],
        )
        .unwrap();

        assert_eq!(rows, 3);
    }

    #[test]
    fn test_validate_length_list_and_empty_struct() {
        let validate = |dt: DataType,
                        is_structural: bool,
                        indices: &[u32],
                        lengths: Vec<u64>|
         -> lance_core::Result<u64> {
            let arrow = ArrowSchema::new(vec![Field::new("f", dt, true)]);
            let schema = Schema::try_from(&arrow).unwrap();
            if is_structural {
                versions::v2_1::test_projection_length(&schema, indices, &lengths)
            } else {
                versions::v2_0::test_projection_length(&schema, indices, &lengths)
            }
        };

        // A list's items have a different cardinality than its rows; that gap
        // must not be flagged as a mismatch. In 2.0 the list is an offsets column
        // (rows) plus an items column (item count); in 2.1 it is a single column
        // whose page rows are the top-level row count.
        let list_ty = DataType::List(Arc::new(Field::new("item", DataType::Int32, true)));
        assert_eq!(
            validate(list_ty.clone(), false, &[0, 1], vec![5, 17]).unwrap(),
            5
        );
        assert_eq!(validate(list_ty, true, &[0], vec![5]).unwrap(), 5);

        // A list of structs: the struct's children sit below the list boundary, so
        // their (item-count) lengths must NOT be compared against the list's row
        // count. In 2.0 each field has a column [list, struct, a, b] = [6, 6, 29,
        // 29]; the list resolves to its own row count (6) and the struct's longer
        // children are not flagged. (Regression: this previously errored because
        // the nested struct's children were checked against the struct's count.)
        let list_of_struct = DataType::List(Arc::new(Field::new(
            "item",
            DataType::Struct(Fields::from(vec![
                Field::new("a", DataType::Int32, true),
                Field::new("b", DataType::Int32, true),
            ])),
            true,
        )));
        assert_eq!(
            validate(
                list_of_struct.clone(),
                false,
                &[0, 1, 2, 3],
                vec![6, 6, 29, 29]
            )
            .unwrap(),
            6
        );
        // In 2.1 only the two leaves carry columns; still no false mismatch.
        assert_eq!(
            validate(list_of_struct, true, &[0, 1], vec![29, 29]).unwrap(),
            29
        );

        // An empty struct contributes a single column and validates to its length.
        let empty_struct = DataType::Struct(Fields::empty());
        assert_eq!(
            validate(empty_struct.clone(), false, &[0], vec![9]).unwrap(),
            9
        );
        assert_eq!(validate(empty_struct, true, &[0], vec![9]).unwrap(), 9);
    }
}
