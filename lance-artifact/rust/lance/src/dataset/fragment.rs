// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Wraps a Fragment of the dataset.

pub mod session;
pub mod write;

use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::ops::Range;
use std::sync::Arc;

use arrow::compute::concat_batches;
use arrow_array::cast::as_primitive_array;
use arrow_array::types::UInt64Type;
use arrow_array::{
    Array, RecordBatch, RecordBatchReader, StructArray, UInt32Array, UInt64Array, new_null_array,
};
use arrow_schema::Schema as ArrowSchema;
use datafusion::logical_expr::Expr;
use datafusion::scalar::ScalarValue;
use futures::future::{BoxFuture, try_join_all};
use futures::{FutureExt, StreamExt, TryFutureExt, TryStreamExt, join, stream};
use lance_arrow::json::{convert_json_columns, has_json_fields, is_arrow_json_field};
use lance_arrow::{RecordBatchExt, SchemaExt};
use lance_core::datatypes::{OnMissing, OnTypeMismatch, SchemaCompareOptions};
use lance_core::utils::address::RowAddress;
use lance_core::utils::deletion::DeletionVector;
use lance_core::utils::tokio::get_num_compute_intensive_cpus;
use lance_core::{
    Error, Result,
    cache::{CacheKey, CacheKeySchema, KeyBuilder},
    datatypes::Schema,
};
use lance_core::{
    ROW_ADDR, ROW_ADDR_FIELD, ROW_CREATED_AT_VERSION_FIELD, ROW_ID, ROW_ID_FIELD,
    ROW_LAST_UPDATED_AT_VERSION_FIELD,
};
use lance_datafusion::utils::StreamingWriteSource;
use lance_encoding::decoder::DecoderPlugins;
use lance_file::reader::{
    CachedFileMetadata, FileMetadataIndex, FileReaderOptions, ProjectedFileReader,
};
use lance_file::version::ConcreteFileVersion;
use lance_file::versions::v1::reader::{FileReader as V1FileReader, read_batch as v1_read_batch};
use lance_file::{LanceEncodingsIo, determine_file_version, versions as file_versions};
use lance_io::ReadBatchParams;
use lance_io::scheduler::{FileScheduler, ScanScheduler, SchedulerConfig};
use lance_io::utils::CachedFileSize;
use lance_table::format::overlay::TOMBSTONE_FIELD_ID;
use lance_table::format::{DataFile, DeletionFile, Fragment};
use lance_table::io::deletion::{deletion_file_path, write_deletion_file};
use lance_table::rowids::RowIdSequence;
use lance_table::utils::stream::{
    ReadBatchFutStream, ReadBatchTask, ReadBatchTaskStream, RowIdAndDeletesConfig,
    wrap_with_row_id_and_delete,
};
use roaring::RoaringBitmap;

use self::write::FragmentCreateBuilder;

use super::hash_joiner::HashJoiner;
use super::rowids::load_row_id_sequence;
use super::scanner::Scanner;

use super::updater::Updater;
use super::{NewColumnTransform, WriteParams, schema_evolution};
use crate::dataset::Dataset;
use crate::dataset::fragment::session::FragmentSession;
use crate::dataset::overlay::{
    OverlayReadPlanner, merge_overlay_batch, plan_overlays, resolve_overlays,
};
use crate::io::deletion::read_dataset_deletion_file;

/// Result of [`FileFragment::update_columns_with_offsets`]: updated fragment metadata, modified field ids,
/// and physical row offsets that matched the join (for stable row-id version metadata).
#[derive(Debug, Clone)]
pub struct FragmentUpdateColumnsResult {
    pub fragment: Fragment,
    pub fields_modified: Vec<u32>,
    /// Physical row offsets (0-based within this fragment) whose columns were rewritten from the right-hand stream.
    pub matched_offsets: RoaringBitmap,
}

/// A Fragment of a Lance [`Dataset`].
///
/// The interface is modeled after `pyarrow.dataset.Fragment`.
#[derive(Debug, Clone)]
pub struct FileFragment {
    dataset: Arc<Dataset>,

    pub(super) metadata: Fragment,
}

const DEFAULT_BATCH_READ_SIZE: u32 = 1024;

/// A trait for file readers to be implemented by both the v1 and v2 readers
///
/// The `read_*_tasks` methods are async because for v2 files they drive
/// the decode scheduler's `initialize` step (and, for small reads, the
/// synchronous scheduling that follows) before returning the stream.
/// Doing that work here keeps it on whichever task awaits this call —
/// typically a per-fragment `tokio::spawn` — instead of smuggling it
/// into the first poll of the returned stream.
#[allow(clippy::len_without_is_empty)]
pub trait GenericFileReader: std::fmt::Debug + Send + Sync {
    /// Reads the requested range of rows from the file, returning as a stream
    /// of tasks.
    fn read_range_tasks(
        &self,
        range: Range<u64>,
        batch_size: u32,
        projection: Arc<lance_core::datatypes::Schema>,
    ) -> BoxFuture<'_, Result<ReadBatchTaskStream>>;
    /// Reads the requested ranges of rows from the file, only supported by v2
    fn read_ranges_tasks(
        &self,
        ranges: Arc<[Range<u64>]>,
        batch_size: u32,
        projection: Arc<lance_core::datatypes::Schema>,
    ) -> BoxFuture<'_, Result<ReadBatchTaskStream>>;
    /// Reads all rows from the file, returning as a stream of tasks
    fn read_all_tasks(
        &self,
        batch_size: u32,
        projection: Arc<lance_core::datatypes::Schema>,
    ) -> BoxFuture<'_, Result<ReadBatchTaskStream>>;
    /// Take specific rows from the file, returning as a stream of tasks
    fn take_all_tasks(
        &self,
        indices: &[u32],
        batch_size: u32,
        projection: Arc<lance_core::datatypes::Schema>,
        take_priority: Option<u32>,
    ) -> BoxFuture<'_, Result<ReadBatchTaskStream>>;

    /// Return the number of rows in the file
    fn len(&self) -> u32;

    /// Schema of the reader
    fn projection(&self) -> &Arc<Schema>;

    /// Get storage statistics for this file (ignored by v1 reader)
    fn storage_stats(&self) -> Result<Vec<(u32, u64)>>;

    // Helper functions to fallback to the legacy implementation while we
    // slowly migrate functionality over to the generic reader

    // Clone the reader, this is needed because Box<dyn Foo: Clone> doesn't
    // implement Clone
    fn clone_box(&self) -> Box<dyn GenericFileReader>;
    // Return true if the reader is a v1 reader
    fn is_legacy(&self) -> bool;
    // Return a reference to the legacy reader, panics if called on a v2
    // file.
    fn as_legacy(&self) -> &V1FileReader {
        self.as_legacy_opt()
            .expect("legacy function called on v2 file")
    }
    // Return a reference to the legacy reader if this is a v1 reader and
    // return None otherwise
    fn as_legacy_opt(&self) -> Option<&V1FileReader>;
    // Return a mutable reference to the legacy reader if this is a v1 reader
    // and return None otherwise
    fn as_legacy_opt_mut(&mut self) -> Option<&mut V1FileReader>;
}

fn ranges_to_tasks(
    reader: &V1FileReader,
    ranges: Vec<(i32, Range<usize>)>,
    projection: Arc<Schema>,
) -> ReadBatchTaskStream {
    let reader = reader.clone();
    stream::iter(ranges)
        .map(move |(batch_idx, range)| {
            let num_rows = range.end - range.start;
            let reader = reader.clone();
            let projection = projection.clone();
            let task = tokio::task::spawn(async move {
                v1_read_batch(
                    &reader,
                    &ReadBatchParams::Range(range.clone()),
                    &projection,
                    batch_idx,
                )
                .await
            })
            .map(|task_out| task_out.unwrap())
            .boxed();
            ReadBatchTask {
                task,
                num_rows: num_rows as u32,
            }
        })
        .boxed()
}

#[derive(Clone, Debug)]
struct V1Reader {
    reader: V1FileReader,
    projection: Arc<Schema>,
}

impl V1Reader {
    fn new(reader: V1FileReader, projection: Arc<Schema>) -> Self {
        Self { reader, projection }
    }
}

impl GenericFileReader for V1Reader {
    /// Reads the requested range of rows from the file, returning as a stream
    fn read_range_tasks(
        &self,
        range: Range<u64>,
        batch_size: u32,
        projection: Arc<Schema>,
    ) -> BoxFuture<'_, Result<ReadBatchTaskStream>> {
        let mut to_skip = range.start as u32;
        let mut remaining = range.end as u32 - to_skip;
        let mut ranges = Vec::new();
        let mut batch_idx = 0;
        while remaining > 0 {
            let next_batch_len = self.reader.num_rows_in_batch(batch_idx) as u32;
            let next_batch_idx = batch_idx;
            batch_idx += 1;
            if to_skip >= next_batch_len {
                to_skip -= next_batch_len;
                continue;
            }
            let batch_start = to_skip;
            to_skip = 0;
            let batch_end = next_batch_len.min(batch_start + remaining);
            remaining -= batch_end - batch_start;
            for chunk_start in (batch_start..batch_end).step_by(batch_size as usize) {
                let chunk_end = (chunk_start + batch_size).min(batch_end);
                ranges.push((next_batch_idx, (chunk_start as usize..chunk_end as usize)));
            }
        }
        let stream = ranges_to_tasks(&self.reader, ranges, projection);
        async move { Ok(stream) }.boxed()
    }

    fn read_all_tasks(
        &self,
        batch_size: u32,
        projection: Arc<Schema>,
    ) -> BoxFuture<'_, Result<ReadBatchTaskStream>> {
        let ranges = (0..self.reader.num_batches())
            .flat_map(move |batch_idx| {
                let rows_in_batch = self.reader.num_rows_in_batch(batch_idx as i32);
                (0..rows_in_batch)
                    .step_by(batch_size as usize)
                    .map(move |start| {
                        let end = (start + batch_size as usize).min(rows_in_batch);
                        (batch_idx as i32, start..end)
                    })
            })
            .collect::<Vec<_>>();
        let stream = ranges_to_tasks(&self.reader, ranges, projection);
        async move { Ok(stream) }.boxed()
    }

    fn read_ranges_tasks(
        &self,
        _ranges: Arc<[Range<u64>]>,
        _batch_size: u32,
        _projection: Arc<Schema>,
    ) -> BoxFuture<'_, Result<ReadBatchTaskStream>> {
        async move {
            Err(Error::internal(
                "Attempt to perform FilteredRead on v1 files".to_string(),
            ))
        }
        .boxed()
    }

    fn take_all_tasks(
        &self,
        indices: &[u32],
        _batch_size: u32,
        projection: Arc<Schema>,
        _take_priority: Option<u32>,
    ) -> BoxFuture<'_, Result<ReadBatchTaskStream>> {
        let indices_vec = indices.to_vec();
        let reader = self.reader.clone();
        let num_rows = indices.len() as u32;
        async move {
            // In the new path the row id is added by the fragment and not the file
            let task_fut =
                async move { reader.take(&indices_vec, projection.as_ref()).await }.boxed();
            let task = std::future::ready(ReadBatchTask {
                task: task_fut,
                num_rows,
            })
            .boxed();
            Ok(futures::stream::once(task).boxed())
        }
        .boxed()
    }

    fn projection(&self) -> &Arc<Schema> {
        &self.projection
    }

    /// Return the number of rows in the file
    fn len(&self) -> u32 {
        self.reader.len() as u32
    }

    fn storage_stats(&self) -> Result<Vec<(u32, u64)>> {
        // No-op for v1 files
        Ok(Vec::new())
    }

    fn clone_box(&self) -> Box<dyn GenericFileReader> {
        Box::new(self.clone())
    }

    fn is_legacy(&self) -> bool {
        true
    }

    fn as_legacy_opt(&self) -> Option<&V1FileReader> {
        Some(&self.reader)
    }

    fn as_legacy_opt_mut(&mut self) -> Option<&mut V1FileReader> {
        Some(&mut self.reader)
    }
}

mod v2_adapter {
    use lance_encoding::decoder::FilterExpression;

    use super::*;

    #[derive(Debug, Clone)]
    pub struct Reader {
        reader: Arc<ProjectedFileReader>,
        projection: Arc<Schema>,
        field_id_to_column_idx: Arc<BTreeMap<u32, u32>>,
        default_priority: u32,
        file_scheduler: FileScheduler,
    }

    impl Reader {
        pub fn new(
            reader: Arc<ProjectedFileReader>,
            projection: Arc<Schema>,
            field_id_to_column_idx: Arc<BTreeMap<u32, u32>>,
            default_priority: u32,
            file_scheduler: FileScheduler,
        ) -> Self {
            Self {
                reader,
                projection,
                field_id_to_column_idx,
                default_priority,
                file_scheduler,
            }
        }
    }

    impl GenericFileReader for Reader {
        /// Reads the requested range of rows from the file, returning as a stream
        fn read_range_tasks(
            &self,
            range: Range<u64>,
            batch_size: u32,
            projection: Arc<Schema>,
        ) -> BoxFuture<'_, Result<ReadBatchTaskStream>> {
            async move {
                let projection = file_versions::reader_projection_from_field_ids(
                    self.reader.version(),
                    projection.as_ref(),
                    self.field_id_to_column_idx.as_ref(),
                )?;
                Ok(self
                    .reader
                    .read_tasks(
                        ReadBatchParams::Range(range.start as usize..range.end as usize),
                        batch_size,
                        Some(projection),
                        FilterExpression::no_filter(),
                    )
                    .await?
                    .map(|v2_task| ReadBatchTask {
                        task: v2_task.task.map_err(Error::from).boxed(),
                        num_rows: v2_task.num_rows,
                    })
                    .boxed())
            }
            .boxed()
        }

        fn read_ranges_tasks(
            &self,
            ranges: Arc<[Range<u64>]>,
            batch_size: u32,
            projection: Arc<Schema>,
        ) -> BoxFuture<'_, Result<ReadBatchTaskStream>> {
            async move {
                let projection = file_versions::reader_projection_from_field_ids(
                    self.reader.version(),
                    projection.as_ref(),
                    self.field_id_to_column_idx.as_ref(),
                )?;
                Ok(self
                    .reader
                    .read_tasks(
                        ReadBatchParams::Ranges(ranges),
                        batch_size,
                        Some(projection),
                        FilterExpression::no_filter(),
                    )
                    .await?
                    .map(|v2_task| ReadBatchTask {
                        task: v2_task.task.map_err(Error::from).boxed(),
                        num_rows: v2_task.num_rows,
                    })
                    .boxed())
            }
            .boxed()
        }

        fn read_all_tasks(
            &self,
            batch_size: u32,
            projection: Arc<Schema>,
        ) -> BoxFuture<'_, Result<ReadBatchTaskStream>> {
            async move {
                let projection = file_versions::reader_projection_from_field_ids(
                    self.reader.version(),
                    projection.as_ref(),
                    self.field_id_to_column_idx.as_ref(),
                )?;
                Ok(self
                    .reader
                    .read_tasks(
                        ReadBatchParams::RangeFull,
                        batch_size,
                        Some(projection),
                        FilterExpression::no_filter(),
                    )
                    .await?
                    .map(|v2_task| ReadBatchTask {
                        task: v2_task.task.map_err(Error::from).boxed(),
                        num_rows: v2_task.num_rows,
                    })
                    .boxed())
            }
            .boxed()
        }

        fn take_all_tasks(
            &self,
            indices: &[u32],
            batch_size: u32,
            projection: Arc<Schema>,
            take_priority: Option<u32>,
        ) -> BoxFuture<'_, Result<ReadBatchTaskStream>> {
            let indices = UInt32Array::from(indices.to_vec());
            async move {
                let projection = file_versions::reader_projection_from_field_ids(
                    self.reader.version(),
                    projection.as_ref(),
                    self.field_id_to_column_idx.as_ref(),
                )?;

                let reader = if let Some(take_priority) = take_priority {
                    let op_priority = ((take_priority as u64) << 32) | self.default_priority as u64;
                    let scheduler = self.file_scheduler.with_priority(op_priority);
                    Arc::new(
                        self.reader
                            .with_scheduler(Arc::new(LanceEncodingsIo::new(scheduler))),
                    )
                } else {
                    self.reader.clone()
                };

                Ok(reader
                    .read_tasks(
                        ReadBatchParams::Indices(indices),
                        batch_size,
                        Some(projection),
                        FilterExpression::no_filter(),
                    )
                    .await?
                    .map(|v2_task| ReadBatchTask {
                        task: v2_task.task.map_err(Error::from).boxed(),
                        num_rows: v2_task.num_rows,
                    })
                    .boxed())
            }
            .boxed()
        }

        fn storage_stats(&self) -> Result<Vec<(u32, u64)>> {
            let file_statistics = self.reader.file_statistics().ok_or_else(|| {
                Error::internal("storage_stats requires full file metadata".to_string())
            })?;
            let column_idx_to_field_id = self
                .field_id_to_column_idx
                .iter()
                .map(|(field_id, column_idx)| (*column_idx, *field_id))
                .collect::<HashMap<_, _>>();

            let mut stats = Vec::new();
            // Some fields span more than one column.  We assume a column that doesn't have an
            // entry in the field_id_to_column_idx map is a continuation of the previous field.
            let mut current_field_id = 0;
            for (column_idx, col_stats) in file_statistics.columns.iter().enumerate() {
                if let Some(field_id) = column_idx_to_field_id.get(&(column_idx as u32)) {
                    current_field_id = *field_id;
                }
                stats.push((current_field_id, col_stats.size_bytes));
            }
            Ok(stats)
        }

        fn projection(&self) -> &Arc<Schema> {
            &self.projection
        }

        /// Return the number of rows in the file
        fn len(&self) -> u32 {
            self.reader.num_rows() as u32
        }

        fn clone_box(&self) -> Box<dyn GenericFileReader> {
            Box::new(self.clone())
        }

        fn is_legacy(&self) -> bool {
            false
        }

        fn as_legacy_opt(&self) -> Option<&V1FileReader> {
            None
        }

        fn as_legacy_opt_mut(&mut self) -> Option<&mut V1FileReader> {
            None
        }
    }
}

/// A reader where all rows are null. Used when there are fields that have no
/// data files in a fragment.
#[derive(Debug, Clone)]
struct NullReader {
    schema: Arc<Schema>,
    num_rows: u32,
}

impl NullReader {
    fn new(schema: Arc<Schema>, num_rows: u32) -> Self {
        Self { schema, num_rows }
    }

    fn batch(projection: Arc<ArrowSchema>, num_rows: usize) -> RecordBatch {
        let columns = projection
            .fields()
            .iter()
            .map(|f| new_null_array(f.data_type(), num_rows))
            .collect::<Vec<_>>();
        RecordBatch::try_new(projection, columns).unwrap()
    }
}

impl GenericFileReader for NullReader {
    fn read_range_tasks(
        &self,
        range: Range<u64>,
        batch_size: u32,
        projection: Arc<Schema>,
    ) -> BoxFuture<'_, Result<ReadBatchTaskStream>> {
        self.read_ranges_tasks(vec![range].into(), batch_size, projection)
    }

    fn read_ranges_tasks(
        &self,
        ranges: Arc<[Range<u64>]>,
        batch_size: u32,
        projection: Arc<Schema>,
    ) -> BoxFuture<'_, Result<ReadBatchTaskStream>> {
        let mut remaining_rows = ranges.iter().map(|r| r.end - r.start).sum::<u64>();
        let projection: Arc<ArrowSchema> = Arc::new(projection.as_ref().into());

        let task_iter = std::iter::from_fn(move || {
            if remaining_rows == 0 {
                return None;
            }

            let num_rows = remaining_rows.min(batch_size as u64) as usize;
            remaining_rows -= num_rows as u64;
            let batch = Self::batch(projection.clone(), num_rows);
            let task = ReadBatchTask {
                task: futures::future::ready(Ok(batch)).boxed(),
                num_rows: num_rows as u32,
            };
            Some(task)
        });

        async move { Ok(futures::stream::iter(task_iter).boxed()) }.boxed()
    }

    fn read_all_tasks(
        &self,
        batch_size: u32,
        projection: Arc<Schema>,
    ) -> BoxFuture<'_, Result<ReadBatchTaskStream>> {
        self.read_ranges_tasks(vec![0..self.num_rows as u64].into(), batch_size, projection)
    }

    fn take_all_tasks(
        &self,
        indices: &[u32],
        batch_size: u32,
        projection: Arc<Schema>,
        _take_priority: Option<u32>,
    ) -> BoxFuture<'_, Result<ReadBatchTaskStream>> {
        let num_rows = indices.len() as u64;
        self.read_ranges_tasks(vec![0..num_rows].into(), batch_size, projection)
    }

    fn storage_stats(&self) -> Result<Vec<(u32, u64)>> {
        // No-op for null reader
        Ok(Vec::new())
    }

    fn projection(&self) -> &Arc<Schema> {
        &self.schema
    }

    fn len(&self) -> u32 {
        self.num_rows
    }

    fn clone_box(&self) -> Box<dyn GenericFileReader> {
        Box::new(self.clone())
    }

    fn is_legacy(&self) -> bool {
        false
    }

    fn as_legacy_opt(&self) -> Option<&V1FileReader> {
        None
    }

    fn as_legacy_opt_mut(&mut self) -> Option<&mut V1FileReader> {
        None
    }
}

#[derive(Debug, Default, Clone)]
pub struct FragReadConfig {
    // Add the row id column
    pub with_row_id: bool,
    // Add the row address column
    pub with_row_address: bool,
    // Add the last updated at version column
    pub with_row_last_updated_at_version: bool,
    // Add the created at version column
    pub with_row_created_at_version: bool,
    /// The scan scheduler to use for reading data files.
    ///
    /// This should be specified if multiple readers are being used in
    /// an operation
    pub scan_scheduler: Option<Arc<ScanScheduler>>,
    /// The default scan priority to use for reading data files
    ///
    /// Only used if `scan_scheduler` is provided
    ///
    /// The overall priority for reads will be
    ///
    /// operation_priority: u32 | reader_priority: u32 | file_position: u64
    pub reader_priority: Option<u32>,
    /// File reader options to use when reading data files.
    pub file_reader_options: Option<FileReaderOptions>,
}

impl FragReadConfig {
    pub fn with_row_id(mut self, value: bool) -> Self {
        self.with_row_id = value;
        self
    }

    pub fn with_row_address(mut self, value: bool) -> Self {
        self.with_row_address = value;
        self
    }

    pub fn with_row_last_updated_at_version(mut self, value: bool) -> Self {
        self.with_row_last_updated_at_version = value;
        self
    }

    pub fn with_row_created_at_version(mut self, value: bool) -> Self {
        self.with_row_created_at_version = value;
        self
    }

    pub fn has_system_cols(&self) -> bool {
        self.with_row_id
            || self.with_row_address
            || self.with_row_last_updated_at_version
            || self.with_row_created_at_version
    }

    pub fn with_scan_scheduler(mut self, value: Arc<ScanScheduler>) -> Self {
        self.scan_scheduler = Some(value);
        self
    }

    pub fn with_reader_priority(mut self, value: u32) -> Self {
        self.reader_priority = Some(value);
        self
    }

    pub fn with_file_reader_options(mut self, value: FileReaderOptions) -> Self {
        self.file_reader_options = Some(value);
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MetadataMode {
    LazyAllowed,
    Full,
}

impl FileFragment {
    /// Creates a new FileFragment.
    pub fn new(dataset: Arc<Dataset>, metadata: Fragment) -> Self {
        Self { dataset, metadata }
    }

    /// Create a new [`FileFragment`] from a [`StreamingWriteSource`].
    ///
    /// This method can be used before a `Dataset` is created. For example,
    /// Fragments can be created distributed first, before a central machine to
    /// commit the dataset with these fragments.
    ///
    pub async fn create(
        dataset_uri: &str,
        id: usize,
        source: impl StreamingWriteSource,
        params: Option<WriteParams>,
    ) -> Result<Fragment> {
        let mut builder = FragmentCreateBuilder::new(dataset_uri);

        if let Some(params) = params.as_ref() {
            builder = builder.write_params(params);
        }

        builder.write(source, Some(id as u64)).await
    }

    /// Create a list of [`FileFragment`] from a [`StreamingWriteSource`].
    pub async fn create_fragments(
        dataset_uri: &str,
        source: impl StreamingWriteSource,
        params: Option<WriteParams>,
    ) -> Result<Vec<Fragment>> {
        let mut builder = FragmentCreateBuilder::new(dataset_uri);

        if let Some(params) = params.as_ref() {
            builder = builder.write_params(params);
        }

        builder.write_fragments(source).await
    }

    pub async fn create_from_file(
        filename: &str,
        dataset: &Dataset,
        fragment_id: usize,
        physical_rows: Option<usize>,
    ) -> Result<Fragment> {
        let filepath = dataset.data_dir().join(filename);
        let file_version =
            determine_file_version(dataset.object_store.as_ref(), &filepath, None).await?;

        if file_version
            != ConcreteFileVersion::from(dataset.manifest.data_storage_format.lance_file_version()?)
        {
            return Err(Error::invalid_input(format!(
                "File version mismatch. Dataset version: {:?} Fragment version: {:?}",
                dataset.manifest.data_storage_format.lance_file_version()?,
                file_version
            )));
        }

        if file_version == ConcreteFileVersion::V1 {
            let fragment = Fragment::with_file_legacy(
                fragment_id as u64,
                filename,
                dataset.schema(),
                physical_rows,
            );
            Ok(fragment)
        } else {
            // Load the file metadata, confirm the schema is compatible, and
            // determine the column offsets
            let mut frag = Fragment::new(fragment_id as u64);
            let scheduler = ScanScheduler::new(
                dataset.object_store.clone(),
                SchedulerConfig::max_bandwidth(&dataset.object_store),
            );
            let file_scheduler = scheduler
                .open_file(&filepath, &CachedFileSize::unknown())
                .await?;
            let reader = lance_file::reader::FileReader::try_open(
                file_scheduler,
                None,
                Arc::<DecoderPlugins>::default(),
                &dataset.metadata_cache.file_metadata_cache(&filepath),
                dataset.file_reader_options.clone().unwrap_or_default(),
            )
            .await?;
            // If the schemas are not compatible we can't calculate field id offsets
            reader
                .schema()
                .check_compatible(dataset.schema(), &SchemaCompareOptions::default())?;
            let projection = file_versions::reader_projection_from_whole_schema(
                dataset.schema(),
                reader.metadata().version(),
            );
            let physical_rows = reader.metadata().num_rows as usize;
            frag.physical_rows = Some(physical_rows);
            frag.id = fragment_id as u64;

            let column_indices = projection
                .column_indices
                .into_iter()
                .map(|c| c as i32)
                .collect();

            frag.add_file(
                filename,
                dataset.schema().field_ids(),
                column_indices,
                file_version,
                None,
            );
            Ok(frag)
        }
    }

    /// Returns storage stats as `(field_id, bytes_on_disk)` pairs for this fragment.
    pub(crate) async fn storage_stats(
        &self,
        dataset_schema: &Schema,
        scan_scheduler: Arc<ScanScheduler>,
    ) -> Result<Vec<(u32, u64)>> {
        let mut stats = Vec::new();
        for reader in self
            .open_readers_with_full_metadata(
                dataset_schema,
                &FragReadConfig::default().with_scan_scheduler(scan_scheduler),
            )
            .await?
        {
            stats.extend(reader.storage_stats()?);
        }
        Ok(stats)
    }

    pub fn dataset(&self) -> &Dataset {
        self.dataset.as_ref()
    }

    pub fn schema(&self) -> &Schema {
        self.dataset.schema()
    }

    /// Returns the fragment's metadata.
    pub fn metadata(&self) -> &Fragment {
        &self.metadata
    }

    /// The id of this [`FileFragment`].
    pub fn id(&self) -> usize {
        self.metadata.id as usize
    }

    /// The number of data files in this fragment.
    pub fn num_data_files(&self) -> usize {
        self.metadata.files.len()
    }

    /// Gets the data file for a given field
    pub fn data_file_for_field(&self, field_id: u32) -> Option<&DataFile> {
        self.metadata
            .files
            .iter()
            .find(|f| f.fields.contains(&(field_id as i32)))
    }

    /// Open a FileFragment with a given default projection.
    ///
    /// All read operations (other than `read_projected`) will use the supplied
    /// default projection. For `read_projected`, the projection must be a subset
    /// of the default projection.
    ///
    /// Parameters
    /// - `projection`: The projection schema.
    /// - `read_config`: Controls what columns are included in the output.
    /// - `scan_scheduler`: The scheduler to use for reading data files.  If not supplied
    ///   and the data is v2 data then a new scheduler will be created
    ///
    /// `projection` may be an empty schema only if `with_row_id` is true. In that
    /// case, the reader will only be generating row ids.
    pub async fn open(
        &self,
        projection: &Schema,
        read_config: FragReadConfig,
    ) -> Result<FragmentReader> {
        let open_files = self.open_readers(projection, &read_config);
        let deletion_vec_load = self.get_deletion_vector();

        let row_id_load = if self.dataset.manifest.uses_stable_row_ids() {
            futures::future::Either::Left(
                load_row_id_sequence(&self.dataset, &self.metadata).map_ok(Some),
            )
        } else {
            futures::future::Either::Right(futures::future::ready(Ok(None)))
        };

        let (opened_files, deletion_vec, row_id_sequence) =
            join!(open_files, deletion_vec_load, row_id_load);
        let opened_files = opened_files?;
        let deletion_vec = deletion_vec?;
        let row_id_sequence = row_id_sequence?;

        if opened_files.is_empty() && !read_config.has_system_cols() {
            return Err(Error::not_found(format!(
                "No data files found for schema: {}, fragment_id={}",
                projection,
                self.id()
            )));
        }

        let num_physical_rows = self.physical_rows().await?;

        let mut reader = FragmentReader::try_new(
            self.id(),
            deletion_vec,
            row_id_sequence,
            opened_files,
            ArrowSchema::from(projection),
            self.count_rows(None).await?,
            num_physical_rows,
            Arc::new(self.metadata.clone()),
        )?;

        // Plan overlay resolution from coverage metadata (no files opened here); the
        // readers are opened lazily on read, pruned to the rows each read touches.
        if !self.metadata.overlays.is_empty() {
            let planner = plan_overlays(self, projection)?;
            if !planner.is_empty() {
                reader.overlay = Some(OverlayReadState {
                    planner: Arc::new(planner),
                    fragment: Arc::new(self.clone()),
                    read_config: Arc::new(read_config.clone()),
                });
            }
        }

        if read_config.with_row_id {
            reader.with_row_id();
        }
        if read_config.with_row_address {
            reader.with_row_address();
        }
        if read_config.with_row_last_updated_at_version {
            reader.with_row_last_updated_at_version();
        }
        if read_config.with_row_created_at_version {
            reader.with_row_created_at_version();
        }

        Ok(reader)
    }

    fn get_field_id_offset(data_file: &DataFile) -> u32 {
        data_file.fields.first().copied().unwrap_or(0) as u32
    }

    pub(super) async fn open_reader(
        &self,
        data_file: &DataFile,
        projection: Option<&Schema>,
        read_config: &FragReadConfig,
    ) -> Result<Option<Box<dyn GenericFileReader>>> {
        self.open_reader_impl(
            data_file,
            projection,
            read_config,
            MetadataMode::LazyAllowed,
        )
        .await
    }

    async fn open_reader_with_full_metadata(
        &self,
        data_file: &DataFile,
        projection: Option<&Schema>,
        read_config: &FragReadConfig,
    ) -> Result<Option<Box<dyn GenericFileReader>>> {
        self.open_reader_impl(data_file, projection, read_config, MetadataMode::Full)
            .await
    }

    fn open_reader_impl<'a>(
        &'a self,
        data_file: &'a DataFile,
        projection: Option<&'a Schema>,
        read_config: &'a FragReadConfig,
        metadata_mode: MetadataMode,
    ) -> BoxFuture<'a, Result<Option<Box<dyn GenericFileReader>>>> {
        async move {
            let full_schema = self.dataset.schema();
            // The data file may contain fields that are not part of the dataset any longer, remove those
            let data_file_schema = Arc::new(data_file.schema(full_schema));
            let projection = projection.unwrap_or(full_schema);
            // Also remove any fields that are not part of the user's provided projection
            let schema_per_file =
                Arc::new(projection.intersection_ignore_types(data_file_schema.as_ref())?);

            if data_file.is_legacy_file() {
                let max_field_id = data_file.fields.iter().max().unwrap();
                if !schema_per_file.fields.is_empty() {
                    let path = self
                        .dataset
                        .data_file_dir(data_file)?
                        .join(data_file.path.as_str());
                    let object_store = self.dataset.object_store_for_data_file(data_file).await?;
                    let field_id_offset = Self::get_field_id_offset(data_file);
                    let reader = V1FileReader::try_new_with_fragment_id(
                        &object_store,
                        &path,
                        self.schema().clone(),
                        self.id() as u32,
                        field_id_offset as i32,
                        *max_field_id,
                        Some(&self.dataset.metadata_cache.file_metadata_cache(&path)),
                    )
                    .await?;
                    let initialized_schema = reader.schema().project_by_schema(
                        schema_per_file.as_ref(),
                        OnMissing::Error,
                        OnTypeMismatch::Error,
                    )?;
                    let reader = V1Reader::new(reader, Arc::new(initialized_schema));
                    let reader: Box<dyn GenericFileReader> = Box::new(reader);
                    Ok(Some(reader))
                } else {
                    Ok(None)
                }
            } else if schema_per_file.fields.is_empty() {
                Ok(None)
            } else {
                let path = self
                    .dataset
                    .data_file_dir(data_file)?
                    .join(data_file.path.as_str());
                let (store_scheduler, reader_priority) = if let Some(base_id) = data_file.base_id {
                    // TODO: make object stores for non-default bases reuse the same scan scheduler
                    //  currently we always create a new one
                    let object_store = self.dataset.object_store(Some(base_id)).await?;
                    let config = SchedulerConfig::max_bandwidth(&object_store);
                    (
                        ScanScheduler::new(object_store, config),
                        read_config.reader_priority.unwrap_or(0),
                    )
                } else if let Some(scan_scheduler) = read_config.scan_scheduler.as_ref() {
                    (
                        scan_scheduler.clone(),
                        read_config.reader_priority.unwrap_or(0),
                    )
                } else {
                    (
                        ScanScheduler::new(
                            self.dataset.object_store.clone(),
                            SchedulerConfig::max_bandwidth(&self.dataset.object_store),
                        ),
                        0,
                    )
                };
                let file_scheduler = store_scheduler
                    .open_file_with_priority(
                        &path,
                        reader_priority as u64,
                        &data_file.file_size_bytes,
                    )
                    .await?;
                let path = file_scheduler.reader().path().clone();
                let metadata_cache = self.dataset.metadata_cache.file_metadata_cache(&path);
                let field_id_to_column_idx = Arc::new(BTreeMap::from_iter(
                    data_file
                        .fields
                        .iter()
                        .copied()
                        .zip(data_file.column_indices.iter().copied())
                        .filter_map(|(field_id, column_index)| {
                            if column_index < 0 {
                                None
                            } else {
                                Some((field_id as u32, column_index as u32))
                            }
                        }),
                ));
                let file_version = data_file.file_version()?;
                let reader_projection = file_versions::reader_projection_from_field_ids(
                    file_version,
                    schema_per_file.as_ref(),
                    field_id_to_column_idx.as_ref(),
                )?;
                let file_reader_options = read_config
                    .file_reader_options
                    .clone()
                    .or_else(|| self.dataset.file_reader_options.clone())
                    .unwrap_or_default();
                let prefer_indexed = metadata_mode == MetadataMode::LazyAllowed
                    && reader_projection.column_indices.len().saturating_mul(4)
                        < data_file
                            .column_indices
                            .iter()
                            .filter(|column_index| **column_index >= 0)
                            .count();
                let known_schema = self
                    .metadata
                    .physical_rows
                    .map(|num_rows| (data_file_schema.clone(), num_rows as u64));
                let encodings_io = Arc::new(
                    LanceEncodingsIo::new(file_scheduler.clone())
                        .with_read_chunk_size(file_reader_options.read_chunk_size),
                );
                let reader = file_versions::open_projected_reader(
                    file_version,
                    &reader_projection,
                    prefer_indexed,
                    || async {
                        let metadata_index = self
                            .get_file_metadata_index(&file_scheduler, known_schema)
                            .await?;
                        if (reader_projection.column_indices.len() as u32).saturating_mul(4)
                            >= metadata_index.num_columns()
                        {
                            return Ok(None);
                        }
                        Ok(Some(
                            ProjectedFileReader::try_open_with_metadata_index(
                                encodings_io.clone(),
                                path.clone(),
                                Some(reader_projection.clone()),
                                Arc::<DecoderPlugins>::default(),
                                metadata_index,
                                &metadata_cache,
                                file_reader_options.clone(),
                            )
                            .await?,
                        ))
                    },
                    || async {
                        let file_metadata = self.get_file_metadata(&file_scheduler).await?;
                        ProjectedFileReader::try_open_with_file_metadata(
                            encodings_io.clone(),
                            path.clone(),
                            None,
                            Arc::<DecoderPlugins>::default(),
                            file_metadata,
                            &metadata_cache,
                            file_reader_options.clone(),
                        )
                        .await
                    },
                )
                .await?;
                let reader = v2_adapter::Reader::new(
                    Arc::new(reader),
                    schema_per_file,
                    field_id_to_column_idx,
                    reader_priority,
                    file_scheduler,
                );
                let reader: Box<dyn GenericFileReader> = Box::new(reader);
                Ok(Some(reader))
            }
        }
        .boxed()
    }

    async fn open_readers(
        &self,
        projection: &Schema,
        read_config: &FragReadConfig,
    ) -> Result<Vec<Box<dyn GenericFileReader>>> {
        self.open_readers_impl(projection, read_config, MetadataMode::LazyAllowed)
            .await
    }

    async fn open_readers_with_full_metadata(
        &self,
        projection: &Schema,
        read_config: &FragReadConfig,
    ) -> Result<Vec<Box<dyn GenericFileReader>>> {
        self.open_readers_impl(projection, read_config, MetadataMode::Full)
            .await
    }

    fn open_readers_impl<'a>(
        &'a self,
        projection: &'a Schema,
        read_config: &'a FragReadConfig,
        metadata_mode: MetadataMode,
    ) -> BoxFuture<'a, Result<Vec<Box<dyn GenericFileReader>>>> {
        async move {
            let mut opened_files = vec![];
            for data_file in &self.metadata.files {
                let reader = match metadata_mode {
                    MetadataMode::LazyAllowed => {
                        self.open_reader(data_file, Some(projection), read_config)
                            .await?
                    }
                    MetadataMode::Full => {
                        self.open_reader_with_full_metadata(
                            data_file,
                            Some(projection),
                            read_config,
                        )
                        .await?
                    }
                };
                if let Some(reader) = reader {
                    opened_files.push(reader);
                }
            }

            // This should return immediately on modern datasets.  Need to use physical_rows because
            // deletions will be applied later
            let num_rows = self.physical_rows().await?;

            // Check if there are any fields that are not in any data files
            let field_ids_in_files = opened_files
                .iter()
                .flat_map(|r| r.projection().fields_pre_order().map(|f| f.id))
                .filter(|id| *id >= 0)
                .collect::<HashSet<_>>();
            let mut missing_fields = projection.field_ids();
            missing_fields.retain(|f| !field_ids_in_files.contains(f) && *f >= 0);
            if !missing_fields.is_empty() {
                let missing_projection = projection.project_by_ids(&missing_fields, true);
                let null_reader = NullReader::new(Arc::new(missing_projection), num_rows as u32);
                opened_files.push(Box::new(null_reader));
            }

            Ok(opened_files)
        }
        .boxed()
    }

    /// Count the rows in this fragment.
    pub async fn count_rows(&self, filter: Option<String>) -> Result<usize> {
        match filter {
            Some(expr) => self
                .scan()
                .project(&Vec::<String>::default())
                .unwrap()
                .with_row_id()
                .filter(&expr)?
                .count_rows()
                .await
                .map(|v| v as usize),
            None => {
                let total_rows = self.physical_rows();
                let deletion_count = self.count_deletions();

                let (total_rows, deletion_count) =
                    futures::future::try_join(total_rows, deletion_count).await?;

                Ok(total_rows - deletion_count)
            }
        }
    }

    /// Get the number of rows that have been deleted in this fragment.
    pub async fn count_deletions(&self) -> Result<usize> {
        match &self.metadata().deletion_file {
            Some(DeletionFile {
                num_deleted_rows: Some(num_deleted),
                ..
            }) => Ok(*num_deleted),
            _ => {
                let deleletion_vector = self.get_deletion_vector().await?;
                if let Some(deletion_vector) = deleletion_vector {
                    Ok(deletion_vector.len())
                } else {
                    Ok(0)
                }
            }
        }
    }

    /// Get the number of physical rows in the fragment synchronously
    ///
    /// Fails if the fragment does not have the physical row count in the metadata.  This method should
    /// only be called in new workflows which are not run on old versions of Lance.
    pub fn fast_physical_rows(&self) -> Result<usize> {
        if self.dataset.manifest.writer_version.is_some() {
            let Some(physical_rows) = self.metadata.physical_rows else {
                return Err(Error::internal(format!(
                    "The method fast_physical_rows was called on a fragment that does not have the physical row count in the metadata. Fragment id: {}",
                    self.id()
                )));
            };
            Ok(physical_rows)
        } else {
            Err(Error::internal(format!(
                "The method fast_physical_rows was called on a fragment that does not have the physical row count in the metadata. Fragment id: {}",
                self.id()
            )))
        }
    }

    /// Get the number of deleted rows in the fragment synchronously
    ///
    /// Fails if the fragment does not have deletion count in the metadata.  This method should only
    /// be called in new workflows which are not run on old versions of Lance.
    pub fn fast_num_deletions(&self) -> Result<usize> {
        match &self.metadata().deletion_file {
            Some(DeletionFile {
                num_deleted_rows: Some(num_deleted),
                ..
            }) => Ok(*num_deleted),
            None => Ok(0),
            _ => Err(Error::internal(format!(
                "The method fast_num_deletions was called on a fragment that does not have the deletion count in the metadata. Fragment id: {}",
                self.id()
            ))),
        }
    }

    /// Get the number of logical rows (physical rows - deleted rows) in the fragment synchronously
    ///
    /// Fails if the fragment does not have the physical row count or deletion count in the metadata.  This method should only
    /// be called in new workflows which are not run on old versions of Lance.
    pub fn fast_logical_rows(&self) -> Result<usize> {
        let num_physical_rows = self.fast_physical_rows()?;
        let num_deleted_rows = self.fast_num_deletions()?;
        Ok(num_physical_rows - num_deleted_rows)
    }

    /// Get the number of physical rows in the fragment. This includes deleted rows.
    ///
    /// If there are no deleted rows, this is equal to the number of rows in the
    /// fragment.
    pub async fn physical_rows(&self) -> Result<usize> {
        if self.metadata.files.is_empty() {
            return Err(Error::not_found(format!(
                "Fragment {} does not contain any data",
                self.id()
            )));
        };

        // Early versions that did not write the writer version also could write
        // incorrect `physical_row` values. So if we don't have a writer version,
        // we should not used the cached value. On write, we update the values
        // in the manifest, fixing the issue for future reads.
        // See: https://github.com/lance-format/lance/issues/1531
        if self.dataset.manifest.writer_version.is_some()
            && let Some(physical_rows) = self.metadata.physical_rows
        {
            return Ok(physical_rows);
        }

        // Just open any file. All of them should have same size.
        let some_file = &self.metadata.files[0];
        let reader = self
            .open_reader(some_file, None, &FragReadConfig::default())
            .await?
            .ok_or_else(|| {
                Error::internal(format!(
                    "The data file {} did not have any fields contained in the dataset schema",
                    some_file.path
                ))
            })?;

        Ok(reader.len() as usize)
    }

    /// Validate the fragment
    ///
    /// Verifies:
    /// * All field ids in the fragment are distinct
    /// * Within each data file, field ids are in increasing order
    /// * All data files exist and have the same length
    /// * Field ids are distinct between data files.
    /// * Deletion file exists and has rowids in the correct range
    /// * `Fragment.physical_rows` matches length of file
    /// * `DeletionFile.num_deleted_rows` matches length of deletion vector
    pub async fn validate(&self) -> Result<()> {
        let mut seen_fields = HashSet::new();
        for data_file in &self.metadata.files {
            let last = -1;
            for field_id in data_file.fields.iter() {
                // A tombstone marks a field superseded by a later data file.
                // It is not a field id: it has no ordering and can repeat.
                if *field_id == TOMBSTONE_FIELD_ID {
                    continue;
                }
                if *field_id <= last {
                    return Err(Error::corrupt_file(
                        self.dataset
                            .data_file_dir(data_file)?
                            .join(data_file.path.as_str()),
                        format!(
                            "Field id {} is not in increasing order in fragment {:#?}",
                            field_id, self
                        ),
                    ));
                }

                if !seen_fields.insert(field_id) {
                    return Err(Error::corrupt_file(
                        self.dataset
                            .data_file_dir(data_file)?
                            .join(data_file.path.as_str()),
                        format!(
                            "Field id {} is duplicated in fragment {:#?}",
                            field_id, self
                        ),
                    ));
                }
            }
        }

        if self.metadata.files.iter().any(|f| f.is_legacy_file())
            != self.metadata.files.iter().all(|f| f.is_legacy_file())
        {
            return Err(Error::corrupt_file(
                self.dataset
                    .data_file_dir(&self.metadata.files[0])?
                    .join(self.metadata.files[0].path.as_str()),
                "Fragment contains a mix of v1 and v2 data files".to_string(),
            ));
        }

        for data_file in &self.metadata.files {
            data_file.validate(&self.dataset.data_file_dir(data_file)?)?;
        }

        let get_lengths = self.metadata.files.iter().map(|data_file| async move {
            let data_file_dir = self.dataset.data_file_dir(data_file)?;
            let reader = self
                .open_reader(data_file, None, &FragReadConfig::default())
                .await?
                .ok_or_else(|| {
                    Error::corrupt_file(
                        data_file_dir.clone().join(data_file.path.as_str()),
                        "did not have any fields in common with the dataset schema",
                    )
                })?;
            Result::Ok(reader.len() as usize)
        });
        let get_lengths = try_join_all(get_lengths);

        let deletion_vector = self.get_deletion_vector();

        let (get_lengths, deletion_vector) = join!(get_lengths, deletion_vector);

        let get_lengths = get_lengths?;
        let expected_length = get_lengths.first().unwrap_or(&0);
        for (length, data_file) in get_lengths.iter().zip(self.metadata.files.iter()) {
            if length != expected_length {
                let path = self
                    .dataset
                    .data_file_dir(data_file)?
                    .join(data_file.path.as_str());
                return Err(Error::corrupt_file(
                    path,
                    format!(
                        "data file has incorrect length. Expected: {} Got: {}",
                        expected_length, length
                    ),
                ));
            }
        }
        if let Some(physical_rows) = self.metadata.physical_rows
            && physical_rows != *expected_length
        {
            return Err(Error::corrupt_file(
                self.dataset
                    .data_file_dir(&self.metadata.files[0])?
                    .join(self.metadata.files[0].path.as_str()),
                format!(
                    "Fragment metadata has incorrect physical_rows. Actual: {} Metadata: {}",
                    expected_length, physical_rows
                ),
            ));
        }

        if let Some(deletion_vector) = deletion_vector? {
            if let Some(num_deletions) = self
                .metadata
                .deletion_file
                .as_ref()
                .unwrap()
                .num_deleted_rows
                && num_deletions != deletion_vector.len()
            {
                return Err(Error::corrupt_file(
                    deletion_file_path(
                        &self.dataset.base,
                        self.metadata.id,
                        self.metadata.deletion_file.as_ref().unwrap(),
                    ),
                    format!(
                        "deletion vector length does not match metadata. Metadata: {} Deletion vector: {}",
                        num_deletions,
                        deletion_vector.len()
                    ),
                ));
            }

            for offset in deletion_vector.iter() {
                if offset >= *expected_length as u32 {
                    let deletion_file_meta = self.metadata.deletion_file.as_ref().unwrap();
                    return Err(Error::corrupt_file(
                        deletion_file_path(
                            &self.dataset.base,
                            self.metadata.id,
                            deletion_file_meta,
                        ),
                        format!(
                            "deletion vector contains an offset that is out of range. Offset: {} Fragment length: {}",
                            offset, expected_length
                        ),
                    ));
                }
            }
        }

        Ok(())
    }

    /// Open a [`FragmentSession`], which manages a short-lived session of [`FileFragment`].
    ///
    /// This API works well for users making repeated requests over the same projected schema.
    pub async fn open_session(
        &self,
        projection: &Schema,
        with_row_address: bool,
    ) -> Result<FragmentSession> {
        FragmentSession::open(Arc::new(self.clone()), projection, with_row_address).await
    }

    /// Take rows from this fragment based on the offset in the file.
    ///
    /// This will always return the same number of rows as the input indices.
    /// If indices are out-of-bounds, this will return an error.
    pub async fn take(&self, indices: &[u32], projection: &Schema) -> Result<RecordBatch> {
        // Re-map the indices to row ids using the deletion vector
        let deletion_vector = self.get_deletion_vector().await?;
        let row_ids = if let Some(deletion_vector) = deletion_vector {
            // Naive case is O(N*M), where N = indices.len() and M = deletion_vector.len()
            // We can do better by sorting the deletion vector and using binary search
            // This is O(N * log M + M log M).
            let mut sorted_deleted_ids = deletion_vector
                .as_ref()
                .clone()
                .into_iter()
                .collect::<Vec<_>>();
            sorted_deleted_ids.sort();

            Cow::Owned(resolve_actual_row_ids(indices, &sorted_deleted_ids))
        } else {
            Cow::Borrowed(indices)
        };

        // Then call take rows
        let batch = self
            .take_rows(&row_ids, projection, false, false, false, false)
            .await?;

        // Convert Lance JSON columns (LargeBinary/JSONB) back to Arrow JSON (Utf8)
        // for user-facing output.
        if batch
            .schema()
            .fields()
            .iter()
            .any(|f| lance_arrow::json::is_json_field(f) || lance_arrow::json::has_json_fields(f))
        {
            Ok(lance_arrow::json::convert_lance_json_to_arrow(&batch)?)
        } else {
            Ok(batch)
        }
    }

    /// Get the deletion vector for this fragment, using the cache if available.
    pub async fn get_deletion_vector(&self) -> Result<Option<Arc<DeletionVector>>> {
        let Some(deletion_file) = self.metadata.deletion_file.as_ref() else {
            return Ok(None);
        };

        let deletion_vector =
            read_dataset_deletion_file(&self.dataset, self.id() as u64, deletion_file).await?;

        Ok(Some(deletion_vector))
    }

    /// Get the file metadata for this fragment, using the cache if available.
    pub async fn get_file_metadata(
        &self,
        file_scheduler: &FileScheduler,
    ) -> Result<Arc<CachedFileMetadata>> {
        let path = file_scheduler.reader().path();
        let cache = self.dataset.metadata_cache.file_metadata_cache(path);

        let file_metadata = cache
            .get_or_insert_with_key(FileMetadataCacheKey, || async {
                let file_metadata: CachedFileMetadata =
                    lance_file::reader::FileReader::read_all_metadata(file_scheduler).await?;
                Ok(file_metadata)
            })
            .await?;
        Ok(file_metadata)
    }

    pub async fn get_file_metadata_index(
        &self,
        file_scheduler: &FileScheduler,
        known_schema: Option<(Arc<Schema>, u64)>,
    ) -> Result<Arc<FileMetadataIndex>> {
        let path = file_scheduler.reader().path();
        let cache = self.dataset.metadata_cache.file_metadata_cache(path);

        let metadata_index = cache
            .get_or_insert_with_key(FileMetadataIndexCacheKey, || async {
                let metadata_index = if let Some((file_schema, num_rows)) = known_schema {
                    lance_file::reader::FileReader::read_metadata_index_with_schema(
                        file_scheduler,
                        file_schema,
                        num_rows,
                    )
                    .await?
                } else {
                    lance_file::reader::FileReader::read_metadata_index(file_scheduler).await?
                };
                Ok(metadata_index)
            })
            .await?;
        Ok(metadata_index)
    }

    /// Take rows based on internal local row offsets
    ///
    /// If the row offsets are out-of-bounds, this will return an error. But if the
    /// row offset is marked deleted, it will be ignored. Thus, the number of rows
    /// returned may be less than the number of row offsets provided.
    ///
    /// To recover the original row addresses from the returned RecordBatch, set the
    /// `with_row_address` parameter to true. This will add a column named `_rowaddr`
    /// to the RecordBatch at the end.
    pub(crate) async fn take_rows(
        &self,
        row_offsets: &[u32],
        projection: &Schema,
        with_row_id: bool,
        with_row_address: bool,
        with_row_created_at_version: bool,
        with_row_last_updated_at_version: bool,
    ) -> Result<RecordBatch> {
        let reader = self
            .open(
                projection,
                FragReadConfig::default()
                    .with_row_id(with_row_id)
                    .with_row_address(with_row_address)
                    .with_row_created_at_version(with_row_created_at_version)
                    .with_row_last_updated_at_version(with_row_last_updated_at_version),
            )
            .await?;

        if row_offsets.len() > 1 && Self::row_ids_contiguous(row_offsets) {
            let range =
                (row_offsets[0] as usize)..(row_offsets[row_offsets.len() - 1] as usize + 1);
            reader.legacy_read_range_as_batch(range).await
        } else {
            // FIXME, change this method to streams
            reader.take_as_batch(row_offsets, None).await
        }
    }

    fn row_ids_contiguous(row_ids: &[u32]) -> bool {
        if row_ids.is_empty() {
            return false;
        }

        let mut last_id = row_ids[0];

        for id in row_ids.iter().skip(1) {
            if *id != last_id + 1 {
                return false;
            }
            last_id = *id;
        }

        true
    }

    /// Scan this [`FileFragment`].
    ///
    /// See [`Dataset::scan`].
    pub fn scan(&self) -> Scanner {
        Scanner::from_fragment(self.dataset.clone(), self.metadata.clone())
    }

    /// Create an [`Updater`] to append new columns.
    ///
    /// The `columns` parameter is a list of existing columns to be read from
    /// the fragment. They can be used to derive new columns. This is allowed to
    /// be empty.
    ///
    /// The columns `_rowaddr` and `_rowid` can be used to load the row id or row address
    ///
    /// The `schemas` parameter is a tuple of the write schema (just the new fields)
    /// and the full schema (the target schema after the update). If the write
    /// schema is None, it is inferred from the first batch of results. The full
    /// schema is inferred by appending the write schema to the existing schema.
    ///
    /// The `batch_size` parameter can be used to influence how much data is processed
    /// at a time. This can be useful to control memory usage when processing very large
    /// fields. The batch_size will only be used if the dataset is a v2 dataset.  It will
    /// be ignored for v1 datasets.
    pub(crate) async fn updater<T: AsRef<str>>(
        &self,
        columns: Option<&[T]>,
        schemas: Option<(Schema, Schema)>,
        batch_size: Option<u32>,
    ) -> Result<Updater> {
        let mut schema = self.dataset.schema().clone();

        let mut with_row_addr = false;
        let mut with_row_id = false;
        if let Some(columns) = columns {
            let mut projection = Vec::new();
            for column in columns {
                if column.as_ref() == ROW_ADDR {
                    with_row_addr = true;
                } else if column.as_ref() == ROW_ID {
                    with_row_id = true;
                } else {
                    projection.push(column.as_ref());
                }
            }
            schema = schema.project(&projection)?;
        }

        // If there is no projection, we at least need to read the row addresses
        with_row_addr |= !with_row_id && schema.fields.is_empty();

        let reader = self.open(
            &schema,
            FragReadConfig::default()
                .with_row_address(with_row_addr)
                .with_row_id(with_row_id),
        );
        let deletion_vector = self.get_deletion_vector();
        let (reader, deletion_vector) = join!(reader, deletion_vector);
        let reader = reader?;
        let deletion_vector = deletion_vector?.unwrap_or_default().as_ref().clone();

        Updater::try_new(self.clone(), reader, deletion_vector, schemas, batch_size).await
    }

    pub async fn merge_columns(
        &mut self,
        stream: impl RecordBatchReader + Send + 'static,
        left_on: &str,
        right_on: &str,
        max_field_id: i32,
    ) -> Result<(Fragment, Schema)> {
        let stream = Box::new(stream);
        if self.schema().field(left_on).is_none() && left_on != ROW_ID && left_on != ROW_ADDR {
            return Err(Error::invalid_input(format!(
                "Column {} does not exist in the left side fragment",
                left_on
            )));
        };
        let right_schema = stream.schema();
        if right_schema.field_with_name(right_on).is_err() {
            return Err(Error::invalid_input(format!(
                "Column {} does not exist in the right side fragment",
                right_on
            )));
        };

        for field in right_schema.fields() {
            if field.name() == right_on {
                // right_on is allowed to exist in the dataset, since it may be
                // the same as left_on.
                continue;
            }
            if self.schema().field(field.name()).is_some() {
                return Err(Error::invalid_input(format!(
                    "Column {} exists in left side fragment and right side dataset",
                    field.name()
                )));
            }
        }
        // Hash join
        let joiner = Arc::new(HashJoiner::try_new(stream, right_on).await?);
        // Final schema is union of current schema, plus the RHS schema without
        // the right_on key.
        let mut new_schema: Schema = self.schema().merge(joiner.out_schema().as_ref())?;
        new_schema.set_field_id(Some(max_field_id));

        let new_fragment = self
            .clone()
            .merge(left_on, &joiner)
            .await
            .map(|f| f.metadata)?;

        Ok((new_fragment, new_schema))
    }

    pub(crate) async fn merge(mut self, join_column: &str, joiner: &HashJoiner) -> Result<Self> {
        let mut updater = self.updater(Some(&[join_column]), None, None).await?;

        while let Some(batch) = updater.next().await? {
            let batch = joiner
                .collect(&self.dataset, batch[join_column].clone())
                .await?;
            updater.update(batch).await?;
        }

        self.metadata = updater.finish().await?;

        Ok(self)
    }

    /// Same as [`Self::update_columns_with_offsets`] but discards the matched row offsets.
    /// Use [`Self::update_columns_with_offsets`] if you need per-row version metadata for stable row IDs.
    pub async fn update_columns(
        &mut self,
        right_stream: impl RecordBatchReader + Send + 'static,
        left_on: &str,
        right_on: &str,
    ) -> Result<(Fragment, Vec<u32>)> {
        let r = self
            .update_columns_with_offsets(right_stream, left_on, right_on)
            .await?;
        Ok((r.fragment, r.fields_modified))
    }

    /// Same operation as [`Self::update_columns`], and also returns matched physical row offsets for stable row IDs.
    pub async fn update_columns_with_offsets(
        &mut self,
        right_stream: impl RecordBatchReader + Send + 'static,
        left_on: &str,
        right_on: &str,
    ) -> Result<FragmentUpdateColumnsResult> {
        if self.schema().field(left_on).is_none() && left_on != ROW_ID && left_on != ROW_ADDR {
            return Err(Error::invalid_input(format!(
                "Column {} does not exist in the left side fragment",
                left_on
            )));
        };
        let right_stream = Box::new(right_stream);
        let right_schema = right_stream.schema();
        if right_schema.field_with_name(right_on).is_err() {
            return Err(Error::invalid_input(format!(
                "Column {} does not exist in the right side fragment",
                right_on
            )));
        };
        let write_schema = right_schema.as_ref().without_column(right_on);
        for field in write_schema.fields() {
            if ROW_ID.eq(field.name()) || ROW_ADDR.eq(field.name()) {
                return Err(Error::invalid_input(format!(
                    "Column {} is a reversed metadata column and cannot be updated",
                    field.name()
                )));
            }
            if self.schema().field(field.name()).is_none() {
                return Err(Error::invalid_input(format!(
                    "Column {} in right side fragment does not exist in left side fragment",
                    field.name()
                )));
            }
        }

        let write_schema = self.schema().project_by_schema(
            &write_schema,
            OnMissing::Error,
            OnTypeMismatch::Error,
        )?;
        // Prepare the read projection: align with the write_schema's columns and append the left_on column.
        let mut read_columns: Vec<String> =
            write_schema.fields.iter().map(|f| f.name.clone()).collect();
        read_columns.push(left_on.to_string());
        // Physical positions for matched rows are taken from `_rowaddr` (fragment id + row offset).
        // The updater scans live rows in physical order; `_rowaddr` encodes the slot index used by row-level version metadata.
        if !read_columns.iter().any(|n| n.as_str() == ROW_ADDR) {
            read_columns.push(ROW_ADDR.to_string());
        }
        let mut updater = self
            .updater(
                Some(&read_columns),
                Some((write_schema.clone(), self.schema().clone())),
                None,
            )
            .await?;
        // Hash join: rows matched on the right-hand stream rewrite columns; track physical offsets via `_rowaddr`.
        // Convert Arrow JSON columns (Utf8) to Lance JSON (LargeBinary) in the right stream
        // so they match the physical storage format read from the fragment's left batch.
        let right_stream: Box<dyn RecordBatchReader + Send> = if right_schema
            .fields()
            .iter()
            .any(|f| is_arrow_json_field(f) || has_json_fields(f))
        {
            Box::new(JsonConvertingReader::new(right_stream))
        } else {
            right_stream
        };
        let joiner = Arc::new(HashJoiner::try_new(right_stream, right_on).await?);
        let mut matched_offsets = RoaringBitmap::new();
        let frag_id_u32 = u32::try_from(self.metadata.id).map_err(|_| {
            Error::invalid_input(format!(
                "Fragment id {} does not fit RowAddress fragment id",
                self.metadata.id
            ))
        })?;
        while let Some(batch) = updater.next().await? {
            let index_column = batch[left_on].clone();
            let matched = joiner.matched_join_rows(index_column.clone())?;
            if let Some(addr_col) = batch.column_by_name(ROW_ADDR) {
                let addrs = as_primitive_array::<UInt64Type>(addr_col.as_ref());
                for (row_idx, &is_matched) in matched.iter().enumerate().take(batch.num_rows()) {
                    if !is_matched || addrs.is_null(row_idx) {
                        continue;
                    }
                    let addr = RowAddress::from(addrs.value(row_idx));
                    if addr.fragment_id() == frag_id_u32 {
                        matched_offsets.insert(addr.row_offset());
                    }
                }
            }
            let updated_batch = joiner
                .collect_with_fallback(batch, index_column, self.dataset())
                .await?;
            updater.update(updated_batch).await?;
        }

        let mut updated_fragment = updater.finish().await?;
        // Mark fields in updated data files as obsolete ("tombstone").
        let updated_fields = updated_fragment.files.last().unwrap().fields.clone();
        for data_file in &mut updated_fragment.files.iter_mut().rev().skip(1) {
            let new_fields: Arc<[i32]> = data_file
                .fields
                .iter()
                .map(|field| {
                    if updated_fields.contains(field) {
                        -2 // Tombstone
                    } else {
                        *field
                    }
                })
                .collect::<Vec<_>>()
                .into();
            data_file.fields = new_fields;
        }
        // Remove data files that have become entirely tombstoned.
        updated_fragment
            .files
            .retain(|data_file| data_file.fields.iter().any(|&field| field != -2));
        let updated_fields = updated_fields
            .iter()
            .filter_map(|&i| u32::try_from(i).ok())
            .collect();
        Ok(FragmentUpdateColumnsResult {
            fragment: updated_fragment,
            fields_modified: updated_fields,
            matched_offsets,
        })
    }

    /// Append new columns to the fragment
    ///
    /// This is the fragment-level version of [`Dataset::add_columns`].
    pub async fn add_columns(
        &self,
        transforms: NewColumnTransform,
        read_columns: Option<Vec<String>>,
        batch_size: Option<u32>,
    ) -> Result<(Fragment, Schema)> {
        let (fragments, schema, _) = schema_evolution::add_columns_to_fragments(
            self.dataset.as_ref(),
            transforms,
            read_columns,
            std::slice::from_ref(self),
            batch_size,
        )
        .await?;
        assert_eq!(fragments.len(), 1);
        Ok((fragments.into_iter().next().unwrap(), schema))
    }

    /// Delete rows from the fragment.
    ///
    /// If all rows are deleted, returns `Ok(None)`. Otherwise, returns a new
    /// fragment with the updated deletion vector. This must be persisted to
    /// the manifest.
    pub async fn delete(self, predicate: &str) -> Result<Option<Self>> {
        // Load existing deletion vector
        let mut deletion_vector = self
            .get_deletion_vector()
            .await?
            .unwrap_or_default()
            .as_ref()
            .clone();

        let starting_length = deletion_vector.len();

        // scan with predicate and row addresses
        let mut scanner = self.scan();

        let predicate_lower = predicate.trim().to_lowercase();
        if predicate_lower == "true" {
            return Ok(None);
        } else if predicate_lower == "false" {
            return Ok(Some(self));
        }

        scanner
            .with_row_address()
            .filter(predicate)?
            .project::<&str>(&[])?;

        // if predicate is `true`, delete the whole fragment
        // else if predicate is `false`, filter the predicate
        // We do this on the expression level after expression optimization has
        // occurred so we also catch expressions that are equivalent to `true`
        if let Some(predicate) = &scanner.get_expr_filter()? {
            if matches!(
                predicate,
                Expr::Literal(ScalarValue::Boolean(Some(false)), _)
            ) {
                return Ok(Some(self));
            }
            if matches!(
                predicate,
                Expr::Literal(ScalarValue::Boolean(Some(true)), _)
            ) {
                return Ok(None);
            }
        }

        // As we get row addrs, add them into our deletion vector
        scanner
            .try_into_stream()
            .await?
            .try_for_each(|batch| {
                let array = batch[ROW_ADDR].clone();
                let int_array: &UInt64Array = as_primitive_array(array.as_ref());

                // _rowaddr is global, not within fragment level. The high bits
                // are the fragment_id, the low bits are the row_id within the
                // fragment.
                let local_row_ids = int_array.values().iter().map(|v| *v as u32);

                deletion_vector.extend(local_row_ids);
                futures::future::ready(Ok(()))
            })
            .await?;

        // If we haven't deleted any additional rows, we can return the fragment as-is.
        if deletion_vector.len() == starting_length {
            return Ok(Some(self));
        }

        self.write_deletions(deletion_vector).await
    }

    pub async fn extend_deletions(
        self,
        new_deletions: impl IntoIterator<Item = u32>,
    ) -> Result<Option<Self>> {
        let mut deletion_vector = self
            .get_deletion_vector()
            .await?
            .unwrap_or_default()
            .as_ref()
            .clone();

        deletion_vector.extend(new_deletions);

        self.write_deletions(deletion_vector).await
    }

    async fn write_deletions(mut self, deletion_vector: DeletionVector) -> Result<Option<Self>> {
        let physical_rows = self.physical_rows().await?;
        if deletion_vector.len() == physical_rows
            && deletion_vector.contains_range(0..physical_rows as u32)
        {
            return Ok(None);
        } else if deletion_vector.len() >= physical_rows {
            let dv_len = deletion_vector.len();
            let examples: Vec<u32> = deletion_vector
                .into_iter()
                .filter(|x| *x >= physical_rows as u32)
                .take(5)
                .collect();
            return Err(Error::internal(format!(
                "Deletion vector includes rows that aren't in the fragment. \
            Num physical rows {}; Deletion vector length: {}; \
            Examples: {:?}",
                physical_rows, dv_len, examples
            )));
        }

        self.metadata.deletion_file = write_deletion_file(
            &self.dataset.base,
            self.metadata.id,
            self.dataset.version().version,
            &deletion_vector,
            self.dataset.object_store.as_ref(),
        )
        .await?;

        Ok(Some(self))
    }
}

/// Using deleted ids to remap row ids into actual row ids.
pub(crate) fn resolve_actual_row_ids(row_ids: &[u32], sorted_deleted_ids: &[u32]) -> Vec<u32> {
    let mut row_ids = row_ids.to_vec();
    for row_id in row_ids.iter_mut() {
        // We find the number of deleted rows that are less than each row
        // index, and that becomes the initial offset. We increment the
        // index by that amount, plus the number of deleted row ids we
        // encounter along the way. So for example, if deleted rows are
        // [2, 3, 5] and we want row 4, we need to advanced by 2 (since
        // 2 and 3 are less than 4). That puts us at row 6, but since
        // we passed row 5, we need to advance by 1 more, giving a final
        // row id of 7.
        let mut new_row_id = *row_id;
        let offset = sorted_deleted_ids.partition_point(|v| *v <= new_row_id);

        let mut deletion_i = offset;
        let mut i = 0;
        while i < offset {
            // Advance the row id
            new_row_id += 1;
            while deletion_i < sorted_deleted_ids.len()
                && sorted_deleted_ids[deletion_i] == new_row_id
            {
                // If we encounter a deleted row, we need to advance
                // again.
                deletion_i += 1;
                new_row_id += 1;
            }
            i += 1;
        }

        *row_id = new_row_id;
    }

    row_ids
}

// Cache key for file metadata
#[derive(Debug, Clone)]
struct FileMetadataCacheKey;

impl CacheKey for FileMetadataCacheKey {
    type ValueType = CachedFileMetadata;

    fn key(&self) -> std::borrow::Cow<'_, str> {
        "".into()
    }

    fn type_name() -> &'static str {
        "FileMetadata"
    }

    fn schema() -> CacheKeySchema {
        CacheKeySchema::new("lance.dataset.fragment-file-metadata-key", 1)
    }

    fn write_key(&self, _builder: &mut KeyBuilder) {}
}

#[derive(Debug, Clone)]
struct FileMetadataIndexCacheKey;

impl CacheKey for FileMetadataIndexCacheKey {
    type ValueType = FileMetadataIndex;

    fn key(&self) -> std::borrow::Cow<'_, str> {
        "metadata_index".into()
    }

    fn type_name() -> &'static str {
        "FileMetadataIndex"
    }

    fn schema() -> CacheKeySchema {
        CacheKeySchema::new("lance.dataset.fragment-file-metadata-index-key", 1)
    }

    fn write_key(&self, _builder: &mut KeyBuilder) {}
}

impl From<FileFragment> for Fragment {
    fn from(fragment: FileFragment) -> Self {
        fragment.metadata
    }
}

/// [`FragmentReader`] is an abstract reader for a [`FileFragment`].
///
/// It opens the data files that contains the columns of the projection schema, and
/// reconstruct the RecordBatch from columns read from each data file.
#[derive(Debug)]
pub struct FragmentReader {
    /// Readers and schema of each opened data file.
    readers: Vec<Box<dyn GenericFileReader>>,

    /// The output schema. The defines the order in which the columns are returned.
    output_schema: ArrowSchema,

    /// The deleted row IDs
    deletion_vec: Option<Arc<DeletionVector>>,

    /// The row id sequence
    ///
    /// Only populated if the stable row id feature is enabled.
    row_id_sequence: Option<Arc<RowIdSequence>>,

    /// ID of the fragment
    fragment_id: usize,

    /// True if we should generate a row id for the output
    with_row_id: bool,

    /// True if we should generate a row address column in output
    with_row_addr: bool,

    /// True if we should generate a last updated at version column in output
    with_row_last_updated_at_version: bool,

    /// True if we should generate a created at version column in output
    with_row_created_at_version: bool,

    /// If true, deleted rows will be set to null, which is fast
    /// If false, deleted rows will be removed from the batch, requiring a copy
    make_deletions_null: bool,

    /// The fragment metadata (needed for version columns)
    fragment: Arc<Fragment>,

    /// The last_updated_at version sequence (loaded from fragment metadata)
    last_updated_at_sequence: Option<Arc<lance_table::rowids::version::RowDatasetVersionSequence>>,

    /// The created_at version sequence (loaded from fragment metadata)
    created_at_sequence: Option<Arc<lance_table::rowids::version::RowDatasetVersionSequence>>,

    // total number of real rows in the fragment (num_physical_rows - num_deleted_rows)
    num_rows: usize,

    // total number of physical rows in the fragment (all rows, ignoring deletions)
    num_physical_rows: usize,

    /// Read-time state for resolving data overlay files: the coverage plan plus
    /// what is needed to open overlay readers. `None` when the fragment has no
    /// overlays. Overlays are merged into base batches (by `offset_in_frag`) before
    /// deletion filtering, opening only the files each read's rows touch.
    overlay: Option<OverlayReadState>,
}

/// What [`FragmentReader`] needs to resolve overlays at read time: the coverage
/// plan (from metadata, cheap to build), and the fragment + config needed to open
/// overlay readers once the read's rows — and therefore which files it touches —
/// are known. All `Arc` so cloning a reader stays cheap.
#[derive(Clone, Debug)]
struct OverlayReadState {
    planner: Arc<OverlayReadPlanner>,
    fragment: Arc<FileFragment>,
    read_config: Arc<FragReadConfig>,
}

// Custom clone impl needed because it is not easy to clone Box<dyn GenericFileReader>
//
// We currently need FragmentReader to be Clone because the pushdown scan clones it
// to reuse the fragment reader for both "scan with row id" and "scan without row id"
impl Clone for FragmentReader {
    fn clone(&self) -> Self {
        Self {
            readers: self
                .readers
                .iter()
                .map(|reader| reader.clone_box())
                .collect::<Vec<_>>(),
            output_schema: self.output_schema.clone(),
            deletion_vec: self.deletion_vec.clone(),
            row_id_sequence: self.row_id_sequence.clone(),
            fragment_id: self.fragment_id,
            with_row_id: self.with_row_id,
            with_row_addr: self.with_row_addr,
            with_row_last_updated_at_version: self.with_row_last_updated_at_version,
            with_row_created_at_version: self.with_row_created_at_version,
            make_deletions_null: self.make_deletions_null,
            fragment: self.fragment.clone(),
            last_updated_at_sequence: self.last_updated_at_sequence.clone(),
            created_at_sequence: self.created_at_sequence.clone(),
            num_rows: self.num_rows,
            num_physical_rows: self.num_physical_rows,
            overlay: self.overlay.clone(),
        }
    }
}

impl std::fmt::Display for FragmentReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FragmentReader(id={})", self.fragment_id)
    }
}

fn merge_batches(batches: &[RecordBatch]) -> Result<RecordBatch> {
    if batches.is_empty() {
        return Err(Error::invalid_input(
            "Cannot merge empty batches".to_string(),
        ));
    }

    let mut merged = batches[0].clone();
    for batch in batches.iter().skip(1) {
        merged = merged.merge(batch)?;
    }
    Ok(merged)
}

impl FragmentReader {
    #[allow(clippy::too_many_arguments)]
    fn try_new(
        fragment_id: usize,
        deletion_vec: Option<Arc<DeletionVector>>,
        row_id_sequence: Option<Arc<RowIdSequence>>,
        readers: Vec<Box<dyn GenericFileReader>>,
        output_schema: ArrowSchema,
        num_rows: usize,
        num_physical_rows: usize,
        fragment: Arc<Fragment>,
    ) -> Result<Self> {
        if let Some(legacy_reader) = readers.first().and_then(|reader| reader.as_legacy_opt()) {
            let num_batches = legacy_reader.num_batches();
            for reader in readers.iter().skip(1) {
                if let Some(other_legacy) = reader.as_legacy_opt() {
                    if other_legacy.num_batches() != num_batches {
                        return Err(Error::invalid_input("Cannot create FragmentReader from data files with different number of batches"
                            .to_string()));
                    }
                } else {
                    return Err(Error::invalid_input(
                        "Cannot mix legacy and non-legacy readers".to_string(),
                    ));
                }
            }
        }
        Ok(Self {
            readers,
            output_schema,
            deletion_vec,
            row_id_sequence,
            fragment_id,
            with_row_id: false,
            with_row_addr: false,
            with_row_last_updated_at_version: false,
            with_row_created_at_version: false,
            make_deletions_null: false,
            fragment,
            last_updated_at_sequence: None,
            created_at_sequence: None,
            num_rows,
            num_physical_rows,
            overlay: None,
        })
    }

    pub(crate) fn with_row_id(&mut self) -> &mut Self {
        self.with_row_id = true;
        self.output_schema = self
            .output_schema
            .try_with_column(ROW_ID_FIELD.clone())
            .expect("Table already has a column named _rowid");
        self
    }

    pub(crate) fn with_row_address(&mut self) -> &mut Self {
        self.with_row_addr = true;
        self.output_schema = self
            .output_schema
            .try_with_column(ROW_ADDR_FIELD.clone())
            .expect("Table already has a column named _rowaddr");
        self
    }

    pub(crate) fn with_make_deletions_null(&mut self) -> &mut Self {
        self.make_deletions_null = true;
        self
    }

    pub(crate) fn with_row_last_updated_at_version(&mut self) -> &mut Self {
        self.with_row_last_updated_at_version = true;

        // Load the version sequence if not already loaded
        if self.last_updated_at_sequence.is_none()
            && let Some(meta) = &self.fragment.last_updated_at_version_meta
            && let Ok(sequence) = meta.load_sequence()
        {
            self.last_updated_at_sequence = Some(Arc::new(sequence));
        }
        // If no metadata or load fails, sequence remains None (will default to version 1)

        // Add the version column to the output schema
        self.output_schema = self
            .output_schema
            .try_with_column(ROW_LAST_UPDATED_AT_VERSION_FIELD.clone())
            .expect("Table already has a column named _row_last_updated_at_version");

        self
    }

    pub(crate) fn with_row_created_at_version(&mut self) -> &mut Self {
        self.with_row_created_at_version = true;

        // Load the version sequence if not already loaded
        if self.created_at_sequence.is_none()
            && let Some(meta) = &self.fragment.created_at_version_meta
            && let Ok(sequence) = meta.load_sequence()
        {
            self.created_at_sequence = Some(Arc::new(sequence));
        }
        // If no metadata or load fails, sequence remains None (will default to version 1)

        // Add the version column to the output schema
        self.output_schema = self
            .output_schema
            .try_with_column(ROW_CREATED_AT_VERSION_FIELD.clone())
            .expect("Table already has a column named _row_created_at_version");

        self
    }

    /// TODO: This method is relied upon by the v1 pushdown mechanism and will need to stay
    /// in place until v1 is removed.  v2 uses a different mechanism for pushdown and so there
    /// is little benefit in updating the v1 pushdown node.
    pub(crate) fn legacy_num_batches(&self) -> usize {
        let legacy_reader = self.readers[0].as_legacy();
        let num_batches = legacy_reader.num_batches();
        assert!(
            self.readers
                .iter()
                .all(|r| r.as_legacy().num_batches() == num_batches),
            "Data files have varying number of batches, which is not yet supported."
        );
        num_batches
    }

    /// TODO: This method is relied upon by the v1 pushdown mechanism and will need to stay
    /// in place until v1 is removed.  v2 uses a different mechanism for pushdown and so there
    /// is little benefit in updating the v1 pushdown node.
    ///
    /// This method is also used by the updater.  Even though the updater has been updated to
    /// use streams, the updater still needs to know the batch size in v1 so that it can create
    /// files with the same batch size.
    pub(crate) fn legacy_num_rows_in_batch(&self, batch_id: u32) -> Option<u32> {
        if let Some(legacy_reader) = self.readers.first().and_then(|r| r.as_legacy_opt()) {
            if batch_id < legacy_reader.num_batches() as u32 {
                Some(legacy_reader.num_rows_in_batch(batch_id as i32) as u32)
            } else {
                None
            }
        } else {
            None
        }
    }

    /// Read the page statistics of the fragment for the specified fields.
    ///
    /// TODO: This method is relied upon by the v1 pushdown mechanism and will need to stay
    /// in place until v1 is removed.  v2 uses a different mechanism for pushdown and so there
    /// is little benefit in updating the v1 pushdown node.
    pub(crate) async fn legacy_read_page_stats(
        &self,
        projection: Option<&Schema>,
    ) -> Result<Option<RecordBatch>> {
        let mut stats_batches = vec![];
        for reader in self.readers.iter() {
            let schema = match projection {
                Some(projection) => Arc::new(reader.projection().intersection(projection)?),
                None => reader.projection().clone(),
            };
            let reader = reader.as_legacy();
            if let Some(stats_batch) = reader.read_page_stats(&schema.field_ids()).await? {
                stats_batches.push(stats_batch);
            }
        }

        if stats_batches.is_empty() {
            Ok(None)
        } else {
            Ok(Some(merge_batches(&stats_batches)?))
        }
    }

    /// Read a batch of rows from the fragment, with a subset of columns.
    ///
    /// Note: the projection must be a subset of the schema the reader was created with.
    /// Otherwise incorrect data will be returned.
    ///
    /// TODO: This method is relied upon by the v1 pushdown mechanism and will need to stay
    /// in place until v1 is removed.  v2 uses a different mechanism for pushdown and so there
    /// is little benefit in updating the v1 pushdown node.
    pub(crate) async fn legacy_read_batch_projected(
        &self,
        batch_id: usize,
        params: impl Into<ReadBatchParams> + Clone,
        projection: &Schema,
    ) -> Result<RecordBatch> {
        let first_reader = self.readers[0].as_legacy();
        // All batches have the same size in v1, except for the last one.
        let batch_offset = batch_id * first_reader.num_rows_in_batch(0);
        let rows_in_batch = first_reader.num_rows_in_batch(batch_id as i32);

        let batches = if !projection.fields.is_empty() {
            let read_tasks = self.readers.iter().map(|reader| {
                let projection = reader.projection().intersection(projection);
                let params = params.clone();

                let reader = reader.as_legacy();

                async move {
                    // Apply ? inside the task to keep read_tasks a simple iter of futures
                    // for try_join_all
                    let projection = projection?;
                    if projection.fields.is_empty() {
                        // The projection caused one of the data files to become
                        // irrelevant and so we can skip it
                        Result::Ok(None)
                    } else {
                        Ok(Some(
                            reader
                                .read_batch(batch_id as i32, params, &projection)
                                .await?,
                        ))
                    }
                }
            });
            let results = try_join_all(read_tasks).await?;
            results.into_iter().flatten().collect::<Vec<RecordBatch>>()
        } else {
            // If we are selecting no columns, we can assume we are just getting
            // the row ids. If this is the case, we need to generate an empty
            // batch with the correct number of rows.
            let expected_rows = params
                .clone()
                .into()
                .slice(0, rows_in_batch)
                .unwrap()
                .to_offsets()?
                .len();
            vec![RecordBatch::from(StructArray::new_empty_fields(
                expected_rows,
                None,
            ))]
        };

        let params = params.into();
        let result = merge_batches(&batches)?;

        // Need to apply deletions and row ids.
        // In order to apply deletions we need to change the parameters to be
        // relative to the file, not the batch.
        let file_params = match params {
            ReadBatchParams::Indices(indices) => ReadBatchParams::Indices(
                indices
                    .values()
                    .iter()
                    .map(|i| *i + batch_offset as u32)
                    .collect(),
            ),
            ReadBatchParams::Ranges(_) => {
                return Err(Error::internal(
                    "ReadBatchParams::Ranges should not be used in v1 files".to_string(),
                ));
            }
            ReadBatchParams::RangeFull => {
                ReadBatchParams::Range(batch_offset..(batch_offset + rows_in_batch))
            }
            ReadBatchParams::RangeFrom(start) => {
                ReadBatchParams::Range((start.start + batch_offset)..(batch_offset + rows_in_batch))
            }
            ReadBatchParams::RangeTo(end) => {
                ReadBatchParams::Range(batch_offset..(end.end + batch_offset))
            }
            ReadBatchParams::Range(range) => {
                ReadBatchParams::Range((range.start + batch_offset)..(range.end + batch_offset))
            }
        };
        let result = lance_table::utils::stream::apply_row_id_and_deletes(
            result,
            0,
            self.fragment_id as u32,
            &RowIdAndDeletesConfig {
                params: file_params,
                deletion_vector: self.deletion_vec.clone(),
                row_id_sequence: self.row_id_sequence.clone(),
                with_row_id: self.with_row_id,
                with_row_addr: self.with_row_addr,
                with_row_last_updated_at_version: self.with_row_last_updated_at_version,
                with_row_created_at_version: self.with_row_created_at_version,
                last_updated_at_sequence: self.last_updated_at_sequence.clone(),
                created_at_sequence: self.created_at_sequence.clone(),
                make_deletions_null: self.make_deletions_null,
                total_num_rows: first_reader.len() as u32,
            },
        )?;

        let output_schema = {
            let mut output_schema = ArrowSchema::from(projection);
            if self.with_row_id {
                output_schema = output_schema.try_with_column(ROW_ID_FIELD.clone())?;
            }
            if self.with_row_addr {
                output_schema = output_schema.try_with_column(ROW_ADDR_FIELD.clone())?;
            }
            output_schema
        };

        Ok(result.project_by_schema(&output_schema)?)
    }

    /// Merge data overlay values onto a stream of base batches.
    ///
    /// Runs on physical rows in read order, *before* deletion filtering, so each
    /// row can be addressed by its position in the fragment (its `offset_in_frag`,
    /// derived from `params`) and deletions take precedence naturally: an overlay
    /// value for a deleted row is dropped along with the row downstream. A no-op
    /// when the fragment has no overlays.
    ///
    /// The read's `offset_in_frag` values are known from `params` up front, so
    /// overlays are resolved here to just the files this read's rows touch — an
    /// overlay whose cells fall outside the read is not opened at all. Within each
    /// batch, the overlay reads (only the values that batch needs) are then issued
    /// concurrently with the base read rather than after it.
    async fn merge_overlays(
        &self,
        merged: ReadBatchTaskStream,
        params: &ReadBatchParams,
        total_num_rows: u32,
    ) -> Result<ReadBatchTaskStream> {
        let Some(overlay) = &self.overlay else {
            return Ok(merged);
        };
        // The offset_in_frag of every row this read will return, materialized once.
        // Cost is one u32 per output row (a whole-fragment scan is 4 bytes/row), and
        // it lets us both prune overlays to the read and slice each batch's offsets
        // below without reading any data. Only paid when the fragment has overlays.
        //
        // TODO(overlay perf): this could be avoided by teaching `ReadBatchParams` to
        // yield a coverage bitmap directly (for pruning) and to slice per batch (for
        // the routing below), or by moving `ReadBatchParams` to a roaring bitmap
        // wholesale — a larger refactor tracked separately.
        let offsets_in_frag: Arc<Vec<u32>> =
            Arc::new(params.to_offsets_total(total_num_rows).values().to_vec());

        // Open only the overlay readers this read touches (pruned by row selection).
        let plans = resolve_overlays(
            &overlay.planner,
            &offsets_in_frag,
            &overlay.fragment,
            &overlay.read_config,
        )
        .await?;
        if plans.is_empty() {
            return Ok(merged);
        }
        let plans = Arc::new(plans);

        // Batches arrive in physical read order, so a running total of the rows seen
        // so far gives each batch its starting offset_in_batch into `offsets_in_frag`.
        let mut rows_seen = 0usize;
        let stream = merged
            .map(move |task| {
                let num_rows = task.num_rows;
                let start = rows_seen;
                rows_seen += num_rows as usize;
                let offsets_in_frag = offsets_in_frag.clone();
                let plans = plans.clone();
                let inner = task.task;
                ReadBatchTask {
                    num_rows,
                    task: async move {
                        let batch_offsets = &offsets_in_frag[start..start + num_rows as usize];
                        merge_overlay_batch(inner, batch_offsets, &plans).await
                    }
                    .boxed(),
                }
            })
            .boxed();
        Ok(stream)
    }

    async fn new_read_impl<'a, F>(
        &'a self,
        params: ReadBatchParams,
        batch_size: u32,
        read_fn: F,
    ) -> Result<ReadBatchFutStream>
    where
        F: Fn(&'a dyn GenericFileReader) -> BoxFuture<'a, Result<ReadBatchTaskStream>>,
    {
        let total_num_rows = self.num_physical_rows as u32;
        // Note that the fragment length might be considerably smaller if there are deleted rows.
        // E.g. if a fragment has 100 rows but rows 0..10 are deleted we still need to make
        // sure it is valid to read / take 0..100
        if !params.valid_given_len(total_num_rows as usize) {
            return Err(Error::invalid_input(format!(
                "Invalid read params {} for fragment with {} addressable rows",
                params, total_num_rows
            )));
        }
        // If just the row id or address there is no need to actually read any data
        // and we don't need to involve the readers at all.
        //
        // The v1 reader does not support reading batches with zero columns, so
        // we need this as a separate code path.
        // In these cases, we can just emit batches with zero columns and rely
        // on `wrap_with_row_id_and_delete` to add the row id or address column.
        //
        // We could potentially delete the support for no-columns in the wrap function or
        // we can delete this path once we migrate away from any support of v1.
        let merged = if self.num_system_cols() == self.output_schema.fields.len() {
            let selected_rows = params.to_offsets_total(total_num_rows).len();
            let tasks = (0..selected_rows)
                .step_by(batch_size as usize)
                .map(move |offset| {
                    let num_rows = (batch_size as usize).min(selected_rows - offset);
                    let batch = RecordBatch::from(StructArray::new_empty_fields(num_rows, None));
                    ReadBatchTask {
                        task: std::future::ready(Ok(batch)).boxed(),
                        num_rows: num_rows as u32,
                    }
                });
            stream::iter(tasks).boxed()
        } else {
            // Read each data file, these reads should produce streams of equal sized
            // tasks.  In other words, if we get 3 tasks of 20 rows and then a task
            // of 10 rows from one data file we should get the same from the other.
            //
            // We launch all readers' scheduling work concurrently — for v2 files
            // this is where the decode scheduler's `initialize` I/O happens, so
            // running them in parallel keeps the per-file scheduling I/Os from
            // serializing.
            let read_futs = self.readers.iter().filter_map(|reader| {
                // Normally we filter out empty readers in the open_readers method
                // However, we will keep the first empty reader to use for row id
                // purposes on some legacy paths and so we need to filter that out
                // here.
                if reader.projection().fields.is_empty() {
                    None
                } else {
                    Some(read_fn(reader.as_ref()))
                }
            });
            let read_streams = futures::future::try_join_all(read_futs).await?;
            // Merge the streams, this merges the generated batches
            lance_table::utils::stream::merge_streams(read_streams)
        };

        let merged = self.merge_overlays(merged, &params, total_num_rows).await?;

        // Add the row id column (if needed) and delete rows (if a deletion
        // vector is present).
        let config = RowIdAndDeletesConfig {
            deletion_vector: self.deletion_vec.clone(),
            row_id_sequence: self.row_id_sequence.clone(),
            make_deletions_null: self.make_deletions_null,
            with_row_id: self.with_row_id,
            with_row_addr: self.with_row_addr,
            with_row_last_updated_at_version: self.with_row_last_updated_at_version,
            with_row_created_at_version: self.with_row_created_at_version,
            last_updated_at_sequence: self.last_updated_at_sequence.clone(),
            created_at_sequence: self.created_at_sequence.clone(),
            params,
            total_num_rows,
        };
        let output_schema = Arc::new(self.output_schema.clone());
        Ok(
            wrap_with_row_id_and_delete(merged, self.fragment_id as u32, config)
                // Finally, reorder the columns to match the order specified in the projection
                .map(move |batch_fut| {
                    let output_schema = output_schema.clone();
                    batch_fut
                        .map(move |batch| {
                            batch?
                                .project_by_schema(&output_schema)
                                .map_err(Error::from)
                        })
                        .boxed()
                })
                .boxed(),
        )
    }

    fn patch_range_for_deletions(&self, range: Range<u32>, dv: &DeletionVector) -> Range<u32> {
        let mut start = range.start;
        let mut end = range.end;
        for val in dv.to_sorted_iter() {
            if val <= start {
                start += 1;
                end += 1;
            } else if val < end {
                end += 1;
            } else {
                break;
            }
        }
        start..end
    }

    async fn do_read_range(
        &self,
        mut range: Range<u32>,
        batch_size: u32,
        skip_deleted_rows: bool,
    ) -> Result<ReadBatchFutStream> {
        if skip_deleted_rows && let Some(deletion_vector) = self.deletion_vec.as_ref() {
            range = self.patch_range_for_deletions(range, deletion_vector.as_ref());
        }
        self.new_read_impl(
            ReadBatchParams::Range(range.start as usize..range.end as usize),
            batch_size,
            move |reader| {
                reader.read_range_tasks(
                    range.start as u64..range.end as u64,
                    batch_size,
                    reader.projection().clone(),
                )
            },
        )
        .await
    }

    fn num_system_cols(&self) -> usize {
        self.with_row_id as usize
            + self.with_row_addr as usize
            + self.with_row_created_at_version as usize
            + self.with_row_last_updated_at_version as usize
    }

    /// Reads a range of rows from the fragment
    ///
    /// This function interprets the request as the Xth to the Nth row of the fragment (after deletions)
    /// and will always return range.len().min(self.num_rows()) rows.
    ///
    /// This is async because it drives the per-data-file decode scheduler
    /// `initialize` work before returning the stream — see
    /// [`GenericFileReader`].
    pub async fn read_range(
        &self,
        range: Range<u32>,
        batch_size: u32,
    ) -> Result<ReadBatchFutStream> {
        self.do_read_range(range, batch_size, true).await
    }

    /// Takes a range of rows from the fragment
    ///
    /// Unlike [`Self::read_range`], this function will NOT skip deleted rows.  If rows are deleted they will
    /// be filtered or set to null.  This function may return less than range.len() rows as a result.
    ///
    /// This is async for the same reason as [`Self::read_range`].
    pub async fn take_range(
        &self,
        range: Range<u32>,
        batch_size: u32,
    ) -> Result<ReadBatchFutStream> {
        self.do_read_range(range, batch_size, false).await
    }

    /// Reads all rows from the fragment.
    ///
    /// This is async for the same reason as [`Self::read_range`].
    pub async fn read_all(&self, batch_size: u32) -> Result<ReadBatchFutStream> {
        self.new_read_impl(ReadBatchParams::RangeFull, batch_size, move |reader| {
            reader.read_all_tasks(batch_size, reader.projection().clone())
        })
        .await
    }

    // This method is a clone of new_read_impl but returns tasks instead of batches
    //
    // It also only supports v2 files
    ///
    /// This is async for the same reason as [`Self::read_range`].
    pub async fn read_ranges(
        &self,
        ranges: Arc<[Range<u64>]>,
        batch_size: u32,
    ) -> Result<ReadBatchFutStream> {
        let total_num_rows = self.num_physical_rows as u32;
        let mut num_requested_rows = 0;
        // Note that row ranges at this point are physical and not logical.
        for range in ranges.as_ref() {
            if range.end > total_num_rows as u64 {
                return Err(Error::internal(format!(
                    "Invalid read of range {:?} for fragment {} with {} addressable rows",
                    range, self.fragment_id, total_num_rows
                )));
            }
            num_requested_rows += range.end - range.start;
        }

        let merged_stream = if self.num_system_cols() == self.output_schema.fields.len() {
            let tasks = (0..num_requested_rows)
                .step_by(batch_size as usize)
                .map(move |offset| {
                    let num_rows = (batch_size as u64).min(num_requested_rows - offset);
                    let batch =
                        RecordBatch::from(StructArray::new_empty_fields(num_rows as usize, None));
                    ReadBatchTask {
                        task: std::future::ready(Ok(batch)).boxed(),
                        num_rows: num_rows as u32,
                    }
                });
            stream::iter(tasks).boxed()
        } else {
            // Read each data file, these reads should produce streams of equal sized
            // tasks.  In other words, if we get 3 tasks of 20 rows and then a task
            // of 10 rows from one data file we should get the same from the other.
            //
            // Run all readers' scheduling concurrently so the per-file
            // `initialize` I/Os overlap.
            let read_futs = self.readers.iter().map(|reader| {
                reader.read_ranges_tasks(ranges.clone(), batch_size, reader.projection().clone())
            });
            let read_streams = futures::future::try_join_all(read_futs).await?;
            // Merge the streams, this merges the generated batches
            lance_table::utils::stream::merge_streams(read_streams)
        };

        let params = ReadBatchParams::Ranges(ranges);
        let merged_stream = self
            .merge_overlays(merged_stream, &params, total_num_rows)
            .await?;

        // Add the row id column (if needed) and delete rows (if a deletion
        // vector is present).
        let config = RowIdAndDeletesConfig {
            deletion_vector: self.deletion_vec.clone(),
            row_id_sequence: self.row_id_sequence.clone(),
            make_deletions_null: self.make_deletions_null,
            with_row_id: self.with_row_id,
            with_row_addr: self.with_row_addr,
            with_row_last_updated_at_version: self.with_row_last_updated_at_version,
            with_row_created_at_version: self.with_row_created_at_version,
            last_updated_at_sequence: self.last_updated_at_sequence.clone(),
            created_at_sequence: self.created_at_sequence.clone(),
            params,
            total_num_rows,
        };
        let output_schema = Arc::new(self.output_schema.clone());
        Ok(
            wrap_with_row_id_and_delete(merged_stream, self.fragment_id as u32, config)
                // Finally, reorder the columns to match the order specified in the projection
                .map(move |batch_fut| {
                    let output_schema = output_schema.clone();
                    batch_fut
                        .map(move |batch| {
                            batch?
                                .project_by_schema(&output_schema)
                                .map_err(Error::from)
                        })
                        .boxed()
                })
                .boxed(),
        )
    }

    // Legacy function that reads a range of data and concatenates the results
    // into a single batch
    //
    // TODO: Move away from this by changing callers to support consuming a stream
    pub async fn legacy_read_range_as_batch(&self, range: Range<usize>) -> Result<RecordBatch> {
        let batches = self
            .take_range(
                range.start as u32..range.end as u32,
                DEFAULT_BATCH_READ_SIZE,
            )
            .await?
            .buffered(get_num_compute_intensive_cpus())
            .try_collect::<Vec<_>>()
            .await?;
        concat_batches(&Arc::new(self.output_schema.clone()), batches.iter()).map_err(Error::from)
    }

    /// Take rows from this fragment.
    pub async fn take(
        &self,
        indices: &[u32],
        batch_size: u32,
        take_priority: Option<u32>,
    ) -> Result<ReadBatchFutStream> {
        let indices_arr = UInt32Array::from(indices.to_vec());
        self.new_read_impl(
            ReadBatchParams::Indices(indices_arr),
            batch_size,
            move |reader| {
                reader.take_all_tasks(
                    indices,
                    batch_size,
                    reader.projection().clone(),
                    take_priority,
                )
            },
        )
        .await
    }

    /// Take rows from this fragment, will perform a copy if the underlying reader returns multiple
    /// batches.  May return an error if the taken rows do not fit into a single batch.
    ///
    /// Duplicate indices are allowed and will produce duplicate rows in the output.
    pub async fn take_as_batch(
        &self,
        indices: &[u32],
        take_priority: Option<u32>,
    ) -> Result<RecordBatch> {
        // The v2 encoding layer requires strictly increasing indices. Deduplicate
        // here so callers (e.g. FTS with duplicate row matches) don't need to.
        let has_duplicates = indices.windows(2).any(|w| w[0] == w[1]);
        let (unique_indices, expand_map) = if has_duplicates {
            let mut unique: Vec<u32> = Vec::with_capacity(indices.len());
            let mut mapping: Vec<u32> = Vec::with_capacity(indices.len());
            for &idx in indices {
                if unique.last() != Some(&idx) {
                    unique.push(idx);
                }
                mapping.push((unique.len() - 1) as u32);
            }
            (Cow::Owned(unique), Some(UInt32Array::from(mapping)))
        } else {
            (Cow::Borrowed(indices), None)
        };

        let batches = self
            .take(&unique_indices, u32::MAX, take_priority)
            .await?
            .buffered(get_num_compute_intensive_cpus())
            .try_collect::<Vec<_>>()
            .await?;
        let mut batch = concat_batches(&Arc::new(self.output_schema.clone()), batches.iter())?;

        if let Some(expand_map) = expand_map {
            batch = arrow_select::take::take_record_batch(&batch, &expand_map)?;
        }

        Ok(batch)
    }
}

/// A wrapper around a `RecordBatchReader` that converts Arrow JSON columns
/// (Utf8/LargeUtf8 with `arrow.json` extension) to Lance JSON columns
/// (LargeBinary with `lance.json` extension / JSONB format).
///
/// This is needed when user-provided data contains Arrow JSON fields but the
/// dataset stores them in Lance's JSONB binary format.
struct JsonConvertingReader {
    inner: Box<dyn RecordBatchReader + Send>,
    schema: arrow_schema::SchemaRef,
}

impl JsonConvertingReader {
    fn new(inner: Box<dyn RecordBatchReader + Send>) -> Self {
        use lance_arrow::json::arrow_json_to_lance_json;

        // Build the converted schema (Arrow JSON fields → Lance JSON fields)
        let orig_schema = inner.schema();
        let new_fields: Vec<arrow_schema::FieldRef> = orig_schema
            .fields()
            .iter()
            .map(|f| {
                if is_arrow_json_field(f) || has_json_fields(f) {
                    Arc::new(arrow_json_to_lance_json(f))
                } else {
                    Arc::clone(f)
                }
            })
            .collect();
        let schema = Arc::new(arrow_schema::Schema::new_with_metadata(
            new_fields,
            orig_schema.metadata().clone(),
        ));

        Self { inner, schema }
    }
}

impl Iterator for JsonConvertingReader {
    type Item = std::result::Result<RecordBatch, arrow_schema::ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner
            .next()
            .map(|result| result.and_then(|batch| convert_json_columns(&batch)))
    }
}

impl RecordBatchReader for JsonConvertingReader {
    fn schema(&self) -> arrow_schema::SchemaRef {
        self.schema.clone()
    }
}

#[cfg(test)]
mod tests {
    use arrow_arith::numeric::mul;
    use arrow_array::{
        ArrayRef, BooleanArray, Int32Array, Int64Array, RecordBatchIterator, StringArray,
    };
    use arrow_schema::{DataType, Field as ArrowField, Schema as ArrowSchema};
    use lance_core::ROW_ID;
    use lance_core::utils::tempfile::TempStrDir;
    use lance_datagen::{RowCount, array, gen_batch};
    use lance_file::version::{ConcreteFileVersion, LanceFileVersion};
    use lance_file::writer::FileWriterOptions;
    use lance_io::{assert_io_eq, assert_io_lt, object_store::ObjectStore};
    use pretty_assertions::assert_eq;
    use rstest::rstest;
    use std::collections::HashMap;

    use super::*;
    use crate::{
        dataset::{
            InsertBuilder,
            transaction::{Operation, UpdateMode, UpdatedFragmentOffsets},
        },
        session::Session,
        utils::test::TestDatasetGenerator,
    };

    async fn create_dataset(test_uri: &str, data_storage_version: LanceFileVersion) -> Dataset {
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("i", DataType::Int32, true),
            ArrowField::new("s", DataType::Utf8, true),
        ]));

        let batches: Vec<RecordBatch> = (0..10)
            .map(|i| {
                RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(Int32Array::from_iter_values(i * 20..(i + 1) * 20)),
                        Arc::new(StringArray::from_iter_values(
                            (i * 20..(i + 1) * 20).map(|v| format!("s-{}", v)),
                        )),
                    ],
                )
                .unwrap()
            })
            .collect();

        let write_params = WriteParams {
            max_rows_per_file: 40,
            max_rows_per_group: 10,
            data_storage_version: Some(data_storage_version),
            ..Default::default()
        };
        let batches = RecordBatchIterator::new(batches.into_iter().map(Ok), schema.clone());
        Dataset::write(batches, test_uri, Some(write_params))
            .await
            .unwrap();

        Dataset::open(test_uri).await.unwrap()
    }

    async fn create_dataset_v2(test_uri: &str) -> Dataset {
        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "i",
            DataType::Int32,
            true,
        )]));

        let batches: Vec<RecordBatch> = (0..10)
            .map(|i| {
                RecordBatch::try_new(
                    schema.clone(),
                    vec![Arc::new(Int32Array::from_iter_values(i * 20..(i + 1) * 20))],
                )
                .unwrap()
            })
            .collect();

        let write_params = WriteParams {
            max_rows_per_file: 40,
            max_rows_per_group: 10,
            data_storage_version: Some(LanceFileVersion::Stable),
            ..Default::default()
        };
        let batches = RecordBatchIterator::new(batches.into_iter().map(Ok), schema.clone());
        Dataset::write(batches, test_uri, Some(write_params))
            .await
            .unwrap();

        Dataset::open(test_uri).await.unwrap()
    }

    /// End-to-end tests for reading data overlay files (OSS-1324): overlays are
    /// written, committed via the `DataOverlay` transaction, and then resolved on
    /// the `take` and scan read paths.
    mod overlay_read {
        use std::sync::Arc;

        use arrow_array::{
            Array, ArrayRef, Int32Array, RecordBatch, RecordBatchIterator, StructArray, UInt64Array,
        };
        use arrow_schema::{DataType, Field as ArrowField, Fields, Schema as ArrowSchema};
        use lance_core::datatypes::Schema;
        use lance_file::version::{ConcreteFileVersion, LanceFileVersion};
        use lance_file::writer::FileWriterOptions;
        use lance_io::utils::CachedFileSize;
        use lance_table::format::DataFile;
        use lance_table::format::overlay::{DataOverlayFile, OverlayCoverage};
        use object_store::path::Path;
        use roaring::RoaringBitmap;
        use rstest::rstest;

        use crate::dataset::transaction::{DataOverlayGroup, Operation};
        use crate::dataset::{Dataset, WriteDestination, WriteParams};

        fn bitmap(offsets: impl IntoIterator<Item = u32>) -> RoaringBitmap {
            RoaringBitmap::from_iter(offsets)
        }

        fn i32_array(values: impl IntoIterator<Item = Option<i32>>) -> ArrayRef {
            Arc::new(Int32Array::from_iter(values))
        }

        /// Two-fragment Int32 dataset: `id` (field 0) = 0..12 and `val` (field 1)
        /// = id * 10, written 6 rows per file (fragments 0 and 1).
        ///
        /// Uses an in-memory store so the test can write overlay files with a
        /// store-relative `data/<name>.lance` path and commit against the returned
        /// dataset directly.
        async fn create_base_dataset(version: LanceFileVersion) -> Dataset {
            let schema = Arc::new(ArrowSchema::new(vec![
                ArrowField::new("id", DataType::Int32, true),
                ArrowField::new("val", DataType::Int32, true),
            ]));
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from_iter_values(0..12)),
                    Arc::new(Int32Array::from_iter_values((0..12).map(|v| v * 10))),
                ],
            )
            .unwrap();
            let write_params = WriteParams {
                max_rows_per_file: 6,
                max_rows_per_group: 6,
                data_storage_version: Some(version),
                ..Default::default()
            };
            let reader = RecordBatchIterator::new(vec![Ok(batch)], schema.clone());
            Dataset::write(reader, "memory://", Some(write_params))
                .await
                .unwrap()
        }

        /// Write an overlay file covering `fields` (dataset field ids) of
        /// `fragment_id` with the given coverage and per-field value columns, then
        /// commit it as a `DataOverlay` transaction. `name` makes the file unique.
        #[allow(clippy::too_many_arguments)]
        async fn commit_overlay(
            dataset: Dataset,
            name: &str,
            fragment_id: u64,
            fields: &[i32],
            coverage: OverlayCoverage,
            columns: Vec<ArrayRef>,
            version: LanceFileVersion,
        ) -> Dataset {
            let read_version = dataset.version().version;
            let overlay_schema = dataset.schema().project_by_ids(fields, true);

            let filename = format!("{name}.lance");
            let path = Path::from(format!("data/{filename}"));
            let obj_writer = dataset.object_store.create(&path).await.unwrap();
            let file_version = ConcreteFileVersion::from(version);
            let mut writer = lance_file::versions::create_writer(
                file_version,
                obj_writer,
                overlay_schema,
                FileWriterOptions::default(),
            )
            .unwrap();
            for (column_index, array) in columns.into_iter().enumerate() {
                writer.write_column(column_index, array).await.unwrap();
            }
            let summary = writer.finish().await.unwrap();

            let mut data_file = DataFile::new_unstarted(filename, file_version);
            data_file.fields = writer
                .field_id_to_column_indices()
                .iter()
                .map(|(field_id, _)| *field_id as i32)
                .collect::<Vec<_>>()
                .into();
            data_file.column_indices = writer
                .field_id_to_column_indices()
                .iter()
                .map(|(_, column_index)| *column_index as i32)
                .collect::<Vec<_>>()
                .into();
            data_file.file_size_bytes = CachedFileSize::new(summary.size_bytes);

            let overlay = DataOverlayFile {
                data_file,
                coverage,
                committed_version: 0,
            };
            Dataset::commit(
                WriteDestination::Dataset(Arc::new(dataset)),
                Operation::DataOverlay {
                    groups: vec![DataOverlayGroup {
                        fragment_id,
                        overlays: vec![overlay],
                    }],
                },
                Some(read_version),
                None,
                None,
                Arc::new(Default::default()),
                false,
            )
            .await
            .unwrap()
        }

        fn full_schema(dataset: &Dataset) -> Schema {
            dataset.schema().clone()
        }

        fn col(batch: &RecordBatch, name: &str) -> Int32Array {
            let idx = batch.schema().index_of(name).unwrap();
            batch
                .column(idx)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .clone()
        }

        #[rstest]
        #[tokio::test]
        async fn test_take_covered_and_uncovered(
            #[values(LanceFileVersion::V2_0, LanceFileVersion::V2_1)] version: LanceFileVersion,
        ) {
            let dataset = create_base_dataset(version).await;
            // Overlay fragment 0's `val` at physical offsets {1, 4}.
            let dataset = commit_overlay(
                dataset,
                "ov",
                0,
                &[1],
                OverlayCoverage::dense(bitmap([1, 4])),
                vec![i32_array([Some(111), Some(444)])],
                version,
            )
            .await;

            let frag = dataset.get_fragment(0).unwrap();
            let batch = frag
                .take(&[0, 1, 2, 4], &full_schema(&dataset))
                .await
                .unwrap();
            // Offsets 1 and 4 take overlay values; 0 and 2 fall through to base.
            assert_eq!(col(&batch, "val").values(), &[0, 111, 20, 444]);
            // The unrelated `id` column is untouched.
            assert_eq!(col(&batch, "id").values(), &[0, 1, 2, 4]);
        }

        #[rstest]
        #[tokio::test]
        async fn test_take_newest_overlay_wins(
            #[values(LanceFileVersion::V2_0, LanceFileVersion::V2_1)] version: LanceFileVersion,
        ) {
            let dataset = create_base_dataset(version).await;
            let dataset = commit_overlay(
                dataset,
                "older",
                0,
                &[1],
                OverlayCoverage::dense(bitmap([1, 4])),
                vec![i32_array([Some(111), Some(444)])],
                version,
            )
            .await;
            // A newer overlay (later commit -> higher committed_version) re-covers
            // offset 1.
            let dataset = commit_overlay(
                dataset,
                "newer",
                0,
                &[1],
                OverlayCoverage::dense(bitmap([1])),
                vec![i32_array([Some(999)])],
                version,
            )
            .await;

            let frag = dataset.get_fragment(0).unwrap();
            let batch = frag.take(&[1, 4], &full_schema(&dataset)).await.unwrap();
            // Offset 1 -> newest overlay (999); offset 4 -> only older covers it.
            assert_eq!(col(&batch, "val").values(), &[999, 444]);
        }

        #[rstest]
        #[tokio::test]
        async fn test_take_per_field_coverage(
            #[values(LanceFileVersion::V2_0, LanceFileVersion::V2_1)] version: LanceFileVersion,
        ) {
            let dataset = create_base_dataset(version).await;
            // Sparse overlay: `id` covers {2}, `val` covers {2, 3} — different
            // offset sets and therefore unequal-length value columns.
            let dataset = commit_overlay(
                dataset,
                "sparse",
                0,
                &[0, 1],
                OverlayCoverage::sparse(vec![bitmap([2]), bitmap([2, 3])]),
                vec![i32_array([Some(777)]), i32_array([Some(220), Some(330)])],
                version,
            )
            .await;

            let frag = dataset.get_fragment(0).unwrap();
            let batch = frag.take(&[2, 3], &full_schema(&dataset)).await.unwrap();
            // id: offset 2 covered (777), offset 3 falls through (3).
            assert_eq!(col(&batch, "id").values(), &[777, 3]);
            // val: both offsets covered (220, 330).
            assert_eq!(col(&batch, "val").values(), &[220, 330]);
        }

        #[rstest]
        #[tokio::test]
        async fn test_take_null_override(
            #[values(LanceFileVersion::V2_0, LanceFileVersion::V2_1)] version: LanceFileVersion,
        ) {
            let dataset = create_base_dataset(version).await;
            let dataset = commit_overlay(
                dataset,
                "nullov",
                0,
                &[1],
                OverlayCoverage::dense(bitmap([0])),
                vec![i32_array([None])],
                version,
            )
            .await;

            let frag = dataset.get_fragment(0).unwrap();
            let batch = frag.take(&[0, 1], &full_schema(&dataset)).await.unwrap();
            let val = col(&batch, "val");
            // Offset 0 is covered with a NULL value -> resolves to NULL; offset 1
            // falls through to the base value.
            assert!(val.is_null(0));
            assert_eq!(val.value(1), 10);
        }

        /// Overlays interact correctly with NULL *base* cells (distinct from a NULL
        /// overlay value): a covered row whose base value is NULL is overridden to the
        /// overlay's non-null value, while an uncovered NULL base cell falls through
        /// and stays NULL.
        #[rstest]
        #[tokio::test]
        async fn test_take_null_base_cell(
            #[values(LanceFileVersion::V2_0, LanceFileVersion::V2_1)] version: LanceFileVersion,
        ) {
            let schema = Arc::new(ArrowSchema::new(vec![
                ArrowField::new("id", DataType::Int32, true),
                ArrowField::new("val", DataType::Int32, true),
            ]));
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from_iter_values(0..6)),
                    // `val` is NULL at offsets 1 and 3.
                    Arc::new(Int32Array::from_iter([
                        Some(0),
                        None,
                        Some(20),
                        None,
                        Some(40),
                        Some(50),
                    ])),
                ],
            )
            .unwrap();
            let write_params = WriteParams {
                max_rows_per_file: 6,
                max_rows_per_group: 6,
                data_storage_version: Some(version),
                ..Default::default()
            };
            let reader = RecordBatchIterator::new(vec![Ok(batch)], schema.clone());
            let dataset = Dataset::write(reader, "memory://", Some(write_params))
                .await
                .unwrap();

            // Cover offset 1 (NULL base) and offset 4 (non-null base); leave offset
            // 3's NULL base uncovered.
            let dataset = commit_overlay(
                dataset,
                "nullbase",
                0,
                &[1],
                OverlayCoverage::dense(bitmap([1, 4])),
                vec![i32_array([Some(111), Some(444)])],
                version,
            )
            .await;

            let frag = dataset.get_fragment(0).unwrap();
            let batch = frag.take(&[1, 3, 4], &full_schema(&dataset)).await.unwrap();
            let val = col(&batch, "val");
            // Offset 1: NULL base overridden to 111. Offset 3: uncovered NULL base
            // stays NULL. Offset 4: non-null base overridden to 444.
            assert_eq!(val.value(0), 111);
            assert!(val.is_null(1));
            assert_eq!(val.value(2), 444);
        }

        #[rstest]
        #[tokio::test]
        async fn test_overlay_on_deleted_row_is_inert(
            #[values(LanceFileVersion::V2_0, LanceFileVersion::V2_1)] version: LanceFileVersion,
        ) {
            let mut dataset = create_base_dataset(version).await;
            // Delete global row 1 (fragment 0, physical offset 1).
            dataset.delete("id = 1").await.unwrap();
            // Overlay covers the deleted offset 1 and the live offset 4.
            let dataset = commit_overlay(
                dataset,
                "delov",
                0,
                &[1],
                OverlayCoverage::dense(bitmap([1, 4])),
                vec![i32_array([Some(111), Some(444)])],
                version,
            )
            .await;

            // Scan fragment 0: row 1 is gone, and offset 4's overlay value survives
            // even though the deletion shifts logical positions — coverage is keyed
            // by physical offset.
            let frag = dataset.get_fragment(0).unwrap();
            let mut scanner = frag.scan();
            let batch = scanner
                .project(&["id", "val"])
                .unwrap()
                .try_into_batch()
                .await
                .unwrap();
            assert_eq!(col(&batch, "id").values(), &[0, 2, 3, 4, 5]);
            assert_eq!(col(&batch, "val").values(), &[0, 20, 30, 444, 50]);
        }

        #[rstest]
        #[tokio::test]
        async fn test_scan_multi_fragment_overlays(
            #[values(LanceFileVersion::V2_0, LanceFileVersion::V2_1)] version: LanceFileVersion,
        ) {
            let dataset = create_base_dataset(version).await;
            // Overlay fragment 0 at offset 0 and fragment 1 at offset 0 (global
            // row 6). Each fragment's coverage is independent.
            let dataset = commit_overlay(
                dataset,
                "frag0",
                0,
                &[1],
                OverlayCoverage::dense(bitmap([0])),
                vec![i32_array([Some(1000)])],
                version,
            )
            .await;
            let dataset = commit_overlay(
                dataset,
                "frag1",
                1,
                &[1],
                OverlayCoverage::dense(bitmap([0])),
                vec![i32_array([Some(6000)])],
                version,
            )
            .await;

            let batch = dataset
                .scan()
                .project(&["id", "val"])
                .unwrap()
                .try_into_batch()
                .await
                .unwrap();
            assert_eq!(batch.num_rows(), 12);
            let expected: Vec<i32> = (0..12)
                .map(|i| match i {
                    0 => 1000,
                    6 => 6000,
                    other => other * 10,
                })
                .collect();
            assert_eq!(col(&batch, "val").values(), &expected);
        }

        /// A `take` of a few rows must read only the overlay values those rows
        /// touch — not the whole column. Uses v2.1 (which slices pages on read) and
        /// an incompressible, all-covering overlay, so reading the full column would
        /// be far more bytes than reading a couple of values. This is the regression
        /// guard for the lazy, value-pushdown overlay read.
        #[tokio::test]
        async fn test_take_reads_only_needed_overlay_values() {
            let version = LanceFileVersion::V2_1;
            const N: usize = 100_000;

            let schema = Arc::new(ArrowSchema::new(vec![
                ArrowField::new("id", DataType::Int32, true),
                ArrowField::new("val", DataType::Int32, true),
            ]));
            let base = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from_iter_values(0..N as i32)),
                    Arc::new(Int32Array::from_iter_values((0..N as i32).map(|v| v * 10))),
                ],
            )
            .unwrap();
            let write_params = WriteParams {
                max_rows_per_file: N,
                max_rows_per_group: N,
                data_storage_version: Some(version),
                ..Default::default()
            };
            let reader = RecordBatchIterator::new(vec![Ok(base)], schema.clone());
            let dataset = Dataset::write(reader, "memory://", Some(write_params))
                .await
                .unwrap();

            // Overlay `val` over ALL N offsets with incompressible values, so the
            // value column is ~N*4 bytes on disk.
            let values: Vec<i32> = (0..N as u64)
                .map(|i| {
                    let mut x = i;
                    x ^= x >> 33;
                    x = x.wrapping_mul(0xff51_afd7_ed55_8ccd);
                    x ^= x >> 33;
                    x as i32
                })
                .collect();
            let dataset = commit_overlay(
                dataset,
                "big",
                0,
                &[1],
                OverlayCoverage::dense(bitmap(0..N as u32)),
                vec![Arc::new(Int32Array::from(values.clone())) as ArrayRef],
                version,
            )
            .await;

            let frag = dataset.get_fragment(0).unwrap();
            let val_only = dataset.schema().project_by_ids(&[1], true);

            // Measure only the reads that resolve the take.
            dataset.object_store.io_stats_incremental();
            let batch = frag.take(&[0, 1], &val_only).await.unwrap();
            let io = dataset.object_store.io_stats_incremental();

            // The overlay's `val` column alone is N*4 bytes; resolving two adjacent
            // offsets must read only a small fraction of it.
            let full_column_bytes = (N * std::mem::size_of::<i32>()) as u64;
            assert!(
                io.read_bytes > 0 && io.read_bytes < full_column_bytes / 4,
                "take read {} bytes; expected far less than the {}-byte overlay \
                 column (a take must not read the whole value column)",
                io.read_bytes,
                full_column_bytes,
            );

            // ...and it still resolves correctly.
            let val = col(&batch, "val");
            assert_eq!(val.value(0), values[0]);
            assert_eq!(val.value(1), values[1]);
        }

        /// Row-selection pruning: an overlay whose coverage is disjoint from the
        /// requested rows must not be opened at all. Proven by deleting the overlay's
        /// data file — a `take` that misses its coverage still succeeds (the file is
        /// never touched), while a `take` that hits it then fails because the file is
        /// genuinely needed.
        #[rstest]
        #[tokio::test]
        async fn test_take_prunes_overlays_outside_row_selection(
            #[values(LanceFileVersion::V2_0, LanceFileVersion::V2_1)] version: LanceFileVersion,
        ) {
            let dataset = create_base_dataset(version).await;
            // Overlay on fragment 0 (offsets 0..6) covering only offset_in_frag 5.
            let dataset = commit_overlay(
                dataset,
                "miss",
                0,
                &[1],
                OverlayCoverage::dense(bitmap([5])),
                vec![i32_array([Some(5000)])],
                version,
            )
            .await;

            // Delete the overlay's data file: opening it now fails.
            dataset
                .object_store
                .delete(&Path::from("data/miss.lance"))
                .await
                .unwrap();

            let frag = dataset.get_fragment(0).unwrap();
            let val_only = dataset.schema().project_by_ids(&[1], true);

            // A take that misses the overlay's coverage must not open it, so it
            // succeeds and returns base values (val = offset * 10).
            let batch = frag.take(&[0, 1], &val_only).await.unwrap();
            assert_eq!(col(&batch, "val").values(), &[0, 10]);

            // A take that hits the coverage does need the file, so it now fails with
            // a not-found error naming the missing overlay file.
            let err = frag.take(&[5], &val_only).await.unwrap_err();
            let message = format!("{err:?}");
            assert!(
                err.is_not_found() && message.contains("miss.lance"),
                "take hitting the overlay's coverage should fail with a not-found error \
                 for its missing file, got: {message}",
            );
        }

        /// The overlay merge runs before `wrap_with_row_id_and_delete`, so the
        /// `_rowid` system column must coexist with overlay-resolved data columns:
        /// the row ids are unaffected by the merge and the overlay value still wins.
        #[rstest]
        #[tokio::test]
        async fn test_scan_with_row_id_alongside_overlay(
            #[values(LanceFileVersion::V2_0, LanceFileVersion::V2_1)] version: LanceFileVersion,
        ) {
            let dataset = create_base_dataset(version).await;
            let dataset = commit_overlay(
                dataset,
                "rowidov",
                0,
                &[1],
                OverlayCoverage::dense(bitmap([0])),
                vec![i32_array([Some(1000)])],
                version,
            )
            .await;

            let frag = dataset.get_fragment(0).unwrap();
            let batch = frag
                .scan()
                .with_row_id()
                .project(&["id", "val"])
                .unwrap()
                .try_into_batch()
                .await
                .unwrap();
            // Overlay value resolves...
            assert_eq!(col(&batch, "val").values()[0], 1000);
            assert_eq!(&col(&batch, "val").values()[1..], &[10, 20, 30, 40, 50]);
            // ...and the row ids for fragment 0 are the untouched physical offsets.
            let row_ids = batch
                .column(batch.schema().index_of("_rowid").unwrap())
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();
            assert_eq!(row_ids.values(), &[0, 1, 2, 3, 4, 5]);
        }

        /// When the newest overlay covers every requested offset, an older overlay
        /// in the same plan needs zero values and its value column must not be read
        /// (the empty-input branch of `fetch_overlay_values`). The result still
        /// resolves to the newest overlay.
        #[rstest]
        #[tokio::test]
        async fn test_take_older_overlay_contributes_no_values(
            #[values(LanceFileVersion::V2_0, LanceFileVersion::V2_1)] version: LanceFileVersion,
        ) {
            let dataset = create_base_dataset(version).await;
            // Older covers {1, 4}; newer re-covers {1}. A take of only offset 1
            // routes entirely to the newer overlay, leaving the older one with no
            // values to fetch even though it is part of the field's plan.
            let dataset = commit_overlay(
                dataset,
                "older",
                0,
                &[1],
                OverlayCoverage::dense(bitmap([1, 4])),
                vec![i32_array([Some(111), Some(444)])],
                version,
            )
            .await;
            let dataset = commit_overlay(
                dataset,
                "newer",
                0,
                &[1],
                OverlayCoverage::dense(bitmap([1])),
                vec![i32_array([Some(999)])],
                version,
            )
            .await;

            let frag = dataset.get_fragment(0).unwrap();
            let batch = frag.take(&[1], &full_schema(&dataset)).await.unwrap();
            assert_eq!(col(&batch, "val").values(), &[999]);
        }

        /// A newest overlay whose value is NULL must shadow an older overlay's
        /// non-null value at the same offset — the merge resolves to NULL, it does
        /// not fall back to the older overlay.
        #[rstest]
        #[tokio::test]
        async fn test_take_newest_null_shadows_older(
            #[values(LanceFileVersion::V2_0, LanceFileVersion::V2_1)] version: LanceFileVersion,
        ) {
            let dataset = create_base_dataset(version).await;
            let dataset = commit_overlay(
                dataset,
                "older",
                0,
                &[1],
                OverlayCoverage::dense(bitmap([1])),
                vec![i32_array([Some(111)])],
                version,
            )
            .await;
            let dataset = commit_overlay(
                dataset,
                "newer_null",
                0,
                &[1],
                OverlayCoverage::dense(bitmap([1])),
                vec![i32_array([None])],
                version,
            )
            .await;

            let frag = dataset.get_fragment(0).unwrap();
            let batch = frag.take(&[1], &full_schema(&dataset)).await.unwrap();
            let val = col(&batch, "val");
            assert!(val.is_null(0), "newest NULL must win over older 111");
        }

        /// Newest-wins is resolved independently per field across multiple sparse
        /// overlays: for the same offset, `id` can resolve to one overlay while
        /// `val` resolves to the other, depending on which overlay newly covers
        /// that field at that offset.
        #[rstest]
        #[tokio::test]
        async fn test_take_multi_sparse_per_field_newest_wins(
            #[values(LanceFileVersion::V2_0, LanceFileVersion::V2_1)] version: LanceFileVersion,
        ) {
            let dataset = create_base_dataset(version).await;
            // Older: id covers {3}, val covers {2}.
            let dataset = commit_overlay(
                dataset,
                "older",
                0,
                &[0, 1],
                OverlayCoverage::sparse(vec![bitmap([3]), bitmap([2])]),
                vec![i32_array([Some(7773)]), i32_array([Some(2772)])],
                version,
            )
            .await;
            // Newer: id covers {2}, val covers {3} — the mirror image.
            let dataset = commit_overlay(
                dataset,
                "newer",
                0,
                &[0, 1],
                OverlayCoverage::sparse(vec![bitmap([2]), bitmap([3])]),
                vec![i32_array([Some(9992)]), i32_array([Some(9993)])],
                version,
            )
            .await;

            let frag = dataset.get_fragment(0).unwrap();
            let batch = frag.take(&[2, 3], &full_schema(&dataset)).await.unwrap();
            // id: offset 2 -> newer (9992), offset 3 -> older (7773).
            assert_eq!(col(&batch, "id").values(), &[9992, 7773]);
            // val: offset 2 -> older (2772), offset 3 -> newer (9993).
            assert_eq!(col(&batch, "val").values(), &[2772, 9993]);
        }

        /// A fragment with an overlay plan, but a take that touches only uncovered
        /// offsets, must fall entirely through to the base values (the
        /// `!routing.any_overlay` early-return with a plan present).
        #[rstest]
        #[tokio::test]
        async fn test_take_plan_present_all_offsets_uncovered(
            #[values(LanceFileVersion::V2_0, LanceFileVersion::V2_1)] version: LanceFileVersion,
        ) {
            let dataset = create_base_dataset(version).await;
            let dataset = commit_overlay(
                dataset,
                "ov",
                0,
                &[1],
                OverlayCoverage::dense(bitmap([1, 4])),
                vec![i32_array([Some(111), Some(444)])],
                version,
            )
            .await;

            let frag = dataset.get_fragment(0).unwrap();
            // None of {0, 2, 5} are covered: the plan exists but contributes nothing.
            let batch = frag.take(&[0, 2, 5], &full_schema(&dataset)).await.unwrap();
            assert_eq!(col(&batch, "val").values(), &[0, 20, 50]);
            assert_eq!(col(&batch, "id").values(), &[0, 2, 5]);
        }

        /// A dataset-level `take` spanning multiple fragments, each with its own
        /// overlay, routes every global row index to the right fragment's overlay.
        #[rstest]
        #[tokio::test]
        async fn test_dataset_take_multi_fragment_overlays(
            #[values(LanceFileVersion::V2_0, LanceFileVersion::V2_1)] version: LanceFileVersion,
        ) {
            let dataset = create_base_dataset(version).await;
            let dataset = commit_overlay(
                dataset,
                "frag0",
                0,
                &[1],
                OverlayCoverage::dense(bitmap([0])),
                vec![i32_array([Some(1000)])],
                version,
            )
            .await;
            let dataset = commit_overlay(
                dataset,
                "frag1",
                1,
                &[1],
                OverlayCoverage::dense(bitmap([0])),
                vec![i32_array([Some(6000)])],
                version,
            )
            .await;

            // Global rows 0 and 6 are the overlaid offset-0 rows of fragments 0 and
            // 1; rows 1 and 7 fall through to base.
            let batch = dataset
                .take(&[0, 1, 6, 7], full_schema(&dataset))
                .await
                .unwrap();
            assert_eq!(col(&batch, "id").values(), &[0, 1, 6, 7]);
            assert_eq!(col(&batch, "val").values(), &[1000, 10, 6000, 70]);
        }

        /// A scan whose read splits into multiple batches must slice
        /// `offsets_in_frag` per batch correctly — the running `rows_seen`
        /// accumulator in `merge_overlays` gives each batch its start. Every other
        /// scan test uses single-batch fragments, so this is the only guard for the
        /// cross-batch (`start > 0`) path.
        #[rstest]
        #[tokio::test]
        async fn test_scan_multi_batch_overlay_slicing(
            #[values(LanceFileVersion::V2_0, LanceFileVersion::V2_1)] version: LanceFileVersion,
        ) {
            use futures::TryStreamExt;

            // One fragment of 10 rows so the read can be chunked below.
            let schema = Arc::new(ArrowSchema::new(vec![
                ArrowField::new("id", DataType::Int32, true),
                ArrowField::new("val", DataType::Int32, true),
            ]));
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from_iter_values(0..10)),
                    Arc::new(Int32Array::from_iter_values((0..10).map(|v| v * 10))),
                ],
            )
            .unwrap();
            let write_params = WriteParams {
                max_rows_per_file: 100,
                max_rows_per_group: 100,
                data_storage_version: Some(version),
                ..Default::default()
            };
            let reader = RecordBatchIterator::new(vec![Ok(batch)], schema.clone());
            let dataset = Dataset::write(reader, "memory://", Some(write_params))
                .await
                .unwrap();

            // Overlay one offset in each batch that batch_size 4 produces (batches
            // [0,4), [4,8), [8,10)): offsets 1, 5, 9 with distinct values. A wrong
            // per-batch slice would misalign these.
            let dataset = commit_overlay(
                dataset,
                "multibatch",
                0,
                &[1],
                OverlayCoverage::dense(bitmap([1, 5, 9])),
                vec![i32_array([Some(111), Some(555), Some(999)])],
                version,
            )
            .await;

            let frag = dataset.get_fragment(0).unwrap();
            let mut scanner = frag.scan();
            scanner.batch_size(4).project(&["val"]).unwrap();
            let batches: Vec<RecordBatch> = scanner
                .try_into_stream()
                .await
                .unwrap()
                .try_collect()
                .await
                .unwrap();
            // Guard the guard: the read must actually span multiple batches, else
            // this would not exercise the cross-batch slice at all.
            assert!(
                batches.len() > 1,
                "expected a multi-batch scan, got {} batch(es)",
                batches.len()
            );

            let merged =
                arrow_select::concat::concat_batches(&batches[0].schema(), &batches).unwrap();
            let expected: Vec<i32> = (0..10)
                .map(|i| match i {
                    1 => 111,
                    5 => 555,
                    9 => 999,
                    other => other * 10,
                })
                .collect();
            assert_eq!(col(&merged, "val").values(), &expected);
        }

        /// An empty selection must not trip over the overlay path: the plan exists
        /// but there are no offsets to route, so the result is an empty batch.
        #[rstest]
        #[tokio::test]
        async fn test_take_empty_selection(
            #[values(LanceFileVersion::V2_0, LanceFileVersion::V2_1)] version: LanceFileVersion,
        ) {
            let dataset = create_base_dataset(version).await;
            let dataset = commit_overlay(
                dataset,
                "ov",
                0,
                &[1],
                OverlayCoverage::dense(bitmap([1, 4])),
                vec![i32_array([Some(111), Some(444)])],
                version,
            )
            .await;

            let frag = dataset.get_fragment(0).unwrap();
            let batch = frag.take(&[], &full_schema(&dataset)).await.unwrap();
            assert_eq!(batch.num_rows(), 0);
        }

        /// Overlays resolve variable-width columns end-to-end, not just fixed-width
        /// ones: the value column is fetched through the real file reader (a
        /// different value-pushdown path than the fixed-width case) and assembled.
        #[rstest]
        #[tokio::test]
        async fn test_string_overlay_end_to_end(
            #[values(LanceFileVersion::V2_0, LanceFileVersion::V2_1)] version: LanceFileVersion,
        ) {
            use arrow_array::StringArray;

            let schema = Arc::new(ArrowSchema::new(vec![
                ArrowField::new("id", DataType::Int32, true),
                ArrowField::new("name", DataType::Utf8, true),
            ]));
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from_iter_values(0..6)),
                    Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e", "f"])),
                ],
            )
            .unwrap();
            let write_params = WriteParams {
                max_rows_per_file: 6,
                max_rows_per_group: 6,
                data_storage_version: Some(version),
                ..Default::default()
            };
            let reader = RecordBatchIterator::new(vec![Ok(batch)], schema.clone());
            let dataset = Dataset::write(reader, "memory://", Some(write_params))
                .await
                .unwrap();

            // Overlay `name` at offsets {1, 4}, one of the values NULL.
            let dataset = commit_overlay(
                dataset,
                "strov",
                0,
                &[1],
                OverlayCoverage::dense(bitmap([1, 4])),
                vec![Arc::new(StringArray::from(vec![Some("B"), None])) as ArrayRef],
                version,
            )
            .await;

            let frag = dataset.get_fragment(0).unwrap();
            let batch = frag.take(&[0, 1, 4], &full_schema(&dataset)).await.unwrap();
            let name = batch
                .column(batch.schema().index_of("name").unwrap())
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            assert_eq!(name.value(0), "a"); // falls through to base
            assert_eq!(name.value(1), "B"); // overlay value
            assert!(name.is_null(2)); // overlay NULL wins
        }

        /// Projection pruning must do NO IO to overlay files whose fields are not
        /// projected. Proven the same way as row-selection pruning: delete the
        /// overlay's data file, then read projecting only the *unrelated* `id`
        /// column — it must succeed (the `val` overlay file is never opened), while
        /// projecting the overlaid `val` column then fails because its file is gone.
        #[rstest]
        #[tokio::test]
        async fn test_projection_prunes_overlay_files_no_io(
            #[values(LanceFileVersion::V2_0, LanceFileVersion::V2_1)] version: LanceFileVersion,
        ) {
            let dataset = create_base_dataset(version).await;
            // Overlay covers `val` (field 1) only.
            let dataset = commit_overlay(
                dataset,
                "valov",
                0,
                &[1],
                OverlayCoverage::dense(bitmap([0, 1])),
                vec![i32_array([Some(1000), Some(1010)])],
                version,
            )
            .await;

            // Delete the overlay's data file: opening it now fails.
            dataset
                .object_store
                .delete(&Path::from("data/valov.lance"))
                .await
                .unwrap();

            let frag = dataset.get_fragment(0).unwrap();
            let id_only = dataset.schema().project_by_ids(&[0], true);
            let val_only = dataset.schema().project_by_ids(&[1], true);

            // Projecting only `id` must not open the `val` overlay file, so it
            // succeeds and returns untouched base values.
            let batch = frag.take(&[0, 1], &id_only).await.unwrap();
            assert_eq!(col(&batch, "id").values(), &[0, 1]);
            // A scan projecting only `id` must likewise never touch the file.
            let batch = frag
                .scan()
                .project(&["id"])
                .unwrap()
                .try_into_batch()
                .await
                .unwrap();
            assert_eq!(col(&batch, "id").values(), &[0, 1, 2, 3, 4, 5]);

            // Projecting the overlaid `val` column does need the file, so it fails
            // with a not-found error naming the missing overlay file.
            let err = frag.take(&[0], &val_only).await.unwrap_err();
            let message = format!("{err:?}");
            assert!(
                err.is_not_found() && message.contains("valov.lance"),
                "projecting the overlaid column should fail with a not-found error \
                 for its missing file, got: {message}",
            );
        }

        /// A top-level struct column resolves through overlays: the overlay stores
        /// the struct's leaf columns (under V2_1 those are the only ids in
        /// `data_file.fields`), and `plan_overlays` maps them back to the top-level
        /// struct so the whole value is fetched and replaced as a unit.
        #[rstest]
        #[tokio::test]
        async fn test_struct_overlay_end_to_end(
            #[values(LanceFileVersion::V2_0, LanceFileVersion::V2_1)] version: LanceFileVersion,
        ) {
            let struct_fields = Fields::from(vec![
                ArrowField::new("x", DataType::Int32, true),
                ArrowField::new("y", DataType::Int32, true),
            ]);
            let schema = Arc::new(ArrowSchema::new(vec![
                ArrowField::new("id", DataType::Int32, true),
                ArrowField::new("info", DataType::Struct(struct_fields.clone()), true),
            ]));
            let info = Arc::new(StructArray::new(
                struct_fields.clone(),
                vec![
                    Arc::new(Int32Array::from_iter_values(0..6)),
                    Arc::new(Int32Array::from_iter_values((0..6).map(|v| v * 100))),
                ],
                None,
            ));
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int32Array::from_iter_values(0..6)), info],
            )
            .unwrap();
            let write_params = WriteParams {
                max_rows_per_file: 6,
                max_rows_per_group: 6,
                data_storage_version: Some(version),
                ..Default::default()
            };
            let reader = RecordBatchIterator::new(vec![Ok(batch)], schema.clone());
            let dataset = Dataset::write(reader, "memory://", Some(write_params))
                .await
                .unwrap();

            // Overlay the whole `info` struct (top-level field id 1) at offset 2.
            let overlay_info = Arc::new(StructArray::new(
                struct_fields,
                vec![
                    Arc::new(Int32Array::from(vec![777])),
                    Arc::new(Int32Array::from(vec![888])),
                ],
                None,
            )) as ArrayRef;
            let dataset = commit_overlay(
                dataset,
                "structov",
                0,
                &[1],
                OverlayCoverage::dense(bitmap([2])),
                vec![overlay_info],
                version,
            )
            .await;

            let frag = dataset.get_fragment(0).unwrap();
            let batch = frag.take(&[1, 2], &full_schema(&dataset)).await.unwrap();
            let info = batch
                .column(batch.schema().index_of("info").unwrap())
                .as_any()
                .downcast_ref::<StructArray>()
                .unwrap();
            let x = info
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            let y = info
                .column(1)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            // Offset 1 falls through to base {1, 100}; offset 2 takes the overlay.
            assert_eq!(x.values(), &[1, 777]);
            assert_eq!(y.values(), &[100, 888]);
        }

        /// A top-level list column resolves through overlays the same way — the
        /// overlay's leaf (item) id maps back to the top-level list, and the whole
        /// list value at a covered offset is replaced.
        #[rstest]
        #[tokio::test]
        async fn test_list_overlay_end_to_end(
            #[values(LanceFileVersion::V2_0, LanceFileVersion::V2_1)] version: LanceFileVersion,
        ) {
            use arrow_array::ListArray;
            use arrow_array::types::Int32Type;

            let item = Arc::new(ArrowField::new("item", DataType::Int32, true));
            let schema = Arc::new(ArrowSchema::new(vec![
                ArrowField::new("id", DataType::Int32, true),
                ArrowField::new("tags", DataType::List(item.clone()), true),
            ]));
            let base_tags = ListArray::from_iter_primitive::<Int32Type, _, _>(
                (0..6i32).map(|i| Some(vec![Some(i), Some(i * 10)])),
            );
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from_iter_values(0..6)),
                    Arc::new(base_tags),
                ],
            )
            .unwrap();
            let write_params = WriteParams {
                max_rows_per_file: 6,
                max_rows_per_group: 6,
                data_storage_version: Some(version),
                ..Default::default()
            };
            let reader = RecordBatchIterator::new(vec![Ok(batch)], schema.clone());
            let dataset = Dataset::write(reader, "memory://", Some(write_params))
                .await
                .unwrap();

            // Overlay `tags` (top-level field id 1) at offset 2 with a new list.
            let overlay_tags =
                ListArray::from_iter_primitive::<Int32Type, _, _>(std::iter::once(Some(vec![
                    Some(77),
                    Some(88),
                    Some(99),
                ])));
            let dataset = commit_overlay(
                dataset,
                "listov",
                0,
                &[1],
                OverlayCoverage::dense(bitmap([2])),
                vec![Arc::new(overlay_tags) as ArrayRef],
                version,
            )
            .await;

            let frag = dataset.get_fragment(0).unwrap();
            let batch = frag.take(&[1, 2], &full_schema(&dataset)).await.unwrap();
            let tags = batch
                .column(batch.schema().index_of("tags").unwrap())
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap();
            let row1 = tags.value(0);
            let row1 = row1.as_any().downcast_ref::<Int32Array>().unwrap();
            let row2 = tags.value(1);
            let row2 = row2.as_any().downcast_ref::<Int32Array>().unwrap();
            // Offset 1 falls through to base [1, 10]; offset 2 takes the overlay.
            assert_eq!(row1.values(), &[1, 10]);
            assert_eq!(row2.values(), &[77, 88, 99]);
        }

        /// A top-level Map column resolves as a single atomic field even though its
        /// value spans two leaves (key and value): both leaf ids map back to the one
        /// Map atomic field, and the whole map value at a covered offset is replaced.
        /// Maps require
        /// the 2.2+ file format, so this runs only at V2_2 (unlike the V2_0/V2_1
        /// parametrized tests).
        #[tokio::test]
        async fn test_map_overlay_end_to_end() {
            use arrow_array::MapArray;
            use arrow_array::builder::{Int32Builder, MapBuilder};

            let version = LanceFileVersion::V2_2;

            // Base row i holds the single entry {i: i * 10}.
            let mut builder = MapBuilder::new(None, Int32Builder::new(), Int32Builder::new());
            for i in 0..6i32 {
                builder.keys().append_value(i);
                builder.values().append_value(i * 10);
                builder.append(true).unwrap();
            }
            let base_attrs = builder.finish();
            let schema = Arc::new(ArrowSchema::new(vec![
                ArrowField::new("id", DataType::Int32, true),
                ArrowField::new("attrs", base_attrs.data_type().clone(), true),
            ]));
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from_iter_values(0..6)),
                    Arc::new(base_attrs),
                ],
            )
            .unwrap();
            let write_params = WriteParams {
                max_rows_per_file: 6,
                max_rows_per_group: 6,
                data_storage_version: Some(version),
                ..Default::default()
            };
            let reader = RecordBatchIterator::new(vec![Ok(batch)], schema.clone());
            let dataset = Dataset::write(reader, "memory://", Some(write_params))
                .await
                .unwrap();

            // Overlay `attrs` (top-level field id 1) at offset 2 with a two-entry map.
            let mut ov = MapBuilder::new(None, Int32Builder::new(), Int32Builder::new());
            ov.keys().append_value(7);
            ov.values().append_value(77);
            ov.keys().append_value(8);
            ov.values().append_value(88);
            ov.append(true).unwrap();
            let overlay_attrs = ov.finish();
            let dataset = commit_overlay(
                dataset,
                "mapov",
                0,
                &[1],
                OverlayCoverage::dense(bitmap([2])),
                vec![Arc::new(overlay_attrs) as ArrayRef],
                version,
            )
            .await;

            let frag = dataset.get_fragment(0).unwrap();
            let batch = frag.take(&[1, 2], &full_schema(&dataset)).await.unwrap();
            let attrs = batch
                .column(batch.schema().index_of("attrs").unwrap())
                .as_any()
                .downcast_ref::<MapArray>()
                .unwrap();

            let entries = |i: usize| -> (Vec<i32>, Vec<i32>) {
                let row = attrs.value(i);
                let keys = row.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
                let vals = row.column(1).as_any().downcast_ref::<Int32Array>().unwrap();
                (keys.values().to_vec(), vals.values().to_vec())
            };
            // Offset 1 falls through to the base entry {1: 10}; offset 2 takes the
            // overlay map {7: 77, 8: 88}.
            assert_eq!(entries(0), (vec![1], vec![10]));
            assert_eq!(entries(1), (vec![7, 8], vec![77, 88]));
        }

        /// Base `id` + a struct `s { a, b }` (6 rows). Field ids: s=1, a=2, b=3.
        async fn create_struct_dataset(version: LanceFileVersion) -> (Dataset, Fields) {
            let s_fields = Fields::from(vec![
                ArrowField::new("a", DataType::Int32, true),
                ArrowField::new("b", DataType::Int32, true),
            ]);
            let schema = Arc::new(ArrowSchema::new(vec![
                ArrowField::new("id", DataType::Int32, true),
                ArrowField::new("s", DataType::Struct(s_fields.clone()), true),
            ]));
            let s = Arc::new(StructArray::new(
                s_fields.clone(),
                vec![
                    Arc::new(Int32Array::from_iter_values(0..6)),
                    Arc::new(Int32Array::from_iter_values((0..6).map(|v| v * 100))),
                ],
                None,
            ));
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int32Array::from_iter_values(0..6)), s],
            )
            .unwrap();
            let write_params = WriteParams {
                max_rows_per_file: 6,
                max_rows_per_group: 6,
                data_storage_version: Some(version),
                ..Default::default()
            };
            let reader = RecordBatchIterator::new(vec![Ok(batch)], schema.clone());
            let dataset = Dataset::write(reader, "memory://", Some(write_params))
                .await
                .unwrap();
            (dataset, s_fields)
        }

        fn struct_col<'a>(batch: &'a RecordBatch, name: &str) -> &'a StructArray {
            batch
                .column(batch.schema().index_of(name).unwrap())
                .as_any()
                .downcast_ref::<StructArray>()
                .unwrap()
        }

        fn i32_child(s: &StructArray, i: usize) -> Int32Array {
            s.column(i)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .clone()
        }

        /// The reviewer's core case (r3553495147): an overlay stores only sub-field
        /// `s.a`, but the read projects the whole struct `s`. The overlay must splice
        /// into `a` and leave `b` untouched (previously this panicked because the merge
        /// fetched the whole `s` from an overlay file holding only `a`).
        #[rstest]
        #[tokio::test]
        async fn test_overlay_subfield_projecting_parent_struct(
            #[values(LanceFileVersion::V2_0, LanceFileVersion::V2_1)] version: LanceFileVersion,
        ) {
            let (dataset, _) = create_struct_dataset(version).await;
            // Overlay ONLY `s.a` (field id 2) at offset 2.
            let a_only = Fields::from(vec![ArrowField::new("a", DataType::Int32, true)]);
            let overlay = Arc::new(StructArray::new(
                a_only,
                vec![Arc::new(Int32Array::from(vec![777]))],
                None,
            )) as ArrayRef;
            let dataset = commit_overlay(
                dataset,
                "aov",
                0,
                &[2],
                OverlayCoverage::dense(bitmap([2])),
                vec![overlay],
                version,
            )
            .await;

            let frag = dataset.get_fragment(0).unwrap();
            let batch = frag.take(&[1, 2], &full_schema(&dataset)).await.unwrap();
            let s = struct_col(&batch, "s");
            // a: offset 1 base (1), offset 2 overlaid (777).
            assert_eq!(i32_child(s, 0).values(), &[1, 777]);
            // b: untouched base (100, 200).
            assert_eq!(i32_child(s, 1).values(), &[100, 200]);
        }

        /// An overlay on a non-projected sibling leaf must be skipped and its file
        /// never opened: overlay covers `s.b`, but the read projects only `s.a`.
        #[rstest]
        #[tokio::test]
        async fn test_overlay_nonprojected_sibling_skipped(
            #[values(LanceFileVersion::V2_0, LanceFileVersion::V2_1)] version: LanceFileVersion,
        ) {
            let (dataset, _) = create_struct_dataset(version).await;
            let b_only = Fields::from(vec![ArrowField::new("b", DataType::Int32, true)]);
            let overlay = Arc::new(StructArray::new(
                b_only,
                vec![Arc::new(Int32Array::from(vec![888]))],
                None,
            )) as ArrayRef;
            let dataset = commit_overlay(
                dataset,
                "bov",
                0,
                &[3],
                OverlayCoverage::dense(bitmap([2])),
                vec![overlay],
                version,
            )
            .await;
            // Delete the overlay file: if projecting only `s.a` opened it, this fails.
            dataset
                .object_store
                .delete(&Path::from("data/bov.lance"))
                .await
                .unwrap();

            let frag = dataset.get_fragment(0).unwrap();
            let a_only = dataset.schema().project_by_ids(&[2], true);
            let batch = frag.take(&[1, 2], &a_only).await.unwrap();
            let s = struct_col(&batch, "s");
            // Only `a` is projected, unchanged base values.
            assert_eq!(i32_child(s, 0).values(), &[1, 2]);
        }

        /// Two overlays target different sub-fields of the same struct, and a third
        /// re-overlays `s.a`. Each leaf resolves independently and newest wins on `a`.
        #[rstest]
        #[tokio::test]
        async fn test_overlay_multiple_subfields_newest_wins(
            #[values(LanceFileVersion::V2_0, LanceFileVersion::V2_1)] version: LanceFileVersion,
        ) {
            let (dataset, _) = create_struct_dataset(version).await;
            let a_field = Fields::from(vec![ArrowField::new("a", DataType::Int32, true)]);
            let b_field = Fields::from(vec![ArrowField::new("b", DataType::Int32, true)]);
            // Older: a := 700 at offset 2.
            let dataset = commit_overlay(
                dataset,
                "a_old",
                0,
                &[2],
                OverlayCoverage::dense(bitmap([2])),
                vec![Arc::new(StructArray::new(
                    a_field.clone(),
                    vec![Arc::new(Int32Array::from(vec![700]))],
                    None,
                )) as ArrayRef],
                version,
            )
            .await;
            // b := 800 at offset 2.
            let dataset = commit_overlay(
                dataset,
                "b_ov",
                0,
                &[3],
                OverlayCoverage::dense(bitmap([2])),
                vec![Arc::new(StructArray::new(
                    b_field,
                    vec![Arc::new(Int32Array::from(vec![800]))],
                    None,
                )) as ArrayRef],
                version,
            )
            .await;
            // Newest: a := 999 at offset 2 (shadows the older `a` overlay).
            let dataset = commit_overlay(
                dataset,
                "a_new",
                0,
                &[2],
                OverlayCoverage::dense(bitmap([2])),
                vec![Arc::new(StructArray::new(
                    a_field,
                    vec![Arc::new(Int32Array::from(vec![999]))],
                    None,
                )) as ArrayRef],
                version,
            )
            .await;

            let frag = dataset.get_fragment(0).unwrap();
            let batch = frag.take(&[2], &full_schema(&dataset)).await.unwrap();
            let s = struct_col(&batch, "s");
            assert_eq!(i32_child(s, 0).values(), &[999]); // newest `a` wins
            assert_eq!(i32_child(s, 1).values(), &[800]); // `b` from its own overlay
        }

        /// Three levels of nesting: `outer { middle { a, b } }`. An overlay on the
        /// deep leaf `outer.middle.a` splices correctly when the whole `outer` is read.
        #[rstest]
        #[tokio::test]
        async fn test_overlay_deeply_nested_subfield(
            #[values(LanceFileVersion::V2_0, LanceFileVersion::V2_1)] version: LanceFileVersion,
        ) {
            let mid_fields = Fields::from(vec![
                ArrowField::new("a", DataType::Int32, true),
                ArrowField::new("b", DataType::Int32, true),
            ]);
            let outer_fields = Fields::from(vec![ArrowField::new(
                "middle",
                DataType::Struct(mid_fields.clone()),
                true,
            )]);
            let schema = Arc::new(ArrowSchema::new(vec![
                ArrowField::new("id", DataType::Int32, true),
                ArrowField::new("outer", DataType::Struct(outer_fields.clone()), true),
            ]));
            // Field ids: outer=1, middle=2, a=3, b=4.
            let middle = Arc::new(StructArray::new(
                mid_fields.clone(),
                vec![
                    Arc::new(Int32Array::from_iter_values(0..6)),
                    Arc::new(Int32Array::from_iter_values((0..6).map(|v| v * 100))),
                ],
                None,
            ));
            let outer = Arc::new(StructArray::new(outer_fields, vec![middle], None));
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int32Array::from_iter_values(0..6)), outer],
            )
            .unwrap();
            let write_params = WriteParams {
                max_rows_per_file: 6,
                max_rows_per_group: 6,
                data_storage_version: Some(version),
                ..Default::default()
            };
            let reader = RecordBatchIterator::new(vec![Ok(batch)], schema.clone());
            let dataset = Dataset::write(reader, "memory://", Some(write_params))
                .await
                .unwrap();

            // Overlay the deep leaf `outer.middle.a` (field id 3) at offset 2.
            let a_leaf = Fields::from(vec![ArrowField::new("a", DataType::Int32, true)]);
            let mid_a = Fields::from(vec![ArrowField::new(
                "middle",
                DataType::Struct(a_leaf.clone()),
                true,
            )]);
            let overlay = Arc::new(StructArray::new(
                mid_a,
                vec![Arc::new(StructArray::new(
                    a_leaf,
                    vec![Arc::new(Int32Array::from(vec![777]))],
                    None,
                ))],
                None,
            )) as ArrayRef;
            let dataset = commit_overlay(
                dataset,
                "deepov",
                0,
                &[3],
                OverlayCoverage::dense(bitmap([2])),
                vec![overlay],
                version,
            )
            .await;

            let frag = dataset.get_fragment(0).unwrap();
            let batch = frag.take(&[1, 2], &full_schema(&dataset)).await.unwrap();
            let outer = struct_col(&batch, "outer");
            let middle = outer
                .column(0)
                .as_any()
                .downcast_ref::<StructArray>()
                .unwrap();
            // a: offset 1 base (1), offset 2 overlaid (777); b untouched.
            assert_eq!(i32_child(middle, 0).values(), &[1, 777]);
            assert_eq!(i32_child(middle, 1).values(), &[100, 200]);

            // Projecting the *intermediate* struct `outer.middle` (field id 2) while
            // the overlay targets a deeper field (id 3) must still apply: the
            // overlay's leaf id falls inside the projected subtree, so it maps to a
            // projected atomic field. (This is the case wjones127/westonpace flagged where a
            // top-level-only mapping would miss the overlay.)
            let middle_only = dataset.schema().project_by_ids(&[2], true);
            let batch = frag.take(&[2], &middle_only).await.unwrap();
            let middle = struct_col(&batch, "outer")
                .column(0)
                .as_any()
                .downcast_ref::<StructArray>()
                .unwrap();
            assert_eq!(i32_child(middle, 0).values(), &[777]);
        }
    }

    #[rstest]
    #[tokio::test]
    async fn test_fragment_scan(
        #[values(LanceFileVersion::Legacy, LanceFileVersion::Stable)]
        data_storage_version: LanceFileVersion,
    ) {
        let test_dir = TempStrDir::default();
        let test_uri = &test_dir;
        let dataset = create_dataset(test_uri, data_storage_version).await;
        let fragment = &dataset.get_fragments()[2];
        let mut scanner = fragment.scan();
        let batches = scanner
            .with_row_id()
            .filter(" i < 105")
            .unwrap()
            .try_into_stream()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        if data_storage_version == LanceFileVersion::Legacy {
            assert_eq!(batches.len(), 3);

            assert_eq!(
                batches[0].column_by_name("i").unwrap().as_ref(),
                &Int32Array::from_iter_values(80..90)
            );
            assert_eq!(
                batches[1].column_by_name("i").unwrap().as_ref(),
                &Int32Array::from_iter_values(90..100)
            );
            assert_eq!(
                batches[2].column_by_name("i").unwrap().as_ref(),
                &Int32Array::from_iter_values(100..105)
            );
        } else {
            assert_eq!(batches.len(), 1);

            assert_eq!(
                batches[0].column_by_name("i").unwrap().as_ref(),
                &Int32Array::from_iter_values(80..105)
            )
        }
    }

    #[tokio::test]
    async fn test_fragment_scan_v2() {
        let test_dir = TempStrDir::default();
        let test_uri = &test_dir;
        let dataset = create_dataset_v2(test_uri).await;
        let fragment = &dataset.get_fragments()[2];
        let mut scanner = fragment.scan();
        let batches = scanner
            .with_row_id()
            .try_into_stream()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert_eq!(batches.len(), 1);

        assert_eq!(
            batches[0].column_by_name("i").unwrap().as_ref(),
            &Int32Array::from_iter_values(80..120)
        );

        let mut scanner = fragment.scan();
        let batches = scanner
            .with_row_id()
            .batch_size(20)
            .try_into_stream()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert_eq!(batches.len(), 2);

        assert_eq!(
            batches[0].column_by_name("i").unwrap().as_ref(),
            &Int32Array::from_iter_values(80..100)
        );
        assert_eq!(
            batches[1].column_by_name("i").unwrap().as_ref(),
            &Int32Array::from_iter_values(100..120)
        );
    }

    #[tokio::test]
    async fn test_fragment_update() {
        let test_dir = TempStrDir::default();
        let test_uri = &test_dir;
        let mut dataset = create_dataset_v2(test_uri).await;

        // Test update with _rowid
        let _ = dataset
            .add_columns(
                NewColumnTransform::SqlExpressions(vec![("col1".into(), "-1".into())]),
                None,
                None,
            )
            .await;
        let mut fragment1 = dataset.get_fragment(0).unwrap();

        let schema1 = Arc::new(ArrowSchema::new(vec![
            ArrowField::new(ROW_ID, DataType::UInt64, false),
            ArrowField::new("col1", DataType::Int64, true),
        ]));
        let update_batch1 = RecordBatch::try_new(
            schema1.clone(),
            vec![
                Arc::new(UInt64Array::from(
                    (0..40).filter(|&v| v != 0 && v != 3).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(vec![2; 38])),
            ],
        )
        .unwrap();
        let right_stream1: Box<dyn RecordBatchReader + Send> = Box::new(RecordBatchIterator::new(
            vec![Ok(update_batch1)].into_iter(),
            schema1,
        ));
        let u1 = fragment1
            .update_columns_with_offsets(right_stream1, ROW_ID, ROW_ID)
            .await
            .unwrap();
        assert_eq!(u1.matched_offsets.iter().count(), 38);
        assert!(!u1.matched_offsets.contains(0));
        assert!(!u1.matched_offsets.contains(3));
        assert!(u1.matched_offsets.contains(1));
        assert!(u1.matched_offsets.contains(39));
        let frag_id_1 = u1.fragment.id;
        let matched_1 = u1.matched_offsets;
        let op1 = Operation::Update {
            removed_fragment_ids: vec![],
            updated_fragments: vec![u1.fragment],
            new_fragments: vec![],
            fields_modified: u1.fields_modified,
            compacted_sstables: Vec::new(),
            fields_for_preserving_frag_bitmap: vec![],
            update_mode: Some(UpdateMode::RewriteColumns),
            inserted_rows_filter: None,
            updated_fragment_offsets: Some(UpdatedFragmentOffsets(HashMap::from([(
                frag_id_1, matched_1,
            )]))),
        };
        let mut dataset1 = Dataset::commit(
            test_uri,
            op1,
            Some(dataset.version().version),
            None,
            None,
            Default::default(),
            true,
        )
        .await
        .unwrap();
        assert_eq!(dataset1.get_fragments().len(), 5);
        let scanner1 = dataset1.get_fragment(0).unwrap().scan();
        let batches1 = scanner1
            .try_into_stream()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(batches1.len(), 1);
        let mut expected_col1 = vec![2; 40];
        expected_col1[0] = -1;
        expected_col1[3] = -1;
        assert_eq!(
            batches1[0].column_by_name("col1").unwrap().as_ref(),
            &Int64Array::from(expected_col1)
        );

        // Test update with user specified keys
        let _ = dataset1
            .add_columns(
                NewColumnTransform::SqlExpressions(vec![("col2".into(), "false".into())]),
                None,
                None,
            )
            .await;
        let mut fragment2 = dataset1.get_fragment(0).unwrap();

        let schema2 = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("i1", DataType::Int32, true),
            ArrowField::new("col2", DataType::Boolean, true),
            ArrowField::new("col1", DataType::Int64, true),
        ]));
        let update_batch2 = RecordBatch::try_new(
            schema2.clone(),
            vec![
                Arc::new(Int32Array::from(
                    (0..40).filter(|&v| v != 0 && v != 3).collect::<Vec<_>>(),
                )),
                Arc::new(BooleanArray::from(vec![true; 38])),
                Arc::new(Int64Array::from(vec![3; 38])),
            ],
        )
        .unwrap();
        let right_stream2: Box<dyn RecordBatchReader + Send> = Box::new(RecordBatchIterator::new(
            vec![Ok(update_batch2)].into_iter(),
            schema2,
        ));
        let u2 = fragment2
            .update_columns_with_offsets(right_stream2, "i", "i1")
            .await
            .unwrap();
        assert_eq!(u2.matched_offsets.iter().count(), 38);
        assert!(!u2.matched_offsets.contains(0));
        assert!(!u2.matched_offsets.contains(3));
        let frag_id_2 = u2.fragment.id;
        let matched_2 = u2.matched_offsets;
        let op = Operation::Update {
            removed_fragment_ids: vec![],
            updated_fragments: vec![u2.fragment],
            new_fragments: vec![],
            fields_modified: u2.fields_modified,
            compacted_sstables: Vec::new(),
            fields_for_preserving_frag_bitmap: vec![],
            update_mode: Some(UpdateMode::RewriteColumns),
            inserted_rows_filter: None,
            updated_fragment_offsets: Some(UpdatedFragmentOffsets(HashMap::from([(
                frag_id_2, matched_2,
            )]))),
        };
        let dataset2 = Dataset::commit(
            test_uri,
            op,
            Some(dataset1.version().version),
            None,
            None,
            Default::default(),
            true,
        )
        .await
        .unwrap();
        assert_eq!(dataset2.get_fragments().len(), 5);
        let scanner2 = dataset2.get_fragment(0).unwrap().scan();
        let batches2 = scanner2
            .try_into_stream()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(batches2.len(), 1);

        expected_col1 = vec![3; 40];
        expected_col1[0] = -1;
        expected_col1[3] = -1;
        assert_eq!(
            batches2[0].column_by_name("col1").unwrap().as_ref(),
            &Int64Array::from(expected_col1)
        );
        let mut expected_col2 = vec![true; 40];
        expected_col2[0] = false;
        expected_col2[3] = false;
        assert_eq!(
            batches2[0].column_by_name("col2").unwrap().as_ref(),
            &BooleanArray::from(expected_col2)
        );
    }

    #[tokio::test]
    async fn test_out_of_range() {
        let test_dir = TempStrDir::default();
        let test_uri = &test_dir;
        // Creates 400 rows in 10 fragments
        let mut dataset = create_dataset(test_uri, LanceFileVersion::Legacy).await;
        // Delete last 20 rows in first fragment
        dataset.delete("i >= 20").await.unwrap();
        // Last fragment has 20 rows but 40 addressable rows
        let fragment = &dataset.get_fragments()[0];
        assert_eq!(fragment.metadata.num_rows().unwrap(), 20);

        // Test with take_range (all rows addressable)
        for with_row_id in [false, true] {
            let reader = fragment
                .open(
                    fragment.schema(),
                    FragReadConfig::default().with_row_id(with_row_id),
                )
                .await
                .unwrap();
            for valid_range in [0..40, 20..40] {
                reader
                    .take_range(valid_range, 100)
                    .await
                    .unwrap()
                    .buffered(1)
                    .try_collect::<Vec<_>>()
                    .await
                    .unwrap();
            }
            for invalid_range in [0..41, 41..42] {
                assert!(reader.take_range(invalid_range, 100).await.is_err());
            }
        }

        // Test with read_range (only non-deleted rows addressable)
        for with_row_id in [false, true] {
            let reader = fragment
                .open(
                    fragment.schema(),
                    FragReadConfig::default().with_row_id(with_row_id),
                )
                .await
                .unwrap();
            for valid_range in [0..20, 0..10, 10..20] {
                reader
                    .read_range(valid_range, 100)
                    .await
                    .unwrap()
                    .buffered(1)
                    .try_collect::<Vec<_>>()
                    .await
                    .unwrap();
            }
            for invalid_range in [0..21, 21..22] {
                assert!(reader.read_range(invalid_range, 100).await.is_err());
            }
        }
    }

    #[tokio::test]
    async fn test_rowid_rowaddr_only() {
        let test_dir = TempStrDir::default();
        let test_uri = &test_dir;
        // Creates 400 rows in 10 fragments
        let mut dataset = create_dataset(test_uri, LanceFileVersion::Legacy).await;
        // Delete last 20 rows in first fragment
        dataset.delete("i >= 20").await.unwrap();
        // Last fragment has 20 rows but 40 addressable rows
        let fragment = &dataset.get_fragments()[0];
        assert_eq!(fragment.metadata.num_rows().unwrap(), 20);

        // Test with take_range (all rows addressable)
        for (with_row_id, with_row_address) in [(false, true), (true, false), (true, true)] {
            let reader = fragment
                .open(
                    &fragment.schema().project::<&str>(&[]).unwrap(),
                    FragReadConfig::default()
                        .with_row_id(with_row_id)
                        .with_row_address(with_row_address),
                )
                .await
                .unwrap();
            for valid_range in [0..40, 20..40] {
                reader
                    .take_range(valid_range, 100)
                    .await
                    .unwrap()
                    .buffered(1)
                    .try_collect::<Vec<_>>()
                    .await
                    .unwrap();
            }
            for invalid_range in [0..41, 41..42] {
                assert!(reader.take_range(invalid_range, 100).await.is_err());
            }
        }

        // Test with read_range (only non-deleted rows addressable)
        for (with_row_id, with_row_address) in [(false, true), (true, false), (true, true)] {
            let reader = fragment
                .open(
                    &fragment.schema().project::<&str>(&[]).unwrap(),
                    FragReadConfig::default()
                        .with_row_id(with_row_id)
                        .with_row_address(with_row_address),
                )
                .await
                .unwrap();
            for valid_range in [0..20, 0..10, 10..20] {
                reader
                    .read_range(valid_range, 100)
                    .await
                    .unwrap()
                    .buffered(1)
                    .try_collect::<Vec<_>>()
                    .await
                    .unwrap();
            }
            for invalid_range in [0..21, 21..22] {
                assert!(reader.read_range(invalid_range, 100).await.is_err());
            }
        }
    }

    #[rstest]
    #[tokio::test]
    async fn test_fragment_take_range_deletions(
        #[values(LanceFileVersion::Legacy, LanceFileVersion::Stable)]
        data_storage_version: LanceFileVersion,
    ) {
        let test_dir = TempStrDir::default();
        let test_uri = &test_dir;
        let mut dataset = create_dataset(test_uri, data_storage_version).await;
        dataset.delete("i >= 0 and i < 15").await.unwrap();

        let fragment = &dataset.get_fragments()[0];
        let mut reader = fragment
            .open(
                dataset.schema(),
                FragReadConfig::default().with_row_id(true),
            )
            .await
            .unwrap();
        reader.with_make_deletions_null();

        if data_storage_version == LanceFileVersion::Legacy {
            // The first batch is entirely deleted, deleted rows will be marked null with null row ids.
            let batch1 = reader
                .legacy_read_batch_projected(0, .., dataset.schema())
                .await
                .unwrap();
            assert_eq!(
                batch1.column_by_name(ROW_ID).unwrap().as_ref(),
                &UInt64Array::from_iter(std::iter::repeat_n(None, 10))
            );

            // The second batch is partially deleted, so the deleted rows will be
            // marked null with null row ids.
            let batch2 = reader
                .legacy_read_batch_projected(1, .., dataset.schema())
                .await
                .unwrap();
            assert_eq!(
                batch2.column_by_name(ROW_ID).unwrap().as_ref(),
                &UInt64Array::from_iter((10..20).map(|v| if v < 15 { None } else { Some(v) }))
            );

            // The final batch is not deleted, so it will be returned as-is.
            let batch3 = reader
                .legacy_read_batch_projected(2, .., dataset.schema())
                .await
                .unwrap();
            assert_eq!(
                batch3.column_by_name(ROW_ID).unwrap().as_ref(),
                &UInt64Array::from_iter_values(20..30)
            );
        } else {
            let to_batches = |range: Range<u32>| {
                let batch_size = range.len() as u32;
                let fut = reader.take_range(range, batch_size);
                async move { fut.await.unwrap().buffered(1).try_collect::<Vec<_>>().await }
            };

            // Since the first batch is all deleted, it will return all nulls row ids.
            let batches = to_batches(0..10).await.unwrap();
            assert_eq!(batches.len(), 1);
            let batch = batches.into_iter().next().unwrap();
            assert_eq!(
                batch.column_by_name(ROW_ID).unwrap().as_ref(),
                &UInt64Array::from_iter(std::iter::repeat_n(None, 10))
            );

            let batches = to_batches(10..20).await.unwrap();
            assert_eq!(batches.len(), 1);
            let batch = batches.into_iter().next().unwrap();
            // The second batch is partially deleted, so the deleted rows will be
            // marked null with null row ids.
            assert_eq!(
                batch.column_by_name(ROW_ID).unwrap().as_ref(),
                &UInt64Array::from_iter((10..20).map(|v| if v < 15 { None } else { Some(v) }))
            );

            // The final batch is not deleted, so it will be returned as-is.
            let batches = to_batches(20..30).await.unwrap();
            assert_eq!(batches.len(), 1);
            let batch = batches.into_iter().next().unwrap();
            assert_eq!(
                batch.column_by_name(ROW_ID).unwrap().as_ref(),
                &UInt64Array::from_iter_values(20..30)
            );
        }
    }

    #[rstest]
    #[tokio::test]
    async fn test_range_scan_deletions(
        #[values(LanceFileVersion::Legacy, LanceFileVersion::Stable)]
        data_storage_version: LanceFileVersion,
    ) {
        let test_dir = TempStrDir::default();
        let test_uri = &test_dir;
        let dataset = create_dataset(test_uri, data_storage_version).await;

        let version = dataset.version().version;

        let check = |cond: &'static str, range: Range<u32>, expected: Vec<i32>| async {
            let mut dataset = dataset.checkout_version(version).await.unwrap();
            dataset.restore().await.unwrap();
            dataset.delete(cond).await.unwrap();

            let fragment = &dataset.get_fragments()[0];
            let reader = fragment
                .open(
                    dataset.schema(),
                    FragReadConfig::default().with_row_id(true),
                )
                .await
                .unwrap();

            // Using batch_size=20 here.  If we use batch_size=range.len() we get
            // multiple batches because we might have to read from a larger range
            // to satisfy the request
            let mut stream = reader.read_range(range, 20).await.unwrap();
            let mut batches = Vec::new();
            while let Some(next) = stream.next().await {
                batches.push(next.await.unwrap());
            }
            let schema = Arc::new(dataset.schema().into());
            let batch = arrow_select::concat::concat_batches(&schema, batches.iter()).unwrap();

            assert_eq!(batch.num_rows(), expected.len());
            assert_eq!(
                batch.column_by_name("i").unwrap().as_ref(),
                &Int32Array::from(expected)
            );
        };
        // Deleting from the start
        check("i < 5", 0..2, vec![5, 6]).await;
        check("i < 5", 0..15, (5..20).collect()).await;
        // Deleting from the middle
        check("i >= 5 and i < 15", 7..9, vec![17, 18]).await;
        check("i >= 5 and i < 15", 3..5, vec![3, 4]).await;
        check("i >= 5 and i < 15", 3..6, vec![3, 4, 15]).await;
        check("i >= 5 and i < 15", 5..6, vec![15]).await;
        check("i >= 5 and i < 15", 5..10, vec![15, 16, 17, 18, 19]).await;
        check(
            "i >= 5 and i < 15",
            0..10,
            vec![0, 1, 2, 3, 4, 15, 16, 17, 18, 19],
        )
        .await;
        // Deleting from the end
        check("i >= 15", 10..15, vec![10, 11, 12, 13, 14]).await;
        check("i >= 15", 0..15, (0..15).collect()).await;
    }

    #[rstest]
    #[tokio::test]
    async fn test_fragment_take_indices(
        #[values(LanceFileVersion::Legacy, LanceFileVersion::Stable)]
        data_storage_version: LanceFileVersion,
    ) {
        let test_dir = TempStrDir::default();
        let test_uri = &test_dir;
        let mut dataset = create_dataset(test_uri, data_storage_version).await;
        let fragment = dataset
            .get_fragments()
            .into_iter()
            .find(|f| f.id() == 3)
            .unwrap();

        // Repeated indices are repeated in result.
        let batch = fragment
            .take(&[1, 2, 4, 5, 5, 8], dataset.schema())
            .await
            .unwrap();
        assert_eq!(
            batch.column_by_name("i").unwrap().as_ref(),
            &Int32Array::from(vec![121, 122, 124, 125, 125, 128])
        );

        dataset.delete("i in (122, 123, 125)").await.unwrap();
        dataset.validate().await.unwrap();

        // Deleted rows are skipped
        let fragment = dataset
            .get_fragments()
            .into_iter()
            .find(|f| f.id() == 3)
            .unwrap();
        assert!(fragment.metadata().deletion_file.is_some());
        let batch = fragment
            .take(&[1, 2, 4, 5, 8], dataset.schema())
            .await
            .unwrap();
        assert_eq!(
            batch.column_by_name("i").unwrap().as_ref(),
            &Int32Array::from(vec![121, 124, 127, 128, 131])
        );

        // Empty indices gives empty result
        let batch = fragment.take(&[], dataset.schema()).await.unwrap();
        assert_eq!(
            batch.column_by_name("i").unwrap().as_ref(),
            &Int32Array::from(Vec::<i32>::new())
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_fragment_take_rows(
        #[values(LanceFileVersion::Legacy, LanceFileVersion::Stable)]
        data_storage_version: LanceFileVersion,
    ) {
        let test_dir = TempStrDir::default();
        let test_uri = &test_dir;
        let mut dataset = create_dataset(test_uri, data_storage_version).await;
        let fragment = dataset
            .get_fragments()
            .into_iter()
            .find(|f| f.id() == 3)
            .unwrap();

        // Repeated indices are repeated in result.
        let batch = fragment
            .take_rows(
                &[1, 2, 4, 5, 5, 8],
                dataset.schema(),
                false,
                false,
                false,
                false,
            )
            .await
            .unwrap();
        assert_eq!(
            batch.column_by_name("i").unwrap().as_ref(),
            &Int32Array::from(vec![121, 122, 124, 125, 125, 128])
        );

        dataset.delete("i in (122, 124)").await.unwrap();
        dataset.validate().await.unwrap();

        // Cannot get rows 2 and 4 anymore
        let fragment = dataset
            .get_fragments()
            .into_iter()
            .find(|f| f.id() == 3)
            .unwrap();
        assert!(fragment.metadata().deletion_file.is_some());
        let batch = fragment
            .take_rows(
                &[1, 2, 4, 5, 8],
                dataset.schema(),
                false,
                false,
                false,
                false,
            )
            .await
            .unwrap();
        assert_eq!(
            batch.column_by_name("i").unwrap().as_ref(),
            &Int32Array::from(vec![121, 125, 128])
        );

        // Empty indices gives empty result
        let batch = fragment
            .take_rows(&[], dataset.schema(), false, false, false, false)
            .await
            .unwrap();
        assert_eq!(
            batch.column_by_name("i").unwrap().as_ref(),
            &Int32Array::from(Vec::<i32>::new())
        );

        // Can get row ids
        let batch = fragment
            .take_rows(
                &[1, 2, 4, 5, 8],
                dataset.schema(),
                false,
                true,
                false,
                false,
            )
            .await
            .unwrap();
        assert_eq!(
            batch.column_by_name("i").unwrap().as_ref(),
            &Int32Array::from(vec![121, 125, 128])
        );
        assert_eq!(
            batch.column_by_name(ROW_ADDR).unwrap().as_ref(),
            &UInt64Array::from(vec![(3 << 32) + 1, (3 << 32) + 5, (3 << 32) + 8])
        );
    }

    #[tokio::test]
    async fn test_recommit_from_file() {
        let test_dir = TempStrDir::default();
        let test_uri = &test_dir;
        let dataset = create_dataset(test_uri, LanceFileVersion::Legacy).await;
        let schema = dataset.schema();
        let dataset_rows = dataset.count_rows(None).await.unwrap();

        let mut paths: Vec<String> = Vec::new();
        for f in dataset.get_fragments() {
            for file in Fragment::from(f.clone()).files {
                let p = file.path.clone();
                paths.push(p);
            }
        }

        let mut fragments: Vec<Fragment> = Vec::new();
        for (idx, path) in paths.iter().enumerate() {
            let f = FileFragment::create_from_file(path, &dataset, idx, None)
                .await
                .unwrap();
            fragments.push(f)
        }

        let op = Operation::Overwrite {
            schema: schema.clone(),
            fragments,
            config_upsert_values: None,
            initial_bases: None,
        };

        let new_dataset =
            Dataset::commit(test_uri, op, None, None, None, Default::default(), false)
                .await
                .unwrap();

        assert_eq!(new_dataset.count_rows(None).await.unwrap(), dataset_rows);

        // Fragments will have number of rows recorded in metadata, even though
        // we passed `None` when constructing the `FileFragment`.
        let fragments = new_dataset.get_fragments();
        assert_eq!(fragments.len(), 5);
        for f in fragments {
            assert_eq!(f.metadata.num_rows(), Some(40));
            assert_eq!(f.count_rows(None).await.unwrap(), 40);
            assert_eq!(f.metadata().deletion_file, None);
        }
    }

    #[rstest]
    #[tokio::test]
    async fn test_fragment_count(
        #[values(LanceFileVersion::Legacy, LanceFileVersion::Stable)]
        data_storage_version: LanceFileVersion,
    ) {
        let test_dir = TempStrDir::default();
        let test_uri = &test_dir;
        let dataset = create_dataset(test_uri, data_storage_version).await;
        let fragment = dataset.get_fragments().pop().unwrap();

        assert_eq!(fragment.count_rows(None).await.unwrap(), 40);
        assert_eq!(fragment.physical_rows().await.unwrap(), 40);
        assert!(fragment.metadata.deletion_file.is_none());

        assert_eq!(
            fragment
                .count_rows(Some("i < 170".to_string()))
                .await
                .unwrap(),
            10
        );

        let fragment = fragment
            .delete("i >= 160 and i <= 172")
            .await
            .unwrap()
            .unwrap();

        fragment.validate().await.unwrap();

        assert_eq!(fragment.count_rows(None).await.unwrap(), 27);
        assert_eq!(fragment.physical_rows().await.unwrap(), 40);
        assert!(fragment.metadata.deletion_file.is_some());
        assert_eq!(
            fragment.metadata.deletion_file.unwrap().num_deleted_rows,
            Some(13)
        );
    }

    #[rstest]
    #[tokio::test]
    async fn test_append_new_columns(
        #[values(LanceFileVersion::Legacy, LanceFileVersion::Stable)]
        data_storage_version: LanceFileVersion,
    ) {
        for with_delete in [true, false] {
            let test_dir = TempStrDir::default();
            let test_uri = &test_dir;
            let mut dataset = create_dataset(test_uri, data_storage_version).await;
            dataset.validate().await.unwrap();
            assert_eq!(dataset.count_rows(None).await.unwrap(), 200);

            if with_delete {
                dataset.delete("i >= 15 and i < 20").await.unwrap();
                dataset.validate().await.unwrap();
                assert_eq!(dataset.count_rows(None).await.unwrap(), 195);
            }

            let new_schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
                "double_i",
                DataType::Int32,
                true,
            )]));
            // Merge keeps the fragment list intact, so every fragment gets the new
            // column. Fragment 0 is the one carrying the deletions.
            let fragment_ids = dataset
                .manifest
                .fragments
                .iter()
                .map(|f| f.id as usize)
                .collect::<Vec<_>>();
            let mut merged_fragments = Vec::new();
            for fragment_id in fragment_ids {
                let fragment = &mut dataset.get_fragment(fragment_id).unwrap();
                let mut updater = fragment.updater(Some(&["i"]), None, None).await.unwrap();
                while let Some(batch) = updater.next().await.unwrap() {
                    let input_col = batch.column_by_name("i").unwrap();
                    let result_col = mul(input_col, &Int32Array::new_scalar(2)).unwrap();
                    let batch = RecordBatch::try_new(
                        new_schema.clone(),
                        vec![Arc::new(result_col) as ArrayRef],
                    )
                    .unwrap();
                    updater.update(batch).await.unwrap();
                }
                let new_fragment = updater.finish().await.unwrap();

                assert_eq!(new_fragment.files.len(), 2);
                merged_fragments.push(new_fragment);
            }

            // Scan again
            let mut full_schema = dataset.schema().merge(new_schema.as_ref()).unwrap();
            full_schema.set_field_id(None);
            let before_version = dataset.version().version;

            let op = Operation::Merge {
                fragments: merged_fragments,
                schema: full_schema.clone(),
            };

            let dataset = Dataset::commit(
                test_uri,
                op,
                Some(before_version),
                None,
                None,
                Default::default(),
                false,
            )
            .await
            .unwrap();

            assert_eq!(
                dataset.count_rows(None).await.unwrap(),
                if with_delete { 195 } else { 200 }
            );
            assert_eq!(dataset.version().version, before_version + 1);
            dataset.validate().await.unwrap();
            let new_projection = full_schema.project(&["i", "double_i"]).unwrap();

            let stream = dataset
                .scan()
                .batch_size(10)
                .project(&["i", "double_i"])
                .unwrap()
                .try_into_stream()
                .await
                .unwrap();
            let batches = stream.try_collect::<Vec<_>>().await.unwrap();

            assert_eq!(batches[1].schema().as_ref(), &(&new_projection).into());
            let expected_i = match (with_delete, data_storage_version) {
                // Legacy format uses old scan node which deletes after read and
                // so the batch is truncated
                (true, LanceFileVersion::Legacy) => vec![10, 11, 12, 13, 14],
                // Newer formats delete before read and so we get a full batch of 10
                (true, _) => vec![10, 11, 12, 13, 14, 20, 21, 22, 23, 24],
                (false, _) => vec![10, 11, 12, 13, 14, 15, 16, 17, 18, 19],
            };
            let expected_batch = RecordBatch::try_new(
                Arc::new(ArrowSchema::new(vec![
                    ArrowField::new("i", DataType::Int32, true),
                    ArrowField::new("double_i", DataType::Int32, true),
                ])),
                vec![
                    Arc::new(Int32Array::from_iter_values(expected_i.iter().copied())),
                    Arc::new(Int32Array::from_iter_values(
                        expected_i.iter().map(|i| 2 * i),
                    )),
                ],
            )
            .unwrap();
            assert_eq!(batches[1], expected_batch);
        }
    }

    #[rstest]
    #[tokio::test]
    async fn test_merge_fragment(
        #[values(LanceFileVersion::Legacy, LanceFileVersion::Stable)]
        data_storage_version: LanceFileVersion,
    ) {
        let test_dir = TempStrDir::default();
        let test_uri = &test_dir;
        let mut dataset = create_dataset(test_uri, data_storage_version).await;
        dataset.validate().await.unwrap();
        assert_eq!(dataset.count_rows(None).await.unwrap(), 200);

        let deleted_range = 15..20;
        dataset.delete("i >= 15 and i < 20").await.unwrap();
        dataset.validate().await.unwrap();
        assert_eq!(dataset.count_rows(None).await.unwrap(), 195);

        // Create data to merge: merge in double the data
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("i", DataType::Int32, true),
            ArrowField::new("double_i", DataType::Int32, true),
        ]));
        let to_merge = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from_iter_values(0..200)),
                Arc::new(Int32Array::from_iter_values((0..400).step_by(2))),
            ],
        )
        .unwrap();

        let stream = RecordBatchIterator::new(vec![Ok(to_merge)], schema.clone());
        dataset.merge(stream, "i", "i").await.unwrap();
        dataset.validate().await.unwrap();

        // Validate the resulting data
        let batches = dataset
            .scan()
            .project(&["i", "double_i"])
            .unwrap()
            .try_into_stream()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let batch = concat_batches(&schema, &batches).unwrap();

        let mut row_id: i32 = 0;
        let mut i: usize = 0;
        let array_i: &Int32Array = as_primitive_array(&batch["i"]);
        let array_double_i: &Int32Array = as_primitive_array(&batch["double_i"]);
        while row_id < 200 {
            if deleted_range.contains(&row_id) {
                row_id += 1;
                continue;
            }
            assert_eq!(array_i.value(i), row_id);
            assert_eq!(array_double_i.value(i), 2 * row_id);
            row_id += 1;
            i += 1;
        }
    }

    #[tokio::test]
    async fn test_write_batch_size() {
        // V1 ONLY
        //
        // This test is only for the legacy version of the file format.
        // It ensures that the `max_rows_per_group` property is respected
        // and this property does not exist in V2.
        let test_dir = TempStrDir::default();
        let test_uri = &test_dir;

        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "i",
            DataType::Int32,
            true,
        )]));

        let in_memory_batch = 1024;
        let batches: Vec<RecordBatch> = (0..10)
            .map(|i| {
                RecordBatch::try_new(
                    schema.clone(),
                    vec![Arc::new(Int32Array::from_iter_values(
                        i * in_memory_batch..(i + 1) * in_memory_batch,
                    ))],
                )
                .unwrap()
            })
            .collect();

        let batch_iter = RecordBatchIterator::new(batches.into_iter().map(Ok), schema.clone());

        let fragment = FileFragment::create(
            test_uri,
            10,
            batch_iter,
            Some(WriteParams {
                max_rows_per_group: 100,
                data_storage_version: Some(LanceFileVersion::Legacy),
                ..Default::default()
            }),
        )
        .await
        .unwrap();

        let (object_store, base_path) = ObjectStore::from_uri(test_uri).await.unwrap();
        let file_reader = V1FileReader::try_new_with_fragment_id(
            &object_store,
            &base_path
                .clone()
                .join("data")
                .join(fragment.files[0].path.as_str()),
            schema.as_ref().try_into().unwrap(),
            10,
            0,
            1,
            None,
        )
        .await
        .unwrap();

        for i in 0..file_reader.num_batches() - 1 {
            assert_eq!(file_reader.num_rows_in_batch(i as i32), 100);
        }
        assert_eq!(
            file_reader.num_rows_in_batch(file_reader.num_batches() as i32 - 1) as i32,
            in_memory_batch * 10 % 100
        );
    }

    #[tokio::test]
    async fn test_shuffled_columns() -> Result<()> {
        // Validates we can handle datasets where the order of columns is not
        // aligned with the order of the data files. This can happen when replacing
        // columns in a dataset.
        let batch_i = RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![ArrowField::new(
                "i",
                DataType::Int32,
                true,
            )])),
            vec![Arc::new(Int32Array::from_iter_values(0..20))],
        )?;

        let batch_s = RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![ArrowField::new(
                "s",
                DataType::Utf8,
                true,
            )])),
            vec![Arc::new(StringArray::from_iter_values(
                (0..20).map(|v| format!("s-{}", v)),
            ))],
        )?;

        // Write batch_i as a fragment
        let test_dir = TempStrDir::default();
        let test_uri = &test_dir;

        let dataset = Dataset::write(
            RecordBatchIterator::new(vec![Ok(batch_i.clone())], batch_i.schema().clone()),
            test_uri,
            None,
        )
        .await?;

        let fragment = dataset.get_fragments().pop().unwrap();

        // Write batch_s using add_columns
        let mut updater = fragment.updater(Some(&["i"]), None, None).await?;
        updater.next().await?;
        updater.update(batch_s.clone()).await?;
        let frag = updater.finish().await?;

        // Rearrange schema so it's `s` then `i`.
        let schema = updater.schema().unwrap().clone().project(&["s", "i"])?;

        let dataset = Dataset::commit(
            test_uri,
            Operation::Merge {
                schema,
                fragments: vec![frag],
            },
            Some(dataset.manifest.version),
            None,
            None,
            Default::default(),
            false,
        )
        .await?;

        let expected_data = batch_s.merge(&batch_i)?;
        let actual_data = dataset.scan().try_into_batch().await?;
        assert_eq!(expected_data, actual_data);

        // Also take, read_range, and read_batch_projected
        let reader = dataset
            .get_fragments()
            .first()
            .unwrap()
            .open(dataset.schema(), FragReadConfig::default())
            .await?;
        let actual_data = reader.take_as_batch(&[0, 1, 2], None).await?;
        assert_eq!(expected_data.slice(0, 3), actual_data);

        let actual_data = reader
            .read_range(0..3, 3)
            .await
            .unwrap()
            .next()
            .await
            .unwrap()
            .await
            .unwrap();
        assert_eq!(expected_data.slice(0, 3), actual_data);

        // Also check case of row_id.
        let expected_data = expected_data.try_with_column(
            ROW_ID_FIELD.clone(),
            Arc::new(UInt64Array::from_iter_values(0..20)),
        )?;
        let actual_data = dataset.scan().with_row_id().try_into_batch().await?;
        assert_eq!(expected_data, actual_data);

        Ok(())
    }

    #[tokio::test]
    async fn test_row_id_reader() -> Result<()> {
        // Make sure we can create a fragment reader that only captures the row_id.
        let batch = RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![ArrowField::new(
                "i",
                DataType::Int32,
                true,
            )])),
            vec![Arc::new(Int32Array::from_iter_values(0..20))],
        )?;

        let test_dir = TempStrDir::default();
        let test_uri = &test_dir;

        let dataset = Dataset::write(
            RecordBatchIterator::new(vec![Ok(batch.clone())], batch.schema().clone()),
            test_uri,
            None,
        )
        .await?;

        let fragment = dataset.get_fragments().pop().unwrap();

        let reader = fragment
            .open(
                &dataset.schema().project::<&str>(&[])?,
                FragReadConfig::default().with_row_id(true),
            )
            .await?;
        let batch = reader.legacy_read_range_as_batch(0..20).await?;

        let expected_data = RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![ROW_ID_FIELD.clone()])),
            vec![Arc::new(UInt64Array::from_iter_values(0..20))],
        )?;
        assert_eq!(expected_data, batch);

        // We should get error if we pass empty schema and with_row_id false
        let res = fragment
            .open(
                &dataset.schema().project::<&str>(&[])?,
                FragReadConfig::default(),
            )
            .await;
        assert!(matches!(res, Err(Error::NotFound { .. })));

        Ok(())
    }

    #[tokio::test]
    async fn create_from_file_v2() {
        let test_dir = TempStrDir::default();
        let test_uri = &test_dir;

        let make_gen = || {
            gen_batch()
                .col("str", array::rand_type(&DataType::Utf8))
                .col("int", array::rand_type(&DataType::Int32))
        };

        let batch = make_gen().into_batch_rows(RowCount::from(128)).unwrap();
        let dataset = TestDatasetGenerator::new(vec![batch], LanceFileVersion::Stable)
            .make_hostile(test_uri)
            .await;

        let new_data = make_gen().into_batch_rows(RowCount::from(128)).unwrap();
        let store = ObjectStore::local();
        let file_path = dataset.data_dir().join("some_file.lance");
        let object_writer = store.create(&file_path).await.unwrap();
        let mut file_writer = lance_file::versions::v2_1::create_lazy_writer(
            object_writer,
            FileWriterOptions::default(),
        );
        file_writer.write_batch(&new_data).await.unwrap();
        file_writer.finish().await.unwrap();

        let frag = FileFragment::create_from_file("some_file.lance", &dataset, 0, Some(128))
            .await
            .unwrap();

        assert_eq!(
            Fragment::try_infer_version(std::slice::from_ref(&frag))
                .unwrap()
                .unwrap(),
            ConcreteFileVersion::from(LanceFileVersion::Stable)
        );

        let op = Operation::Append {
            fragments: vec![frag],
        };
        let dataset = Dataset::commit(
            &dataset.uri,
            op,
            Some(dataset.version().version),
            None,
            None,
            Default::default(),
            false,
        )
        .await
        .unwrap();

        assert_eq!(
            dataset
                .count_rows(Some("int IS NOT NULL".to_string()))
                .await
                .unwrap(),
            256
        );
    }

    #[tokio::test]
    async fn test_lazy_column_metadata_scan_reads_less_than_full_projection() {
        let num_columns = 512;
        let rows_per_batch = 100;
        let num_batches = 10;
        let schema = Arc::new(ArrowSchema::new(
            (0..num_columns)
                .map(|i| ArrowField::new(format!("col_{i}"), DataType::Int32, true))
                .collect::<Vec<_>>(),
        ));
        let batches = (0..num_batches)
            .map(|batch_idx| {
                let columns = (0..num_columns)
                    .map(|column_idx| {
                        Arc::new(Int32Array::from_iter_values((0..rows_per_batch).map(
                            |row_idx| (batch_idx * rows_per_batch + row_idx) as i32 + column_idx,
                        ))) as ArrayRef
                    })
                    .collect::<Vec<_>>();
                RecordBatch::try_new(schema.clone(), columns).unwrap()
            })
            .collect::<Vec<_>>();

        let test_dir = TempStrDir::default();
        let write_params = WriteParams {
            max_rows_per_file: rows_per_batch * num_batches,
            max_rows_per_group: rows_per_batch,
            data_storage_version: Some(LanceFileVersion::V2_1),
            ..Default::default()
        };
        let reader = RecordBatchIterator::new(batches.into_iter().map(Ok), schema);
        let dataset = Dataset::write(reader, &test_dir, Some(write_params))
            .await
            .unwrap();

        let projection = dataset.schema().project(&["col_0"]).unwrap();
        let fragment = dataset.get_fragment(0).unwrap();

        dataset.object_store.as_ref().io_stats_incremental();
        let narrow_reader = fragment
            .open(&projection, FragReadConfig::default())
            .await
            .unwrap();
        let mut empty_narrow_stream = narrow_reader.take_range(0..0, 1024).await.unwrap();
        assert!(empty_narrow_stream.next().await.is_none());
        let narrow_metadata_stats = dataset.object_store.as_ref().io_stats_incremental();
        assert!(
            narrow_metadata_stats.read_iops <= 3,
            "expected lazy metadata open to skip the schema buffer read, iops={}, bytes={}",
            narrow_metadata_stats.read_iops,
            narrow_metadata_stats.read_bytes
        );

        let full_projection = dataset.schema().clone();
        let full_reader = fragment
            .open(&full_projection, FragReadConfig::default())
            .await
            .unwrap();
        let mut empty_full_stream = full_reader.take_range(0..0, 1024).await.unwrap();
        assert!(empty_full_stream.next().await.is_none());
        let full_metadata_stats = dataset.object_store.as_ref().io_stats_incremental();

        assert!(
            full_metadata_stats.read_bytes > narrow_metadata_stats.read_bytes * 4,
            "expected narrow lazy metadata read to fetch much less than full metadata, narrow={} bytes, full={} bytes",
            narrow_metadata_stats.read_bytes,
            full_metadata_stats.read_bytes
        );

        let mut narrow_scan = dataset.scan();
        let narrow_batch = narrow_scan
            .project(&["col_0"])
            .unwrap()
            .try_into_batch()
            .await
            .unwrap();
        assert_eq!(narrow_batch.num_columns(), 1);
        assert_eq!(narrow_batch.num_rows(), rows_per_batch * num_batches);

        let taken = fragment.take(&[0, 777, 999], &projection).await.unwrap();
        let taken_values = taken
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(taken_values.values(), &[0, 777, 999]);

        let projected_readers = fragment
            .open_readers(&projection, &FragReadConfig::default())
            .await
            .unwrap();
        let err = projected_readers[0].storage_stats().unwrap_err();
        assert!(
            err.to_string()
                .contains("storage_stats requires full file metadata"),
            "expected storage_stats to reject projected metadata, got {err:?}"
        );
    }

    #[tokio::test]
    async fn test_iops_read_small() {
        // Create a file that has 8 columns.
        let schema = Arc::new(ArrowSchema::new(
            (0..8)
                .map(|i| ArrowField::new(format!("col_{}", i), DataType::Int32, true))
                .collect::<Vec<_>>(),
        ));

        // Single row batch
        let batch = RecordBatch::try_new(
            schema.clone(),
            (0..8)
                .map(|i| Arc::new(Int32Array::from(vec![i])) as ArrayRef)
                .collect(),
        )
        .unwrap();
        let session = Arc::new(Session::default());
        let write_params = WriteParams {
            session: Some(session.clone()),
            ..Default::default()
        };
        let dataset = InsertBuilder::new("memory://test")
            .with_params(&write_params)
            .execute(vec![batch])
            .await
            .unwrap();
        let fragment = dataset.get_fragments().pop().unwrap();

        // Assert file is small (< 4300 bytes)
        {
            let stats = dataset.object_store.as_ref().io_stats_incremental();
            assert_io_eq!(stats, write_iops, 3);
            assert_io_lt!(stats, written_bytes, 4300);
        }

        // Measure IOPS needed to scan all data first time.
        let projection = Schema::try_from(schema.as_ref())
            .unwrap()
            .project_by_ids(&[0, 1, 2, 3, 4, 6, 7, 8, 9], true);
        let reader = fragment
            .open(&projection, Default::default())
            .await
            .unwrap();
        let mut data = reader
            .read_all(1024)
            .await
            .unwrap()
            .buffered(1)
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(data.len(), 1);
        let data = data.pop().unwrap();
        assert_eq!(data.num_rows(), 1);
        assert_eq!(data.num_columns(), 7);

        let stats = dataset.object_store.as_ref().io_stats_incremental();
        assert_io_eq!(stats, read_iops, 1);
        assert_io_lt!(stats, read_bytes, 4096);
    }

    #[tokio::test]
    async fn test_update_columns_with_json_extension_type() {
        use arrow_array::UInt64Array;
        use lance_arrow::ARROW_EXT_NAME_KEY;
        use lance_arrow::json::ARROW_JSON_EXT_NAME;
        use lance_core::ROW_ID;
        use std::collections::HashMap;

        // Create a dataset with an Arrow JSON extension column
        let test_dir = TempStrDir::default();
        let mut json_metadata = HashMap::new();
        json_metadata.insert(
            ARROW_EXT_NAME_KEY.to_string(),
            ARROW_JSON_EXT_NAME.to_string(),
        );
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", DataType::Int64, false),
            ArrowField::new("name", DataType::Utf8, true),
            ArrowField::new("meta", DataType::Utf8, true).with_metadata(json_metadata.clone()),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
                Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e"])),
                Arc::new(StringArray::from(vec![
                    r#"{"x":1}"#,
                    r#"{"x":2}"#,
                    r#"{"x":3}"#,
                    r#"{"x":4}"#,
                    r#"{"x":5}"#,
                ])),
            ],
        )
        .unwrap();
        let reader = RecordBatchIterator::new(vec![Ok(batch)], schema.clone());
        let dataset = Dataset::write(reader, test_dir.as_ref(), None)
            .await
            .unwrap();

        // Build the right stream with Arrow JSON column (Utf8 + arrow.json extension)
        // Only update rows with row_id 1 and 3
        let update_schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new(ROW_ID, DataType::UInt64, false),
            ArrowField::new("meta", DataType::Utf8, true).with_metadata(json_metadata),
        ]));
        let update_batch = RecordBatch::try_new(
            update_schema.clone(),
            vec![
                Arc::new(UInt64Array::from(vec![1, 3])),
                Arc::new(StringArray::from(vec![
                    r#"{"updated":true,"id":2}"#,
                    r#"{"updated":true,"id":4}"#,
                ])),
            ],
        )
        .unwrap();
        let right_stream: Box<dyn RecordBatchReader + Send> = Box::new(RecordBatchIterator::new(
            vec![Ok(update_batch)],
            update_schema,
        ));

        // Perform update_columns - this should NOT fail with type mismatch
        // Previously this would error with:
        //   "It is not possible to interleave arrays of different data types (Utf8 and LargeBinary)"
        let mut fragment = dataset.get_fragment(0).unwrap();
        let (updated_fragment, fields_modified) = fragment
            .update_columns(right_stream, ROW_ID, ROW_ID)
            .await
            .unwrap();

        // Verify the operation produced valid results
        assert!(!fields_modified.is_empty());
        assert!(!updated_fragment.files.is_empty());
    }
}
