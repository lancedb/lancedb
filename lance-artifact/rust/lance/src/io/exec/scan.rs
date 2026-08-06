// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow_array::RecordBatch;
use arrow_schema::{Schema as ArrowSchema, SchemaRef};
use datafusion::common::stats::Precision;
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties, RecordBatchStream,
    SendableRecordBatchStream, Statistics,
};
use datafusion_physical_expr::EquivalenceProperties;
use futures::future::BoxFuture;
use futures::stream::{BoxStream, Stream};
use futures::{FutureExt, TryFutureExt, stream};
use futures::{StreamExt, TryStreamExt};
use lance_arrow::SchemaExt;
use lance_core::utils::tokio::get_num_compute_intensive_cpus;
use lance_core::utils::tracing::StreamTracingExt;
use lance_core::{
    Error, ROW_ADDR_FIELD, ROW_CREATED_AT_VERSION_FIELD, ROW_ID_FIELD,
    ROW_LAST_UPDATED_AT_VERSION_FIELD,
};
use lance_file::reader::FileReaderOptions;
use lance_io::scheduler::{ScanScheduler, SchedulerConfig};
use lance_table::format::Fragment;
use log::debug;
use tracing::Instrument;

use crate::dataset::Dataset;
use crate::dataset::fragment::{FileFragment, FragReadConfig, FragmentReader};
use crate::dataset::scanner::{
    BATCH_SIZE_FALLBACK, DEFAULT_FRAGMENT_READAHEAD, DEFAULT_IO_BUFFER_SIZE,
    LEGACY_DEFAULT_FRAGMENT_READAHEAD,
};
use crate::datatypes::Schema;

use super::utils::IoMetrics;

async fn open_file(
    file_fragment: FileFragment,
    projection: Arc<Schema>,
    mut read_config: FragReadConfig,
    with_make_deletions_null: bool,
    scan_scheduler: Option<(Arc<ScanScheduler>, u32)>,
) -> Result<FragmentReader> {
    if let Some((scan_scheduler, reader_priority)) = scan_scheduler {
        read_config = read_config
            .with_scan_scheduler(scan_scheduler)
            .with_reader_priority(reader_priority);
    }

    let mut reader = file_fragment.open(projection.as_ref(), read_config).await?;

    if with_make_deletions_null {
        reader.with_make_deletions_null();
    };
    Ok(reader)
}

struct FragmentWithRange {
    fragment: FileFragment,
    range: Option<Range<u32>>,
}

struct ScanMetrics {
    baseline_metrics: BaselineMetrics,
    io_metrics: IoMetrics,
}

impl ScanMetrics {
    fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        Self {
            baseline_metrics: BaselineMetrics::new(metrics, partition),
            io_metrics: IoMetrics::new(metrics, partition),
        }
    }
}

/// Default behavior
/// polling method for non-strict batch size mode.
///
/// # Use Case
/// When strict batch size is disabled, this method allows natural batch sizes from storage,
/// concatenating residuals across fragments. Ideal for streaming scenarios prioritizing throughput
/// over consistent batch sizes.
///
/// # Example
/// With batch_size=5 and a fragment containing 7 rows:
/// Output batches: [5 rows], [2 rows]
/// Next fragment with 4 rows won't combine residuals with the next fragment.:
/// Output batches: [4 rows]
impl Stream for LanceStream {
    type Item = Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let timer = this.scan_metrics.baseline_metrics.elapsed_compute().timer();

        let inner_poll = this.inner_stream.poll_next_unpin(cx);
        timer.done();

        let poll_result = match inner_poll {
            Poll::Ready(None) => {
                if let Some(scheduler) = &this.scan_scheduler {
                    this.scan_metrics.io_metrics.record(scheduler);
                }
                Poll::Ready(None)
            }
            Poll::Ready(Some(Ok(batch))) => Poll::Ready(Some(Ok(batch))),
            other => other,
        };

        this.scan_metrics.baseline_metrics.record_poll(poll_result)
    }
}

/// Dataset Scan Node.
pub struct LanceStream {
    inner_stream: stream::BoxStream<'static, Result<RecordBatch>>,

    /// Manifest of the dataset
    projection: Arc<Schema>,

    config: LanceScanConfig,

    scan_metrics: ScanMetrics,

    /// Scan scheduler for the scan node.
    ///
    /// Only set on v2 scans.  Used to record scan metrics.
    scan_scheduler: Option<Arc<ScanScheduler>>,
}

impl LanceStream {
    /// Create a new dataset scan node.
    ///
    /// Parameters
    ///
    ///  - ***dataset***: The source dataset.
    ///  - ***fragments***: The fragments to scan.
    ///  - ***offsets***: The range of offsets to scan (scan all rows if None).
    ///  - ***projection***: the projection [Schema].
    ///  - ***filter***: filter [`PhysicalExpr`], optional.
    ///  - ***read_size***: the number of rows to read for each request.
    ///  - ***batch_readahead***: the number of batches to read ahead.
    ///  - ***fragment_readahead***: the number of fragments to read ahead (only
    ///    if scan_in_order = false).
    ///  - ***with_row_id***: load row ID from the datasets.
    ///  - ***with_row_address***: load row address from the datasets.
    ///  - ***with_make_deletions_null***: make deletions null.
    ///  - ***scan_in_order***: whether to scan the fragments in the provided order.
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        dataset: Arc<Dataset>,
        fragments: Arc<Vec<Fragment>>,
        offsets: Option<Range<u64>>,
        projection: Arc<Schema>,
        config: LanceScanConfig,
        metrics: &ExecutionPlanMetricsSet,
        partition: usize,
    ) -> Result<Self> {
        let is_v2_scan = fragments
            .iter()
            .filter_map(|frag| frag.files.first().map(|f| !f.is_legacy_file()))
            .next()
            .unwrap_or(false);
        if is_v2_scan {
            Self::try_new_v2(
                dataset, fragments, offsets, projection, config, metrics, partition,
            )
        } else {
            Self::try_new_v1(dataset, fragments, projection, config, metrics, partition)
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn try_new_v2(
        dataset: Arc<Dataset>,
        fragments: Arc<Vec<Fragment>>,
        offsets: Option<Range<u64>>,
        projection: Arc<Schema>,
        config: LanceScanConfig,
        metrics: &ExecutionPlanMetricsSet,
        partition: usize,
    ) -> Result<Self> {
        let scan_metrics = ScanMetrics::new(metrics, partition);
        let timer = scan_metrics.baseline_metrics.elapsed_compute().timer();
        let materialize_blob_v2_binary =
            crate::dataset::blob::schema_has_blob_v2_binary_view(projection.as_ref());
        let read_projection = if materialize_blob_v2_binary {
            Arc::new(crate::dataset::blob::blob_v2_descriptor_schema(
                projection.as_ref(),
            ))
        } else {
            projection.clone()
        };
        let project_schema = read_projection;
        let output_projection = if materialize_blob_v2_binary {
            let mut output_projection = projection.as_ref().clone();
            let mut system_fields = Vec::with_capacity(4);
            if config.with_row_id {
                system_fields.push(ROW_ID_FIELD.clone());
            }
            if config.with_row_address {
                system_fields.push(ROW_ADDR_FIELD.clone());
            }
            if config.with_row_last_updated_at_version {
                system_fields.push(ROW_LAST_UPDATED_AT_VERSION_FIELD.clone());
            }
            if config.with_row_created_at_version {
                system_fields.push(ROW_CREATED_AT_VERSION_FIELD.clone());
            }
            output_projection.extend(&system_fields)?;
            Arc::new(output_projection)
        } else {
            projection.clone()
        };
        let io_parallelism = dataset.object_store.io_parallelism();
        // First, use the value specified by the user in the call
        // Second, use the default from the environment variable, if specified
        // Finally, use a default based on the io_parallelism
        //
        // Opening a fragment is pretty cheap so we can open a lot of them at once
        // Scheduling a fragment is also pretty cheap
        // The scheduler backpressure will control fragment priority and total data
        //
        // As a result, we don't really need to worry too much about fragment readahead.  We also want this
        // to be pretty high.  While we are reading one set of fragments we should be scheduling the next set
        // this should help ensure that we don't have breaks in I/O
        let frag_parallelism = config
            .fragment_readahead
            .unwrap_or((*DEFAULT_FRAGMENT_READAHEAD).unwrap_or(io_parallelism * 2))
            // fragment_readhead=0 doesn't make sense so we just bump it to 1
            .max(1);
        debug!(
            "Given io_parallelism={} and num_columns={} we will read {} fragments at once while scanning v2 dataset",
            io_parallelism,
            projection.fields.len(),
            frag_parallelism
        );

        let mut file_fragments = fragments
            .iter()
            .map(|fragment| FileFragment::new(dataset.clone(), fragment.clone()))
            .map(|fragment| FragmentWithRange {
                fragment,
                range: None,
            })
            .collect::<Vec<_>>();

        if let Some(offsets) = offsets {
            let mut rows_to_skip = offsets.start;
            let mut rows_to_take = offsets.end - offsets.start;
            let mut filtered_fragments = Vec::with_capacity(file_fragments.len());

            let mut frags_iter = file_fragments.into_iter();
            while rows_to_take > 0 {
                if let Some(next_frag) = frags_iter.next() {
                    let num_rows_in_frag = next_frag
                        .fragment
                        .count_rows(None)
                        // count_rows should be a fast operation in v2 files
                        .now_or_never()
                        .ok_or(Error::internal(
                            "Encountered fragment without row count metadata in v2 file"
                                .to_string(),
                        ))??;
                    if rows_to_skip >= num_rows_in_frag as u64 {
                        rows_to_skip -= num_rows_in_frag as u64;
                    } else {
                        let rows_to_take_in_frag =
                            (num_rows_in_frag as u64 - rows_to_skip).min(rows_to_take);
                        let range =
                            Some(rows_to_skip as u32..(rows_to_skip + rows_to_take_in_frag) as u32);
                        filtered_fragments.push(FragmentWithRange {
                            fragment: next_frag.fragment,
                            range,
                        });
                        rows_to_skip = 0;
                        rows_to_take -= rows_to_take_in_frag;
                    }
                } else {
                    log::warn!(
                        "Ran out of fragments before we were done scanning for range: {:?}",
                        offsets
                    );
                    rows_to_take = 0;
                }
            }
            file_fragments = filtered_fragments;
        }

        let scan_scheduler = ScanScheduler::new(
            dataset.object_store.clone(),
            SchedulerConfig::new(config.io_buffer_size),
        );

        let scan_scheduler_clone = scan_scheduler.clone();

        let materialize_dataset = dataset;
        let config_for_stream = config.clone();
        let batches = stream::iter(file_fragments.into_iter().enumerate())
            .map(move |(priority, file_fragment)| {
                let project_schema = project_schema.clone();
                let scan_scheduler = scan_scheduler.clone();
                let config = config_for_stream.clone();
                let force_row_address = materialize_blob_v2_binary;
                #[allow(clippy::type_complexity)]
                let frag_task: BoxFuture<
                    Result<BoxStream<Result<BoxFuture<Result<RecordBatch>>>>>,
                > = tokio::spawn(
                    (async move {
                        let mut frag_config = FragReadConfig::default()
                            .with_row_id(config.with_row_id)
                            .with_row_address(config.with_row_address || force_row_address)
                            .with_row_last_updated_at_version(
                                config.with_row_last_updated_at_version,
                            )
                            .with_row_created_at_version(config.with_row_created_at_version);
                        if let Some(file_reader_options) = config.file_reader_options {
                            frag_config = frag_config.with_file_reader_options(file_reader_options);
                        }
                        let reader = open_file(
                            file_fragment.fragment,
                            project_schema,
                            frag_config,
                            config.with_make_deletions_null,
                            Some((scan_scheduler, priority as u32)),
                        )
                        .await?;
                        let batch_stream = if let Some(range) = file_fragment.range {
                            reader
                                .read_range(range, config.batch_size as u32)
                                .await?
                                .boxed()
                        } else {
                            reader.read_all(config.batch_size as u32).await?.boxed()
                        };
                        let batch_stream: BoxStream<Result<BoxFuture<Result<RecordBatch>>>> =
                            batch_stream
                                .map(|fut| {
                                    Result::Ok(
                                        fut.map_err(|e| DataFusionError::External(Box::new(e)))
                                            .boxed(),
                                    )
                                })
                                .boxed();
                        Result::Ok(batch_stream)
                    })
                    .in_current_span(),
                )
                .map(|res_res| res_res.unwrap())
                .boxed();
                Ok(frag_task)
            })
            // We need two levels of try_buffered here.  The first kicks off the tasks to read the fragments.
            // As soon as we open the fragment we will start scheduling and that will kick off many background
            // tasks (not tracked by this stream) to read I/O.  The limit here is really to limit how many open
            // files we have.  It's not going to have much affect on how much RAM we are using.
            .try_buffered(frag_parallelism)
            .boxed();
        let inner_stream = batches
            .try_flatten()
            // The second try_buffered controls how many CPU decode tasks we kick off in parallel.
            //
            // TODO: Ideally this will eventually get tied into datafusion as a # of partitions.  This will let
            // us fully fuse decode into the first half of the plan.  Currently there is likely to be a thread
            // transfer between the two steps.
            .try_buffered(
                get_num_compute_intensive_cpus()
                    .min(config.parallelism_cap.unwrap_or(usize::MAX))
                    .max(1),
            )
            .stream_in_current_span()
            .boxed();
        let inner_stream = if materialize_blob_v2_binary {
            inner_stream
                .and_then(move |batch| {
                    let dataset = materialize_dataset.clone();
                    let output_projection = output_projection.clone();
                    async move {
                        crate::dataset::blob::materialize_blob_v2_binary_batch(
                            &dataset,
                            output_projection.as_ref(),
                            batch,
                        )
                        .await
                        .map_err(DataFusionError::from)
                    }
                })
                .boxed()
        } else {
            inner_stream
        };

        timer.done();
        Ok(Self {
            inner_stream,
            projection,
            config,
            scan_metrics,
            scan_scheduler: Some(scan_scheduler_clone),
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn try_new_v1(
        dataset: Arc<Dataset>,
        fragments: Arc<Vec<Fragment>>,
        projection: Arc<Schema>,
        config: LanceScanConfig,
        metrics: &ExecutionPlanMetricsSet,
        partition: usize,
    ) -> Result<Self> {
        let scan_metrics = ScanMetrics::new(metrics, partition);
        let timer = scan_metrics.baseline_metrics.elapsed_compute().timer();
        let project_schema = projection.clone();
        let fragment_readahead = config
            .fragment_readahead
            .unwrap_or(LEGACY_DEFAULT_FRAGMENT_READAHEAD);
        let batch_readahead = config
            .batch_readahead
            .min(config.parallelism_cap.unwrap_or(usize::MAX))
            .max(1);
        debug!(
            "Scanning v1 dataset with frag_readahead={} and batch_readahead={}",
            fragment_readahead, batch_readahead
        );

        let file_fragments = fragments
            .iter()
            .map(|fragment| FileFragment::new(dataset.clone(), fragment.clone()))
            .collect::<Vec<_>>();

        let batches = if config.ordered_output {
            let readers = stream::iter(file_fragments)
                .map(move |file_fragment| {
                    Ok(open_file(
                        file_fragment,
                        project_schema.clone(),
                        FragReadConfig::default()
                            .with_row_id(config.with_row_id)
                            .with_row_address(config.with_row_address)
                            .with_row_last_updated_at_version(
                                config.with_row_last_updated_at_version,
                            )
                            .with_row_created_at_version(config.with_row_created_at_version),
                        config.with_make_deletions_null,
                        None,
                    ))
                })
                .try_buffered(fragment_readahead);
            let tasks = readers.and_then(move |reader| async move {
                reader
                    .read_all(config.batch_size as u32)
                    .await
                    .map(|task_stream| task_stream.map(Ok))
                    .map_err(DataFusionError::from)
            });
            tasks
                // We must be waiting to finish a file before moving onto thenext. That's an issue.
                .try_flatten()
                // We buffer up to `batch_readahead` batches across all streams.
                .try_buffered(batch_readahead)
                .stream_in_current_span()
                .boxed()
        } else {
            let readers = stream::iter(file_fragments)
                .map(move |file_fragment| {
                    Ok(open_file(
                        file_fragment,
                        project_schema.clone(),
                        FragReadConfig::default()
                            .with_row_id(config.with_row_id)
                            .with_row_address(config.with_row_address)
                            .with_row_last_updated_at_version(
                                config.with_row_last_updated_at_version,
                            )
                            .with_row_created_at_version(config.with_row_created_at_version),
                        config.with_make_deletions_null,
                        None,
                    ))
                })
                .try_buffered(fragment_readahead);
            let tasks = readers.and_then(move |reader| async move {
                reader
                    .read_all(config.batch_size as u32)
                    .await
                    .map(|task_stream| task_stream.map(Ok))
                    .map_err(DataFusionError::from)
            });
            // When we flatten the streams (one stream per fragment), we allow
            // `fragment_readahead` stream to be read concurrently.
            tasks
                .try_flatten_unordered(config.fragment_readahead)
                // We buffer up to `batch_readahead` batches across all streams.
                .try_buffer_unordered(batch_readahead)
                .stream_in_current_span()
                .boxed()
        };

        let inner_stream = Box::pin(batches.map_err(|e| DataFusionError::External(Box::new(e))))
            as Pin<Box<dyn Stream<Item = Result<_, _>> + Send>>;

        timer.done();
        Ok(Self {
            inner_stream,
            projection,
            config,
            scan_metrics,
            scan_scheduler: None,
        })
    }
}

impl core::fmt::Debug for LanceStream {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LanceStream")
            .field("projection", &self.projection)
            .field("with_row_id", &self.config.with_row_id)
            .field("with_row_address", &self.config.with_row_address)
            .finish()
    }
}

impl RecordBatchStream for LanceStream {
    fn schema(&self) -> SchemaRef {
        let output_projection =
            crate::dataset::blob::public_blob_v2_binary_output_schema(self.projection.as_ref());
        let mut schema: ArrowSchema = (&output_projection).into();
        if self.config.with_row_id {
            schema = schema.try_with_column(ROW_ID_FIELD.clone()).unwrap();
        }
        if self.config.with_row_address {
            schema = schema.try_with_column(ROW_ADDR_FIELD.clone()).unwrap();
        }
        if self.config.with_row_last_updated_at_version {
            schema = schema
                .try_with_column((*lance_core::ROW_LAST_UPDATED_AT_VERSION_FIELD).clone())
                .unwrap();
        }
        if self.config.with_row_created_at_version {
            schema = schema
                .try_with_column((*lance_core::ROW_CREATED_AT_VERSION_FIELD).clone())
                .unwrap();
        }
        Arc::new(schema)
    }
}

#[derive(Debug, Clone)]
pub struct LanceScanConfig {
    pub batch_size: usize,
    pub batch_readahead: usize,
    pub fragment_readahead: Option<usize>,
    pub io_buffer_size: u64,
    pub with_row_id: bool,
    pub with_row_address: bool,
    pub with_row_last_updated_at_version: bool,
    pub with_row_created_at_version: bool,
    pub with_make_deletions_null: bool,
    pub ordered_output: bool,
    pub file_reader_options: Option<FileReaderOptions>,
    /// Upper bound on frag_parallelism and CPU decode concurrency. Set from
    /// DataFusion's `target_partitions` session config in `LanceScanExec::execute`.
    pub parallelism_cap: Option<usize>,
}

// This is mostly for testing purposes, end users are unlikely to create this
// on their own.
impl Default for LanceScanConfig {
    fn default() -> Self {
        Self {
            batch_size: BATCH_SIZE_FALLBACK,
            batch_readahead: get_num_compute_intensive_cpus(),
            fragment_readahead: None,
            io_buffer_size: *DEFAULT_IO_BUFFER_SIZE,
            with_row_id: false,
            with_row_address: false,
            with_row_last_updated_at_version: false,
            with_row_created_at_version: false,
            with_make_deletions_null: false,
            ordered_output: false,
            file_reader_options: None,
            parallelism_cap: None,
        }
    }
}

/// DataFusion [ExecutionPlan] for scanning one Lance dataset
#[derive(Debug)]
pub struct LanceScanExec {
    dataset: Arc<Dataset>,
    fragments: Arc<Vec<Fragment>>,
    range: Option<Range<u64>>,
    projection: Arc<Schema>,
    output_schema: Arc<ArrowSchema>,
    properties: Arc<PlanProperties>,
    config: LanceScanConfig,
    metrics: ExecutionPlanMetricsSet,
}

impl DisplayAs for LanceScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let columns = self
            .projection
            .fields
            .iter()
            .map(|f| f.name.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "LanceScan: uri={}, projection=[{}], row_id={}, row_addr={}, ordered={}, range={:?}",
                    self.dataset.data_dir(),
                    columns,
                    self.config.with_row_id,
                    self.config.with_row_address,
                    self.config.ordered_output,
                    self.range
                )
            }
            DisplayFormatType::TreeRender => {
                write!(
                    f,
                    "LanceScan\nuri={}\nprojection=[{}]\nrow_id={}\nrow_addr={}\nordered={}\nrange={:?}",
                    self.dataset.data_dir(),
                    columns,
                    self.config.with_row_id,
                    self.config.with_row_address,
                    self.config.ordered_output,
                    self.range
                )
            }
        }
    }
}

impl LanceScanExec {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        dataset: Arc<Dataset>,
        fragments: Arc<Vec<Fragment>>,
        range: Option<Range<u64>>,
        projection: Arc<Schema>,
        config: LanceScanConfig,
    ) -> Self {
        let output_projection =
            crate::dataset::blob::public_blob_v2_binary_output_schema(projection.as_ref());
        let mut output_schema: ArrowSchema = (&output_projection).into();

        if config.with_row_id {
            output_schema = output_schema.try_with_column(ROW_ID_FIELD.clone()).unwrap();
        }
        if config.with_row_address {
            output_schema = output_schema
                .try_with_column(ROW_ADDR_FIELD.clone())
                .unwrap();
        }
        if config.with_row_last_updated_at_version {
            output_schema = output_schema
                .try_with_column((*lance_core::ROW_LAST_UPDATED_AT_VERSION_FIELD).clone())
                .unwrap();
        }
        if config.with_row_created_at_version {
            output_schema = output_schema
                .try_with_column((*lance_core::ROW_CREATED_AT_VERSION_FIELD).clone())
                .unwrap();
        }
        let output_schema = Arc::new(output_schema);

        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(output_schema.clone()),
            Partitioning::RoundRobinBatch(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            dataset,
            fragments,
            range,
            projection,
            output_schema,
            properties,
            config,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    /// Get the dataset for this scan.
    pub fn dataset(&self) -> &Arc<Dataset> {
        &self.dataset
    }

    /// Get the fragments for this scan.
    pub fn fragments(&self) -> &Arc<Vec<Fragment>> {
        &self.fragments
    }

    /// Get the range for this scan.
    pub fn range(&self) -> &Option<Range<u64>> {
        &self.range
    }

    /// Get the projection for this scan.
    pub fn projection(&self) -> &Arc<Schema> {
        &self.projection
    }

    // Get the scan config for this scan.
    pub fn config(&self) -> &LanceScanConfig {
        &self.config
    }
}

impl ExecutionPlan for LanceScanExec {
    fn name(&self) -> &str {
        "LanceScanExec"
    }

    fn schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }

    /// Scan is the leaf node, so returns an empty vector.
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(self)
        } else {
            Err(DataFusionError::Internal(
                "LanceScanExec cannot be assigned children".to_string(),
            ))
        }
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::context::TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let dataset = self.dataset.clone();
        let fragments = self.fragments.clone();
        let range = self.range.clone();
        let projection = self.projection.clone();
        let target_partitions = context.session_config().target_partitions();
        let config = LanceScanConfig {
            parallelism_cap: Some(target_partitions),
            ..self.config.clone()
        };
        let metrics = self.metrics.clone();

        let lance_fut_stream = stream::once(async move {
            LanceStream::try_new(
                dataset, fragments, range, projection, config, &metrics, partition,
            )
        });
        let lance_stream = lance_fut_stream.try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            lance_stream,
        )))
    }

    fn partition_statistics(&self, _partition: Option<usize>) -> Result<Arc<Statistics>> {
        // Some fragments from older datasets might have the row count stats missing.
        let (row_count, is_exact) =
            self.fragments
                .iter()
                .fold(
                    (0, true),
                    |(row_count, is_exact), fragment| match fragment.num_rows() {
                        Some(num_rows) => (row_count + num_rows, is_exact),
                        None => (row_count, false),
                    },
                );
        let num_rows = match is_exact {
            true => Precision::Exact(row_count),
            false => Precision::Absent,
        };

        Ok(Arc::new(Statistics {
            num_rows,
            ..Statistics::new_unknown(self.schema().as_ref())
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn supports_limit_pushdown(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use datafusion::execution::TaskContext;
    use datafusion::prelude::SessionConfig;
    use futures::TryStreamExt;
    use lance_datagen::gen_batch;

    use crate::utils::test::NoContextTestFixture;

    use super::*;

    #[test]
    fn no_context_scan() {
        // These tests ensure we can create nodes and call execute without a tokio Runtime
        // being active.  This is a requirement for proper implementation of a Datafusion foreign
        // table provider.
        let fixture = NoContextTestFixture::new();

        let scan = LanceScanExec::new(
            Arc::new(fixture.dataset.clone()),
            fixture.dataset.fragments().clone(),
            None,
            Arc::new(fixture.dataset.schema().clone()),
            LanceScanConfig::default(),
        );

        scan.execute(0, Arc::new(TaskContext::default())).unwrap();
    }

    /// Verify that executing with target_partitions=1 produces the same row count as the
    /// default context. Regression guard for the parallelism cap.
    #[tokio::test]
    async fn test_target_partitions_cap_produces_correct_results() {
        use lance_core::utils::tempfile::TempStrDir;
        use lance_datagen::{Dimension, array};

        use crate::utils::test::{DatagenExt, FragmentCount, FragmentRowCount};

        let tmp = TempStrDir::default();
        let dataset = gen_batch()
            .col("x", array::step::<arrow_array::types::Int32Type>())
            .col(
                "v",
                array::rand_vec::<arrow_array::types::Float32Type>(Dimension::from(4)),
            )
            .into_dataset(
                tmp.as_str(),
                FragmentCount::from(4),
                FragmentRowCount::from(100),
            )
            .await
            .unwrap();
        let dataset = Arc::new(dataset);

        let scan = LanceScanExec::new(
            dataset.clone(),
            dataset.fragments().clone(),
            None,
            Arc::new(dataset.schema().clone()),
            LanceScanConfig::default(),
        );

        let low_ctx = Arc::new(
            TaskContext::default()
                .with_session_config(SessionConfig::default().with_target_partitions(1)),
        );
        let stream = scan.execute(0, low_ctx).unwrap();
        let batches = stream.try_collect::<Vec<_>>().await.unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 400);
    }
}
