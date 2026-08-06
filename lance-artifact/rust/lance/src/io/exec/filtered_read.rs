// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors
use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::task::Poll;
use std::{
    ops::{Range, RangeInclusive},
    sync::Arc,
};

use arrow_array::cast::AsArray;
use arrow_array::types::UInt64Type;
use arrow_array::{Array, BooleanArray, RecordBatch, UInt32Array};
use arrow_schema::{Schema as ArrowSchema, SchemaRef};
use datafusion::common::runtime::SpawnedTask;
use datafusion::common::stats::Precision;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::stream::{RecordBatchReceiverStream, RecordBatchStreamAdapter};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
    execution_plan::{Boundedness, EmissionType},
};
use datafusion_expr::Expr;
use datafusion_physical_expr::{EquivalenceProperties, Partitioning, PhysicalExpr};
use datafusion_physical_plan::Statistics;
use datafusion_physical_plan::filter::FilterExec;
use datafusion_physical_plan::metrics::{BaselineMetrics, Count, MetricsSet, Time};
use futures::stream::BoxStream;
use futures::{FutureExt, Stream, StreamExt, TryFutureExt, TryStreamExt, future};
use lance_arrow::RecordBatchExt;
use lance_core::datatypes::OnMissing;
use lance_core::utils::deletion::DeletionVector;
use lance_core::utils::futures::FinallyStreamExt;
use lance_core::utils::tokio::get_num_compute_intensive_cpus;
use lance_core::{
    Error, ROW_ADDR, ROW_ADDR_FIELD, ROW_ID, ROW_ID_FIELD, Result, datatypes::Projection,
};
use lance_datafusion::planner::Planner;
use lance_datafusion::utils::{
    ExecutionPlanMetricsSetExt, FRAGMENTS_SCANNED_METRIC, RANGES_SCANNED_METRIC,
    ROWS_SCANNED_METRIC, TASK_WAIT_TIME_METRIC,
};
use lance_file::reader::FileReaderOptions;
use lance_index::scalar::expression::FilterPlan;
use lance_io::scheduler::{ScanScheduler, SchedulerConfig};
use lance_select::{
    IndexExprResult, RowAddrMask, RowAddrSelection, RowAddrTreeMap, bitmap_to_ranges,
    ranges_to_bitmap, result::IndexExprResultWireFormat,
};
use lance_table::format::Fragment;
use lance_table::rowids::RowIdSequence;
use lance_table::utils::stream::ReadBatchFut;
use roaring::RoaringBitmap;
use tokio::sync::{Mutex as AsyncMutex, OnceCell};
use tracing::{Instrument, instrument};

use crate::Dataset;
use crate::dataset::fragment::{FileFragment, FragReadConfig};
use crate::dataset::rowids::load_row_id_sequence;
use crate::dataset::scanner::{
    BATCH_SIZE_FALLBACK, DEFAULT_FRAGMENT_READAHEAD, get_default_batch_size,
    get_default_io_buffer_size_override,
};

use super::utils::IoMetrics;

fn public_blob_v2_binary_projection_schema(projection: &Projection) -> SchemaRef {
    let schema = projection.to_schema();
    let schema = crate::dataset::blob::public_blob_v2_binary_output_schema(&schema);
    let schema: ArrowSchema = (&schema).into();
    Arc::new(schema)
}

#[derive(Debug)]
pub struct EvaluatedIndex {
    index_result: IndexExprResult,
    applicable_fragments: RoaringBitmap,
}

impl EvaluatedIndex {
    /// Get the row id mask representing which rows matched the index filter.
    pub fn index_result(&self) -> &IndexExprResult {
        &self.index_result
    }

    /// Get a reference to the applicable fragments bitmap, containing the set of fragment IDs
    /// implicated by the filter.
    pub fn applicable_fragments(&self) -> &RoaringBitmap {
        &self.applicable_fragments
    }

    pub fn try_from_arrow(batch: &RecordBatch) -> Result<Self> {
        let (index_result, applicable_fragments) = IndexExprResult::deserialize(batch)?;

        Ok(Self {
            index_result,
            applicable_fragments,
        })
    }

    /// Block `rows` (stale overlay row addresses) from the index result so the index never
    /// emits them. Their fragments stay in the covered set, so non-stale rows keep the index;
    /// the blocked rows are re-evaluated against their current (overlay-merged) values on a
    /// separate targeted take path built by the scanner.
    fn without_rows(mut self, block_list: &RowAddrTreeMap) -> Self {
        self.index_result.upper =
            std::mem::take(&mut self.index_result.upper).also_block(block_list.clone());
        self.index_result.lower =
            std::mem::take(&mut self.index_result.lower).also_block(block_list.clone());
        self
    }
}

/// A fragment along with ranges of row offsets to read
struct ScopedFragmentRead {
    fragment: Arc<FileFragment>,
    ranges: Vec<Range<u64>>,
    projection: Arc<Projection>,
    with_deleted_rows: bool,
    batch_size: u32,
    file_reader_options: Option<FileReaderOptions>,
    // An in-memory filter to apply after reading the fragment (whatever couldn't be
    // pushed down into the index query)
    filter: Option<Expr>,
    priority: u32,
    scan_scheduler: Arc<ScanScheduler>,
}

impl ScopedFragmentRead {
    fn frag_read_config(&self) -> FragReadConfig {
        let mut config = FragReadConfig::default()
            .with_row_id(self.with_deleted_rows || self.projection.with_row_id)
            .with_row_address(self.projection.with_row_addr)
            .with_row_last_updated_at_version(self.projection.with_row_last_updated_at_version)
            .with_row_created_at_version(self.projection.with_row_created_at_version)
            .with_scan_scheduler(self.scan_scheduler.clone())
            .with_reader_priority(self.priority);
        if let Some(file_reader_options) = &self.file_reader_options {
            config = config.with_file_reader_options(file_reader_options.clone());
        }
        config
    }
}

/// A fragment with all of its metadata loaded
#[derive(Debug, Clone)]
struct LoadedFragment {
    row_id_sequence: Arc<RowIdSequence>,
    deletion_vector: Option<Arc<DeletionVector>>,
    fragment: Arc<FileFragment>,
    // The number of physical rows in the fragment
    //
    // This count includes deleted rows
    num_physical_rows: u64,
    // The number of logical rows in the fragment
    //
    // This count does not include deleted rows
    num_logical_rows: u64,
}

/// Given a sorted iterator of deleted row offsets, return a sorted iterator of valid row ranges
///
/// For example, given a fragment with 100 rows, and a deletion vector of 10, 15, 16 this would
/// return 0..10, 11..15, 17..100
struct DvToValidRanges<I: Iterator<Item = u64> + Send> {
    deleted_rows: I,
    num_rows: u64,
    position: u64,
}

impl<I: Iterator<Item = u64> + Send> DvToValidRanges<I> {
    fn new(deleted_rows: I, num_rows: u64) -> Self {
        Self {
            deleted_rows,
            num_rows,
            position: 0,
        }
    }
}

impl<I: Iterator<Item = u64> + Send> Iterator for DvToValidRanges<I> {
    type Item = Range<u64>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.position >= self.num_rows {
            return None;
        }
        for next_deleted_row in self.deleted_rows.by_ref() {
            if next_deleted_row == self.position {
                self.position += 1;
            } else {
                let position = self.position;
                self.position = next_deleted_row + 1;
                return Some(position..next_deleted_row);
            }
        }
        let position = self.position;
        self.position = self.num_rows;
        if position == self.num_rows {
            // Last deleted row was end of the fragment, return None
            None
        } else {
            // Still some rows after the last deleted row, return them
            Some(position..self.num_rows)
        }
    }
}

/// Global metrics for the FilteredReadExec node
///
/// These represent work that is not divisible by partition and this work is always
/// reported on partition 0
pub struct FilteredReadGlobalMetrics {
    fragments_scanned: Count,
    ranges_scanned: Count,
    rows_scanned: Count,
    io_metrics: IoMetrics,
}

impl FilteredReadGlobalMetrics {
    pub fn new(metrics: &ExecutionPlanMetricsSet) -> Self {
        Self {
            fragments_scanned: metrics.new_count(FRAGMENTS_SCANNED_METRIC, 0),
            ranges_scanned: metrics.new_count(RANGES_SCANNED_METRIC, 0),
            rows_scanned: metrics.new_count(ROWS_SCANNED_METRIC, 0),
            io_metrics: IoMetrics::new(metrics, 0),
        }
    }
}

/// Partition metrics for the FilteredReadExec node
///
/// These represent work that is divisible by partition and this work is reported on the
/// partition that it belongs to
pub struct FilteredReadPartitionMetrics {
    // Records the amount of time spent waiting on the lock to grab the next task
    //
    // This should typically be fairly small relative to the overall execution time.  If this
    // value is large then it means we are bottlenecked on the read scheduler which is preventing
    // this partition from being utilized.
    task_wait_time: Time,
    baseline_metrics: BaselineMetrics,
}

impl FilteredReadPartitionMetrics {
    pub fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        Self {
            task_wait_time: metrics.new_time(TASK_WAIT_TIME_METRIC, partition),
            baseline_metrics: BaselineMetrics::new(metrics, partition),
        }
    }
}

/// Tracks the number of ranges scanned based on the number of rows processed
struct RangeMetricsTracker {
    ranges: Vec<Range<u64>>,
    cumulative_rows: usize,
    current_range_index: usize,
    rows_processed_in_range: usize,
}

impl RangeMetricsTracker {
    fn new(ranges: Vec<Range<u64>>) -> Self {
        Self {
            ranges,
            cumulative_rows: 0,
            current_range_index: 0,
            rows_processed_in_range: 0,
        }
    }

    // Counts ranges started scanning (not necessarily finished).
    fn incremental_ranges_scanned(&mut self, num_rows: usize) -> usize {
        self.cumulative_rows += num_rows;
        let mut additional_ranges = 0;

        while self.current_range_index < self.ranges.len() {
            let current_range = &self.ranges[self.current_range_index];
            let range_size = (current_range.end - current_range.start) as usize;

            if self.cumulative_rows >= range_size {
                // We've completed this range
                if self.rows_processed_in_range == 0 {
                    // We are completing a range we never started
                    additional_ranges += 1;
                }
                self.cumulative_rows -= range_size;
                self.current_range_index += 1;
                self.rows_processed_in_range = 0;
            } else {
                // Still within the current range
                if self.rows_processed_in_range == 0 {
                    additional_ranges += 1;
                }
                self.rows_processed_in_range += num_rows;
                break;
            }
        }

        additional_ranges
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FilteredReadThreadingMode {
    /// This mode allows for multi-threading to be used even if there is only a single
    /// partition.  In this mode, readahead will be added via the try_buffered method.
    ///
    /// This mode is slightly less efficient as it is unlikely the decode will happen
    /// on the same thread as any downstream logic.  However, it is simple, and the reads
    /// are sequential.
    ///
    /// The number of threads is specified by the parameter
    OnePartitionMultipleThreads(usize),

    /// This mode will use a single thread per partition.  This is more traditional for
    /// DataFusion and should give better performance for complex queries that have a
    /// lot of downstream processing.  However, you will want to make sure to create the
    /// node with enough partitions or else you will not get any parallelism.
    ///
    /// The number of partitions is specified by the parameter.
    MultiplePartitions(usize),
}

/// The stream of filtered rows that satisfies the FilteredReadExec node
///
/// This represents a scan of a Lance dataset.  Upon creation of the stream we will
/// load the fragments, execute any scalar index query, and then plan out which portions
/// of the fragments we need to read.
///
/// For each fragment, we may read the entire fragment or we may read a portion of it.  We
/// can use both the scan range and the index result to limit the amount of a fragment that
/// we read.
struct FilteredReadStream {
    /// The schema of the output of the scan
    output_schema: SchemaRef,
    /// The stream of filtered rows, expressed as a stream of tasks (batch futures)
    ///
    /// This stream can be shared by multiple partitions
    task_stream: Arc<AsyncMutex<BoxStream<'static, Result<ReadBatchFut>>>>,
    /// The scan scheduler for the scan
    scan_scheduler: Arc<ScanScheduler>,
    /// The global metrics for the scan
    metrics: Arc<FilteredReadGlobalMetrics>,
    /// The number of partitions currently running
    ///
    /// We need to know when the final partition completes so that we can
    /// gather the final I/O stats
    active_partitions_counter: Arc<AtomicUsize>,
    /// The threading mode for the scan
    threading_mode: FilteredReadThreadingMode,
    /// Range to apply to the result stream if not already pushed down in planning phase
    scan_range_after_filter: Option<Range<u64>>,
    /// Fragments planned non-empty, and their total planned rows; the output
    /// side uses these to detect take-shaped plans (batch size resolves at
    /// execute time, so the detection lives there too)
    touched_fragments: usize,
    planned_rows: u64,
}

/// Below this many fragments there are too few handoffs to be worth
/// consolidating
const CONSOLIDATE_MIN_FRAGMENTS: usize = 8;

/// Above this per-fragment average, batches are big enough to amortize
/// their handoff
const CONSOLIDATE_MAX_AVG_PLANNED_ROWS_PER_FRAGMENT: u64 = 1024;

/// Pump a take-shaped read on a spawned task, handing the consumer
/// consolidated batches. Inline polling would otherwise execute the
/// per-batch pipeline work on the consumer, which serializes concurrent
/// small reads.
fn consolidated_stream(
    inner: SendableRecordBatchStream,
    target: usize,
) -> SendableRecordBatchStream {
    let mut builder = RecordBatchReceiverStream::builder(inner.schema(), 4);
    let tx = builder.tx();
    builder.spawn(async move {
        let mut stream = coalesce_batches(inner, target).boxed();
        while let Some(item) = stream.next().await {
            if tx.send(item).await.is_err() {
                // Receiver dropped: the query was cancelled
                break;
            }
        }
        Ok(())
    });
    builder.build()
}

/// Merge batches up to `target` rows; batches already at the target pass
/// through whole (never split). Order is preserved.
pub fn coalesce_batches(
    input: SendableRecordBatchStream,
    target: usize,
) -> impl Stream<Item = DataFusionResult<RecordBatch>> {
    struct Coalescer {
        input: SendableRecordBatchStream,
        schema: SchemaRef,
        target: usize,
        buffered: Vec<RecordBatch>,
        buffered_rows: usize,
        exhausted: bool,
    }

    impl Coalescer {
        fn ready_to_emit(&self) -> bool {
            self.buffered_rows >= self.target || (self.exhausted && !self.buffered.is_empty())
        }

        fn buffer(&mut self, batch: RecordBatch) {
            self.buffered_rows += batch.num_rows();
            self.buffered.push(batch);
        }

        fn emit(&mut self) -> DataFusionResult<RecordBatch> {
            self.buffered_rows = 0;
            if self.buffered.len() > 1 {
                let batch = arrow::compute::concat_batches(&self.schema, self.buffered.iter())?;
                self.buffered.clear();
                Ok(batch)
            } else {
                self.buffered.pop().ok_or_else(|| {
                    DataFusionError::Internal(
                        "coalesce_batches emitted with an empty buffer".to_string(),
                    )
                })
            }
        }
    }

    let schema = input.schema();
    let coalescer = Coalescer {
        input,
        schema,
        target,
        buffered: Vec::new(),
        buffered_rows: 0,
        exhausted: false,
    };
    futures::stream::try_unfold(coalescer, |mut this| async move {
        loop {
            if this.ready_to_emit() {
                return Ok(Some((this.emit()?, this)));
            }
            if this.exhausted {
                return Ok(None);
            }
            match this.input.try_next().await? {
                Some(batch) if batch.num_rows() >= this.target && !this.buffered.is_empty() => {
                    // Emit the partial buffer on its own; the large batch
                    // then passes through whole on the next iteration
                    let out = this.emit()?;
                    this.buffer(batch);
                    return Ok(Some((out, this)));
                }
                Some(batch) if batch.num_rows() > 0 => this.buffer(batch),
                Some(_) => {}
                None => this.exhausted = true,
            }
        }
    })
}

impl std::fmt::Debug for FilteredReadStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FilteredReadStream").finish()
    }
}

impl FilteredReadStream {
    /// Create a new FilteredReadStream from a pre-computed internal plan.
    /// Fragment handles are constructed I/O-free from the manifest
    /// descriptors, only for the fragments the plan selects. A `None`
    /// scheduler is created here; the row-stream path injects its per-query
    /// shared one and a per-batch priority offset.
    #[instrument(name = "init_filtered_read_stream", skip_all)]
    fn try_new(
        dataset: Arc<Dataset>,
        options: FilteredReadOptions,
        global_metrics: Arc<FilteredReadGlobalMetrics>,
        plan: FilteredReadInternalPlan,
        scan_scheduler: Option<Arc<ScanScheduler>>,
        priority_offset: Option<u32>,
    ) -> Self {
        let scan_scheduler =
            scan_scheduler.unwrap_or_else(|| Self::make_scan_scheduler(&dataset, &options));
        let threading_mode = options.threading_mode;

        let io_parallelism = dataset.object_store.io_parallelism();
        let fragment_readahead = options
            .fragment_readahead
            .unwrap_or_else(|| (*DEFAULT_FRAGMENT_READAHEAD).unwrap_or(io_parallelism * 2))
            .max(1);

        let fragment_descriptors = options
            .fragments
            .clone()
            .unwrap_or_else(|| dataset.fragments().clone());

        log::debug!(
            "Filtered read on {} fragments with frag_readahead={} and io_parallelism={}",
            fragment_descriptors.len(),
            fragment_readahead,
            io_parallelism
        );

        let output_schema = public_blob_v2_binary_projection_schema(&options.projection);

        // Get scan_range_after_filter from the plan
        let scan_range_after_filter = plan.scan_range_after_filter.clone();

        // Convert plan to scoped fragments for I/O
        let mut scoped_fragments = Self::plan_to_scoped_fragments(
            &plan,
            &fragment_descriptors,
            &dataset,
            &options,
            scan_scheduler.clone(),
        );
        if let Some(priority_offset) = priority_offset.filter(|offset| *offset != 0) {
            for scoped in &mut scoped_fragments {
                scoped.priority = scoped.priority.saturating_add(priority_offset);
            }
        }

        let global_metrics_clone = global_metrics.clone();

        let fragment_streams = futures::stream::iter(scoped_fragments)
            .map({
                let scan_range_after_filter = scan_range_after_filter.clone();
                move |scoped_fragment| {
                    let metrics = global_metrics_clone.clone();
                    let limit = scan_range_after_filter.as_ref().map(|r| r.end);
                    let dataset = dataset.clone();
                    SpawnedTask::spawn(
                        Self::read_fragment(dataset, scoped_fragment, metrics, limit)
                            .in_current_span(),
                    )
                    .map(|thread_result| thread_result.unwrap())
                }
            })
            .buffered(fragment_readahead);
        let task_stream = fragment_streams.try_flatten().boxed();

        // A batch never spans fragments, so a plan touching many fragments
        // with few rows each emits one tiny batch per fragment. Fragments
        // planned empty produce no batch and don't count. Filtered scans
        // stay dense here: their planned rows are a pre-refine upper bound.
        let (touched_fragments, planned_rows) =
            plan.rows
                .values()
                .fold((0usize, 0u64), |(fragments, rows), ranges| {
                    let fragment_rows: u64 =
                        ranges.iter().map(|range| range.end - range.start).sum();
                    if fragment_rows > 0 {
                        (fragments + 1, rows + fragment_rows)
                    } else {
                        (fragments, rows)
                    }
                });
        Self {
            output_schema,
            task_stream: Arc::new(AsyncMutex::new(task_stream)),
            scan_scheduler,
            metrics: global_metrics,
            active_partitions_counter: Arc::new(AtomicUsize::new(0)),
            threading_mode,
            scan_range_after_filter,
            touched_fragments,
            planned_rows,
        }
    }

    /// Drain the entire read into batches (used by the row-stream path,
    /// which is the stream's only consumer and records metrics per batch)
    async fn collect_all(&self, decode_parallelism: usize) -> Result<Vec<RecordBatch>> {
        let mut task_stream = self.task_stream.lock().await;
        (&mut *task_stream)
            .try_buffered(decode_parallelism)
            .try_collect()
            .await
    }

    async fn load_all_fragments(
        dataset: &Arc<Dataset>,
        options: &FilteredReadOptions,
    ) -> Result<Vec<LoadedFragment>> {
        let io_parallelism = dataset.object_store.io_parallelism();
        let fragments = options
            .fragments
            .clone()
            .unwrap_or_else(|| dataset.fragments().clone());
        // Ideally we don't need to collect here but if we don't we get "implementation of FnOnce is
        // not general enough" false positives from rustc
        let frag_futs = fragments
            .iter()
            .map(|frag| {
                Result::Ok(Self::load_fragment(
                    dataset.clone(),
                    frag.clone(),
                    options.with_deleted_rows,
                ))
            })
            .collect::<Vec<_>>();
        futures::stream::iter(frag_futs)
            // Cannot use unordered because we need to populate logical_offset based on user-provided order
            .try_buffered(io_parallelism)
            .try_collect::<Vec<_>>()
            .await
    }

    /// Create the I/O scheduler for a read (explicit option → env override →
    /// max bandwidth)
    fn make_scan_scheduler(dataset: &Dataset, options: &FilteredReadOptions) -> Arc<ScanScheduler> {
        let obj_store = dataset.object_store.clone();
        let scheduler_config = if let Some(io_buffer_size_bytes) = options
            .io_buffer_size_bytes
            .or_else(get_default_io_buffer_size_override)
        {
            SchedulerConfig::new(io_buffer_size_bytes)
        } else {
            SchedulerConfig::max_bandwidth(obj_store.as_ref())
        };
        ScanScheduler::new(obj_store, scheduler_config)
    }

    async fn load_fragment(
        dataset: Arc<Dataset>,
        frag: Fragment,
        include_deleted_rows: bool,
    ) -> Result<LoadedFragment> {
        let file_fragment = FileFragment::new(dataset.clone(), frag.clone());
        let deletion_vector = if include_deleted_rows {
            None
        } else {
            file_fragment.get_deletion_vector().await?
        };

        let num_physical_rows = file_fragment.physical_rows().await? as u64;
        let (row_id_sequence, num_logical_rows) = if dataset.manifest.uses_stable_row_ids() {
            let row_id_sequence = load_row_id_sequence(dataset.as_ref(), &frag).await?;
            let num_logical_rows = row_id_sequence.len();
            (row_id_sequence, num_logical_rows)
        } else {
            let row_ids_start = frag.id << 32;
            let row_ids_end = row_ids_start + num_physical_rows;
            let num_logical_rows = file_fragment.count_rows(None).await? as u64;
            let addrs_as_ids = Arc::new(RowIdSequence::from(row_ids_start..row_ids_end));
            (addrs_as_ids, num_logical_rows)
        };
        Ok(LoadedFragment {
            row_id_sequence,
            fragment: Arc::new(file_fragment),
            num_physical_rows,
            num_logical_rows,
            deletion_vector,
        })
    }

    // This method is a bit complicated
    //
    // We start with a list of fragments, potentially a scalar index result, and a scan range.
    //
    // We need to figure out which ranges to read from each fragment.
    //
    // If the scan range is ignoring the filters we can push it down here.
    // If the scan range is not ignoring the filters we can only push it down if:
    // 1. The index result is an exact match (we know exactly which rows will be in the result)
    // 2. The index result is AtLeast with guaranteed rows >= limit (we have enough guaranteed matches)
    // Returns: FilteredReadInternalPlan
    #[instrument(name = "plan_scan", skip_all)]
    fn plan_scan(
        fragments: &[LoadedFragment],
        evaluated_index: &Option<Arc<EvaluatedIndex>>,
        options: &FilteredReadOptions,
    ) -> FilteredReadInternalPlan {
        // For pushing down scan_range_after_filter.
        //
        // This is only valid when there is no refine filter left to evaluate.  An exact scalar
        // index result is exact for the indexed predicate, but not for the full predicate if a
        // refine filter can still reject rows.
        let can_push_down_scan_range_after_filter = options.refine_filter.is_none();
        let mut scan_planned_with_limit_pushed_down = false;
        let mut to_skip = if can_push_down_scan_range_after_filter {
            options
                .scan_range_after_filter
                .as_ref()
                .map(|r| r.start)
                .unwrap_or(0)
        } else {
            0
        };
        let mut to_take = if can_push_down_scan_range_after_filter {
            options
                .scan_range_after_filter
                .as_ref()
                .map(|r| r.end - r.start)
                .unwrap_or(u64::MAX)
        } else {
            u64::MAX
        };

        // Full fragment ranges to read before applying scan_range_after_filter
        let mut fragments_to_read: BTreeMap<u32, Vec<Range<u64>>> = BTreeMap::new();
        // Fragment ranges to read after applying scan_range_after_filter
        // Adds an extra map because if scan_range_after_filter cannot be fulfilled we need to
        // fall back to read the full fragment in fragments_to_read
        // Used only when index guarantees enough rows to satisfy scan_range_after_filter
        let mut scan_push_down_fragments_to_read: BTreeMap<u32, Vec<Range<u64>>> = BTreeMap::new();

        // The current offset, includes filtered rows, but not deleted rows
        let mut range_offset = 0;
        for LoadedFragment {
            row_id_sequence,
            fragment,
            num_logical_rows,
            num_physical_rows,
            deletion_vector,
        } in fragments.iter()
        {
            if let Some(range_before_filter) = &options.scan_range_before_filter
                && range_offset >= range_before_filter.end
            {
                break;
            }

            let mut to_read: Vec<Range<u64>> =
                Self::full_frag_range(*num_physical_rows, deletion_vector);

            if let Some(range_before_filter) = &options.scan_range_before_filter {
                let range_start = range_offset;
                let range_end = if options.with_deleted_rows {
                    range_offset += num_physical_rows;
                    range_start + num_physical_rows
                } else {
                    range_offset += num_logical_rows;
                    range_start + num_logical_rows
                };
                to_read = Self::trim_ranges(to_read, range_start..range_end, range_before_filter);
                if to_read.is_empty() {
                    continue;
                }
            }

            // Apply index and apply scan range after filter if applicable
            Self::apply_index_to_fragment(
                evaluated_index,
                fragment,
                row_id_sequence,
                to_read,
                &mut to_skip,
                &mut to_take,
                &mut fragments_to_read,
                &mut scan_push_down_fragments_to_read,
                options.only_indexed_fragments,
            );

            if can_push_down_scan_range_after_filter && to_take == 0 {
                scan_planned_with_limit_pushed_down = true;
                fragments_to_read = scan_push_down_fragments_to_read;
                break;
            }
        }

        // Build filters for each fragment
        let mut filters = HashMap::new();
        for fragment in fragments.iter() {
            let fragment_id = fragment.fragment.id() as u32;
            if let Some(to_read) = fragments_to_read.get(&fragment_id) {
                if !to_read.is_empty() {
                    // Resolve filter for this fragment
                    let filter = if let Some(evaluated_index) = evaluated_index {
                        if evaluated_index.applicable_fragments.contains(fragment_id) {
                            let r = &evaluated_index.index_result;
                            // `Exact` results don't need a recheck. `AtLeast`
                            // results can also skip recheck when the
                            // skip/take pushdown is in play (we only read the
                            // guaranteed-match ranges in that case).
                            let can_skip_recheck = r.is_exact()
                                || (r.is_at_least() && scan_planned_with_limit_pushed_down);
                            if can_skip_recheck {
                                options.refine_filter.clone()
                            } else {
                                options.full_filter.clone()
                            }
                        } else {
                            options.full_filter.clone()
                        }
                    } else {
                        options.full_filter.clone()
                    };

                    if let Some(f) = filter {
                        filters.insert(fragment_id, Arc::new(f));
                    }

                    log::trace!(
                        "Planning {} ranges ({} rows) from fragment {} with filter: {:?}",
                        to_read.len(),
                        to_read.iter().map(|r| r.end - r.start).sum::<u64>(),
                        fragment_id,
                        filters.get(&fragment_id)
                    );
                } else {
                    log::trace!(
                        "Skipping fragment {} because it was outside the scan range",
                        fragment_id
                    );
                }
            }
        }

        // If scan_range_after_filter was pushed down, don't include it in the plan
        let scan_range_after_filter = if scan_planned_with_limit_pushed_down {
            None
        } else {
            options.scan_range_after_filter.clone()
        };

        FilteredReadInternalPlan {
            rows: fragments_to_read,
            filters,
            scan_range_after_filter,
        }
    }

    /// Handles are constructed here, I/O-free, only for the fragments the
    /// plan selects; priority is the fragment's position in the candidate
    /// list so a sparse plan keeps the original I/O order.
    fn plan_to_scoped_fragments(
        plan: &FilteredReadInternalPlan,
        fragments: &[Fragment],
        dataset: &Arc<Dataset>,
        options: &FilteredReadOptions,
        scan_scheduler: Arc<ScanScheduler>,
    ) -> Vec<ScopedFragmentRead> {
        let default_batch_size = options.batch_size.unwrap_or_else(|| {
            get_default_batch_size().unwrap_or_else(|| {
                std::cmp::max(
                    dataset.object_store.as_ref().block_size() / 4,
                    BATCH_SIZE_FALLBACK,
                )
            }) as u32
        });
        let projection = Arc::new(options.projection.clone());
        let mut scoped_fragments = Vec::new();

        for (priority, fragment) in fragments.iter().enumerate() {
            let fragment_id = fragment.id as u32;

            // Check if this fragment is in the plan
            if let Some(ranges) = plan.rows.get(&fragment_id) {
                if ranges.is_empty() {
                    continue;
                }

                // Get filter for this fragment (convert Arc<Expr> back to Expr)
                let filter = plan.filters.get(&fragment_id).map(|f| (**f).clone());

                scoped_fragments.push(ScopedFragmentRead {
                    fragment: Arc::new(FileFragment::new(dataset.clone(), fragment.clone())),
                    ranges: ranges.clone(),
                    projection: projection.clone(),
                    with_deleted_rows: options.with_deleted_rows,
                    batch_size: default_batch_size,
                    file_reader_options: options.file_reader_options.clone(),
                    filter,
                    priority: priority as u32,
                    scan_scheduler: scan_scheduler.clone(),
                });
            }
        }

        scoped_fragments
    }

    /// Apply index to a fragment and apply skip/take to matched ranges if possible
    #[allow(clippy::too_many_arguments)]
    fn apply_index_to_fragment(
        evaluated_index: &Option<Arc<EvaluatedIndex>>,
        fragment: &FileFragment,
        row_id_sequence: &Arc<RowIdSequence>,
        to_read: Vec<Range<u64>>,
        to_skip: &mut u64,
        to_take: &mut u64,
        fragments_to_read: &mut BTreeMap<u32, Vec<Range<u64>>>,
        scan_push_down_fragments_to_read: &mut BTreeMap<u32, Vec<Range<u64>>>,
        only_indexed_fragments: bool,
    ) {
        let fragment_id = fragment.id() as u32;

        if let Some(evaluated_index) = evaluated_index {
            if evaluated_index.applicable_fragments.contains(fragment_id) {
                let _span = tracing::span!(tracing::Level::DEBUG, "apply_index_result").entered();

                let index_result = &evaluated_index.index_result;
                if index_result.is_exact() {
                    // lower == upper; either side gives the precise answer.
                    let valid_ranges = row_id_sequence.mask_to_offset_ranges(&index_result.upper);
                    let mut matched_ranges = Self::intersect_ranges(&to_read, &valid_ranges);
                    fragments_to_read.insert(fragment_id, matched_ranges.clone());

                    Self::apply_skip_take_to_ranges(&mut matched_ranges, to_skip, to_take);
                    scan_push_down_fragments_to_read.insert(fragment_id, matched_ranges);
                } else if index_result.is_at_least() {
                    // upper is universe; lower is the guaranteed-match set
                    // used for the skip/take push-down path.
                    let valid_ranges = row_id_sequence.mask_to_offset_ranges(&index_result.lower);
                    let mut guaranteed_ranges = Self::intersect_ranges(&to_read, &valid_ranges);
                    fragments_to_read.insert(fragment_id, guaranteed_ranges.clone());

                    Self::apply_skip_take_to_ranges(&mut guaranteed_ranges, to_skip, to_take);
                    scan_push_down_fragments_to_read.insert(fragment_id, guaranteed_ranges);
                } else {
                    // AtMost or true Refined: read everything in `upper`
                    // and rely on the full-filter recheck for survivors.
                    //
                    // For AtMost the index gives no lower bound, so there's
                    // no skip/take push-down to do. For Refined the lower
                    // bound *is* a guaranteed-match set, but exploiting it
                    // requires per-range filter push-down (different filter
                    // per range within a fragment), which the current plan
                    // doesn't support. The recheck-skip opportunity on the
                    // `lower` portion would also be visible up at the
                    // `can_skip_recheck` block — both are deferred. See
                    // TODO(refined-pushdown).
                    let valid_ranges = row_id_sequence.mask_to_offset_ranges(&index_result.upper);
                    let matched_ranges = Self::intersect_ranges(&to_read, &valid_ranges);
                    fragments_to_read.insert(fragment_id, matched_ranges);
                }
            } else {
                // Fragment not indexed.  Normally we add the full fragment to keep
                // results complete.  Fast search intentionally accepts staleness.
                if !only_indexed_fragments {
                    fragments_to_read.insert(fragment_id, to_read);
                }
            }
        } else if !only_indexed_fragments {
            // No index at all - add full fragment to unindexed_ranges
            fragments_to_read.insert(fragment_id, to_read);
        }
    }

    /// Trim physical ranges to skip `to_skip` rows and take at most `to_take` rows
    fn trim_ranges_by_offset(physical_ranges: &mut Vec<Range<u64>>, to_skip: u64, to_take: u64) {
        let mut skip_remaining = to_skip;
        let mut take_remaining = to_take;
        let mut write_idx = 0;

        for read_idx in 0..physical_ranges.len() {
            if take_remaining == 0 {
                break;
            }
            let range = physical_ranges[read_idx].clone();
            let range_size = range.end - range.start;

            if range_size <= skip_remaining {
                skip_remaining -= range_size;
                continue;
            }

            if skip_remaining == 0 && take_remaining >= range_size {
                physical_ranges[write_idx] = range;
                write_idx += 1;
                take_remaining -= range_size;
                continue;
            }

            let skip_in_range = skip_remaining;
            let available_in_range = range_size.saturating_sub(skip_in_range);
            let take_from_range = available_in_range.min(take_remaining);

            let new_start = range.start + skip_in_range;
            let new_end = new_start + take_from_range;
            physical_ranges[write_idx] = new_start..new_end;
            write_idx += 1;
            skip_remaining = 0;
            take_remaining -= take_from_range;
        }

        physical_ranges.truncate(write_idx);
    }

    /// Intersect two sets of sorted ranges
    fn intersect_ranges(ranges1: &[Range<u64>], ranges2: &[Range<u64>]) -> Vec<Range<u64>> {
        let mut result = Vec::new();
        let mut i = 0;
        let mut j = 0;

        while i < ranges1.len() && j < ranges2.len() {
            let r1 = &ranges1[i];
            let r2 = &ranges2[j];

            // Check for intersection
            let start = r1.start.max(r2.start);
            let end = r1.end.min(r2.end);

            if start < end {
                result.push(start..end);
            }

            // Advance the range that ends first
            if r1.end <= r2.end {
                i += 1;
            } else {
                j += 1;
            }
        }

        result
    }

    /// Apply skip and take to ranges and update the counters
    fn apply_skip_take_to_ranges(
        to_read: &mut Vec<Range<u64>>,
        to_skip: &mut u64,
        to_take: &mut u64,
    ) {
        if *to_take == 0 {
            to_read.clear();
            *to_skip = 0;
            return;
        }
        let original_rows: u64 = to_read.iter().map(|r| r.end - r.start).sum();
        if *to_skip >= original_rows {
            to_read.clear();
            *to_skip -= original_rows;
            return;
        }
        Self::trim_ranges_by_offset(to_read, *to_skip, *to_take);
        let rows_taken: u64 = to_read.iter().map(|r| r.end - r.start).sum();
        *to_skip = 0;
        *to_take = to_take.saturating_sub(rows_taken);
    }

    #[instrument(level = "debug", skip_all)]
    fn full_frag_range(
        num_physical_rows: u64,
        deletion_vector: &Option<Arc<DeletionVector>>,
    ) -> Vec<Range<u64>> {
        if let Some(deletion_vector) = deletion_vector {
            DvToValidRanges::new(
                deletion_vector.to_sorted_iter().map(|pos| pos as u64),
                num_physical_rows,
            )
            .collect()
        } else {
            vec![0..num_physical_rows]
        }
    }

    // Given a logical position and bounds, calculate the number of rows to skip and take
    fn calculate_fetch(
        position: Range<u64>, // position of the fragment in dataset/fragment coordinates
        bounds: &Range<u64>,  // bounds of the scan in dataset/fragment coordinates
    ) -> (u64, u64) {
        // Position:         | --- |
        // Bounds  : | --- |
        // Result  : to_skip = 0, to_take = 0
        //
        // Position: | --- |
        // Bounds  :         | --- |
        // Result  : to_skip = 0, to_take = 0
        //
        // Position: | --- |
        // Bounds  :   | --- |
        // Result  : to_skip > 0, to_take = (position.end - bounds.start)
        //
        // Position:   | --- |
        // Bounds  : | -------- |
        // Result  : to_skip = 0, to_take = (position.end - position.start)
        //
        // Position:   | --- |
        // Bounds  : | --- |
        // Result  : to_skip = 0, to_take = (bounds.end - position.start)
        let to_skip = bounds.start.saturating_sub(position.start);
        let to_take = bounds
            .end
            .min(position.end)
            .saturating_sub(position.start.max(bounds.start));

        // Note: to_skip may be > 0 even if to_take == 0
        (to_skip, to_take)
    }

    #[instrument(level = "debug", skip_all)]
    fn trim_ranges(
        physical_ranges: Vec<Range<u64>>,
        logical_position: Range<u64>,
        bounds: &Range<u64>,
    ) -> Vec<Range<u64>> {
        let num_logical_rows = logical_position.end - logical_position.start;
        let (mut to_skip, mut to_take) = Self::calculate_fetch(logical_position, bounds);

        if to_skip == 0 && to_take == num_logical_rows {
            return physical_ranges;
        }

        let mut trimmed = Vec::with_capacity(physical_ranges.len());
        for range in physical_ranges {
            let range_len = range.end - range.start;
            if to_skip >= range_len {
                to_skip -= range_len;
                continue;
            }
            let avail_here = range_len - to_skip;
            let to_take_here = avail_here.min(to_take);
            to_take -= to_take_here;
            if to_take_here > 0 {
                trimmed.push(range.start + to_skip..range.start + to_skip + to_take_here);
            }
            to_skip = 0;
            if to_take == 0 {
                break;
            }
        }

        trimmed
    }

    // There is one underlying task stream, and it can be shared by as many partitions as we
    // want.
    //
    // The behavior of this method depends on the threading mode.  If the threading mode is
    // `OneThreadedPartition` then this method should only be called once.  We will create a
    // stream with readahead using buffered.
    //
    // If the threading mode is `MultiplePartitions` then this method should be called once per
    // partition.  Each stream will have a copy of the same underlying task stream.  Only one stream
    // can poll the underlying task stream at a time (there is a lock on the task stream).  This is
    // generally fine because grabbing a task is cheap (unless we are waiting on I/O).
    //
    // If the threading mode is `MultiplePartitions` then we may operate on the data out-of-order.
    fn get_stream(
        &self,
        metrics: &ExecutionPlanMetricsSet,
        partition: usize,
    ) -> SendableRecordBatchStream {
        self.active_partitions_counter
            .fetch_add(1, Ordering::Relaxed);

        // Each partition needs these to record incremental metrics.
        let global_metrics = self.metrics.clone();
        let scan_scheduler = self.scan_scheduler.clone();

        let partition_metrics = Arc::new(FilteredReadPartitionMetrics::new(metrics, partition));

        match self.threading_mode {
            FilteredReadThreadingMode::OnePartitionMultipleThreads(num_threads) => {
                assert_eq!(partition, 0);
                let output_schema = self.output_schema.clone();
                let task_stream = self.task_stream.clone();
                let partition_metrics_clone = partition_metrics.clone();
                let futures_stream = futures::stream::try_unfold(task_stream, {
                    move |task_stream| {
                        let partition_metrics = partition_metrics_clone.clone();
                        async move {
                            // There is no compute we can meaningfully measure here.  The actual work is
                            // done by spawned background threads.
                            let _timer =
                                partition_metrics.baseline_metrics.elapsed_compute().timer();
                            let _task_wait_timer = partition_metrics.task_wait_time.timer();
                            let maybe_task = task_stream.lock().await.next().await.transpose()?;
                            Result::Ok(maybe_task.map(|task| (task, task_stream)))
                        }
                    }
                });
                let partition_metrics_clone = partition_metrics.clone();
                let base_batch_stream =
                    futures_stream
                        .try_buffered(num_threads)
                        .try_filter_map(move |batch| {
                            std::future::ready(Ok(if batch.num_rows() == 0 {
                                None
                            } else {
                                Some(batch)
                            }))
                        });

                let batch_stream = if let Some(ref range) = self.scan_range_after_filter {
                    Self::apply_hard_range(base_batch_stream, range.clone()).boxed()
                } else {
                    // Need to box here otherwise the if/else returns incompatible types
                    base_batch_stream.boxed()
                };

                // Clone so the finally handler can record a final snapshot even when
                // no output batches were produced (inspect_ok never fires in that case).
                let global_metrics_final = global_metrics.clone();
                let scan_scheduler_final = scan_scheduler.clone();
                let batch_stream = batch_stream
                    .inspect_ok(move |batch| {
                        partition_metrics_clone
                            .baseline_metrics
                            .record_output(batch.num_rows());
                        global_metrics.io_metrics.record(&scan_scheduler);
                    })
                    .finally(move || {
                        global_metrics_final
                            .io_metrics
                            .record(&scan_scheduler_final);
                        partition_metrics.baseline_metrics.done();
                    })
                    .map_err(|e: lance_core::Error| DataFusionError::External(e.into()))
                    .boxed();

                Box::pin(RecordBatchStreamAdapter::new(output_schema, batch_stream))
            }
            FilteredReadThreadingMode::MultiplePartitions(num_partitions) => {
                assert!(partition < num_partitions);
                let output_schema = self.output_schema.clone();
                let task_stream = self.task_stream.clone();
                let global_metrics_clone = global_metrics.clone();
                let scan_scheduler_clone = scan_scheduler.clone();
                let batch_stream = futures::stream::try_unfold(task_stream, {
                    move |task_stream| {
                        let partition_metrics = partition_metrics.clone();
                        let global_metrics = global_metrics_clone.clone();
                        let scan_scheduler = scan_scheduler_clone.clone();
                        async move {
                            // This isn't quite right.  It's counting I/O time in addition to
                            // compute time.
                            //
                            // TODO: Modify the "read task" concept to have a way of marking when
                            // the 'wait' portion of the task is complete.
                            let _timer =
                                partition_metrics.baseline_metrics.elapsed_compute().timer();
                            let maybe_task = {
                                let _task_wait_timer = partition_metrics.task_wait_time.timer();
                                task_stream.lock().await.next().await
                            };
                            if let Some(task) = maybe_task {
                                let task = task?;
                                let batch = task.await?;
                                partition_metrics
                                    .baseline_metrics
                                    .record_output(batch.num_rows());

                                global_metrics.io_metrics.record(&scan_scheduler);

                                Ok(Some((batch, task_stream)))
                            } else {
                                partition_metrics.baseline_metrics.done();
                                Ok(None)
                            }
                        }
                        .instrument(tracing::debug_span!("filtered_read_task"))
                    }
                })
                .try_filter_map(move |batch| {
                    std::future::ready(Ok(if batch.num_rows() == 0 {
                        None
                    } else {
                        Some(batch)
                    }))
                })
                .map_err(|e: lance_core::Error| DataFusionError::External(e.into()));
                Box::pin(RecordBatchStreamAdapter::new(output_schema, batch_stream))
            }
        }
    }

    // Reads a single fragment into a stream of batch tasks
    #[instrument(name = "read_fragment", level = "debug", skip_all)]
    async fn read_fragment(
        dataset: Arc<Dataset>,
        mut fragment_read_task: ScopedFragmentRead,
        global_metrics: Arc<FilteredReadGlobalMetrics>,
        fragment_soft_limit: Option<u64>,
    ) -> Result<BoxStream<'static, Result<ReadBatchFut>>> {
        let output_schema =
            public_blob_v2_binary_projection_schema(fragment_read_task.projection.as_ref());

        if let Some(filter) = &fragment_read_task.filter {
            let filter_cols = Planner::column_names_in_expr(filter);
            if !filter_cols.is_empty() {
                fragment_read_task.projection = Arc::new(
                    fragment_read_task
                        .projection
                        .as_ref()
                        .clone()
                        .union_columns(filter_cols, OnMissing::Error)?,
                );
            }
        }

        let output_read_schema = Arc::new(fragment_read_task.projection.to_schema());
        let bare_read_schema = fragment_read_task.projection.to_bare_schema();
        let materialize_blob_v2_binary =
            crate::dataset::blob::schema_has_blob_v2_binary_view(&bare_read_schema);
        let read_schema = if materialize_blob_v2_binary {
            crate::dataset::blob::blob_v2_descriptor_schema(&bare_read_schema)
        } else {
            bare_read_schema
        };
        let mut frag_read_config = fragment_read_task.frag_read_config();
        if materialize_blob_v2_binary {
            frag_read_config = frag_read_config.with_row_address(true);
        }
        let mut fragment_reader = fragment_read_task
            .fragment
            .open(&read_schema, frag_read_config)
            .await?;

        if fragment_read_task.with_deleted_rows {
            fragment_reader.with_make_deletions_null();
        }

        // The reader expects sorted ranges and it may be possible to get non-sorted ranges if
        // the row ids are not contiguous
        fragment_read_task.ranges.sort_by_key(|r| r.start);

        let physical_filter = fragment_read_task
            .filter
            .map(|filter| {
                let planner = Planner::new(public_blob_v2_binary_projection_schema(
                    fragment_read_task.projection.as_ref(),
                ));
                planner.create_physical_expr(&filter)
            })
            .transpose()?;

        // We are going to count the fragment as scanned on the first batch we
        // read. This might miss empty fragments, but we assume that wouldn't be
        // used in the scan anyways.
        let fragment_counted = Arc::new(AtomicBool::new(false));
        let range_tracker = Arc::new(Mutex::new(RangeMetricsTracker::new(
            fragment_read_task.ranges.clone(),
        )));

        let fragment_stream = fragment_reader
            .read_ranges(
                fragment_read_task.ranges.into(),
                fragment_read_task.batch_size,
            )
            .await?
            .map(move |batch_fut: ReadBatchFut| {
                let global_metrics = global_metrics.clone();
                let fragment_counted = fragment_counted.clone();
                let range_tracker = range_tracker.clone();
                let batch_fut = batch_fut
                    .inspect_ok(move |batch| {
                        let num_rows = batch.num_rows();
                        global_metrics.rows_scanned.add(num_rows);
                        if !fragment_counted.swap(true, Ordering::Relaxed) {
                            global_metrics.fragments_scanned.add(1);
                        }
                        // Note: this is an approximation. Batches may come in out-of-order,
                        // in which case this might be inaccurate.
                        if let Ok(mut range_tracker) = range_tracker.lock() {
                            let additional_ranges =
                                range_tracker.incremental_ranges_scanned(num_rows);
                            global_metrics.ranges_scanned.add(additional_ranges);
                        }
                    })
                    .boxed();
                if materialize_blob_v2_binary {
                    let dataset = dataset.clone();
                    let output_read_schema = output_read_schema.clone();
                    batch_fut
                        .and_then(move |batch| async move {
                            crate::dataset::blob::materialize_blob_v2_binary_batch(
                                &dataset,
                                output_read_schema.as_ref(),
                                batch,
                            )
                            .await
                        })
                        .boxed()
                } else {
                    batch_fut
                }
            })
            .zip(futures::stream::repeat((
                physical_filter.clone(),
                output_schema.clone(),
            )))
            .map(|(batch_fut, args)| Self::wrap_with_filter(batch_fut, args.0, args.1));

        let result = if let Some(limit) = fragment_soft_limit {
            Self::apply_soft_limit(fragment_stream, limit).boxed()
        } else {
            fragment_stream.boxed()
        };
        Ok(result)
    }

    fn wrap_with_filter(
        batch_fut: ReadBatchFut,
        filter: Option<Arc<dyn PhysicalExpr>>,
        output_schema: SchemaRef,
    ) -> Result<ReadBatchFut> {
        if let Some(filter) = filter {
            Ok(batch_fut
                .map(move |batch| {
                    let batch = batch?;
                    let batch = datafusion_physical_plan::filter::batch_filter(&batch, &filter)
                        .map_err(|e| {
                            Error::execution(format!(
                                "Error applying filter expression to batch: {e}"
                            ))
                        })?;
                    // Drop any fields loaded purely for the purpose of applying the filter
                    Ok(batch.project_by_schema(output_schema.as_ref())?)
                })
                .boxed())
        } else {
            Ok(batch_fut)
        }
    }

    fn apply_soft_limit<S>(stream: S, limit: u64) -> impl Stream<Item = Result<ReadBatchFut>>
    where
        S: Stream<Item = Result<ReadBatchFut>>,
    {
        let rows_read = Arc::new(AtomicUsize::new(0));

        stream
            .take_while({
                let rows_read = rows_read.clone();
                move |_| future::ready(rows_read.load(Ordering::Relaxed) < limit as usize)
            })
            .map(move |batch_fut_result| {
                let rows_read = rows_read.clone();
                batch_fut_result.map(move |batch_fut| {
                    batch_fut
                        .map(move |batch_result| {
                            batch_result.inspect(|batch| {
                                let batch_rows = batch.num_rows();
                                rows_read.fetch_add(batch_rows, Ordering::Relaxed);
                            })
                        })
                        .boxed()
                })
            })
    }

    fn apply_hard_range<S>(stream: S, range: Range<u64>) -> impl Stream<Item = Result<RecordBatch>>
    where
        S: Stream<Item = Result<RecordBatch>>,
    {
        let start = range.start as usize;
        let end = range.end as usize;
        let rows_seen = Arc::new(AtomicUsize::new(0));
        let rows_seen_clone = rows_seen.clone();

        stream
            .take_while(move |_| {
                let rows_seen = rows_seen.load(Ordering::Relaxed);
                future::ready(rows_seen <= end)
            })
            .try_filter_map(move |batch| {
                if batch.num_rows() == 0 {
                    return future::ready(Ok(None));
                }

                let batch_rows = batch.num_rows();
                let current_position = rows_seen_clone.fetch_add(batch_rows, Ordering::Relaxed);
                let batch_end = current_position + batch_rows;

                if batch_end <= start || current_position >= end {
                    return future::ready(Ok(None));
                }

                let skip = start.saturating_sub(current_position);
                let end_pos = (end - current_position).min(batch_rows);
                let take = end_pos.saturating_sub(skip);

                if take == 0 {
                    return future::ready(Ok(None));
                }

                let result = if skip == 0 && take == batch_rows {
                    batch
                } else {
                    batch.slice(skip, take)
                };
                future::ready(Ok(Some(result)))
            })
    }
}

/// Options for a filtered read.
#[derive(Debug, Clone)]
pub struct FilteredReadOptions {
    /// The range of rows to read before applying the filter.
    pub scan_range_before_filter: Option<Range<u64>>,
    /// The range of rows to read after applying the filter.
    pub scan_range_after_filter: Option<Range<u64>>,
    /// Include deleted rows in the scan; they are returned with a null row id
    pub with_deleted_rows: bool,
    /// The maximum number of rows per batch
    pub batch_size: Option<u32>,
    /// File reader options to use when reading data files.
    pub file_reader_options: Option<FileReaderOptions>,
    /// Controls how many fragments to read ahead
    pub fragment_readahead: Option<usize>,
    /// The fragments to read
    pub fragments: Option<Arc<Vec<Fragment>>>,
    /// The projection to use for the scan
    pub projection: Projection,
    /// If there is a scalar index input, and the index result we get from that input is exact,
    /// then we will only apply the refine filter to batches covered by the result.
    pub refine_filter: Option<Expr>,
    /// The filter to apply during the read.  If possible we will try and use the scalar index
    /// result to avoid applying this (and instead only apply the refine filter) but in some cases
    /// the index result does not cover all fragments or is not exact.
    pub full_filter: Option<Expr>,
    /// The threading mode to use for the scan
    pub threading_mode: FilteredReadThreadingMode,
    /// The size of the I/O buffer to use for the scan
    pub io_buffer_size_bytes: Option<u64>,
    /// If true, skip fragments that are not covered by the scalar index result.
    pub only_indexed_fragments: bool,
    /// Row addresses whose index entries may be stale because an overlay committed after the
    /// index was built touches an indexed field. They are blocked from the index result so the
    /// index never emits them; the scanner re-evaluates just these rows against their current
    /// (overlay-merged) values on a targeted take path. Their fragments stay in the covered set,
    /// so non-stale rows keep the index. `None` on the common no-overlay fast path.
    pub overlay_block: Option<RowAddrMask>,
}

impl FilteredReadOptions {
    /// Create a basic full scan of the dataset
    ///
    /// This will read all data, without any filters, and will read all
    /// columns (but not the row id or row address).  Deleted rows will
    /// not be included and the default batch size will be used.
    ///
    /// This is the default behavior and you can use the various builder
    /// methods on this type to modify the behavior.
    pub fn basic_full_read(dataset: &Arc<Dataset>) -> Self {
        Self::new(dataset.full_projection())
    }

    pub fn new(projection: Projection) -> Self {
        Self {
            scan_range_before_filter: None,
            scan_range_after_filter: None,
            with_deleted_rows: false,
            batch_size: None,
            file_reader_options: None,
            fragment_readahead: None,
            fragments: None,
            projection,
            refine_filter: None,
            full_filter: None,
            io_buffer_size_bytes: None,
            only_indexed_fragments: false,
            overlay_block: None,
            threading_mode: FilteredReadThreadingMode::OnePartitionMultipleThreads(
                get_num_compute_intensive_cpus(),
            ),
        }
    }

    /// Block the given stale overlay row addresses (see the `overlay_block` field) from the
    /// scalar index result so the index never emits them.
    pub fn with_overlay_block(mut self, block: RowAddrMask) -> Self {
        self.overlay_block = Some(block);
        self
    }

    /// Include deleted rows in the scan
    ///
    /// This is currently only supported if there is no scan_range specified
    ///
    /// The projection will be updated to always include the row id column.  The
    /// row id column will be null for all deleted rows.
    ///
    /// This function only materializes deleted rows that are masked by a deletion
    /// vector.  If the deleted row has been materialized via compaction, or if an
    /// entire fragment was deleted, it will not be read by this function.
    pub fn with_deleted_rows(mut self) -> Result<Self> {
        if self.scan_range_before_filter.is_some() || self.scan_range_after_filter.is_some() {
            return Err(Error::invalid_input_source(
                "with_deleted_rows is not supported when there is a scan range".into(),
            ));
        }
        self.with_deleted_rows = true;
        Ok(self)
    }

    /// Specify the range of rows to read before applying the filter.
    ///
    /// This can be used to pushdown a limit/offset when there is no filter.
    ///
    /// It's also possible to specify this when there is a filter, in order to only scan
    /// a subset of the data (and apply the filter on this subset).  For example, if the
    /// data as a column `count` that steps from 0 to 1000 and the filter is `count > 200`
    /// and the range is 100..300, then scan will read rows 100..300 and return rows 200..300
    pub fn with_scan_range_before_filter(mut self, scan_range: Range<u64>) -> Result<Self> {
        if self.with_deleted_rows {
            return Err(Error::invalid_input_source(
                "with_deleted_rows is not supported when there is a scan range".into(),
            ));
        }
        self.scan_range_before_filter = Some(scan_range);
        Ok(self)
    }

    /// The range of rows to read after applying the filter.
    ///
    /// In many cases we are not able to push this down and the range will be applied after-the-fact.
    ///
    /// However, if there is a scalar index on the column, and that scalar index returns an exact
    /// match, then we can use this to skip reading the data entirely.
    ///
    /// We currently do not support setting this when there is more than one partition.
    pub fn with_scan_range_after_filter(mut self, scan_range: Range<u64>) -> Result<Self> {
        if self.with_deleted_rows {
            return Err(Error::invalid_input_source(
                "with_deleted_rows is not supported when there is a scan range".into(),
            ));
        }
        self.scan_range_after_filter = Some(scan_range);
        Ok(self)
    }

    /// Specify the fragments to read.
    ///
    /// Scan results will be returned in the order of the fragments given here.
    pub fn with_fragments(mut self, fragments: Arc<Vec<Fragment>>) -> Self {
        self.fragments = Some(fragments);
        self
    }

    /// Specify the batch size to use for the read
    ///
    /// This will be a maximum number of rows per batch.  It is possible for batches to be smaller
    /// either due to filtering or because we have reached the end of a fragment (we do not combine
    /// batches across fragments).
    ///
    /// A CoalesceBatchesExec can (and often should) be used to merge together tiny batches
    pub fn with_batch_size(mut self, batch_size: u32) -> Self {
        self.batch_size = Some(batch_size);
        self
    }

    /// Specify the file reader options to use when reading data files.
    pub fn with_file_reader_options(mut self, file_reader_options: FileReaderOptions) -> Self {
        self.file_reader_options = Some(file_reader_options);
        self
    }

    /// Controls how many fragments to read ahead.
    ///
    /// If not set, the default will be 2 * the I/O parallelism.  Generally, reading ahead
    /// in fragments is very cheap.  We will accumulate more I/O requests but these are very tiny.
    /// This has no significant impact on the RAM cost of the scan.  Backpressure is handled by the
    /// scheduler.
    pub fn with_fragment_readahead(mut self, fragment_readahead: usize) -> Self {
        self.fragment_readahead = Some(fragment_readahead);
        self
    }

    /// Specify the filter plan to use for the scan.
    ///
    /// This consists of up to two filters.  The full filter is the filter that needs to be satisfied
    /// by the read.
    ///
    /// The refine filter is a smaller filter that is applied to batches that have exact matches from the
    /// index search.  Since these batches matched the index exactly we already know some predicates about
    /// the rows in the batch and may not have to apply the full filter.
    ///
    /// If the full_filter is None then the refine_filter must be None.
    ///
    /// If the full_filter is Some and the refine_filter is None then that means the filter is completely
    /// satisfied by the index search.  If we get an exact match from the index search we can skip filtering
    /// entirely.
    pub fn with_filter(
        mut self,
        refine_filter: Option<Expr>,
        full_filter: Option<Expr>,
    ) -> Result<Self> {
        if refine_filter.is_some() && full_filter.is_none() {
            return Err(Error::invalid_input_source(
                "refine_filter is set but full_filter is not".into(),
            ));
        }
        self.refine_filter = refine_filter;
        self.full_filter = full_filter;
        Ok(self)
    }

    /// An alternative to [`Self::with_filter`] to set the filters from a FilterPlan if you already have one
    pub fn with_filter_plan(mut self, filter_plan: FilterPlan) -> Self {
        self.refine_filter = filter_plan.refine_expr;
        self.full_filter = filter_plan.full_expr;
        self
    }

    /// Specify the projection to use for the scan
    ///
    /// If the row id or row address are requested then they will be placed at the end
    /// of the output schema.  If both are requested then the row id will come before
    /// the row address.
    pub fn with_projection(mut self, projection: Projection) -> Self {
        self.projection = projection;
        self
    }

    /// Specify the size of the I/O buffer (in bytes) to use for the scan
    ///
    /// See [`crate::dataset::scanner::Scanner::io_buffer_size`] for more details.
    pub fn with_io_buffer_size(mut self, io_buffer_size: u64) -> Self {
        self.io_buffer_size_bytes = Some(io_buffer_size);
        self
    }

    /// Only read fragments covered by a scalar index result.
    pub fn with_only_indexed_fragments(mut self) -> Self {
        self.only_indexed_fragments = true;
        self
    }

    /// Specify the threading mode to use for the scan.
    ///
    /// This controls how decode work is parallelized.  For the default single-partition
    /// scan, the parameter of [`FilteredReadThreadingMode::OnePartitionMultipleThreads`]
    /// bounds how many batch-decode tasks are buffered in flight (via `try_buffered`).
    ///
    /// The parallelism must be greater than 0.  A value of 0 is rejected by
    /// [`FilteredReadExec::try_new`].
    pub fn with_threading_mode(mut self, threading_mode: FilteredReadThreadingMode) -> Self {
        self.threading_mode = threading_mode;
        self
    }
}

/// A plan node that reads a dataset, applying an optional filter and projection.
///
/// This node may execute a scan or it may execute a take.  By default, it picks the best
/// approach based the expected query cost which is determined by:
///  - Size of data in desired columns
///  - Number of rows matching the index search
///  - Filesystem parameters (e.g. block size)
///
/// This decision is made during execution, after the index search is complete, and not during
/// planning.
///
/// In the future, we may introduce high-level cardinality statistics similar to those used by query
/// engines like Postgres.  This might allow us to know, without executing an index search, that a scan
/// would be better.  In that case we accept the force_scan hint to skip the index search.
#[derive(Debug)]
pub struct FilteredReadExec {
    dataset: Arc<Dataset>,
    options: FilteredReadOptions,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
    input: RowSelector,
    // Precomputed internal plan
    plan: Arc<OnceCell<FilteredReadInternalPlan>>,
    // When execute is first called we will initialize the FilteredReadStream.  In order to support
    // multiple partitions, each partition will share the stream.
    running_stream: Arc<AsyncMutex<Option<FilteredReadStream>>>,
}

/// Describes which rows a [`FilteredReadExec`] should read
#[derive(Debug)]
enum RowSelector {
    /// Every live row of the dataset (no input plan)
    AllRows,
    /// A set of rows: one serialized [`IndexExprResult`] batch.  Output is in
    /// storage order and deduplicated.
    RowSet(Arc<dyn ExecutionPlan>),
    /// A stream of rows: record batches with a `_rowid`/`_rowaddr` column
    /// and other payload columns (just carried)
    RowStream(Arc<RowStreamSource>),
}

impl RowSelector {
    fn row_set_plan(&self) -> Option<&Arc<dyn ExecutionPlan>> {
        match self {
            Self::RowSet(plan) => Some(plan),
            _ => None,
        }
    }

    fn child(&self) -> Option<&Arc<dyn ExecutionPlan>> {
        match self {
            Self::AllRows => None,
            Self::RowSet(plan) => Some(plan),
            Self::RowStream(source) => Some(&source.plan),
        }
    }
}

/// State derived at construction for a row-stream source
#[derive(Debug)]
struct RowStreamSource {
    plan: Arc<dyn ExecutionPlan>,
    /// The stream column identifying rows: [`ROW_ID`] or [`ROW_ADDR`]
    key_column: &'static str,
    /// Options for the internal fragment read; carries the projection that
    /// reflects the actual columns to read (plus the alignment key column)
    read_options: FilteredReadOptions,
    /// The schema for newly read columns
    new_fields_schema: SchemaRef,
}

/// Public plan for distributed execution - uses bitmap for flexibility
#[derive(Clone)]
pub struct FilteredReadPlan {
    /// What fragments and physical rows to read
    pub rows: RowAddrTreeMap,
    /// Filter to apply per fragment
    /// fragments not here don't need filtering
    pub filters: HashMap<u32, Arc<Expr>>,
    /// Row offset range to apply after filtering (skip N rows, take M rows).
    /// If the index guarantees enough matching rows, this is pushed down during planning
    /// and set to None. Otherwise, it's applied during execution.
    pub scan_range_after_filter: Option<Range<u64>>,
}

/// Internal plan representation - uses ranges for efficiency in local execution
/// This avoids expensive range↔bitmap conversion
#[derive(Clone, Debug)]
struct FilteredReadInternalPlan {
    /// Fragment ID to ranges to read (BTreeMap for deterministic order with scan_range_after_filter)
    rows: BTreeMap<u32, Vec<Range<u64>>>,
    /// Filter to apply per fragment (fragments not here don't need filtering)
    filters: HashMap<u32, Arc<Expr>>,
    /// Row offset range to apply after filtering (skip N rows, take M rows).
    /// If the index guarantees enough matching rows, this is pushed down during planning
    /// and set to None. Otherwise, it's applied during execution.
    scan_range_after_filter: Option<Range<u64>>,
}

impl FilteredReadInternalPlan {
    /// Convert internal plan (ranges) to external plan (bitmap) for distributed execution
    fn to_external_plan(&self) -> FilteredReadPlan {
        let mut rows = RowAddrTreeMap::new();
        for (fragment_id, ranges) in &self.rows {
            if !ranges.is_empty() {
                rows.insert_bitmap(*fragment_id, ranges_to_bitmap(ranges, true));
            }
        }
        FilteredReadPlan {
            rows,
            filters: self.filters.clone(),
            scan_range_after_filter: self.scan_range_after_filter.clone(),
        }
    }
}

impl FilteredReadExec {
    /// Create a new filtered read
    pub fn try_new(
        dataset: Arc<Dataset>,
        options: FilteredReadOptions,
        input: Option<Arc<dyn ExecutionPlan>>,
    ) -> Result<Self> {
        match input {
            Some(input) if Self::is_index_query_schema(input.schema().as_ref()) => {
                Self::try_new_scan(dataset, options, Some(input))
            }
            Some(input) => Self::try_new_row_stream(dataset, options, input),
            None => Self::try_new_scan(dataset, options, None),
        }
    }

    /// Whether `schema` is one of the serialized [`IndexExprResult`] wire
    /// layouts (see [`IndexExprResultWireFormat`])
    fn is_index_query_schema(schema: &arrow_schema::Schema) -> bool {
        [
            IndexExprResultWireFormat::TwoMask,
            IndexExprResultWireFormat::ThreeVariant,
        ]
        .iter()
        .any(|format| schema.fields() == format.schema().fields())
    }

    /// The input columns that carry through to the output: identity columns
    /// appear iff their flag is requested, ordinary columns always carry
    fn carried_schema(input_schema: &arrow_schema::Schema, projection: &Projection) -> SchemaRef {
        Arc::new(arrow_schema::Schema::new(
            input_schema
                .fields()
                .iter()
                .filter(|f| {
                    (f.name() != ROW_ID || projection.with_row_id)
                        && (f.name() != ROW_ADDR || projection.with_row_addr)
                })
                .cloned()
                .collect::<Vec<_>>(),
        ))
    }

    /// Construct a read over a row-stream source
    fn try_new_row_stream(
        dataset: Arc<Dataset>,
        options: FilteredReadOptions,
        input: Arc<dyn ExecutionPlan>,
    ) -> Result<Self> {
        if dataset.is_legacy_storage() {
            return Err(Error::not_supported_source(
                "taking rows through FilteredReadExec requires the v2 storage format"
                    .to_string()
                    .into(),
            ));
        }
        if options.refine_filter.is_some() || options.full_filter.is_some() {
            return Err(Error::invalid_input_source(
                "filters are not supported when taking rows from an input plan".into(),
            ));
        }
        // A limit is safer to apply upstream, on the cheap keyed rows
        if options.scan_range_before_filter.is_some() || options.scan_range_after_filter.is_some() {
            return Err(Error::invalid_input_source(
                "scan ranges are not supported when taking rows from an input plan".into(),
            ));
        }
        // Row-stream reads do not support deleted rows yet; deleted rows are
        // excluded from the output by default
        if options.with_deleted_rows || options.only_indexed_fragments {
            return Err(Error::invalid_input_source(
                "with_deleted_rows / only_indexed_fragments are not supported when taking rows from an input plan".into(),
            ));
        }
        let input_schema = input.schema();
        let key_column = if input_schema.column_with_name(ROW_ID).is_some() {
            ROW_ID
        } else if input_schema.column_with_name(ROW_ADDR).is_some() {
            ROW_ADDR
        } else {
            return Err(Error::invalid_input_source(
                format!(
                    "a row-stream input plan must have a column named '{}' or '{}'",
                    ROW_ADDR, ROW_ID
                )
                .into(),
            ));
        };

        let fields_to_read = options
            .projection
            .clone()
            .subtract_arrow_schema(input_schema.as_ref(), OnMissing::Ignore)?;
        let synthesize_row_id = fields_to_read.with_row_id;
        let synthesize_row_addr = fields_to_read.with_row_addr;
        if !fields_to_read.has_data_fields() && !synthesize_row_id && !synthesize_row_addr {
            return Err(Error::invalid_input_source(
                "the input plan already contains every projected field; there is nothing to read"
                    .into(),
            ));
        }

        let carried_schema = Self::carried_schema(input_schema.as_ref(), &options.projection);

        // Output = carried columns ⊕ fetched fields ⊕ synthesized identity
        let output_schema = Arc::new(arrow_schema::Schema::from(
            &super::TakeExec::calculate_output_schema(
                dataset.schema(),
                carried_schema.as_ref(),
                &fields_to_read,
            ),
        ));

        // Partitioning and emission behavior follow the input
        let properties = Arc::new(
            input
                .properties()
                .as_ref()
                .clone()
                .with_eq_properties(EquivalenceProperties::new(output_schema)),
        );

        let bare_schema = arrow_schema::Schema::from(&fields_to_read.to_bare_schema());
        let mut new_fields = bare_schema.fields().iter().cloned().collect::<Vec<_>>();
        if synthesize_row_id {
            new_fields.push(Arc::new(ROW_ID_FIELD.clone()));
        }
        if synthesize_row_addr {
            new_fields.push(Arc::new(ROW_ADDR_FIELD.clone()));
        }
        let new_fields_schema = Arc::new(arrow_schema::Schema::new(new_fields));

        // fields_to_read keeps the synthesis flags; add the key column on top
        let mut read_options = options.clone();
        read_options.projection = if key_column == ROW_ID {
            fields_to_read.with_row_id()
        } else {
            fields_to_read.with_row_addr()
        };

        Ok(Self {
            dataset,
            options,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
            input: RowSelector::RowStream(Arc::new(RowStreamSource {
                plan: input,
                key_column,
                read_options,
                new_fields_schema,
            })),
            plan: Arc::new(OnceCell::new()),
            running_stream: Arc::new(AsyncMutex::new(None)),
        })
    }

    fn try_new_scan(
        dataset: Arc<Dataset>,
        mut options: FilteredReadOptions,
        index_input: Option<Arc<dyn ExecutionPlan>>,
    ) -> Result<Self> {
        let input = match index_input {
            Some(plan) => RowSelector::RowSet(plan),
            None => RowSelector::AllRows,
        };
        if options.with_deleted_rows {
            // Ensure we have the row id column if with_deleted_rows is set
            options.projection = options.projection.with_row_id();
        }

        if options.projection.is_empty() {
            return Err(Error::invalid_input_source("no columns were selected and with_row_id / with_row_address is false, there is nothing to scan"
                .into()));
        }

        // A parallelism of 0 would cause `try_buffered(0)` to hang forever instead of erroring
        match options.threading_mode {
            FilteredReadThreadingMode::OnePartitionMultipleThreads(0) => {
                return Err(Error::invalid_input_source(
                    "FilteredReadThreadingMode::OnePartitionMultipleThreads must be greater than 0, got 0"
                        .into(),
                ));
            }
            FilteredReadThreadingMode::MultiplePartitions(0) => {
                return Err(Error::invalid_input_source(
                    "FilteredReadThreadingMode::MultiplePartitions must be greater than 0, got 0"
                        .into(),
                ));
            }
            _ => {}
        }

        if options.scan_range_after_filter.is_some() {
            // Validate that there's a filter when using scan_range_after_filter
            if options.full_filter.is_none()
                && options.refine_filter.is_none()
                && input.row_set_plan().is_none()
            {
                return Err(Error::invalid_input_source("scan_range_after_filter requires a filter to be applied. Use scan_range_before_filter for unfiltered scans."
                    .into()));
            }

            // TODO: support multi partition
            if matches!(
                options.threading_mode,
                FilteredReadThreadingMode::MultiplePartitions(_)
            ) {
                return Err(Error::not_supported_source(
                    "scan_range_after_filter not yet supported with multiple partitions"
                        .to_string()
                        .into(),
                ));
            }
        }
        let output_schema = public_blob_v2_binary_projection_schema(&options.projection);
        let num_partitions = match options.threading_mode {
            FilteredReadThreadingMode::OnePartitionMultipleThreads(_) => 1,
            FilteredReadThreadingMode::MultiplePartitions(n) => n,
        };

        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(output_schema),
            Partitioning::RoundRobinBatch(num_partitions),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));

        let metrics = ExecutionPlanMetricsSet::new();

        Ok(Self {
            dataset,
            options,
            properties,
            running_stream: Arc::new(AsyncMutex::new(None)),
            metrics,
            input,
            plan: Arc::new(OnceCell::new()),
        })
    }

    /// Set the pre-computed plan for execution
    pub async fn with_plan(self, plan: FilteredReadPlan) -> Result<Self> {
        let mut rows = BTreeMap::new();
        for (fragment_id, selection) in plan.rows.iter() {
            let ranges = match selection {
                RowAddrSelection::Partial(bitmap) => bitmap_to_ranges(bitmap),
                RowAddrSelection::Full => {
                    let fragment = self
                        .dataset
                        .get_fragment(*fragment_id as usize)
                        .ok_or_else(|| {
                            Error::invalid_input_source(
                                format!("Fragment {} not found", fragment_id).into(),
                            )
                        })?;
                    let num_rows = fragment.physical_rows().await?;
                    vec![0..num_rows as u64]
                }
            };
            if !ranges.is_empty() {
                rows.insert(*fragment_id, ranges);
            }
        }
        let internal_plan = FilteredReadInternalPlan {
            rows,
            filters: plan.filters,
            scan_range_after_filter: plan.scan_range_after_filter,
        };
        let plan_cell = Arc::new(OnceCell::new());
        let _ = plan_cell.set(internal_plan);
        Ok(Self {
            plan: plan_cell,
            ..self
        })
    }

    /// Get or create the internal plan
    async fn get_or_create_plan_impl<'a>(
        plan_cell: &'a OnceCell<FilteredReadInternalPlan>,
        dataset: Arc<Dataset>,
        options: &FilteredReadOptions,
        index_input: Option<&Arc<dyn ExecutionPlan>>,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> Result<&'a FilteredReadInternalPlan> {
        plan_cell
            .get_or_try_init(|| async {
                // Execute index if present
                let mut evaluated_index = None;
                if let Some(index_input) = index_input {
                    let mut index_search = index_input.execute(partition, ctx)?;
                    let index_search_result = index_search.next().await.ok_or_else(|| {
                        Error::internal("Index search did not yield any results".to_string())
                    })??;
                    let mut idx = EvaluatedIndex::try_from_arrow(&index_search_result)?;
                    // `overlay_block` is always constructed as a block list (see
                    // `Scanner::stale_rows_block_mask`), so `block_list()` is always `Some`.
                    if let Some(block_list) =
                        options.overlay_block.as_ref().and_then(|b| b.block_list())
                    {
                        idx = idx.without_rows(block_list);
                    }
                    evaluated_index = Some(Arc::new(idx));
                }

                // Load fragments to compute the plan
                let io_parallelism = dataset.object_store.io_parallelism();
                let fragments = options
                    .fragments
                    .clone()
                    .unwrap_or_else(|| dataset.fragments().clone());

                let with_deleted_rows = options.with_deleted_rows;
                let frag_futs = fragments
                    .iter()
                    .map(|frag| {
                        Result::Ok(FilteredReadStream::load_fragment(
                            dataset.clone(),
                            frag.clone(),
                            with_deleted_rows,
                        ))
                    })
                    .collect::<Vec<_>>();
                let loaded_fragments = futures::stream::iter(frag_futs)
                    .try_buffered(io_parallelism)
                    .try_collect::<Vec<_>>()
                    .await?;

                // Plan the scan; the metadata loaded here drops when planning
                // finishes — stream construction rebuilds I/O-free handles
                // from the manifest descriptors
                Ok(FilteredReadStream::plan_scan(
                    &loaded_fragments,
                    &evaluated_index,
                    options,
                ))
            })
            .await
    }

    /// Get the existing plan or create it if it doesn't exist
    pub async fn get_or_create_plan(&self, ctx: Arc<TaskContext>) -> Result<FilteredReadPlan> {
        if self.row_stream_input().is_some() {
            return Err(Error::not_supported_source(
                "a FilteredReadExec with a row-stream source does not have a precomputable plan"
                    .to_string()
                    .into(),
            ));
        }
        let internal_plan = Self::get_or_create_plan_impl(
            &self.plan,
            self.dataset.clone(),
            &self.options,
            self.input.row_set_plan(),
            0,
            ctx,
        )
        .await?;
        Ok(internal_plan.to_external_plan())
    }

    fn obtain_stream(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> SendableRecordBatchStream {
        // There are two subtleties here:
        //
        // First, we need to defer execution until first polled (hence the once/flatten)
        //
        // Second, multiple partitions all share the same underlying task stream (see get_stream)
        let running_stream_lock = self.running_stream.clone();
        let dataset = self.dataset.clone();
        let target_partitions = context.session_config().target_partitions();
        let mut options = self.options.clone();
        if let FilteredReadThreadingMode::OnePartitionMultipleThreads(n) = options.threading_mode {
            options.threading_mode = FilteredReadThreadingMode::OnePartitionMultipleThreads(
                n.min(target_partitions).max(1),
            );
        }
        let batch_size_rows = options.batch_size;
        let batch_size_bytes = options
            .file_reader_options
            .as_ref()
            .and_then(|o| o.batch_size_bytes);
        let metrics = self.metrics.clone();
        let index_input = self.input.row_set_plan().cloned();
        let plan_cell = self.plan.clone();

        let stream = futures::stream::once(async move {
            let mut running_stream = running_stream_lock.lock().await;
            let inner = if let Some(running_stream) = &*running_stream {
                running_stream.get_stream(&metrics, partition)
            } else {
                let plan = Self::get_or_create_plan_impl(
                    &plan_cell,
                    dataset.clone(),
                    &options,
                    index_input.as_ref(),
                    partition,
                    context.clone(),
                )
                .await
                .map_err(|e| DataFusionError::External(e.into()))?;
                let new_running_stream = FilteredReadStream::try_new(
                    dataset,
                    options,
                    Arc::new(FilteredReadGlobalMetrics::new(&metrics)),
                    plan.clone(),
                    None,
                    None,
                );
                let first_stream = new_running_stream.get_stream(&metrics, partition);
                *running_stream = Some(new_running_stream);
                first_stream
            };
            // Only masked reads consolidate; plain scans keep their batch
            // boundaries, and the byte-based rechunk merges on its own
            let consolidate = if index_input.is_some() && batch_size_bytes.is_none() {
                running_stream.as_ref().and_then(|running| {
                    // Explicit option → lance env default → session batch size
                    let batch_target_rows = batch_size_rows
                        .map(|batch_size| batch_size as usize)
                        .or_else(get_default_batch_size)
                        .unwrap_or_else(|| context.session_config().batch_size());
                    let is_sparse_plan = batch_target_rows > 0
                        && running.touched_fragments >= CONSOLIDATE_MIN_FRAGMENTS
                        && running.planned_rows
                            < running.touched_fragments as u64
                                * CONSOLIDATE_MAX_AVG_PLANNED_ROWS_PER_FRAGMENT;
                    is_sparse_plan.then_some(batch_target_rows)
                })
            } else {
                None
            };
            drop(running_stream);

            let stream = match (consolidate, batch_size_bytes) {
                (Some(target), _) => consolidated_stream(inner, target),
                (None, Some(bytes)) => {
                    let schema = inner.schema();
                    Box::pin(RecordBatchStreamAdapter::new(
                        schema.clone(),
                        lance_arrow::stream::rechunk_stream_by_size(
                            inner,
                            schema,
                            0,
                            bytes as usize,
                        ),
                    ))
                }
                (None, None) => inner,
            };
            DataFusionResult::<SendableRecordBatchStream>::Ok(stream)
        })
        .try_flatten();

        Box::pin(RecordBatchStreamAdapter::new(self.schema(), stream))
    }

    pub fn dataset(&self) -> &Arc<Dataset> {
        &self.dataset
    }

    pub fn options(&self) -> &FilteredReadOptions {
        &self.options
    }

    pub fn index_input(&self) -> Option<&Arc<dyn ExecutionPlan>> {
        self.input.row_set_plan()
    }

    pub fn row_stream_input(&self) -> Option<&Arc<dyn ExecutionPlan>> {
        match &self.input {
            RowSelector::RowStream(source) => Some(&source.plan),
            _ => None,
        }
    }

    /// Return the pre-computed plan if one exists, without triggering initialization.
    pub fn plan(&self) -> Option<FilteredReadPlan> {
        self.plan.get().map(|p| p.to_external_plan())
    }

    fn execute_row_stream(
        &self,
        source: &Arc<RowStreamSource>,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let input_stream = source.plan.execute(partition, context)?;
        let dataset = self.dataset.clone();
        let source = source.clone();
        let carried_schema =
            Self::carried_schema(source.plan.schema().as_ref(), &self.options.projection);
        let output_schema = self.schema();
        let metrics = self.metrics.clone();

        let lazy_stream = futures::stream::once(async move {
            let row_stream_read = Arc::new(RowStreamRead::new(
                dataset,
                source,
                carried_schema,
                output_schema,
                &metrics,
                partition,
            ));
            row_stream_read.apply(input_stream)
        })
        .flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            lazy_stream,
        )))
    }
}

/// How many batches run concurrently.  Each batch's read already carries
/// the full fragment-readahead and decode parallelism, so a shallow pipeline
/// keeps the I/O pipe full; running every batch at once only multiplies that
/// into lock contention
const ROW_STREAM_CONCURRENT_BATCHES: usize = 4;

/// Fragment metadata, loaded on the first batch and reused afterwards
struct StreamFragments {
    /// All dataset (or scoped) fragments, in dataset order
    fragments: Vec<LoadedFragment>,
    /// Fragment id → position in `fragments`
    positions: HashMap<u32, usize>,
    /// Each fragment's row-id span, for skipping fragments a batch cannot
    /// touch (None = empty fragment)
    id_spans: Vec<Option<RangeInclusive<u64>>>,
}

impl StreamFragments {
    fn get(&self, fragment_id: u32) -> Option<&LoadedFragment> {
        self.positions
            .get(&fragment_id)
            .map(|position| &self.fragments[*position])
    }
}

/// Executes a [`FilteredReadExec`] over a row-stream source
struct RowStreamRead {
    dataset: Arc<Dataset>,
    source: Arc<RowStreamSource>,
    /// The input columns that carry through to the output
    carried_schema: SchemaRef,
    output_schema: SchemaRef,
    scan_scheduler: Arc<ScanScheduler>,
    loaded_fragments: OnceCell<StreamFragments>,
    global_metrics: Arc<FilteredReadGlobalMetrics>,
    baseline_metrics: BaselineMetrics,
}

impl RowStreamRead {
    fn new(
        dataset: Arc<Dataset>,
        source: Arc<RowStreamSource>,
        carried_schema: SchemaRef,
        output_schema: SchemaRef,
        metrics: &ExecutionPlanMetricsSet,
        partition: usize,
    ) -> Self {
        let scan_scheduler =
            FilteredReadStream::make_scan_scheduler(&dataset, &source.read_options);
        Self {
            dataset,
            source,
            carried_schema,
            output_schema,
            scan_scheduler,
            loaded_fragments: OnceCell::new(),
            global_metrics: Arc::new(FilteredReadGlobalMetrics::new(metrics)),
            baseline_metrics: BaselineMetrics::new(metrics, partition),
        }
    }

    async fn load_fragments(&self) -> Result<&StreamFragments> {
        self.loaded_fragments
            .get_or_try_init(|| async {
                let fragments = FilteredReadStream::load_all_fragments(
                    &self.dataset,
                    &self.source.read_options,
                )
                .await?;
                let positions = fragments
                    .iter()
                    .enumerate()
                    .map(|(position, fragment)| (fragment.fragment.id() as u32, position))
                    .collect();
                let id_spans = fragments
                    .iter()
                    .map(|fragment| fragment.row_id_sequence.row_id_range())
                    .collect();
                Ok(StreamFragments {
                    fragments,
                    positions,
                    id_spans,
                })
            })
            .await
    }

    /// Build a batch's read ranges directly from physical row addresses
    fn plan_batch_from_addresses(
        addrs: &RowAddrTreeMap,
        fragments: &StreamFragments,
    ) -> FilteredReadInternalPlan {
        let mut rows: BTreeMap<u32, Vec<Range<u64>>> = BTreeMap::new();
        for (fragment_id, requested) in addrs.iter() {
            // Unknown fragments (e.g. fully deleted) drop like stale keys
            let Some(fragment) = fragments.get(*fragment_id) else {
                continue;
            };
            let requested = match requested {
                RowAddrSelection::Full => vec![0..fragment.num_physical_rows],
                RowAddrSelection::Partial(bitmap) => bitmap_to_ranges(bitmap),
            };
            let valid = FilteredReadStream::full_frag_range(
                fragment.num_physical_rows,
                &fragment.deletion_vector,
            );
            let matched = FilteredReadStream::intersect_ranges(&valid, &requested);
            if !matched.is_empty() {
                rows.insert(*fragment_id, matched);
            }
        }
        FilteredReadInternalPlan {
            rows,
            filters: HashMap::new(),
            scan_range_after_filter: None,
        }
    }

    /// Build a batch's read ranges by resolving stable row ids through the
    /// fragments' row-id sequences
    fn plan_batch_from_row_ids(
        ids: RowAddrTreeMap,
        keys: &arrow_array::PrimitiveArray<UInt64Type>,
        fragments: &StreamFragments,
    ) -> FilteredReadInternalPlan {
        let mut rows: BTreeMap<u32, Vec<Range<u64>>> = BTreeMap::new();
        let (Some(min_key), Some(max_key)) = (arrow::compute::min(keys), arrow::compute::max(keys))
        else {
            // Every key is null
            return FilteredReadInternalPlan {
                rows,
                filters: HashMap::new(),
                scan_range_after_filter: None,
            };
        };
        let requested = RowAddrMask::from_allowed(ids);
        for (fragment, id_span) in fragments.fragments.iter().zip(&fragments.id_spans) {
            // Only fragments whose id span overlaps the batch's key range
            // can hold requested rows
            let Some(id_span) = id_span else { continue };
            if *id_span.end() < min_key || *id_span.start() > max_key {
                continue;
            }
            let offsets = fragment.row_id_sequence.mask_to_offset_ranges(&requested);
            if offsets.is_empty() {
                continue;
            }
            let valid = FilteredReadStream::full_frag_range(
                fragment.num_physical_rows,
                &fragment.deletion_vector,
            );
            let matched = FilteredReadStream::intersect_ranges(&valid, &offsets);
            if !matched.is_empty() {
                rows.insert(fragment.fragment.id() as u32, matched);
            }
        }
        FilteredReadInternalPlan {
            rows,
            filters: HashMap::new(),
            scan_range_after_filter: None,
        }
    }

    fn key_array<'a>(
        &self,
        batch: &'a RecordBatch,
        producer: &str,
    ) -> DataFusionResult<&'a arrow_array::PrimitiveArray<UInt64Type>> {
        let keys = batch
            .column_by_name(self.source.key_column)
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "the row-stream {} is missing the '{}' column",
                    producer, self.source.key_column
                ))
            })?;
        keys.as_primitive_opt::<UInt64Type>().ok_or_else(|| {
            DataFusionError::Internal(format!(
                "expected the row-stream column '{}' to be UInt64 but it was {}",
                self.source.key_column,
                keys.data_type()
            ))
        })
    }

    async fn plan_batch(
        &self,
        keys: &arrow_array::PrimitiveArray<UInt64Type>,
    ) -> DataFusionResult<FilteredReadInternalPlan> {
        let compute_timer = self.baseline_metrics.elapsed_compute().timer();
        // Null keys are excluded; attach_columns drops their rows
        let batch_keys = if keys.null_count() == 0 {
            RowAddrTreeMap::from_iter(keys.values().iter().copied())
        } else {
            RowAddrTreeMap::from_iter(keys.iter().flatten())
        };
        drop(compute_timer);

        let fragments = self.load_fragments().await?;
        // Row ids equal row addresses when the dataset does not use stable
        // row ids, so either key resolves directly by position
        if self.source.key_column == ROW_ADDR || !self.dataset.manifest.uses_stable_row_ids() {
            Ok(Self::plan_batch_from_addresses(&batch_keys, fragments))
        } else {
            Ok(Self::plan_batch_from_row_ids(batch_keys, keys, fragments))
        }
    }

    /// Read the batch's planned ranges through the same executor as a scan,
    /// returning the rows in storage order, deduplicated, with the key
    /// column included
    async fn read_batch(
        &self,
        internal_plan: FilteredReadInternalPlan,
        batch_index: u32,
    ) -> DataFusionResult<RecordBatch> {
        let fragment_count = self.load_fragments().await?.fragments.len();
        // I/O priority: earlier batches strictly first (output emits in batch
        // order), fragments keep dataset order within a batch
        let priority_offset = batch_index.saturating_mul(fragment_count as u32);
        let read = FilteredReadStream::try_new(
            self.dataset.clone(),
            self.source.read_options.clone(),
            self.global_metrics.clone(),
            internal_plan,
            Some(self.scan_scheduler.clone()),
            Some(priority_offset),
        );
        let decode_parallelism = match self.source.read_options.threading_mode {
            FilteredReadThreadingMode::OnePartitionMultipleThreads(n) => n,
            FilteredReadThreadingMode::MultiplePartitions(n) => n,
        };
        let read_batches = read.collect_all(decode_parallelism.max(1)).await?;
        Ok(arrow::compute::concat_batches(
            &read.output_schema,
            read_batches.iter(),
        )?)
    }

    /// Align the read rows back to the batch's row order and merge the
    /// fetched columns on
    fn attach_columns(
        &self,
        batch: RecordBatch,
        read_data: RecordBatch,
    ) -> DataFusionResult<RecordBatch> {
        let _compute_timer = self.baseline_metrics.elapsed_compute().timer();
        let keys = self.key_array(&batch, "input")?;
        let read_keys = self.key_array(&read_data, "read")?;
        attach_read_columns(
            &batch,
            keys,
            &read_data,
            read_keys,
            self.carried_schema.as_ref(),
            self.source.new_fields_schema.as_ref(),
            &self.output_schema,
        )
    }

    async fn execute_batch(
        self: Arc<Self>,
        batch: RecordBatch,
        batch_index: u32,
    ) -> DataFusionResult<RecordBatch> {
        if batch.num_rows() == 0 {
            return Ok(RecordBatch::new_empty(self.output_schema.clone()));
        }
        let internal_plan = self.plan_batch(self.key_array(&batch, "input")?).await?;
        let read_data = self.read_batch(internal_plan, batch_index).await?;
        self.attach_columns(batch, read_data)
    }

    fn apply(
        self: Arc<Self>,
        input: SendableRecordBatchStream,
    ) -> impl Stream<Item = DataFusionResult<RecordBatch>> {
        let batch_target_rows = self
            .source
            .read_options
            .batch_size
            .map(|batch_size| batch_size as usize)
            .unwrap_or_else(|| get_default_batch_size().unwrap_or(BATCH_SIZE_FALLBACK));
        let on_result = self.clone();
        let on_done = self.clone();
        coalesce_batches(input, batch_target_rows)
            .enumerate()
            .map(move |(batch_index, batch)| {
                let batch = batch?;
                let this = self.clone();
                DataFusionResult::Ok(
                    // SpawnedTask aborts on drop: cancelling the query
                    // cancels in-flight batches
                    SpawnedTask::spawn(
                        this.execute_batch(batch, batch_index as u32)
                            .in_current_span(),
                    )
                    .map(|res| match res {
                        Ok(result) => result,
                        Err(join_error) => Err(DataFusionError::External(Box::new(join_error))),
                    }),
                )
            })
            .boxed()
            .try_buffered(ROW_STREAM_CONCURRENT_BATCHES)
            .map(move |result| {
                on_result
                    .global_metrics
                    .io_metrics
                    .record(&on_result.scan_scheduler);
                match on_result
                    .baseline_metrics
                    .record_poll(Poll::Ready(Some(result)))
                {
                    Poll::Ready(Some(result)) => result,
                    _ => unreachable!("record_poll returned a different poll state"),
                }
            })
            .finally(move || {
                on_done.baseline_metrics.done();
                on_done
                    .global_metrics
                    .io_metrics
                    .record(&on_done.scan_scheduler);
            })
    }
}

/// Align `read_data` rows back to a keyed batch's row order and merge the
/// fetched columns on.
///
/// `keys` is the row-id-space key of each `batch` row (usually the batch's
/// own key column; a caller that keys by address passes the resolved ids
/// instead) and `read_keys` the key of each `read_data` row, which must be
/// unique. Input rows whose key has no read row — null or stale keys — are
/// DROPPED; duplicate input keys re-expand through the gather. Output
/// columns follow `output_schema`: `carried_schema` names come from `batch`,
/// `new_fields_schema` names from `read_data`.
pub fn attach_read_columns(
    batch: &RecordBatch,
    keys: &arrow_array::PrimitiveArray<UInt64Type>,
    read_data: &RecordBatch,
    read_keys: &arrow_array::PrimitiveArray<UInt64Type>,
    carried_schema: &arrow_schema::Schema,
    new_fields_schema: &arrow_schema::Schema,
    output_schema: &SchemaRef,
) -> DataFusionResult<RecordBatch> {
    // Fast path: one read row per input row with an identical key sequence —
    // already aligned, skip the hash map and the permutation
    if keys.null_count() == 0
        && read_data.num_rows() == batch.num_rows()
        && read_keys.values() == keys.values()
    {
        let new_data = read_data.project_by_schema(new_fields_schema)?;
        let carried = batch.project_by_schema(carried_schema)?;
        return Ok(carried.merge_with_schema(&new_data, output_schema.as_ref())?);
    }

    let key_to_index: HashMap<u64, u32> = read_keys
        .values()
        .iter()
        .enumerate()
        .map(|(index, key)| (*key, index as u32))
        .collect();

    // Sizes differ only when some input keys have no live row (null or
    // stale keys): drop those input rows first
    let (batch, keys) = if read_data.num_rows() != batch.num_rows() {
        let matched: BooleanArray = keys
            .iter()
            .map(|key| key.map(|key| key_to_index.contains_key(&key)))
            .collect();
        let keys = arrow::compute::filter(keys, &matched)?
            .as_primitive::<UInt64Type>()
            .clone();
        (arrow::compute::filter_record_batch(batch, &matched)?, keys)
    } else {
        (batch.clone(), keys.clone())
    };
    if batch.num_rows() == 0 {
        return Ok(RecordBatch::new_empty(output_schema.clone()));
    }

    // Gather the read rows into input order — every remaining key hits
    let indices = UInt32Array::from_iter_values(keys.values().iter().map(|key| key_to_index[key]));
    let new_data = arrow_select::take::take_record_batch(read_data, &indices)?;
    let new_data = new_data.project_by_schema(new_fields_schema)?;
    let carried = batch.project_by_schema(carried_schema)?;
    Ok(carried.merge_with_schema(&new_data, output_schema.as_ref())?)
}

impl DisplayAs for FilteredReadExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        if let RowSelector::RowStream(source) = &self.input {
            let columns = source
                .new_fields_schema
                .fields
                .iter()
                .map(|f| f.name().as_str())
                .collect::<Vec<_>>()
                .join(", ");
            return match t {
                DisplayFormatType::Default | DisplayFormatType::Verbose => {
                    write!(
                        f,
                        "LanceRead: uri={}, projection=[{}], source=stream({})",
                        self.dataset.data_dir(),
                        columns,
                        source.key_column,
                    )
                }
                DisplayFormatType::TreeRender => {
                    write!(
                        f,
                        "LanceRead\nuri={}\nprojection=[{}]\nsource=stream({})",
                        self.dataset.data_dir(),
                        columns,
                        source.key_column,
                    )
                }
            };
        }
        let columns = self
            .options
            .projection
            .to_bare_schema()
            .fields
            .iter()
            .map(|f| f.name.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "LanceRead: uri={}, projection=[{}], num_fragments={}, range_before={:?}, range_after={:?}, row_id={}, row_addr={}, full_filter={}, refine_filter={}",
                    self.dataset.data_dir(),
                    columns,
                    self.options
                        .fragments
                        .as_ref()
                        .map(|f| f.len())
                        .unwrap_or(self.dataset.fragments().len()),
                    self.options.scan_range_before_filter,
                    self.options.scan_range_after_filter,
                    self.options.projection.with_row_id,
                    self.options.projection.with_row_addr,
                    self.options
                        .full_filter
                        .as_ref()
                        .map(|i| i.to_string())
                        .unwrap_or("--".to_string()),
                    self.options
                        .refine_filter
                        .as_ref()
                        .map(|i| i.to_string())
                        .unwrap_or("--".to_string()),
                )
            }
            DisplayFormatType::TreeRender => {
                write!(
                    f,
                    "LanceRead\nuri={}\nprojection=[{}]\nnum_fragments={}\nrange_before={:?}\nrange_after={:?}\nrow_id={}\nrow_addr={}\nfull_filter={}\nrefine_filter={}",
                    self.dataset.data_dir(),
                    columns,
                    self.options
                        .fragments
                        .as_ref()
                        .map(|f| f.len())
                        .unwrap_or(self.dataset.fragments().len()),
                    self.options.scan_range_before_filter,
                    self.options.scan_range_after_filter,
                    self.options.projection.with_row_id,
                    self.options.projection.with_row_addr,
                    self.options
                        .full_filter
                        .as_ref()
                        .map(|i| i.to_string())
                        .unwrap_or("true".to_string()),
                    self.options
                        .refine_filter
                        .as_ref()
                        .map(|i| i.to_string())
                        .unwrap_or("true".to_string()),
                )
            }
        }
    }
}

impl ExecutionPlan for FilteredReadExec {
    fn name(&self) -> &str {
        "FilteredReadExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        if let Some(child) = self.input.child() {
            vec![child]
        } else {
            vec![]
        }
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        // Partitioning a row-stream read would create multiple I/O schedulers
        // (RAM heavy); the other selectors have no row input
        vec![false; self.children().len()]
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn partition_statistics(
        &self,
        partition: Option<usize>,
    ) -> datafusion::error::Result<Arc<Statistics>> {
        if let RowSelector::RowStream(source) = &self.input {
            // At most one output row per input row
            return Ok(Arc::new(Statistics {
                num_rows: source.plan.partition_statistics(partition)?.num_rows,
                ..Statistics::new_unknown(self.schema().as_ref())
            }));
        }
        let fragments = self
            .options
            .fragments
            .clone()
            .unwrap_or_else(|| self.dataset.fragments().clone());

        if fragments.iter().any(|f| f.num_rows().is_none()) {
            return Err(DataFusionError::Internal(
                "Fragments are missing row count stats".to_string(),
            ));
        }

        let total_rows: u64 = fragments.iter().map(|f| f.num_rows().unwrap() as u64).sum();

        let Some(filter) = self.options.full_filter.as_ref() else {
            // If there is no filter, we just return the total number of rows (sans any before-filter range)
            // divided by the number of partitions.
            let total_rows =
                if let Some(scan_range_before_filter) = &self.options.scan_range_before_filter {
                    total_rows.min(scan_range_before_filter.end - scan_range_before_filter.start)
                } else {
                    total_rows
                };

            let total_rows = if partition.is_some() {
                match self.options.threading_mode {
                    FilteredReadThreadingMode::MultiplePartitions(num_partitions) => {
                        total_rows / num_partitions as u64
                    }
                    // Pretty sure this shouldn't be encountered in practice
                    FilteredReadThreadingMode::OnePartitionMultipleThreads(_) => total_rows,
                }
            } else {
                total_rows
            };

            return Ok(Arc::new(Statistics {
                num_rows: Precision::Exact(total_rows as usize),
                ..datafusion::physical_plan::Statistics::new_unknown(self.schema().as_ref())
            }));
        };

        // We could evaluate the indexed filter here but this is still during the planning
        // phase so we want to avoid that.
        //
        // Instead, we create a mock input which is the filtered read (without the filter)
        // and then use DF's FilterExec logic to calculate the statistics (which uses column
        // stats and basic filter shape)

        // Need to add in filter columns even though they aren't part of the projection
        let filter_columns = Planner::column_names_in_expr(filter);
        let read_projection = self
            .options
            .projection
            .clone()
            .union_columns(filter_columns, OnMissing::Error)?;

        let read_schema = public_blob_v2_binary_projection_schema(&read_projection);

        let planner = Arc::new(Planner::new(read_schema.clone()));
        let physical_filter = planner.create_physical_expr(filter)?;

        let mock_input = Arc::new(Self::try_new(
            self.dataset.clone(),
            FilteredReadOptions {
                scan_range_after_filter: None,
                refine_filter: None,
                full_filter: None,
                projection: read_projection,
                ..self.options.clone()
            },
            None,
        )?);
        let df_filter_exec = FilterExec::try_new(physical_filter, mock_input)?;
        let mut df_stats = Arc::unwrap_or_clone(df_filter_exec.partition_statistics(partition)?);

        // If we have an after-filter range, we should apply it to the stats (the before-filter range
        // is applied in the mock input)
        let total_rows =
            if let Some(scan_range_after_filter) = &self.options.scan_range_after_filter {
                df_stats.num_rows.min(&Precision::Exact(
                    scan_range_after_filter.end as usize - scan_range_after_filter.start as usize,
                ))
            } else {
                df_stats.num_rows
            };
        df_stats.num_rows = total_rows;

        let schema = self.schema();

        // We might have added some columns to the schema so the filter compiles but we drop this
        // columns during the filtered read and they aren't part of the output.  So we need to make
        // sure and drop them from the column stats as well.
        assert_eq!(read_schema.fields.len(), df_stats.column_statistics.len());
        let mut proj_iter = schema.fields.iter().peekable();
        let mut stats_iter = read_schema.fields.iter();
        df_stats.column_statistics.retain(|_| {
            let stats_field = stats_iter.next().unwrap();
            if let Some(proj_field) = proj_iter.peek() {
                if proj_field.name() == stats_field.name() {
                    proj_iter.next();
                    true
                } else {
                    false
                }
            } else {
                false
            }
        });

        Ok(Arc::new(df_stats))
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        if children.len() > 1 {
            Err(DataFusionError::External(
                Error::internal("A FilteredReadExec cannot have two children".to_string()).into(),
            ))
        } else {
            // Rebuild via try_new so the selector and derived state are
            // re-derived from the new child's schema
            let child = children.into_iter().next();
            let rebuilt = Self::try_new(self.dataset.clone(), self.options.clone(), child)
                .map_err(|e| DataFusionError::External(e.into()))?;
            Ok(Arc::new(rebuilt))
        }
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        match &self.input {
            RowSelector::RowStream(source) => self.execute_row_stream(source, partition, context),
            _ => Ok(self.obtain_stream(partition, context)),
        }
    }

    fn fetch(&self) -> Option<usize> {
        if self.row_stream_input().is_some() {
            return None;
        }
        if self.options.full_filter.is_none() {
            self.options
                .scan_range_before_filter
                .as_ref()
                .map(|range| (range.end - range.start) as usize)
        } else {
            self.options
                .scan_range_after_filter
                .as_ref()
                .map(|range| (range.end - range.start) as usize)
        }
    }

    fn supports_limit_pushdown(&self) -> bool {
        // A limit pushes through to a row-stream input (one output row per
        // input row); the other selectors have no node to push it to
        self.row_stream_input().is_some()
    }

    fn with_fetch(&self, limit: Option<usize>) -> Option<Arc<dyn ExecutionPlan>> {
        if self.row_stream_input().is_some() {
            return None;
        }
        // TODO: Support multiple partitions in the future by coordinating limits across partitions
        if matches!(
            self.options.threading_mode,
            FilteredReadThreadingMode::MultiplePartitions(_)
        ) {
            return None;
        }
        let limit = limit?;

        if self.dataset.manifest.uses_stable_row_ids() {
            return None;
        }

        let mut updated_options = self.options.clone();

        if self.options.full_filter.is_none() && self.options.refine_filter.is_none() {
            if self.options.scan_range_before_filter.is_some() {
                return None;
            }
            updated_options.scan_range_before_filter = Some(0..(limit as u64));
        } else {
            if self.options.scan_range_after_filter.is_some() {
                return None;
            }
            updated_options.scan_range_after_filter = Some(0..(limit as u64));
        }

        match Self::try_new(
            self.dataset.clone(),
            updated_options,
            self.input.row_set_plan().cloned(),
        ) {
            Ok(exec) => Some(Arc::new(exec)),
            Err(e) => {
                log::warn!(
                    "Failed to create FilteredReadExec for {} with fetch limit: {}",
                    self.dataset.uri(),
                    e
                );
                None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use crate::index::DatasetIndexExt;
    use arrow::{
        compute::concat_batches,
        datatypes::{Float32Type, UInt32Type, UInt64Type},
    };
    use arrow_array::{
        Array, ArrayRef, Int32Array, RecordBatch, RecordBatchIterator, UInt32Array, cast::AsArray,
    };
    use itertools::Itertools;
    use lance_core::datatypes::OnMissing;
    use lance_core::utils::address::RowAddress;
    use lance_core::utils::tempfile::TempStrDir;
    use lance_datafusion::exec::OneShotExec;
    use lance_datagen::{BatchCount, Dimension, RowCount, array, gen_batch};
    use lance_index::{
        IndexType,
        optimize::OptimizeOptions,
        scalar::{ScalarIndexParams, expression::PlannerIndexExt},
    };
    use lance_select::result::IndexExprResultWireFormat;
    use lance_select::{RowAddrMask, RowAddrTreeMap};

    use crate::{
        dataset::{InsertBuilder, WriteDestination, WriteMode, WriteParams},
        index::DatasetIndexInternalExt,
        io::exec::scalar_index::ScalarIndexExec,
        utils::test::{DatagenExt, FragmentCount, FragmentRowCount},
    };

    use super::*;

    struct TestFixture {
        _tmp_path: TempStrDir,
        dataset: Arc<Dataset>,
    }

    /// The test dataset first creates 200 rows and then 200 more, each
    /// with 100 rows per fragment, for a total of 4 fragments.  The column
    /// fully_indexed is indexed on all 4 fragments.  The column partly_indexed
    /// is only indexed on the first 2 fragments.
    ///
    /// The second fragment is then deleted, leaving a gap in the fragment sequence
    /// The third fragment has a deletion file with 50 rows deleted.
    ///
    /// The fragment ids are 0 (values 0..100), 2 (values 250..300), 3 (values 300..400)
    impl TestFixture {
        async fn new() -> Self {
            let tmp_path = TempStrDir::default();

            let mut dataset = gen_batch()
                .col("fully_indexed", array::step::<UInt32Type>())
                .col("partly_indexed", array::step::<UInt64Type>())
                .col("not_indexed", array::step::<UInt32Type>())
                .col(
                    "recheck_idx",
                    array::cycle_utf8_literals(&["cat", "caterpillar", "dog"]),
                )
                .col("vector", array::rand_vec::<Float32Type>(Dimension::from(4)))
                .into_dataset(
                    tmp_path.as_str(),
                    FragmentCount::from(2),
                    FragmentRowCount::from(100),
                )
                .await
                .unwrap();

            dataset
                .create_index(
                    &["fully_indexed"],
                    IndexType::BTree,
                    None,
                    &ScalarIndexParams::default(),
                    false,
                )
                .await
                .unwrap();
            dataset
                .create_index(
                    &["partly_indexed"],
                    IndexType::BTree,
                    None,
                    &ScalarIndexParams::default(),
                    false,
                )
                .await
                .unwrap();
            dataset
                .create_index(
                    &["recheck_idx"],
                    IndexType::NGram,
                    None,
                    &ScalarIndexParams::default(),
                    false,
                )
                .await
                .unwrap();

            let new_data = gen_batch()
                .col("fully_indexed", array::step_custom::<UInt32Type>(200, 1))
                .col("partly_indexed", array::step_custom::<UInt64Type>(200, 1))
                .col("not_indexed", array::step_custom::<UInt32Type>(200, 1))
                .into_reader_rows(RowCount::from(100), BatchCount::from(2))
                .try_collect()
                .unwrap();

            let mut dataset =
                InsertBuilder::new(WriteDestination::Dataset(Arc::new(dataset.clone())))
                    .with_params(&WriteParams {
                        mode: WriteMode::Append,
                        max_rows_per_file: 100,
                        ..Default::default()
                    })
                    .execute(new_data)
                    .await
                    .unwrap();

            dataset
                .optimize_indices(&OptimizeOptions::new().index_names(vec![
                    "fully_indexed_idx".to_string(),
                    "recheck_idx_idx".to_string(),
                ]))
                .await
                .unwrap();

            dataset
                .delete("fully_indexed >= 100 AND fully_indexed < 250")
                .await
                .unwrap();

            dataset.load_indices().await.unwrap();

            Self {
                _tmp_path: tmp_path,
                dataset: Arc::new(dataset),
            }
        }

        async fn index_input(
            &self,
            options: &FilteredReadOptions,
        ) -> Option<Arc<dyn ExecutionPlan>> {
            if let Some(filter) = &options.full_filter {
                let planner = Planner::new(Arc::new(self.dataset.schema().into()));
                let index_info = self.dataset.scalar_index_info().await.unwrap();
                let filter_plan = planner
                    .create_filter_plan(filter.clone(), &index_info, true)
                    .unwrap();
                if let Some(index_query) = filter_plan.index_query {
                    Some(Arc::new(ScalarIndexExec::new(
                        self.dataset.clone(),
                        index_query,
                        IndexExprResultWireFormat::default(),
                    )))
                } else {
                    None
                }
            } else {
                None
            }
        }

        async fn make_plan(&self, options: FilteredReadOptions) -> FilteredReadExec {
            let index_input = self.index_input(&options).await;
            FilteredReadExec::try_new(self.dataset.clone(), options, index_input).unwrap()
        }

        async fn test_plan(&self, options: FilteredReadOptions, expected: &dyn Array) {
            let index_input = self.index_input(&options).await;
            let plan =
                FilteredReadExec::try_new(self.dataset.clone(), options, index_input).unwrap();

            let stream = plan.execute(0, Arc::new(TaskContext::default())).unwrap();
            let schema = stream.schema();
            let batches = stream.try_collect::<Vec<_>>().await.unwrap();

            let batch = concat_batches(&schema, &batches).unwrap();

            assert_eq!(batch.num_rows(), expected.len());

            let col = batch.column(0);
            assert_eq!(col.as_ref(), expected);
        }

        fn frags(&self, ids: &[u32]) -> Arc<Vec<Fragment>> {
            Arc::new(
                ids.iter()
                    .map(|id| {
                        self.dataset
                            .fragments()
                            .iter()
                            .find(|f| f.id == *id as u64)
                            .unwrap()
                            .clone()
                    })
                    .collect(),
            )
        }

        async fn filter_plan(&self, filter: &str, use_scalar_index: bool) -> FilterPlan {
            let arrow_schema = Arc::new(arrow_schema::Schema::from(self.dataset.schema()));
            let planner = Planner::new(arrow_schema);
            let expr = planner.parse_filter(filter).unwrap();
            let index_info = self.dataset.scalar_index_info().await.unwrap();
            planner
                .create_filter_plan(expr, &index_info, use_scalar_index)
                .unwrap()
        }
    }

    async fn dataset_with_bloom_filter_nulls() -> (TempStrDir, Arc<Dataset>) {
        let tmp_path = TempStrDir::default();
        let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "value",
            arrow_schema::DataType::Int32,
            true,
        )]));
        let values: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(1),
            None,
            Some(2),
            None,
            Some(3),
        ]));
        let batch = RecordBatch::try_new(schema.clone(), vec![values]).unwrap();
        let reader = RecordBatchIterator::new(vec![Ok(batch)].into_iter(), schema.clone());
        let mut dataset = Dataset::write(reader, tmp_path.as_str(), None)
            .await
            .unwrap();
        dataset
            .create_index(
                &["value"],
                IndexType::BloomFilter,
                None,
                &ScalarIndexParams::default(),
                false,
            )
            .await
            .unwrap();
        (tmp_path, Arc::new(dataset))
    }

    fn u32s(ranges: Vec<Range<u32>>) -> Arc<dyn Array> {
        Arc::new(UInt32Array::from_iter_values(
            ranges.into_iter().flat_map(|r| r.into_iter()),
        ))
    }

    /// Take-shaped masked reads consolidate their tiny per-fragment batches;
    /// few-fragment and dense masked reads keep per-fragment boundaries.
    #[test_log::test(tokio::test)]
    async fn test_take_shaped_mask_consolidation() {
        // 20 fragments x 2000 rows, value = global row number
        let tmp_path = TempStrDir::default();
        let data = gen_batch()
            .col("value", array::step::<UInt32Type>())
            .into_reader_rows(RowCount::from(2000), BatchCount::from(20));
        let dataset = Arc::new(
            Dataset::write(
                data,
                tmp_path.as_str(),
                Some(WriteParams {
                    max_rows_per_file: 2000,
                    ..Default::default()
                }),
            )
            .await
            .unwrap(),
        );

        let mask_input = |addrs: Vec<u64>| -> Arc<dyn ExecutionPlan> {
            let covered: RoaringBitmap = dataset.fragments().iter().map(|f| f.id as u32).collect();
            let batch =
                IndexExprResult::exact(RowAddrMask::from_allowed(RowAddrTreeMap::from_iter(addrs)))
                    .serialize(&covered, IndexExprResultWireFormat::default())
                    .unwrap();
            let schema = batch.schema();
            let stream = futures::stream::once(async move { Ok(batch) });
            Arc::new(OneShotExec::new(Box::pin(RecordBatchStreamAdapter::new(
                schema, stream,
            ))))
        };
        let run = |input: Arc<dyn ExecutionPlan>| {
            let dataset = dataset.clone();
            async move {
                // Pin the batch size so batch-count assertions don't depend
                // on LANCE_DEFAULT_BATCH_SIZE
                let options = FilteredReadOptions::basic_full_read(&dataset).with_batch_size(2000);
                let plan =
                    FilteredReadExec::try_new(dataset.clone(), options, Some(input)).unwrap();
                let stream = plan.execute(0, Arc::new(TaskContext::default())).unwrap();
                stream.try_collect::<Vec<_>>().await.unwrap()
            }
        };
        let addr = |frag: u32, offset: u32| u64::from(RowAddress::new_from_parts(frag, offset));

        // Take shape: 20 fragments, 2 rows each -> one consolidated batch,
        // rows in fragment order
        let addrs: Vec<u64> = (0..20u32).flat_map(|f| [addr(f, 3), addr(f, 7)]).collect();
        let batches = run(mask_input(addrs)).await;
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 40);
        assert_eq!(batches.len(), 1);
        let expected =
            UInt32Array::from_iter_values((0..20u32).flat_map(|f| [f * 2000 + 3, f * 2000 + 7]));
        assert_eq!(batches[0].column(0).as_ref(), &expected);

        // Too few fragments -> inline path, one batch per fragment
        let batches = run(mask_input(vec![addr(0, 3), addr(1, 7)])).await;
        assert_eq!(batches.len(), 2);

        // Dense (2000 planned rows per fragment) -> inline path
        let addrs: Vec<u64> = (0..8u32)
            .flat_map(|f| (0..2000u32).map(move |o| addr(f, o)))
            .collect();
        let batches = run(mask_input(addrs)).await;
        assert_eq!(batches.len(), 8);
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 16000);
    }

    /// Round-trip every interval shape through the arrow wire format and
    /// confirm the endpoints survive. Exercises both
    /// `IndexExprResult::serialize` and `EvaluatedIndex::try_from_arrow`
    /// so the schema names stay in sync.
    #[test]
    fn test_index_expr_result_serialize_roundtrip() {
        use lance_select::{RowAddrMask, RowAddrTreeMap};

        let mk = |rows: &[u64]| RowAddrMask::from_allowed(RowAddrTreeMap::from_iter(rows));

        let mut frags = RoaringBitmap::new();
        frags.insert(0);
        frags.insert(7);

        let cases = vec![
            ("exact", IndexExprResult::exact(mk(&[1, 2, 3]))),
            ("at_most", IndexExprResult::at_most(mk(&[1, 2, 3]))),
            ("at_least", IndexExprResult::at_least(mk(&[1, 2]))),
            // Refined: non-empty lower strictly inside non-universe upper.
            ("refined", IndexExprResult::new(mk(&[1, 2]), mk(&[1, 2, 3]))),
        ];

        for (name, original) in cases {
            let batch = original
                .serialize(&frags, IndexExprResultWireFormat::default())
                .unwrap_or_else(|e| panic!("serialize {name}: {e}"));
            let decoded = EvaluatedIndex::try_from_arrow(&batch)
                .unwrap_or_else(|e| panic!("try_from_arrow {name}: {e}"));

            // The underlying RowAddrTreeMap is PartialEq; compare via mask
            // emptiness + symmetric difference being empty would be more
            // robust, but the canonical builders preserve representation.
            assert_eq!(
                decoded.index_result.lower, original.lower,
                "{name}: lower endpoint changed across batch-trip",
            );
            assert_eq!(
                decoded.index_result.upper, original.upper,
                "{name}: upper endpoint changed across batch-trip",
            );
            assert_eq!(
                decoded.applicable_fragments, frags,
                "{name}: applicable fragments changed across batch-trip",
            );
        }
    }

    #[test_log::test(tokio::test)]
    async fn test_bloom_filter_is_not_null_prefilter() {
        let (_tmp_path, dataset) = dataset_with_bloom_filter_nulls().await;
        let arrow_schema = Arc::new(arrow_schema::Schema::from(dataset.schema()));
        let planner = Planner::new(arrow_schema);
        let expr = planner.parse_filter("value IS NOT NULL").unwrap();
        let index_info = dataset.scalar_index_info().await.unwrap();
        let filter_plan = planner.create_filter_plan(expr, &index_info, true).unwrap();
        assert!(
            filter_plan.index_query.is_none(),
            "bloom filter IS NOT NULL should not use an index query"
        );

        let options = FilteredReadOptions::basic_full_read(&dataset).with_filter_plan(filter_plan);
        let plan = FilteredReadExec::try_new(dataset.clone(), options, None).unwrap();
        let stream = plan.execute(0, Arc::new(TaskContext::default())).unwrap();
        let batches = stream.try_collect::<Vec<_>>().await.unwrap();
        let row_count: usize = batches.iter().map(|batch| batch.num_rows()).sum();

        assert_eq!(row_count, 3);
    }

    #[test_log::test(tokio::test)]
    async fn test_range_no_scalar_index() {
        let fixture = TestFixture::new().await;

        let base_options = FilteredReadOptions::basic_full_read(&fixture.dataset);
        // Basic full scan
        fixture
            .test_plan(base_options.clone(), &u32s(vec![0..100, 250..400]))
            .await;

        // Basic range scan (whole dataset, no filter)
        let options = base_options
            .clone()
            .with_scan_range_before_filter(25..125)
            .unwrap();
        fixture
            .test_plan(options, &u32s(vec![25..100, 250..275]))
            .await;

        // Range scan against user-specified fragments
        let options = base_options
            .clone()
            .with_fragments(fixture.frags(&[3, 2]))
            .with_scan_range_before_filter(25..125)
            .unwrap();
        fixture
            .test_plan(options, &u32s(vec![325..400, 250..275]))
            .await;

        // Range scan that goes past the end of the dataset (100 rows
        // requested, only 50 can be returned)
        let options = base_options
            .clone()
            .with_scan_range_before_filter(200..300)
            .unwrap();
        fixture.test_plan(options, &u32s(vec![350..400])).await;

        // Range scan that completely misses the dataset
        let options = base_options
            .clone()
            .with_scan_range_before_filter(300..400)
            .unwrap();
        fixture.test_plan(options, &u32s(vec![])).await;
    }

    #[test_log::test(tokio::test)]
    async fn test_batch_size() {
        let fixture = TestFixture::new().await;

        // First, test with the default batch size, which is bigger than any fragment in our
        // test dataset (we have tests for larger batch sizes in python, let's avoid duplicating
        // them here)
        let base_options = FilteredReadOptions::basic_full_read(&fixture.dataset);

        let plan = fixture.make_plan(base_options.clone()).await;

        let stream = plan.execute(0, Arc::new(TaskContext::default())).unwrap();
        let batches = stream.try_collect::<Vec<_>>().await.unwrap();
        let batch_sizes = batches.iter().map(|b| b.num_rows()).collect::<Vec<_>>();
        assert_eq!(batch_sizes, vec![100, 50, 100]);

        // Now, test with a batch size that is smaller than any fragment in our
        // test dataset
        let options = base_options.with_batch_size(35);

        let plan = fixture.make_plan(options).await;

        let stream = plan.execute(0, Arc::new(TaskContext::default())).unwrap();
        let batches = stream.try_collect::<Vec<_>>().await.unwrap();
        let batch_sizes = batches.iter().map(|b| b.num_rows()).collect::<Vec<_>>();

        // Some batches will be smaller because we don't coalesce batches across fragments
        assert_eq!(batch_sizes, vec![35, 35, 30, 35, 15, 35, 35, 30]);
    }

    #[test_log::test(tokio::test)]
    async fn test_recheck() {
        let fixture = TestFixture::new().await;

        // First, test with the default batch size, which is bigger than any fragment in our
        // test dataset (we have tests for larger batch sizes in python, let's avoid duplicating
        // them here)
        let base_options = FilteredReadOptions::basic_full_read(&fixture.dataset);

        let filter_plan = fixture
            .filter_plan("contains(recheck_idx, 'cat')", true)
            .await;

        let options = base_options.clone().with_filter_plan(filter_plan);
        let plan = fixture.make_plan(options).await;

        let stream = plan.execute(0, Arc::new(TaskContext::default())).unwrap();
        let batches = stream.try_collect::<Vec<_>>().await.unwrap();
        let batch_sizes = batches.iter().map(|b| b.num_rows()).collect::<Vec<_>>();
        assert_eq!(batch_sizes, vec![67]);
    }

    #[test_log::test(tokio::test)]
    async fn test_projection() {
        let fixture = Arc::new(TestFixture::new().await);

        // By default we get all columns
        let base_options = FilteredReadOptions::basic_full_read(&fixture.dataset);

        let check_projection =
            |projection: Option<Projection>, expected_columns: Vec<&'static str>| {
                let fixture = fixture.clone();
                let base_options = base_options.clone();
                async move {
                    let mut options = base_options.clone();
                    if let Some(projection) = projection {
                        options = options.with_projection(projection);
                    }
                    let plan = fixture.make_plan(options).await;

                    let stream = plan.execute(0, Arc::new(TaskContext::default())).unwrap();
                    let batches = stream.try_collect::<Vec<_>>().await.unwrap();
                    for batch in batches {
                        assert_eq!(batch.num_columns(), expected_columns.len());
                        for (i, col) in batch.schema().fields().iter().enumerate() {
                            assert_eq!(col.name(), expected_columns[i]);
                        }
                    }
                }
            };

        check_projection(
            None,
            vec![
                "fully_indexed",
                "partly_indexed",
                "not_indexed",
                "recheck_idx",
                "vector",
            ],
        )
        .await;
        let projection = fixture
            .dataset
            .empty_projection()
            .union_column("fully_indexed", OnMissing::Error)
            .unwrap();
        check_projection(Some(projection), vec!["fully_indexed"]).await;
        let row_id_only = fixture.dataset.empty_projection().with_row_id();
        check_projection(Some(row_id_only), vec!["_rowid"]).await;
        let row_addr_only = fixture.dataset.empty_projection().with_row_addr();
        check_projection(Some(row_addr_only), vec!["_rowaddr"]).await;
        let everything = fixture
            .dataset
            .full_projection()
            .with_row_addr()
            .with_row_id();
        check_projection(
            Some(everything),
            vec![
                "fully_indexed",
                "partly_indexed",
                "not_indexed",
                "recheck_idx",
                "vector",
                "_rowid",
                "_rowaddr",
            ],
        )
        .await;

        // It is an error to scan an empty projection
        let options = base_options
            .clone()
            .with_projection(fixture.dataset.empty_projection());
        let index_input = fixture.index_input(&options).await;
        let Err(Error::InvalidInput { source, .. }) =
            FilteredReadExec::try_new(fixture.dataset.clone(), options, index_input)
        else {
            panic!("Expected an InvalidInput error when given an empty projection");
        };
        assert!(source.to_string().contains("no columns were selected"));
    }

    #[test_log::test(tokio::test)]
    async fn test_filter_no_scalar_index() {
        let fixture = Arc::new(TestFixture::new().await);

        let base_options = FilteredReadOptions::basic_full_read(&fixture.dataset);

        // Basic filter
        let filter_plan = fixture.filter_plan("not_indexed >= 75", false).await;
        let options = base_options.clone().with_filter_plan(filter_plan);
        fixture
            .test_plan(options, &u32s(vec![75..100, 250..400]))
            .await;

        // Filter matches no rows
        let filter_plan = fixture.filter_plan("not_indexed >= 1000", false).await;
        let options = base_options.clone().with_filter_plan(filter_plan);
        fixture.test_plan(options, &u32s(vec![])).await;

        // Filter with before_filter scan range
        let filter_plan = fixture.filter_plan("not_indexed >= 75", false).await;
        let options = base_options
            .clone()
            .with_scan_range_before_filter(25..125)
            .unwrap()
            .with_filter_plan(filter_plan);
        fixture
            .test_plan(options, &u32s(vec![75..100, 250..275]))
            .await;

        // Filter removes all rows specified by the scan range
        let filter_plan = fixture.filter_plan("not_indexed >= 75", false).await;
        let options = base_options
            .clone()
            .with_scan_range_before_filter(25..50)
            .unwrap()
            .with_filter_plan(filter_plan);
        fixture.test_plan(options, &u32s(vec![])).await;

        // Can filter on columns with scalar index info, if use_scalar_index is false
        let filter_plan = fixture.filter_plan("fully_indexed >= 200", false).await;
        let options = base_options.clone().with_filter_plan(filter_plan);
        fixture.test_plan(options, &u32s(vec![250..400])).await;
    }

    #[test_log::test(tokio::test)]
    async fn test_filter_scalar_index() {
        let fixture = Arc::new(TestFixture::new().await);

        let base_options = FilteredReadOptions::basic_full_read(&fixture.dataset);

        for index in ["fully_indexed", "partly_indexed"] {
            // Basic filter
            let filter_plan = fixture.filter_plan(&format!("{index} >= 200"), true).await;
            let options = base_options.clone().with_filter_plan(filter_plan);
            fixture.test_plan(options, &u32s(vec![250..400])).await;

            let filter_plan = fixture
                .filter_plan(&format!("{index} >= 230 AND {index} < 270"), true)
                .await;
            let options = base_options.clone().with_filter_plan(filter_plan);
            fixture.test_plan(options, &u32s(vec![250..270])).await;

            // Filter with before filter scan range
            let filter_plan = fixture.filter_plan(&format!("{index} < 270"), true).await;
            let options = base_options
                .clone()
                .with_scan_range_before_filter(25..125)
                .unwrap()
                .with_filter_plan(filter_plan);
            fixture
                .test_plan(options, &u32s(vec![25..100, 250..270]))
                .await;

            // Query asks for a subset of columns that does not include the
            // filter columns.
            let filter_plan = fixture.filter_plan(&format!("{index} >= 200"), true).await;
            let options = base_options
                .clone()
                .with_projection(
                    fixture
                        .dataset
                        .empty_projection()
                        .union_column("not_indexed", OnMissing::Error)
                        .unwrap(),
                )
                .with_filter_plan(filter_plan);
            fixture.test_plan(options, &u32s(vec![250..400])).await;
        }
    }

    #[test_log::test(tokio::test)]
    async fn test_filter_empty_batches() {
        let fixture = Arc::new(TestFixture::new().await);

        let base_options = FilteredReadOptions::basic_full_read(&fixture.dataset);

        let filter_plan = fixture.filter_plan("not_indexed == 317", false).await;
        let options = base_options
            .clone()
            .with_filter_plan(filter_plan)
            .with_batch_size(10);

        let plan = fixture.make_plan(options).await;

        let stream = plan.execute(0, Arc::new(TaskContext::default())).unwrap();
        let batches = stream.try_collect::<Vec<_>>().await.unwrap();

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
    }

    #[test_log::test(tokio::test)]
    async fn test_with_deleted_rows() {
        let fixture = Arc::new(TestFixture::new().await);

        let base_options = FilteredReadOptions::basic_full_read(&fixture.dataset);

        // Basic full scan
        fixture
            .test_plan(
                base_options.clone().with_deleted_rows().unwrap(),
                &u32s(vec![0..100, 200..400]),
            )
            .await;

        // With only row id
        let options = base_options
            .clone()
            .with_deleted_rows()
            .unwrap()
            .with_projection(fixture.dataset.empty_projection().with_row_id());
        let plan = fixture.make_plan(options).await;
        let stream = plan.execute(0, Arc::new(TaskContext::default())).unwrap();
        let num_rows = stream
            .map_ok(|batch| batch.num_rows())
            .try_fold(0, |acc, val| std::future::ready(Ok(acc + val)))
            .await
            .unwrap();
        assert_eq!(num_rows, 300);
    }

    /// A stale (not rebuilt after a delete) index hit drops on the live view
    /// and returns as a null-_rowid tombstone with with_deleted_rows
    #[test_log::test(tokio::test)]
    async fn test_with_deleted_rows_stale_index() {
        let fixture = Arc::new(TestFixture::new().await);
        let base_options = FilteredReadOptions::basic_full_read(&fixture.dataset);

        // Row 220 is deletion-vector-deleted but still in the index
        let filter_plan = fixture.filter_plan("fully_indexed == 220", true).await;

        // Live view: the stale index hit drops
        fixture
            .test_plan(
                base_options.clone().with_filter_plan(filter_plan),
                &u32s(vec![]),
            )
            .await;

        // Physical view: the tombstone returns
        let filter_plan = fixture.filter_plan("fully_indexed == 220", true).await;
        let options = base_options
            .with_deleted_rows()
            .unwrap()
            .with_filter_plan(filter_plan);
        let plan = fixture.make_plan(options).await;
        let stream = plan.execute(0, Arc::new(TaskContext::default())).unwrap();
        let batches = stream.try_collect::<Vec<_>>().await.unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 1);
        let batch = batches.iter().find(|b| b.num_rows() > 0).unwrap();
        let values = batch
            .column_by_name("fully_indexed")
            .unwrap()
            .as_primitive::<UInt32Type>();
        assert_eq!(values.value(0), 220);
        let row_ids = batch
            .column_by_name(ROW_ID)
            .unwrap()
            .as_primitive::<UInt64Type>();
        assert!(row_ids.is_null(0));
    }

    #[test]
    fn test_dv_to_ranges() {
        let dv = Arc::new(DeletionVector::from_iter(vec![1]));
        let ranges = DvToValidRanges::new(dv.iter().map(|i| i as u64), 2).collect::<Vec<_>>();
        assert_eq!(ranges, vec![0..1]);
    }

    #[tokio::test]
    async fn test_statistics() {
        let fixture = Arc::new(TestFixture::new().await);

        let base_options = FilteredReadOptions::basic_full_read(&fixture.dataset);

        let plan = fixture.make_plan(base_options.clone()).await;

        let stats = plan.partition_statistics(None).unwrap();
        // With no filter and no range we have an exact count
        assert_eq!(stats.num_rows, Precision::Exact(250));

        // No filter with range (before or after) is still exact
        let options = base_options
            .clone()
            .with_scan_range_before_filter(25..125)
            .unwrap();
        let plan = fixture.make_plan(options).await;
        let stats = plan.partition_statistics(None).unwrap();
        assert_eq!(stats.num_rows, Precision::Exact(100));

        // With a filter, we don't know the exact count but DF can make some guesses

        // In this case DF recognizes the expression as simple and without column stats it errs on
        // the side of nothing getting filtered out.
        let options = base_options
            .clone()
            .with_filter_plan(fixture.filter_plan("not_indexed >= 200", false).await);
        let plan = fixture.make_plan(options).await;
        let stats = plan.partition_statistics(None).unwrap();
        assert_eq!(stats.num_rows, Precision::Inexact(250));

        // In this case DF doesn't recognize the expression as simple and so it assumes a default
        // selectivity of 0.2
        let options = base_options
            .clone()
            .with_filter_plan(fixture.filter_plan("random() < 0.5", false).await);
        let plan = fixture.make_plan(options).await;
        let stats = plan.partition_statistics(None).unwrap();
        assert_eq!(stats.num_rows, Precision::Inexact(50));

        // Filter columns not part of projection, make sure statistics using correct input schema
        let options = base_options
            .clone()
            .with_filter_plan(fixture.filter_plan("not_indexed >= 200", false).await)
            .with_projection(
                fixture
                    .dataset
                    .empty_projection()
                    // Loading a vector here regresses a bug found during development where the input schema
                    // to the filter exec in statistics was incorrect.
                    .union_column("vector", OnMissing::Error)
                    .unwrap(),
            );
        let plan = fixture.make_plan(options).await;
        let stats = plan.partition_statistics(None).unwrap();
        assert_eq!(stats.num_rows, Precision::Inexact(250));
        assert_eq!(stats.column_statistics.len(), 1);
    }

    #[test_log::test(tokio::test)]
    async fn test_limit_offset_with_deleted_rows() {
        // This test reproduces the issue from the Python test_limit_offset[stable] failure
        // Create a simple dataset with 10 rows (0-9)
        let tmp_path = TempStrDir::default();
        let mut dataset = gen_batch()
            .col("a", array::step::<UInt32Type>())
            .into_dataset(
                tmp_path.as_str(),
                FragmentCount::from(1),
                FragmentRowCount::from(10),
            )
            .await
            .unwrap();

        // Delete rows where a > 2 AND a < 7 (should delete a=3,4,5,6)
        // This leaves: a=0,1,2,7,8,9
        dataset.delete("a > 2 AND a < 7").await.unwrap();
        let dataset = Arc::new(dataset);

        // Test offset=3, limit=1 which should return a=7 (the 4th remaining row)
        let base_options = FilteredReadOptions::basic_full_read(&dataset);
        let options = base_options.with_scan_range_before_filter(3..4).unwrap();

        let plan = FilteredReadExec::try_new(dataset.clone(), options, None).unwrap();
        let stream = plan.execute(0, Arc::new(TaskContext::default())).unwrap();
        let schema = stream.schema();
        let batches = stream.try_collect::<Vec<_>>().await.unwrap();
        let batch = concat_batches(&schema, &batches).unwrap();

        // This should return 1 row with a=7
        assert_eq!(
            batch.num_rows(),
            1,
            "Expected 1 row but got {}",
            batch.num_rows()
        );

        if batch.num_rows() > 0 {
            let col = batch.column(0).as_primitive::<UInt32Type>();
            assert_eq!(col.value(0), 7, "Expected a=7 but got a={}", col.value(0));
        }
    }

    #[test]
    fn test_trim_ranges() {
        let ranges = vec![0..10, 15..25, 30..40];

        assert_eq!(
            FilteredReadStream::trim_ranges(ranges.clone(), 0..25, &(0..10)),
            vec![0..10]
        );

        assert_eq!(
            FilteredReadStream::trim_ranges(ranges.clone(), 0..25, &(10..15)),
            vec![15..20]
        );

        assert_eq!(
            FilteredReadStream::trim_ranges(ranges.clone(), 0..25, &(15..20)),
            vec![20..25]
        );

        assert_eq!(
            FilteredReadStream::trim_ranges(ranges, 0..25, &(15..25)),
            vec![20..25, 30..35]
        );
    }

    #[test]
    fn test_full_frag_range() {
        let dv = Arc::new(DeletionVector::Set(HashSet::from_iter([
            13, 52, 51, 51, 17,
        ])));
        let ranges = FilteredReadStream::full_frag_range(53, &Some(dv));
        let expected = vec![0..13, 14..17, 18..51];
        assert_eq!(ranges, expected);
    }

    #[test]
    fn test_trim_ranges_by_offset() {
        // Test case 1: No skip, take all
        let mut ranges = vec![0..10, 20..30, 40..50];
        let expected = ranges.clone();
        FilteredReadStream::trim_ranges_by_offset(&mut ranges, 0, 100);
        assert_eq!(ranges, expected);

        // Test case 2: Skip some, take all remaining
        let mut ranges = vec![0..10, 20..30, 40..50];
        FilteredReadStream::trim_ranges_by_offset(&mut ranges, 5, 100);
        assert_eq!(ranges, vec![5..10, 20..30, 40..50]);

        // Test case 3: Skip first range entirely
        let mut ranges = vec![0..10, 20..30, 40..50];
        FilteredReadStream::trim_ranges_by_offset(&mut ranges, 10, 100);
        assert_eq!(ranges, vec![20..30, 40..50]);

        // Test case 4: Skip into second range
        let mut ranges = vec![0..10, 20..30, 40..50];
        FilteredReadStream::trim_ranges_by_offset(&mut ranges, 15, 100);
        assert_eq!(ranges, vec![25..30, 40..50]);

        // Test case 5: Take limited rows
        let mut ranges = vec![0..10, 20..30, 40..50];
        FilteredReadStream::trim_ranges_by_offset(&mut ranges, 0, 15);
        assert_eq!(ranges, vec![0..10, 20..25]);

        // Test case 6: Skip and take limited
        let mut ranges = vec![0..10, 20..30, 40..50];
        FilteredReadStream::trim_ranges_by_offset(&mut ranges, 5, 10);
        assert_eq!(ranges, vec![5..10, 20..25]);

        // Test case 7: Skip all
        let mut ranges = vec![0..10, 20..30, 40..50];
        FilteredReadStream::trim_ranges_by_offset(&mut ranges, 100, 10);
        assert_eq!(ranges, vec![]);

        // Test case 8: Take 0
        let mut ranges = vec![0..10, 20..30, 40..50];
        FilteredReadStream::trim_ranges_by_offset(&mut ranges, 0, 0);
        assert_eq!(ranges, vec![]);
    }

    #[tokio::test]
    async fn test_with_fetch_limit_pushdown() {
        // Test that with_fetch() properly updates scan ranges for limit pushdown
        let fixture = Arc::new(TestFixture::new().await);
        let base_options = FilteredReadOptions::basic_full_read(&fixture.dataset);

        // Case 1: No filter, no existing scan_range - should set scan_range_before_filter
        {
            let plan = fixture.make_plan(base_options.clone()).await;
            assert_eq!(plan.options().scan_range_before_filter, None);
            assert_eq!(plan.fetch(), None);
            let new_plan = plan.with_fetch(Some(100)).unwrap();
            let new_plan = new_plan.downcast_ref::<FilteredReadExec>().unwrap();
            assert_eq!(new_plan.options().scan_range_before_filter, Some(0..100));
            assert_eq!(new_plan.fetch(), Some(100));
        }

        // Case 2: No filter with existing scan_range_before_filter - should reject (return None)
        {
            let options = base_options
                .clone()
                .with_scan_range_before_filter(50..200)
                .unwrap();
            let plan = fixture.make_plan(options).await;
            assert_eq!(plan.options().scan_range_before_filter, Some(50..200));
            assert_eq!(plan.fetch(), Some(150));

            // Should return None because scan_range_before_filter already exists
            let result = plan.with_fetch(Some(80));
            assert!(result.is_none());
        }

        // Case 3: With filter, no existing scan_range_after_filter - should set scan_range_after_filter
        {
            let filter_plan = fixture.filter_plan("fully_indexed < 200", false).await;
            let options = base_options.clone().with_filter_plan(filter_plan);
            let plan = fixture.make_plan(options).await;
            assert_eq!(plan.options().scan_range_after_filter, None);
            assert_eq!(plan.fetch(), None);
            let new_plan = plan.with_fetch(Some(50)).unwrap();
            let new_plan = new_plan.downcast_ref::<FilteredReadExec>().unwrap();
            assert_eq!(new_plan.options().scan_range_after_filter, Some(0..50));
            assert_eq!(new_plan.fetch(), Some(50));
        }

        // Case 4: With filter and existing scan_range_after_filter - should reject (return None)
        {
            let filter_plan = fixture.filter_plan("fully_indexed < 200", false).await;
            let options = base_options
                .clone()
                .with_filter_plan(filter_plan)
                .with_scan_range_after_filter(100..300)
                .unwrap();
            let plan = fixture.make_plan(options).await;
            assert_eq!(plan.options().scan_range_after_filter, Some(100..300));

            // Should return None because scan_range_after_filter already exists
            let result = plan.with_fetch(Some(50));
            assert!(result.is_none());
        }

        // Case 5: Multiple partitions mode - with_fetch should reject pushdown
        {
            let mut options = base_options.clone();
            options.threading_mode = FilteredReadThreadingMode::MultiplePartitions(4);
            let filter_plan = fixture.filter_plan("fully_indexed < 200", false).await;
            options = options.with_filter_plan(filter_plan);
            let plan = fixture.make_plan(options).await;
            let result = plan.with_fetch(Some(100));
            assert!(result.is_none());
        }

        // Case 6: None limit value - should be rejected
        {
            let plan = fixture.make_plan(base_options.clone()).await;
            let result = plan.with_fetch(None);
            assert!(result.is_none());
        }
    }

    #[tokio::test]
    async fn test_with_fetch_limit_after_scalar_index_refine_filter() {
        let fixture = Arc::new(TestFixture::new().await);
        let base_options = FilteredReadOptions::basic_full_read(&fixture.dataset);
        let filter_plan = fixture
            .filter_plan("fully_indexed < 50 AND not_indexed >= 10", true)
            .await;
        let options = base_options.with_filter_plan(filter_plan);
        let plan = fixture.make_plan(options).await;

        assert!(plan.index_input().is_some());
        assert!(plan.options().refine_filter.is_some());

        let limited_plan = plan.with_fetch(Some(10)).unwrap();
        let limited_plan = limited_plan.downcast_ref::<FilteredReadExec>().unwrap();
        assert_eq!(limited_plan.options().scan_range_after_filter, Some(0..10));

        let stream = limited_plan
            .execute(0, Arc::new(TaskContext::default()))
            .unwrap();
        let batches = stream.try_collect::<Vec<_>>().await.unwrap();
        let actual_values = get_fully_indexed_values(batches).await;
        assert_eq!(actual_values, (10..20).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn test_limit_pushdown_comprehensive() {
        let fixture = Arc::new(TestFixture::new().await);
        let base_options = FilteredReadOptions::basic_full_read(&fixture.dataset);

        // Test 1: No index with limit - should pushdown to scan_range_before_filter
        let options = base_options
            .clone()
            .with_scan_range_before_filter(0..100)
            .unwrap();
        let plan = fixture.make_plan(options.clone()).await;
        assert_eq!(plan.options().scan_range_before_filter, Some(0..100));
        assert_eq!(plan.options().scan_range_after_filter, None);
        test_scan_range(&fixture, options, (0..100).collect(), "No index with limit").await;

        // Test 2: Exact match index with limit
        let filter_plan = fixture.filter_plan("fully_indexed < 50", false).await;
        let options = base_options
            .clone()
            .with_filter_plan(filter_plan)
            .with_scan_range_after_filter(0..25)
            .unwrap()
            .with_batch_size(10);
        let plan = fixture.make_plan(options.clone()).await;
        assert_eq!(plan.options().scan_range_after_filter, Some(0..25));
        assert_eq!(plan.options().scan_range_before_filter, None);
        test_scan_range(
            &fixture,
            options,
            (0..25).collect(),
            "Exact match index with limit",
        )
        .await;

        // Test 3: Regression test for batch boundary bug
        let filter_plan = fixture.filter_plan("not_indexed >= 0", false).await;
        let options = base_options
            .with_filter_plan(filter_plan)
            .with_scan_range_after_filter(0..250)
            .unwrap()
            .with_batch_size(50);
        let expected_values: Vec<u32> = (0..100).chain(250..400).take(250).collect();
        test_scan_range(
            &fixture,
            options,
            expected_values,
            "Batch boundary regression",
        )
        .await;
    }

    /// Helper to extract fully_indexed column values from batches
    async fn get_fully_indexed_values(batches: Vec<RecordBatch>) -> Vec<u32> {
        batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column_by_name("fully_indexed")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<arrow_array::UInt32Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .copied()
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    /// Helper to test scan range with expected values
    async fn test_scan_range(
        fixture: &TestFixture,
        options: FilteredReadOptions,
        expected_values: Vec<u32>,
        test_description: &str,
    ) {
        let plan = fixture.make_plan(options).await;
        let stream = plan.execute(0, Arc::new(TaskContext::default())).unwrap();
        let batches = stream.try_collect::<Vec<_>>().await.unwrap();
        let actual_values = get_fully_indexed_values(batches).await;
        assert_eq!(
            actual_values, expected_values,
            "Failed test: {}",
            test_description
        );
    }

    /// Helper to compute expected values for scan range tests
    /// Dataset layout: [0..100] deleted:[100..250] [250..400]
    fn compute_range_values(range: Range<u64>) -> Vec<u32> {
        let mut result = Vec::new();
        for pos in range {
            if pos < 100 {
                result.push(pos as u32);
            } else if pos < 250 {
                // Positions 100-249 map to values 250-399
                result.push((250 + (pos - 100)) as u32);
            }
        }
        result
    }

    #[tokio::test]
    async fn test_no_filter_scan_range_before_filter() {
        let fixture = Arc::new(TestFixture::new().await);
        let base_options = FilteredReadOptions::basic_full_read(&fixture.dataset);

        // Test cases: (scan_range, description)
        let test_cases = vec![
            // Basic cases
            (0..50, "Limit from start"),
            (30..80, "Offset + limit"),
            (0..250, "Limit equals total rows"),
            (0..500, "Limit exceeds total rows"),
            // Edge cases
            (0..1, "Single row"),
            (99..100, "Last row of first fragment"),
            (100..101, "First row of second fragment (deleted area)"),
            (249..250, "Last available row"),
            // Fragment boundaries
            (0..100, "Entire first fragment"),
            (100..200, "Middle of dataset (deleted area)"),
            (50..150, "Across fragment boundary"),
            (90..110, "Around deletion boundary"),
            // Large offsets
            (200..250, "Large offset into second fragment"),
            (240..260, "Near end with overrun"),
            (300..400, "Beyond available data"),
            // Zero-width ranges
            (50..50, "Empty range in data"),
            (150..150, "Empty range in deleted area"),
            (400..400, "Empty range beyond data"),
        ];

        for (range, description) in test_cases {
            let options = base_options
                .clone()
                .with_scan_range_before_filter(range.clone())
                .unwrap();
            let expected = compute_range_values(range);
            test_scan_range(&fixture, options, expected, description).await;
        }
    }

    #[tokio::test]
    async fn test_exact_match_filter_scan_range_after_filter() {
        let fixture = Arc::new(TestFixture::new().await);
        let base_options = FilteredReadOptions::basic_full_read(&fixture.dataset);

        // Test cases: (filter, scan_range, expected_values, description)
        let test_cases = vec![
            // Basic limit tests with diverse ranges
            (
                "fully_indexed < 100",
                0..50,
                (0..50).collect(),
                "Limit < matches",
            ),
            (
                "fully_indexed < 100",
                20..50,
                (20..50).collect(),
                "Offset + limit within matches",
            ),
            (
                "fully_indexed < 100",
                0..100,
                (0..100).collect(),
                "Limit = matches",
            ),
            (
                "fully_indexed < 50",
                0..200,
                (0..50).collect(),
                "Limit > matches",
            ),
            ("fully_indexed < 100", 0..1, vec![0], "Single row"),
            (
                "fully_indexed < 100",
                99..100,
                vec![99],
                "Last matching row",
            ),
            (
                "fully_indexed < 100",
                5..15,
                (5..15).collect(),
                "Small window",
            ),
            (
                "fully_indexed < 100",
                90..110,
                (90..100).collect(),
                "Range beyond matches",
            ),
            (
                "fully_indexed < 100",
                45..55,
                (45..55).collect(),
                "Mid-range window",
            ),
            (
                "fully_indexed < 100",
                0..10000,
                (0..100).collect(),
                "Huge limit",
            ),
            // Range filter tests with more diverse ranges
            (
                "fully_indexed >= 50 AND fully_indexed < 80",
                0..20,
                (50..70).collect(),
                "Range filter with limit",
            ),
            (
                "fully_indexed >= 50 AND fully_indexed < 80",
                10..25,
                (60..75).collect(),
                "Range filter with offset+limit",
            ),
            (
                "fully_indexed >= 50 AND fully_indexed < 80",
                0..30,
                (50..80).collect(),
                "Range filter exact match",
            ),
            (
                "fully_indexed >= 50 AND fully_indexed < 80",
                0..100,
                (50..80).collect(),
                "Range filter limit exceeds",
            ),
            (
                "fully_indexed >= 50 AND fully_indexed < 80",
                0..5,
                (50..55).collect(),
                "First 5 rows",
            ),
            (
                "fully_indexed >= 50 AND fully_indexed < 80",
                25..30,
                (75..80).collect(),
                "Last 5 rows",
            ),
            (
                "fully_indexed >= 50 AND fully_indexed < 80",
                15..16,
                vec![65],
                "Single row middle",
            ),
            (
                "fully_indexed >= 50 AND fully_indexed < 80",
                2..8,
                (52..58).collect(),
                "Small offset window",
            ),
            (
                "fully_indexed >= 50 AND fully_indexed < 80",
                100..200,
                vec![],
                "Offset beyond data",
            ),
            // Boundary tests
            ("fully_indexed = 0", 0..10, vec![0], "Single value at start"),
            (
                "fully_indexed = 99",
                0..10,
                vec![99],
                "Single value at fragment end",
            ),
            (
                "fully_indexed = 250",
                0..10,
                vec![250],
                "Single value at second fragment start",
            ),
            (
                "fully_indexed = 399",
                0..10,
                vec![399],
                "Single value at dataset end",
            ),
            // Empty result tests
            (
                "fully_indexed = 150",
                0..10,
                vec![],
                "No match in deleted range",
            ),
            (
                "fully_indexed > 500",
                0..100,
                vec![],
                "No match beyond data",
            ),
            // Fragment boundary tests with diverse ranges
            (
                "fully_indexed > 200",
                0..100,
                (250..350).collect(),
                "Filter skips deleted fragment",
            ),
            (
                "fully_indexed >= 250",
                0..50,
                (250..300).collect(),
                "Start of second fragment",
            ),
            (
                "fully_indexed >= 350",
                0..100,
                (350..400).collect(),
                "End of second fragment",
            ),
            (
                "fully_indexed < 400",
                200..250,
                (350..400).collect(),
                "Large offset into second fragment",
            ),
            (
                "fully_indexed >= 250",
                0..1,
                vec![250],
                "First row second fragment",
            ),
            (
                "fully_indexed >= 250",
                149..150,
                vec![399],
                "Last row second fragment",
            ),
            (
                "fully_indexed >= 250",
                10..20,
                (260..270).collect(),
                "Small window in second",
            ),
            (
                "fully_indexed >= 250",
                75..100,
                (325..350).collect(),
                "Middle of second fragment",
            ),
            (
                "fully_indexed >= 250",
                100..200,
                (350..400).collect(),
                "End portion of second",
            ),
            (
                "fully_indexed >= 300",
                25..75,
                (325..375).collect(),
                "Mid to late second fragment",
            ),
            // Complex filters with various ranges
            (
                "fully_indexed IN (5, 15, 25, 35, 45)",
                0..10,
                vec![5, 15, 25, 35, 45],
                "IN clause all",
            ),
            (
                "fully_indexed IN (5, 15, 25, 35, 45)",
                0..3,
                vec![5, 15, 25],
                "IN clause first 3",
            ),
            (
                "fully_indexed IN (5, 15, 25, 35, 45)",
                2..4,
                vec![25, 35],
                "IN clause middle 2",
            ),
            (
                "fully_indexed IN (5, 15, 25, 35, 45)",
                1..5,
                vec![15, 25, 35, 45],
                "IN clause skip first",
            ),
            (
                "fully_indexed % 10 = 0",
                0..15,
                vec![
                    0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 250, 260, 270, 280, 290,
                ],
                "Modulo all",
            ),
            (
                "fully_indexed % 10 = 0",
                0..3,
                vec![0, 10, 20],
                "Modulo first 3",
            ),
            (
                "fully_indexed % 10 = 0",
                5..10,
                vec![50, 60, 70, 80, 90],
                "Modulo middle range",
            ),
            (
                "fully_indexed % 10 = 0",
                8..12,
                vec![80, 90, 250, 260],
                "Modulo cross fragment",
            ),
            (
                "fully_indexed % 10 = 0",
                10..15,
                vec![250, 260, 270, 280, 290],
                "Modulo second fragment",
            ),
            (
                "fully_indexed >= 80 AND fully_indexed <= 280",
                0..50,
                vec![
                    80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99,
                    250, 251, 252, 253, 254, 255, 256, 257, 258, 259, 260, 261, 262, 263, 264, 265,
                    266, 267, 268, 269, 270, 271, 272, 273, 274, 275, 276, 277, 278, 279,
                ],
                "Cross-fragment full",
            ),
            (
                "fully_indexed >= 80 AND fully_indexed <= 280",
                0..10,
                (80..90).collect(),
                "Cross-fragment first 10",
            ),
            (
                "fully_indexed >= 80 AND fully_indexed <= 280",
                15..25,
                vec![95, 96, 97, 98, 99, 250, 251, 252, 253, 254],
                "Cross-fragment boundary",
            ),
            (
                "fully_indexed >= 80 AND fully_indexed <= 280",
                20..40,
                (250..270).collect(),
                "Cross-fragment second only",
            ),
            (
                "fully_indexed >= 80 AND fully_indexed <= 280",
                18..22,
                vec![98, 99, 250, 251],
                "Cross-fragment exact boundary",
            ),
            // Edge cases
            ("fully_indexed < 400", 0..0, vec![], "Zero-width range"),
            (
                "fully_indexed >= 0",
                1000..2000,
                vec![],
                "Huge offset beyond data",
            ),
            (
                "fully_indexed BETWEEN 95 AND 255",
                3..8,
                vec![98, 99, 250, 251, 252],
                "BETWEEN crossing deletion",
            ),
        ];

        for (filter_expr, range, expected, description) in test_cases {
            let filter_plan = fixture.filter_plan(filter_expr, false).await;
            let options = base_options
                .clone()
                .with_filter_plan(filter_plan)
                .with_scan_range_after_filter(range)
                .unwrap();
            test_scan_range(&fixture, options, expected, description).await;
        }
    }

    #[tokio::test]
    async fn test_at_least_match_filter_scan_range_after_filter() {
        let fixture = Arc::new(TestFixture::new().await);
        let base_options = FilteredReadOptions::basic_full_read(&fixture.dataset);

        struct TestCase {
            filter: &'static str,
            scan_range: Range<u64>,
            validate: Box<dyn Fn(Vec<u32>)>,
        }

        let test_cases = vec![
            TestCase {
                filter: "recheck_idx = 'cat'",
                scan_range: 0..30,
                validate: Box::new(|values| {
                    assert!(values.len() <= 30, "Should have at most 30 rows");
                    for val in &values {
                        assert_eq!(*val % 3, 0, "Values should be multiples of 3");
                    }
                }),
            },
            TestCase {
                filter: "recheck_idx = 'cat'",
                scan_range: 10..40,
                validate: Box::new(|values| {
                    assert!(values.len() <= 30, "Should have at most 30 rows");
                    assert!(
                        values[0] > 0,
                        "Should have skipped initial matches due to offset"
                    );
                    for val in &values {
                        assert_eq!(*val % 3, 0, "Values should be multiples of 3");
                    }
                }),
            },
            TestCase {
                filter: "recheck_idx = 'cat' AND fully_indexed < 100",
                scan_range: 0..20,
                validate: Box::new(|values| {
                    assert!(values.len() <= 20, "Should have at most 20 rows");
                    for val in &values {
                        assert!(*val < 100, "Values should be < 100");
                        assert_eq!(*val % 3, 0, "Values should be multiples of 3");
                    }
                }),
            },
        ];

        for test_case in test_cases {
            let filter_plan = fixture.filter_plan(test_case.filter, false).await;
            let options = base_options
                .clone()
                .with_filter_plan(filter_plan)
                .with_scan_range_after_filter(test_case.scan_range)
                .unwrap();

            let plan = fixture.make_plan(options).await;
            let stream = plan.execute(0, Arc::new(TaskContext::default())).unwrap();
            let batches = stream.try_collect::<Vec<_>>().await.unwrap();
            let values = get_fully_indexed_values(batches).await;
            (test_case.validate)(values);
        }
    }

    #[tokio::test]
    async fn test_edge_cases_limit_pushdown() {
        let fixture = Arc::new(TestFixture::new().await);
        let base_options = FilteredReadOptions::basic_full_read(&fixture.dataset);

        // Test 5.1: Batch boundary test (regression for original bug)
        let filter_plan = fixture.filter_plan("not_indexed >= 0", false).await;
        let options = base_options
            .clone()
            .with_filter_plan(filter_plan)
            .with_scan_range_after_filter(0..250)
            .unwrap()
            .with_batch_size(24);
        let plan = fixture.make_plan(options).await;
        let stream = plan.execute(0, Arc::new(TaskContext::default())).unwrap();
        let batches = stream.try_collect::<Vec<_>>().await.unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 250);

        // Test 5.2: Empty result set
        let filter_plan = fixture.filter_plan("fully_indexed < 0", false).await;
        let options = base_options
            .clone()
            .with_filter_plan(filter_plan)
            .with_scan_range_after_filter(0..100)
            .unwrap();
        let plan = fixture.make_plan(options).await;
        let stream = plan.execute(0, Arc::new(TaskContext::default())).unwrap();
        let num_rows = stream
            .map_ok(|batch| batch.num_rows())
            .try_fold(0, |acc, val| std::future::ready(Ok(acc + val)))
            .await
            .unwrap();
        assert_eq!(num_rows, 0);

        // Test 5.3: Offset + Limit combination
        let options = base_options
            .clone()
            .with_scan_range_before_filter(100..150)
            .unwrap();
        let plan = fixture.make_plan(options).await;
        let stream = plan.execute(0, Arc::new(TaskContext::default())).unwrap();
        let batches = stream.try_collect::<Vec<_>>().await.unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 50);

        // Due to fragment deletion, rows 100-199 don't exist
        // Row offset 100 starts at fragment 2 which has values 250+
        let all_values: Vec<u32> = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column_by_name("fully_indexed")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<arrow_array::UInt32Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .copied()
                    .collect::<Vec<_>>()
            })
            .collect();
        let expected: Vec<u32> = (250..300).collect();
        assert_eq!(all_values, expected);
    }

    #[tokio::test]
    async fn test_metrics_with_limit_partial_fragment() {
        let fixture = TestFixture::new().await;
        let options = FilteredReadOptions::basic_full_read(&fixture.dataset).with_batch_size(10);
        let filtered_read =
            Arc::new(FilteredReadExec::try_new(fixture.dataset.clone(), options, None).unwrap());

        let batches = filtered_read
            .execute(0, Arc::new(TaskContext::default()))
            .unwrap()
            .take(3)
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 30);

        // Check metrics reflect partial fragment read
        let metrics = filtered_read.metrics().unwrap();

        // Should show approximately 30 rows scanned (might be slightly more due to buffering)
        // But should be significantly less than full fragment (100 rows)
        let rows_scanned = metrics
            .sum_by_name("rows_scanned")
            .map(|v| v.as_usize())
            .unwrap_or(0);
        assert!(
            (30..100).contains(&rows_scanned),
            "rows_scanned ({}) should be close to limit (30), not full fragment (100)",
            rows_scanned
        );

        // Should show 1 fragment was accessed
        let fragments_scanned = metrics
            .sum_by_name("fragments_scanned")
            .map(|v| v.as_usize())
            .unwrap_or(0);
        assert_eq!(fragments_scanned, 1);

        let ranges_scanned = metrics
            .sum_by_name("ranges_scanned")
            .map(|v| v.as_usize())
            .unwrap_or(0);
        assert!(ranges_scanned > 0, "Should have scanned some ranges");

        // Should have some IO metrics
        let iops = metrics
            .sum_by_name("iops")
            .map(|v| v.as_usize())
            .unwrap_or(0);
        assert!(iops > 0, "Should have recorded IO operations");
    }

    // Reproduces a bug where bytes_read (and iops/requests) stay at 0 when a filter matches
    // no rows. io_metrics.record is only called inside inspect_ok on the output batch stream,
    // so when the filter produces zero output batches, the I/O that did occur is never counted.
    #[tokio::test]
    async fn test_io_metrics_recorded_when_filter_matches_no_rows() {
        let fixture = TestFixture::new().await;
        // not_indexed values in the fixture go up to ~400; this filter matches nothing
        let filter_plan = fixture.filter_plan("not_indexed > 10000", false).await;
        let options =
            FilteredReadOptions::basic_full_read(&fixture.dataset).with_filter_plan(filter_plan);
        let filtered_read =
            Arc::new(FilteredReadExec::try_new(fixture.dataset.clone(), options, None).unwrap());

        let batches = filtered_read
            .execute(0, Arc::new(TaskContext::default()))
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(
            batches.iter().map(|b| b.num_rows()).sum::<usize>(),
            0,
            "filter should match no rows"
        );

        let metrics = filtered_read.metrics().unwrap();

        let rows_scanned = metrics
            .sum_by_name("rows_scanned")
            .map(|v| v.as_usize())
            .unwrap_or(0);
        assert!(
            rows_scanned > 0,
            "rows_scanned ({}) should be > 0: data was read even though filter matched nothing",
            rows_scanned
        );

        let bytes_read = metrics
            .sum_by_name("bytes_read")
            .map(|v| v.as_usize())
            .unwrap_or(0);
        assert!(
            bytes_read > 0,
            "bytes_read ({}) should be > 0: io_metrics.record is only called when output batches \
             are produced, so bytes_read stays 0 even though I/O occurred",
            bytes_read
        );
    }

    /// Test that direct execution gives the same result as get_plan + execute_with_plan
    #[test_log::test(tokio::test)]
    async fn test_plan_batch_trip() {
        let fixture = TestFixture::new().await;
        let ctx = Arc::new(TaskContext::default());

        // Test with filter
        let filter_plan = fixture.filter_plan("fully_indexed = 50", true).await;
        let options = FilteredReadOptions::basic_full_read(&fixture.dataset)
            .with_filter_plan(filter_plan.clone());

        // Path 1: Direct execution (no plan provided)
        let index_input = fixture.index_input(&options).await;
        let exec1 =
            FilteredReadExec::try_new(fixture.dataset.clone(), options.clone(), index_input)
                .unwrap();
        let stream1 = exec1.execute(0, ctx.clone()).unwrap();
        let schema1 = stream1.schema();
        let batches1 = stream1.try_collect::<Vec<_>>().await.unwrap();
        let result1 = concat_batches(&schema1, &batches1).unwrap();

        // Path 2: Get plan first, then create new exec with plan via with_plan
        let index_input = fixture.index_input(&options).await;
        let exec2 =
            FilteredReadExec::try_new(fixture.dataset.clone(), options.clone(), index_input)
                .unwrap();
        let plan = exec2.get_or_create_plan(ctx.clone()).await.unwrap();

        // Create new exec and use with_plan to set the plan
        let index_input = fixture.index_input(&options).await;
        let exec3 =
            FilteredReadExec::try_new(fixture.dataset.clone(), options.clone(), index_input)
                .unwrap()
                .with_plan(plan)
                .await
                .unwrap();
        let stream3 = exec3.execute(0, ctx.clone()).unwrap();
        let schema3 = stream3.schema();
        let batches3 = stream3.try_collect::<Vec<_>>().await.unwrap();
        let result3 = concat_batches(&schema3, &batches3).unwrap();

        // Results should match
        assert_eq!(result1.num_rows(), result3.num_rows());
        assert_eq!(result1.schema(), result3.schema());
        for i in 0..result1.num_columns() {
            assert_eq!(result1.column(i).as_ref(), result3.column(i).as_ref());
        }

        // Test with range scan
        let options = FilteredReadOptions::basic_full_read(&fixture.dataset)
            .with_scan_range_before_filter(10..50)
            .unwrap();

        // Path 1: Direct execution
        let exec1 =
            FilteredReadExec::try_new(fixture.dataset.clone(), options.clone(), None).unwrap();
        let stream1 = exec1.execute(0, ctx.clone()).unwrap();
        let schema1 = stream1.schema();
        let batches1 = stream1.try_collect::<Vec<_>>().await.unwrap();
        let result1 = concat_batches(&schema1, &batches1).unwrap();

        // Path 2: Get plan, then create new exec with_plan
        let exec2 =
            FilteredReadExec::try_new(fixture.dataset.clone(), options.clone(), None).unwrap();
        let plan = exec2.get_or_create_plan(ctx.clone()).await.unwrap();

        let exec3 = FilteredReadExec::try_new(fixture.dataset.clone(), options.clone(), None)
            .unwrap()
            .with_plan(plan)
            .await
            .unwrap();
        let stream3 = exec3.execute(0, ctx.clone()).unwrap();
        let schema3 = stream3.schema();
        let batches3 = stream3.try_collect::<Vec<_>>().await.unwrap();
        let result3 = concat_batches(&schema3, &batches3).unwrap();

        // Results should match
        assert_eq!(result1.num_rows(), result3.num_rows());
        for i in 0..result1.num_columns() {
            assert_eq!(result1.column(i).as_ref(), result3.column(i).as_ref());
        }
    }

    /// Verify that executing with target_partitions=1 produces the same results as the default
    /// context and does not panic. This is a regression guard for the parallelism cap.
    #[test_log::test(tokio::test)]
    async fn test_target_partitions_cap_produces_correct_results() {
        use datafusion::prelude::SessionConfig;

        let fixture = TestFixture::new().await;

        let options = FilteredReadOptions::basic_full_read(&fixture.dataset);
        let plan =
            FilteredReadExec::try_new(fixture.dataset.clone(), options.clone(), None).unwrap();

        // Execute with default context (high thread count)
        let default_ctx = Arc::new(TaskContext::default());
        let stream = plan.execute(0, default_ctx).unwrap();
        let schema = stream.schema();
        let batches = stream.try_collect::<Vec<_>>().await.unwrap();
        let default_result = concat_batches(&schema, &batches).unwrap();

        // Execute fresh plan with target_partitions=1
        let plan2 = FilteredReadExec::try_new(fixture.dataset.clone(), options, None).unwrap();
        let low_ctx = Arc::new(
            TaskContext::default()
                .with_session_config(SessionConfig::default().with_target_partitions(1)),
        );
        let stream2 = plan2.execute(0, low_ctx).unwrap();
        let schema2 = stream2.schema();
        let batches2 = stream2.try_collect::<Vec<_>>().await.unwrap();
        let capped_result = concat_batches(&schema2, &batches2).unwrap();

        assert_eq!(default_result.num_rows(), capped_result.num_rows());
    }

    // Row-stream selector tests

    mod row_stream {
        use super::*;
        use arrow_array::{Float32Array, StringArray, UInt64Array};
        use arrow_schema::{DataType, Field as ArrowField, Fields, Schema as ArrowSchema};
        use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
        use lance_datafusion::exec::OneShotExec;
        use rstest::rstest;

        use crate::dataset::{Dataset, WriteParams};
        use crate::utils::test::NoContextTestFixture;

        struct TakeFixture {
            dataset: Arc<Dataset>,
            _tmp_dir: TempStrDir,
        }

        /// 30 rows across 3 fragments with columns i, s, and struct{x, y}
        async fn take_fixture(stable_row_ids: bool) -> TakeFixture {
            let struct_fields = Fields::from(vec![
                Arc::new(ArrowField::new("x", DataType::Int32, false)),
                Arc::new(ArrowField::new("y", DataType::Int32, false)),
            ]);
            let schema = Arc::new(ArrowSchema::new(vec![
                ArrowField::new("i", DataType::Int32, false),
                ArrowField::new("s", DataType::Utf8, false),
                ArrowField::new("struct", DataType::Struct(struct_fields.clone()), false),
            ]));
            let batches: Vec<RecordBatch> = (0..3)
                .map(|batch_id| {
                    let value_range = batch_id * 10..batch_id * 10 + 10;
                    RecordBatch::try_new(
                        schema.clone(),
                        vec![
                            Arc::new(Int32Array::from_iter_values(value_range.clone())),
                            Arc::new(StringArray::from_iter_values(
                                value_range.clone().map(|v| format!("s-{v}")),
                            )),
                            Arc::new(arrow_array::StructArray::new(
                                struct_fields.clone(),
                                vec![
                                    Arc::new(Int32Array::from_iter(value_range.clone())),
                                    Arc::new(Int32Array::from_iter(value_range)),
                                ],
                                None,
                            )),
                        ],
                    )
                    .unwrap()
                })
                .collect();

            let tmp_dir = TempStrDir::default();
            let uri = tmp_dir.as_str();
            let params = WriteParams {
                max_rows_per_file: 10,
                enable_stable_row_ids: stable_row_ids,
                ..Default::default()
            };
            let reader = RecordBatchIterator::new(batches.into_iter().map(Ok), schema);
            Dataset::write(reader, uri, Some(params)).await.unwrap();
            TakeFixture {
                dataset: Arc::new(Dataset::open(uri).await.unwrap()),
                _tmp_dir: tmp_dir,
            }
        }

        /// Wrap batches of (payload, key) rows into an input plan
        fn rows_input(batches: Vec<RecordBatch>) -> Arc<dyn ExecutionPlan> {
            let schema = batches[0].schema();
            let stream = futures::stream::iter(batches.into_iter().map(Ok));
            let stream = Box::pin(RecordBatchStreamAdapter::new(schema, stream));
            Arc::new(OneShotExec::new(stream))
        }

        fn take_plan(
            dataset: &Arc<Dataset>,
            input: Arc<dyn ExecutionPlan>,
            columns: &[&str],
        ) -> Result<FilteredReadExec> {
            let projection = dataset
                .empty_projection()
                .union_columns(columns, OnMissing::Error)
                .unwrap();
            FilteredReadExec::try_new(
                dataset.clone(),
                FilteredReadOptions::new(projection),
                Some(input),
            )
        }

        fn take_plan_sized(
            dataset: &Arc<Dataset>,
            input: Arc<dyn ExecutionPlan>,
            columns: &[&str],
            batch_size: u32,
        ) -> Result<FilteredReadExec> {
            let projection = dataset
                .empty_projection()
                .union_columns(columns, OnMissing::Error)
                .unwrap();
            FilteredReadExec::try_new(
                dataset.clone(),
                FilteredReadOptions::new(projection).with_batch_size(batch_size),
                Some(input),
            )
        }

        async fn run(plan: &FilteredReadExec) -> Vec<RecordBatch> {
            plan.execute(0, Arc::new(TaskContext::default()))
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
                .unwrap()
        }

        /// A sparse plan constructs fragment handles only for the fragments
        /// it selects, keeping their candidate-list position as priority —
        /// no metadata is loaded or retained for unselected fragments
        #[tokio::test]
        async fn sparse_plan_scopes_only_selected_fragments() {
            let fixture = take_fixture(false).await;
            let dataset = &fixture.dataset;
            let descriptors = dataset.fragments().clone();
            assert_eq!(descriptors.len(), 3);

            let mut rows = BTreeMap::new();
            rows.insert(2u32, vec![0u64..5]);
            let plan = FilteredReadInternalPlan {
                rows,
                filters: HashMap::new(),
                scan_range_after_filter: None,
            };
            let options = FilteredReadOptions::basic_full_read(dataset);
            let scheduler = FilteredReadStream::make_scan_scheduler(dataset, &options);

            let scoped = FilteredReadStream::plan_to_scoped_fragments(
                &plan,
                &descriptors,
                dataset,
                &options,
                scheduler,
            );
            assert_eq!(scoped.len(), 1);
            assert_eq!(scoped[0].fragment.id(), 2);
            assert_eq!(scoped[0].priority, 2);
        }

        /// Output preserves the input's row order, duplicates, and payload
        #[rstest]
        #[case::by_row_addr(false, ROW_ADDR)]
        #[case::by_row_id(false, ROW_ID)]
        #[case::stable_by_row_addr(true, ROW_ADDR)]
        #[case::stable_by_row_id(true, ROW_ID)]
        #[tokio::test]
        async fn take_preserves_order_dups_and_payload(
            #[case] stable_row_ids: bool,
            #[case] key: &str,
        ) {
            let fixture = take_fixture(stable_row_ids).await;

            // Stable row ids are assigned sequentially on write, so the id of
            // row `i` is `i`; without them id == address
            let addr = |frag: u64, off: u64| (frag << 32) | off;
            let keys: Vec<u64> = if key == ROW_ID && stable_row_ids {
                vec![21, 3, 15, 21, 0]
            } else {
                vec![
                    addr(2, 1), // i = 21
                    addr(0, 3), // i = 3
                    addr(1, 5), // i = 15
                    addr(2, 1), // i = 21 (duplicate)
                    addr(0, 0), // i = 0
                ]
            };
            let expected_i: Vec<i32> = vec![21, 3, 15, 21, 0];

            let input_schema = Arc::new(ArrowSchema::new(vec![
                ArrowField::new("payload", DataType::Float32, false),
                ArrowField::new(key, DataType::UInt64, true),
            ]));
            let payload: Vec<f32> = (0..keys.len()).map(|v| v as f32 * 0.5).collect();
            let batch = RecordBatch::try_new(
                input_schema.clone(),
                vec![
                    Arc::new(Float32Array::from(payload.clone())),
                    Arc::new(UInt64Array::from(keys.clone())),
                ],
            )
            .unwrap();
            let batches = vec![batch.slice(0, 3), batch.slice(3, 2)];

            let plan = take_plan(&fixture.dataset, rows_input(batches), &["s", "i"]).unwrap();
            assert!(plan.row_stream_input().is_some());
            // Input columns, then new fields; the unrequested key is stripped
            assert_eq!(
                plan.schema()
                    .fields()
                    .iter()
                    .map(|f| f.name().clone())
                    .collect::<Vec<_>>(),
                vec!["payload", "i", "s"]
            );

            let result = run(&plan).await;
            assert_eq!(result.len(), 1);
            assert_eq!(result[0].num_rows(), 5);
            let result = concat_batches(&plan.schema(), &result).unwrap();

            let i_col = result.column_by_name("i").unwrap();
            assert_eq!(
                i_col.as_primitive::<arrow::datatypes::Int32Type>().values(),
                &expected_i[..]
            );
            let s_col = result.column_by_name("s").unwrap().as_string::<i32>();
            for (row, i) in expected_i.iter().enumerate() {
                assert_eq!(s_col.value(row), format!("s-{i}"));
            }
            let payload_col = result
                .column_by_name("payload")
                .unwrap()
                .as_primitive::<Float32Type>();
            assert_eq!(payload_col.values(), &payload[..]);
        }

        /// Tiny input batches merge up to the target and oversized ones pass
        /// through whole, preserving order across the boundaries
        #[tokio::test]
        async fn take_coalesces_input_to_batch_size() {
            let fixture = take_fixture(false).await;

            let addr = |frag: u64, off: u64| (frag << 32) | off;
            let keys: Vec<u64> = vec![
                addr(2, 3), // i = 23
                addr(0, 1), // i = 1
                addr(1, 4), // i = 14
                addr(0, 7), // i = 7
                addr(2, 0), // i = 20
                addr(1, 1), // i = 11
                addr(0, 2), // i = 2
            ];
            let expected_i: Vec<i32> = vec![23, 1, 14, 7, 20, 11, 2];
            let input_schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
                ROW_ADDR,
                DataType::UInt64,
                true,
            )]));
            let batch = RecordBatch::try_new(
                input_schema.clone(),
                vec![Arc::new(UInt64Array::from(keys.clone()))],
            )
            .unwrap();

            let assert_batches =
                |result: Vec<RecordBatch>, schema: SchemaRef, expected_sizes: Vec<usize>| {
                    assert_eq!(
                        result.iter().map(|b| b.num_rows()).collect::<Vec<_>>(),
                        expected_sizes
                    );
                    let merged = concat_batches(&schema, &result).unwrap();
                    let i_col = merged.column_by_name("i").unwrap();
                    assert_eq!(
                        i_col.as_primitive::<arrow::datatypes::Int32Type>().values(),
                        &expected_i[..]
                    );
                };

            // Seven one-row batches merge whenever the buffer reaches 3 rows
            let tiny = (0..7).map(|i| batch.slice(i, 1)).collect::<Vec<_>>();
            let plan = take_plan_sized(&fixture.dataset, rows_input(tiny), &["i"], 3).unwrap();
            assert_batches(run(&plan).await, plan.schema(), vec![3, 3, 1]);

            // One oversized batch passes through whole — never split
            let plan =
                take_plan_sized(&fixture.dataset, rows_input(vec![batch.clone()]), &["i"], 3)
                    .unwrap();
            assert_batches(run(&plan).await, plan.schema(), vec![7]);

            // A large batch flushes the partial buffer and passes through
            let mixed = vec![batch.slice(0, 2), batch.slice(2, 5)];
            let plan = take_plan_sized(&fixture.dataset, rows_input(mixed), &["i"], 3).unwrap();
            assert_batches(run(&plan).await, plan.schema(), vec![2, 5]);
        }

        /// Storage-ordered input exercises the aligned fast path
        #[rstest]
        #[case::by_row_addr(ROW_ADDR)]
        #[case::by_row_id(ROW_ID)]
        #[tokio::test]
        async fn take_aligned_input_fast_path(#[case] key: &str) {
            let fixture = take_fixture(false).await;

            let addr = |frag: u64, off: u64| (frag << 32) | off;
            let keys: Vec<u64> = vec![
                addr(0, 0), // i = 0
                addr(0, 3), // i = 3
                addr(1, 5), // i = 15
                addr(2, 1), // i = 21
                addr(2, 9), // i = 29
            ];
            let expected_i: Vec<i32> = vec![0, 3, 15, 21, 29];

            let input_schema = Arc::new(ArrowSchema::new(vec![
                ArrowField::new("payload", DataType::Float32, false),
                ArrowField::new(key, DataType::UInt64, true),
            ]));
            let payload: Vec<f32> = (0..keys.len()).map(|v| v as f32 * 0.5).collect();
            let batch = RecordBatch::try_new(
                input_schema,
                vec![
                    Arc::new(Float32Array::from(payload.clone())),
                    Arc::new(UInt64Array::from(keys)),
                ],
            )
            .unwrap();

            let plan = take_plan(&fixture.dataset, rows_input(vec![batch]), &["i"]).unwrap();
            let result = concat_batches(&plan.schema(), &run(&plan).await).unwrap();
            assert_eq!(result.num_rows(), 5);
            let i_col = result.column_by_name("i").unwrap();
            assert_eq!(
                i_col.as_primitive::<arrow::datatypes::Int32Type>().values(),
                &expected_i[..]
            );
            let payload_col = result
                .column_by_name("payload")
                .unwrap()
                .as_primitive::<Float32Type>();
            assert_eq!(payload_col.values(), &payload[..]);
        }

        /// A fragment-scoped take reads from the scoped fragments only; keys
        /// pointing outside the scope drop like stale rows
        #[rstest]
        #[case::by_row_addr(false, ROW_ADDR)]
        #[case::by_row_id(false, ROW_ID)]
        #[case::stable_by_row_addr(true, ROW_ADDR)]
        #[case::stable_by_row_id(true, ROW_ID)]
        #[tokio::test]
        async fn take_scoped_to_fragments(#[case] stable_row_ids: bool, #[case] key: &str) {
            let fixture = take_fixture(stable_row_ids).await;
            let subset = Arc::new(vec![fixture.dataset.fragments()[1].clone()]);

            let addr = |frag: u64, off: u64| (frag << 32) | off;
            // i = 12 inside the scoped fragment, i = 3 outside the scope
            let keys: Vec<u64> = if key == ROW_ID && stable_row_ids {
                vec![12, 3]
            } else {
                vec![addr(1, 2), addr(0, 3)]
            };
            let input_schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
                key,
                DataType::UInt64,
                true,
            )]));
            let batch = RecordBatch::try_new(input_schema, vec![Arc::new(UInt64Array::from(keys))])
                .unwrap();

            let projection = fixture
                .dataset
                .empty_projection()
                .union_columns(["i"], OnMissing::Error)
                .unwrap();
            let plan = FilteredReadExec::try_new(
                fixture.dataset.clone(),
                FilteredReadOptions::new(projection).with_fragments(subset),
                Some(rows_input(vec![batch])),
            )
            .unwrap();

            let result = concat_batches(&plan.schema(), &run(&plan).await).unwrap();
            assert_eq!(result.num_rows(), 1);
            let i_col = result
                .column_by_name("i")
                .unwrap()
                .as_primitive::<arrow::datatypes::Int32Type>();
            assert_eq!(i_col.value(0), 12);
        }

        /// A batch whose keys span the whole id range but hit only two rows:
        /// the span prefilter must not misread coverage as membership
        #[tokio::test]
        async fn take_stable_ids_wide_key_span() {
            let fixture = take_fixture(true).await;
            // Last row of the last fragment, first row of the first: every
            // fragment's span overlaps, only two rows match
            let keys: Vec<u64> = vec![29, 0];
            let input_schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
                ROW_ID,
                DataType::UInt64,
                true,
            )]));
            let batch = RecordBatch::try_new(input_schema, vec![Arc::new(UInt64Array::from(keys))])
                .unwrap();

            let plan = take_plan(&fixture.dataset, rows_input(vec![batch]), &["i"]).unwrap();
            let result = concat_batches(&plan.schema(), &run(&plan).await).unwrap();
            let i_col = result
                .column_by_name("i")
                .unwrap()
                .as_primitive::<arrow::datatypes::Int32Type>();
            assert_eq!(i_col.values(), &[29, 0]);
        }

        /// Identity flags: requested-but-missing columns are synthesized,
        /// carried ones kept, unrequested carried ones stripped
        #[rstest]
        #[case::unstable(false)]
        #[case::stable(true)]
        #[tokio::test]
        async fn take_identity_flags(#[case] stable_row_ids: bool) {
            let fixture = take_fixture(stable_row_ids).await;
            let addr = |frag: u64, off: u64| (frag << 32) | off;

            let ids: Vec<u64> = if stable_row_ids {
                vec![21, 3]
            } else {
                vec![addr(2, 1), addr(0, 3)]
            };
            let expected_addrs: Vec<u64> = vec![addr(2, 1), addr(0, 3)];
            let expected_i: Vec<i32> = vec![21, 3];
            let id_schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
                ROW_ID,
                DataType::UInt64,
                true,
            )]));
            let id_batch =
                RecordBatch::try_new(id_schema, vec![Arc::new(UInt64Array::from(ids.clone()))])
                    .unwrap();

            // Keep the carried _rowid and synthesize _rowaddr
            let projection = fixture
                .dataset
                .empty_projection()
                .union_columns(["i"], OnMissing::Error)
                .unwrap()
                .with_row_id()
                .with_row_addr();
            let plan = FilteredReadExec::try_new(
                fixture.dataset.clone(),
                FilteredReadOptions::new(projection),
                Some(rows_input(vec![id_batch.clone()])),
            )
            .unwrap();
            assert_eq!(
                plan.schema()
                    .fields()
                    .iter()
                    .map(|f| f.name().clone())
                    .collect::<Vec<_>>(),
                vec![ROW_ID, "i", ROW_ADDR]
            );
            let result = concat_batches(&plan.schema(), &run(&plan).await).unwrap();
            let id_col = result
                .column_by_name(ROW_ID)
                .unwrap()
                .as_primitive::<arrow::datatypes::UInt64Type>();
            assert_eq!(id_col.values(), &ids[..]);
            let addr_col = result
                .column_by_name(ROW_ADDR)
                .unwrap()
                .as_primitive::<arrow::datatypes::UInt64Type>();
            assert_eq!(addr_col.values(), &expected_addrs[..]);
            let i_col = result
                .column_by_name("i")
                .unwrap()
                .as_primitive::<arrow::datatypes::Int32Type>();
            assert_eq!(i_col.values(), &expected_i[..]);

            // Synthesize _rowaddr but strip the unrequested carried _rowid
            let projection = fixture
                .dataset
                .empty_projection()
                .union_columns(["i"], OnMissing::Error)
                .unwrap()
                .with_row_addr();
            let plan = FilteredReadExec::try_new(
                fixture.dataset.clone(),
                FilteredReadOptions::new(projection),
                Some(rows_input(vec![id_batch.clone()])),
            )
            .unwrap();
            assert_eq!(
                plan.schema()
                    .fields()
                    .iter()
                    .map(|f| f.name().clone())
                    .collect::<Vec<_>>(),
                vec!["i", ROW_ADDR]
            );
            let result = concat_batches(&plan.schema(), &run(&plan).await).unwrap();
            let addr_col = result
                .column_by_name(ROW_ADDR)
                .unwrap()
                .as_primitive::<arrow::datatypes::UInt64Type>();
            assert_eq!(addr_col.values(), &expected_addrs[..]);

            // Address-keyed input, synthesize _rowid (the reverse direction)
            let addr_schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
                ROW_ADDR,
                DataType::UInt64,
                true,
            )]));
            let addr_batch = RecordBatch::try_new(
                addr_schema,
                vec![Arc::new(UInt64Array::from(expected_addrs.clone()))],
            )
            .unwrap();
            let projection = fixture
                .dataset
                .empty_projection()
                .union_columns(["i"], OnMissing::Error)
                .unwrap()
                .with_row_id();
            let plan = FilteredReadExec::try_new(
                fixture.dataset.clone(),
                FilteredReadOptions::new(projection),
                Some(rows_input(vec![addr_batch])),
            )
            .unwrap();
            assert_eq!(
                plan.schema()
                    .fields()
                    .iter()
                    .map(|f| f.name().clone())
                    .collect::<Vec<_>>(),
                vec!["i", ROW_ID]
            );
            let result = concat_batches(&plan.schema(), &run(&plan).await).unwrap();
            let id_col = result
                .column_by_name(ROW_ID)
                .unwrap()
                .as_primitive::<arrow::datatypes::UInt64Type>();
            assert_eq!(id_col.values(), &ids[..]);

            // Fetch nothing, synthesize only (the AddRowAddrExec shape)
            let projection = fixture
                .dataset
                .empty_projection()
                .with_row_id()
                .with_row_addr();
            let plan = FilteredReadExec::try_new(
                fixture.dataset.clone(),
                FilteredReadOptions::new(projection),
                Some(rows_input(vec![id_batch])),
            )
            .unwrap();
            assert_eq!(
                plan.schema()
                    .fields()
                    .iter()
                    .map(|f| f.name().clone())
                    .collect::<Vec<_>>(),
                vec![ROW_ID, ROW_ADDR]
            );
            let result = concat_batches(&plan.schema(), &run(&plan).await).unwrap();
            let addr_col = result
                .column_by_name(ROW_ADDR)
                .unwrap()
                .as_primitive::<arrow::datatypes::UInt64Type>();
            assert_eq!(addr_col.values(), &expected_addrs[..]);
        }

        /// New sub-fields merge into an existing struct column
        #[tokio::test]
        async fn take_merges_nested_struct() {
            let fixture = take_fixture(false).await;

            let data = fixture
                .dataset
                .scan()
                .project(&["struct"])
                .unwrap()
                .with_row_id()
                .try_into_batch()
                .await
                .unwrap();
            // Rebuild the input with only struct.y so struct.x must be taken
            let full_struct = data.column_by_name("struct").unwrap().as_struct();
            let y_only = arrow_array::StructArray::new(
                Fields::from(vec![Arc::new(ArrowField::new("y", DataType::Int32, false))]),
                vec![full_struct.column_by_name("y").unwrap().clone()],
                None,
            );
            let input_schema = Arc::new(ArrowSchema::new(vec![
                ArrowField::new("struct", y_only.data_type().clone(), false),
                ArrowField::new(ROW_ID, DataType::UInt64, true),
            ]));
            let data = RecordBatch::try_new(
                input_schema,
                vec![
                    Arc::new(y_only),
                    data.column_by_name(ROW_ID).unwrap().clone(),
                ],
            )
            .unwrap();

            let projection = fixture
                .dataset
                .empty_projection()
                .union_column("struct.x", OnMissing::Error)
                .unwrap();
            let plan = FilteredReadExec::try_new(
                fixture.dataset.clone(),
                FilteredReadOptions::new(projection),
                Some(rows_input(vec![data])),
            )
            .unwrap();

            let expected_struct_type = DataType::Struct(Fields::from(vec![
                Arc::new(ArrowField::new("x", DataType::Int32, false)),
                Arc::new(ArrowField::new("y", DataType::Int32, false)),
            ]));
            assert_eq!(
                plan.schema().field_with_name("struct").unwrap().data_type(),
                &expected_struct_type
            );

            let result = concat_batches(&plan.schema(), &run(&plan).await).unwrap();
            assert_eq!(result.num_rows(), 30);
            let struct_col = result.column_by_name("struct").unwrap().as_struct();
            assert_eq!(
                struct_col.column_by_name("x").unwrap(),
                struct_col.column_by_name("y").unwrap()
            );
        }

        /// Input rows whose key no longer exists (deleted rows) are dropped
        #[rstest]
        #[case::by_row_addr(false, ROW_ADDR)]
        #[case::by_row_id(false, ROW_ID)]
        #[case::stable_by_row_addr(true, ROW_ADDR)]
        #[case::stable_by_row_id(true, ROW_ID)]
        #[tokio::test]
        async fn take_drops_stale_keys(#[case] stable_row_ids: bool, #[case] key: &str) {
            let fixture = take_fixture(stable_row_ids).await;
            let mut dataset = fixture.dataset.as_ref().clone();
            dataset.delete("i = 15").await.unwrap();
            let dataset = Arc::new(dataset);

            let addr = |frag: u64, off: u64| (frag << 32) | off;
            // The pre-delete identifiers of rows 15 (now deleted) and 16
            let keys: Vec<u64> = if key == ROW_ID && stable_row_ids {
                vec![15, 16]
            } else {
                vec![addr(1, 5), addr(1, 6)]
            };
            let input_schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
                key,
                DataType::UInt64,
                true,
            )]));
            let batch = RecordBatch::try_new(input_schema, vec![Arc::new(UInt64Array::from(keys))])
                .unwrap();

            let plan = take_plan(&dataset, rows_input(vec![batch]), &["i"]).unwrap();
            let result = concat_batches(&plan.schema(), &run(&plan).await).unwrap();
            assert_eq!(result.num_rows(), 1);
            let i_col = result
                .column_by_name("i")
                .unwrap()
                .as_primitive::<arrow::datatypes::Int32Type>();
            assert_eq!(i_col.value(0), 16);
        }

        /// Keys of a fully deleted fragment (gone from the manifest) are
        /// dropped like stale rows
        #[rstest]
        #[case::by_row_addr(false, ROW_ADDR)]
        #[case::by_row_id(false, ROW_ID)]
        #[case::stable_by_row_addr(true, ROW_ADDR)]
        #[case::stable_by_row_id(true, ROW_ID)]
        #[tokio::test]
        async fn take_drops_keys_of_deleted_fragment(
            #[case] stable_row_ids: bool,
            #[case] key: &str,
        ) {
            let fixture = take_fixture(stable_row_ids).await;
            let mut dataset = fixture.dataset.as_ref().clone();
            dataset.delete("i >= 10 and i < 20").await.unwrap();
            let dataset = Arc::new(dataset);

            let addr = |frag: u64, off: u64| (frag << 32) | off;
            // The pre-delete identifiers of row 15 (fragment 1, now gone
            // from the manifest) and row 20 (fragment 2, still live)
            let keys: Vec<u64> = if key == ROW_ID && stable_row_ids {
                vec![15, 20]
            } else {
                vec![addr(1, 5), addr(2, 0)]
            };
            let input_schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
                key,
                DataType::UInt64,
                true,
            )]));
            let batch = RecordBatch::try_new(input_schema, vec![Arc::new(UInt64Array::from(keys))])
                .unwrap();

            let plan = take_plan(&dataset, rows_input(vec![batch]), &["i"]).unwrap();
            let result = concat_batches(&plan.schema(), &run(&plan).await).unwrap();
            assert_eq!(result.num_rows(), 1);
            let i_col = result
                .column_by_name("i")
                .unwrap()
                .as_primitive::<arrow::datatypes::Int32Type>();
            assert_eq!(i_col.value(0), 20);
        }

        /// After delete + compaction the stable row-id sequences are no
        /// longer simple contiguous ranges; ids must still resolve to the
        /// moved rows and deleted ids must still drop
        #[tokio::test]
        async fn take_stable_ids_after_compaction() {
            use crate::dataset::optimize::{CompactionOptions, compact_files};

            let fixture = take_fixture(true).await;
            let mut dataset = fixture.dataset.as_ref().clone();
            // Punch holes, then rewrite all fragments into one
            dataset.delete("i % 3 = 0").await.unwrap();
            compact_files(&mut dataset, CompactionOptions::default(), None)
                .await
                .unwrap();
            let dataset = Arc::new(dataset);
            assert_eq!(dataset.get_fragments().len(), 1);

            // Survivors in scattered order, plus a compacted-away id (15)
            let keys: Vec<u64> = vec![25, 1, 15, 14];
            let input_schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
                ROW_ID,
                DataType::UInt64,
                true,
            )]));
            let batch = RecordBatch::try_new(input_schema, vec![Arc::new(UInt64Array::from(keys))])
                .unwrap();

            let plan = take_plan(&dataset, rows_input(vec![batch]), &["i"]).unwrap();
            let result = concat_batches(&plan.schema(), &run(&plan).await).unwrap();
            let i_col = result
                .column_by_name("i")
                .unwrap()
                .as_primitive::<arrow::datatypes::Int32Type>();
            assert_eq!(i_col.values(), &[25, 1, 14]);
        }

        /// with_deleted_rows is rejected for a row-stream read
        #[rstest]
        #[case::unstable(false)]
        #[case::stable(true)]
        #[tokio::test]
        async fn take_rejects_with_deleted_rows(#[case] stable_row_ids: bool) {
            let fixture = take_fixture(stable_row_ids).await;
            let input_schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
                ROW_ADDR,
                DataType::UInt64,
                true,
            )]));
            let batch =
                RecordBatch::try_new(input_schema, vec![Arc::new(UInt64Array::from(vec![0_u64]))])
                    .unwrap();
            let projection = fixture
                .dataset
                .empty_projection()
                .union_columns(["i"], OnMissing::Error)
                .unwrap();
            let err = FilteredReadExec::try_new(
                fixture.dataset,
                FilteredReadOptions::new(projection)
                    .with_deleted_rows()
                    .unwrap(),
                Some(rows_input(vec![batch])),
            )
            .unwrap_err();
            assert!(matches!(err, Error::InvalidInput { .. }), "{err}");
            assert!(err.to_string().contains("with_deleted_rows"));
        }

        /// Construction errors: no key column, nothing to read
        #[tokio::test]
        async fn take_construction_errors() {
            let fixture = take_fixture(false).await;
            let no_key_schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
                "payload",
                DataType::UInt64,
                true,
            )]));
            let batch = RecordBatch::try_new(
                no_key_schema,
                vec![Arc::new(UInt64Array::from(vec![0_u64]))],
            )
            .unwrap();
            let err = take_plan(&fixture.dataset, rows_input(vec![batch]), &["s"]).unwrap_err();
            assert!(matches!(err, Error::InvalidInput { .. }), "{err}");
            assert!(err.to_string().contains("must have a column"));

            // Taking fields the input already has: nothing to read
            let with_s_schema = Arc::new(ArrowSchema::new(vec![
                ArrowField::new(ROW_ADDR, DataType::UInt64, true),
                ArrowField::new("s", DataType::Utf8, false),
            ]));
            let with_s_batch = RecordBatch::try_new(
                with_s_schema,
                vec![
                    Arc::new(UInt64Array::from(vec![0_u64])),
                    Arc::new(StringArray::from(vec!["x"])),
                ],
            )
            .unwrap();
            let err =
                take_plan(&fixture.dataset, rows_input(vec![with_s_batch]), &["s"]).unwrap_err();
            assert!(matches!(err, Error::InvalidInput { .. }), "{err}");
            assert!(err.to_string().contains("nothing to read"));
        }

        /// with_new_children re-derives the row-stream source and preserves the schema
        #[tokio::test]
        async fn take_with_new_children() {
            let fixture = take_fixture(false).await;
            let input_schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
                ROW_ID,
                DataType::UInt64,
                true,
            )]));
            let batch = RecordBatch::try_new(
                input_schema,
                vec![Arc::new(UInt64Array::from(vec![0_u64, 1]))],
            )
            .unwrap();
            let input = rows_input(vec![batch]);
            let plan: Arc<dyn ExecutionPlan> =
                Arc::new(take_plan(&fixture.dataset, input.clone(), &["s"]).unwrap());
            let rebuilt = plan.clone().with_new_children(vec![input]).unwrap();
            assert_eq!(plan.schema(), rebuilt.schema());
            assert!(
                rebuilt
                    .downcast_ref::<FilteredReadExec>()
                    .unwrap()
                    .row_stream_input()
                    .is_some()
            );
        }

        /// Take-mode nodes can be created and executed without an active
        /// tokio runtime (required for DataFusion foreign table providers)
        #[test]
        fn no_context_take_rows() {
            use lance_datafusion::datagen::DatafusionDatagenExt;
            use lance_datagen::{BatchCount, RowCount};

            let fixture = NoContextTestFixture::new();
            let dataset = Arc::new(fixture.dataset);
            let input = lance_datagen::gen_batch()
                .col(ROW_ID, lance_datagen::array::step::<UInt64Type>())
                .into_df_exec(RowCount::from(50), BatchCount::from(2));
            let plan = take_plan(&dataset, input, &["text"]).unwrap();
            plan.execute(0, Arc::new(TaskContext::default())).unwrap();
        }
    }
}
