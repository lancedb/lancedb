// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use lance_datafusion::utils::{
    BYTES_READ_METRIC, ExecutionPlanMetricsSetExt, INDEX_CACHE_HITS_METRIC,
    INDEX_CACHE_MISSES_METRIC, INDEX_COMPARISONS_METRIC, INDICES_LOADED_METRIC, IOPS_METRIC,
    PARTS_LOADED_METRIC, REQUESTS_METRIC,
};
use lance_index::metrics::MetricsCollector;
use lance_io::scheduler::{IoStats, ScanScheduler, ScanStats};
use lance_table::format::IndexMetadata;
use pin_project::pin_project;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use arrow_array::{RecordBatch, UInt64Array};
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::physical_plan::metrics::{
    BaselineMetrics, Count, ExecutionPlanMetricsSet, Gauge, MetricBuilder, MetricValue,
};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, RecordBatchStream, SendableRecordBatchStream,
};
use futures::stream::FuturesUnordered;
use futures::{Stream, StreamExt, TryStreamExt};
use lance_core::error::{CloneableResult, Error};
use lance_core::utils::futures::{Capacity, SharedStreamExt};
use lance_core::{ROW_ID, Result};
use lance_index::prefilter::FilterLoader;
use lance_select::{RowAddrMask, RowAddrTreeMap, result::IndexExprResult};

use crate::Dataset;
use crate::index::prefilter::DatasetPreFilter;

#[derive(Debug, Clone)]
pub enum PreFilterSource {
    /// The prefilter input is an array of row ids that match the filter condition
    FilteredRowIds(Arc<dyn ExecutionPlan>),
    /// The prefilter input is a selection vector from an index query
    ScalarIndexQuery(Arc<dyn ExecutionPlan>),
    /// There is no prefilter
    None,
}

pub(crate) fn build_prefilter(
    context: Arc<datafusion::execution::TaskContext>,
    partition: usize,
    prefilter_source: &PreFilterSource,
    ds: Arc<Dataset>,
    index_meta: &[IndexMetadata],
    overlay_block: Option<RowAddrMask>,
) -> Result<Arc<DatasetPreFilter>> {
    let prefilter_loader = match &prefilter_source {
        PreFilterSource::FilteredRowIds(src_node) => {
            let stream = src_node.execute(partition, context)?;
            Some(Box::new(FilteredRowIdsToPrefilter(stream)) as Box<dyn FilterLoader>)
        }
        PreFilterSource::ScalarIndexQuery(src_node) => {
            let stream = src_node.execute(partition, context)?;
            Some(Box::new(SelectionVectorToPrefilter(stream)) as Box<dyn FilterLoader>)
        }
        PreFilterSource::None => None,
    };
    let mut prefilter = DatasetPreFilter::new(ds, index_meta, prefilter_loader);
    if let Some(overlay_block) = overlay_block {
        prefilter = prefilter.with_overlay_block(overlay_block);
    }
    Ok(Arc::new(prefilter))
}

// Utility to convert an input (containing row ids) into a prefilter
pub(crate) struct FilteredRowIdsToPrefilter(pub SendableRecordBatchStream);

#[async_trait]
impl FilterLoader for FilteredRowIdsToPrefilter {
    async fn load(mut self: Box<Self>) -> Result<RowAddrMask> {
        let mut allow_list = RowAddrTreeMap::new();
        while let Some(batch) = self.0.next().await {
            let batch = batch?;
            let row_ids = batch.column_by_name(ROW_ID).ok_or_else(|| Error::internal("input batch missing row id column even though it is in the schema for the stream"))?;
            let row_ids = row_ids
                .as_any()
                .downcast_ref::<UInt64Array>()
                .expect("row id column in input batch had incorrect type");
            allow_list.extend(row_ids.iter().flatten())
        }
        Ok(RowAddrMask::from_allowed(allow_list))
    }
}

// Utility to convert a serialized selection vector into a prefilter
pub(crate) struct SelectionVectorToPrefilter(pub SendableRecordBatchStream);

#[async_trait]
impl FilterLoader for SelectionVectorToPrefilter {
    async fn load(mut self: Box<Self>) -> Result<RowAddrMask> {
        let batch = self.0.try_next().await?.ok_or_else(|| {
            Error::internal("Selection vector source for prefilter did not yield any batches")
        })?;
        // The vector-search prefilter wants the set of rows the search is
        // allowed to consider — the `upper` bound of the index expression
        // result. Rows outside the upper bound are guaranteed not to match,
        // so the vector search can skip them.
        //
        // Use deserialize() here (rather than indexing "upper" directly) to
        // support both the TwoMask and the legacy ThreeVariant wire formats
        // that ScalarIndexExec may emit.
        let (result, _) = IndexExprResult::deserialize(&batch)?;
        Ok(result.upper)
    }
}

struct InnerState {
    cached: Option<SendableRecordBatchStream>,
    taken: bool,
}

impl std::fmt::Debug for InnerState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InnerState")
            .field("cached", &self.cached.is_some())
            .field("taken", &self.taken)
            .finish()
    }
}

/// An execution node that can be used as an input twice
///
/// This can be used to broadcast an input to multiple outputs.
///
/// Note: this is done by caching the results.  If one output is consumed
/// more quickly than the other, this can lead to increased memory usage.
/// The `capacity` parameter can bound this, by blocking the faster output
/// when the cache is full.  Take care not to cause deadlock.
///
/// For example, if both outputs are fed to a HashJoinExec then one side
/// of the join will be fully consumed before the other side is read.  In
/// this case, you should probably use an unbounded capacity.
#[derive(Debug)]
pub struct ReplayExec {
    capacity: Capacity,
    input: Arc<dyn ExecutionPlan>,
    inner_state: Arc<Mutex<InnerState>>,
}

impl ReplayExec {
    pub fn new(capacity: Capacity, input: Arc<dyn ExecutionPlan>) -> Self {
        Self {
            capacity,
            input,
            inner_state: Arc::new(Mutex::new(InnerState {
                cached: None,
                taken: false,
            })),
        }
    }
}

impl DisplayAs for ReplayExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "Replay: capacity={:?}", self.capacity)
            }
            DisplayFormatType::TreeRender => {
                write!(f, "Replay\ncapacity={:?}", self.capacity)
            }
        }
    }
}

// There's some annoying adapter-work that needs to happen here.  In order
// to share a stream we need its items to be Clone and DataFusionError is
// not Clone.  So we wrap the stream in a CloneableResult.  However, in order
// for that shared stream to be a SendableRecordBatchStream, it needs to be
// using DataFusionError.  So we need to adapt the stream back to a
// SendableRecordBatchStream.
pub struct ShareableRecordBatchStream(pub SendableRecordBatchStream);

impl Stream for ShareableRecordBatchStream {
    type Item = CloneableResult<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        match self.0.poll_next_unpin(cx) {
            std::task::Poll::Ready(None) => std::task::Poll::Ready(None),
            std::task::Poll::Ready(Some(res)) => {
                std::task::Poll::Ready(Some(CloneableResult::from(res.map_err(Error::from))))
            }
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }
}

pub struct ShareableRecordBatchStreamAdapter<S: Stream<Item = CloneableResult<RecordBatch>> + Unpin>
{
    schema: SchemaRef,
    stream: S,
}

impl<S: Stream<Item = CloneableResult<RecordBatch>> + Unpin> ShareableRecordBatchStreamAdapter<S> {
    pub fn new(schema: SchemaRef, stream: S) -> Self {
        Self { schema, stream }
    }
}

impl<S: Stream<Item = CloneableResult<RecordBatch>> + Unpin> Stream
    for ShareableRecordBatchStreamAdapter<S>
{
    type Item = DataFusionResult<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        match self.stream.poll_next_unpin(cx) {
            std::task::Poll::Ready(None) => std::task::Poll::Ready(None),
            std::task::Poll::Ready(Some(res)) => std::task::Poll::Ready(Some(
                res.0
                    .map_err(|e| DataFusionError::External(e.0.to_string().into())),
            )),
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }
}

impl<S: Stream<Item = CloneableResult<RecordBatch>> + Unpin> RecordBatchStream
    for ShareableRecordBatchStreamAdapter<S>
{
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[pin_project]
pub struct InstrumentedRecordBatchStreamAdapter<S> {
    schema: SchemaRef,

    #[pin]
    stream: S,
    baseline_metrics: BaselineMetrics,
    batch_count: Count,
}

impl<S> InstrumentedRecordBatchStreamAdapter<S> {
    pub fn new(
        schema: SchemaRef,
        stream: S,
        partition: usize,
        metrics: &ExecutionPlanMetricsSet,
    ) -> Self {
        let batch_count = Count::new();
        MetricBuilder::new(metrics)
            .with_partition(partition)
            .build(MetricValue::OutputBatches(batch_count.clone()));
        Self {
            schema,
            stream,
            baseline_metrics: BaselineMetrics::new(metrics, partition),
            batch_count,
        }
    }
}

impl<S> Stream for InstrumentedRecordBatchStreamAdapter<S>
where
    S: Stream<Item = DataFusionResult<RecordBatch>>,
{
    type Item = DataFusionResult<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.as_mut().project();
        let timer = this.baseline_metrics.elapsed_compute().timer();
        let poll = this.stream.poll_next(cx);
        timer.done();
        if let Poll::Ready(Some(Ok(_))) = &poll {
            this.batch_count.add(1);
        }
        this.baseline_metrics.record_poll(poll)
    }
}

impl<S> RecordBatchStream for InstrumentedRecordBatchStreamAdapter<S>
where
    S: Stream<Item = DataFusionResult<RecordBatch>>,
{
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// Stream wrapper for an `ExecutionPlan` node that pulls from a child input and
/// applies a per-batch async transform.
///
/// `elapsed_compute` measures only the time spent driving the transform
/// futures -- never the time spent polling the child input -- so wrapping a
/// chain of nodes does not double-count child CPU. `output_rows` and
/// `output_batches` are recorded as the transform produces batches.
///
/// `concurrency` caps how many transform futures may be in flight at once.
/// Use `1` for sequential transforms; larger values parallelize per-batch
/// work (e.g., KNN distance computation).
///
/// For leaf nodes (no child input), use [`InstrumentedRecordBatchStreamAdapter`]
/// instead.
pub struct InstrumentedChildInputStream<F, Fut> {
    schema: SchemaRef,
    input: SendableRecordBatchStream,
    transform: F,
    concurrency: usize,
    in_flight: FuturesUnordered<Fut>,
    input_done: bool,
    baseline_metrics: BaselineMetrics,
    batch_count: Count,
}

impl<F, Fut> InstrumentedChildInputStream<F, Fut>
where
    F: FnMut(RecordBatch) -> Fut,
    Fut: Future<Output = DataFusionResult<RecordBatch>>,
{
    pub fn new(
        input: SendableRecordBatchStream,
        schema: SchemaRef,
        transform: F,
        concurrency: usize,
        partition: usize,
        metrics: &ExecutionPlanMetricsSet,
    ) -> Self {
        assert!(concurrency >= 1, "concurrency must be >= 1");
        let batch_count = Count::new();
        MetricBuilder::new(metrics)
            .with_partition(partition)
            .build(MetricValue::OutputBatches(batch_count.clone()));
        Self {
            schema,
            input,
            transform,
            concurrency,
            in_flight: FuturesUnordered::new(),
            input_done: false,
            baseline_metrics: BaselineMetrics::new(metrics, partition),
            batch_count,
        }
    }
}

impl<F, Fut> Stream for InstrumentedChildInputStream<F, Fut>
where
    F: FnMut(RecordBatch) -> Fut + Unpin,
    Fut: Future<Output = DataFusionResult<RecordBatch>>,
{
    type Item = DataFusionResult<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        // Fill in-flight transforms up to `concurrency` from the input.
        // Polling the input does not count toward `elapsed_compute`.
        while !this.input_done && this.in_flight.len() < this.concurrency {
            match this.input.poll_next_unpin(cx) {
                Poll::Ready(Some(Ok(batch))) => {
                    this.in_flight.push((this.transform)(batch));
                }
                Poll::Ready(Some(Err(e))) => {
                    return Poll::Ready(Some(Err(e)));
                }
                Poll::Ready(None) => {
                    this.input_done = true;
                }
                Poll::Pending => break,
            }
        }

        // Drive in-flight transforms; their poll time is counted.
        if !this.in_flight.is_empty() {
            let timer = this.baseline_metrics.elapsed_compute().timer();
            let poll = this.in_flight.poll_next_unpin(cx);
            timer.done();
            match poll {
                Poll::Ready(Some(result)) => {
                    if result.is_ok() {
                        this.batch_count.add(1);
                    }
                    return this.baseline_metrics.record_poll(Poll::Ready(Some(result)));
                }
                // `FuturesUnordered::poll_next` returns `Ready(None)` only
                // when empty, and we just checked `!is_empty` above.
                Poll::Ready(None) => unreachable!("non-empty transform queue yielded None"),
                Poll::Pending => return Poll::Pending,
            }
        }

        if this.input_done {
            return Poll::Ready(None);
        }

        Poll::Pending
    }
}

impl<F, Fut> RecordBatchStream for InstrumentedChildInputStream<F, Fut>
where
    F: FnMut(RecordBatch) -> Fut + Unpin,
    Fut: Future<Output = DataFusionResult<RecordBatch>>,
{
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl ExecutionPlan for ReplayExec {
    fn name(&self) -> &str {
        "ReplayExec"
    }

    fn schema(&self) -> arrow_schema::SchemaRef {
        self.input.schema()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        unimplemented!()
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        // We aren't doing any work here, and it would be a little confusing
        // to have multiple replay queues.
        vec![false]
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::TaskContext>,
    ) -> datafusion::error::Result<SendableRecordBatchStream> {
        let mut inner_state = self.inner_state.lock().unwrap();
        if let Some(cached) = inner_state.cached.take() {
            if inner_state.taken {
                panic!("ReplayExec can only be executed twice");
            }
            inner_state.taken = true;
            Ok(cached)
        } else {
            let input = self.input.execute(partition, context)?;
            let schema = input.schema();
            let input = ShareableRecordBatchStream(input);
            let (to_return, to_cache) = input.boxed().share(self.capacity);
            inner_state.cached = Some(Box::pin(ShareableRecordBatchStreamAdapter {
                schema: schema.clone(),
                stream: to_cache,
            }));
            Ok(Box::pin(ShareableRecordBatchStreamAdapter {
                schema,
                stream: to_return,
            }))
        }
    }

    fn properties(&self) -> &Arc<datafusion::physical_plan::PlanProperties> {
        self.input.properties()
    }
}

#[derive(Debug, Clone)]
pub struct IoMetrics {
    // We use gauge and not counter here because the underlying ScanScheduler
    // reports cumulative stats, not deltas. We use set_max to ensure the gauge
    // always shows the highest value seen.
    iops: Gauge,
    requests: Gauge,
    bytes_read: Gauge,
}

impl IoMetrics {
    pub fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        let iops = metrics.new_gauge(IOPS_METRIC, partition);
        let requests = metrics.new_gauge(REQUESTS_METRIC, partition);
        let bytes_read = metrics.new_gauge(BYTES_READ_METRIC, partition);
        Self {
            iops,
            requests,
            bytes_read,
        }
    }

    pub fn record(&self, scan_scheduler: &ScanScheduler) {
        self.record_stats(scan_scheduler.stats());
    }

    /// Record a snapshot of cumulative I/O statistics.
    ///
    /// Uses `set_max` because the underlying counters are cumulative; the gauge
    /// always reflects the highest (i.e. final) value seen.
    pub fn record_stats(&self, stats: ScanStats) {
        self.iops.set_max(stats.iops as usize);
        self.requests.set_max(stats.requests as usize);
        self.bytes_read.set_max(stats.bytes_read as usize);
    }
}

#[derive(Clone)]
pub struct IndexMetrics {
    indices_loaded: Count,
    parts_loaded: Count,
    index_comparisons: Count,
    index_cache_hits: Count,
    index_cache_misses: Count,
    /// Per-query sink that accumulates exact index-file I/O as partitions are
    /// loaded from storage.  Shared by all clones of this `IndexMetrics`, so
    /// concurrent partition loads all funnel into the same counters.  Published
    /// to `io_metrics` for display via [`IndexMetrics::flush_io`].
    io_stats: IoStats,
    io_metrics: IoMetrics,
}

impl IndexMetrics {
    pub fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        Self {
            indices_loaded: metrics.new_count(INDICES_LOADED_METRIC, partition),
            parts_loaded: metrics.new_count(PARTS_LOADED_METRIC, partition),
            index_comparisons: metrics.new_count(INDEX_COMPARISONS_METRIC, partition),
            index_cache_hits: metrics.new_count(INDEX_CACHE_HITS_METRIC, partition),
            index_cache_misses: metrics.new_count(INDEX_CACHE_MISSES_METRIC, partition),
            io_stats: IoStats::new(),
            io_metrics: IoMetrics::new(metrics, partition),
        }
    }

    /// Publish the I/O accumulated in the per-query sink to the displayed
    /// `iops`/`requests`/`bytes_read` metrics.  Call once when the operator's
    /// stream finishes; the sink only accumulates on cache misses, so a fully
    /// cache-resident query publishes zeros.
    pub fn flush_io(&self) {
        self.io_metrics.record_stats(self.io_stats.snapshot());
    }
}

impl MetricsCollector for IndexMetrics {
    fn record_parts_loaded(&self, num_shards: usize) {
        self.parts_loaded.add(num_shards);
    }
    fn record_index_loads(&self, num_indexes: usize) {
        self.indices_loaded.add(num_indexes);
    }
    fn record_comparisons(&self, num_comparisons: usize) {
        self.index_comparisons.add(num_comparisons);
    }
    fn record_index_cache_hits(&self, num_hits: usize) {
        self.index_cache_hits.add(num_hits);
    }
    fn record_index_cache_misses(&self, num_misses: usize) {
        self.index_cache_misses.add(num_misses);
    }
    fn io_stats(&self) -> Option<IoStats> {
        Some(self.io_stats.clone())
    }
}

#[cfg(test)]
mod tests {

    use std::sync::Arc;

    use arrow_array::{RecordBatchReader, types::UInt32Type};
    use arrow_schema::SortOptions;
    use datafusion::common::NullEquality;
    use datafusion::{
        logical_expr::JoinType,
        physical_expr::expressions::Column,
        physical_plan::{
            ExecutionPlan, joins::SortMergeJoinExec, stream::RecordBatchStreamAdapter,
        },
    };
    use futures::{StreamExt, TryStreamExt};
    use lance_core::utils::futures::Capacity;
    use lance_datafusion::exec::OneShotExec;
    use lance_datagen::{BatchCount, RowCount, array};

    use super::{InstrumentedChildInputStream, ReplayExec};

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn instrumented_child_input_stream_excludes_child_poll_time() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::task::Poll;
        use std::time::Duration;

        use arrow_array::Int32Array;
        use arrow_schema::{DataType, Field, Schema};
        use datafusion::physical_plan::SendableRecordBatchStream;
        use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;

        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let n_batches: usize = 3;
        let child_delay = Duration::from_millis(150);

        let counter = Arc::new(AtomicUsize::new(0));
        let s = schema.clone();
        let child = futures::stream::poll_fn(move |_cx| {
            let n = counter.fetch_add(1, Ordering::SeqCst);
            if n >= n_batches {
                return Poll::Ready(None);
            }
            std::thread::sleep(child_delay);
            let batch = arrow_array::RecordBatch::try_new(
                s.clone(),
                vec![Arc::new(Int32Array::from(vec![n as i32]))],
            )
            .unwrap();
            Poll::Ready(Some(Ok(batch)))
        });
        let child: SendableRecordBatchStream =
            Box::pin(RecordBatchStreamAdapter::new(schema.clone(), child));

        let metrics = ExecutionPlanMetricsSet::new();
        let stream = InstrumentedChildInputStream::new(
            child,
            schema,
            move |batch| async move { Ok(batch) },
            1,
            0,
            &metrics,
        );

        let batches: Vec<_> = stream.try_collect().await.unwrap();
        assert_eq!(batches.len(), n_batches);

        let elapsed_ns = metrics
            .clone_inner()
            .elapsed_compute()
            .expect("elapsed_compute should be recorded");
        let elapsed = Duration::from_nanos(elapsed_ns as u64);

        // The transform is immediate, so `elapsed_compute` should stay well
        // below even one child poll delay. A version that double-counts child
        // input time would include roughly `child_delay * n_batches`.
        let upper = child_delay;
        assert!(
            elapsed < upper,
            "elapsed_compute={:?} >= {:?}; child input time was double-counted",
            elapsed,
            upper,
        );
    }

    #[tokio::test]
    async fn instrumented_child_input_stream_propagates_child_error() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::task::Poll;

        use arrow_array::Int32Array;
        use arrow_schema::{DataType, Field, Schema};
        use datafusion::error::DataFusionError;
        use datafusion::physical_plan::SendableRecordBatchStream;
        use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;

        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let s = schema.clone();
        let step = Arc::new(AtomicUsize::new(0));
        // Yield one OK batch, then an Err, then None.
        let child = futures::stream::poll_fn(move |_cx| {
            let n = step.fetch_add(1, Ordering::SeqCst);
            match n {
                0 => {
                    let batch = arrow_array::RecordBatch::try_new(
                        s.clone(),
                        vec![Arc::new(Int32Array::from(vec![1]))],
                    )
                    .unwrap();
                    Poll::Ready(Some(Ok(batch)))
                }
                1 => Poll::Ready(Some(Err(DataFusionError::Execution("boom".into())))),
                _ => Poll::Ready(None),
            }
        });
        let child: SendableRecordBatchStream =
            Box::pin(RecordBatchStreamAdapter::new(schema.clone(), child));

        let metrics = ExecutionPlanMetricsSet::new();
        let stream = InstrumentedChildInputStream::new(
            child,
            schema,
            move |batch| async move { Ok(batch) },
            1,
            0,
            &metrics,
        );

        let mut stream = Box::pin(stream);
        let first = stream.next().await.expect("first item present");
        assert!(first.is_ok(), "expected first batch ok, got {:?}", first);

        let second = stream.next().await.expect("error item present");
        let err = second.expect_err("expected propagated error");
        assert!(err.to_string().contains("boom"), "got {}", err);
    }

    #[tokio::test]
    async fn test_replay() {
        let data = lance_datagen::gen_batch()
            .col("x", array::step::<UInt32Type>())
            .into_reader_rows(RowCount::from(1024), BatchCount::from(16));
        let schema = data.schema();
        let data = Box::pin(RecordBatchStreamAdapter::new(
            schema,
            futures::stream::iter(data).map_err(datafusion::error::DataFusionError::from),
        ));

        let input = Arc::new(OneShotExec::new(data));
        let shared = Arc::new(ReplayExec::new(Capacity::Bounded(4), input));

        let joined = Arc::new(
            SortMergeJoinExec::try_new(
                shared.clone(),
                shared,
                vec![(Arc::new(Column::new("x", 0)), Arc::new(Column::new("x", 0)))],
                None,
                JoinType::Inner,
                vec![SortOptions::default()],
                NullEquality::NullEqualsNull,
            )
            .unwrap(),
        );

        let mut join_stream = joined
            .execute(0, Arc::new(datafusion::execution::TaskContext::default()))
            .unwrap();

        while let Some(batch) = join_stream.next().await {
            // We don't test much here but shouldn't really need to.  The join and stream sharing
            // are tested on their own.  We just need to make sure they get hooked up correctly
            assert_eq!(batch.unwrap().num_columns(), 2);
        }
    }
}
