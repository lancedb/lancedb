// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Utilities for working with datafusion execution plans

use std::{
    collections::HashMap,
    fmt::{self, Formatter},
    num::NonZero,
    sync::{Arc, Mutex, OnceLock},
    time::Duration,
};

use chrono::{DateTime, Utc};

use arrow_array::RecordBatch;
use arrow_schema::Schema as ArrowSchema;
use datafusion::{
    catalog::{TableProvider, streaming::StreamingTable},
    dataframe::DataFrame,
    execution::{
        TaskContext,
        context::{SessionConfig, SessionContext},
        disk_manager::DiskManagerBuilder,
        memory_pool::FairSpillPool,
        runtime_env::RuntimeEnvBuilder,
    },
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
        SendableRecordBatchStream,
        analyze::AnalyzeExec,
        coalesce_partitions::CoalescePartitionsExec,
        display::DisplayableExecutionPlan,
        execution_plan::{Boundedness, CardinalityEffect, EmissionType},
        metrics::MetricValue,
        sorts::sort_preserving_merge::SortPreservingMergeExec,
        stream::RecordBatchStreamAdapter,
        streaming::PartitionStream,
    },
};
use datafusion::{execution::memory_pool::TrackConsumersPool, physical_plan::metrics::MetricType};
use datafusion_common::{DataFusionError, Statistics};
use datafusion_physical_expr::{EquivalenceProperties, Partitioning};

use futures::{StreamExt, stream};
use lance_arrow::SchemaExt;
use lance_core::{
    Error, Result,
    utils::{
        futures::FinallyStreamExt,
        tracing::{EXECUTION_PLAN_RUN, StreamTracingExt, TRACE_EXECUTION},
    },
};
use log::{debug, info, warn};
use tracing::Span;

use crate::udf::register_functions;
use crate::{
    chunker::StrictBatchSizeStream,
    utils::{
        BYTES_READ_METRIC, INDEX_CACHE_HITS_METRIC, INDEX_CACHE_MISSES_METRIC,
        INDEX_COMPARISONS_METRIC, INDICES_LOADED_METRIC, IOPS_METRIC, MetricsExt,
        PARTS_LOADED_METRIC, REQUESTS_METRIC,
    },
};

/// An source execution node created from an existing stream
///
/// It can only be used once, and will return the stream.  After that the node
/// is exhausted.
///
/// Note: the stream should be finite, otherwise we will report datafusion properties
/// incorrectly.
pub struct OneShotExec {
    stream: Mutex<Option<SendableRecordBatchStream>>,
    // We save off a copy of the schema to speed up formatting and so ExecutionPlan::schema & display_as
    // can still function after exhausted
    schema: Arc<ArrowSchema>,
    properties: Arc<PlanProperties>,
}

impl OneShotExec {
    /// Create a new instance from a given stream
    pub fn new(stream: SendableRecordBatchStream) -> Self {
        let schema = stream.schema();
        Self {
            stream: Mutex::new(Some(stream)),
            schema: schema.clone(),
            properties: Arc::new(PlanProperties::new(
                EquivalenceProperties::new(schema),
                Partitioning::RoundRobinBatch(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
            )),
        }
    }

    pub fn from_batch(batch: RecordBatch) -> Self {
        let schema = batch.schema();
        let stream = Box::pin(RecordBatchStreamAdapter::new(
            schema,
            stream::iter(vec![Ok(batch)]),
        ));
        Self::new(stream)
    }
}

impl std::fmt::Debug for OneShotExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let stream = self.stream.lock().unwrap();
        f.debug_struct("OneShotExec")
            .field("exhausted", &stream.is_none())
            .field("schema", self.schema.as_ref())
            .finish()
    }
}

impl DisplayAs for OneShotExec {
    fn fmt_as(
        &self,
        t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        let stream = self.stream.lock().unwrap();
        let exhausted = if stream.is_some() { "" } else { "EXHAUSTED" };
        let columns = self
            .schema
            .field_names()
            .iter()
            .cloned()
            .cloned()
            .collect::<Vec<_>>();
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "OneShotStream: {}columns=[{}]",
                    exhausted,
                    columns.join(",")
                )
            }
            DisplayFormatType::TreeRender => {
                write!(
                    f,
                    "OneShotStream\nexhausted={}\ncolumns=[{}]",
                    exhausted,
                    columns.join(",")
                )
            }
        }
    }
}

impl ExecutionPlan for OneShotExec {
    fn name(&self) -> &str {
        "OneShotExec"
    }

    fn schema(&self) -> arrow_schema::SchemaRef {
        self.schema.clone()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        // OneShotExec has no children, so this should only be called with an empty vector
        if !children.is_empty() {
            return Err(datafusion_common::DataFusionError::Internal(
                "OneShotExec does not support children".to_string(),
            ));
        }
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<datafusion::execution::TaskContext>,
    ) -> datafusion_common::Result<SendableRecordBatchStream> {
        let stream = self
            .stream
            .lock()
            .map_err(|err| DataFusionError::Execution(err.to_string()))?
            .take();
        if let Some(stream) = stream {
            Ok(stream)
        } else {
            Err(DataFusionError::Execution(
                "OneShotExec has already been executed".to_string(),
            ))
        }
    }

    fn properties(&self) -> &Arc<datafusion::physical_plan::PlanProperties> {
        &self.properties
    }
}

struct TracedExec {
    input: Arc<dyn ExecutionPlan>,
    properties: Arc<PlanProperties>,
    span: Span,
}

impl TracedExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, span: Span) -> Self {
        Self {
            properties: input.properties().clone(),
            input,
            span,
        }
    }
}

impl DisplayAs for TracedExec {
    fn fmt_as(
        &self,
        t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                write!(f, "TracedExec")
            }
        }
    }
}

impl std::fmt::Debug for TracedExec {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "TracedExec")
    }
}
impl ExecutionPlan for TracedExec {
    fn name(&self) -> &str {
        "TracedExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self {
            input: children[0].clone(),
            properties: self.properties.clone(),
            span: self.span.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion_common::Result<SendableRecordBatchStream> {
        let _guard = self.span.enter();
        let stream = self.input.execute(partition, context)?;
        let schema = stream.schema();
        let stream = stream.stream_in_span(self.span.clone());
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

/// Callback for reporting statistics after a scan
pub type ExecutionStatsCallback = Arc<dyn Fn(&ExecutionSummaryCounts) + Send + Sync>;

#[derive(Default, Clone)]
pub struct LanceExecutionOptions {
    pub use_spilling: bool,
    pub mem_pool_size: Option<u64>,
    pub max_temp_directory_size: Option<u64>,
    pub batch_size: Option<usize>,
    pub target_partition: Option<usize>,
    pub execution_stats_callback: Option<ExecutionStatsCallback>,
    pub skip_logging: bool,
}

impl std::fmt::Debug for LanceExecutionOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LanceExecutionOptions")
            .field("use_spilling", &self.use_spilling)
            .field("mem_pool_size", &self.mem_pool_size)
            .field("max_temp_directory_size", &self.max_temp_directory_size)
            .field("batch_size", &self.batch_size)
            .field("target_partition", &self.target_partition)
            .field("skip_logging", &self.skip_logging)
            .field(
                "execution_stats_callback",
                &self.execution_stats_callback.is_some(),
            )
            .finish()
    }
}

const DEFAULT_LANCE_MEM_POOL_SIZE_PER_PARTITION: u64 = 150 * 1024 * 1024;
const DEFAULT_LANCE_MAX_TEMP_DIRECTORY_SIZE: u64 = 100 * 1024 * 1024 * 1024; // 100GB

impl LanceExecutionOptions {
    pub fn mem_pool_size(&self) -> u64 {
        let num_partitions = self.target_partition.unwrap_or(1) as u64;
        self.mem_pool_size.unwrap_or_else(|| {
            std::env::var("LANCE_MEM_POOL_SIZE")
                .map(|s| match s.parse::<u64>() {
                    Ok(v) => v,
                    Err(e) => {
                        warn!("Failed to parse LANCE_MEM_POOL_SIZE: {}, using default", e);
                        DEFAULT_LANCE_MEM_POOL_SIZE_PER_PARTITION * num_partitions
                    }
                })
                .unwrap_or(DEFAULT_LANCE_MEM_POOL_SIZE_PER_PARTITION * num_partitions)
        })
    }

    pub fn max_temp_directory_size(&self) -> u64 {
        self.max_temp_directory_size.unwrap_or_else(|| {
            std::env::var("LANCE_MAX_TEMP_DIRECTORY_SIZE")
                .map(|s| match s.parse::<u64>() {
                    Ok(v) => v,
                    Err(e) => {
                        warn!(
                            "Failed to parse LANCE_MAX_TEMP_DIRECTORY_SIZE: {}, using default",
                            e
                        );
                        DEFAULT_LANCE_MAX_TEMP_DIRECTORY_SIZE
                    }
                })
                .unwrap_or(DEFAULT_LANCE_MAX_TEMP_DIRECTORY_SIZE)
        })
    }

    pub fn use_spilling(&self) -> bool {
        if !self.use_spilling {
            return false;
        }
        std::env::var("LANCE_BYPASS_SPILLING")
            .map(|_| {
                info!("Bypassing spilling because LANCE_BYPASS_SPILLING is set");
                false
            })
            .unwrap_or(true)
    }
}

pub fn new_session_context(options: &LanceExecutionOptions) -> SessionContext {
    let mut session_config = SessionConfig::new();
    let mut runtime_env_builder = RuntimeEnvBuilder::new();
    if let Some(target_partition) = options.target_partition {
        session_config = session_config.with_target_partitions(target_partition);
    }
    if options.use_spilling() {
        // The default 10MB sort spill reservation seems to be too small for many common cases.
        //
        // There currently is no reasonable guidance provided by DataFusion for setting this value.
        // We bump this to 40MB but try a smaller value if the mem pool is small.
        let sort_spill_reservation_bytes =
            (options.mem_pool_size() / 3).min(40 * 1024 * 1024) as usize;
        session_config =
            session_config.with_sort_spill_reservation_bytes(sort_spill_reservation_bytes);
        let disk_manager_builder = DiskManagerBuilder::default()
            .with_max_temp_directory_size(options.max_temp_directory_size());
        runtime_env_builder = runtime_env_builder
            .with_disk_manager_builder(disk_manager_builder)
            .with_memory_pool(Arc::new(TrackConsumersPool::new(
                FairSpillPool::new(options.mem_pool_size() as usize),
                NonZero::try_from(16).unwrap(),
            )));
    }
    let runtime_env = runtime_env_builder.build_arc().unwrap();

    let ctx = SessionContext::new_with_config_rt(session_config, runtime_env);
    register_functions(&ctx);

    ctx
}

/// Cache key for session contexts based on resolved configuration values.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct SessionContextCacheKey {
    mem_pool_size: u64,
    max_temp_directory_size: u64,
    target_partition: Option<usize>,
    use_spilling: bool,
}

impl SessionContextCacheKey {
    fn from_options(options: &LanceExecutionOptions) -> Self {
        Self {
            mem_pool_size: options.mem_pool_size(),
            max_temp_directory_size: options.max_temp_directory_size(),
            target_partition: options.target_partition,
            use_spilling: options.use_spilling(),
        }
    }
}

struct CachedSessionContext {
    context: SessionContext,
    last_access: std::time::Instant,
}

fn get_session_cache() -> &'static Mutex<HashMap<SessionContextCacheKey, CachedSessionContext>> {
    static SESSION_CACHE: OnceLock<Mutex<HashMap<SessionContextCacheKey, CachedSessionContext>>> =
        OnceLock::new();
    SESSION_CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn get_max_cache_size() -> usize {
    const DEFAULT_CACHE_SIZE: usize = 4;
    static MAX_CACHE_SIZE: OnceLock<usize> = OnceLock::new();
    *MAX_CACHE_SIZE.get_or_init(|| {
        std::env::var("LANCE_SESSION_CACHE_SIZE")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_CACHE_SIZE)
    })
}

pub fn get_session_context(options: &LanceExecutionOptions) -> SessionContext {
    let key = SessionContextCacheKey::from_options(options);
    let mut cache = get_session_cache()
        .lock()
        .unwrap_or_else(|e| e.into_inner());

    // If key exists, update access time and return
    if let Some(entry) = cache.get_mut(&key) {
        entry.last_access = std::time::Instant::now();
        return entry.context.clone();
    }

    // Evict least recently used entry if cache is full
    if cache.len() >= get_max_cache_size()
        && let Some(lru_key) = cache
            .iter()
            .min_by_key(|(_, v)| v.last_access)
            .map(|(k, _)| k.clone())
    {
        cache.remove(&lru_key);
    }

    let context = new_session_context(options);
    cache.insert(
        key,
        CachedSessionContext {
            context: context.clone(),
            last_access: std::time::Instant::now(),
        },
    );
    context
}

fn get_task_context(
    session_ctx: &SessionContext,
    options: &LanceExecutionOptions,
) -> Arc<TaskContext> {
    let mut state = session_ctx.state();
    if let Some(batch_size) = options.batch_size.as_ref() {
        state.config_mut().options_mut().execution.batch_size = *batch_size;
    }

    state.task_ctx()
}

#[derive(Default, Clone, Debug, PartialEq, Eq)]
pub struct ExecutionSummaryCounts {
    /// The number of I/O operations performed
    pub iops: usize,
    /// The number of requests made to the storage layer (may be larger or smaller than iops
    /// depending on coalescing configuration)
    pub requests: usize,
    /// The number of bytes read during the execution of the plan
    pub bytes_read: usize,
    /// The number of top-level indices loaded
    pub indices_loaded: usize,
    /// The number of index partitions loaded
    pub parts_loaded: usize,
    /// The number of index comparisons performed (the exact meaning depends on the index type)
    pub index_comparisons: usize,
    /// Additional metrics for more detailed statistics.  These are subject to change in the future
    /// and should only be used for debugging purposes.
    ///
    /// Newer metrics (e.g. [`INDEX_CACHE_HITS_METRIC`], [`INDEX_CACHE_MISSES_METRIC`]) are added
    /// here rather than as `pub` fields, so this struct stays backwards compatible for callers
    /// that construct or destructure it. Prefer the typed accessors below.
    pub all_counts: HashMap<String, usize>,
    /// Additional time metrics for more detailed statistics, stored in nanoseconds.
    /// These are subject to change in the future and should only be used for debugging purposes.
    pub all_times: HashMap<String, usize>,
}

impl ExecutionSummaryCounts {
    /// Number of index cache page lookups where the loader was not executed
    /// (per-page granularity).
    ///
    /// A "hit" is any page-level lookup at an instrumented cache boundary that
    /// did not run the loader on this call. That covers both a true cache hit
    /// on an already-populated entry and a coalesced concurrent load where an
    /// in-flight loader started by a different caller produced the value.
    ///
    /// Instrumented boundaries in this release:
    /// BTree page, IVF partition (v2, `write_cache=true` scan path), inverted
    /// posting list (grouped and per-token), inverted per-token metadata
    /// (`PostingMetadataKey`), inverted phrase positions (`PositionKey`),
    /// bitmap posting (Equals / Range / IsIn), ngram posting, and rtree page
    /// / null slot.
    ///
    /// Caveats:
    /// * IVF v2 streaming scans and legacy v1 IVF partitions run
    ///   `load_partition` with `write_cache=false`. Those loads always execute
    ///   the loader and never write the result back, so they are reported as a
    ///   miss on every call. See [`Self::index_cache_hit_ratio`].
    /// * A cold posting-list lookup on the grouped inverted layout can record
    ///   up to two misses (posting-list group + per-token metadata) for a
    ///   single term.
    ///
    /// Other index cache boundaries such as HNSW graph pages and quantizer
    /// codebooks are not yet instrumented; a scan that only touches those
    /// paths returns `0` here.
    pub fn index_cache_hits(&self) -> usize {
        self.all_counts
            .get(INDEX_CACHE_HITS_METRIC)
            .copied()
            .unwrap_or(0)
    }

    /// Number of index cache page lookups that had to execute the loader
    /// (per-page granularity).
    ///
    /// A "miss" is any page-level lookup at an instrumented cache boundary
    /// where the loader ran, i.e. the page was not resident and had to be
    /// materialised (typically from storage). See
    /// [`Self::index_cache_hits`] for the paired counter and the list of
    /// instrumented boundaries.
    pub fn index_cache_misses(&self) -> usize {
        self.all_counts
            .get(INDEX_CACHE_MISSES_METRIC)
            .copied()
            .unwrap_or(0)
    }

    /// Ratio of index cache hits to total lookups. Returns `0.0` when no lookups
    /// were recorded in this scan.
    ///
    /// This ratio only reflects paths that write their result back to the
    /// index cache. Streaming scans (IVF v2 `write_cache=false` and legacy v1
    /// IVF `load_partition_stream`) intentionally bypass the cache and are
    /// counted as misses on every call, so a workload dominated by streaming
    /// vector scans will report a hit ratio near `0.0` regardless of cache
    /// size.
    pub fn index_cache_hit_ratio(&self) -> f32 {
        // Widen to u128 before summing so a pathological (hits + misses)
        // overflow can't panic in debug builds nor wrap in release builds.
        let hits = self.index_cache_hits() as u128;
        let total = hits + self.index_cache_misses() as u128;
        if total == 0 {
            0.0
        } else {
            hits as f32 / total as f32
        }
    }
}

pub fn collect_execution_metrics(node: &dyn ExecutionPlan, counts: &mut ExecutionSummaryCounts) {
    if let Some(metrics) = node.metrics() {
        for (metric_name, count) in metrics.iter_counts() {
            match metric_name.as_ref() {
                IOPS_METRIC => counts.iops += count.value(),
                REQUESTS_METRIC => counts.requests += count.value(),
                BYTES_READ_METRIC => counts.bytes_read += count.value(),
                INDICES_LOADED_METRIC => counts.indices_loaded += count.value(),
                PARTS_LOADED_METRIC => counts.parts_loaded += count.value(),
                INDEX_COMPARISONS_METRIC => counts.index_comparisons += count.value(),
                _ => {
                    let existing = counts
                        .all_counts
                        .entry(metric_name.as_ref().to_string())
                        .or_insert(0);
                    *existing += count.value();
                }
            }
        }
        for (metric_name, time) in metrics.iter_times() {
            let existing = counts
                .all_times
                .entry(metric_name.as_ref().to_string())
                .or_insert(0);
            *existing += time.value();
        }
        // Include gauge-based I/O metrics (some nodes record I/O as gauges)
        for (metric_name, gauge) in metrics.iter_gauges() {
            match metric_name.as_ref() {
                IOPS_METRIC => counts.iops += gauge.value(),
                REQUESTS_METRIC => counts.requests += gauge.value(),
                BYTES_READ_METRIC => counts.bytes_read += gauge.value(),
                _ => {}
            }
        }
    }
    for child in node.children() {
        collect_execution_metrics(child.as_ref(), counts);
    }
}

fn report_plan_summary_metrics(plan: &dyn ExecutionPlan, options: &LanceExecutionOptions) {
    let output_rows = plan
        .metrics()
        .map(|m| m.output_rows().unwrap_or(0))
        .unwrap_or(0);
    let mut counts = ExecutionSummaryCounts::default();
    collect_execution_metrics(plan, &mut counts);
    if !options.skip_logging {
        tracing::info!(
            target: TRACE_EXECUTION,
            r#type = EXECUTION_PLAN_RUN,
            plan_summary = display_plan_one_liner(plan),
            output_rows,
            iops = counts.iops,
            requests = counts.requests,
            bytes_read = counts.bytes_read,
            indices_loaded = counts.indices_loaded,
            parts_loaded = counts.parts_loaded,
            index_comparisons = counts.index_comparisons,
            index_cache_hits = counts.index_cache_hits(),
            index_cache_misses = counts.index_cache_misses(),
        );
    }
    if let Some(callback) = options.execution_stats_callback.as_ref() {
        callback(&counts);
    }
}

/// Create a one-line rough summary of the given execution plan.
///
/// The summary just shows the name of the operators in the plan. It omits any
/// details such as parameters or schema information.
///
/// Example: `Projection(Take(CoalesceBatches(Filter(LanceScan))))`
fn display_plan_one_liner(plan: &dyn ExecutionPlan) -> String {
    let mut output = String::new();

    display_plan_one_liner_impl(plan, &mut output);

    output
}

fn display_plan_one_liner_impl(plan: &dyn ExecutionPlan, output: &mut String) {
    // Remove the "Exec" suffix from the plan name if present for brevity
    let name = plan.name().trim_end_matches("Exec");
    output.push_str(name);

    let children = plan.children();
    if !children.is_empty() {
        output.push('(');
        for (i, child) in children.iter().enumerate() {
            if i > 0 {
                output.push(',');
            }
            display_plan_one_liner_impl(child.as_ref(), output);
        }
        output.push(')');
    }
}

/// Executes a plan using default session & runtime configuration
///
/// Only executes a single partition.  Panics if the plan has more than one partition.
pub fn execute_plan(
    plan: Arc<dyn ExecutionPlan>,
    options: LanceExecutionOptions,
) -> Result<SendableRecordBatchStream> {
    if !options.skip_logging {
        debug!(
            "Executing plan:\n{}",
            DisplayableExecutionPlan::new(plan.as_ref()).indent(true)
        );
    }

    let session_ctx = get_session_context(&options);

    // Coalesce to a single partition if the optimizer left more than one.
    // EnforceDistribution may remove RepartitionExec(1) nodes when the parent
    // declares UnspecifiedDistribution, leaving multi-partition plans here.
    //
    // If the plan carries an output ordering (e.g. a top-k `SortExec` whose
    // result was later repartitioned to parallelize downstream operators),
    // a plain `CoalescePartitionsExec` would scramble that order because it
    // merges partitions in scheduling-dependent order. Use an order-preserving
    // merge in that case instead, mirroring what `EnforceDistribution` itself
    // does when it needs to merge an ordered, multi-partition plan.
    let plan: Arc<dyn ExecutionPlan> = if plan.properties().partitioning.partition_count() == 1 {
        plan
    } else if let Some(ordering) = plan.output_ordering() {
        Arc::new(SortPreservingMergeExec::new(ordering.clone(), plan))
    } else {
        Arc::new(CoalescePartitionsExec::new(plan))
    };

    let stream = plan.execute(0, get_task_context(&session_ctx, &options))?;

    let schema = stream.schema();
    let stream = stream.finally(move || {
        if !options.skip_logging || options.execution_stats_callback.is_some() {
            report_plan_summary_metrics(plan.as_ref(), &options);
        }
    });
    Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
}

pub async fn analyze_plan(
    plan: Arc<dyn ExecutionPlan>,
    options: LanceExecutionOptions,
) -> Result<String> {
    // This is needed as AnalyzeExec launches a thread task per
    // partition, and we want these to be connected to the parent span
    let plan = Arc::new(TracedExec::new(plan, Span::current()));

    let schema = plan.schema();
    // TODO(tsaucer) I chose SUMMARY here but do we also want DEV?
    let analyze = Arc::new(AnalyzeExec::new(
        true,
        true,
        vec![MetricType::Summary],
        None,
        plan,
        schema,
    ));

    let session_ctx = get_session_context(&options);
    assert_eq!(analyze.properties().partitioning.partition_count(), 1);
    let mut stream = analyze
        .execute(0, get_task_context(&session_ctx, &options))
        .map_err(|err| Error::io(format!("Failed to execute analyze plan: {}", err)))?;

    // fully execute the plan
    while (stream.next().await).is_some() {}

    let result = format_plan(analyze);
    Ok(result)
}

pub fn format_plan(plan: Arc<dyn ExecutionPlan>) -> String {
    /// A visitor which calculates additional metrics for all the plans.
    struct CalculateVisitor {
        highest_index: usize,
        index_to_elapsed: HashMap<usize, Duration>,
    }

    /// Result of calculating metrics for a subtree
    struct SubtreeMetrics {
        min_start: Option<DateTime<Utc>>,
        max_end: Option<DateTime<Utc>>,
    }

    impl CalculateVisitor {
        fn calculate_metrics(&mut self, plan: &Arc<dyn ExecutionPlan>) -> SubtreeMetrics {
            self.highest_index += 1;
            let plan_index = self.highest_index;

            // Get timestamps for this node
            let (mut min_start, mut max_end) = Self::node_timerange(plan);

            // Accumulate from children
            for child in plan.children() {
                let child_metrics = self.calculate_metrics(child);
                min_start = Self::min_option(min_start, child_metrics.min_start);
                max_end = Self::max_option(max_end, child_metrics.max_end);
            }

            // Calculate wall clock duration for this subtree (only if we have timestamps)
            let elapsed = match (min_start, max_end) {
                (Some(start), Some(end)) => Some((end - start).to_std().unwrap_or_default()),
                _ => None,
            };

            if let Some(e) = elapsed {
                self.index_to_elapsed.insert(plan_index, e);
            }

            SubtreeMetrics { min_start, max_end }
        }

        fn node_timerange(
            plan: &Arc<dyn ExecutionPlan>,
        ) -> (Option<DateTime<Utc>>, Option<DateTime<Utc>>) {
            let Some(metrics) = plan.metrics() else {
                return (None, None);
            };
            let min_start = metrics
                .iter()
                .filter_map(|m| match m.value() {
                    MetricValue::StartTimestamp(ts) => ts.value(),
                    _ => None,
                })
                .min();
            let max_end = metrics
                .iter()
                .filter_map(|m| match m.value() {
                    MetricValue::EndTimestamp(ts) => ts.value(),
                    _ => None,
                })
                .max();
            (min_start, max_end)
        }

        fn min_option(a: Option<DateTime<Utc>>, b: Option<DateTime<Utc>>) -> Option<DateTime<Utc>> {
            [a, b].into_iter().flatten().min()
        }

        fn max_option(a: Option<DateTime<Utc>>, b: Option<DateTime<Utc>>) -> Option<DateTime<Utc>> {
            [a, b].into_iter().flatten().max()
        }
    }

    /// A visitor which prints out all the plans.
    struct PrintVisitor {
        highest_index: usize,
        indent: usize,
    }
    impl PrintVisitor {
        fn write_output(
            &mut self,
            plan: &Arc<dyn ExecutionPlan>,
            f: &mut Formatter,
            calcs: &CalculateVisitor,
        ) -> std::fmt::Result {
            self.highest_index += 1;
            write!(f, "{:indent$}", "", indent = self.indent * 2)?;

            // Format the plan description
            let displayable =
                datafusion::physical_plan::display::DisplayableExecutionPlan::new(plan.as_ref());
            let plan_str = displayable.one_line().to_string();
            let plan_str = plan_str.trim();

            // Write operator with elapsed time inserted after the name
            match calcs.index_to_elapsed.get(&self.highest_index) {
                Some(elapsed) => match plan_str.find(": ") {
                    Some(i) => write!(
                        f,
                        "{}: elapsed={elapsed:?}, {}",
                        &plan_str[..i],
                        &plan_str[i + 2..]
                    )?,
                    None => write!(f, "{plan_str}, elapsed={elapsed:?}")?,
                },
                None => write!(f, "{plan_str}")?,
            }

            if let Some(metrics) = plan.metrics() {
                let metrics = metrics
                    .aggregate_by_name()
                    .sorted_for_display()
                    .timestamps_removed();

                write!(f, ", metrics=[{metrics}]")?;
            } else {
                write!(f, ", metrics=[]")?;
            }
            writeln!(f)?;
            self.indent += 1;
            for child in plan.children() {
                self.write_output(child, f, calcs)?;
            }
            self.indent -= 1;
            std::fmt::Result::Ok(())
        }
    }
    // A wrapper which prints out a plan.
    struct PrintWrapper {
        plan: Arc<dyn ExecutionPlan>,
    }
    impl fmt::Display for PrintWrapper {
        fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
            let mut calcs = CalculateVisitor {
                highest_index: 0,
                index_to_elapsed: HashMap::new(),
            };
            calcs.calculate_metrics(&self.plan);
            let mut prints = PrintVisitor {
                highest_index: 0,
                indent: 0,
            };
            prints.write_output(&self.plan, f, &calcs)
        }
    }
    let wrapper = PrintWrapper { plan };
    format!("{}", wrapper)
}

pub trait SessionContextExt {
    /// Creates a DataFrame for reading a stream of data
    ///
    /// This dataframe may only be queried once, future queries will fail
    fn read_one_shot(
        &self,
        data: SendableRecordBatchStream,
    ) -> datafusion::common::Result<DataFrame>;
}

pub struct OneShotPartitionStream {
    data: Arc<Mutex<Option<SendableRecordBatchStream>>>,
    schema: Arc<ArrowSchema>,
}

impl std::fmt::Debug for OneShotPartitionStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let data = self.data.lock().unwrap();
        f.debug_struct("OneShotPartitionStream")
            .field("exhausted", &data.is_none())
            .field("schema", self.schema.as_ref())
            .finish()
    }
}

impl OneShotPartitionStream {
    pub fn new(data: SendableRecordBatchStream) -> Self {
        let schema = data.schema();
        Self {
            data: Arc::new(Mutex::new(Some(data))),
            schema,
        }
    }
}

impl PartitionStream for OneShotPartitionStream {
    fn schema(&self) -> &arrow_schema::SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let mut stream = self.data.lock().unwrap();
        stream
            .take()
            .expect("Attempt to consume a one shot dataframe multiple times")
    }
}

impl SessionContextExt for SessionContext {
    fn read_one_shot(
        &self,
        data: SendableRecordBatchStream,
    ) -> datafusion::common::Result<DataFrame> {
        let schema = data.schema();
        let part_stream = Arc::new(OneShotPartitionStream::new(data));
        let provider = StreamingTable::try_new(schema, vec![part_stream])?;
        self.read_table(Arc::new(provider))
    }
}

/// Scan a [`TableProvider`] into a single-partition [`SendableRecordBatchStream`].
///
/// Multi-partition providers are coalesced into a single partition. This adapts a
/// re-scannable provider back into the one stream the writer pipeline consumes;
/// re-scanning the same provider (e.g. on a write retry) yields a fresh stream.
///
/// # Examples
///
/// ```
/// # use std::sync::Arc;
/// # use arrow_array::{Int32Array, RecordBatch};
/// # use arrow_schema::{DataType, Field, Schema};
/// # use datafusion::catalog::TableProvider;
/// # use datafusion::datasource::MemTable;
/// # use futures::TryStreamExt;
/// # use lance_datafusion::exec::provider_to_stream;
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
/// let batch =
///     RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![1, 2, 3]))])?;
/// let provider: Arc<dyn TableProvider> = Arc::new(MemTable::try_new(schema, vec![vec![batch]])?);
///
/// // A re-scannable provider yields a fresh stream on each call.
/// let batches: Vec<RecordBatch> = provider_to_stream(provider).await?.try_collect().await?;
/// assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 3);
/// # Ok(())
/// # }
/// ```
pub async fn provider_to_stream(
    provider: Arc<dyn TableProvider>,
) -> Result<SendableRecordBatchStream> {
    let ctx = SessionContext::new();
    let plan = provider.scan(&ctx.state(), None, &[], None).await?;
    let plan: Arc<dyn ExecutionPlan> =
        if plan.properties().output_partitioning().partition_count() > 1 {
            Arc::new(CoalescePartitionsExec::new(plan))
        } else {
            plan
        };
    Ok(plan.execute(0, ctx.task_ctx())?)
}

#[derive(Clone, Debug)]
pub struct StrictBatchSizeExec {
    input: Arc<dyn ExecutionPlan>,
    batch_size: usize,
}

impl StrictBatchSizeExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, batch_size: usize) -> Self {
        Self { input, batch_size }
    }
}

impl DisplayAs for StrictBatchSizeExec {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "StrictBatchSizeExec")
    }
}

impl ExecutionPlan for StrictBatchSizeExec {
    fn name(&self) -> &str {
        "StrictBatchSizeExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.input.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self {
            input: children[0].clone(),
            batch_size: self.batch_size,
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion_common::Result<SendableRecordBatchStream> {
        let stream = self.input.execute(partition, context)?;
        let schema = stream.schema();
        let stream = StrictBatchSizeStream::new(stream, self.batch_size);
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn partition_statistics(
        &self,
        partition: Option<usize>,
    ) -> datafusion_common::Result<std::sync::Arc<Statistics>> {
        self.input.partition_statistics(partition)
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::Equal
    }

    fn supports_limit_pushdown(&self) -> bool {
        true
    }
}

/// Exec node that rechunks batches so no output batch exceeds `max_bytes`.
///
/// # Why this exists
///
/// DataFusion's sort operator cannot handle batches larger than the memory
/// pool size.  When upstream operators produce very large batches this can
/// cause the sort to fail.  This node caps batch sizes
/// *before* the sort so the operation succeeds.  The trade-off is a
/// potentially expensive deep copy of the batch data — see below — but that
/// is preferable to failing the operation entirely.  This workaround may
/// become unnecessary if a fix is upstreamed to DataFusion.
///
/// # Deep copy
///
/// After slicing a RecordBatch, `get_array_memory_size` still reports the
/// size of the *original* backing buffers, not the slice.  To get accurate
/// sizes the slices must be deep-copied.  This is a last resort and can be
/// expensive for large batches, but the deep copy is only performed when a
/// batch actually needs to be sliced — batches that are already within the
/// target range pass through at zero cost.
///
/// If a single row exceeds `max_bytes`, execution fails with an error.
#[derive(Clone, Debug)]
pub struct HardCapBatchSizeExec {
    input: Arc<dyn ExecutionPlan>,
    max_bytes: usize,
}

impl HardCapBatchSizeExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, max_bytes: usize) -> Self {
        Self { input, max_bytes }
    }
}

impl DisplayAs for HardCapBatchSizeExec {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "HardCapBatchSizeExec(max_bytes={})", self.max_bytes)
    }
}

impl ExecutionPlan for HardCapBatchSizeExec {
    fn name(&self) -> &str {
        "HardCapBatchSizeExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.input.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self {
            input: children[0].clone(),
            max_bytes: self.max_bytes,
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion_common::Result<SendableRecordBatchStream> {
        let stream = self.input.execute(partition, context)?;
        let schema = stream.schema();
        let max_bytes = self.max_bytes;
        let rechunked = lance_arrow::stream::rechunk_stream_by_size_deep_copy(
            stream,
            schema.clone(),
            0,
            max_bytes,
        );
        // Check that no single-row batch exceeds the limit.
        let validated = rechunked.map(move |result| {
            let batch = result?;
            if batch.num_rows() == 1 && batch.get_array_memory_size() > max_bytes {
                return Err(DataFusionError::External(Box::new(Error::invalid_input(
                    format!(
                        "a single row is {} bytes which exceeds the maximum allowed batch \
                         size of {} bytes",
                        batch.get_array_memory_size(),
                        max_bytes,
                    ),
                ))));
            }
            Ok(batch)
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, validated)))
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn partition_statistics(
        &self,
        partition: Option<usize>,
    ) -> datafusion_common::Result<std::sync::Arc<Statistics>> {
        self.input.partition_statistics(partition)
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::Equal
    }

    fn supports_limit_pushdown(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Serialize cache tests since they share global state
    static CACHE_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[test]
    fn test_session_context_cache() {
        let _lock = CACHE_TEST_LOCK.lock().unwrap();
        let cache = get_session_cache();

        // Clear any existing entries from other tests
        cache.lock().unwrap().clear();

        // Create first session with default options
        let opts1 = LanceExecutionOptions::default();
        let _ctx1 = get_session_context(&opts1);

        {
            let cache_guard = cache.lock().unwrap();
            assert_eq!(cache_guard.len(), 1);
        }

        // Same options should reuse cached session (no new entry)
        let _ctx1_again = get_session_context(&opts1);
        {
            let cache_guard = cache.lock().unwrap();
            assert_eq!(cache_guard.len(), 1);
        }

        // Different options should create new entry
        let opts2 = LanceExecutionOptions {
            use_spilling: true,
            ..Default::default()
        };
        let _ctx2 = get_session_context(&opts2);
        {
            let cache_guard = cache.lock().unwrap();
            assert_eq!(cache_guard.len(), 2);
        }
    }

    #[test]
    fn test_session_context_cache_lru_eviction() {
        let _lock = CACHE_TEST_LOCK.lock().unwrap();
        let cache = get_session_cache();

        // Clear any existing entries from other tests
        cache.lock().unwrap().clear();

        // Create 4 different configurations to fill the cache
        let configs: Vec<LanceExecutionOptions> = (0..4)
            .map(|i| LanceExecutionOptions {
                mem_pool_size: Some((i + 1) as u64 * 1024 * 1024),
                ..Default::default()
            })
            .collect();

        for config in &configs {
            let _ctx = get_session_context(config);
        }

        {
            let cache_guard = cache.lock().unwrap();
            assert_eq!(cache_guard.len(), 4);
        }

        // Access config[0] to make it more recently used than config[1]
        // (config[0] was inserted first, so without this access it would be evicted)
        std::thread::sleep(std::time::Duration::from_millis(1));
        let _ctx = get_session_context(&configs[0]);

        // Add a 5th configuration - should evict config[1] (now least recently used)
        let opts5 = LanceExecutionOptions {
            mem_pool_size: Some(5 * 1024 * 1024),
            ..Default::default()
        };
        let _ctx5 = get_session_context(&opts5);

        {
            let cache_guard = cache.lock().unwrap();
            assert_eq!(cache_guard.len(), 4);

            // config[0] should still be present (was accessed recently)
            let key0 = SessionContextCacheKey::from_options(&configs[0]);
            assert!(
                cache_guard.contains_key(&key0),
                "config[0] should still be cached after recent access"
            );

            // config[1] should be evicted (was least recently used)
            let key1 = SessionContextCacheKey::from_options(&configs[1]);
            assert!(
                !cache_guard.contains_key(&key1),
                "config[1] should have been evicted"
            );

            // New config should be present
            let key5 = SessionContextCacheKey::from_options(&opts5);
            assert!(
                cache_guard.contains_key(&key5),
                "new config should be cached"
            );
        }
    }

    #[test]
    fn test_mem_pool_size_scales_with_partitions() {
        let default_per_partition = DEFAULT_LANCE_MEM_POOL_SIZE_PER_PARTITION;

        // No partitions specified → defaults to 1 partition
        let opts = LanceExecutionOptions::default();
        assert_eq!(opts.mem_pool_size(), default_per_partition);

        // 4 partitions → 4x the per-partition size
        let opts = LanceExecutionOptions {
            target_partition: Some(4),
            ..Default::default()
        };
        assert_eq!(opts.mem_pool_size(), default_per_partition * 4);

        // 8 partitions → 8x the per-partition size
        let opts = LanceExecutionOptions {
            target_partition: Some(8),
            ..Default::default()
        };
        assert_eq!(opts.mem_pool_size(), default_per_partition * 8);

        // Explicit mem_pool_size is not scaled
        let opts = LanceExecutionOptions {
            mem_pool_size: Some(50 * 1024 * 1024),
            target_partition: Some(8),
            ..Default::default()
        };
        assert_eq!(opts.mem_pool_size(), 50 * 1024 * 1024);
    }
}
