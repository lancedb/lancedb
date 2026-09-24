// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Index-partition duplicate pairs. No source-vector scan or shuffle is needed.

use std::collections::BTreeSet;
use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::catalog::TableFunctionImpl;
use datafusion_catalog::{Session, TableProvider};
use datafusion_common::{DataFusionError, Result, ScalarValue, plan_err};
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_expr::{Expr, TableType};
use datafusion_physical_expr::EquivalenceProperties;
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    execution_plan::{Boundedness, EmissionType},
    metrics::{ExecutionPlanMetricsSet, MetricBuilder, MetricsSet},
    projection::ProjectionExec,
    stream::RecordBatchStreamAdapter,
};
use futures::{StreamExt, TryStreamExt, stream};
use lance::{
    Dataset,
    index::{DatasetIndexExt, DatasetIndexInternalExt},
};
use lance_index::metrics::NoOpMetricsCollector;
use uuid::Uuid;

use super::super::BaseTableAdapter;
use crate::table::NativeTableExt;

// Admission estimates for Lance 13's bounded pairwise kernel. Keep these with
// the minimum dependency when the kernel's buffering contract changes.
/// Version of the native task descriptor and its execution semantics.
pub const DUPLICATE_PAIRS_OPERATOR_VERSION: u32 = 2;

const CODE_STAGING_BYTES: usize = 16 * 1024 * 1024;
const INDEX_READ_ROWS: usize = 8192;
const VECTOR_BATCH_ROWS: usize = 8192;
const VECTOR_BATCH_BYTES: usize = 16 * 1024 * 1024;
const MIN_VECTOR_BATCH_ROWS: usize = 32;
const ANCHOR_BLOCK_ROWS: usize = 32;
const SCORING_CONCURRENCY: usize = 4;
const MAX_COORDINATE_BYTES: usize = 8;
const STAGED_ROW_OVERHEAD_BYTES: usize = 64;
const PAIR_ROW_BYTES: usize = 20;

/// Configuration belongs to one immutable source snapshot. Threshold uses
/// Lance's float32 index distance, including symmetric reconstructed distances
/// for quantized indexes; it is not an exact source-embedding distance.
#[derive(Debug, Clone)]
pub struct DuplicatePairsConfig {
    pub dataset_version: u64,
    pub column: String,
    pub distance_threshold: f32,
}

/// One worker input, never a row of source vectors. The complete task identity
/// also includes the dataset URI/version, column, threshold and operator version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DuplicatePairTask {
    pub segment_id: Uuid,
    pub partition_id: usize,
}

/// Resolve an authorized source table and preserve its catalog identity.
pub trait DuplicatePairsResolver: std::fmt::Debug + Send + Sync {
    fn resolve(
        &self,
        table: &str,
        config: DuplicatePairsConfig,
        tasks_only: bool,
    ) -> Result<Arc<dyn TableProvider>>;
}

/// `vector_duplicate_pairs('table', version, 'column', threshold)`.
/// Registration is explicit so the host controls catalog access and execution.
#[derive(Debug)]
pub struct DuplicatePairsTableFunction {
    resolver: Arc<dyn DuplicatePairsResolver>,
    tasks_only: bool,
}

impl DuplicatePairsTableFunction {
    pub fn new(resolver: Arc<dyn DuplicatePairsResolver>) -> Self {
        Self {
            resolver,
            tasks_only: false,
        }
    }
}

impl DuplicatePairsTableFunction {
    /// Register as `vector_duplicate_pair_tasks` to inspect or persist exactly
    /// the descriptors used by the pair operator, without producing any pairs.
    pub fn task_manifest(resolver: Arc<dyn DuplicatePairsResolver>) -> Self {
        Self {
            resolver,
            tasks_only: true,
        }
    }
}

impl TableFunctionImpl for DuplicatePairsTableFunction {
    fn call(&self, args: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        if args.len() != 4 {
            return plan_err!(
                "vector_duplicate_pairs requires (table, dataset_version, column, distance_threshold)"
            );
        }
        let string = |expr: &Expr| match expr {
            Expr::Literal(ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)), _) => {
                Ok(s.clone())
            }
            _ => plan_err!("table and column must be string literals"),
        };
        let dataset_version = match &args[1] {
            Expr::Literal(ScalarValue::Int64(Some(v)), _) if *v > 0 => *v as u64,
            Expr::Literal(ScalarValue::UInt64(Some(v)), _) if *v > 0 => *v,
            _ => return plan_err!("dataset_version must be a positive integer literal"),
        };
        let distance_threshold = match &args[3] {
            Expr::Literal(value, _) if value.data_type().is_numeric() => {
                value.cast_to(&DataType::Float32)?
            }
            _ => return plan_err!("distance_threshold must be a finite numeric literal"),
        };
        let ScalarValue::Float32(Some(distance_threshold)) = distance_threshold else {
            return plan_err!("distance_threshold cannot be null");
        };
        if !distance_threshold.is_finite() {
            return plan_err!("distance_threshold must be finite");
        }
        self.resolver.resolve(
            &string(&args[0])?,
            DuplicatePairsConfig {
                dataset_version,
                column: string(&args[2])?,
                distance_threshold,
            },
            self.tasks_only,
        )
    }
}

/// Three non-null columns; pair direction is storage traversal order, not row-ID order.
pub fn duplicate_pair_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("row_id_a", DataType::UInt64, false),
        Field::new("row_id_b", DataType::UInt64, false),
        Field::new("distance", DataType::Float32, false),
    ]))
}

#[derive(Debug)]
pub struct DuplicatePairsTable {
    source: Arc<dyn crate::table::BaseTable>,
    config: DuplicatePairsConfig,
    tasks_only: bool,
}

impl BaseTableAdapter {
    /// Bind the builtin to this authorized table; only metadata is read while
    /// planning. The source snapshot must remain retained until execution ends.
    pub fn duplicate_pairs(
        &self,
        config: DuplicatePairsConfig,
        tasks_only: bool,
    ) -> DuplicatePairsTable {
        DuplicatePairsTable {
            source: self.table.clone(),
            config,
            tasks_only,
        }
    }
}

#[async_trait]
impl TableProvider for DuplicatePairsTable {
    fn schema(&self) -> SchemaRef {
        if self.tasks_only {
            duplicate_pair_task_schema()
        } else {
            duplicate_pair_schema()
        }
    }
    fn table_type(&self) -> TableType {
        TableType::View
    }
    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let native = self.source.as_native().ok_or_else(|| {
            DataFusionError::Plan("duplicate pairs requires a native indexed table".into())
        })?;
        let current = native
            .dataset
            .get()
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let dataset = Arc::new(
            current
                .checkout_version(self.config.dataset_version)
                .await?,
        );
        let tasks = plan_duplicate_pairs(dataset.clone(), &self.config).await?;
        if self.tasks_only {
            let batch = duplicate_pair_task_batch(&dataset, &self.config, &tasks)?;
            let table =
                datafusion::datasource::MemTable::try_new(batch.schema(), vec![vec![batch]])?;
            return table.scan(_state, projection, _filters, _limit).await;
        }
        let plan: Arc<dyn ExecutionPlan> = Arc::new(DuplicatePairsExec::try_new(
            dataset,
            self.config.clone(),
            tasks,
        )?);
        match projection {
            Some(indices) => {
                let schema = plan.schema();
                let exprs = indices
                    .iter()
                    .map(|&i| {
                        (
                            Arc::new(datafusion_physical_expr::expressions::Column::new(
                                schema.field(i).name(),
                                i,
                            )) as _,
                            schema.field(i).name().clone(),
                        )
                    })
                    .collect::<Vec<_>>();
                Ok(Arc::new(ProjectionExec::try_new(exprs, plan)?))
            }
            None => Ok(plan),
        }
    }
}

/// Enumerate segment-local IVF partitions using metadata only. Live-row masks
/// and index codes are opened by the scoped worker, never by a source-vector
/// scan on the coordinator. The high-level pair API is only a test oracle.
pub async fn plan_duplicate_pairs(
    dataset: Arc<Dataset>,
    config: &DuplicatePairsConfig,
) -> Result<Vec<DuplicatePairTask>> {
    if dataset.version().version != config.dataset_version || !config.distance_threshold.is_finite()
    {
        return plan_err!("duplicate pairs source snapshot or threshold is invalid");
    }
    let field = dataset.schema().field_id(&config.column)?;
    let indices = dataset.load_indices().await?;
    let segments = indices
        .iter()
        .filter(|meta| {
            meta.keyed_fields() == [field]
                && meta.index_details.as_ref().map_or_else(
                    || {
                        meta.files.as_ref().is_some_and(|files| {
                            files
                                .iter()
                                .any(|file| file.path == lance_index::INDEX_FILE_NAME)
                        })
                    },
                    |details| details.type_url.ends_with("VectorIndexDetails"),
                )
        })
        .collect::<Vec<_>>();
    let names = segments
        .iter()
        .map(|meta| meta.name.as_str())
        .collect::<BTreeSet<_>>();
    if names.len() != 1 {
        return plan_err!(
            "column '{}' requires exactly one current-format vector index, found {}",
            config.column,
            names.len()
        );
    }
    let name = names.into_iter().next().expect("one index name");
    if !dataset.unindexed_fragments(name).await?.is_empty() {
        return plan_err!(
            "vector index on column '{}' does not cover all fragments; optimize the index first",
            config.column
        );
    }
    let mut tasks = Vec::new();
    for meta in segments {
        if dataset.fragments().iter().any(|fragment| {
            meta.fragment_bitmap
                .as_ref()
                .is_none_or(|ids| ids.contains(fragment.id as u32))
                && fragment.overlays.iter().any(|overlay| {
                    overlay.committed_version > meta.dataset_version
                        && overlay.data_file.fields.contains(&field)
                })
        }) {
            return plan_err!(
                "column '{}' has stale index values in segment {}; rebuild the index",
                config.column,
                meta.uuid
            );
        }
        let index = dataset
            .open_vector_index(&config.column, &meta.uuid, &NoOpMetricsCollector)
            .await?;
        if !index.supports_pairwise_vectors() {
            return plan_err!(
                "segment {} requires a current-format pairwise vector index; rebuild the index",
                meta.uuid
            );
        }
        for partition_id in 0..index.ivf_model().num_partitions() {
            tasks.push(DuplicatePairTask {
                segment_id: meta.uuid,
                partition_id,
            });
        }
    }
    if tasks.is_empty() {
        return plan_err!("no vector index partitions found");
    }
    Ok(tasks)
}

/// Streaming native operator over partition descriptors, never source-vector
/// rows. Assigned descriptors are consumed sequentially; distributed hosts send
/// one descriptor per worker request. No Python or per-vector ANN calls.
/// Distributed merging deliberately advertises no global row ordering.
#[derive(Debug)]
pub struct DuplicatePairsExec {
    dataset: Arc<Dataset>,
    config: DuplicatePairsConfig,
    tasks: Vec<DuplicatePairTask>,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl DuplicatePairsExec {
    pub fn try_new(
        dataset: Arc<Dataset>,
        config: DuplicatePairsConfig,
        tasks: Vec<DuplicatePairTask>,
    ) -> Result<Self> {
        if dataset.version().version != config.dataset_version
            || !config.distance_threshold.is_finite()
            || tasks.is_empty()
        {
            return plan_err!("invalid duplicate-pair snapshot, threshold, or empty task list");
        }
        let unique = tasks
            .iter()
            .map(|t| (t.segment_id, t.partition_id))
            .collect::<BTreeSet<_>>();
        if unique.len() != tasks.len() {
            return plan_err!("duplicate partition tasks");
        }
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(duplicate_pair_schema()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Ok(Self {
            dataset,
            config,
            tasks,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }
    pub fn dataset(&self) -> &Arc<Dataset> {
        &self.dataset
    }
    pub fn config(&self) -> &DuplicatePairsConfig {
        &self.config
    }
    pub fn tasks(&self) -> &[DuplicatePairTask] {
        &self.tasks
    }
}

impl DisplayAs for DuplicatePairsExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "DuplicatePairsExec: source_version={}, column={}, threshold={}, tasks={}",
            self.config.dataset_version,
            self.config.column,
            self.config.distance_threshold,
            self.tasks.len()
        )
    }
}

impl ExecutionPlan for DuplicatePairsExec {
    fn name(&self) -> &str {
        "DuplicatePairsExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return plan_err!("DuplicatePairsExec has no children");
        }
        Ok(self)
    }
    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return plan_err!("duplicate-pair output partition out of bounds");
        }
        let tasks = self.tasks.clone();
        let dataset = self.dataset.clone();
        let config = self.config.clone();
        let output_rows = MetricBuilder::new(&self.metrics).counter("output_rows", partition);
        let completed =
            MetricBuilder::new(&self.metrics).counter("completed_partitions", partition);
        // Beta.14 stages native codes and scores bounded tiles concurrently.
        // Keep the host staging budget explicit rather than inheriting Lance's
        // larger default or multiplying CPU-pool concurrency across PE tasks.
        let dimension = match dataset
            .schema()
            .field(&config.column)
            .map(|f| f.data_type())
        {
            Some(DataType::FixedSizeList(_, dimension)) if dimension > 0 => dimension as usize,
            _ => return plan_err!("duplicate pairs requires a fixed-size vector column"),
        };
        let batch_bytes = VECTOR_BATCH_BYTES.max(
            MIN_VECTOR_BATCH_ROWS * (dimension * MAX_COORDINATE_BYTES + STAGED_ROW_OVERHEAD_BYTES),
        );
        // Allow staging/preparation overlap, at least three live spill batches,
        // source input, and in-flight output plus the batch being encoded.
        // Quantizer models and caches remain outside this admission estimate.
        let budget = 2 * CODE_STAGING_BYTES
            + 3 * batch_bytes
            + INDEX_READ_ROWS * (dimension * MAX_COORDINATE_BYTES + STAGED_ROW_OVERHEAD_BYTES)
            + (SCORING_CONCURRENCY + 1) * ANCHOR_BLOCK_ROWS * VECTOR_BATCH_ROWS * PAIR_ROW_BYTES;
        let reservation =
            datafusion_execution::memory_pool::MemoryConsumer::new("DuplicatePairsExec")
                .register(context.memory_pool());
        reservation.try_grow(budget)?;
        let reservation = Arc::new(reservation);
        // Sequential local work prevents one query from reserving a kernel for
        // every IVF partition at once. Distributed hosts split these descriptors
        // into one-task PE requests before execution.
        let stream = stream::iter(tasks)
            .then(move |task| {
                let dataset = dataset.clone();
                let config = config.clone();
                let reservation = reservation.clone();
                let output_rows = output_rows.clone();
                let completed = completed.clone();
                async move {
                    let reader = lance::index::vector::dedup::find_duplicate_pairs_in_partition_with_options(
                        dataset,
                        &config.column,
                        task.segment_id,
                        task.partition_id,
                        config.distance_threshold,
                        lance::index::vector::dedup::DuplicatePairsOptions::default()
                            .with_memory_limit(CODE_STAGING_BYTES)
                            .with_max_concurrency(SCORING_CONCURRENCY),
                    )
                    .await?;
                    let measured = stream::try_unfold(
                        (reader, reservation),
                        move |(mut reader, reservation)| {
                            let output_rows = output_rows.clone();
                            let completed = completed.clone();
                            async move {
                                match reader.try_next().await? {
                                    Some(batch) => {
                                        output_rows.add(batch.num_rows());
                                        Ok::<_, DataFusionError>(Some((
                                            batch,
                                            (reader, reservation),
                                        )))
                                    }
                                    None => {
                                        completed.add(1);
                                        Ok(None)
                                    }
                                }
                            }
                        },
                    );
                    Ok::<_, DataFusionError>(Box::pin(RecordBatchStreamAdapter::new(
                        duplicate_pair_schema(),
                        measured,
                    )) as SendableRecordBatchStream)
                }
            })
            .try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            duplicate_pair_schema(),
            stream,
        )))
    }
}

/// Inspectable immutable task manifest. Credentials are never included.
pub fn duplicate_pair_task_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("task_id", DataType::Utf8, false),
        Field::new("dataset_uri", DataType::Utf8, false),
        Field::new("dataset_version", DataType::UInt64, false),
        Field::new("column", DataType::Utf8, false),
        Field::new("segment_id", DataType::Utf8, false),
        Field::new("partition_id", DataType::UInt64, false),
        Field::new("distance_threshold", DataType::Float32, false),
        Field::new("operator_version", DataType::UInt32, false),
    ]))
}

pub fn duplicate_pair_task_batch(
    dataset: &Dataset,
    config: &DuplicatePairsConfig,
    tasks: &[DuplicatePairTask],
) -> Result<arrow_array::RecordBatch> {
    use arrow_array::{Float32Array, StringArray, UInt32Array, UInt64Array};
    use sha2::{Digest, Sha256};
    let ids = tasks
        .iter()
        .map(|task| {
            // Length-delimited strings prevent ambiguous concatenation; threshold
            // bits preserve the exact float32 value sent to the native primitive.
            let bytes = serde_json::to_vec(&(
                DUPLICATE_PAIRS_OPERATOR_VERSION,
                dataset.uri(),
                config.dataset_version,
                &config.column,
                task.segment_id.to_string(),
                task.partition_id,
                config.distance_threshold.to_bits(),
            ))
            .expect("task fields are serializable");
            format!("{:x}", Sha256::digest(bytes))
        })
        .collect::<Vec<_>>();
    let n = tasks.len();
    Ok(arrow_array::RecordBatch::try_new(
        duplicate_pair_task_schema(),
        vec![
            Arc::new(StringArray::from(ids)),
            Arc::new(StringArray::from(vec![dataset.uri(); n])),
            Arc::new(UInt64Array::from(vec![config.dataset_version; n])),
            Arc::new(StringArray::from(vec![config.column.as_str(); n])),
            Arc::new(StringArray::from(
                tasks
                    .iter()
                    .map(|t| t.segment_id.to_string())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(UInt64Array::from(
                tasks
                    .iter()
                    .map(|t| t.partition_id as u64)
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Float32Array::from(vec![config.distance_threshold; n])),
            Arc::new(UInt32Array::from(vec![DUPLICATE_PAIRS_OPERATOR_VERSION; n])),
        ],
    )?)
}
