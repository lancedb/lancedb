// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Composable index-backed pair and dedup relations. Hosts bind a snapshot once
//! during planning, distribute the pair child, and use their ordinary SQL sink.

use std::sync::Arc;

use arrow_array::{RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::prelude::{SessionConfig, SessionContext, col};
use datafusion_catalog::{Session, TableProvider};
use datafusion_common::{DataFusionError, Result, plan_err};
use datafusion_execution::{
    SendableRecordBatchStream, TaskContext, runtime_env::RuntimeEnvBuilder,
};
use datafusion_expr::{Expr, TableType};
use datafusion_physical_expr::{EquivalenceProperties, Partitioning};
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
    coalesce_partitions::CoalescePartitionsExec,
    empty::EmptyExec,
    execution_plan::{Boundedness, EmissionType},
    projection::ProjectionExec,
    stream::RecordBatchStreamAdapter,
};
use futures::{TryStreamExt, stream};
use lance::dataset::scanner::{RowAddrMask, RowAddrTreeMap};
use lance::{Dataset, index::DatasetIndexInternalExt};
use lance_core::datatypes::BlobHandling;
use lance_datafusion::exec::SessionContextExt;
use lance_index::metrics::NoOpMetricsCollector;
use roaring::RoaringTreemap;

use super::duplicate_pairs::{
    DuplicatePairTask, DuplicatePairsConfig, DuplicatePairsExec, DuplicatePairsOutput,
    plan_duplicate_pairs,
};
use crate::table::{NativeTableExt, datafusion::BaseTableAdapter};

const REPRESENTATIVE_SORT_MEMORY_BYTES: usize = 256 * 1024 * 1024;
const MATERIALIZE_BATCH_ROWS: usize = 1024;

fn edge_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("a", DataType::UInt64, false),
        Field::new("b", DataType::UInt64, false),
    ]))
}

/// Sorting canonical edges makes the first unremoved endpoint the smallest
/// unassigned representative. The sort spills; only vertex masks stay in RAM.
async fn select_representatives(
    stream: SendableRecordBatchStream,
    memory: usize,
) -> crate::Result<(RoaringTreemap, RoaringTreemap)> {
    let schema = edge_schema();
    let batch_schema = schema.clone();
    let canonical = stream
        .map_ok(move |batch| {
            let a = batch
                .column(0)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .expect("native row id");
            let b = batch
                .column(1)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .expect("native row id");
            RecordBatch::try_new(
                batch_schema.clone(),
                vec![
                    Arc::new(UInt64Array::from_iter_values(
                        a.values().iter().zip(b.values()).map(|(&a, &b)| a.min(b)),
                    )),
                    Arc::new(UInt64Array::from_iter_values(
                        a.values().iter().zip(b.values()).map(|(&a, &b)| a.max(b)),
                    )),
                ],
            )
            .map_err(DataFusionError::from)
        })
        .and_then(futures::future::ready);
    let canonical = Box::pin(RecordBatchStreamAdapter::new(schema, canonical));
    let mut config = SessionConfig::default().with_batch_size(MATERIALIZE_BATCH_ROWS);
    config.options_mut().execution.sort_spill_reservation_bytes = memory / 2;
    let ctx = SessionContext::new_with_config_rt(
        config,
        RuntimeEnvBuilder::new()
            .with_memory_limit(memory, 1.0)
            .build_arc()?,
    );
    let mut sorted = ctx
        .read_one_shot(canonical)?
        .sort_by(vec![col("a"), col("b")])?
        .execute_stream()
        .await?;
    let mut removed = RoaringTreemap::new();
    let mut representatives = RoaringTreemap::new();
    while let Some(batch) = sorted.try_next().await? {
        let a = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("sorted id");
        let b = batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("sorted id");
        for (&a, &b) in a.values().iter().zip(b.values()) {
            if a != b && !removed.contains(a) && !removed.contains(b) {
                representatives.insert(a);
                removed.insert(b);
            }
        }
    }
    Ok((removed, representatives))
}

/// Select nonempty physical IVF scopes before dispatch, never by filtering the
/// resulting pairs. A count spans all segments; partition IDs are segment-local.
#[derive(Debug, Clone, Default)]
pub enum PartitionSelection {
    #[default]
    All,
    First {
        count: usize,
    },
    Random {
        count: usize,
        seed: u64,
    },
}

/// Query-only options. Snapshot binding is separate so multiple calls in one
/// statement can share the same immutable dataset.
#[derive(Debug, Clone)]
pub struct VectorQueryOptions {
    pub column: String,
    pub distance_threshold: f32,
    pub selection: PartitionSelection,
    pub output: DuplicatePairsOutput,
    pub dedup: bool,
}

impl BaseTableAdapter {
    /// Open a retained version, or refresh and capture the latest manifest once.
    /// No worker is allowed to independently resolve "latest".
    pub async fn vector_query_snapshot(&self, version: Option<u64>) -> Result<Arc<Dataset>> {
        let native = self.table.as_native().ok_or_else(|| {
            datafusion_common::DataFusionError::Plan(
                "vector queries require a native indexed table".into(),
            )
        })?;
        if version == Some(0) {
            return plan_err!("dataset_version must be positive");
        }
        let current = native
            .dataset
            .get()
            .await
            .map_err(|e| datafusion_common::DataFusionError::External(Box::new(e)))?;
        Ok(match version {
            Some(version) => Arc::new(current.checkout_version(version).await?),
            None => {
                // Do not change a shared table handle's pinned version.
                let mut latest = (*current).clone();
                latest.checkout_latest().await?;
                Arc::new(latest)
            }
        })
    }
}

/// A table provider bound to one source snapshot. The host retains the user's
/// unbound SQL for subsequent refreshes; this provider lives only for this run.
#[derive(Debug)]
pub struct VectorQueryTable {
    dataset: Arc<Dataset>,
    config: DuplicatePairsConfig,
    tasks: Vec<DuplicatePairTask>,
    options: VectorQueryOptions,
    schema: SchemaRef,
}

impl VectorQueryTable {
    pub async fn try_new(dataset: Arc<Dataset>, options: VectorQueryOptions) -> Result<Self> {
        if options.dedup
            && (!matches!(options.selection, PartitionSelection::All)
                || options.output.id_column.is_some())
        {
            return plan_err!(
                "dedupe requires all partitions and returns original rows; sampling and id_column are pair-query options"
            );
        }
        let config = DuplicatePairsConfig {
            dataset_version: dataset.version().version,
            column: options.column.clone(),
            distance_threshold: options.distance_threshold,
        };
        let mut tasks = plan_duplicate_pairs(dataset.clone(), &config).await?;
        // Canonical scope order is independent of metadata enumeration order.
        tasks.sort_by_key(|task| (task.segment_id, task.partition_id));
        let mut nonempty = Vec::new();
        for group in tasks.chunk_by(|a, b| a.segment_id == b.segment_id) {
            let index = dataset
                .open_vector_index(&config.column, &group[0].segment_id, &NoOpMetricsCollector)
                .await?;
            nonempty.extend(
                group
                    .iter()
                    .filter(|task| index.partition_size(task.partition_id) > 0)
                    .cloned(),
            );
        }
        match options.selection {
            PartitionSelection::All => {}
            PartitionSelection::First { count } | PartitionSelection::Random { count, .. }
                if count == 0 =>
            {
                return plan_err!("partition count must be positive");
            }
            PartitionSelection::First { count } => nonempty.truncate(count),
            PartitionSelection::Random { count, seed } => {
                // A single segment matches ivf_partition's SplitMix64 sampling.
                // With multiple segments the canonical scope ordinal extends the
                // same policy without conflating equal partition IDs.
                let segments = tasks
                    .iter()
                    .map(|t| t.segment_id)
                    .collect::<std::collections::BTreeSet<_>>()
                    .len();
                let mut ranked = nonempty
                    .into_iter()
                    .enumerate()
                    .map(|(ordinal, task)| {
                        let key = if segments == 1 {
                            task.partition_id as u64
                        } else {
                            ordinal as u64
                        };
                        (sample_rank(seed, key), task)
                    })
                    .collect::<Vec<_>>();
                ranked.sort_by_key(|(rank, task)| (*rank, task.segment_id, task.partition_id));
                ranked.truncate(count);
                nonempty = ranked.into_iter().map(|(_, task)| task).collect();
                nonempty.sort_by_key(|task| (task.segment_id, task.partition_id));
            }
        }
        let schema = if options.dedup {
            let mut scanner = dataset.scan();
            scanner.blob_handling(BlobHandling::AllBinary);
            scanner.schema().await?
        } else {
            options.output.schema(&dataset)?
        };
        Ok(Self {
            dataset,
            config,
            tasks: nonempty,
            options,
            schema,
        })
    }
}

fn sample_rank(seed: u64, ordinal: u64) -> u64 {
    let mut value = seed.wrapping_add(ordinal).wrapping_add(0x9E3779B97F4A7C15);
    value = (value ^ (value >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94D049BB133111EB);
    value ^ (value >> 31)
}

#[async_trait]
impl TableProvider for VectorQueryTable {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
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
        let output = if self.options.dedup {
            DuplicatePairsOutput::default()
        } else {
            self.options.output.clone()
        };
        let pairs: Arc<dyn ExecutionPlan> = if self.tasks.is_empty() {
            Arc::new(EmptyExec::new(output.schema(&self.dataset)?))
        } else {
            Arc::new(
                DuplicatePairsExec::try_new(
                    self.dataset.clone(),
                    self.config.clone(),
                    self.tasks.clone(),
                )?
                .with_output(output)?,
            )
        };
        let plan: Arc<dyn ExecutionPlan> = if self.options.dedup {
            Arc::new(VectorDedupExec::new(
                self.dataset.clone(),
                pairs,
                self.schema.clone(),
            ))
        } else {
            pairs
        };
        if let Some(indices) = projection {
            let expressions: Vec<_> = indices
                .iter()
                .map(|&i| {
                    (
                        Arc::new(datafusion_physical_expr::expressions::Column::new(
                            self.schema.field(i).name(),
                            i,
                        ))
                            as Arc<dyn datafusion_physical_expr::PhysicalExpr>,
                        self.schema.field(i).name().clone(),
                    )
                })
                .collect();
            Ok(Arc::new(ProjectionExec::try_new(expressions, plan)?))
        } else {
            Ok(plan)
        }
    }
}

/// Greedy direct representatives over the complete pair relation. Sorting spills
/// to disk; only vertex masks are retained. This has no durable checkpoint and
/// publishes nothing: failure is handled by the enclosing query/MV transaction.
#[derive(Debug)]
pub struct VectorDedupExec {
    dataset: Arc<Dataset>,
    pairs: Arc<dyn ExecutionPlan>,
    properties: Arc<PlanProperties>,
}

impl VectorDedupExec {
    fn new(dataset: Arc<Dataset>, pairs: Arc<dyn ExecutionPlan>, schema: SchemaRef) -> Self {
        Self {
            dataset,
            pairs,
            properties: Arc::new(PlanProperties::new(
                EquivalenceProperties::new(schema),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Final,
                Boundedness::Bounded,
            )),
        }
    }
}

impl DisplayAs for VectorDedupExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "VectorDedupExec: source_version={}, policy=direct_representative",
            self.dataset.version().version
        )
    }
}

impl ExecutionPlan for VectorDedupExec {
    fn name(&self) -> &str {
        "VectorDedupExec"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.pairs]
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return plan_err!("VectorDedupExec requires one pair input");
        }
        Ok(Arc::new(Self::new(
            self.dataset.clone(),
            children[0].clone(),
            self.schema(),
        )))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return plan_err!("invalid dedup output partition");
        }
        let dataset = self.dataset.clone();
        let pairs = CoalescePartitionsExec::new(self.pairs.clone()).execute(0, context)?;
        let stream = stream::once(async move {
            let (removed, _) = select_representatives(pairs, REPRESENTATIVE_SORT_MEMORY_BYTES)
                .await
                .map_err(|e| datafusion_common::DataFusionError::External(Box::new(e)))?;
            let mut scanner = dataset.scan();
            scanner
                .with_row_addr_prefilter(RowAddrMask::from_block(RowAddrTreeMap::from_iter(
                    removed,
                )))
                .blob_handling(BlobHandling::AllBinary)
                .batch_size(MATERIALIZE_BATCH_ROWS)
                .strict_batch_size(true);
            Ok::<_, datafusion_common::DataFusionError>(
                scanner
                    .try_into_stream()
                    .await?
                    .map_err(datafusion_common::DataFusionError::from),
            )
        })
        .try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }
}
