// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! MemTableDedupScanExec — newest-per-PK scan over the active memtable that
//! fuses within-source dedup with the scalar predicate in a single pass.
//!
//! The active memtable is an append log, so a PK update is a later append
//! with the same key. [`super::MemTableScanExec`] pushes the filter into the
//! scan, which removes a PK's newest row *before* dedup runs — an older row
//! that still satisfies the predicate then leaks through (a "phantom").
//!
//! This exec walks rows newest-first (batches reversed, rows iterated
//! back-to-front), seeds a seen-set from the *newest* occurrence of every PK
//! regardless of the predicate (so older versions stay suppressed even when
//! the newest row fails the filter), and records the keep/drop verdict into a
//! forward-aligned mask. A single `filter_record_batch` over the original
//! batch then emits the survivors with no per-column reverse copy.

use std::collections::HashSet;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use arrow_array::{Array, BooleanArray, RecordBatch, UInt64Array};
use arrow_schema::SchemaRef;
use datafusion::common::stats::Precision;
use datafusion::error::Result as DataFusionResult;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream, Statistics,
};
use datafusion::prelude::Expr;
use datafusion_physical_expr::{EquivalenceProperties, PhysicalExprRef};
use futures::stream::{self, StreamExt};

use crate::dataset::mem_wal::scanner::exec::compute_pk_hash;
use crate::dataset::mem_wal::write::BatchStore;

/// Scans the active memtable newest-first and emits the newest-per-PK rows
/// that satisfy the (optional) predicate. See the module doc.
pub struct MemTableDedupScanExec {
    batch_store: Arc<BatchStore>,
    visible_count: usize,
    /// Column indices to project (into the source schema).
    projection: Option<Vec<usize>>,
    output_schema: SchemaRef,
    /// Column indices of the primary key in the source schema.
    pk_indices: Vec<usize>,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
    with_row_id: bool,
    with_row_address: bool,
    filter_predicate: Option<PhysicalExprRef>,
    /// Original filter expression, for display only.
    filter_expr: Option<Expr>,
}

impl Debug for MemTableDedupScanExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemTableDedupScanExec")
            .field("visible_count", &self.visible_count)
            .field("projection", &self.projection)
            .field("pk_indices", &self.pk_indices)
            .field("with_row_address", &self.with_row_address)
            .field("has_filter", &self.filter_predicate.is_some())
            .finish()
    }
}

impl MemTableDedupScanExec {
    /// Create a new fused dedup + predicate scan over the active memtable.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        batch_store: Arc<BatchStore>,
        visible_count: usize,
        projection: Option<Vec<usize>>,
        output_schema: SchemaRef,
        pk_indices: Vec<usize>,
        with_row_id: bool,
        with_row_address: bool,
        filter_predicate: Option<PhysicalExprRef>,
        filter_expr: Option<Expr>,
    ) -> Self {
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(output_schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));

        Self {
            batch_store,
            visible_count,
            projection,
            output_schema,
            pk_indices,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
            with_row_id,
            with_row_address,
            filter_predicate,
            filter_expr,
        }
    }
}

impl DisplayAs for MemTableDedupScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter<'_>) -> std::fmt::Result {
        let projection_names: Vec<&str> = self
            .output_schema
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect();
        let filter_str = self
            .filter_expr
            .as_ref()
            .map(|e| format!(", filter={}", e))
            .unwrap_or_default();
        let row_addr_str = if self.with_row_address {
            ", with_row_address=true"
        } else {
            ""
        };
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => write!(
                f,
                "MemTableDedupScanExec: projection=[{}], with_row_id={}{}{}",
                projection_names.join(", "),
                self.with_row_id,
                row_addr_str,
                filter_str
            ),
            DisplayFormatType::TreeRender => write!(
                f,
                "MemTableDedupScanExec\nprojection=[{}]\nwith_row_id={}{}{}",
                projection_names.join(", "),
                self.with_row_id,
                row_addr_str,
                filter_str
            ),
        }
    }
}

impl ExecutionPlan for MemTableDedupScanExec {
    fn name(&self) -> &str {
        "MemTableDedupScanExec"
    }

    fn schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return Err(datafusion::error::DataFusionError::Internal(
                "MemTableDedupScanExec does not have children".to_string(),
            ));
        }
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        // Newest-first iteration: reverse batches here, rows are walked
        // back-to-front below.
        let mut batches = self
            .batch_store
            .visible_batches_with_offsets(self.visible_count);
        batches.reverse();

        let projection = self.projection.clone();
        let schema = self.output_schema.clone();
        let with_row_id = self.with_row_id;
        let with_row_address = self.with_row_address;
        let filter_predicate = self.filter_predicate.clone();
        let pk_indices = self.pk_indices.clone();
        let need_row_offsets = with_row_id || with_row_address;

        // Cross-batch seen-set: first time a PK hash is seen (newest-first) wins.
        let mut seen: HashSet<u64> = HashSet::new();
        let mut out: Vec<DataFusionResult<RecordBatch>> = Vec::with_capacity(batches.len());

        for (batch, row_offset) in batches {
            let n = batch.num_rows();
            if n == 0 {
                continue;
            }

            // Predicate mask over the original (forward) rows; null counts as
            // no-match.
            let filter_array = match &filter_predicate {
                Some(predicate) => {
                    let value = predicate.evaluate(&batch)?;
                    let array = value.into_array(n)?;
                    let Some(boolean) = array.as_any().downcast_ref::<BooleanArray>() else {
                        return Err(datafusion::error::DataFusionError::Internal(
                            "Filter predicate did not evaluate to boolean".to_string(),
                        ));
                    };
                    Some(boolean.clone())
                }
                None => None,
            };

            // Walk newest-first; first insertion into `seen` is the newest
            // occurrence (keep), later ones are older (drop). `seen` is
            // updated even when the newest row fails the predicate so its
            // older versions stay suppressed (no phantom).
            let mut emit_forward = vec![false; n];
            for j in (0..n).rev() {
                let pk_hash = compute_pk_hash(&batch, &pk_indices, j);
                let is_newest = seen.insert(pk_hash);
                let passes = match &filter_array {
                    Some(mask) => mask.is_valid(j) && mask.value(j),
                    None => true,
                };
                emit_forward[j] = is_newest && passes;
            }
            let emit_mask = BooleanArray::from(emit_forward);

            let emitted = arrow_select::filter::filter_record_batch(&batch, &emit_mask)?;
            if emitted.num_rows() == 0 {
                continue;
            }

            let filtered_offsets: Vec<u64> = if need_row_offsets {
                (0..n)
                    .filter(|&j| emit_mask.value(j))
                    .map(|j| row_offset + j as u64)
                    .collect()
            } else {
                vec![]
            };

            let mut columns: Vec<Arc<dyn Array>> = if let Some(ref indices) = projection {
                indices.iter().map(|&i| emitted.column(i).clone()).collect()
            } else {
                emitted.columns().to_vec()
            };
            if with_row_id {
                columns.push(Arc::new(UInt64Array::from(filtered_offsets.clone())));
            }
            if with_row_address {
                columns.push(Arc::new(UInt64Array::from(filtered_offsets)));
            }

            out.push(
                RecordBatch::try_new(schema.clone(), columns)
                    .map_err(datafusion::error::DataFusionError::from),
            );
        }

        let stream = stream::iter(out).boxed();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.output_schema.clone(),
            stream,
        )))
    }

    fn partition_statistics(&self, _partition: Option<usize>) -> DataFusionResult<Arc<Statistics>> {
        Ok(Arc::new(Statistics {
            num_rows: Precision::Absent,
            total_byte_size: Precision::Absent,
            column_statistics: vec![],
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
    use super::*;
    use arrow_array::Int32Array;
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::prelude::col;
    use futures::TryStreamExt;
    use lance_datafusion::planner::Planner;
    use std::collections::HashMap;

    fn source_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Int32, true),
        ]))
    }

    fn output_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Int32, true),
            Field::new(
                crate::dataset::mem_wal::memtable::scanner::exec::ROW_ADDRESS_COLUMN,
                DataType::UInt64,
                true,
            ),
        ]))
    }

    /// id PK + nullable value. Each tuple is one appended row, in order.
    fn batch(rows: &[(i32, Option<i32>)]) -> RecordBatch {
        let ids: Vec<i32> = rows.iter().map(|(id, _)| *id).collect();
        let values: Vec<Option<i32>> = rows.iter().map(|(_, v)| *v).collect();
        RecordBatch::try_new(
            source_schema(),
            vec![
                Arc::new(Int32Array::from(ids)),
                Arc::new(Int32Array::from(values)),
            ],
        )
        .unwrap()
    }

    /// Run the exec and collect (id -> (value, rowaddr)).
    async fn run(
        store: Arc<BatchStore>,
        visible_count: usize,
        filter: Option<Expr>,
    ) -> HashMap<i32, (Option<i32>, u64)> {
        let filter_predicate = filter.map(|expr| {
            let planner = Planner::new(source_schema());
            let optimized = planner.optimize_expr(expr).unwrap();
            planner.create_physical_expr(&optimized).unwrap()
        });
        let filter_expr = None;
        let exec = MemTableDedupScanExec::new(
            store,
            visible_count,
            None,
            output_schema(),
            vec![0],
            false,
            true,
            filter_predicate,
            filter_expr,
        );
        let ctx = Arc::new(TaskContext::default());
        let batches: Vec<RecordBatch> = exec.execute(0, ctx).unwrap().try_collect().await.unwrap();

        let mut out = HashMap::new();
        for b in &batches {
            let ids = b.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
            let values = b.column(1).as_any().downcast_ref::<Int32Array>().unwrap();
            let addrs = b.column(2).as_any().downcast_ref::<UInt64Array>().unwrap();
            for i in 0..b.num_rows() {
                let value = (!values.is_null(i)).then(|| values.value(i));
                let prev = out.insert(ids.value(i), (value, addrs.value(i)));
                assert!(prev.is_none(), "duplicate PK {} in output", ids.value(i));
            }
        }
        out
    }

    /// Within a single batch: insert + update of one PK collapses to newest,
    /// and a predicate that the newest version fails must NOT resurrect the old.
    #[tokio::test]
    async fn within_batch_phantom_suppressed() {
        let store = Arc::new(BatchStore::with_capacity(16));
        // id=10 inserted (100) then updated to NULL, all in one batch.
        store.append(batch(&[(10, Some(100)), (10, None)])).unwrap();

        let no_filter = run(store.clone(), 1, None).await;
        assert_eq!(no_filter.len(), 1);
        assert_eq!(no_filter[&10].0, None, "newest version of id=10 is NULL");

        let not_null = run(store, 1, Some(col("value").is_not_null())).await;
        assert!(
            !not_null.contains_key(&10),
            "id=10 newest is NULL; the stale value=100 must not leak under value IS NOT NULL"
        );
    }

    /// Cross-batch dedup + predicate, mirroring the design doc worked example.
    #[tokio::test]
    async fn cross_batch_newest_per_pk_with_filter() {
        let store = Arc::new(BatchStore::with_capacity(16));
        store
            .append(batch(&[(10, Some(100)), (20, Some(200)), (10, None)]))
            .unwrap();
        store.append(batch(&[(20, Some(999)), (30, None)])).unwrap();

        // No filter: newest per PK = {10:NULL@2, 20:999@3, 30:NULL@4}.
        let all = run(store.clone(), 2, None).await;
        assert_eq!(all.len(), 3);
        assert_eq!(all[&10], (None, 2));
        assert_eq!(all[&20], (Some(999), 3));
        assert_eq!(all[&30], (None, 4));

        // value IS NOT NULL: only id=20 (newest 999) survives; 10 and 30 are
        // newest-NULL so they must be absent (no stale leak).
        let not_null = run(store, 2, Some(col("value").is_not_null())).await;
        assert_eq!(not_null.len(), 1);
        assert_eq!(not_null[&20], (Some(999), 3));
    }

    /// value IS NULL is the mirror case: a PK whose newest version is non-NULL
    /// must not leak an older NULL version.
    #[tokio::test]
    async fn is_null_predicate_no_stale_leak() {
        let store = Arc::new(BatchStore::with_capacity(16));
        // id=40 inserted NULL then updated to 400 (newest non-NULL).
        store.append(batch(&[(40, None), (40, Some(400))])).unwrap();

        let is_null = run(store, 1, Some(col("value").is_null())).await;
        assert!(
            !is_null.contains_key(&40),
            "id=40 newest is 400; the stale NULL must not leak under value IS NULL"
        );
    }
}
