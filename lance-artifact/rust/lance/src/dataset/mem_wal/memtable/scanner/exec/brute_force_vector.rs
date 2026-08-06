// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! MemTableBruteForceVectorExec — KNN over the active memtable without an HNSW.
//!
//! Mirrors [`super::VectorIndexExec`]'s output contract (same schema, same row
//! shape, same `_distance` / `_rowid` semantics) so the LSM caller can swap one
//! for the other based on whether the memtable's `IndexStore` has an HNSW for
//! the queried column. The active memtable is the LSM's unindexed-rows path:
//! whenever the HNSW config is absent (cold-start before the Indexer commits,
//! or new rows in the window between commit and next memtable rotation), this
//! exec keeps KNN correct by computing exact distances row-by-row.

use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use arrow_array::{Array, BooleanArray, Float32Array, RecordBatch, UInt64Array, cast::AsArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
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
use datafusion_physical_expr::{EquivalenceProperties, PhysicalExprRef};
use futures::stream::{self, StreamExt};
use lance_core::{Error, Result};
use lance_linalg::distance::DistanceType;

use super::super::builder::VectorQuery;
use super::newest_pk_positions;
use super::vector::DISTANCE_COLUMN;
use crate::dataset::mem_wal::write::BatchStore;

/// Distance metric used when [`VectorQuery::distance_type`] is `None`. The
/// indexed path defers to the index's own metric, but with no index there is
/// no inherent default — L2 matches what most callers configure and what the
/// SSTable/base arms use when re-ranking unindexed candidates.
const DEFAULT_DISTANCE_TYPE: DistanceType = DistanceType::L2;

/// Brute-force KNN over an active memtable without an HNSW. Produces the same
/// output schema as [`super::VectorIndexExec`].
pub struct MemTableBruteForceVectorExec {
    batch_store: Arc<BatchStore>,
    query: VectorQuery,
    visible_count: usize,
    projection: Option<Vec<usize>>,
    output_schema: SchemaRef,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
    with_row_id: bool,
    /// Optional prefilter predicate, compiled against the memtable schema.
    /// Applied per row before the top-k cut so the KNN only ranks matching
    /// rows (true prefilter, not a lossy post-filter on the top-k).
    filter: Option<PhysicalExprRef>,
    /// Primary-key columns. When set, only the newest version of each PK is
    /// eligible for top-k. With a filter, this evaluates the predicate against
    /// the current PK version instead of falling back to a stale older version.
    pk_columns: Option<Vec<String>>,
}

impl Debug for MemTableBruteForceVectorExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemTableBruteForceVectorExec")
            .field("column", &self.query.column)
            .field("k", &self.query.k)
            .field("visible_count", &self.visible_count)
            .field("with_row_id", &self.with_row_id)
            .finish()
    }
}

impl MemTableBruteForceVectorExec {
    /// Build the exec. `base_schema` is the post-projection row schema (no
    /// `_distance`, no `_rowid`); `_distance` is appended unconditionally and
    /// `_rowid` only when `with_row_id` is set, matching [`VectorIndexExec`].
    pub fn new(
        batch_store: Arc<BatchStore>,
        query: VectorQuery,
        visible_count: usize,
        projection: Option<Vec<usize>>,
        base_schema: SchemaRef,
        with_row_id: bool,
    ) -> Result<Self> {
        let mut fields: Vec<Field> = base_schema
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect();
        fields.push(Field::new(DISTANCE_COLUMN, DataType::Float32, true));
        if with_row_id {
            fields.push(Field::new(lance_core::ROW_ID, DataType::UInt64, true));
        }
        let output_schema = Arc::new(Schema::new(fields));

        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(output_schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));

        Ok(Self {
            batch_store,
            query,
            visible_count,
            projection,
            output_schema,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
            with_row_id,
            filter: None,
            pk_columns: None,
        })
    }

    /// Attach an optional prefilter predicate (compiled against the memtable
    /// schema). Rows that fail the predicate are excluded before the top-k cut.
    pub fn with_filter(mut self, filter: Option<PhysicalExprRef>) -> Self {
        self.filter = filter;
        self
    }

    /// Provide the primary-key columns so search keeps only the newest version
    /// of each PK (see `pk_columns`).
    pub fn with_pk_columns(mut self, pk_columns: Option<Vec<String>>) -> Self {
        self.pk_columns = pk_columns.filter(|columns| !columns.is_empty());
        self
    }

    /// Evaluate the prefilter predicate against a memtable batch, returning a
    /// keep-mask (`true` = retain). `Ok(None)` when no filter is configured.
    fn filter_mask(&self, batch: &RecordBatch) -> Result<Option<BooleanArray>> {
        let Some(ref predicate) = self.filter else {
            return Ok(None);
        };
        let values = predicate
            .evaluate(batch)
            .and_then(|v| v.into_array(batch.num_rows()))
            .map_err(|e| {
                Error::invalid_input(format!("vector prefilter evaluation failed: {}", e))
            })?;
        let mask = values
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| {
                Error::invalid_input(
                    "vector prefilter predicate did not evaluate to boolean".to_string(),
                )
            })?
            .clone();
        Ok(Some(mask))
    }

    /// Last row position visible under `visible_count`, or `None`
    /// if no batches are visible. Identical to `VectorIndexExec`'s helper so
    /// both arms cut at the same MVCC boundary.
    fn compute_max_visible_row(&self) -> Option<u64> {
        let mut max_visible_row_exclusive: u64 = 0;
        let mut current_row: u64 = 0;

        for (batch_position, stored_batch) in self.batch_store.iter().enumerate() {
            let batch_end = current_row + stored_batch.num_rows as u64;
            if batch_position < self.visible_count {
                max_visible_row_exclusive = batch_end;
            }
            current_row = batch_end;
        }

        if max_visible_row_exclusive > 0 {
            Some(max_visible_row_exclusive - 1)
        } else {
            None
        }
    }

    /// Extract the flat per-element query vector. `arrow_batch_func` wants
    /// `(from: &dyn Array, to: &FixedSizeListArray)` where `from` is the raw
    /// primitive array of one vector (NOT an FSL), so unwrap an FSL with one
    /// row if that's how the caller built it.
    fn query_as_flat(&self) -> Result<Arc<dyn Array>> {
        let query_array = self.query.query_vector.as_ref();
        if let Some(fsl) = query_array.as_fixed_size_list_opt() {
            if fsl.len() != 1 {
                return Err(Error::invalid_input(format!(
                    "brute-force vector search expects a single query vector, got {}",
                    fsl.len()
                )));
            }
            return Ok(fsl.value(0));
        }
        Ok(self.query.query_vector.clone())
    }

    /// Compute `(distance, row_position)` for every visible row, then top-k by
    /// distance ascending. Rows where the vector column is null or where the
    /// computed distance is non-finite are skipped — same convention as the
    /// HNSW search (which filters on `result.distance.is_finite()`).
    fn compute_topk(&self) -> Result<Vec<(f32, u64)>> {
        if self.query.k == 0 {
            return Ok(Vec::new());
        }
        let Some(max_visible_row) = self.compute_max_visible_row() else {
            return Ok(Vec::new());
        };
        let query_flat = self.query_as_flat()?;
        let column_name = self.query.column.as_str();
        let distance_type = self.query.distance_type.unwrap_or(DEFAULT_DISTANCE_TYPE);
        let batch_func = distance_type.arrow_batch_func();

        // When PK columns are configured, only the newest version of each PK is
        // eligible. This keeps top-k slots from being consumed by superseded
        // rows and makes filtered search evaluate the predicate against the
        // current version of the PK.
        let newest_positions = if let Some(pk_columns) = &self.pk_columns {
            Some(
                newest_pk_positions(
                    &self.batch_store,
                    pk_columns,
                    self.visible_count,
                    max_visible_row,
                )
                .map_err(|e| Error::invalid_input(e.to_string()))?,
            )
        } else {
            None
        };

        // Walk batches in append order. `current_row` is the global row offset
        // of the *next* row about to be visited; rows past `max_visible_row`
        // are dropped before they reach the heap.
        let mut current_row: u64 = 0;
        let mut candidates: Vec<(f32, u64)> = Vec::new();

        for (batch_position, stored_batch) in self.batch_store.iter().enumerate() {
            let n = stored_batch.num_rows;
            if n == 0 {
                continue;
            }
            if batch_position >= self.visible_count {
                current_row += n as u64;
                continue;
            }

            let column = stored_batch
                .data
                .column_by_name(column_name)
                .ok_or_else(|| {
                    Error::invalid_input(format!(
                        "Vector column '{}' not found in memtable schema",
                        column_name
                    ))
                })?;
            let column_fsl = column.as_fixed_size_list_opt().ok_or_else(|| {
                Error::invalid_input(format!(
                    "Vector column '{}' must be FixedSizeList; got {:?}",
                    column_name,
                    column.data_type()
                ))
            })?;

            let distances = batch_func(query_flat.as_ref(), column_fsl).map_err(|e| {
                Error::invalid_input(format!(
                    "brute-force distance computation failed for column '{}': {}",
                    column_name, e
                ))
            })?;

            // Prefilter: drop rows that fail the predicate before they reach the
            // top-k heap (a NULL predicate result excludes the row, matching SQL).
            let filter_mask = self.filter_mask(&stored_batch.data)?;

            for row in 0..n {
                let pos = current_row + row as u64;
                if pos > max_visible_row {
                    break;
                }
                // Skip superseded versions: only the newest version of each PK is
                // eligible, so a newer non-matching version excludes the PK.
                if let Some(ref newest) = newest_positions
                    && !newest.contains(&pos)
                {
                    continue;
                }
                if let Some(ref mask) = filter_mask
                    && (!mask.is_valid(row) || !mask.value(row))
                {
                    continue;
                }
                if distances.is_null(row) {
                    continue;
                }
                let dist = distances.value(row);
                if !dist.is_finite() {
                    continue;
                }
                candidates.push((dist, pos));
            }

            current_row += n as u64;
        }

        // `partial_cmp` defaults Equal on NaN; we filtered non-finite above so
        // every remaining value compares deterministically.
        candidates.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));

        if self.query.distance_lower_bound.is_some() || self.query.distance_upper_bound.is_some() {
            candidates.retain(|&(dist, _)| {
                let above_lower = self.query.distance_lower_bound.is_none_or(|lb| dist >= lb);
                let below_upper = self.query.distance_upper_bound.is_none_or(|ub| dist < ub);
                above_lower && below_upper
            });
        }

        candidates.truncate(self.query.k);
        Ok(candidates)
    }

    /// Materialize the top-k rows from the batch store, mirroring
    /// `VectorIndexExec::materialize_rows`. Groups by batch so the per-batch
    /// `take` is amortized; emits one output batch per source batch that
    /// contributes.
    fn materialize_rows(&self, results: &[(f32, u64)]) -> DataFusionResult<Vec<RecordBatch>> {
        if results.is_empty() {
            return Ok(vec![]);
        }

        let mut batch_ranges = Vec::new();
        let mut current_row = 0usize;
        for stored_batch in self.batch_store.iter() {
            let start = current_row;
            let end = current_row + stored_batch.num_rows;
            batch_ranges.push((start, end));
            current_row = end;
        }

        let mut batches_data: std::collections::HashMap<usize, Vec<(usize, f32, u64)>> =
            std::collections::HashMap::new();
        for &(distance, pos) in results {
            let pos_usize = pos as usize;
            for (batch_id, &(start, end)) in batch_ranges.iter().enumerate() {
                if pos_usize >= start && pos_usize < end {
                    batches_data.entry(batch_id).or_default().push((
                        pos_usize - start,
                        distance,
                        pos,
                    ));
                    break;
                }
            }
        }

        let mut all_batches = Vec::new();
        for (batch_id, rows_with_dist) in batches_data {
            if let Some(stored) = self.batch_store.get(batch_id) {
                let rows: Vec<u32> = rows_with_dist.iter().map(|&(r, _, _)| r as u32).collect();
                let distances: Vec<f32> = rows_with_dist.iter().map(|&(_, d, _)| d).collect();
                let row_positions: Vec<u64> =
                    rows_with_dist.iter().map(|&(_, _, pos)| pos).collect();

                let indices = arrow_array::UInt32Array::from(rows);

                let mut columns: Vec<Arc<dyn arrow_array::Array>> = stored
                    .data
                    .columns()
                    .iter()
                    .map(|col| arrow_select::take::take(col.as_ref(), &indices, None).unwrap())
                    .collect();

                columns.push(Arc::new(Float32Array::from(distances)));

                let mut final_columns = if let Some(ref proj_indices) = self.projection {
                    let mut projected: Vec<_> =
                        proj_indices.iter().map(|&i| columns[i].clone()).collect();
                    // Distance was just pushed onto `columns`; keep it last.
                    projected.push(columns.last().unwrap().clone());
                    projected
                } else {
                    columns
                };

                if self.with_row_id {
                    final_columns.push(Arc::new(UInt64Array::from(row_positions)));
                }

                let batch = RecordBatch::try_new(self.output_schema.clone(), final_columns)?;
                all_batches.push(batch);
            }
        }

        Ok(all_batches)
    }
}

impl DisplayAs for MemTableBruteForceVectorExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "MemTableBruteForceVectorExec: column={}, k={}, with_row_id={}",
                    self.query.column, self.query.k, self.with_row_id
                )
            }
            DisplayFormatType::TreeRender => {
                write!(
                    f,
                    "MemTableBruteForceVectorExec\ncolumn={}\nk={}\nwith_row_id={}",
                    self.query.column, self.query.k, self.with_row_id
                )
            }
        }
    }
}

impl ExecutionPlan for MemTableBruteForceVectorExec {
    fn name(&self) -> &str {
        "MemTableBruteForceVectorExec"
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
                "MemTableBruteForceVectorExec does not have children".to_string(),
            ));
        }
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let results = self
            .compute_topk()
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
        let batches = self.materialize_rows(&results)?;
        let stream = stream::iter(batches.into_iter().map(Ok)).boxed();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.output_schema.clone(),
            stream,
        )))
    }

    fn partition_statistics(&self, _partition: Option<usize>) -> DataFusionResult<Arc<Statistics>> {
        Ok(Arc::new(Statistics {
            num_rows: Precision::Exact(self.query.k),
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
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{BooleanArray, FixedSizeListArray, Float32Array, Int32Array};
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::physical_plan::common::collect;
    use datafusion::prelude::{Expr, SessionContext, col, lit};
    use lance_datafusion::planner::Planner;

    fn make_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                "vector",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 2),
                true,
            ),
        ]))
    }

    fn make_schema_with_active() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                "vector",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 2),
                true,
            ),
            Field::new("active", DataType::Boolean, true),
        ]))
    }

    fn make_batch(schema: SchemaRef, ids: &[i32], vectors: &[[f32; 2]]) -> RecordBatch {
        let id_array = Arc::new(Int32Array::from(ids.to_vec())) as Arc<dyn Array>;
        let values: Vec<f32> = vectors.iter().flat_map(|v| v.iter().copied()).collect();
        let inner = Arc::new(Float32Array::from(values));
        let field = Arc::new(Field::new("item", DataType::Float32, true));
        let vec_array =
            Arc::new(FixedSizeListArray::try_new(field, 2, inner, None).expect("build fsl"))
                as Arc<dyn Array>;
        RecordBatch::try_new(schema, vec![id_array, vec_array]).expect("build batch")
    }

    fn make_batch_with_active(
        schema: SchemaRef,
        ids: &[i32],
        vectors: &[[f32; 2]],
        active: &[Option<bool>],
    ) -> RecordBatch {
        let id_array = Arc::new(Int32Array::from(ids.to_vec())) as Arc<dyn Array>;
        let values: Vec<f32> = vectors.iter().flat_map(|v| v.iter().copied()).collect();
        let inner = Arc::new(Float32Array::from(values));
        let field = Arc::new(Field::new("item", DataType::Float32, true));
        let vec_array =
            Arc::new(FixedSizeListArray::try_new(field, 2, inner, None).expect("build fsl"))
                as Arc<dyn Array>;
        let active_array = Arc::new(BooleanArray::from(active.to_vec())) as Arc<dyn Array>;
        RecordBatch::try_new(schema, vec![id_array, vec_array, active_array]).expect("build batch")
    }

    fn store_with_batches(batches: Vec<RecordBatch>) -> Arc<BatchStore> {
        let store = Arc::new(BatchStore::with_capacity(batches.len().max(1)));
        for batch in batches {
            store.append(batch).expect("append batch");
        }
        store
    }

    fn query_for(vector: [f32; 2], k: usize) -> VectorQuery {
        let values = Arc::new(Float32Array::from(vector.to_vec())) as Arc<dyn Array>;
        VectorQuery {
            column: "vector".to_string(),
            query_vector: values,
            k,
            nprobes: 1,
            maximum_nprobes: None,
            distance_type: Some(DistanceType::L2),
            ef: None,
            refine_factor: None,
            distance_lower_bound: None,
            distance_upper_bound: None,
        }
    }

    fn physical_filter(schema: SchemaRef, expr: Expr) -> PhysicalExprRef {
        let planner = Planner::new(schema);
        let optimized = planner.optimize_expr(expr).expect("optimize filter");
        planner
            .create_physical_expr(&optimized)
            .expect("create physical filter")
    }

    async fn execute_to_batches(exec: Arc<dyn ExecutionPlan>) -> Vec<RecordBatch> {
        let ctx = SessionContext::new();
        let stream = exec.execute(0, ctx.task_ctx()).expect("execute");
        collect(stream).await.expect("collect")
    }

    fn ids_from_batches(batches: &[RecordBatch]) -> Vec<i32> {
        let mut ids = Vec::new();
        for batch in batches {
            let id_arr = batch
                .column_by_name("id")
                .unwrap()
                .as_primitive::<arrow_array::types::Int32Type>();
            for row in 0..batch.num_rows() {
                ids.push(id_arr.value(row));
            }
        }
        ids
    }

    #[tokio::test]
    async fn top_k_by_distance() {
        // Five rows; query at (0,0); L2 distances are id² each — expect ids in
        // ascending order, capped at k=3.
        let schema = make_schema();
        let batch = make_batch(
            schema.clone(),
            &[0, 1, 2, 3, 4],
            &[[0.0, 0.0], [1.0, 0.0], [2.0, 0.0], [3.0, 0.0], [4.0, 0.0]],
        );
        let store = store_with_batches(vec![batch]);
        let query = query_for([0.0, 0.0], 3);
        let exec = Arc::new(
            MemTableBruteForceVectorExec::new(
                store,
                query,
                /* visible_count = */ usize::MAX,
                None,
                schema,
                false,
            )
            .expect("ctor"),
        );
        let out = execute_to_batches(exec).await;
        // Concat and check ids + distances in order.
        let total: usize = out.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 3, "k=3 cap not honored: got {total} rows");

        let mut id_dist: Vec<(i32, f32)> = Vec::new();
        for batch in &out {
            let ids = batch
                .column_by_name("id")
                .unwrap()
                .as_primitive::<arrow_array::types::Int32Type>();
            let dists = batch
                .column_by_name(DISTANCE_COLUMN)
                .unwrap()
                .as_primitive::<arrow_array::types::Float32Type>();
            for i in 0..batch.num_rows() {
                id_dist.push((ids.value(i), dists.value(i)));
            }
        }
        id_dist.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        assert_eq!(id_dist[0].0, 0);
        assert_eq!(id_dist[1].0, 1);
        assert_eq!(id_dist[2].0, 2);
    }

    #[tokio::test]
    async fn empty_memtable_returns_empty_with_distance_schema() {
        let schema = make_schema();
        let store = Arc::new(BatchStore::with_capacity(4));
        let query = query_for([0.5, 0.5], 10);
        let exec = Arc::new(
            MemTableBruteForceVectorExec::new(store, query, usize::MAX, None, schema, false)
                .expect("ctor"),
        );
        let out_schema = exec.schema();
        assert!(
            out_schema.field_with_name(DISTANCE_COLUMN).is_ok(),
            "output schema must contain `{DISTANCE_COLUMN}` even with empty memtable; got {:?}",
            out_schema
        );
        let out = execute_to_batches(exec).await;
        let total: usize = out.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 0);
    }

    #[tokio::test]
    async fn respects_indexed_count() {
        // Two batches of two rows. Freeze at batch 0 — only ids 0,1 are
        // visible candidates; the (closer) ids 2,3 in batch 1 are excluded.
        let schema = make_schema();
        let b0 = make_batch(schema.clone(), &[0, 1], &[[5.0, 0.0], [6.0, 0.0]]);
        let b1 = make_batch(schema.clone(), &[2, 3], &[[1.0, 0.0], [2.0, 0.0]]);
        let store = store_with_batches(vec![b0, b1]);
        let query = query_for([0.0, 0.0], 4);
        let exec = Arc::new(
            MemTableBruteForceVectorExec::new(
                store, query, /* visible_count = */ 1, None, schema, false,
            )
            .expect("ctor"),
        );
        let out = execute_to_batches(exec).await;
        let mut returned_ids: Vec<i32> = Vec::new();
        for batch in &out {
            let ids = batch
                .column_by_name("id")
                .unwrap()
                .as_primitive::<arrow_array::types::Int32Type>();
            for i in 0..batch.num_rows() {
                returned_ids.push(ids.value(i));
            }
        }
        returned_ids.sort();
        assert_eq!(returned_ids, vec![0, 1]);
    }

    #[tokio::test]
    async fn applies_distance_bounds() {
        let schema = make_schema();
        let batch = make_batch(
            schema.clone(),
            &[0, 1, 2, 3],
            &[[0.0, 0.0], [1.0, 0.0], [2.0, 0.0], [3.0, 0.0]],
        );
        let store = store_with_batches(vec![batch]);
        let mut query = query_for([0.0, 0.0], 10);
        // L2² distances: 0, 1, 4, 9. Keep only distances in [1, 5) — ids 1, 2.
        query.distance_lower_bound = Some(1.0);
        query.distance_upper_bound = Some(5.0);
        let exec = Arc::new(
            MemTableBruteForceVectorExec::new(store, query, usize::MAX, None, schema, false)
                .expect("ctor"),
        );
        let out = execute_to_batches(exec).await;
        let mut ids: Vec<i32> = Vec::new();
        for batch in &out {
            let id_arr = batch
                .column_by_name("id")
                .unwrap()
                .as_primitive::<arrow_array::types::Int32Type>();
            for i in 0..batch.num_rows() {
                ids.push(id_arr.value(i));
            }
        }
        ids.sort();
        assert_eq!(ids, vec![1, 2]);
    }

    #[tokio::test]
    async fn populates_row_id_when_requested() {
        let schema = make_schema();
        let batch = make_batch(
            schema.clone(),
            &[10, 11, 12],
            &[[3.0, 0.0], [1.0, 0.0], [2.0, 0.0]],
        );
        let store = store_with_batches(vec![batch]);
        let query = query_for([0.0, 0.0], 3);
        let exec = Arc::new(
            MemTableBruteForceVectorExec::new(
                store,
                query,
                usize::MAX,
                None,
                schema,
                /* with_row_id = */ true,
            )
            .expect("ctor"),
        );
        let out_schema = exec.schema();
        assert!(out_schema.field_with_name(lance_core::ROW_ID).is_ok());

        let out = execute_to_batches(exec).await;
        let mut pairs: Vec<(i32, u64)> = Vec::new();
        for batch in &out {
            let ids = batch
                .column_by_name("id")
                .unwrap()
                .as_primitive::<arrow_array::types::Int32Type>();
            let rowids = batch
                .column_by_name(lance_core::ROW_ID)
                .unwrap()
                .as_primitive::<arrow_array::types::UInt64Type>();
            for i in 0..batch.num_rows() {
                pairs.push((ids.value(i), rowids.value(i)));
            }
        }
        // Row offsets are insert-order: id=10 → 0, id=11 → 1, id=12 → 2.
        pairs.sort_by_key(|(id, _)| *id);
        assert_eq!(pairs, vec![(10, 0), (11, 1), (12, 2)]);
    }

    #[tokio::test]
    async fn prefilter_null_predicate_excludes_rows() {
        let schema = make_schema_with_active();
        let batch = make_batch_with_active(
            schema.clone(),
            &[1, 2, 3],
            &[[0.0, 0.0], [3.0, 0.0], [1.0, 0.0]],
            &[None, Some(true), Some(false)],
        );
        let store = store_with_batches(vec![batch]);
        let query = query_for([0.0, 0.0], 3);
        let filter = physical_filter(schema.clone(), col("active").eq(lit(true)));
        let exec = Arc::new(
            MemTableBruteForceVectorExec::new(store, query, usize::MAX, None, schema, false)
                .expect("ctor")
                .with_filter(Some(filter)),
        );

        let out = execute_to_batches(exec).await;
        assert_eq!(
            ids_from_batches(&out),
            vec![2],
            "NULL predicate results must be excluded from vector prefilter candidates"
        );
    }

    #[tokio::test]
    async fn prefilter_with_pk_columns_drops_stale_matching_version() {
        let schema = make_schema_with_active();
        let batch = make_batch_with_active(
            schema.clone(),
            &[5, 5],
            &[[0.0, 0.0], [10.0, 0.0]],
            &[Some(true), Some(false)],
        );
        let store = store_with_batches(vec![batch]);
        let query = query_for([0.0, 0.0], 10);
        let filter = physical_filter(schema.clone(), col("active").eq(lit(true)));
        let exec = Arc::new(
            MemTableBruteForceVectorExec::new(store, query, usize::MAX, None, schema, false)
                .expect("ctor")
                .with_filter(Some(filter))
                .with_pk_columns(Some(vec!["id".to_string()])),
        );

        let out = execute_to_batches(exec).await;
        assert!(
            out.iter().all(|batch| batch.num_rows() == 0),
            "the older matching vector version must not leak when the newest PK fails the filter"
        );
    }

    #[tokio::test]
    async fn pk_columns_keep_newest_version_without_filter() {
        let schema = make_schema();
        let batch = make_batch(schema.clone(), &[5, 5], &[[0.0, 0.0], [10.0, 0.0]]);
        let store = store_with_batches(vec![batch]);
        let query = query_for([0.0, 0.0], 10);
        let exec = Arc::new(
            MemTableBruteForceVectorExec::new(
                store,
                query,
                usize::MAX,
                None,
                schema,
                /* with_row_id = */ true,
            )
            .expect("ctor")
            .with_pk_columns(Some(vec!["id".to_string()])),
        );

        let out = execute_to_batches(exec).await;
        let row_ids: Vec<u64> = out
            .iter()
            .flat_map(|batch| {
                let row_ids = batch
                    .column_by_name(lance_core::ROW_ID)
                    .unwrap()
                    .as_primitive::<arrow_array::types::UInt64Type>();
                (0..batch.num_rows())
                    .map(|row| row_ids.value(row))
                    .collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(
            row_ids,
            vec![1],
            "brute-force vector PK recency must keep only the newest duplicate"
        );
    }

    #[tokio::test]
    async fn empty_pk_columns_do_not_collapse_results() {
        let schema = make_schema();
        let batch = make_batch(
            schema.clone(),
            &[1, 2, 3],
            &[[0.0, 0.0], [1.0, 0.0], [2.0, 0.0]],
        );
        let store = store_with_batches(vec![batch]);
        let query = query_for([0.0, 0.0], 3);
        let exec = Arc::new(
            MemTableBruteForceVectorExec::new(store, query, usize::MAX, None, schema, false)
                .expect("ctor")
                .with_pk_columns(Some(vec![])),
        );

        let out = execute_to_batches(exec).await;
        let mut ids = ids_from_batches(&out);
        ids.sort_unstable();
        assert_eq!(
            ids,
            vec![1, 2, 3],
            "empty PK columns should behave like no PK columns, not one empty tuple key"
        );
    }
}
