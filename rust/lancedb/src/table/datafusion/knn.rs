// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use core::fmt;
use std::cmp::Ordering;
use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::compute::{concat_batches, take_record_batch};
use arrow_array::cast::AsArray;
use arrow_array::{Array, FixedSizeListArray, Float32Array, RecordBatch, UInt32Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::execution::SessionState;
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use datafusion_common::{
    DFSchema, DFSchemaRef, DataFusionError, Result as DFResult, Statistics, stats::Precision,
};
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion_physical_expr::{EquivalenceProperties, Partitioning};
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    execution_plan::{Boundedness, EmissionType},
    stream::RecordBatchStreamAdapter,
};
use futures::StreamExt;
use lance_linalg::distance::DistanceType as LanceDistanceType;

use crate::DistanceType;

// ---------------------------------------------------------------------------
// Schema helper
// ---------------------------------------------------------------------------

/// Extend an input Arrow schema with `query_index: Int32` and `_distance: Float32`.
pub(crate) fn knn_output_schema(input_schema: &SchemaRef) -> SchemaRef {
    let mut fields = input_schema.fields().to_vec();
    fields.push(Arc::new(Field::new("query_index", DataType::Int32, false)));
    fields.push(Arc::new(Field::new("_distance", DataType::Float32, true)));
    Arc::new(Schema::new(fields))
}

// ---------------------------------------------------------------------------
// KnnNode — logical plan node
// ---------------------------------------------------------------------------

/// A logical plan node representing an exact (brute-force) KNN search over
/// all query vectors at once.
///
/// The output schema is the input schema augmented with:
/// - `query_index: Int32` — 0-based index into `query_vectors`
/// - `_distance: Float32` — distance from that query to this row
///
/// At most `k` rows are emitted per distinct `query_index`.
pub(crate) struct KnnNode {
    pub input: LogicalPlan,
    pub column: String,
    /// All query vectors; one row per query.
    pub query_vectors: Arc<FixedSizeListArray>,
    pub k: usize,
    pub distance_type: DistanceType,
    schema: DFSchemaRef,
}

impl fmt::Debug for KnnNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KnnNode")
            .field("column", &self.column)
            .field("k", &self.k)
            .field("distance_type", &self.distance_type)
            .field("num_queries", &self.query_vectors.len())
            .finish()
    }
}

impl KnnNode {
    pub fn try_new(
        input: LogicalPlan,
        column: impl Into<String>,
        query_vectors: Arc<FixedSizeListArray>,
        k: usize,
        distance_type: DistanceType,
    ) -> DFResult<Self> {
        let column = column.into();
        let input_arrow = input.schema().as_arrow().clone();
        let output_arrow = knn_output_schema(&Arc::new(input_arrow));
        let schema = Arc::new(DFSchema::try_from(output_arrow.as_ref().clone())?);
        Ok(Self {
            input,
            column,
            query_vectors,
            k,
            distance_type,
            schema,
        })
    }
}

// PartialOrd is required by UserDefinedLogicalNodeCore but has no meaningful
// semantics for KnnNode. Return None (incomparable) always.
impl PartialOrd for KnnNode {
    fn partial_cmp(&self, _other: &Self) -> Option<Ordering> {
        None
    }
}

// Eq/PartialEq: structural fields only, not vector data.
impl PartialEq for KnnNode {
    fn eq(&self, other: &Self) -> bool {
        self.column == other.column
            && self.k == other.k
            && self.distance_type == other.distance_type
            && Arc::ptr_eq(&self.query_vectors, &other.query_vectors)
    }
}

impl Eq for KnnNode {}

// Hash: same fields as PartialEq; use pointer identity for query_vectors.
impl Hash for KnnNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.column.hash(state);
        self.k.hash(state);
        // DistanceType doesn't derive Hash, use discriminant instead.
        std::mem::discriminant(&self.distance_type).hash(state);
        Arc::as_ptr(&self.query_vectors).hash(state);
    }
}

impl UserDefinedLogicalNodeCore for KnnNode {
    fn name(&self) -> &str {
        "KnnNode"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "KnnNode: column={}, k={}, num_queries={}, distance_type={}",
            self.column,
            self.k,
            self.query_vectors.len(),
            self.distance_type,
        )
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> DFResult<Self> {
        Ok(Self {
            input: inputs.remove(0),
            column: self.column.clone(),
            query_vectors: self.query_vectors.clone(),
            k: self.k,
            distance_type: self.distance_type,
            schema: self.schema.clone(),
        })
    }

    fn prevent_predicate_push_down_columns(&self) -> HashSet<String> {
        ["query_index", "_distance"]
            .iter()
            .map(|s| s.to_string())
            .collect()
    }
}

// ---------------------------------------------------------------------------
// KnnPlanner — converts KnnNode to physical plan
// ---------------------------------------------------------------------------

pub(crate) struct KnnPlanner;

#[async_trait]
impl ExtensionPlanner for KnnPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn datafusion_expr::UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &SessionState,
    ) -> DFResult<Option<Arc<dyn ExecutionPlan>>> {
        let Some(knn) = node.as_any().downcast_ref::<KnnNode>() else {
            return Ok(None);
        };
        let partial = PartialKnnExec::try_new(
            physical_inputs[0].clone(),
            knn.column.clone(),
            knn.query_vectors.clone(),
            knn.k,
            knn.distance_type,
        )?;
        let merged = MergeKnnExec::try_new(Arc::new(partial), knn.k)?;
        Ok(Some(Arc::new(merged)))
    }
}

// ---------------------------------------------------------------------------
// PartialKnnExec — physical plan node (per-partition brute-force)
// ---------------------------------------------------------------------------

/// Scans one partition and returns the top-k nearest rows for **every** query
/// vector in `query_vectors`.
///
/// Output schema: input columns + `query_index: Int32` + `_distance: Float32`.
/// Each query vector contributes at most `k` rows, differentiated by
/// `query_index` (0-based position in `query_vectors`).
pub(crate) struct PartialKnnExec {
    input: Arc<dyn ExecutionPlan>,
    column: String,
    query_vectors: Arc<FixedSizeListArray>,
    k: usize,
    distance_type: DistanceType,
    properties: PlanProperties,
}

impl fmt::Debug for PartialKnnExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PartialKnnExec")
            .field("column", &self.column)
            .field("num_queries", &self.query_vectors.len())
            .field("k", &self.k)
            .field("schema", &self.schema())
            .finish()
    }
}

impl PartialKnnExec {
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        column: impl Into<String>,
        query_vectors: Arc<FixedSizeListArray>,
        k: usize,
        distance_type: DistanceType,
    ) -> DFResult<Self> {
        let output_schema = knn_output_schema(&input.schema());
        let num_partitions = input.output_partitioning().partition_count();
        let properties = PlanProperties::new(
            EquivalenceProperties::new(output_schema),
            Partitioning::UnknownPartitioning(num_partitions),
            EmissionType::Final,
            Boundedness::Bounded,
        );
        Ok(Self {
            input,
            column: column.into(),
            query_vectors,
            k,
            distance_type,
            properties,
        })
    }
}

impl DisplayAs for PartialKnnExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "PartialKnnExec: column={}, num_queries={}, k={}, distance_type={}",
            self.column,
            self.query_vectors.len(),
            self.k,
            self.distance_type,
        )
    }
}

impl ExecutionPlan for PartialKnnExec {
    fn name(&self) -> &str {
        "PartialKnnExec"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(
                "PartialKnnExec requires exactly one child".to_string(),
            ));
        }
        Ok(Arc::new(Self::try_new(
            children.remove(0),
            self.column.clone(),
            self.query_vectors.clone(),
            self.k,
            self.distance_type,
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let mut input_stream = self.input.execute(partition, context)?;
        let column = self.column.clone();
        let query_vectors = self.query_vectors.clone();
        let k = self.k;
        let lance_dist: LanceDistanceType = self.distance_type.into();
        let schema = self.schema();

        let fut = async move {
            // Buffer all input batches for this partition.
            let mut batches: Vec<RecordBatch> = Vec::new();
            while let Some(batch) = input_stream.next().await {
                batches.push(batch?);
            }

            if batches.is_empty() {
                return Ok(RecordBatch::new_empty(schema));
            }

            let col_idx = batches[0].schema().index_of(&column).map_err(|_| {
                DataFusionError::Plan(format!("KNN column '{}' not found in schema", column))
            })?;

            let dist_fn = lance_dist.arrow_batch_func();
            let num_queries = query_vectors.len();
            let mut result_batches: Vec<RecordBatch> = Vec::with_capacity(num_queries);

            for qi in 0..num_queries {
                let query_vec = query_vectors.value(qi);

                // Collect (distance, batch_idx, row_idx) across all batches.
                let mut candidates: Vec<(f32, usize, usize)> = Vec::new();
                for (bi, batch) in batches.iter().enumerate() {
                    let vec_col = batch.column(col_idx);
                    let fsl = vec_col
                        .as_any()
                        .downcast_ref::<FixedSizeListArray>()
                        .ok_or_else(|| {
                            DataFusionError::Plan(format!(
                                "Column '{}' is not a FixedSizeListArray",
                                column
                            ))
                        })?;

                    let distances: Arc<Float32Array> = dist_fn(query_vec.as_ref(), fsl)
                        .map_err(|e| DataFusionError::External(e.into()))?;

                    for (row, dist) in distances.values().iter().enumerate() {
                        candidates.push((*dist, bi, row));
                    }
                }

                // Sort ascending by distance, keep top k.
                candidates
                    .sort_unstable_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(Ordering::Equal));
                candidates.truncate(k);

                if candidates.is_empty() {
                    continue;
                }

                let n = candidates.len();

                // For each selected (batch_idx, row_idx), extract as a
                // single-row batch then concatenate.
                let mut row_batches: Vec<RecordBatch> = Vec::with_capacity(n);
                for &(_, bi, ri) in &candidates {
                    let idx = UInt32Array::from(vec![ri as u32]);
                    // take_record_batch takes all columns at once.
                    let input_cols_count = batches[bi].num_columns();
                    let row_batch = RecordBatch::try_new(
                        batches[bi].schema(),
                        batches[bi]
                            .columns()
                            .iter()
                            .map(|c| {
                                arrow::compute::take(c.as_ref(), &idx, None)
                                    .map(|a| a as Arc<dyn Array>)
                            })
                            .collect::<Result<Vec<_>, _>>()?,
                    )?;
                    // Drop the extra info — we only need the data columns (not
                    // query_index/_distance which aren't in the input schema).
                    let _ = input_cols_count;
                    row_batches.push(row_batch);
                }

                let refs: Vec<&RecordBatch> = row_batches.iter().collect();
                let mut combined = concat_batches(&batches[0].schema(), refs)?;

                // Append query_index and _distance columns.
                let qi_col =
                    Arc::new(arrow_array::Int32Array::from(vec![qi as i32; n])) as Arc<dyn Array>;
                let dist_col = Arc::new(Float32Array::from(
                    candidates.iter().map(|&(d, _, _)| d).collect::<Vec<_>>(),
                )) as Arc<dyn Array>;

                let mut cols = combined.columns().to_vec();
                cols.push(qi_col);
                cols.push(dist_col);
                combined = RecordBatch::try_new(schema.clone(), cols)?;
                result_batches.push(combined);
            }

            if result_batches.is_empty() {
                return Ok(RecordBatch::new_empty(schema));
            }

            let refs: Vec<&RecordBatch> = result_batches.iter().collect();
            concat_batches(&schema, refs).map_err(Into::into)
        };

        let schema_clone = self.schema();
        let output = futures::stream::once(fut);
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema_clone,
            output,
        )))
    }

    fn partition_statistics(&self, _partition: Option<usize>) -> DFResult<Statistics> {
        Ok(Statistics {
            num_rows: Precision::Exact(self.k * self.query_vectors.len()),
            total_byte_size: Precision::Absent,
            column_statistics: vec![],
        })
    }
}

// ---------------------------------------------------------------------------
// MergeKnnExec — physical plan node (global top-k merge)
// ---------------------------------------------------------------------------

/// Merges the per-partition top-k results from [`PartialKnnExec`] into a
/// globally-sorted top-k result for each query vector.
pub(crate) struct MergeKnnExec {
    input: Arc<dyn ExecutionPlan>,
    k: usize,
    properties: PlanProperties,
}

impl fmt::Debug for MergeKnnExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MergeKnnExec")
            .field("k", &self.k)
            .field("schema", &self.schema())
            .finish()
    }
}

impl MergeKnnExec {
    pub fn try_new(input: Arc<dyn ExecutionPlan>, k: usize) -> DFResult<Self> {
        let schema = input.schema();
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        );
        Ok(Self {
            input,
            k,
            properties,
        })
    }
}

impl DisplayAs for MergeKnnExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MergeKnnExec: k={}", self.k)
    }
}

impl ExecutionPlan for MergeKnnExec {
    fn name(&self) -> &str {
        "MergeKnnExec"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(
                "MergeKnnExec requires exactly one child".to_string(),
            ));
        }
        Ok(Arc::new(Self::try_new(children.remove(0), self.k)?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "MergeKnnExec only has one output partition, got {}",
                partition
            )));
        }

        let num_partitions = self.input.output_partitioning().partition_count();
        let input_streams: Vec<_> = (0..num_partitions)
            .map(|p| self.input.execute(p, context.clone()))
            .collect::<DFResult<_>>()?;

        let k = self.k;
        let schema = self.schema();

        let fut = async move {
            // Collect all per-partition batches concurrently.
            let partition_batches: Vec<Vec<RecordBatch>> =
                futures::future::try_join_all(input_streams.into_iter().map(|mut s| async move {
                    let mut batches = Vec::new();
                    while let Some(b) = s.next().await {
                        batches.push(b?);
                    }
                    Ok::<_, DataFusionError>(batches)
                }))
                .await?;

            let all_batches: Vec<&RecordBatch> =
                partition_batches.iter().flat_map(|v| v.iter()).collect();

            if all_batches.is_empty() {
                return Ok(RecordBatch::new_empty(schema));
            }

            let combined = concat_batches(&schema, all_batches)?;

            let qi_col_idx = combined.schema().index_of("query_index")?;
            let dist_col_idx = combined.schema().index_of("_distance")?;

            let qi_arr = combined
                .column(qi_col_idx)
                .as_primitive::<arrow_array::types::Int32Type>();
            let dist_arr = combined
                .column(dist_col_idx)
                .as_primitive::<arrow_array::types::Float32Type>();

            let max_qi = qi_arr.values().iter().copied().max().unwrap_or(-1);
            if max_qi < 0 {
                return Ok(RecordBatch::new_empty(schema));
            }

            // For each query group, pick top-k rows by distance.
            let mut kept_rows: Vec<u32> = Vec::new();
            for qi in 0..=max_qi {
                let mut rows_for_qi: Vec<(f32, usize)> = qi_arr
                    .values()
                    .iter()
                    .enumerate()
                    .filter(|(_, q)| **q == qi)
                    .map(|(i, _)| (dist_arr.value(i), i))
                    .collect();

                rows_for_qi
                    .sort_unstable_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(Ordering::Equal));
                rows_for_qi.truncate(k);
                kept_rows.extend(rows_for_qi.into_iter().map(|(_, i)| i as u32));
            }

            // Sort final output by (query_index ASC, _distance ASC).
            kept_rows.sort_unstable_by(|&a, &b| {
                let qa = qi_arr.value(a as usize);
                let qb = qi_arr.value(b as usize);
                qa.cmp(&qb).then_with(|| {
                    dist_arr
                        .value(a as usize)
                        .partial_cmp(&dist_arr.value(b as usize))
                        .unwrap_or(Ordering::Equal)
                })
            });

            let indices = UInt32Array::from(kept_rows);
            take_record_batch(&combined, &indices).map_err(Into::into)
        };

        let schema_clone = self.schema();
        let output = futures::stream::once(fut);
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema_clone,
            output,
        )))
    }

    fn partition_statistics(&self, _partition: Option<usize>) -> DFResult<Statistics> {
        Ok(Statistics::new_unknown(&self.schema()))
    }

    fn required_input_distribution(&self) -> Vec<datafusion_physical_expr::Distribution> {
        vec![datafusion_physical_expr::Distribution::UnspecifiedDistribution]
    }
}
