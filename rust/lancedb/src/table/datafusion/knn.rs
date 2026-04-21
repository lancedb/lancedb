// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use core::fmt;
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::{Arc, OnceLock};

use arrow::compute::{concat, interleave_record_batch};
use arrow_array::cast::AsArray;
use arrow_array::types::Int32Type;
use arrow_array::{
    Array, ArrayRef, DictionaryArray, FixedSizeListArray, Float32Array, Int32Array, RecordBatch,
};
use arrow_row::{OwnedRow, RowConverter, SortField};
use arrow_schema::{DataType, Field, FieldRef, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::execution::SessionState;
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use datafusion_common::{
    DFSchema, DFSchemaRef, DataFusionError, Result as DFResult, Statistics, TableReference,
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
// Static field helpers — allocated once
// ---------------------------------------------------------------------------

static QUERY_INDEX_FIELD: OnceLock<FieldRef> = OnceLock::new();
static DISTANCE_FIELD: OnceLock<FieldRef> = OnceLock::new();

fn query_index_field() -> &'static FieldRef {
    QUERY_INDEX_FIELD.get_or_init(|| Arc::new(Field::new("_query_index", DataType::Int32, false)))
}

fn distance_field() -> &'static FieldRef {
    DISTANCE_FIELD.get_or_init(|| Arc::new(Field::new("_distance", DataType::Float32, true)))
}

fn query_vector_field(value_field: FieldRef, dim: i32) -> FieldRef {
    Arc::new(Field::new(
        "_query_vector",
        DataType::Dictionary(
            Box::new(DataType::Int32),
            Box::new(DataType::FixedSizeList(value_field, dim)),
        ),
        true,
    ))
}

// ---------------------------------------------------------------------------
// Schema helper
// ---------------------------------------------------------------------------

/// Extend an input Arrow schema with `_query_index: Int32`, `_distance: Float32`,
/// and `_query_vector: Dictionary(Int32, FixedSizeList(...))`.
pub(crate) fn knn_output_schema(
    input_schema: &SchemaRef,
    query_vectors: &FixedSizeListArray,
) -> SchemaRef {
    let mut fields = input_schema.fields().to_vec();
    fields.push(query_index_field().clone());
    fields.push(distance_field().clone());
    // Derive the value field and dimension from the query vector array's type.
    let (value_field, dim) = match query_vectors.data_type() {
        DataType::FixedSizeList(vf, d) => (vf.clone(), *d),
        _ => unreachable!("query_vectors must be FixedSizeListArray"),
    };
    fields.push(query_vector_field(value_field, dim));
    Arc::new(Schema::new(fields))
}

// ---------------------------------------------------------------------------
// KnnNode — logical plan node
// ---------------------------------------------------------------------------

/// A logical plan node representing an exact (brute-force) KNN search over
/// all query vectors at once.
///
/// The output schema is the input schema augmented with:
/// - `_query_index: Int32` — 0-based index into `query_vectors`
/// - `_distance: Float32` — distance from that query to this row
/// - `_query_vector: Dictionary(Int32, FixedSizeList(...))` — the original query vector
///
/// At most `k` rows are emitted per distinct `_query_index`.
pub(crate) struct KnnNode {
    pub input: LogicalPlan,
    /// Qualifier and resolved field for the vector column (validated at construction).
    pub column_qualifier: Option<TableReference>,
    pub column_field: FieldRef,
    /// All query vectors; one row per query.
    pub query_vectors: Arc<FixedSizeListArray>,
    pub k: usize,
    pub distance_type: DistanceType,
    /// When false, the vector column is excluded from output rows to save memory.
    /// Set by [`KnnProjectVectorRule`] when the column isn't used downstream.
    pub project_vector: bool,
    schema: DFSchemaRef,
}

impl fmt::Debug for KnnNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KnnNode")
            .field("column", &self.column_field.name())
            .field("k", &self.k)
            .field("distance_type", &self.distance_type)
            .field("num_queries", &self.query_vectors.len())
            .field("project_vector", &self.project_vector)
            .finish()
    }
}

impl KnnNode {
    pub fn try_new(
        input: LogicalPlan,
        column: impl Into<String> + AsRef<str>,
        query_vectors: Arc<FixedSizeListArray>,
        k: usize,
        distance_type: DistanceType,
    ) -> DFResult<Self> {
        // Validate the column exists in the input schema at construction time.
        let (qualifier, field) = input
            .schema()
            .qualified_field_with_name(None, column.as_ref())?;
        let column_qualifier = qualifier.cloned();
        let column_field = field.clone();

        let input_arrow = input.schema().as_arrow().clone();
        let output_arrow = knn_output_schema(&Arc::new(input_arrow), &query_vectors);
        let schema = Arc::new(DFSchema::try_from(output_arrow.as_ref().clone())?);
        Ok(Self {
            input,
            column_qualifier,
            column_field,
            query_vectors,
            k,
            distance_type,
            project_vector: true,
            schema,
        })
    }

    /// Rebuild this node with `project_vector = false`, recomputing the schema
    /// so `_query_vector` reflects the correct type still (schema is unchanged —
    /// the output schema always includes `_query_vector`).
    pub fn without_vector_projection(self) -> Self {
        Self {
            project_vector: false,
            ..self
        }
    }
}

// PartialOrd is required by UserDefinedLogicalNodeCore but has no meaningful
// semantics for KnnNode. Return None (incomparable) always.
impl PartialOrd for KnnNode {
    fn partial_cmp(&self, _other: &Self) -> Option<Ordering> {
        None
    }
}

// Eq/PartialEq: structural fields only; use pointer identity for query_vectors.
impl PartialEq for KnnNode {
    fn eq(&self, other: &Self) -> bool {
        self.column_field.name() == other.column_field.name()
            && self.k == other.k
            && self.distance_type == other.distance_type
            && self.project_vector == other.project_vector
            && Arc::ptr_eq(&self.query_vectors, &other.query_vectors)
    }
}

impl Eq for KnnNode {}

// Hash: same fields as PartialEq; use pointer identity for query_vectors.
impl Hash for KnnNode {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.column_field.name().hash(state);
        self.k.hash(state);
        // DistanceType doesn't derive Hash; use discriminant.
        std::mem::discriminant(&self.distance_type).hash(state);
        self.project_vector.hash(state);
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
            "KnnNode: column={}, k={}, num_queries={}, distance_type={}, project_vector={}",
            self.column_field.name(),
            self.k,
            self.query_vectors.len(),
            self.distance_type,
            self.project_vector,
        )
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> DFResult<Self> {
        Ok(Self {
            input: inputs.remove(0),
            column_qualifier: self.column_qualifier.clone(),
            column_field: self.column_field.clone(),
            query_vectors: self.query_vectors.clone(),
            k: self.k,
            distance_type: self.distance_type,
            project_vector: self.project_vector,
            schema: self.schema.clone(),
        })
    }

    fn prevent_predicate_push_down_columns(&self) -> HashSet<String> {
        ["_query_index", "_distance", "_query_vector"]
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
        // Resolve the column index once at physical plan construction time.
        let input_schema = physical_inputs[0].schema();
        let col_idx = input_schema
            .fields()
            .iter()
            .position(|f| f.name() == knn.column_field.name())
            .ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "KNN column '{}' not found in physical schema",
                    knn.column_field.name()
                ))
            })?;

        let partial = PartialKnnExec::try_new(
            physical_inputs[0].clone(),
            col_idx,
            knn.query_vectors.clone(),
            knn.k,
            knn.distance_type,
            knn.project_vector,
        )?;
        let merged = MergeKnnExec::try_new(Arc::new(partial), knn.k)?;
        Ok(Some(Arc::new(merged)))
    }
}

// ---------------------------------------------------------------------------
// Heap entry for PartialKnnExec — max-heap keyed by distance (descending)
// ---------------------------------------------------------------------------

/// Heap entry stored per query during streaming top-k.
///
/// We use a max-heap (`BinaryHeap`) and pop the *largest* entry when the heap
/// exceeds `k`, leaving only the `k` smallest distances. For positive IEEE 754
/// floats, the bit representation as `u32` has the same ordering as the float,
/// so natural `u32` comparison gives us a correct max-heap on distance.
struct HeapEntry {
    /// f32 distance bits; `u32` ordering matches f32 ordering for positive values.
    dist_bits: u32,
    row: OwnedRow,
}

impl HeapEntry {
    fn new(dist: f32, row: OwnedRow) -> Self {
        Self {
            dist_bits: dist.to_bits(),
            row,
        }
    }

    fn dist(&self) -> f32 {
        f32::from_bits(self.dist_bits)
    }
}

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.dist_bits == other.dist_bits
    }
}
impl Eq for HeapEntry {}
impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // Larger dist_bits = farther candidate = higher priority to pop.
        self.dist_bits.cmp(&other.dist_bits)
    }
}

// ---------------------------------------------------------------------------
// PartialKnnExec — physical plan node (per-partition streaming top-k)
// ---------------------------------------------------------------------------

/// Scans one partition and returns the top-k nearest rows for **every** query
/// vector in `query_vectors`.
///
/// Output schema: input columns (minus the vector column if `project_vector` is
/// false) + `_query_index: Int32` + `_distance: Float32` +
/// `_query_vector: Dictionary(Int32, FixedSizeList(...))`.
///
/// Each query vector contributes at most `k` rows, differentiated by
/// `_query_index` (0-based position in `query_vectors`).
pub(crate) struct PartialKnnExec {
    input: Arc<dyn ExecutionPlan>,
    /// Pre-resolved column index in the input schema.
    column_idx: usize,
    query_vectors: Arc<FixedSizeListArray>,
    k: usize,
    distance_type: DistanceType,
    /// When false, the vector column is not included in the `RowConverter` to
    /// save memory on large embedding columns.
    project_vector: bool,
    /// Converts non-vector columns to/from row format for the heap.
    /// Built once at construction time.
    row_converter: Arc<RowConverter>,
    properties: PlanProperties,
}

impl fmt::Debug for PartialKnnExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PartialKnnExec")
            .field("column_idx", &self.column_idx)
            .field("num_queries", &self.query_vectors.len())
            .field("k", &self.k)
            .field("project_vector", &self.project_vector)
            .field("schema", &self.schema())
            .finish()
    }
}

impl PartialKnnExec {
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        column_idx: usize,
        query_vectors: Arc<FixedSizeListArray>,
        k: usize,
        distance_type: DistanceType,
        project_vector: bool,
    ) -> DFResult<Self> {
        let input_schema = input.schema();

        // Build the RowConverter from the columns we want to store in the heap
        // (all columns, or all except the vector column when project_vector=false).
        let sort_fields: Vec<SortField> = input_schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(i, _)| project_vector || *i != column_idx)
            .map(|(_, f)| SortField::new(f.data_type().clone()))
            .collect();
        let row_converter = Arc::new(
            RowConverter::new(sort_fields)
                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?,
        );

        // The output schema is derived from the input schema (excluding the
        // vector column when project_vector=false) plus the KNN metadata columns.
        let data_fields: Vec<FieldRef> = input_schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(i, _)| project_vector || *i != column_idx)
            .map(|(_, f)| f.clone())
            .collect();
        let data_schema = Arc::new(Schema::new(data_fields));
        let output_schema = knn_output_schema(&data_schema, &query_vectors);

        let num_partitions = input.output_partitioning().partition_count();
        let properties = PlanProperties::new(
            EquivalenceProperties::new(output_schema),
            Partitioning::UnknownPartitioning(num_partitions),
            EmissionType::Final,
            Boundedness::Bounded,
        );
        Ok(Self {
            input,
            column_idx,
            query_vectors,
            k,
            distance_type,
            project_vector,
            row_converter,
            properties,
        })
    }
}

impl DisplayAs for PartialKnnExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "PartialKnnExec: column_idx={}, num_queries={}, k={}, project_vector={}",
            self.column_idx,
            self.query_vectors.len(),
            self.k,
            self.project_vector,
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
            self.column_idx,
            self.query_vectors.clone(),
            self.k,
            self.distance_type,
            self.project_vector,
        )?))
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![true]
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let mut input_stream = self.input.execute(partition, context)?;
        let column_idx = self.column_idx;
        let query_vectors = self.query_vectors.clone();
        let k = self.k;
        let project_vector = self.project_vector;
        let lance_dist: LanceDistanceType = self.distance_type.into();
        let row_converter = self.row_converter.clone();
        let schema = self.schema();

        let fut = async move {
            let num_queries = query_vectors.len();
            let dist_fn = lance_dist.arrow_batch_func();

            // One max-heap per query vector. We keep at most k entries, discarding
            // the farthest candidate whenever the heap exceeds k.
            let mut heaps: Vec<BinaryHeap<HeapEntry>> = (0..num_queries)
                .map(|_| BinaryHeap::with_capacity(k + 1))
                .collect();

            while let Some(batch) = input_stream.next().await {
                let batch = batch?;

                // Extract only the columns we want to store in the row converter.
                let data_cols: Vec<ArrayRef> = batch
                    .columns()
                    .iter()
                    .enumerate()
                    .filter(|(i, _)| project_vector || *i != column_idx)
                    .map(|(_, c)| c.clone())
                    .collect();
                let rows = row_converter
                    .convert_columns(&data_cols)
                    .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;

                let fsl = batch
                    .column(column_idx)
                    .as_any()
                    .downcast_ref::<FixedSizeListArray>()
                    .ok_or_else(|| {
                        DataFusionError::Plan("KNN column is not a FixedSizeListArray".into())
                    })?;

                for (qi, heap) in heaps.iter_mut().enumerate() {
                    let query_vec = query_vectors.value(qi);
                    let distances: Arc<Float32Array> = dist_fn(query_vec.as_ref(), fsl)
                        .map_err(|e| DataFusionError::External(e.into()))?;

                    for (row_idx, &dist) in distances.values().iter().enumerate() {
                        // Only allocate an OwnedRow when this candidate can enter
                        // the heap — either the heap isn't full yet, or this
                        // distance beats the current worst.
                        if heap.len() < k || dist < heap.peek().unwrap().dist() {
                            heap.push(HeapEntry::new(dist, rows.row(row_idx).owned()));
                            if heap.len() > k {
                                heap.pop();
                            }
                        }
                    }
                }
            }

            if heaps.iter().all(|h| h.is_empty()) {
                return Ok(RecordBatch::new_empty(schema));
            }

            // Flatten heaps into (qi, dist, row) triples ordered by (qi ASC, dist ASC).
            // into_sorted_vec() returns ascending order by Ord, and our Ord has
            // larger distance = Greater, so ascending = nearest first.
            let mut entries: Vec<(i32, f32, OwnedRow)> = Vec::with_capacity(k * num_queries);
            for (qi, heap) in heaps.into_iter().enumerate() {
                for entry in heap.into_sorted_vec() {
                    entries.push((qi as i32, entry.dist(), entry.row));
                }
            }

            // Reconstruct data columns from the stored rows.
            let data_cols = row_converter
                .convert_rows(entries.iter().map(|(_, _, r)| r.row()))
                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;

            // Build the KNN metadata columns.
            let qi_arr = Arc::new(Int32Array::from_iter_values(
                entries.iter().map(|(qi, _, _)| *qi),
            )) as ArrayRef;
            let dist_arr = Arc::new(Float32Array::from_iter_values(
                entries.iter().map(|(_, d, _)| *d),
            )) as ArrayRef;
            // _query_vector is a dictionary with keys = _query_index and values = the
            // original query_vectors array. This is memory-efficient: the dictionary
            // values are shared and the keys are just the same Int32 buffer.
            let dict_col = Arc::new(
                DictionaryArray::<Int32Type>::try_new(
                    Int32Array::from_iter_values(entries.iter().map(|(qi, _, _)| *qi)),
                    query_vectors.clone(),
                )
                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?,
            ) as ArrayRef;

            let mut all_cols = data_cols;
            all_cols.push(qi_arr);
            all_cols.push(dist_arr);
            all_cols.push(dict_col);
            RecordBatch::try_new(schema, all_cols).map_err(Into::into)
        };

        let schema_clone = self.schema();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema_clone,
            futures::stream::once(fut),
        )))
    }

    fn partition_statistics(&self, _partition: Option<usize>) -> DFResult<Statistics> {
        Ok(Statistics::new_unknown(&self.schema()))
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
    /// Pre-resolved column indices in the merged schema.
    query_index_col_idx: usize,
    distance_col_idx: usize,
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
        let query_index_col_idx = schema
            .fields()
            .iter()
            .position(|f| f.name() == "_query_index")
            .ok_or_else(|| {
                DataFusionError::Internal("_query_index not found in PartialKnnExec schema".into())
            })?;
        let distance_col_idx = schema
            .fields()
            .iter()
            .position(|f| f.name() == "_distance")
            .ok_or_else(|| {
                DataFusionError::Internal("_distance not found in PartialKnnExec schema".into())
            })?;
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        );
        Ok(Self {
            input,
            k,
            query_index_col_idx,
            distance_col_idx,
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

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "MergeKnnExec only has one output partition, got {partition}"
            )));
        }

        let num_partitions = self.input.output_partitioning().partition_count();
        let input_streams: Vec<_> = (0..num_partitions)
            .map(|p| self.input.execute(p, context.clone()))
            .collect::<DFResult<_>>()?;

        let k = self.k;
        let schema = self.schema();
        let query_index_col_idx = self.query_index_col_idx;
        let distance_col_idx = self.distance_col_idx;

        let fut = async move {
            // Drain all partitions concurrently, then concatenate.
            let partition_batches: Vec<Vec<RecordBatch>> =
                futures::future::try_join_all(input_streams.into_iter().map(|mut s| async move {
                    let mut batches = Vec::new();
                    while let Some(b) = s.next().await {
                        batches.push(b?);
                    }
                    Ok::<_, DataFusionError>(batches)
                }))
                .await?;

            let batch_refs: Vec<&RecordBatch> =
                partition_batches.iter().flat_map(|v| v.iter()).collect();

            if batch_refs.is_empty() {
                return Ok(RecordBatch::new_empty(schema));
            }

            // Build batch offsets for global→(batch, local) index mapping.
            let mut batch_offsets: Vec<usize> = Vec::with_capacity(batch_refs.len() + 1);
            batch_offsets.push(0);
            for b in &batch_refs {
                batch_offsets.push(batch_offsets.last().unwrap() + b.num_rows());
            }

            // Lightweight concat of just the qi and distance columns for grouping.
            let qi_arrays: Vec<&dyn Array> = batch_refs
                .iter()
                .map(|b| b.column(query_index_col_idx).as_ref())
                .collect();
            let dist_arrays: Vec<&dyn Array> = batch_refs
                .iter()
                .map(|b| b.column(distance_col_idx).as_ref())
                .collect();
            let qi_concat = concat(&qi_arrays)?;
            let dist_concat = concat(&dist_arrays)?;
            let qi_arr = qi_concat.as_primitive::<Int32Type>();
            let dist_arr = dist_concat.as_primitive::<arrow_array::types::Float32Type>();

            let max_qi = qi_arr.values().iter().copied().max().unwrap_or(-1);
            if max_qi < 0 {
                return Ok(RecordBatch::new_empty(schema));
            }

            // Single-pass grouping: bucket rows by query index.
            let num_queries = (max_qi + 1) as usize;
            let mut groups: Vec<Vec<(f32, usize)>> = vec![Vec::new(); num_queries];
            for (i, &qi) in qi_arr.values().iter().enumerate() {
                groups[qi as usize].push((dist_arr.value(i), i));
            }

            // Per-group: sort by distance, keep top-k, append to kept_rows.
            // Groups are iterated in qi order and within each group rows are
            // sorted by distance, so the output is already in (qi ASC, dist ASC).
            let mut kept_rows: Vec<(usize, usize)> = Vec::with_capacity(k * num_queries);
            for group in &mut groups {
                group.sort_unstable_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(Ordering::Equal));
                group.truncate(k);
                for &(_, global_idx) in group.iter() {
                    let batch_idx = batch_offsets.partition_point(|&off| off <= global_idx) - 1;
                    kept_rows.push((batch_idx, global_idx - batch_offsets[batch_idx]));
                }
            }

            interleave_record_batch(&batch_refs, &kept_rows)
                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
        };

        let schema_clone = self.schema();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema_clone,
            futures::stream::once(fut),
        )))
    }

    fn partition_statistics(&self, _partition: Option<usize>) -> DFResult<Statistics> {
        Ok(Statistics::new_unknown(&self.schema()))
    }

    fn required_input_distribution(&self) -> Vec<datafusion_physical_expr::Distribution> {
        vec![datafusion_physical_expr::Distribution::UnspecifiedDistribution]
    }
}
