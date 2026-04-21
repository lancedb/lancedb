// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use core::fmt;
use std::sync::Arc;

use arrow_array::{Array, FixedSizeListArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion_common::{DataFusionError, Result as DFResult, Statistics, stats::Precision};
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_physical_expr::{EquivalenceProperties, Partitioning};
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    execution_plan::{Boundedness, EmissionType},
};

/// Compute the output schema for both KNN exec nodes: the input table columns
/// plus `query_index: Int32` (which query vector this row belongs to) and
/// `_distance: Float32` (distance from that query vector).
fn knn_output_schema(input_schema: &SchemaRef) -> SchemaRef {
    let mut fields = input_schema.fields().to_vec();
    fields.push(Arc::new(Field::new("query_index", DataType::Int32, false)));
    fields.push(Arc::new(Field::new("_distance", DataType::Float32, true)));
    Arc::new(Schema::new(fields))
}

/// Scans one partition and returns the top-k nearest rows for **every** query
/// vector in `query_vectors`.
///
/// Output schema: input columns + `query_index: Int32` + `_distance: Float32`.
/// Each query vector contributes at most `k` rows, differentiated by
/// `query_index` (0-based position in `query_vectors`).
pub(crate) struct PartialKnnExec {
    /// The plan that produces the rows to search (e.g. a partition scan).
    input: Arc<dyn ExecutionPlan>,
    /// The column containing the stored vectors.
    column: String,
    /// All query vectors for this request, one per row.
    query_vectors: FixedSizeListArray,
    /// How many candidates to return per query vector per partition.
    k: usize,
    /// Cached plan properties (with the augmented output schema).
    properties: PlanProperties,
}

impl std::fmt::Debug for PartialKnnExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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
        query_vectors: FixedSizeListArray,
        k: usize,
    ) -> DFResult<Self> {
        let output_schema = knn_output_schema(&input.schema());
        let num_partitions = input.output_partitioning().partition_count();
        let properties = PlanProperties::new(
            EquivalenceProperties::new(output_schema),
            // Preserve input partitioning — each partition is searched independently.
            Partitioning::UnknownPartitioning(num_partitions),
            EmissionType::Final,
            Boundedness::Bounded,
        );
        Ok(Self {
            input,
            column: column.into(),
            query_vectors,
            k,
            properties,
        })
    }
}

impl DisplayAs for PartialKnnExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "PartialKnnExec: column={}, num_queries={}, k={}",
            self.column,
            self.query_vectors.len(),
            self.k,
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
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let _input_stream = self.input.execute(partition, context)?;
        let _column = self.column.clone();
        let _query_vectors = self.query_vectors.clone();
        let _k = self.k;
        let _schema = self.schema();

        // TODO: for each query vector i in query_vectors:
        //   1. Read all batches from input_stream, computing distances to
        //      query_vectors.value(i) for every row in `column`.
        //   2. Maintain a max-heap of size k over (distance, row).
        //   3. After all batches are consumed, emit up to k rows with
        //      `query_index = i` and `_distance` filled in.
        //
        // All per-query results are concatenated into one output batch, so the
        // stream emits a single RecordBatch with up to k * num_queries rows.
        //
        // Note: we must scan each query vector against the same input stream,
        // so either buffer the input batches in memory or make multiple passes
        // if the input plan supports re-execution per partition.
        Err(DataFusionError::NotImplemented(
            "PartialKnnExec::execute not yet implemented".to_string(),
        ))
    }

    fn partition_statistics(&self, _partition: Option<usize>) -> DFResult<Statistics> {
        Ok(Statistics {
            // Each partition emits at most k rows per query vector.
            num_rows: Precision::Exact(self.k * self.query_vectors.len()),
            total_byte_size: Precision::Absent,
            column_statistics: vec![],
        })
    }
}

/// Merges the per-partition top-k results from [`PartialKnnExec`] into a
/// globally-sorted top-k result for each query vector.
///
/// Output schema: same as [`PartialKnnExec`] (input columns + `query_index` +
/// `_distance`), with at most `k` rows per distinct `query_index` value, sorted
/// ascending by `_distance` within each group.
pub(crate) struct MergeKnnExec {
    /// The plan that produces per-partition top-k candidates.
    input: Arc<dyn ExecutionPlan>,
    /// Final number of results to return per query vector.
    k: usize,
    /// Cached plan properties.
    properties: PlanProperties,
}

impl std::fmt::Debug for MergeKnnExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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
            // Merge produces a single output partition.
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
        let _input_streams: Vec<_> = (0..num_partitions)
            .map(|p| self.input.execute(p, context.clone()))
            .collect::<DFResult<_>>()?;
        let _k = self.k;
        let _schema = self.schema();

        // TODO: merge _input_streams into one output batch:
        //   1. Drive all streams to completion concurrently, collecting their
        //      RecordBatches. Each stream emits at most one batch of
        //      k * num_queries rows.
        //   2. Group rows by `query_index`.
        //   3. Within each group, sort ascending by `_distance` and take the
        //      first k rows.
        //   4. Concatenate all groups and emit as a single RecordBatch.
        Err(DataFusionError::NotImplemented(
            "MergeKnnExec::execute not yet implemented".to_string(),
        ))
    }

    fn partition_statistics(&self, _partition: Option<usize>) -> DFResult<Statistics> {
        // We don't know num_queries here, so leave total row count unknown.
        Ok(Statistics::new_unknown(&self.schema()))
    }

    fn required_input_distribution(&self) -> Vec<datafusion_physical_expr::Distribution> {
        // We pull all partitions ourselves in execute() above.
        vec![datafusion_physical_expr::Distribution::UnspecifiedDistribution]
    }
}
