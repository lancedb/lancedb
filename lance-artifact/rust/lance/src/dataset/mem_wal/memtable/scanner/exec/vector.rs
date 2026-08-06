// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! VectorIndexExec - HNSW vector search with MVCC visibility.

use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use arrow_array::{FixedSizeListArray, Float32Array, RecordBatch, UInt64Array, cast::AsArray};
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
use datafusion_physical_expr::EquivalenceProperties;
use futures::stream::{self, StreamExt};
use lance_core::{Error, Result};

use super::super::builder::VectorQuery;
use crate::dataset::mem_wal::write::{BatchStore, IndexStore};

/// Distance column name in output.
pub const DISTANCE_COLUMN: &str = "_distance";

/// ExecutionPlan node that queries the in-memory HNSW vector index with
/// MVCC visibility.
pub struct VectorIndexExec {
    batch_store: Arc<BatchStore>,
    indexes: Arc<IndexStore>,
    query: VectorQuery,
    visible_count: usize,
    projection: Option<Vec<usize>>,
    output_schema: SchemaRef,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
    /// Whether to include _rowid column (row position) in output.
    with_row_id: bool,
}

impl Debug for VectorIndexExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut debug = f.debug_struct("VectorIndexExec");
        debug
            .field("column", &self.query.column)
            .field("k", &self.query.k);
        if let Some(ef) = self.query.ef {
            debug.field("ef", &ef);
        }
        if let Some(refine) = self.query.refine_factor {
            debug.field("refine_factor", &refine);
        }
        if let Some(metric) = &self.query.distance_type {
            debug.field("distance_type", metric);
        }
        debug.field("visible_count", &self.visible_count);
        debug.field("with_row_id", &self.with_row_id);
        debug.finish()
    }
}

impl VectorIndexExec {
    /// Create a new VectorIndexExec.
    ///
    /// # Arguments
    ///
    /// * `batch_store` - Lock-free batch store containing data
    /// * `indexes` - Index registry with HNSW vector indexes
    /// * `query` - Vector query parameters
    /// * `visible_count` - MVCC visibility sequence number
    /// * `projection` - Optional column indices to project
    /// * `base_schema` - Schema after projection (will add _distance column, and _rowid if with_row_id)
    /// * `with_row_id` - Whether to include _rowid column (row position)
    pub fn new(
        batch_store: Arc<BatchStore>,
        indexes: Arc<IndexStore>,
        query: VectorQuery,
        visible_count: usize,
        projection: Option<Vec<usize>>,
        base_schema: SchemaRef,
        with_row_id: bool,
    ) -> Result<Self> {
        let column = &query.column;
        if indexes.get_hnsw_by_column(column).is_none() {
            return Err(Error::invalid_input(format!(
                "No HNSW vector index found for column '{}'",
                column
            )));
        }

        // Build output schema: base fields + _distance + optional _rowid
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
            indexes,
            query,
            visible_count,
            projection,
            output_schema,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
            with_row_id,
        })
    }

    /// Compute the maximum visible row position based on visible_count.
    ///
    /// Returns the last row position that is visible at the given visible_count,
    /// or None if no batches are visible.
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

    /// Query the HNSW index and return matching rows with exact distances.
    ///
    /// Distances are exact because the in-memory HNSW is backed by FLAT
    /// (uncompressed) vectors; no refine step is needed.
    fn query_index(&self) -> Result<Vec<(f32, u64)>> {
        let Some(index) = self.indexes.get_hnsw_by_column(&self.query.column) else {
            return Ok(vec![]);
        };

        let Some(max_visible_row) = self.compute_max_visible_row() else {
            return Ok(vec![]);
        };

        // Normalize the query vector to a single-row FixedSizeListArray.
        let query_array = self.query.query_vector.as_ref();
        let fsl = if let Some(fsl) = query_array.as_fixed_size_list_opt() {
            fsl.clone()
        } else {
            let values = self.query.query_vector.clone();
            let dim = values.len() as i32;
            let field = Arc::new(Field::new("item", values.data_type().clone(), true));
            FixedSizeListArray::try_new(field, dim, values, None).map_err(|e| {
                Error::invalid_input(format!(
                    "Failed to wrap vector query into FixedSizeListArray (dim={}): {}",
                    dim, e
                ))
            })?
        };

        let mut results = index.search(&fsl, self.query.k, self.query.ef, max_visible_row)?;

        if self.query.distance_lower_bound.is_some() || self.query.distance_upper_bound.is_some() {
            results.retain(|&(dist, _)| {
                let above_lower = self.query.distance_lower_bound.is_none_or(|lb| dist >= lb);
                let below_upper = self.query.distance_upper_bound.is_none_or(|ub| dist < ub);
                above_lower && below_upper
            });
        }

        results.truncate(self.query.k);
        Ok(results)
    }

    /// Materialize rows from batch store with distance column.
    fn materialize_rows(&self, results: &[(f32, u64)]) -> DataFusionResult<Vec<RecordBatch>> {
        if results.is_empty() {
            return Ok(vec![]);
        }

        // Build batch ranges
        let mut batch_ranges = Vec::new();
        let mut current_row = 0usize;

        for stored_batch in self.batch_store.iter() {
            let batch_start = current_row;
            let batch_end = current_row + stored_batch.num_rows;
            batch_ranges.push((batch_start, batch_end));
            current_row = batch_end;
        }

        // Group rows by batch, tracking (row_in_batch, distance, row_position)
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

                // Add distance column
                columns.push(Arc::new(Float32Array::from(distances)));

                // Apply projection if needed (excluding distance column which is always included)
                let mut final_columns = if let Some(ref proj_indices) = self.projection {
                    let mut projected: Vec<_> =
                        proj_indices.iter().map(|&i| columns[i].clone()).collect();
                    // Always include distance as last column
                    projected.push(columns.last().unwrap().clone());
                    projected
                } else {
                    columns
                };

                // Add _rowid column if requested
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

impl DisplayAs for VectorIndexExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "VectorIndexExec: column={}, k={}",
                    self.query.column, self.query.k
                )?;
                if let Some(ef) = self.query.ef {
                    write!(f, ", ef={}", ef)?;
                }
                write!(f, ", with_row_id={}", self.with_row_id)
            }
            DisplayFormatType::TreeRender => {
                write!(
                    f,
                    "VectorIndexExec\ncolumn={}\nk={}",
                    self.query.column, self.query.k
                )?;
                if let Some(ef) = self.query.ef {
                    write!(f, "\nef={}", ef)?;
                }
                write!(f, "\nwith_row_id={}", self.with_row_id)
            }
        }
    }
}

impl ExecutionPlan for VectorIndexExec {
    fn name(&self) -> &str {
        "VectorIndexExec"
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
                "VectorIndexExec does not have children".to_string(),
            ));
        }
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        // Query the index (visibility filtering happens inside search)
        let results = self
            .query_index()
            .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

        // Materialize the rows
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
        true // Vector search naturally supports limit
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Note: end-to-end tests for VectorIndexExec live next to the in-memory
    // HNSW index in `dataset/mem_wal/index/hnsw.rs`. The structural tests
    // here only exercise the MVCC visibility plumbing.

    #[test]
    fn test_distance_column_name() {
        assert_eq!(DISTANCE_COLUMN, "_distance");
    }
}
