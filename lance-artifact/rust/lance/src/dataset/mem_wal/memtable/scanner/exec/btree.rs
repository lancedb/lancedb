// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! BTreeIndexExec - BTree index queries with MVCC visibility.

use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use arrow_array::{RecordBatch, UInt64Array};
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
use datafusion_physical_expr::EquivalenceProperties;
use futures::stream::{self, StreamExt};
use lance_core::{Error, Result};

use super::super::builder::ScalarPredicate;
use crate::dataset::mem_wal::write::{BatchStore, IndexStore};

/// ExecutionPlan node that queries BTree index with visibility filtering.
pub struct BTreeIndexExec {
    batch_store: Arc<BatchStore>,
    indexes: Arc<IndexStore>,
    predicate: ScalarPredicate,
    visible_count: usize,
    projection: Option<Vec<usize>>,
    output_schema: SchemaRef,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
    /// Column name of the indexed field.
    column: String,
    /// Whether to include _rowid column (row position) in output.
    with_row_id: bool,
    /// Whether to include _rowaddr column (same as row position) in output.
    with_row_address: bool,
}

impl Debug for BTreeIndexExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BTreeIndexExec")
            .field("predicate", &self.predicate)
            .field("visible_count", &self.visible_count)
            .field("with_row_id", &self.with_row_id)
            .field("with_row_address", &self.with_row_address)
            .field("column", &self.column)
            .finish()
    }
}

impl BTreeIndexExec {
    /// Create a new BTreeIndexExec.
    ///
    /// # Arguments
    ///
    /// * `batch_store` - Lock-free batch store containing data
    /// * `indexes` - Index registry with BTree indexes
    /// * `predicate` - Scalar predicate to apply
    /// * `visible_count` - MVCC visibility sequence number
    /// * `projection` - Optional column indices to project
    /// * `output_schema` - Schema after projection (should include _rowid/_rowaddr if requested)
    /// * `with_row_id` - Whether to include _rowid column (row position)
    /// * `with_row_address` - Whether to include _rowaddr column (same as row position)
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        batch_store: Arc<BatchStore>,
        indexes: Arc<IndexStore>,
        predicate: ScalarPredicate,
        visible_count: usize,
        projection: Option<Vec<usize>>,
        output_schema: SchemaRef,
        with_row_id: bool,
        with_row_address: bool,
    ) -> Result<Self> {
        // Verify the index exists for this column
        let column = predicate.column().to_string();
        if indexes.get_btree_by_column(&column).is_none() {
            return Err(Error::invalid_input(format!(
                "No BTree index found for column '{}'",
                column
            )));
        }

        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(output_schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));

        Ok(Self {
            batch_store,
            indexes,
            predicate,
            visible_count,
            projection,
            output_schema,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
            column,
            with_row_id,
            with_row_address,
        })
    }

    /// Compute the maximum visible row position based on visible_count.
    /// Returns None if no batches are visible.
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

    /// Query the index and return matching row positions filtered by visibility.
    fn query_index(&self) -> Vec<u64> {
        let Some(index) = self.indexes.get_btree_by_column(&self.column) else {
            return vec![];
        };

        let Some(max_visible_row) = self.compute_max_visible_row() else {
            return vec![];
        };

        let positions = match &self.predicate {
            ScalarPredicate::Eq { value, .. } => index.get(value),
            ScalarPredicate::Range { lower, upper, .. } => {
                // For range queries, use a range scan approach
                // This is simplified - in production we'd need proper range iteration
                let mut results = Vec::new();
                let snapshot = index.snapshot();

                for (key, positions) in snapshot {
                    let in_range = match (lower, upper) {
                        (Some(l), Some(u)) => &key.0 >= l && &key.0 < u,
                        (Some(l), None) => &key.0 >= l,
                        (None, Some(u)) => &key.0 < u,
                        (None, None) => true,
                    };

                    if in_range {
                        results.extend(positions);
                    }
                }
                results
            }
            ScalarPredicate::In { values, .. } => {
                let mut results = Vec::new();
                for value in values {
                    results.extend(index.get(value));
                }
                results
            }
        };

        // Filter by visibility
        positions
            .into_iter()
            .filter(|&pos| pos <= max_visible_row)
            .collect()
    }

    /// Convert row positions to batch_id, row_within_batch, and original row_position tuples.
    fn positions_to_batch_rows(&self, positions: &[u64]) -> Vec<(usize, usize, u64)> {
        // Build a map of batch_id -> (start_row, end_row)
        let mut batch_ranges = Vec::new();
        let mut current_row = 0usize;

        for stored_batch in self.batch_store.iter() {
            let batch_start = current_row;
            let batch_end = current_row + stored_batch.num_rows;
            batch_ranges.push((batch_start, batch_end));
            current_row = batch_end;
        }

        // Convert positions to (batch_id, row_in_batch, original_row_position) tuples
        let mut result = Vec::new();
        for &pos in positions {
            let pos_usize = pos as usize;
            for (batch_id, &(start, end)) in batch_ranges.iter().enumerate() {
                if pos_usize >= start && pos_usize < end {
                    result.push((batch_id, pos_usize - start, pos));
                    break;
                }
            }
        }
        result
    }

    /// Materialize rows from batch store.
    fn materialize_rows(
        &self,
        batch_rows: &[(usize, usize, u64)],
    ) -> DataFusionResult<Vec<RecordBatch>> {
        if batch_rows.is_empty() {
            return Ok(vec![]);
        }

        // Group rows by batch, preserving row_position for _rowid
        let mut batches_to_rows: std::collections::HashMap<usize, Vec<(usize, u64)>> =
            std::collections::HashMap::new();
        for &(batch_id, row_in_batch, row_position) in batch_rows {
            batches_to_rows
                .entry(batch_id)
                .or_default()
                .push((row_in_batch, row_position));
        }

        let mut results = Vec::new();
        for (batch_id, rows_with_positions) in batches_to_rows {
            if let Some(stored) = self.batch_store.get(batch_id) {
                // Extract row indices and row positions
                let row_indices: Vec<u32> = rows_with_positions
                    .iter()
                    .map(|&(row_in_batch, _)| row_in_batch as u32)
                    .collect();
                let row_positions: Vec<u64> = rows_with_positions
                    .iter()
                    .map(|&(_, row_position)| row_position)
                    .collect();

                // Use take to select specific rows
                let indices = arrow_array::UInt32Array::from(row_indices);

                let columns: std::result::Result<Vec<_>, datafusion::error::DataFusionError> =
                    stored
                        .data
                        .columns()
                        .iter()
                        .map(|col| {
                            arrow_select::take::take(col.as_ref(), &indices, None).map_err(|e| {
                                datafusion::error::DataFusionError::ArrowError(Box::new(e), None)
                            })
                        })
                        .collect();

                let columns = columns?;

                // Apply projection
                let mut final_columns: Vec<Arc<dyn arrow_array::Array>> =
                    if let Some(ref proj_indices) = self.projection {
                        proj_indices.iter().map(|&i| columns[i].clone()).collect()
                    } else {
                        columns
                    };

                // Add _rowid column if requested
                if self.with_row_id {
                    final_columns.push(Arc::new(UInt64Array::from(row_positions.clone())));
                }

                // Add _rowaddr column if requested (same value as row position)
                if self.with_row_address {
                    final_columns.push(Arc::new(UInt64Array::from(row_positions)));
                }

                let batch = RecordBatch::try_new(self.output_schema.clone(), final_columns)?;
                results.push(batch);
            }
        }

        Ok(results)
    }
}

impl DisplayAs for BTreeIndexExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "BTreeIndexExec: predicate={:?}, column={}, with_row_id={}, with_row_address={}",
                    self.predicate, self.column, self.with_row_id, self.with_row_address
                )
            }
            DisplayFormatType::TreeRender => {
                write!(
                    f,
                    "BTreeIndexExec\npredicate={:?}\ncolumn={}\nwith_row_id={}\nwith_row_address={}",
                    self.predicate, self.column, self.with_row_id, self.with_row_address
                )
            }
        }
    }
}

impl ExecutionPlan for BTreeIndexExec {
    fn name(&self) -> &str {
        "BTreeIndexExec"
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
                "BTreeIndexExec does not have children".to_string(),
            ));
        }
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        // Query the index
        let positions = self.query_index();

        // Convert positions to batch/row pairs with visibility filtering
        let batch_rows = self.positions_to_batch_rows(&positions);

        // Materialize the rows
        let batches = self.materialize_rows(&batch_rows)?;

        let stream = stream::iter(batches.into_iter().map(Ok)).boxed();

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.output_schema.clone(),
            stream,
        )))
    }

    fn partition_statistics(&self, _partition: Option<usize>) -> DataFusionResult<Arc<Statistics>> {
        // We can't know the exact count without querying the index
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
    use arrow_array::{Int32Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::common::ScalarValue;
    use futures::TryStreamExt;

    fn create_test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]))
    }

    fn create_test_batch(schema: &Schema, start_id: i32, count: usize) -> RecordBatch {
        let ids: Vec<i32> = (start_id..start_id + count as i32).collect();
        let names: Vec<String> = ids.iter().map(|id| format!("name_{}", id)).collect();

        RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(Int32Array::from(ids)),
                Arc::new(StringArray::from(names)),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_btree_index_eq_query() {
        let schema = create_test_schema();
        let batch_store = Arc::new(BatchStore::with_capacity(100));

        // Create index registry with btree index on "id" (field_id = 0)
        let mut registry = IndexStore::new();
        registry.add_btree("id_idx".to_string(), 0, "id".to_string());

        // Insert test data and update index
        let batch = create_test_batch(&schema, 0, 10);
        registry.insert(&batch, 0).unwrap();
        batch_store.append(batch).unwrap();

        let indexes = Arc::new(registry);

        let predicate = ScalarPredicate::Eq {
            column: "id".to_string(),
            value: ScalarValue::Int32(Some(5)),
        };

        let exec = BTreeIndexExec::new(
            batch_store,
            indexes,
            predicate,
            1, // visible_count (batch at position 0)
            None,
            schema,
            false,
            false,
        )
        .unwrap();

        let ctx = Arc::new(TaskContext::default());
        let stream = exec.execute(0, ctx).unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();

        // Should find one row with id=5
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 1);
    }

    #[tokio::test]
    async fn test_btree_index_in_query() {
        let schema = create_test_schema();
        let batch_store = Arc::new(BatchStore::with_capacity(100));

        let mut registry = IndexStore::new();
        registry.add_btree("id_idx".to_string(), 0, "id".to_string());

        let batch = create_test_batch(&schema, 0, 10);
        registry.insert(&batch, 0).unwrap();
        batch_store.append(batch).unwrap();

        let indexes = Arc::new(registry);

        let predicate = ScalarPredicate::In {
            column: "id".to_string(),
            values: vec![
                ScalarValue::Int32(Some(2)),
                ScalarValue::Int32(Some(5)),
                ScalarValue::Int32(Some(8)),
            ],
        };

        let exec = BTreeIndexExec::new(
            batch_store,
            indexes,
            predicate,
            1,
            None,
            schema,
            false,
            false,
        )
        .unwrap();

        let ctx = Arc::new(TaskContext::default());
        let stream = exec.execute(0, ctx).unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();

        // Should find 3 rows
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);
    }

    #[tokio::test]
    async fn test_btree_index_visibility() {
        let schema = create_test_schema();
        let batch_store = Arc::new(BatchStore::with_capacity(100));

        let mut registry = IndexStore::new();
        registry.add_btree("id_idx".to_string(), 0, "id".to_string());

        // Insert two batches at positions 0 and 1
        let batch1 = create_test_batch(&schema, 0, 10);
        let batch2 = create_test_batch(&schema, 10, 10);
        registry.insert(&batch1, 0).unwrap();
        registry.insert(&batch2, 10).unwrap();
        batch_store.append(batch1).unwrap();
        batch_store.append(batch2).unwrap();

        let indexes = Arc::new(registry);

        let predicate = ScalarPredicate::Eq {
            column: "id".to_string(),
            value: ScalarValue::Int32(Some(15)),
        };

        // Query with max_visible=0 should not see batch at position 1
        let exec = BTreeIndexExec::new(
            batch_store.clone(),
            indexes.clone(),
            predicate.clone(),
            1,
            None,
            schema.clone(),
            false,
            false,
        )
        .unwrap();

        let ctx = Arc::new(TaskContext::default());
        let stream = exec.execute(0, ctx).unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 0);

        // Query with max_visible=1 should see both batches
        let exec = BTreeIndexExec::new(
            batch_store,
            indexes,
            predicate,
            2,
            None,
            schema,
            false,
            false,
        )
        .unwrap();

        let ctx = Arc::new(TaskContext::default());
        let stream = exec.execute(0, ctx).unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 1);
    }

    #[tokio::test]
    async fn test_btree_index_with_row_id() {
        let schema = create_test_schema();
        let batch_store = Arc::new(BatchStore::with_capacity(100));

        let mut indexes = IndexStore::new();
        indexes.add_btree("id_idx".to_string(), 0, "id".to_string());

        // Insert batch with 10 rows at position 0
        let batch = create_test_batch(&schema, 0, 10);
        batch_store.append(batch.clone()).unwrap();
        indexes
            .insert_with_batch_position(&batch, 0, Some(0))
            .unwrap();

        let indexes = Arc::new(indexes);

        // Add _rowid to schema
        let schema_with_rowid = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("_rowid", DataType::UInt64, true),
        ]));

        let predicate = ScalarPredicate::Eq {
            column: "id".to_string(),
            value: ScalarValue::Int32(Some(5)),
        };

        let exec = BTreeIndexExec::new(
            batch_store,
            indexes,
            predicate,
            1,
            None,
            schema_with_rowid.clone(),
            true,
            false,
        )
        .unwrap();

        // Verify the plan output
        let debug_str = format!("{:?}", exec);
        assert!(debug_str.contains("with_row_id: true"));
        assert!(debug_str.contains("with_row_address: false"));

        let ctx = Arc::new(TaskContext::default());
        let stream = exec.execute(0, ctx).unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();

        // Should find one row with id=5
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 1);

        // Verify _rowid column is present and has correct value
        let batch = &batches[0];
        assert_eq!(batch.num_columns(), 3);
        assert_eq!(batch.schema().field(2).name(), "_rowid");

        let row_ids = batch
            .column(2)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(row_ids.value(0), 5); // Row position for id=5 is 5
    }

    #[tokio::test]
    async fn test_btree_plan_display() {
        use crate::utils::test::assert_plan_node_equals;
        use datafusion::physical_plan::ExecutionPlan;

        let schema = create_test_schema();
        let batch_store = Arc::new(BatchStore::with_capacity(100));

        let mut indexes = IndexStore::new();
        indexes.add_btree("id_idx".to_string(), 0, "id".to_string());

        let batch = create_test_batch(&schema, 0, 10);
        batch_store.append(batch.clone()).unwrap();
        indexes
            .insert_with_batch_position(&batch, 0, Some(0))
            .unwrap();

        let indexes = Arc::new(indexes);

        let predicate = ScalarPredicate::Eq {
            column: "id".to_string(),
            value: ScalarValue::Int32(Some(5)),
        };

        // Test plan display without _rowid
        let exec: Arc<dyn ExecutionPlan> = Arc::new(
            BTreeIndexExec::new(
                batch_store.clone(),
                indexes.clone(),
                predicate.clone(),
                1,
                None,
                schema.clone(),
                false,
                false,
            )
            .unwrap(),
        );

        assert_plan_node_equals(
            exec,
            "BTreeIndexExec: predicate=Eq { column: \"id\", value: Int32(5) }, column=id, with_row_id=false, with_row_address=false",
        )
        .await
        .unwrap();

        // Test plan display with _rowid
        let schema_with_rowid = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("_rowid", DataType::UInt64, true),
        ]));

        let exec: Arc<dyn ExecutionPlan> = Arc::new(
            BTreeIndexExec::new(
                batch_store,
                indexes,
                predicate,
                1,
                None,
                schema_with_rowid,
                true,
                false,
            )
            .unwrap(),
        );

        assert_plan_node_equals(
            exec,
            "BTreeIndexExec: predicate=Eq { column: \"id\", value: Int32(5) }, column=id, with_row_id=true, with_row_address=false",
        )
        .await
        .unwrap();
    }
}
