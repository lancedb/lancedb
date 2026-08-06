// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! MemTableScanExec - Full table scan with MVCC visibility filtering.

use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use arrow_array::{BooleanArray, RecordBatch, UInt64Array};
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

use crate::dataset::mem_wal::write::BatchStore;

/// Column name for row address (consistent with base table scanner).
pub const ROW_ADDRESS_COLUMN: &str = "_rowaddr";

/// ExecutionPlan node that scans all visible batches from a MemTable.
///
/// This node implements visibility filtering, returning only batches
/// where `batch_position <= visible_count`.
///
/// Supports filter pushdown for efficient predicate evaluation during scan.
pub struct MemTableScanExec {
    batch_store: Arc<BatchStore>,
    visible_count: usize,
    projection: Option<Vec<usize>>,
    output_schema: SchemaRef,
    /// Schema of the source data (before projection), used for filter evaluation.
    source_schema: SchemaRef,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
    /// Whether to include _rowid column (row position) in output.
    with_row_id: bool,
    /// Whether to include _rowaddr column (row position, same as _rowid but different name).
    with_row_address: bool,
    /// Optional filter predicate (physical expression).
    filter_predicate: Option<PhysicalExprRef>,
    /// Original filter expression for display purposes.
    filter_expr: Option<Expr>,
}

impl Debug for MemTableScanExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemTableScanExec")
            .field("visible_count", &self.visible_count)
            .field("projection", &self.projection)
            .field("with_row_id", &self.with_row_id)
            .field("with_row_address", &self.with_row_address)
            .field("has_filter", &self.filter_predicate.is_some())
            .finish()
    }
}

impl MemTableScanExec {
    /// Create a new MemTableScanExec without filter.
    ///
    /// # Arguments
    ///
    /// * `batch_store` - Lock-free batch store containing data
    /// * `visible_count` - Maximum batch position visible (inclusive)
    /// * `projection` - Optional column indices to project
    /// * `output_schema` - Schema after projection (should include _rowid/_rowaddr if requested)
    /// * `with_row_id` - Whether to include _rowid column (row position)
    pub fn new(
        batch_store: Arc<BatchStore>,
        visible_count: usize,
        projection: Option<Vec<usize>>,
        output_schema: SchemaRef,
        with_row_id: bool,
    ) -> Self {
        Self::with_filter(
            batch_store,
            visible_count,
            projection,
            output_schema.clone(),
            output_schema,
            with_row_id,
            false, // with_row_address
            None,
            None,
        )
    }

    /// Create a new MemTableScanExec with optional filter pushdown.
    ///
    /// # Arguments
    ///
    /// * `batch_store` - Lock-free batch store containing data
    /// * `visible_count` - Maximum batch position visible (inclusive)
    /// * `projection` - Optional column indices to project
    /// * `output_schema` - Schema after projection (should include _rowid/_rowaddr if requested)
    /// * `source_schema` - Schema of source data (before projection), used for filter evaluation
    /// * `with_row_id` - Whether to include _rowid column (row position)
    /// * `with_row_address` - Whether to include _rowaddr column (row position, for LSM scanner)
    /// * `filter_predicate` - Optional physical expression for filtering
    /// * `filter_expr` - Optional logical expression for display
    #[allow(clippy::too_many_arguments)]
    pub fn with_filter(
        batch_store: Arc<BatchStore>,
        visible_count: usize,
        projection: Option<Vec<usize>>,
        output_schema: SchemaRef,
        source_schema: SchemaRef,
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
            source_schema,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
            with_row_id,
            with_row_address,
            filter_predicate,
            filter_expr,
        }
    }
}

impl DisplayAs for MemTableScanExec {
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
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "MemTableScanExec: projection=[{}], with_row_id={}{}{}",
                    projection_names.join(", "),
                    self.with_row_id,
                    row_addr_str,
                    filter_str
                )
            }
            DisplayFormatType::TreeRender => {
                write!(
                    f,
                    "MemTableScanExec\nprojection=[{}]\nwith_row_id={}{}{}",
                    projection_names.join(", "),
                    self.with_row_id,
                    row_addr_str,
                    filter_str
                )
            }
        }
    }
}

impl ExecutionPlan for MemTableScanExec {
    fn name(&self) -> &str {
        "MemTableScanExec"
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
                "MemTableScanExec does not have children".to_string(),
            ));
        }
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        // Get visible batches with their row offsets
        let batches_with_offsets = self
            .batch_store
            .visible_batches_with_offsets(self.visible_count);

        let projection = self.projection.clone();
        let schema = self.output_schema.clone();
        let source_schema = self.source_schema.clone();
        let with_row_id = self.with_row_id;
        let with_row_address = self.with_row_address;
        let filter_predicate = self.filter_predicate.clone();

        // We need row offsets if either _rowid or _rowaddr is requested
        let need_row_offsets = with_row_id || with_row_address;

        let projected_batches: Vec<DataFusionResult<RecordBatch>> = batches_with_offsets
            .into_iter()
            .filter_map(|(batch, row_offset)| {
                // Apply filter first (on unprojected data)
                let (filtered_batch, filtered_row_offsets) = if let Some(ref predicate) =
                    filter_predicate
                {
                    // Evaluate filter predicate
                    let filter_result = predicate.evaluate(&batch);
                    let filter_array = match filter_result {
                        Ok(v) => match v.into_array(batch.num_rows()) {
                            Ok(arr) => arr,
                            Err(e) => return Some(Err(e)),
                        },
                        Err(e) => return Some(Err(e)),
                    };

                    let Some(filter_array) = filter_array.as_any().downcast_ref::<BooleanArray>()
                    else {
                        return Some(Err(datafusion::error::DataFusionError::Internal(
                            "Filter predicate did not evaluate to boolean".to_string(),
                        )));
                    };

                    // Apply filter to batch
                    let filtered =
                        match arrow_select::filter::filter_record_batch(&batch, filter_array) {
                            Ok(b) => b,
                            Err(e) => return Some(Err(e.into())),
                        };

                    // Compute filtered row offsets if needed
                    let row_offsets = if need_row_offsets {
                        let mut offsets = Vec::with_capacity(filtered.num_rows());
                        for (i, valid) in filter_array.iter().enumerate() {
                            if valid.unwrap_or(false) {
                                offsets.push(row_offset + i as u64);
                            }
                        }
                        offsets
                    } else {
                        vec![]
                    };

                    (filtered, row_offsets)
                } else {
                    // No filter - generate sequential row offsets if needed
                    let row_offsets = if need_row_offsets {
                        (0..batch.num_rows() as u64)
                            .map(|i| row_offset + i)
                            .collect()
                    } else {
                        vec![]
                    };
                    (batch, row_offsets)
                };

                // Skip empty batches after filtering
                if filtered_batch.num_rows() == 0 {
                    return None;
                }

                // Apply projection
                let mut columns: Vec<Arc<dyn arrow_array::Array>> =
                    if let Some(ref indices) = projection {
                        indices
                            .iter()
                            .map(|&i| filtered_batch.column(i).clone())
                            .collect()
                    } else {
                        filtered_batch.columns().to_vec()
                    };

                // Add _rowid column if requested
                if with_row_id {
                    columns.push(Arc::new(UInt64Array::from(filtered_row_offsets.clone())));
                }

                // Add _rowaddr column if requested (same value as _rowid, different name)
                if with_row_address {
                    columns.push(Arc::new(UInt64Array::from(filtered_row_offsets)));
                }

                Some(
                    RecordBatch::try_new(schema.clone(), columns)
                        .map_err(datafusion::error::DataFusionError::from),
                )
            })
            .collect();

        // Suppress unused variable warning
        let _ = source_schema;

        let stream = stream::iter(projected_batches).boxed();

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.output_schema.clone(),
            stream,
        )))
    }

    fn partition_statistics(&self, _partition: Option<usize>) -> DataFusionResult<Arc<Statistics>> {
        // Report statistics as Absent to avoid DataFusion analysis bugs
        // with selectivity calculation on in-memory tables.
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
    async fn test_scan_exec_basic() {
        let schema = create_test_schema();
        let batch_store = Arc::new(BatchStore::with_capacity(100));

        let batch = create_test_batch(&schema, 0, 10);
        batch_store.append(batch).unwrap();

        // Batch is at position 0, max_visible=0 means position 0 is visible
        let exec = MemTableScanExec::new(batch_store, 1, None, schema, false);

        let ctx = Arc::new(TaskContext::default());
        let stream = exec.execute(0, ctx).unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 10);
    }

    #[tokio::test]
    async fn test_scan_exec_visibility() {
        let schema = create_test_schema();
        let batch_store = Arc::new(BatchStore::with_capacity(100));

        // Insert 3 batches at positions 0, 1, 2
        batch_store
            .append(create_test_batch(&schema, 0, 10))
            .unwrap();
        batch_store
            .append(create_test_batch(&schema, 10, 10))
            .unwrap();
        batch_store
            .append(create_test_batch(&schema, 20, 10))
            .unwrap();

        // visible_count=1 means positions 0 and 1 are visible (2 batches)
        let exec = MemTableScanExec::new(batch_store.clone(), 2, None, schema.clone(), false);
        let ctx = Arc::new(TaskContext::default());
        let stream = exec.execute(0, ctx).unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();

        assert_eq!(batches.len(), 2);
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 20);
    }

    #[tokio::test]
    async fn test_scan_exec_projection() {
        let schema = create_test_schema();
        let batch_store = Arc::new(BatchStore::with_capacity(100));

        let batch = create_test_batch(&schema, 0, 10);
        batch_store.append(batch).unwrap();

        // Project only "id" column (index 0)
        let projected_schema =
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let exec = MemTableScanExec::new(batch_store, 1, Some(vec![0]), projected_schema, false);

        let ctx = Arc::new(TaskContext::default());
        let stream = exec.execute(0, ctx).unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_columns(), 1);
        assert_eq!(batches[0].schema().field(0).name(), "id");
    }

    #[tokio::test]
    async fn test_scan_exec_empty() {
        let schema = create_test_schema();
        let batch_store = Arc::new(BatchStore::with_capacity(100));

        // Empty store with max_visible=0 should return no batches
        let exec = MemTableScanExec::new(batch_store, 1, None, schema, false);

        let ctx = Arc::new(TaskContext::default());
        let stream = exec.execute(0, ctx).unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();

        assert!(batches.is_empty());
    }

    #[tokio::test]
    async fn test_scan_exec_statistics() {
        let schema = create_test_schema();
        let batch_store = Arc::new(BatchStore::with_capacity(100));

        batch_store
            .append(create_test_batch(&schema, 0, 10))
            .unwrap();
        batch_store
            .append(create_test_batch(&schema, 10, 20))
            .unwrap();

        // max_visible=1 means positions 0 and 1 are visible
        let exec = MemTableScanExec::new(batch_store, 2, None, schema, false);

        let stats = exec.partition_statistics(None).unwrap();
        // Statistics are Absent to avoid DataFusion analysis bugs
        assert_eq!(stats.num_rows, Precision::Absent);
    }

    #[tokio::test]
    async fn test_scan_exec_with_row_id() {
        let schema = create_test_schema();
        let batch_store = Arc::new(BatchStore::with_capacity(100));

        // Insert 2 batches: first with 5 rows, second with 3 rows
        batch_store
            .append(create_test_batch(&schema, 0, 5))
            .unwrap();
        batch_store
            .append(create_test_batch(&schema, 5, 3))
            .unwrap();

        // Schema with _rowid column
        let schema_with_rowid = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("_rowid", DataType::UInt64, true),
        ]));

        let exec = MemTableScanExec::new(batch_store, 2, None, schema_with_rowid, true);

        let ctx = Arc::new(TaskContext::default());
        let stream = exec.execute(0, ctx).unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();

        assert_eq!(batches.len(), 2);

        // First batch should have row_ids 0-4
        let row_ids_1 = batches[0]
            .column(2)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(row_ids_1.len(), 5);
        assert_eq!(row_ids_1.value(0), 0);
        assert_eq!(row_ids_1.value(4), 4);

        // Second batch should have row_ids 5-7
        let row_ids_2 = batches[1]
            .column(2)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(row_ids_2.len(), 3);
        assert_eq!(row_ids_2.value(0), 5);
        assert_eq!(row_ids_2.value(2), 7);
    }
}
