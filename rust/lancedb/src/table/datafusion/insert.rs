// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! DataFusion ExecutionPlan for inserting data into LanceDB tables.

use std::any::Any;
use std::sync::{Arc, LazyLock, Mutex};

use arrow_array::{RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema as ArrowSchema, SchemaRef};
use datafusion_common::{DataFusionError, Result as DataFusionResult};
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_physical_expr::{EquivalenceProperties, Partitioning};
use datafusion_physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
};
use lance::dataset::transaction::{Operation, Transaction};
use lance::dataset::{CommitBuilder, InsertBuilder, WriteParams};
use lance::Dataset;
use lance_table::format::Fragment;

use crate::table::dataset::DatasetConsistencyWrapper;

pub(crate) static COUNT_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(ArrowSchema::new(vec![Field::new(
        "count",
        DataType::UInt64,
        false,
    )]))
});

fn operation_fragments(operation: &Operation) -> &[Fragment] {
    match operation {
        Operation::Append { fragments } => fragments,
        Operation::Overwrite { fragments, .. } => fragments,
        _ => &[],
    }
}

fn count_rows_from_operation(operation: &Operation) -> u64 {
    operation_fragments(operation)
        .iter()
        .map(|f| f.num_rows().unwrap_or(0) as u64)
        .sum()
}

fn operation_fragments_mut(operation: &mut Operation) -> &mut Vec<Fragment> {
    match operation {
        Operation::Append { fragments } => fragments,
        Operation::Overwrite { fragments, .. } => fragments,
        _ => panic!("Unsupported operation type for getting mutable fragments"),
    }
}

fn merge_transactions(mut transactions: Vec<Transaction>) -> Option<Transaction> {
    let mut first = transactions.pop()?;

    for txn in transactions {
        let first_fragments = operation_fragments_mut(&mut first.operation);
        let txn_fragments = operation_fragments(&txn.operation);
        first_fragments.extend_from_slice(txn_fragments);
    }

    Some(first)
}

/// ExecutionPlan for inserting data into a native LanceDB table.
///
/// This plan executes inserts by:
/// 1. Each partition writes data independently using InsertBuilder::execute_uncommitted_stream
/// 2. The last partition to complete commits all transactions atomically
/// 3. Returns the count of inserted rows per partition
#[derive(Debug)]
pub struct InsertExec {
    ds_wrapper: DatasetConsistencyWrapper,
    dataset: Arc<Dataset>,
    input: Arc<dyn ExecutionPlan>,
    write_params: WriteParams,
    properties: PlanProperties,
    partial_transactions: Arc<Mutex<Vec<Transaction>>>,
}

impl InsertExec {
    pub fn new(
        ds_wrapper: DatasetConsistencyWrapper,
        dataset: Arc<Dataset>,
        input: Arc<dyn ExecutionPlan>,
        write_params: WriteParams,
    ) -> Self {
        let schema = COUNT_SCHEMA.clone();
        let num_partitions = input.output_partitioning().partition_count();
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(num_partitions),
            EmissionType::Final,
            Boundedness::Bounded,
        );

        Self {
            ds_wrapper,
            dataset,
            input,
            write_params,
            properties,
            partial_transactions: Arc::new(Mutex::new(Vec::with_capacity(num_partitions))),
        }
    }
}

impl DisplayAs for InsertExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "InsertExec: mode={:?}", self.write_params.mode)
            }
            DisplayFormatType::TreeRender => {
                write!(f, "InsertExec")
            }
        }
    }
}

impl ExecutionPlan for InsertExec {
    fn name(&self) -> &str {
        Self::static_name()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![false]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(
                "InsertExec requires exactly one child".to_string(),
            ));
        }
        Ok(Arc::new(Self::new(
            self.ds_wrapper.clone(),
            self.dataset.clone(),
            children[0].clone(),
            self.write_params.clone(),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let input_stream = self.input.execute(partition, context)?;
        let dataset = self.dataset.clone();
        let write_params = self.write_params.clone();
        let partial_transactions = self.partial_transactions.clone();
        let total_partitions = self.input.output_partitioning().partition_count();
        let ds_wrapper = self.ds_wrapper.clone();

        let stream = futures::stream::once(async move {
            let transaction = InsertBuilder::new(dataset.clone())
                .with_params(&write_params)
                .execute_uncommitted_stream(input_stream)
                .await?;

            let num_rows = count_rows_from_operation(&transaction.operation);

            let to_commit = {
                // Don't hold the lock over an await point.
                let mut txns = partial_transactions.lock().unwrap();
                txns.push(transaction);
                if txns.len() == total_partitions {
                    Some(std::mem::take(&mut *txns))
                } else {
                    None
                }
            };

            if let Some(transactions) = to_commit {
                if let Some(merged_txn) = merge_transactions(transactions) {
                    let new_dataset = CommitBuilder::new(dataset.clone())
                        .execute(merged_txn)
                        .await?;
                    ds_wrapper.update(new_dataset);
                }
            }

            Ok(RecordBatch::try_new(
                COUNT_SCHEMA.clone(),
                vec![Arc::new(UInt64Array::from(vec![num_rows]))],
            )?)
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            COUNT_SCHEMA.clone(),
            stream,
        )))
    }
}

#[cfg(test)]
mod tests {
    use std::vec;

    use super::*;
    use arrow_array::{record_batch, Int32Array, RecordBatchIterator};
    use datafusion::prelude::SessionContext;
    use datafusion_catalog::MemTable;
    use tempfile::tempdir;

    use crate::connect;

    #[tokio::test]
    async fn test_insert_via_sql() {
        let tmp_dir = tempdir().unwrap();
        let uri = tmp_dir.path().to_str().unwrap();

        let db = connect(uri).execute().await.unwrap();

        // Create initial table
        let batch = record_batch!(("id", Int32, [1, 2, 3])).unwrap();
        let schema = batch.schema();
        let reader = RecordBatchIterator::new(vec![Ok(batch)], schema);

        let table = db
            .create_table("test_insert", Box::new(reader))
            .execute()
            .await
            .unwrap();

        // Verify initial count
        assert_eq!(table.count_rows(None).await.unwrap(), 3);

        let ctx = SessionContext::new();
        let provider =
            crate::table::datafusion::BaseTableAdapter::try_new(table.base_table().clone())
                .await
                .unwrap();
        ctx.register_table("test_insert", Arc::new(provider))
            .unwrap();

        ctx.sql("INSERT INTO test_insert VALUES (4), (5), (6)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        // Verify final count
        table.checkout_latest().await.unwrap();
        assert_eq!(table.count_rows(None).await.unwrap(), 6);
    }

    #[tokio::test]
    async fn test_insert_overwrite_via_sql() {
        let tmp_dir = tempdir().unwrap();
        let uri = tmp_dir.path().to_str().unwrap();

        let db = connect(uri).execute().await.unwrap();

        // Create initial table with 3 rows
        let batch = record_batch!(("id", Int32, [1, 2, 3])).unwrap();
        let schema = batch.schema();
        let reader = RecordBatchIterator::new(vec![Ok(batch)], schema);

        let table = db
            .create_table("test_overwrite", Box::new(reader))
            .execute()
            .await
            .unwrap();

        assert_eq!(table.count_rows(None).await.unwrap(), 3);

        let ctx = SessionContext::new();
        let provider =
            crate::table::datafusion::BaseTableAdapter::try_new(table.base_table().clone())
                .await
                .unwrap();
        ctx.register_table("test_overwrite", Arc::new(provider))
            .unwrap();

        ctx.sql("INSERT OVERWRITE INTO test_overwrite VALUES (10), (20)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        // Verify: should have 2 rows (overwritten, not appended)
        table.checkout_latest().await.unwrap();
        assert_eq!(table.count_rows(None).await.unwrap(), 2);
    }

    #[tokio::test]
    async fn test_insert_empty_batch() {
        let tmp_dir = tempdir().unwrap();
        let uri = tmp_dir.path().to_str().unwrap();

        let db = connect(uri).execute().await.unwrap();

        // Create initial table
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            DataType::Int32,
            false,
        )]));
        let batches = vec![RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap()];
        let reader = RecordBatchIterator::new(batches.into_iter().map(Ok), schema.clone());

        let table = db
            .create_table("test_empty", Box::new(reader))
            .execute()
            .await
            .unwrap();

        assert_eq!(table.count_rows(None).await.unwrap(), 3);

        let ctx = SessionContext::new();
        let provider =
            crate::table::datafusion::BaseTableAdapter::try_new(table.base_table().clone())
                .await
                .unwrap();
        ctx.register_table("test_empty", Arc::new(provider))
            .unwrap();

        let source_schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            DataType::Int32,
            false,
        )]));
        // Empty batches
        let source_reader = RecordBatchIterator::new(
            std::iter::empty::<Result<RecordBatch, arrow_schema::ArrowError>>(),
            source_schema,
        );
        let source_table = db
            .create_table("empty_source", Box::new(source_reader))
            .execute()
            .await
            .unwrap();
        let source_provider =
            crate::table::datafusion::BaseTableAdapter::try_new(source_table.base_table().clone())
                .await
                .unwrap();
        ctx.register_table("empty_source", Arc::new(source_provider))
            .unwrap();

        // Execute INSERT with empty source
        ctx.sql("INSERT INTO test_empty SELECT * FROM empty_source")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        // Verify: should still have 3 rows (nothing inserted)
        table.checkout_latest().await.unwrap();
        assert_eq!(table.count_rows(None).await.unwrap(), 3);
    }

    #[tokio::test]
    async fn test_insert_multiple_batches() {
        let tmp_dir = tempdir().unwrap();
        let uri = tmp_dir.path().to_str().unwrap();

        let db = connect(uri).execute().await.unwrap();

        // Create initial table
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            DataType::Int32,
            true,
        )]));
        let batches =
            vec![
                RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![1]))])
                    .unwrap(),
            ];
        let reader = RecordBatchIterator::new(batches.into_iter().map(Ok), schema.clone());

        let table = db
            .create_table("test_multi_batch", Box::new(reader))
            .execute()
            .await
            .unwrap();

        let ctx = SessionContext::new();
        let provider =
            crate::table::datafusion::BaseTableAdapter::try_new(table.base_table().clone())
                .await
                .unwrap();
        ctx.register_table("test_multi_batch", Arc::new(provider))
            .unwrap();

        // Memtable with multiple batches and multiple partitions
        let source_table = MemTable::try_new(
            schema.clone(),
            vec![
                // Partition 0
                vec![
                    record_batch!(("id", Int32, [2, 3])).unwrap(),
                    record_batch!(("id", Int32, [4, 5])).unwrap(),
                ],
                // Partition 1
                vec![record_batch!(("id", Int32, [6, 7, 8])).unwrap()],
            ],
        )
        .unwrap();
        ctx.register_table("multi_batch_source", Arc::new(source_table))
            .unwrap();

        ctx.sql("INSERT INTO test_multi_batch SELECT * FROM multi_batch_source")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        // Verify: should have 1 + 2 + 2 + 3 = 8 rows
        table.checkout_latest().await.unwrap();
        assert_eq!(table.count_rows(None).await.unwrap(), 8);
    }
}
