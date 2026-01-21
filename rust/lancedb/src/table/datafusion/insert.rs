// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! DataFusion execution plan for inserting data into LanceDB tables.

use std::{
    any::Any,
    fmt,
    sync::{Arc, Mutex},
};

use arrow_array::{ArrayRef, RecordBatch, RecordBatchReader, UInt64Array};
use arrow_schema::{DataType, Field, Schema as ArrowSchema, SchemaRef};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion_common::{DataFusionError, Result as DataFusionResult};
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_physical_plan::{
    memory::LazyBatchGenerator, memory::LazyMemoryExec, stream::RecordBatchStreamAdapter,
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, Partitioning,
    PlanProperties,
};
use futures::{stream, StreamExt};
use lance::{
    dataset::{
        transaction::{Operation, Transaction},
        CommitBuilder, InsertBuilder, WriteParams,
    },
    Dataset,
};
use lance_table::format::Fragment;

use crate::table::dataset::DatasetConsistencyWrapper;

/// Execution plan for inserting data into a LanceDB table.
///
/// This plan handles inserting data from DataFusion queries into Lance tables.
/// Each partition's data is written and committed independently.
#[derive(Debug)]
pub struct InsertExec {
    ds_wrapper: DatasetConsistencyWrapper,
    dataset: Arc<Dataset>,
    input: Arc<dyn ExecutionPlan>,
    write_params: WriteParams,
    properties: PlanProperties,
    transactions: Arc<Mutex<Vec<Transaction>>>,
}

impl InsertExec {
    /// Create a new InsertExec.
    ///
    /// # Arguments
    /// * `ds_wrapper` - The dataset wrapper to insert into
    /// * `dataset` - The current dataset snapshot
    /// * `input` - The input execution plan providing data to insert
    /// * `write_params` - Write parameters including mode (Append/Overwrite)
    pub fn new(
        ds_wrapper: DatasetConsistencyWrapper,
        dataset: Arc<Dataset>,
        input: Arc<dyn ExecutionPlan>,
        write_params: WriteParams,
    ) -> Self {
        let num_partitions = input.output_partitioning().partition_count();
        let output_schema = make_count_schema();
        let properties = PlanProperties::new(
            EquivalenceProperties::new(output_schema),
            Partitioning::UnknownPartitioning(num_partitions),
            datafusion_physical_plan::execution_plan::EmissionType::Final,
            datafusion_physical_plan::execution_plan::Boundedness::Bounded,
        );

        Self {
            ds_wrapper,
            dataset,
            input,
            write_params,
            properties,
            transactions: Arc::new(Mutex::new(Vec::with_capacity(num_partitions))),
        }
    }
}

fn make_count_schema() -> SchemaRef {
    Arc::new(ArrowSchema::new(vec![Field::new(
        "count",
        DataType::UInt64,
        false,
    )]))
}

impl DisplayAs for InsertExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "InsertExec")
    }
}

impl ExecutionPlan for InsertExec {
    fn name(&self) -> &str {
        "InsertExec"
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

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        // Input partitioning decides the number of output files, which we want
        // to control carefully with a custom optimizer rule.
        vec![false]
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let input_stream = self.input.execute(partition, context)?;

        let dataset_wrapper = self.ds_wrapper.clone();
        let dataset = self.dataset.clone();
        let output_schema = make_count_schema();
        let transactions = self.transactions.clone();
        let num_partitions = self.input.output_partitioning().partition_count();
        let write_params = self.write_params.clone();

        let fut = async move {
            let transaction = InsertBuilder::new(dataset.clone())
                .with_params(&write_params)
                .execute_uncommitted_stream(input_stream)
                .await?;

            let num_rows = count_rows_from_operation(&transaction.operation);

            let to_commit = {
                // Don't hold the lock over an await point.
                let mut txns = transactions.lock().unwrap();
                txns.push(transaction);
                if txns.len() == num_partitions {
                    Some(std::mem::take(&mut *txns))
                } else {
                    None
                }
            };

            if let Some(transactions) = to_commit {
                // We are the last writer, so we can commit the transactions
                let is_overwrite = transactions
                    .iter()
                    .any(|t| matches!(t.operation, Operation::Overwrite { .. }));

                let new_dataset = if is_overwrite {
                    // Overwrite requires merging into a single transaction and using execute()
                    let merged = merge_overwrite_transactions(transactions);
                    CommitBuilder::new(dataset)
                        .execute(merged.into_iter().next().unwrap())
                        .await?
                } else {
                    // Append transactions can be committed as a batch
                    CommitBuilder::new(dataset)
                        .execute_batch(transactions)
                        .await?
                        .dataset
                };
                dataset_wrapper.set_latest(new_dataset).await;
            }

            // Return a single batch with the count for this partition
            let count_array: ArrayRef = Arc::new(UInt64Array::from(vec![num_rows]));
            let batch = RecordBatch::try_new(make_count_schema(), vec![count_array])?;

            Ok::<_, DataFusionError>(batch)
        };

        // Convert the future to a stream
        let stream = stream::once(fut).boxed();
        let stream = RecordBatchStreamAdapter::new(output_schema, stream);

        Ok(Box::pin(stream))
    }
}

/// Count rows from an operation's fragments.
fn count_rows_from_operation(operation: &Operation) -> u64 {
    match operation {
        Operation::Append { fragments } | Operation::Overwrite { fragments, .. } => fragments
            .iter()
            .map(|f| f.num_rows().unwrap_or_default() as u64)
            .sum(),
        _ => 0,
    }
}

/// Merge multiple Overwrite transactions into a single transaction.
///
/// When multiple partitions each produce an Overwrite operation, we need to
/// combine all their fragments into a single Overwrite to commit atomically.
/// For Append operations, transactions can be committed as a batch directly.
fn merge_overwrite_transactions(mut transactions: Vec<Transaction>) -> Vec<Transaction> {
    if transactions.is_empty() {
        return transactions;
    }

    // Check if all transactions are Overwrite operations
    let all_overwrites = transactions
        .iter()
        .all(|t| matches!(t.operation, Operation::Overwrite { .. }));

    if !all_overwrites {
        return transactions;
    }

    // Single transaction doesn't need merging
    if transactions.len() == 1 {
        return transactions;
    }

    // Collect all fragments from the other transactions
    let mut all_extra_fragments: Vec<Fragment> = Vec::new();
    for txn in transactions.iter().skip(1) {
        if let Operation::Overwrite { fragments, .. } = &txn.operation {
            all_extra_fragments.extend(fragments.iter().cloned());
        }
    }

    // Merge into the first transaction
    if let Some(first_txn) = transactions.first_mut() {
        if let Operation::Overwrite { fragments, .. } = &mut first_txn.operation {
            fragments.extend(all_extra_fragments);
        }
    }

    // Return just the first transaction with all fragments merged
    transactions.truncate(1);
    transactions
}

/// A lazy batch generator that wraps a RecordBatchReader.
struct RecordBatchReaderGenerator {
    reader: Mutex<Box<dyn RecordBatchReader + Send>>,
    schema: SchemaRef,
}

impl fmt::Debug for RecordBatchReaderGenerator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RecordBatchReaderGenerator")
            .field("schema", &self.schema)
            .finish()
    }
}

impl fmt::Display for RecordBatchReaderGenerator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RecordBatchReaderGenerator")
    }
}

impl LazyBatchGenerator for RecordBatchReaderGenerator {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn generate_next_batch(&mut self) -> DataFusionResult<Option<RecordBatch>> {
        let mut reader = self.reader.lock().unwrap();
        match reader.next() {
            Some(Ok(batch)) => Ok(Some(batch)),
            Some(Err(e)) => Err(DataFusionError::ArrowError(Box::new(e), None)),
            None => Ok(None),
        }
    }
}

/// Convert a RecordBatchReader into a DataFusion ExecutionPlan.
///
/// This wraps the reader in a LazyMemoryExec that will produce batches on demand.
pub fn reader_to_execution_plan(data: Box<dyn RecordBatchReader + Send>) -> Arc<dyn ExecutionPlan> {
    use parking_lot::RwLock;

    let schema = data.schema();
    let generator = RecordBatchReaderGenerator {
        reader: Mutex::new(data),
        schema: schema.clone(),
    };
    let generators: Vec<Arc<RwLock<dyn LazyBatchGenerator>>> =
        vec![Arc::new(RwLock::new(generator))];
    Arc::new(LazyMemoryExec::try_new(schema, generators).unwrap())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_count_schema() {
        let schema = make_count_schema();
        assert_eq!(schema.fields().len(), 1);
        assert_eq!(schema.field(0).name(), "count");
        assert_eq!(schema.field(0).data_type(), &DataType::UInt64);
    }
}
