// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! DataFusion execution plan for inserting data into LanceDB tables.

use std::{
    any::Any,
    sync::{Arc, Mutex},
};

use arrow_array::{ArrayRef, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema as ArrowSchema, SchemaRef};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion_common::{DataFusionError, Result as DataFusionResult};
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_physical_plan::{
    stream::RecordBatchStreamAdapter, DisplayAs, DisplayFormatType, ExecutionPlan,
    ExecutionPlanProperties, Partitioning, PlanProperties,
};
use futures::{stream, StreamExt};
use lance::{
    dataset::{
        transaction::{Operation, Transaction},
        CommitBuilder, InsertBuilder, WriteMode, WriteParams,
    },
    Dataset,
};

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
    properties: PlanProperties,
    transactions: Arc<Mutex<Vec<Transaction>>>,
}

impl InsertExec {
    /// Create a new InsertExec.
    ///
    /// # Arguments
    /// * `dataset` - The dataset wrapper to insert into
    /// * `input` - The input execution plan providing data to insert
    /// * `overwrite` - If true, overwrite existing data; if false, append
    pub fn new(
        ds_wrapper: DatasetConsistencyWrapper,
        dataset: Arc<Dataset>,
        input: Arc<dyn ExecutionPlan>,
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

        let fut = async move {
            let transaction = InsertBuilder::new(dataset.clone())
                .with_params(&WriteParams {
                    mode: WriteMode::Append,
                    ..Default::default()
                })
                .execute_uncommitted_stream(input_stream)
                .await?;

            let num_rows = if let Operation::Append { fragments } = &transaction.operation {
                fragments
                    .iter()
                    .map(|f| f.num_rows().unwrap_or_default() as u64)
                    .sum()
            } else {
                0
            };

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
                let ds = CommitBuilder::new(dataset)
                    .execute_batch(transactions)
                    .await?;
                dataset_wrapper.set_latest(ds.dataset).await;
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
