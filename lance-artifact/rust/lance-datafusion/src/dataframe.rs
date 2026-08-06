// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Lance extensions for [DataFrame].

use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow_array::RecordBatch;
use arrow_ord::partition::partition;
use arrow_schema::Schema;
use datafusion::dataframe::DataFrame;
use datafusion::error::Result as DFResult;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::scalar::ScalarValue;
use futures::{Stream, StreamExt};
use lance_arrow::RecordBatchExt;

#[async_trait::async_trait]
pub trait DataFrameExt {
    /// Execute the query and return as a grouped stream.
    ///
    /// The data is assumed to have already been sorted by the partition columns.
    async fn group_by_stream(self, partition_columns: &[&str]) -> DFResult<BatchStreamGrouper>;
}

#[async_trait::async_trait]
impl DataFrameExt for DataFrame {
    async fn group_by_stream(self, partition_columns: &[&str]) -> DFResult<BatchStreamGrouper> {
        if partition_columns.is_empty() {
            return Err(datafusion::error::DataFusionError::Execution(
                "No partition columns specified".into(),
            ));
        }
        if partition_columns.len() > 1 {
            return Err(datafusion::error::DataFusionError::NotImplemented(
                "Only one partition column supported".into(),
            ));
        }
        for col in partition_columns {
            if self.schema().field_with_name(None, col).is_err() {
                return Err(datafusion::error::DataFusionError::Execution(format!(
                    "Partition column '{}' not found",
                    col
                )));
            }
        }

        Ok(BatchStreamGrouper::new(
            self.execute_stream().await?,
            partition_columns[0].into(),
        ))
    }
}

type GroupRange = (ScalarValue, Range<usize>);

/// A stream of record batch groups.
///
/// The stream works by pulling batches from the input stream and buffering them
/// into `buffer`. Once a new partition value is pulled from the input stream,
/// the buffered batches are grouped by the partition value and returned.
///
/// The partition columns are removed from the schema as they are pulled from
/// `input`.
pub struct BatchStreamGrouper {
    /// The input stream.
    input: SendableRecordBatchStream,
    /// The partition columns.
    partition_column: String, // TODO: support multiple
    /// The output schema. This is computed as the input schema minus the
    /// partition columns.
    schema: Arc<Schema>,
    /// The buffer containing the batches to be grouped for the current partition.
    buffer: Vec<RecordBatch>,
    current_partition: Option<ScalarValue>,
    /// Data that has been pulled from the input stream but not yet processed
    /// into a group.
    unprocessed: Option<(Vec<GroupRange>, RecordBatch)>,
}

impl std::fmt::Debug for BatchStreamGrouper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BatchStreamGrouper")
            .field("input", &"...")
            .field("partition_column", &self.partition_column)
            .field("schema", &self.schema)
            .field("buffer", &self.buffer)
            .field("current_partition", &self.current_partition)
            .field("unprocessed", &self.unprocessed)
            .finish()
    }
}

impl BatchStreamGrouper {
    pub fn new(input: SendableRecordBatchStream, partition_column: String) -> Self {
        let schema = Arc::new(Schema::new(
            input
                .schema()
                .fields()
                .iter()
                .filter(|f| f.name() != &partition_column)
                .cloned()
                .collect::<Vec<_>>(),
        ));
        Self {
            input,
            partition_column,
            schema,
            buffer: vec![],
            current_partition: None,
            unprocessed: None,
        }
    }

    /// Get the output schema of the stream.
    pub fn schema(&self) -> &Arc<Schema> {
        &self.schema
    }

    /// Given a record batch, find the distinct ranges of partition values.
    ///
    /// Returns the values in reverse order, so that we can pop them off the
    /// end of the vector one-by-one.
    fn compute_ranges(&self, batch: &RecordBatch) -> DFResult<Vec<(ScalarValue, Range<usize>)>> {
        let column = batch.column_by_name(&self.partition_column).ok_or(
            datafusion::error::DataFusionError::Execution("Partition column not found".into()),
        )?;
        let ranges = partition(std::slice::from_ref(column))?.ranges();
        ranges
            .into_iter()
            .rev()
            .map(|r| Ok((ScalarValue::try_from_array(column, r.start)?, r)))
            .collect::<DFResult<Vec<_>>>()
    }

    /// Fill the buffer with data from `unprocessed`.
    ///
    /// If we encounter data from a new partition, returns the current batch.
    ///
    /// If we exhaust the unprocessed data, returns None.
    fn fill_buffer(&mut self) -> Option<(Vec<ScalarValue>, Vec<RecordBatch>)> {
        // If there is data in the unprocessed buffer that matches, bring it
        // into the buffer
        if self.unprocessed.is_some() {
            let unprocessed_value = self.peek_unprocessed_value();
            match (&mut self.current_partition, unprocessed_value) {
                (Some(current), Some(next)) if current == &next => {
                    if let Some(batch) = self.pop_next_unprocessed() {
                        self.buffer.push(batch);
                    }
                }
                (None, Some(next)) => {
                    self.current_partition = Some(next);
                    if let Some(batch) = self.pop_next_unprocessed() {
                        self.buffer.push(batch);
                    }
                }
                _ => {}
            }
        }

        if self.unprocessed.is_some() && self.current_partition.is_some() {
            // If there is remaining data in the unprocessed buffer, we have reached
            // end of group, so we should return the current.
            Some((
                vec![self.current_partition.take().unwrap()],
                self.buffer.drain(..).collect(),
            ))
        } else {
            // If there is no data in the unprocessed buffer, return None as we aren't finished.
            None
        }
    }

    /// Peek at the next partition value in the unprocessed buffer.
    fn peek_unprocessed_value(&self) -> Option<ScalarValue> {
        self.unprocessed
            .as_ref()
            .map(|data| data.0.last().unwrap().0.clone())
    }

    /// Get the next unprocessed slice of data with constant partition value.
    fn pop_next_unprocessed(&mut self) -> Option<RecordBatch> {
        if let Some(data) = &mut self.unprocessed {
            if data.0.is_empty() {
                self.unprocessed = None;
                return None;
            }
            let (_part, range) = data.0.pop().unwrap();
            let batch = data.1.slice(range.start, range.end - range.start);
            let batch = batch.drop_column(&self.partition_column).unwrap();
            if data.0.is_empty() {
                self.unprocessed = None;
            }
            Some(batch)
        } else {
            None
        }
    }
}

impl Stream for BatchStreamGrouper {
    type Item = DFResult<(Vec<ScalarValue>, Vec<RecordBatch>)>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            if let Some(ready_data) = self.fill_buffer() {
                return Poll::Ready(Some(Ok(ready_data)));
            }
            debug_assert!(
                self.unprocessed.is_none(),
                "Something went wrong with state: {:?}",
                self
            );

            match self.input.poll_next_unpin(cx) {
                Poll::Ready(Some(Ok(batch))) => {
                    self.unprocessed = Some((self.compute_ranges(&batch)?, batch));
                }
                Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e))),
                Poll::Ready(None) => {
                    if self.current_partition.is_some() {
                        let batches = std::mem::take(&mut self.buffer);
                        let partition = vec![self.current_partition.take().unwrap()];
                        return Poll::Ready(Some(Ok((partition, batches))));
                    } else {
                        return Poll::Ready(None);
                    }
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::Int32Array;
    use arrow_schema::{DataType, Field};
    use datafusion::{datasource::MemTable, execution::context::SessionContext};
    use futures::TryStreamExt;

    use super::*;

    #[tokio::test]
    async fn test_group_by_stream() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8])),
                Arc::new(Int32Array::from(vec![1, 1, 2, 2, 2, 3, 3, 4])),
            ],
        )
        .unwrap();
        let batches = vec![
            batch.slice(0, 3), // a = [1, 2, 3], b = [1, 1, 2]
            batch.slice(3, 2), // a = [4, 5], b = [2, 2]
            batch.slice(5, 3), // a = [6, 7, 8], b = [3, 3, 4]
        ];

        let table = MemTable::try_new(schema, vec![batches]).unwrap();
        let ctx = SessionContext::new();
        let df = ctx.read_table(Arc::new(table)).unwrap();
        let actual = df
            .group_by_stream(&["b"])
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        let expected_batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)])),
            vec![batch["a"].clone()],
        )
        .unwrap();
        let expected = vec![
            (
                vec![ScalarValue::Int32(Some(1))],
                vec![expected_batch.slice(0, 2)],
            ),
            (
                vec![ScalarValue::Int32(Some(2))],
                vec![expected_batch.slice(2, 1), expected_batch.slice(3, 2)],
            ),
            (
                vec![ScalarValue::Int32(Some(3))],
                vec![expected_batch.slice(5, 2)],
            ),
            (
                vec![ScalarValue::Int32(Some(4))],
                vec![expected_batch.slice(7, 1)],
            ),
        ];

        assert_eq!(expected, actual);
    }

    // TODO: test the stream more.
}
