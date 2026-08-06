// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! CoalesceFirstExec - Returns first non-empty result with short-circuit evaluation.
//!
//! Used in point lookup queries to stop searching after finding the first match.

use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion::error::Result as DFResult;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    SendableRecordBatchStream,
};
use futures::{Stream, StreamExt};

/// Returns the first non-empty result from multiple inputs with short-circuit evaluation.
///
/// Inputs are evaluated lazily in order; once a non-empty result is found,
/// remaining inputs are not evaluated. This is critical for point lookup
/// performance where we want to stop after finding the newest version.
///
/// # Behavior
///
/// 1. Execute inputs in order (first to last)
/// 2. For each input, collect all batches
/// 3. If total rows > 0, return those batches and skip remaining inputs
/// 4. If total rows == 0, move to next input
/// 5. If all inputs are empty, return empty
///
/// # Use Case
///
/// For point lookup with generations [gen3, gen2, gen1, base]:
/// - If gen3 has the key, return immediately without checking gen2, gen1, base
/// - If gen3 is empty, check gen2, and so on
#[derive(Debug)]
pub struct CoalesceFirstExec {
    /// Child execution plans (ordered: newest first for point lookup).
    inputs: Vec<Arc<dyn ExecutionPlan>>,
    /// Output schema (must be same for all inputs).
    schema: SchemaRef,
    /// Plan properties.
    properties: Arc<PlanProperties>,
}

impl CoalesceFirstExec {
    /// Create a new CoalesceFirstExec.
    ///
    /// # Arguments
    ///
    /// * `inputs` - Child plans to evaluate in order
    ///
    /// # Panics
    ///
    /// Panics if inputs is empty or if schemas don't match.
    pub fn new(inputs: Vec<Arc<dyn ExecutionPlan>>) -> Self {
        assert!(
            !inputs.is_empty(),
            "CoalesceFirstExec requires at least one input"
        );

        let schema = inputs[0].schema();

        for (i, input) in inputs.iter().enumerate().skip(1) {
            assert!(
                input.schema() == schema,
                "Input {} schema doesn't match: expected {:?}, got {:?}",
                i,
                schema,
                input.schema()
            );
        }

        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            inputs[0].pipeline_behavior(),
            inputs[0].boundedness(),
        ));

        Self {
            inputs,
            schema,
            properties,
        }
    }
}

impl DisplayAs for CoalesceFirstExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                write!(f, "CoalesceFirstExec: inputs={}", self.inputs.len())
            }
        }
    }
}

impl ExecutionPlan for CoalesceFirstExec {
    fn name(&self) -> &str {
        "CoalesceFirstExec"
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.inputs.iter().collect()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::new(children)))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let inputs: Vec<Arc<dyn ExecutionPlan>> = self.inputs.clone();
        let schema = self.schema.clone();

        Ok(Box::pin(CoalesceFirstStream::new(
            inputs, partition, context, schema,
        )))
    }
}

/// Stream that evaluates inputs in order and returns first non-empty.
struct CoalesceFirstStream {
    /// Inputs to evaluate.
    inputs: Vec<Arc<dyn ExecutionPlan>>,
    /// Current input index.
    current_input: usize,
    /// Current input stream (if active).
    current_stream: Option<SendableRecordBatchStream>,
    /// Partition to execute.
    partition: usize,
    /// Task context.
    context: Arc<TaskContext>,
    /// Output schema.
    schema: SchemaRef,
    /// Accumulated batches from current input.
    accumulated_batches: Vec<RecordBatch>,
    /// Whether we've found a non-empty result.
    found_result: bool,
    /// Index into accumulated_batches for returning.
    return_index: usize,
}

impl CoalesceFirstStream {
    fn new(
        inputs: Vec<Arc<dyn ExecutionPlan>>,
        partition: usize,
        context: Arc<TaskContext>,
        schema: SchemaRef,
    ) -> Self {
        Self {
            inputs,
            current_input: 0,
            current_stream: None,
            partition,
            context,
            schema,
            accumulated_batches: Vec::new(),
            found_result: false,
            return_index: 0,
        }
    }

    fn start_next_input(&mut self) -> DFResult<bool> {
        if self.current_input >= self.inputs.len() {
            return Ok(false);
        }

        let input = &self.inputs[self.current_input];
        let stream = input.execute(self.partition, self.context.clone())?;
        self.current_stream = Some(stream);
        self.accumulated_batches.clear();
        Ok(true)
    }
}

impl Stream for CoalesceFirstStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            if self.found_result {
                if self.return_index < self.accumulated_batches.len() {
                    let batch = self.accumulated_batches[self.return_index].clone();
                    self.return_index += 1;
                    return Poll::Ready(Some(Ok(batch)));
                } else {
                    return Poll::Ready(None);
                }
            }

            if self.current_stream.is_none() {
                match self.start_next_input() {
                    Ok(true) => {}
                    Ok(false) => return Poll::Ready(None),
                    Err(e) => return Poll::Ready(Some(Err(e))),
                }
            }

            if let Some(ref mut stream) = self.current_stream {
                match stream.poll_next_unpin(cx) {
                    Poll::Ready(Some(Ok(batch))) => {
                        if batch.num_rows() > 0 {
                            self.accumulated_batches.push(batch);
                        }
                    }
                    Poll::Ready(Some(Err(e))) => {
                        return Poll::Ready(Some(Err(e)));
                    }
                    Poll::Ready(None) => {
                        self.current_stream = None;

                        let total_rows: usize =
                            self.accumulated_batches.iter().map(|b| b.num_rows()).sum();
                        if total_rows > 0 {
                            self.found_result = true;
                            self.return_index = 0;
                            continue;
                        }

                        self.current_input += 1;
                        if self.current_input >= self.inputs.len() {
                            return Poll::Ready(None);
                        }

                        match self.start_next_input() {
                            Ok(true) => continue,
                            Ok(false) => return Poll::Ready(None),
                            Err(e) => return Poll::Ready(Some(Err(e))),
                        }
                    }
                    Poll::Pending => {
                        return Poll::Pending;
                    }
                }
            }
        }
    }
}

impl datafusion::physical_plan::RecordBatchStream for CoalesceFirstStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int32Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::physical_plan::displayable;
    use datafusion::prelude::SessionContext;
    use datafusion_physical_plan::test::TestMemoryExec;
    use futures::TryStreamExt;

    fn create_test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]))
    }

    fn create_test_batch(schema: &Schema, ids: &[i32], prefix: &str) -> RecordBatch {
        let names: Vec<String> = ids.iter().map(|id| format!("{}_{}", prefix, id)).collect();
        RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(Int32Array::from(ids.to_vec())),
                Arc::new(StringArray::from(names)),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_coalesce_first_returns_first_non_empty() {
        let schema = create_test_schema();

        // Create three inputs:
        // 1. Empty
        // 2. Has data (should be returned)
        // 3. Has data (should NOT be evaluated)
        let empty_batch = RecordBatch::new_empty(schema.clone());
        let batch2 = create_test_batch(&schema, &[1, 2], "second");
        let batch3 = create_test_batch(&schema, &[3, 4], "third");

        let input1 =
            TestMemoryExec::try_new_exec(&[vec![empty_batch]], schema.clone(), None).unwrap();
        let input2 = TestMemoryExec::try_new_exec(&[vec![batch2]], schema.clone(), None).unwrap();
        let input3 = TestMemoryExec::try_new_exec(&[vec![batch3]], schema.clone(), None).unwrap();

        let coalesce = CoalesceFirstExec::new(vec![input1, input2, input3]);

        let ctx = SessionContext::new();
        let stream = coalesce.execute(0, ctx.task_ctx()).unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();

        // Should return batch2 (first non-empty)
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 2);

        let names = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "second_1");
        assert_eq!(names.value(1), "second_2");
    }

    #[tokio::test]
    async fn test_coalesce_first_returns_first_input() {
        let schema = create_test_schema();

        // First input has data
        let batch1 = create_test_batch(&schema, &[1], "first");
        let batch2 = create_test_batch(&schema, &[2], "second");

        let input1 = TestMemoryExec::try_new_exec(&[vec![batch1]], schema.clone(), None).unwrap();
        let input2 = TestMemoryExec::try_new_exec(&[vec![batch2]], schema.clone(), None).unwrap();

        let coalesce = CoalesceFirstExec::new(vec![input1, input2]);

        let ctx = SessionContext::new();
        let stream = coalesce.execute(0, ctx.task_ctx()).unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();

        // Should return batch1
        assert_eq!(batches.len(), 1);
        let names = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "first_1");
    }

    #[tokio::test]
    async fn test_coalesce_first_all_empty() {
        let schema = create_test_schema();

        let empty1 = RecordBatch::new_empty(schema.clone());
        let empty2 = RecordBatch::new_empty(schema.clone());

        let input1 = TestMemoryExec::try_new_exec(&[vec![empty1]], schema.clone(), None).unwrap();
        let input2 = TestMemoryExec::try_new_exec(&[vec![empty2]], schema.clone(), None).unwrap();

        let coalesce = CoalesceFirstExec::new(vec![input1, input2]);

        let ctx = SessionContext::new();
        let stream = coalesce.execute(0, ctx.task_ctx()).unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();

        // Should be empty
        assert!(batches.is_empty());
    }

    #[tokio::test]
    async fn test_coalesce_first_multiple_batches_in_input() {
        let schema = create_test_schema();

        // First input has two batches
        let batch1a = create_test_batch(&schema, &[1], "first");
        let batch1b = create_test_batch(&schema, &[2], "first");
        let batch2 = create_test_batch(&schema, &[3], "second");

        let input1 =
            TestMemoryExec::try_new_exec(&[vec![batch1a, batch1b]], schema.clone(), None).unwrap();
        let input2 = TestMemoryExec::try_new_exec(&[vec![batch2]], schema.clone(), None).unwrap();

        let coalesce = CoalesceFirstExec::new(vec![input1, input2]);

        let ctx = SessionContext::new();
        let stream = coalesce.execute(0, ctx.task_ctx()).unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();

        // Should return both batches from first input
        assert_eq!(batches.len(), 2);
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);
    }

    #[test]
    fn test_display() {
        let schema = create_test_schema();
        let batch = RecordBatch::new_empty(schema.clone());
        let input = TestMemoryExec::try_new_exec(&[vec![batch]], schema, None).unwrap();

        let coalesce: Arc<dyn ExecutionPlan> = Arc::new(CoalesceFirstExec::new(vec![input]));
        // Just verify it doesn't panic
        let _ = format!("{:?}", coalesce);
        // Test that the display representation is valid
        let display_str = format!("{}", displayable(coalesce.as_ref()).indent(true));
        assert!(display_str.contains("CoalesceFirstExec"));
    }
}
