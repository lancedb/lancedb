// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! MemTable generation tagging execution node.

use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow_array::{RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::error::Result as DFResult;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    SendableRecordBatchStream,
};
use futures::{Stream, StreamExt};

use crate::dataset::mem_wal::scanner::data_source::LsmGeneration;

/// Column name for MemTable generation in LSM scans.
///
/// This column indicates which generation (MemTable flush version) a row came from:
/// - Base table rows have generation 0
/// - MemTable rows have generation 1, 2, 3, ... (higher = newer)
pub const MEMTABLE_GEN_COLUMN: &str = "_memtable_gen";

/// Wraps a scan executor to add MemTable generation column.
///
/// This node adds a `_memtable_gen` column with a constant value to all output batches.
/// The generation column is used for deduplication ordering:
/// - Base table: gen = 0
/// - MemTables: gen = 1, 2, 3, ... (higher = newer)
#[derive(Debug)]
pub struct MemtableGenTagExec {
    /// Child execution plan.
    input: Arc<dyn ExecutionPlan>,
    /// Generation number to tag rows with.
    generation: LsmGeneration,
    /// Output schema (input schema + _gen column).
    schema: SchemaRef,
    /// Plan properties.
    properties: Arc<PlanProperties>,
}

impl MemtableGenTagExec {
    /// Create a new generation tagging executor.
    pub fn new(input: Arc<dyn ExecutionPlan>, generation: LsmGeneration) -> Self {
        let input_schema = input.schema();

        // Build output schema: input columns + _gen
        let mut fields: Vec<Arc<Field>> = input_schema.fields().iter().cloned().collect();
        fields.push(Arc::new(Field::new(
            MEMTABLE_GEN_COLUMN,
            DataType::UInt64,
            false,
        )));
        let schema = Arc::new(Schema::new(fields));

        // Preserve input properties
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            input.output_partitioning().clone(),
            input.pipeline_behavior(),
            input.boundedness(),
        ));

        Self {
            input,
            generation,
            schema,
            properties,
        }
    }

    /// Get the generation this executor tags.
    pub fn generation(&self) -> LsmGeneration {
        self.generation
    }
}

impl DisplayAs for MemtableGenTagExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                write!(f, "MemtableGenTagExec: gen={}", self.generation)
            }
        }
    }
}

impl ExecutionPlan for MemtableGenTagExec {
    fn name(&self) -> &str {
        "MemtableGenTagExec"
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(datafusion::error::DataFusionError::Internal(
                "MemtableGenTagExec requires exactly one child".to_string(),
            ));
        }
        Ok(Arc::new(Self::new(children[0].clone(), self.generation)))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let input_stream = self.input.execute(partition, context)?;
        Ok(Box::pin(GenerationTagStream {
            input: input_stream,
            generation: self.generation,
            schema: self.schema.clone(),
        }))
    }
}

/// Stream that adds generation column to batches.
struct GenerationTagStream {
    input: SendableRecordBatchStream,
    generation: LsmGeneration,
    schema: SchemaRef,
}

impl Stream for GenerationTagStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.input.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                let result = self.add_generation_column(batch);
                Poll::Ready(Some(result))
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl GenerationTagStream {
    fn add_generation_column(&self, batch: RecordBatch) -> DFResult<RecordBatch> {
        let num_rows = batch.num_rows();
        let gen_value = self.generation.as_u64();

        // Create generation column with constant value
        let gen_array = Arc::new(UInt64Array::from(vec![gen_value; num_rows]));

        // Append to existing columns
        let mut columns: Vec<Arc<dyn arrow_array::Array>> = batch.columns().to_vec();
        columns.push(gen_array);

        RecordBatch::try_new(self.schema.clone(), columns)
            .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))
    }
}

impl datafusion::physical_plan::RecordBatchStream for GenerationTagStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int32Array, StringArray, UInt64Array};
    use datafusion::prelude::SessionContext;
    use datafusion_physical_plan::test::TestMemoryExec;

    fn create_test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_generation_tag_exec() {
        let batch = create_test_batch();
        let schema = batch.schema();

        let input = TestMemoryExec::try_new_exec(&[vec![batch]], schema, None).unwrap();

        let tag_exec = MemtableGenTagExec::new(input, LsmGeneration::memtable(5));

        // Verify schema has _gen column
        let output_schema = tag_exec.schema();
        assert_eq!(output_schema.fields().len(), 3);
        assert_eq!(output_schema.field(2).name(), MEMTABLE_GEN_COLUMN);
        assert_eq!(output_schema.field(2).data_type(), &DataType::UInt64);

        // Execute and verify data
        let ctx = SessionContext::new();
        let stream = tag_exec.execute(0, ctx.task_ctx()).unwrap();
        let batches: Vec<_> = stream.collect::<Vec<_>>().await;

        assert_eq!(batches.len(), 1);
        let result = batches[0].as_ref().unwrap();
        assert_eq!(result.num_columns(), 3);
        assert_eq!(result.num_rows(), 3);

        // Check _gen column values
        let gen_col = result
            .column(2)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(gen_col.value(0), 5);
        assert_eq!(gen_col.value(1), 5);
        assert_eq!(gen_col.value(2), 5);
    }

    #[tokio::test]
    async fn test_generation_tag_base_table() {
        let batch = create_test_batch();
        let schema = batch.schema();

        let input = TestMemoryExec::try_new_exec(&[vec![batch]], schema, None).unwrap();

        let tag_exec = MemtableGenTagExec::new(input, LsmGeneration::BASE_TABLE);

        let ctx = SessionContext::new();
        let stream = tag_exec.execute(0, ctx.task_ctx()).unwrap();
        let batches: Vec<_> = stream.collect::<Vec<_>>().await;

        let result = batches[0].as_ref().unwrap();
        let gen_col = result
            .column(2)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();

        // Base table has gen = 0
        assert_eq!(gen_col.value(0), 0);
    }

    #[test]
    fn test_display() {
        let batch = create_test_batch();
        let schema = batch.schema();
        let input = TestMemoryExec::try_new_exec(&[vec![batch]], schema, None).unwrap();
        let tag_exec = MemtableGenTagExec::new(input, LsmGeneration::memtable(3));

        // Test fmt_as directly
        let mut buf = String::new();
        use std::fmt::Write;
        write!(buf, "{:?}", tag_exec).unwrap();
        assert!(buf.contains("MemtableGenTagExec"));
    }
}
