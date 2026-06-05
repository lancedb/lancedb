// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::any::Any;
use std::sync::Arc;

use arrow_schema::{FieldRef, Schema, SchemaRef};
use datafusion_common::Result as DataFusionResult;
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::StreamExt;
use lance_arrow::FieldExt;

use crate::blob::coerce_record_batch_for_blob_columns;
use crate::error::Result;

/// Coerces user `LargeBinary` inputs into blob v2 struct columns before insert.
#[derive(Debug)]
pub struct BlobCoerceExec {
    input: Arc<dyn ExecutionPlan>,
    table_schema: SchemaRef,
    output_schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl BlobCoerceExec {
    fn output_schema(input_schema: &Schema, table_schema: &Schema) -> SchemaRef {
        let mut fields: Vec<FieldRef> = input_schema.fields().to_vec();
        for table_field in table_schema.fields() {
            if !table_field.is_blob_v2() {
                continue;
            }
            if let Ok(idx) = input_schema.index_of(table_field.name()) {
                fields[idx] = table_field.clone();
            }
        }
        Arc::new(Schema::new(fields))
    }

    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        table_schema: SchemaRef,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let has_blob = table_schema
            .fields()
            .iter()
            .any(|f| crate::blob::is_blob_v2(f.as_ref()));
        if !has_blob {
            return Ok(input);
        }
        let input_schema = input.schema();
        let output_schema = Self::output_schema(input_schema.as_ref(), table_schema.as_ref());
        Ok(Arc::new(Self {
            properties: input.properties().clone(),
            input,
            table_schema,
            output_schema,
        }))
    }
}

impl DisplayAs for BlobCoerceExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "BlobCoerceExec")
    }
}

impl ExecutionPlan for BlobCoerceExec {
    fn name(&self) -> &str {
        "BlobCoerceExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
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
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(datafusion_common::DataFusionError::Internal(
                "BlobCoerceExec requires exactly one child".into(),
            ));
        }
        let input = children[0].clone();
        let output_schema =
            Self::output_schema(input.schema().as_ref(), self.table_schema.as_ref());
        Ok(Arc::new(Self {
            input,
            table_schema: self.table_schema.clone(),
            output_schema,
            properties: children[0].properties().clone(),
        }))
    }

    fn schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let input = self.input.execute(partition, context)?;
        let schema = self.output_schema.clone();
        let table_schema = self.table_schema.clone();
        let stream = input.map(move |batch| {
            batch.and_then(|b| {
                coerce_record_batch_for_blob_columns(b, &table_schema)
                    .map_err(|e| datafusion_common::DataFusionError::External(e.into()))
            })
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}
