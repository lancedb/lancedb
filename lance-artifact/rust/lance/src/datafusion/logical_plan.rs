// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::{borrow::Cow, sync::Arc};

use arrow_schema::Schema as ArrowSchema;
use async_trait::async_trait;
use datafusion::{
    catalog::Session,
    datasource::TableProvider,
    error::Result as DatafusionResult,
    logical_expr::{LogicalPlan, TableType},
    physical_plan::ExecutionPlan,
    prelude::Expr,
};
use lance_core::datatypes::{OnMissing, OnTypeMismatch};

use crate::Dataset;

#[async_trait]
impl TableProvider for Dataset {
    fn schema(&self) -> Arc<ArrowSchema> {
        Arc::new(self.schema().into())
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn get_table_definition(&self) -> Option<&str> {
        None
    }

    fn get_logical_plan(&self) -> Option<Cow<'_, LogicalPlan>> {
        None
    }

    async fn scan(
        &self,
        _: &dyn Session,
        projection: Option<&Vec<usize>>,
        _: &[Expr],
        limit: Option<usize>,
    ) -> DatafusionResult<Arc<dyn ExecutionPlan>> {
        let scanner = self.scan();

        let schema_ref = self.schema();
        let projections = if let Some(projection) = projection {
            if projection.len() != schema_ref.fields.len() {
                let arrow_schema: ArrowSchema = schema_ref.into();
                let arrow_schema = arrow_schema.project(projection)?;
                schema_ref.project_by_schema(
                    &arrow_schema,
                    OnMissing::Error,
                    OnTypeMismatch::Error,
                )?
            } else {
                schema_ref.clone()
            }
        } else {
            schema_ref.clone()
        };

        let scan_range = limit.map(|l| 0..l as u64);
        let plan: Arc<dyn ExecutionPlan> = scanner.scan(
            false,
            false,
            false,
            false,
            false,
            scan_range,
            projections.into(),
        );

        Ok(plan)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{dataset::WriteParams, io::exec::LanceScanExec};
    use arrow_array::{
        Float64Array, RecordBatch, RecordBatchIterator, StringArray, StructArray,
        builder::{FixedSizeListBuilder, Int32Builder},
    };
    use arrow_schema::{DataType, Field as ArrowField, Schema as ArrowSchema, SchemaRef};
    use datafusion::prelude::*;
    use datafusion_physical_plan::coop::CooperativeExec;
    use lance_core::utils::tempfile::TempStrDir;

    fn create_batches() -> (SchemaRef, Vec<RecordBatch>) {
        let nested_fields = vec![
            ArrowField::new("lat", DataType::Float64, true),
            ArrowField::new("long", DataType::Float64, true),
        ];
        let nested = ArrowField::new_struct("point", nested_fields.clone(), true);

        let vector = ArrowField::new(
            "vector",
            DataType::FixedSizeList(ArrowField::new("item", DataType::Int32, true).into(), 2),
            true,
        );
        let utf8_fld = ArrowField::new("utf8", DataType::Utf8, true);

        let arrow_schema: SchemaRef = ArrowSchema::new(vec![vector, nested, utf8_fld]).into();

        let mut batches: Vec<RecordBatch> = Vec::new();
        let lat = vec![45.5, 46.5, -23.0]
            .into_iter()
            .collect::<Float64Array>();
        let long = vec![-73.5, -74.5, 0.0]
            .into_iter()
            .collect::<Float64Array>();

        let sa = StructArray::new(
            nested_fields.into(),
            vec![Arc::new(lat), Arc::new(long)],
            None,
        );
        let values_builder = Int32Builder::new();
        let mut vector_builder = FixedSizeListBuilder::new(values_builder, 2);
        vector_builder.values().append_value(0);
        vector_builder.values().append_value(1);
        vector_builder.append(true);
        vector_builder.values().append_value(0);
        vector_builder.values().append_value(1);
        vector_builder.append(true);
        vector_builder.values().append_value(2);
        vector_builder.values().append_value(3);
        vector_builder.append(true);
        let vector = vector_builder.finish();

        let utf8_values = StringArray::from(vec!["foo", "bar", "baz"]);

        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![Arc::new(vector), Arc::new(sa), Arc::new(utf8_values)],
        )
        .unwrap();
        batches.push(batch);

        (arrow_schema, batches)
    }

    #[tokio::test]
    async fn test_dataset_logicalplan_projection_pd() {
        let (schema, batches) = create_batches();
        let test_uri = TempStrDir::default();

        let batch_reader =
            RecordBatchIterator::new(batches.clone().into_iter().map(Ok), schema.clone());

        Dataset::write(batch_reader, &test_uri, Some(WriteParams::default()))
            .await
            .unwrap();

        let dataset = Dataset::open(&test_uri).await.unwrap();
        let ctx = SessionContext::new();
        ctx.register_table("my_table", Arc::new(dataset)).unwrap();
        let df = ctx.sql("SELECT vector, utf8 FROM my_table").await.unwrap();
        let physical_plan = df.clone().create_physical_plan().await.unwrap();

        // DataFusion will create a cooperative execution plan, so we need to get its inner plan
        let physical_plan = physical_plan
            .downcast_ref::<CooperativeExec>()
            .unwrap()
            .children()[0];

        assert!(physical_plan.downcast_ref::<LanceScanExec>().is_some());

        let expected_fields = schema
            .fields()
            .iter()
            .filter_map(|f| {
                if f.name() == "vector" || f.name() == "utf8" {
                    Some(f.as_ref().clone())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        let expected_schema = ArrowSchema::new(expected_fields);
        assert_eq!(physical_plan.schema().as_ref(), &expected_schema);
    }

    #[tokio::test]
    async fn test_dataset_logicalplan_struct_fields() {
        let (schema, batches) = create_batches();
        let test_uri = TempStrDir::default();

        let batch_reader =
            RecordBatchIterator::new(batches.clone().into_iter().map(Ok), schema.clone());

        Dataset::write(batch_reader, &test_uri, Some(WriteParams::default()))
            .await
            .unwrap();

        let dataset = Dataset::open(&test_uri).await.unwrap();
        let ctx = SessionContext::new();
        ctx.register_table("my_table", Arc::new(dataset)).unwrap();
        let df = ctx
            .sql("SELECT point.lat as lat, point.long as long FROM my_table")
            .await
            .unwrap();
        let out = df.collect().await.unwrap();
        let batch = out.first().unwrap();

        let out_schema = batch.schema();
        let expected_fields = vec![
            ArrowField::new("lat", DataType::Float64, true),
            ArrowField::new("long", DataType::Float64, true),
        ];
        let actual = out_schema
            .fields()
            .into_iter()
            .map(|f| f.as_ref().clone())
            .collect::<Vec<ArrowField>>();
        assert_eq!(actual, expected_fields);
    }
}
