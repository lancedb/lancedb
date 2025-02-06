// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! This module contains adapters to allow LanceDB tables to be used as DataFusion table providers.
use std::{collections::HashMap, sync::Arc};

use arrow_schema::Schema as ArrowSchema;
use async_trait::async_trait;
use datafusion_catalog::{Session, TableProvider};
use datafusion_common::{DataFusionError, Result as DataFusionResult, Statistics};
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion_physical_plan::{
    stream::RecordBatchStreamAdapter, DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
};
use futures::{TryFutureExt, TryStreamExt};

use super::{AnyQuery, BaseTable};
use crate::{
    query::{QueryExecutionOptions, QueryRequest, Select},
    Result,
};

/// Datafusion attempts to maintain batch metadata
///
/// This is needless and it triggers bugs in DF.  This operator erases metadata from the batches.
#[derive(Debug)]
struct MetadataEraserExec {
    input: Arc<dyn ExecutionPlan>,
    schema: Arc<ArrowSchema>,
    properties: PlanProperties,
}

impl MetadataEraserExec {
    fn compute_properties_from_input(
        input: &Arc<dyn ExecutionPlan>,
        schema: &Arc<ArrowSchema>,
    ) -> PlanProperties {
        let input_properties = input.properties();
        let eq_properties = input_properties
            .eq_properties
            .clone()
            .with_new_schema(schema.clone())
            .unwrap();
        input_properties.clone().with_eq_properties(eq_properties)
    }

    fn new(input: Arc<dyn ExecutionPlan>) -> Self {
        let schema = Arc::new(
            input
                .schema()
                .as_ref()
                .clone()
                .with_metadata(HashMap::new()),
        );
        Self {
            properties: Self::compute_properties_from_input(&input, &schema),
            input,
            schema,
        }
    }
}

impl DisplayAs for MetadataEraserExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MetadataEraserExec")
    }
}

impl ExecutionPlan for MetadataEraserExec {
    fn name(&self) -> &str {
        "MetadataEraserExec"
    }

    fn as_any(&self) -> &dyn std::any::Any {
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
        assert_eq!(children.len(), 1);
        let new_properties = Self::compute_properties_from_input(&children[0], &self.schema);
        Ok(Arc::new(Self {
            input: children[0].clone(),
            schema: self.schema.clone(),
            properties: new_properties,
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let stream = self.input.execute(partition, context)?;
        let schema = self.schema.clone();
        let stream = stream.map_ok(move |batch| batch.with_schema(schema.clone()).unwrap());
        Ok(
            Box::pin(RecordBatchStreamAdapter::new(self.schema.clone(), stream))
                as SendableRecordBatchStream,
        )
    }
}

#[derive(Debug)]
pub struct BaseTableAdapter {
    table: Arc<dyn BaseTable>,
    schema: Arc<ArrowSchema>,
}

impl BaseTableAdapter {
    pub async fn try_new(table: Arc<dyn BaseTable>) -> Result<Self> {
        let schema = Arc::new(
            table
                .schema()
                .await?
                .as_ref()
                .clone()
                .with_metadata(HashMap::default()),
        );
        Ok(Self { table, schema })
    }
}

#[async_trait]
impl TableProvider for BaseTableAdapter {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> Arc<ArrowSchema> {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let mut query = QueryRequest::default();
        if let Some(projection) = projection {
            let field_names = projection
                .iter()
                .map(|i| self.schema.field(*i).name().to_string())
                .collect();
            query.select = Select::Columns(field_names);
        }
        assert!(filters.is_empty());
        if let Some(limit) = limit {
            query.limit = Some(limit);
        } else {
            // Need to override the default of 10
            query.limit = None;
        }
        let plan = self
            .table
            .create_plan(&AnyQuery::Query(query), QueryExecutionOptions::default())
            .map_err(|err| DataFusionError::External(err.into()))
            .await?;
        Ok(Arc::new(MetadataEraserExec::new(plan)))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        // TODO: Pushdown unsupported until we can support datafusion filters in BaseTable::create_plan
        Ok(vec![
            TableProviderFilterPushDown::Unsupported;
            filters.len()
        ])
    }

    fn statistics(&self) -> Option<Statistics> {
        // TODO
        None
    }
}

#[cfg(test)]
pub mod tests {
    use std::{collections::HashMap, sync::Arc};

    use arrow_array::{Int32Array, RecordBatch, RecordBatchIterator, RecordBatchReader};
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::{datasource::provider_as_source, prelude::SessionContext};
    use datafusion_catalog::TableProvider;
    use datafusion_expr::LogicalPlanBuilder;
    use futures::TryStreamExt;

    use crate::{connect, table::datafusion::BaseTableAdapter};

    fn make_test_batches() -> impl RecordBatchReader + Send + Sync + 'static {
        let metadata = HashMap::from_iter(vec![("foo".to_string(), "bar".to_string())]);
        let schema = Arc::new(
            Schema::new(vec![Field::new("i", DataType::Int32, false)]).with_metadata(metadata),
        );
        RecordBatchIterator::new(
            vec![RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int32Array::from_iter_values(0..10))],
            )],
            schema,
        )
    }

    #[tokio::test]
    async fn test_metadata_erased() {
        let db = connect("memory://test").execute().await.unwrap();

        let tbl = db
            .create_table("foo", make_test_batches())
            .execute()
            .await
            .unwrap();

        let provider = Arc::new(
            BaseTableAdapter::try_new(tbl.base_table().clone())
                .await
                .unwrap(),
        );

        assert!(provider.schema().metadata().is_empty());

        let plan = LogicalPlanBuilder::scan("foo", provider_as_source(provider), None)
            .unwrap()
            .build()
            .unwrap();

        let mut stream = SessionContext::new()
            .execute_logical_plan(plan)
            .await
            .unwrap()
            .execute_stream()
            .await
            .unwrap();

        while let Some(batch) = stream.try_next().await.unwrap() {
            assert!(batch.schema().metadata().is_empty());
        }
    }
}
