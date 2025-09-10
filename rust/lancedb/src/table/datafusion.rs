// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! This module contains adapters to allow LanceDB tables to be used as DataFusion table providers.
use std::{collections::HashMap, sync::Arc};

use arrow_array::RecordBatch;
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
    query::{QueryExecutionOptions, QueryFilter, QueryRequest, Select},
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

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true; self.children().len()]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false; self.children().len()]
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
        let stream = stream.map_ok(move |batch| {
            RecordBatch::try_new(schema.clone(), batch.columns().to_vec()).unwrap()
        });
        Ok(
            Box::pin(RecordBatchStreamAdapter::new(self.schema.clone(), stream))
                as SendableRecordBatchStream,
        )
    }

    fn partition_statistics(&self, partition: Option<usize>) -> DataFusionResult<Statistics> {
        self.input.partition_statistics(partition)
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
        state: &dyn Session,
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
        if !filters.is_empty() {
            let first = filters.first().unwrap().clone();
            let filter = filters[1..]
                .iter()
                .fold(first, |acc, expr| acc.and(expr.clone()));
            query.filter = Some(QueryFilter::Datafusion(filter));
        }
        if let Some(limit) = limit {
            query.limit = Some(limit);
        } else {
            // Need to override the default of 10
            query.limit = None;
        }

        let options = QueryExecutionOptions {
            max_batch_length: state.config().batch_size() as u32,
            ..Default::default()
        };

        let plan = self
            .table
            .create_plan(&AnyQuery::Query(query), options)
            .map_err(|err| DataFusionError::External(err.into()))
            .await?;
        Ok(Arc::new(MetadataEraserExec::new(plan)))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Exact; filters.len()])
    }

    fn statistics(&self) -> Option<Statistics> {
        // TODO
        None
    }
}

#[cfg(test)]
pub mod tests {
    use std::{collections::HashMap, sync::Arc};

    use arrow::array::AsArray;
    use arrow_array::{
        BinaryArray, Float64Array, Int32Array, Int64Array, RecordBatch, RecordBatchIterator,
        RecordBatchReader, StringArray, UInt32Array,
    };
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::{
        datasource::provider_as_source,
        prelude::{SessionConfig, SessionContext},
    };
    use datafusion_catalog::TableProvider;
    use datafusion_common::stats::Precision;
    use datafusion_execution::SendableRecordBatchStream;
    use datafusion_expr::{col, lit, LogicalPlan, LogicalPlanBuilder};
    use futures::{StreamExt, TryStreamExt};
    use tempfile::tempdir;

    use crate::{
        connect,
        index::{scalar::BTreeIndexBuilder, Index},
        table::datafusion::BaseTableAdapter,
    };

    fn make_test_batches() -> impl RecordBatchReader + Send + Sync + 'static {
        let metadata = HashMap::from_iter(vec![("foo".to_string(), "bar".to_string())]);
        let schema = Arc::new(
            Schema::new(vec![
                Field::new("i", DataType::Int32, false),
                Field::new("indexed", DataType::UInt32, false),
            ])
            .with_metadata(metadata),
        );
        RecordBatchIterator::new(
            vec![RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from_iter_values(0..10)),
                    Arc::new(UInt32Array::from_iter_values(0..10)),
                ],
            )],
            schema,
        )
    }

    fn make_tbl_two_test_batches() -> impl RecordBatchReader + Send + Sync + 'static {
        let metadata = HashMap::from_iter(vec![("foo".to_string(), "bar".to_string())]);
        let schema = Arc::new(
            Schema::new(vec![
                Field::new("ints", DataType::Int64, true),
                Field::new("strings", DataType::Utf8, true),
                Field::new("floats", DataType::Float64, true),
                Field::new("jsons", DataType::Utf8, true),
                Field::new("bins", DataType::Binary, true),
                Field::new("nodates", DataType::Utf8, true),
            ])
            .with_metadata(metadata),
        );
        RecordBatchIterator::new(
            vec![RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from_iter_values(0..1000)),
                    Arc::new(StringArray::from_iter_values(
                        (0..1000).map(|i| i.to_string()),
                    )),
                    Arc::new(Float64Array::from_iter_values((0..1000).map(|i| i as f64))),
                    Arc::new(StringArray::from_iter_values(
                        (0..1000).map(|i| format!("{{\"i\":{}}}", i)),
                    )),
                    Arc::new(BinaryArray::from_iter_values(
                        (0..1000).map(|i| (i as u32).to_be_bytes().to_vec()),
                    )),
                    Arc::new(StringArray::from_iter_values(
                        (0..1000).map(|i| i.to_string()),
                    )),
                ],
            )],
            schema,
        )
    }

    struct TestFixture {
        _tmp_dir: tempfile::TempDir,
        // An adapter for a table with make_test_batches batches
        adapter: Arc<BaseTableAdapter>,
        // an adapter for a table with make_tbl_two_test_batches batches
        adapter2: Arc<BaseTableAdapter>,
    }

    impl TestFixture {
        async fn new() -> Self {
            let tmp_dir = tempdir().unwrap();
            let dataset_path = tmp_dir.path().join("test.lance");
            let uri = dataset_path.to_str().unwrap();

            let db = connect(uri).execute().await.unwrap();

            let tbl = db
                .create_table("foo", make_test_batches())
                .execute()
                .await
                .unwrap();

            tbl.create_index(&["indexed"], Index::BTree(BTreeIndexBuilder::default()))
                .execute()
                .await
                .unwrap();

            let tbl2 = db
                .create_table("tbl2", make_tbl_two_test_batches())
                .execute()
                .await
                .unwrap();

            let adapter = Arc::new(
                BaseTableAdapter::try_new(tbl.base_table().clone())
                    .await
                    .unwrap(),
            );

            let adapter2 = Arc::new(
                BaseTableAdapter::try_new(tbl2.base_table().clone())
                    .await
                    .unwrap(),
            );

            Self {
                _tmp_dir: tmp_dir,
                adapter,
                adapter2,
            }
        }

        async fn plan_to_stream(plan: LogicalPlan) -> SendableRecordBatchStream {
            Self::plan_to_stream_with_config(plan, SessionConfig::default()).await
        }

        async fn plan_to_stream_with_config(
            plan: LogicalPlan,
            config: SessionConfig,
        ) -> SendableRecordBatchStream {
            SessionContext::new_with_config(config)
                .execute_logical_plan(plan)
                .await
                .unwrap()
                .execute_stream()
                .await
                .unwrap()
        }

        async fn plan_to_explain(plan: LogicalPlan) -> String {
            let mut explain_stream = SessionContext::new()
                .execute_logical_plan(plan)
                .await
                .unwrap()
                .explain(true, false)
                .unwrap()
                .execute_stream()
                .await
                .unwrap();
            let batch = explain_stream.try_next().await.unwrap().unwrap();
            assert!(explain_stream.try_next().await.unwrap().is_none());

            let plan_descs = batch.columns()[0].as_string::<i32>();
            let plans = batch.columns()[1].as_string::<i32>();

            for (desc, plan) in plan_descs.iter().zip(plans.iter()) {
                if desc.unwrap() == "physical_plan" {
                    return plan.unwrap().to_string();
                }
            }
            panic!("No physical plan found in explain output");
        }

        async fn check_plan(plan: LogicalPlan, expected: &str) {
            let physical_plan = Self::plan_to_explain(plan).await;
            let mut lines_checked = 0;
            for (actual_line, expected_line) in physical_plan.lines().zip(expected.lines()) {
                lines_checked += 1;
                let actual_trimmed = actual_line.trim();
                let expected_trimmed = if let Some(ellipsis_pos) = expected_line.find("...") {
                    expected_line[0..ellipsis_pos].trim()
                } else {
                    expected_line.trim()
                };
                assert_eq!(
                    &actual_trimmed[..expected_trimmed.len()],
                    expected_trimmed,
                    "\nactual:\n{physical_plan}\nexpected:\n{expected}"
                );
            }
            assert_eq!(
                lines_checked,
                expected.lines().count(),
                "\nlines_checked:\n{lines_checked}\nexpected:\n{}",
                expected.lines().count()
            );
        }
    }

    #[tokio::test]
    async fn test_batch_size() {
        let fixture = TestFixture::new().await;

        let plan = LogicalPlanBuilder::scan("foo", provider_as_source(fixture.adapter2), None)
            .unwrap()
            .build()
            .unwrap();

        let config = SessionConfig::default().with_batch_size(100);

        let stream = TestFixture::plan_to_stream_with_config(plan.clone(), config).await;

        let batch_count = stream.count().await;
        assert_eq!(batch_count, 10);

        let config = SessionConfig::default().with_batch_size(250);

        let stream = TestFixture::plan_to_stream_with_config(plan, config).await;

        let batch_count = stream.count().await;
        assert_eq!(batch_count, 4);
    }

    #[tokio::test]
    async fn test_metadata_erased() {
        let fixture = TestFixture::new().await;

        assert!(fixture.adapter.schema().metadata().is_empty());

        let plan = LogicalPlanBuilder::scan("foo", provider_as_source(fixture.adapter), None)
            .unwrap()
            .build()
            .unwrap();

        let mut stream = TestFixture::plan_to_stream(plan).await;

        while let Some(batch) = stream.try_next().await.unwrap() {
            assert!(batch.schema().metadata().is_empty());
        }
    }

    #[tokio::test]
    async fn test_metadata_erased_with_filter() {
        // This is a regression test where the metadata eraser was not properly erasing metadata
        let fixture = TestFixture::new().await;

        assert!(fixture.adapter.schema().metadata().is_empty());

        let plan = LogicalPlanBuilder::scan("foo", provider_as_source(fixture.adapter2), None)
            .unwrap()
            .filter(col("ints").lt(lit(10)))
            .unwrap()
            .build()
            .unwrap();

        let mut stream = TestFixture::plan_to_stream(plan).await;

        while let Some(batch) = stream.try_next().await.unwrap() {
            assert!(batch.schema().metadata().is_empty());
        }
    }

    #[tokio::test]
    async fn test_filter_pushdown() {
        let fixture = TestFixture::new().await;

        // Basic filter, not much different pushed down than run from DF
        let plan =
            LogicalPlanBuilder::scan("foo", provider_as_source(fixture.adapter.clone()), None)
                .unwrap()
                .filter(col("i").gt_eq(lit(5)))
                .unwrap()
                .build()
                .unwrap();

        TestFixture::check_plan(
            plan,
            "MetadataEraserExec
             ProjectionExec:...
             CooperativeExec...
             LanceRead:...",
        )
        .await;

        // Filter utilizing scalar index, make sure it gets pushed down
        let plan = LogicalPlanBuilder::scan("foo", provider_as_source(fixture.adapter), None)
            .unwrap()
            .filter(col("indexed").eq(lit(5)))
            .unwrap()
            .build()
            .unwrap();

        TestFixture::check_plan(plan, "").await;
    }

    #[tokio::test]
    async fn test_metadata_eraser_propagates_statistics() {
        let fixture = TestFixture::new().await;

        let plan =
            LogicalPlanBuilder::scan("foo", provider_as_source(fixture.adapter.clone()), None)
                .unwrap()
                .build()
                .unwrap();

        let ctx = SessionContext::new();
        let physical_plan = ctx.state().create_physical_plan(&plan).await.unwrap();

        assert_eq!(physical_plan.name(), "MetadataEraserExec");

        let partition_stats = physical_plan.partition_statistics(None).unwrap();

        assert!(matches!(partition_stats.num_rows, Precision::Exact(10)));
    }
}
