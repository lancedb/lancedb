// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! This module contains adapters to allow LanceDB tables to be used as DataFusion table providers.

mod blob_coerce;
mod blob_nulls;
pub mod cast;
pub mod insert;
pub mod reject_nan;
pub mod scannable_exec;
pub mod udtf;

use std::{collections::HashMap, sync::Arc};

use arrow_array::{RecordBatch, RecordBatchOptions};
use arrow_schema::Schema as ArrowSchema;
use async_trait::async_trait;
use datafusion_catalog::{Session, TableProvider};
use datafusion_common::{DataFusionError, Result as DataFusionResult, Statistics};
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_expr::{Expr, TableProviderFilterPushDown, TableType, dml::InsertOp};
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, execution_plan::CardinalityEffect,
    stream::RecordBatchStreamAdapter,
};
use futures::{TryFutureExt, TryStreamExt};
use lance::dataset::{WriteMode, WriteParams};
use lance_core::datatypes::{Projection, Schema as LanceSchema};

use super::{AnyQuery, BaseTable};
use crate::{
    Result,
    query::{QueryExecutionOptions, QueryFilter, QueryRequest, Select},
};
use arrow_schema::{DataType, Field};
use lance_index::scalar::FullTextSearchQuery;

/// An execution plan that erases Arrow schema-level metadata from its input's batches.
///
/// DataFusion attempts to maintain batch metadata. This is needless and it triggers bugs in
/// DF, so [`BaseTableAdapter::scan`] wraps every scan it produces in one of these.
///
/// ```
/// use std::sync::Arc;
///
/// use datafusion_physical_plan::ExecutionPlan;
/// use lancedb::table::datafusion::MetadataEraserExec;
///
/// # fn erase_metadata(scan: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
/// Arc::new(MetadataEraserExec::new(scan))
/// # }
/// ```
#[derive(Debug)]
pub struct MetadataEraserExec {
    input: Arc<dyn ExecutionPlan>,
    schema: Arc<ArrowSchema>,
    properties: Arc<PlanProperties>,
}

impl MetadataEraserExec {
    fn compute_properties_from_input(
        input: &Arc<dyn ExecutionPlan>,
        schema: &Arc<ArrowSchema>,
    ) -> Arc<PlanProperties> {
        let input_properties = input.properties();
        let eq_properties = input_properties
            .eq_properties
            .clone()
            .with_new_schema(schema.clone())
            .unwrap();
        Arc::new(
            input_properties
                .as_ref()
                .clone()
                .with_eq_properties(eq_properties),
        )
    }

    /// Wrap `input` in an operator that strips schema-level metadata from its batches.
    pub fn new(input: Arc<dyn ExecutionPlan>) -> Self {
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

    fn properties(&self) -> &Arc<PlanProperties> {
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
        Ok(Arc::new(Self::new(children[0].clone())))
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::Equal
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let stream = self.input.execute(partition, context)?;
        let schema = self.schema.clone();
        let stream = stream.map_ok(move |batch| {
            RecordBatch::try_new_with_options(
                schema.clone(),
                batch.columns().to_vec(),
                &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
            )
            .unwrap()
        });
        Ok(
            Box::pin(RecordBatchStreamAdapter::new(self.schema.clone(), stream))
                as SendableRecordBatchStream,
        )
    }

    fn partition_statistics(&self, partition: Option<usize>) -> DataFusionResult<Arc<Statistics>> {
        self.input.partition_statistics(partition)
    }

    fn supports_limit_pushdown(&self) -> bool {
        true
    }
}

#[derive(Debug)]
pub struct BaseTableAdapter {
    table: Arc<dyn BaseTable>,
    schema: Arc<ArrowSchema>,
    fts_query: Option<FullTextSearchQuery>,
    use_memwal: Option<bool>,
}

impl BaseTableAdapter {
    /// Create an adapter whose schema matches default scans, including blob descriptors.
    ///
    /// Deriving the descriptor schema does not execute a query or read blob payloads.
    pub async fn try_new(table: Arc<dyn BaseTable>) -> Result<Self> {
        let write_schema = table.schema().await?;
        let schema = Projection::full(Arc::new(LanceSchema::try_from(write_schema.as_ref())?))
            .to_arrow_schema();
        let schema = blob_nulls::read_schema(&schema).with_metadata(HashMap::default());

        Ok(Self {
            table,
            schema: Arc::new(schema),
            fts_query: None,
            use_memwal: None,
        })
    }

    /// Set the MemWAL read routing of every scan; see [`QueryRequest::use_memwal`].
    pub fn with_use_memwal(mut self, use_memwal: Option<bool>) -> Self {
        self.use_memwal = use_memwal;
        self
    }

    /// Create a new adapter with an FTS query applied.
    pub fn with_fts_query(&self, fts_query: FullTextSearchQuery) -> Self {
        // Add _score column to the schema
        let score_field = Field::new("_score", DataType::Float32, true);
        let mut fields = self.schema.fields().to_vec();
        fields.push(Arc::new(score_field));
        let schema = Arc::new(ArrowSchema::new(fields));

        Self {
            table: self.table.clone(),
            schema,
            fts_query: Some(fts_query),
            use_memwal: self.use_memwal,
        }
    }
}

#[async_trait]
impl TableProvider for BaseTableAdapter {
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
        // For FTS queries, disable auto-projection of _score to match DataFusion expectations
        let disable_scoring = self.fts_query.is_some() && projection.is_some();

        let mut query = QueryRequest {
            full_text_search: self.fts_query.clone(),
            disable_scoring_autoprojection: disable_scoring,
            use_memwal: self.use_memwal,
            ..Default::default()
        };

        if let Some(projection) = projection {
            let field_names = projection
                .iter()
                .map(|i| self.schema.field(*i).name().clone())
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
        let plan = blob_nulls::mask_plan(plan)?;
        Ok(Arc::new(MetadataEraserExec::new(plan)))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        // Lance evaluates pushed filters before the descriptor validity mask.
        // Keep predicates touching blobs above that mask, including IS NULL:
        // the stored descriptor's children are not independently nullable.
        filters
            .iter()
            .map(|filter| {
                Ok(
                    if blob_nulls::references_descriptor(filter, &self.schema)? {
                        TableProviderFilterPushDown::Unsupported
                    } else {
                        TableProviderFilterPushDown::Exact
                    },
                )
            })
            .collect()
    }

    fn statistics(&self) -> Option<Statistics> {
        // TODO
        None
    }

    async fn insert_into(
        &self,
        _state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: InsertOp,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let mode = match insert_op {
            InsertOp::Append => WriteMode::Append,
            InsertOp::Overwrite => WriteMode::Overwrite,
            InsertOp::Replace => {
                return Err(DataFusionError::NotImplemented(
                    "Replace mode is not supported for LanceDB tables".to_string(),
                ));
            }
        };

        let write_params = WriteParams {
            mode,
            ..Default::default()
        };

        self.table
            .create_insert_exec(input, write_params)
            .await
            .map_err(|e| DataFusionError::External(e.into()))
    }
}

#[cfg(test)]
pub mod tests {
    use std::{collections::HashMap, sync::Arc};

    use arrow::array::AsArray;
    use arrow_array::{
        BinaryArray, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray, UInt32Array,
    };
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::{
        datasource::provider_as_source,
        prelude::{SessionConfig, SessionContext},
    };
    use datafusion_catalog::TableProvider;
    use datafusion_common::stats::Precision;
    use datafusion_execution::SendableRecordBatchStream;
    use datafusion_expr::{LogicalPlan, LogicalPlanBuilder, col, lit};
    use futures::{StreamExt, TryStreamExt};
    use tempfile::tempdir;

    use crate::{
        connect,
        index::{Index, scalar::BTreeIndexBuilder},
        table::datafusion::BaseTableAdapter,
    };

    fn make_test_batches() -> RecordBatch {
        let metadata = HashMap::from_iter(vec![("foo".to_string(), "bar".to_string())]);
        let schema = Arc::new(
            Schema::new(vec![
                Field::new("i", DataType::Int32, false),
                Field::new("indexed", DataType::UInt32, false),
            ])
            .with_metadata(metadata),
        );
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from_iter_values(0..10)),
                Arc::new(UInt32Array::from_iter_values(0..10)),
            ],
        )
        .unwrap()
    }

    fn make_tbl_two_test_batches() -> RecordBatch {
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
        RecordBatch::try_new(
            schema,
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
        )
        .unwrap()
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
    async fn test_sql_blob_descriptor_nulls() {
        use arrow_array::{UInt8Array, UInt64Array};
        use arrow_select::concat::concat_batches;

        let tmp = tempdir().unwrap();
        let db = connect(tmp.path().to_str().unwrap())
            .execute()
            .await
            .unwrap();
        let mut blobs = lance::blob::BlobArrayBuilder::new(3);
        blobs.push_bytes(b"hello").unwrap();
        blobs.push_bytes(b"").unwrap();
        blobs.push_null().unwrap();
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                crate::blob("payload", true),
            ])),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                blobs.finish().unwrap(),
            ],
        )
        .unwrap();
        let table = db.create_table("docs", batch).execute().await.unwrap();
        let adapter = Arc::new(
            BaseTableAdapter::try_new(table.base_table().clone())
                .await
                .unwrap(),
        );
        let ctx = SessionContext::new_with_config(SessionConfig::new().with_batch_size(2));
        ctx.register_table("docs", adapter.clone()).unwrap();

        let batches = ctx
            .sql("SELECT id, payload.kind, payload.position, payload.size, payload.blob_id, payload.blob_uri FROM docs ORDER BY id")
            .await.unwrap().collect().await.unwrap();
        let batch = concat_batches(&batches[0].schema(), &batches).unwrap();
        assert_eq!(batch.num_rows(), 3);
        assert_eq!(
            batch
                .column(1)
                .as_any()
                .downcast_ref::<UInt8Array>()
                .unwrap(),
            &UInt8Array::from(vec![Some(0), Some(0), None])
        );
        assert_eq!(
            batch
                .column(3)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap(),
            &UInt64Array::from(vec![Some(5), Some(0), None])
        );
        for column in &batch.columns()[1..] {
            assert!(!column.is_null(0));
            assert!(!column.is_null(1));
            assert!(column.is_null(2));
        }

        for (predicate, expected) in [
            ("payload.size = 0", vec![2]),
            ("payload.kind = 0", vec![1, 2]),
            ("payload.blob_uri = ''", vec![1, 2]),
            ("payload.size IS NULL", vec![3]),
            ("payload.size IS NOT NULL", vec![1, 2]),
            ("payload.kind IS NULL", vec![3]),
            ("payload.position IS NULL", vec![3]),
            ("payload.blob_id IS NULL", vec![3]),
            ("payload.blob_uri IS NULL", vec![3]),
            ("payload IS NULL", vec![3]),
            ("payload.size = 0 OR payload.size IS NULL", vec![2, 3]),
            ("id > 1 AND payload.size = 0", vec![2]),
        ] {
            let batches = ctx
                .sql(&format!(
                    "SELECT id FROM docs WHERE {predicate} ORDER BY id"
                ))
                .await
                .unwrap()
                .collect()
                .await
                .unwrap();
            let ids: Vec<i32> = batches
                .iter()
                .flat_map(|batch| {
                    batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .unwrap()
                        .values()
                        .to_vec()
                })
                .collect();
            assert_eq!(ids, expected, "{predicate}");
        }

        let batches = ctx
            .sql("SELECT id FROM docs WHERE payload.size IS NULL LIMIT 1")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(
            batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            3
        );
        let batches = ctx
            .sql("SELECT COUNT(*), COUNT(payload.size) FROM docs")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(
            batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            3
        );
        assert_eq!(
            batches[0]
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            2
        );
        assert!(ctx.sql("SELECT payload.data FROM docs").await.is_err());

        // Whole-struct scans must agree with the declared schema too.
        let plan = adapter.scan(&ctx.state(), None, &[], None).await.unwrap();
        assert_eq!(plan.schema(), adapter.schema());
    }

    #[tokio::test]
    async fn test_nested_blob_sibling_filter_pushdown() {
        use arrow_array::{Array, StructArray};

        let tmp = tempdir().unwrap();
        let db = connect(tmp.path().to_str().unwrap())
            .execute()
            .await
            .unwrap();
        let mut blobs = lance::blob::BlobArrayBuilder::new(3);
        blobs.push_bytes(b"hello").unwrap();
        blobs.push_null().unwrap();
        blobs.push_bytes(b"").unwrap();
        let fields = vec![
            Arc::new(Field::new("note", DataType::Utf8, false)),
            Arc::new(crate::blob("clip", true)),
            Arc::new(Field::new("note.with.dot", DataType::Utf8, false)),
        ];
        let extra = StructArray::new(
            fields.into(),
            vec![
                Arc::new(StringArray::from(vec!["note 7", "note 8", "note 9"])),
                blobs.finish().unwrap(),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
            None,
        );
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("extra", extra.data_type().clone(), false),
            ])),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3])), Arc::new(extra)],
        )
        .unwrap();
        let table = db.create_table("docs", batch).execute().await.unwrap();
        let adapter = Arc::new(
            BaseTableAdapter::try_new(table.base_table().clone())
                .await
                .unwrap(),
        );
        let ctx = SessionContext::new();
        ctx.register_table("docs", adapter).unwrap();

        for (predicate, pushed, expected) in [
            ("extra.note = 'note 7'", true, vec![1]),
            ("extra.\"note.with.dot\" = 'b'", true, vec![2]),
            ("extra.clip.size IS NULL", false, vec![2]),
            ("get_field(extra, 'clip', 'size') = 0", false, vec![3]),
            ("extra.clip IS NULL", false, vec![2]),
            (
                "extra.note = 'note 7' OR extra.clip.size IS NULL",
                false,
                vec![1, 2],
            ),
        ] {
            let sql = format!("SELECT id FROM docs WHERE {predicate}");
            let explain = ctx
                .sql(&format!("EXPLAIN {sql}"))
                .await
                .unwrap()
                .collect()
                .await
                .unwrap();
            let explain = explain
                .iter()
                .flat_map(|batch| batch.column(1).as_string::<i32>().iter())
                .flatten()
                .collect::<Vec<_>>()
                .join("\n");
            assert_eq!(explain.contains("FilterExec"), !pushed, "{explain}");
            if pushed {
                assert!(explain.contains("full_filters=["), "{explain}");
            }
            let batches = ctx
                .sql(&format!("{sql} ORDER BY id"))
                .await
                .unwrap()
                .collect()
                .await
                .unwrap();
            let ids = batches
                .iter()
                .flat_map(|batch| {
                    batch
                        .column(0)
                        .as_primitive::<arrow_array::types::Int32Type>()
                        .values()
                        .iter()
                        .copied()
                })
                .collect::<Vec<_>>();
            assert_eq!(ids, expected, "{predicate}");
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

    /// A scan with an EMPTY projection (the shape a filtered `COUNT(*)` feeds in) yields
    /// zero-column batches. `MetadataEraserExec::execute` rebuilt each batch with
    /// `RecordBatch::try_new(schema, cols).unwrap()`; for a column-less batch that errors with
    /// "must either specify a row count or at least one column" and the `.unwrap()` panics.
    #[tokio::test]
    async fn test_metadata_eraser_empty_projection_preserves_row_count() {
        let fixture = TestFixture::new().await;

        // Empty projection => zero output columns over N rows. Table "foo" has 10 rows.
        let plan =
            LogicalPlanBuilder::scan("foo", provider_as_source(fixture.adapter), Some(vec![]))
                .unwrap()
                .build()
                .unwrap();

        let mut stream = TestFixture::plan_to_stream(plan).await;

        let mut rows = 0usize;
        while let Some(batch) = stream.try_next().await.unwrap() {
            assert_eq!(
                batch.num_columns(),
                0,
                "empty projection must yield zero columns"
            );
            rows += batch.num_rows();
        }
        assert_eq!(
            rows, 10,
            "row count must survive MetadataEraserExec on a zero-column batch"
        );

        // End-to-end SQL regression for the previous panic.
        let fixture = TestFixture::new().await;

        let ctx = SessionContext::new();
        ctx.register_table("foo", fixture.adapter.clone()).unwrap();

        let batches = ctx
            .sql("SELECT COUNT(*) FROM foo WHERE i < 5")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let count = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(count, 5, "COUNT(*) WHERE i < 5 over 0..10 must be 5");
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

    #[tokio::test]
    async fn test_count_pushdown_through_metadata_eraser() {
        let fixture = TestFixture::new().await;
        let state = datafusion::execution::SessionStateBuilder::new()
            .with_default_features()
            .with_physical_optimizer_rule(Arc::new(lance::io::exec::count_pushdown::CountPushdown))
            .build();
        let ctx = SessionContext::new_with_state(state);
        ctx.register_table("foo", fixture.adapter.clone()).unwrap();

        let df = ctx
            .sql("SELECT COUNT(*) FROM foo WHERE indexed > 2")
            .await
            .unwrap();
        let plan = df.clone().create_physical_plan().await.unwrap();
        let plan = datafusion_physical_plan::displayable(plan.as_ref())
            .indent(true)
            .to_string();
        assert!(plan.contains("CountFromMask"), "{plan}");

        let batches = df.collect().await.unwrap();
        assert_eq!(
            batches[0]
                .column(0)
                .as_primitive::<arrow_array::types::Int64Type>()
                .value(0),
            7
        );
    }

    #[tokio::test]
    async fn test_topk_late_materialization_over_table_adapter() {
        use lance::io::exec::topk_late_materialization::TopKLateMaterialization;

        let fixture = TestFixture::new().await;
        let run = |rule: Option<TopKLateMaterialization>| {
            let adapter = fixture.adapter2.clone();
            async move {
                let mut state =
                    datafusion::execution::SessionStateBuilder::new().with_default_features();
                if let Some(rule) = rule {
                    state = state.with_physical_optimizer_rule(Arc::new(rule));
                }
                let ctx = SessionContext::new_with_state(state.build());
                ctx.register_table("tbl2", adapter).unwrap();
                let df = ctx
                    .sql("SELECT ints, strings, jsons FROM tbl2 WHERE floats > 10 ORDER BY ints DESC LIMIT 5")
                    .await
                    .unwrap();
                let plan = df.clone().create_physical_plan().await.unwrap();
                let plan = datafusion_physical_plan::displayable(plan.as_ref())
                    .indent(true)
                    .to_string();
                (plan, df.collect().await.unwrap())
            }
        };

        let (plan, expected) = run(None).await;
        assert!(!plan.contains("Take"), "{plan}");

        // The sort lands below `MetadataEraserExec`, so the rule needs no
        // certification for it.
        let (plan, batches) = run(Some(TopKLateMaterialization::new())).await;
        assert!(plan.contains("Take"), "{plan}");
        assert_eq!(batches, expected);
        assert!(batches[0].schema().metadata().is_empty());
    }

    #[tokio::test]
    async fn test_metadata_eraser_with_new_children_takes_child_schema() {
        let fixture = TestFixture::new().await;
        let ctx = SessionContext::new();
        let scan = |adapter: Arc<BaseTableAdapter>, name: &str| {
            LogicalPlanBuilder::scan(name, provider_as_source(adapter), None)
                .unwrap()
                .build()
                .unwrap()
        };
        let foo = ctx
            .state()
            .create_physical_plan(&scan(fixture.adapter.clone(), "foo"))
            .await
            .unwrap();
        let tbl2 = ctx
            .state()
            .create_physical_plan(&scan(fixture.adapter2.clone(), "tbl2"))
            .await
            .unwrap();

        let rewired = foo.with_new_children(vec![tbl2.clone()]).unwrap();
        assert_eq!(rewired.schema(), tbl2.schema());
    }
}
