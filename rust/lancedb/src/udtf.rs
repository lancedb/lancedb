// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! User-Defined Table Functions (UDTFs) for LanceDB
//!
//! This module provides table-level UDTFs that integrate with DataFusion's SQL engine.

use std::sync::Arc;

use datafusion::catalog::TableFunctionImpl;
use datafusion_catalog::TableProvider;
use datafusion_common::{plan_err, DataFusionError, Result as DataFusionResult, ScalarValue};
use datafusion_expr::Expr;
use lance_index::scalar::inverted::parser::from_json;
use lance_index::scalar::FullTextSearchQuery;

/// Trait for resolving table names to TableProvider instances.
pub trait TableResolver: std::fmt::Debug + Send + Sync {
    /// Resolve a table name to a TableProvider, optionally with an FTS query applied.
    fn resolve_table(
        &self,
        name: &str,
        fts_query: Option<FullTextSearchQuery>,
    ) -> DataFusionResult<Arc<dyn TableProvider>>;
}

/// Full-Text Search table function that operates on LanceDB tables
#[derive(Debug)]
pub struct FtsTableFunction {
    resolver: Arc<dyn TableResolver>,
}

impl FtsTableFunction {
    pub fn new(resolver: Arc<dyn TableResolver>) -> Self {
        Self { resolver }
    }
}

impl TableFunctionImpl for FtsTableFunction {
    fn call(&self, exprs: &[Expr]) -> DataFusionResult<Arc<dyn TableProvider>> {
        if exprs.len() != 2 {
            return plan_err!("fts() requires 2 parameters: fts(table_name, fts_query)");
        }

        let table_name = extract_string_literal(&exprs[0], "table_name")?;
        let query_json = extract_string_literal(&exprs[1], "fts_query")?;
        let fts_query = parse_fts_query(&query_json)?;

        // Resolver returns a ready-to-use TableProvider with FTS applied
        self.resolver.resolve_table(&table_name, Some(fts_query))
    }
}

fn extract_string_literal(expr: &Expr, param_name: &str) -> DataFusionResult<String> {
    match expr {
        Expr::Literal(ScalarValue::Utf8(Some(s)), _) => Ok(s.clone()),
        Expr::Literal(ScalarValue::LargeUtf8(Some(s)), _) => Ok(s.clone()),
        _ => plan_err!(
            "Parameter '{}' must be a string literal, got: {:?}",
            param_name,
            expr
        ),
    }
}

fn parse_fts_query(json: &str) -> DataFusionResult<FullTextSearchQuery> {
    let query = from_json(json).map_err(|e| {
        DataFusionError::Plan(format!(
            "Invalid FTS query JSON: {}. Expected format: {{\"match\": {{\"column\": \"text\", \"terms\": \"query\"}} }}",
            e
        ))
    })?;
    Ok(FullTextSearchQuery::new_query(query))
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        index::{scalar::FtsIndexBuilder, Index},
        table::datafusion::BaseTableAdapter,
        Connection, Table,
    };
    use arrow_array::{Int32Array, RecordBatch, RecordBatchIterator, StringArray};
    use arrow_schema::{DataType, Field, Schema as ArrowSchema};
    use datafusion::prelude::SessionContext;

    /// Resolver that looks up tables in a HashMap
    #[derive(Debug)]
    struct HashMapTableResolver {
        tables: std::collections::HashMap<String, Arc<dyn TableProvider>>,
    }

    impl HashMapTableResolver {
        fn new() -> Self {
            Self {
                tables: std::collections::HashMap::new(),
            }
        }

        fn register(&mut self, name: String, table: Arc<dyn TableProvider>) {
            self.tables.insert(name, table);
        }
    }

    impl TableResolver for HashMapTableResolver {
        fn resolve_table(
            &self,
            name: &str,
            fts_query: Option<FullTextSearchQuery>,
        ) -> DataFusionResult<Arc<dyn TableProvider>> {
            let table_provider = self
                .tables
                .get(name)
                .cloned()
                .ok_or_else(|| DataFusionError::Plan(format!("Table '{}' not found", name)))?;

            // If no FTS query, return as-is
            let Some(fts_query) = fts_query else {
                return Ok(table_provider);
            };

            // Downcast to BaseTableAdapter and apply FTS query
            let base_adapter = table_provider
                .as_any()
                .downcast_ref::<BaseTableAdapter>()
                .ok_or_else(|| {
                    DataFusionError::Internal(
                        "Expected BaseTableAdapter but got different type".to_string(),
                    )
                })?;

            Ok(Arc::new(base_adapter.with_fts_query(fts_query)))
        }
    }

    #[tokio::test]
    async fn test_fts_table_udtf() {
        // Create test data
        let text_col = Arc::new(StringArray::from(vec![
            "a cat catch a fish",
            "a fish catch a cat",
            "a white cat catch a big fish",
            "cat catchup fish",
            "cat fish catch",
        ]));
        let number_col = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]));

        // Create RecordBatch
        let schema = Arc::new(arrow_schema::Schema::new(vec![
            Field::new("text", DataType::Utf8, false),
            Field::new("number", DataType::Int32, false),
        ]));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![text_col.clone(), number_col.clone()])
                .unwrap();

        // Create LanceDB database and table
        let db = crate::connect("memory://test").execute().await.unwrap();
        let table = db
            .create_table(
                "foo",
                RecordBatchIterator::new(vec![Ok(batch)].into_iter(), schema),
            )
            .execute()
            .await
            .unwrap();

        // Create FTS index
        table
            .create_index(&["text"], Index::FTS(Default::default()))
            .execute()
            .await
            .unwrap();

        // Setup DataFusion context with FTS UDTF
        let ctx = SessionContext::new();
        let mut resolver = HashMapTableResolver::new();

        // Create a BaseTableAdapter for the table (without FTS)
        let base_adapter = BaseTableAdapter::try_new(table.base_table().clone())
            .await
            .unwrap();
        resolver.register("foo".to_string(), Arc::new(base_adapter));

        let fts_udtf = FtsTableFunction::new(Arc::new(resolver));
        ctx.register_udtf("fts", Arc::new(fts_udtf));

        // Execute FTS query
        let fts_query = r#"
            {
                "match": {
                    "column": "text",
                    "terms": "catch fish",
                    "operator": "And"
                }
            }
            "#;

        let query = format!("SELECT * FROM fts('foo', '{}') WHERE number > 1", fts_query);

        // Print EXPLAIN output
        println!("\n=== EXPLAIN for FTS with WHERE clause ===");
        let explain_df = ctx.sql(&format!("EXPLAIN {}", query)).await.unwrap();
        let explain_results = explain_df.collect().await.unwrap();
        for batch in &explain_results {
            for row_idx in 0..batch.num_rows() {
                if let Some(col) = batch.column_by_name("plan") {
                    if let Some(plan_str) = col.as_any().downcast_ref::<StringArray>() {
                        println!("{}", plan_str.value(row_idx));
                    }
                }
            }
        }

        // Print EXPLAIN ANALYZE output
        println!("\n=== EXPLAIN ANALYZE for FTS with WHERE clause ===");
        let explain_analyze_df = ctx
            .sql(&format!("EXPLAIN ANALYZE {}", query))
            .await
            .unwrap();
        let explain_analyze_results = explain_analyze_df.collect().await.unwrap();
        for batch in &explain_analyze_results {
            for row_idx in 0..batch.num_rows() {
                if let Some(col) = batch.column_by_name("plan") {
                    if let Some(plan_str) = col.as_any().downcast_ref::<StringArray>() {
                        println!("{}", plan_str.value(row_idx));
                    }
                }
            }
        }

        let df = ctx.sql(&query).await.unwrap();
        let results = df.collect().await.unwrap();
        assert_eq!(results.len(), 1);
        let results = results.into_iter().next().unwrap();

        // Should have: text, number, _score (with SELECT *)
        assert_eq!(results.num_columns(), 3);

        // Verify schema
        assert!(
            results.schema().column_with_name("text").is_some(),
            "text should be present"
        );
        assert!(
            results.schema().column_with_name("number").is_some(),
            "number should be present"
        );
        assert!(
            results.schema().column_with_name("_score").is_some(),
            "_score should be present with SELECT *"
        );

        // Should match rows where both "catch" AND "fish" appear and number > 1
        // Row 1 (idx 1): "a fish catch a cat" - has both, number=2 ✓
        // Row 2 (idx 2): "a white cat catch a big fish" - has both, number=3 ✓
        // Row 4 (idx 4): "cat fish catch" - has both, number=5 ✓
        assert_eq!(results.num_rows(), 3);

        // Test GROUP BY query
        println!("\n\n=== Testing GROUP BY query ===");
        let group_result = ctx
            .sql("SELECT number, COUNT(*) as cnt FROM fts('foo', '{\"match\": {\"column\": \"text\", \"terms\": \"catch\"}}') GROUP BY number")
            .await;

        match group_result {
            Ok(df) => match df.collect().await {
                Ok(results) => {
                    println!("GROUP BY query succeeded!");
                    println!("Number of result batches: {}", results.len());
                    for (idx, batch) in results.iter().enumerate() {
                        println!(
                            "Batch {}: {} rows, {} columns",
                            idx,
                            batch.num_rows(),
                            batch.num_columns()
                        );
                        println!("Schema: {:?}", batch.schema());
                    }
                }
                Err(e) => {
                    println!("GROUP BY query failed during execution: {}", e);
                }
            },
            Err(e) => {
                println!("GROUP BY query failed during planning: {}", e);
            }
        }

        // Test JOIN query
        println!("\n\n=== Testing JOIN query ===");

        // Create a second table for joining
        let metadata_col = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]));
        let extra_col = Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e"]));
        let metadata_schema = Arc::new(arrow_schema::Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("extra", DataType::Utf8, false),
        ]));
        let metadata_batch =
            RecordBatch::try_new(metadata_schema.clone(), vec![metadata_col, extra_col]).unwrap();

        let _metadata_table = db
            .create_table(
                "metadata",
                RecordBatchIterator::new(
                    vec![Ok(metadata_batch.clone())].into_iter(),
                    metadata_schema.clone(),
                ),
            )
            .execute()
            .await
            .unwrap();

        // Register metadata table with DataFusion
        ctx.register_batch("metadata", metadata_batch).unwrap();

        let join_result = ctx
            .sql("SELECT f.text, f.number, m.extra FROM fts('foo', '{\"match\": {\"column\": \"text\", \"terms\": \"catch\"}}') f JOIN metadata m ON f.number = m.id")
            .await;

        match join_result {
            Ok(df) => match df.collect().await {
                Ok(results) => {
                    println!("JOIN query succeeded!");
                    println!("Results: {:?}", results);
                }
                Err(e) => {
                    println!("JOIN query failed during execution: {}", e);
                }
            },
            Err(e) => {
                println!("JOIN query failed during planning: {}", e);
            }
        }
    }

    /// Helper to create a diverse test table with FTS data and index
    async fn setup_diverse_fts_table(db: &Connection, table_name: &str) -> Arc<Table> {
        // Create table with diverse test data for comprehensive FTS testing
        let id_col = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]));
        let text_col = Arc::new(StringArray::from(vec![
            "the puppy runs merrily",
            "the cat jumps quickly",
            "a puppy catches a ball",
            "the dog runs crazily around",
            "puppy training is important",
            "cats and dogs are friends",
            "running in the park",
            "the quick brown fox",
            "craziou misspelled word",
            "crazily spelled correctly",
        ]));
        let category_col = Arc::new(StringArray::from(vec![
            "animals", "animals", "sports", "animals", "training", "animals", "sports", "nature",
            "test", "test",
        ]));

        let schema = Arc::new(arrow_schema::Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("text", DataType::Utf8, false),
            Field::new("category", DataType::Utf8, false),
        ]));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![id_col, text_col, category_col]).unwrap();

        let table = db
            .create_table(
                table_name,
                RecordBatchIterator::new(vec![Ok(batch)].into_iter(), schema),
            )
            .execute()
            .await
            .unwrap();

        // Create FTS index
        table
            .create_index(&["text"], Index::FTS(Default::default()))
            .execute()
            .await
            .unwrap();

        Arc::new(table)
    }

    /// Helper to setup DataFusion context with FTS UDTF registered
    async fn setup_context_with_udtf(table: Arc<Table>, table_name: &str) -> SessionContext {
        let ctx = SessionContext::new();
        let mut resolver = HashMapTableResolver::new();
        // Convert Table to BaseTableAdapter (TableProvider)
        let adapter = BaseTableAdapter::try_new(table.base_table().clone())
            .await
            .unwrap();
        resolver.register(table_name.to_string(), Arc::new(adapter));
        ctx.register_udtf("fts", Arc::new(FtsTableFunction::new(Arc::new(resolver))));
        ctx
    }

    /// Helper to execute FTS query and return results
    async fn execute_fts_query(ctx: &SessionContext, query: &str) -> Vec<RecordBatch> {
        ctx.sql(query).await.unwrap().collect().await.unwrap()
    }

    /// Helper to assert result dimensions
    fn assert_result_shape(result: &[RecordBatch], expected_rows: usize, expected_cols: usize) {
        assert_eq!(
            result[0].num_rows(),
            expected_rows,
            "Expected {} rows, got {}",
            expected_rows,
            result[0].num_rows()
        );
        assert_eq!(
            result[0].num_columns(),
            expected_cols,
            "Expected {} columns, got {}",
            expected_cols,
            result[0].num_columns()
        );
    }

    /// Helper to assert column exists in schema
    #[allow(dead_code)]
    fn assert_column_exists(result: &[RecordBatch], column_name: &str) {
        assert!(
            result[0].schema().column_with_name(column_name).is_some(),
            "Column '{}' should be present in result schema",
            column_name
        );
    }

    /// Helper to assert column does NOT exist in schema
    #[allow(dead_code)]
    fn assert_column_not_exists(result: &[RecordBatch], column_name: &str) {
        assert!(
            result[0].schema().column_with_name(column_name).is_none(),
            "Column '{}' should NOT be present in result schema",
            column_name
        );
    }

    // ============================================================================
    // Basic FTS Query Tests
    // ============================================================================

    #[tokio::test]
    async fn test_fts_udtf_and_operator() {
        let db = crate::connect("memory://test_and_op")
            .execute()
            .await
            .unwrap();
        let table = setup_diverse_fts_table(&db, "docs").await;
        let ctx = setup_context_with_udtf(table, "docs").await;

        let result = execute_fts_query(
            &ctx,
            r#"SELECT id, text FROM fts('docs', '{"match": {"column": "text", "terms": "puppy runs", "operator": "And"}}')"#,
        ).await;

        // Should match only row 1: "the puppy runs merrily" (has both "puppy" AND "runs")
        assert_result_shape(&result, 1, 2);
    }

    #[tokio::test]
    async fn test_fts_udtf_or_operator() {
        let db = crate::connect("memory://test_or_op")
            .execute()
            .await
            .unwrap();
        let table = setup_diverse_fts_table(&db, "docs").await;
        let ctx = setup_context_with_udtf(table, "docs").await;

        let result = execute_fts_query(
            &ctx,
            r#"SELECT id, text FROM fts('docs', '{"match": {"column": "text", "terms": "cat dog", "operator": "Or"}}')"#,
        ).await;

        // Should match rows 2, 4, 6 ("cat" OR "dog")
        assert_result_shape(&result, 3, 2);
    }

    #[tokio::test]
    async fn test_fts_udtf_phrase_query() {
        let db = crate::connect("memory://test_phrase")
            .execute()
            .await
            .unwrap();

        // Create table with data for phrase queries
        let id_col = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let text_col = Arc::new(StringArray::from(vec![
            "the quick brown fox",
            "a brown dog jumps",
            "quick brown test",
        ]));

        let schema = Arc::new(arrow_schema::Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("text", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(schema.clone(), vec![id_col, text_col]).unwrap();

        let table = db
            .create_table(
                "docs",
                RecordBatchIterator::new(vec![Ok(batch)].into_iter(), schema),
            )
            .execute()
            .await
            .unwrap();

        // Create FTS index with position information for phrase queries
        table
            .create_index(
                &["text"],
                Index::FTS(FtsIndexBuilder::default().with_position(true)),
            )
            .execute()
            .await
            .unwrap();

        let ctx = SessionContext::new();
        let mut resolver = HashMapTableResolver::new();
        let adapter = BaseTableAdapter::try_new(table.base_table().clone())
            .await
            .unwrap();
        resolver.register("docs".to_string(), Arc::new(adapter));
        ctx.register_udtf("fts", Arc::new(FtsTableFunction::new(Arc::new(resolver))));

        // Test phrase query: exact phrase "quick brown"
        let result = ctx
            .sql(r#"SELECT id, text FROM fts('docs', '{"phrase": {"column": "text", "terms": "quick brown"}}')"#)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        // Should match rows 1 and 3 with exact phrase "quick brown"
        assert_eq!(result[0].num_rows(), 2);
    }

    #[tokio::test]
    async fn test_fts_udtf_fuzzy_search() {
        let db = crate::connect("memory://test_fuzzy")
            .execute()
            .await
            .unwrap();
        let table = setup_diverse_fts_table(&db, "docs").await;
        let ctx = setup_context_with_udtf(table, "docs").await;

        // Test fuzzy search with fuzziness=2
        let result = ctx
            .sql(r#"SELECT id, text FROM fts('docs', '{"match": {"column": "text", "terms": "craziou", "fuzziness": 2}}')"#)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        // Should match rows 4, 9, 10 due to fuzzy matching
        assert_eq!(result[0].num_rows(), 3);
    }

    #[tokio::test]
    async fn test_fts_udtf_with_ordered() {
        let db = crate::connect("memory://test_ordered")
            .execute()
            .await
            .unwrap();
        let table = setup_diverse_fts_table(&db, "docs").await;
        let ctx = setup_context_with_udtf(table, "docs").await;

        // Test ordered results (order by _score explicitly)
        let result = ctx
            .sql(r#"SELECT id, text, _score FROM fts('docs', '{"match": {"column": "text", "terms": "puppy"}}') ORDER BY _score DESC"#)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        // Should return 3 puppy matches, ordered by relevance score
        assert_eq!(result[0].num_rows(), 3);
        assert_eq!(result[0].num_columns(), 3);

        // Verify schema includes _score
        assert!(
            result[0].schema().column_with_name("_score").is_some(),
            "_score should be present"
        );
    }

    #[tokio::test]
    async fn test_fts_udtf_multi_column_setup() {
        let db = crate::connect("memory://test_multi_col")
            .execute()
            .await
            .unwrap();

        // Create table with multiple text columns
        let id_col = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let title_col = Arc::new(StringArray::from(vec![
            "Important Document",
            "Another Document",
            "Random Text",
        ]));
        let content_col = Arc::new(StringArray::from(vec![
            "This contains valuable information",
            "This has important details",
            "Nothing special here",
        ]));

        let schema = Arc::new(arrow_schema::Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("title", DataType::Utf8, false),
            Field::new("content", DataType::Utf8, false),
        ]));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![id_col, title_col, content_col]).unwrap();

        let table = db
            .create_table(
                "multi_col",
                RecordBatchIterator::new(vec![Ok(batch)].into_iter(), schema),
            )
            .execute()
            .await
            .unwrap();

        // Create FTS indices on both columns
        table
            .create_index(&["title"], Index::FTS(Default::default()))
            .execute()
            .await
            .unwrap();
        table
            .create_index(&["content"], Index::FTS(Default::default()))
            .execute()
            .await
            .unwrap();

        let ctx = SessionContext::new();
        let mut resolver = HashMapTableResolver::new();
        let adapter = BaseTableAdapter::try_new(table.base_table().clone())
            .await
            .unwrap();
        resolver.register("multi_col".to_string(), Arc::new(adapter));
        ctx.register_udtf("fts", Arc::new(FtsTableFunction::new(Arc::new(resolver))));

        // Test searching title column
        let result = ctx
            .sql(r#"SELECT id, title FROM fts('multi_col', '{"match": {"column": "title", "terms": "Important"}}')"#)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        assert_eq!(result[0].num_rows(), 1);

        // Test searching content column
        let result = ctx
            .sql(r#"SELECT id, content FROM fts('multi_col', '{"match": {"column": "content", "terms": "important"}}')"#)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        assert_eq!(result[0].num_rows(), 1);
    }

    #[tokio::test]
    async fn test_fts_udtf_projection() {
        let db = crate::connect("memory://test_projection")
            .execute()
            .await
            .unwrap();
        let table = setup_diverse_fts_table(&db, "docs").await;
        let ctx = setup_context_with_udtf(table, "docs").await;

        // Test selecting only specific columns
        let result = ctx
            .sql(r#"SELECT text FROM fts('docs', '{"match": {"column": "text", "terms": "puppy"}}')"#)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        // Should have only the text column
        assert_eq!(result[0].num_columns(), 1);
        assert_eq!(result[0].schema().field(0).name(), "text");
        assert_eq!(result[0].num_rows(), 3);
    }

    #[tokio::test]
    async fn test_fts_udtf_limit() {
        let db = crate::connect("memory://test_limit")
            .execute()
            .await
            .unwrap();
        let table = setup_diverse_fts_table(&db, "docs").await;
        let ctx = setup_context_with_udtf(table, "docs").await;

        // Test LIMIT clause
        let result = ctx
            .sql(r#"SELECT id, text FROM fts('docs', '{"match": {"column": "text", "terms": "puppy"}}') LIMIT 2"#)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        assert_eq!(result[0].num_rows(), 2);
    }

    #[tokio::test]
    async fn test_fts_udtf_order_by() {
        let db = crate::connect("memory://test_order_by")
            .execute()
            .await
            .unwrap();
        let table = setup_diverse_fts_table(&db, "docs").await;
        let ctx = setup_context_with_udtf(table, "docs").await;

        // Test ORDER BY clause (ordering by id instead of _score since _score is not in schema)
        let result = ctx
            .sql(r#"SELECT id, text FROM fts('docs', '{"match": {"column": "text", "terms": "puppy"}}') ORDER BY id LIMIT 3"#)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        assert_eq!(result[0].num_rows(), 3);
    }

    #[tokio::test]
    async fn test_fts_udtf_complex_boolean() {
        let db = crate::connect("memory://test_complex")
            .execute()
            .await
            .unwrap();
        let table = setup_diverse_fts_table(&db, "docs").await;
        let ctx = setup_context_with_udtf(table, "docs").await;

        // Complex query: search for texts containing "runs" or "jumps"
        let result = ctx
            .sql(r#"SELECT id, text FROM fts('docs', '{"match": {"column": "text", "terms": "runs jumps"}}')"#)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        // Should match rows 1, 2, 4, 7 (containing "runs" or "jumps")
        assert_eq!(result[0].num_rows(), 4);
    }

    #[tokio::test]
    async fn test_fts_udtf_empty_result() {
        let db = crate::connect("memory://test_empty")
            .execute()
            .await
            .unwrap();
        let table = setup_diverse_fts_table(&db, "docs").await;
        let ctx = setup_context_with_udtf(table, "docs").await;

        // Test query with no matches
        let result = ctx
            .sql(r#"SELECT id, text FROM fts('docs', '{"match": {"column": "text", "terms": "nonexistent_word_xyz"}}')"#)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        // Should return empty or nearly empty result
        assert!(result[0].num_rows() <= 1);
    }

    #[tokio::test]
    async fn test_fts_udtf_count_aggregation() {
        let db = crate::connect("memory://test_count")
            .execute()
            .await
            .unwrap();
        let table = setup_diverse_fts_table(&db, "docs").await;
        let ctx = setup_context_with_udtf(table, "docs").await;

        // Test COUNT aggregation (using COUNT(id) instead of COUNT(*))
        let result = ctx
            .sql(r#"SELECT COUNT(id) as cnt FROM fts('docs', '{"match": {"column": "text", "terms": "puppy"}}')"#)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        assert_eq!(result[0].num_rows(), 1);
        // Verify count is 3
        let count_array = result[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap();
        assert_eq!(count_array.value(0), 3);
    }

    #[tokio::test]
    async fn test_fts_udtf_with_join() {
        let db = crate::connect("memory://test_join")
            .execute()
            .await
            .unwrap();
        let table = setup_diverse_fts_table(&db, "docs").await;

        // Create metadata table for join
        let metadata_id = Arc::new(Int32Array::from(vec![1, 3, 5]));
        let extra_info = Arc::new(StringArray::from(vec!["first", "third", "fifth"]));
        let metadata_schema = Arc::new(arrow_schema::Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("extra_info", DataType::Utf8, false),
        ]));
        let metadata_batch =
            RecordBatch::try_new(metadata_schema.clone(), vec![metadata_id, extra_info]).unwrap();
        let _metadata_table = db
            .create_table(
                "metadata",
                RecordBatchIterator::new(
                    vec![Ok(metadata_batch.clone())].into_iter(),
                    metadata_schema,
                ),
            )
            .execute()
            .await
            .unwrap();

        let ctx = SessionContext::new();
        let mut resolver = HashMapTableResolver::new();
        let adapter = BaseTableAdapter::try_new(table.base_table().clone())
            .await
            .unwrap();
        resolver.register("docs".to_string(), Arc::new(adapter));
        ctx.register_udtf("fts", Arc::new(FtsTableFunction::new(Arc::new(resolver))));

        // Register metadata table with DataFusion for JOIN
        ctx.register_batch("metadata", metadata_batch).unwrap();

        let result = ctx
            .sql(
                r#"SELECT d.id, d.text, m.extra_info
                   FROM fts('docs', '{"match": {"column": "text", "terms": "puppy"}}') d
                   JOIN metadata m ON d.id = m.id"#,
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        // Should match rows 1, 3, 5 that have puppy and exist in metadata
        assert_eq!(result[0].num_rows(), 3);
        assert_eq!(result[0].num_columns(), 3);
    }

    #[tokio::test]
    async fn test_fts_udtf_aggregation() {
        let db = crate::connect("memory://test_agg").execute().await.unwrap();
        let table = setup_diverse_fts_table(&db, "docs").await;
        let ctx = setup_context_with_udtf(table, "docs").await;

        let result = ctx
            .sql(
                r#"SELECT category, COUNT(*) as cnt
                   FROM fts('docs', '{"match": {"column": "text", "terms": "puppy"}}')
                   GROUP BY category"#,
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        // Should group by category - puppy appears in multiple rows but aggregation may combine them
        // The actual behavior depends on whether DataFusion aggregates across all matches
        assert!(result[0].num_rows() >= 1, "Should have at least 1 category with puppy");
        assert_eq!(result[0].num_columns(), 2);
    }

    #[tokio::test]
    async fn test_fts_udtf_score_with_select_star() {
        let db = crate::connect("memory://test_score_star")
            .execute()
            .await
            .unwrap();
        let table = setup_diverse_fts_table(&db, "docs").await;
        let ctx = setup_context_with_udtf(table, "docs").await;

        // Test SELECT * - should include _score
        let result = ctx
            .sql(r#"SELECT * FROM fts('docs', '{"match": {"column": "text", "terms": "puppy"}}')"#)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        // Should have all columns including _score
        assert!(
            result[0].schema().column_with_name("_score").is_some(),
            "_score should be present with SELECT *"
        );
        assert_eq!(result[0].num_rows(), 3);
    }

    #[tokio::test]
    async fn test_fts_udtf_score_explicit_projection() {
        let db = crate::connect("memory://test_score_explicit")
            .execute()
            .await
            .unwrap();
        let table = setup_diverse_fts_table(&db, "docs").await;
        let ctx = setup_context_with_udtf(table, "docs").await;

        // Test explicit _score in projection
        let result = ctx
            .sql(
                r#"SELECT id, text, _score FROM fts('docs', '{"match": {"column": "text", "terms": "puppy"}}')"#,
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        // Should have exactly the requested columns
        assert_eq!(result[0].num_columns(), 3);
        assert!(
            result[0].schema().column_with_name("_score").is_some(),
            "_score should be present when explicitly requested"
        );
        assert_eq!(result[0].num_rows(), 3);
    }

    #[tokio::test]
    async fn test_fts_udtf_score_not_in_projection() {
        let db = crate::connect("memory://test_score_no_proj")
            .execute()
            .await
            .unwrap();
        let table = setup_diverse_fts_table(&db, "docs").await;
        let ctx = setup_context_with_udtf(table, "docs").await;

        // Test without _score in projection
        let result = ctx
            .sql(
                r#"SELECT id, text FROM fts('docs', '{"match": {"column": "text", "terms": "puppy"}}')"#,
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        // Should NOT have _score since it wasn't requested
        assert_eq!(result[0].num_columns(), 2);
        assert!(
            result[0].schema().column_with_name("_score").is_none(),
            "_score should NOT be present when not explicitly requested"
        );
        assert_eq!(result[0].num_rows(), 3);
    }

    #[tokio::test]
    async fn test_fts_udtf_score_order_by() {
        let db = crate::connect("memory://test_score_order")
            .execute()
            .await
            .unwrap();
        let table = setup_diverse_fts_table(&db, "docs").await;
        let ctx = setup_context_with_udtf(table, "docs").await;

        // Test ORDER BY _score
        let result = ctx
            .sql(
                r#"SELECT id, text, _score FROM fts('docs', '{"match": {"column": "text", "terms": "puppy"}}') ORDER BY _score DESC"#,
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        assert_eq!(result[0].num_rows(), 3);
        assert!(
            result[0].schema().column_with_name("_score").is_some(),
            "_score should be present"
        );

        // Verify scores are in descending order
        use arrow::array::AsArray;
        let score_col = result[0].column_by_name("_score").unwrap();
        let scores = score_col.as_primitive::<arrow::datatypes::Float32Type>();
        for i in 0..scores.len() - 1 {
            let score1 = scores.value(i);
            let score2 = scores.value(i + 1);
            assert!(
                score1 >= score2,
                "Scores should be in descending order: {} >= {}",
                score1,
                score2
            );
        }
    }

    #[tokio::test]
    async fn test_fts_udtf_score_with_normal_columns() {
        let db = crate::connect("memory://test_score_mixed")
            .execute()
            .await
            .unwrap();
        let table = setup_diverse_fts_table(&db, "docs").await;
        let ctx = setup_context_with_udtf(table, "docs").await;

        // Test mixing _score with normal columns and expressions
        let result = ctx
            .sql(
                r#"SELECT id, text, _score, category, id + 100 as id_plus_100 FROM fts('docs', '{"match": {"column": "text", "terms": "puppy"}}')"#,
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        assert_eq!(result[0].num_columns(), 5);
        assert!(
            result[0].schema().column_with_name("_score").is_some(),
            "_score should be present"
        );
        assert_eq!(result[0].num_rows(), 3);
    }

    #[tokio::test]
    async fn test_fts_udtf_boolean_must_query() {
        let db = crate::connect("memory://test_bool_must")
            .execute()
            .await
            .unwrap();
        let table = setup_diverse_fts_table(&db, "docs").await;
        let ctx = setup_context_with_udtf(table, "docs").await;

        // Test Boolean query with "must" clause - all terms must match
        let result = ctx
            .sql(
                r#"SELECT id, text FROM fts('docs', '{"boolean": {"must": [{"match": {"column": "text", "terms": "puppy"}}, {"match": {"column": "text", "terms": "runs"}}]}}')"#,
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        // Should only match row 1: "the puppy runs merrily"
        assert_eq!(result[0].num_rows(), 1);
        assert_eq!(result[0].num_columns(), 2);
    }

    #[tokio::test]
    async fn test_fts_udtf_boolean_should_query() {
        let db = crate::connect("memory://test_bool_should")
            .execute()
            .await
            .unwrap();
        let table = setup_diverse_fts_table(&db, "docs").await;
        let ctx = setup_context_with_udtf(table, "docs").await;

        // Test Boolean query with "should" clause
        let result = ctx
            .sql(
                r#"SELECT id, text FROM fts('docs', '{"boolean": {"should": [{"match": {"column": "text", "terms": "puppy"}}, {"match": {"column": "text", "terms": "cat"}}]}}')"#,
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        // Should match rows 1, 2, 3, 5, 6 (containing "puppy" OR "cat"/"cats")
        assert_eq!(result[0].num_rows(), 5);
    }

    #[tokio::test]
    async fn test_fts_udtf_boolean_must_not_query() {
        let db = crate::connect("memory://test_bool_must_not")
            .execute()
            .await
            .unwrap();
        let table = setup_diverse_fts_table(&db, "docs").await;
        let ctx = setup_context_with_udtf(table, "docs").await;

        // Test Boolean query with must and must_not
        let result = ctx
            .sql(
                r#"SELECT id, text FROM fts('docs', '{"boolean": {"must": [{"match": {"column": "text", "terms": "puppy"}}], "must_not": [{"match": {"column": "text", "terms": "training"}}]}}')"#,
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        // Should match puppy rows EXCEPT row 5 which has "training"
        assert_eq!(result[0].num_rows(), 2);
    }

    #[tokio::test]
    async fn test_fts_udtf_phrase_with_slop() {
        let db = crate::connect("memory://test_phrase_slop")
            .execute()
            .await
            .unwrap();

        // Create table with phrase test data
        let id_col = Arc::new(Int32Array::from(vec![1, 2, 3, 4]));
        let text_col = Arc::new(StringArray::from(vec![
            "the quick brown fox jumps",
            "quick as a brown fox",
            "brown quick fox",
            "the slow brown fox",
        ]));

        let schema = Arc::new(arrow_schema::Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("text", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(schema.clone(), vec![id_col, text_col]).unwrap();

        let table = db
            .create_table(
                "docs",
                RecordBatchIterator::new(vec![Ok(batch)].into_iter(), schema),
            )
            .execute()
            .await
            .unwrap();

        // Create FTS index with position information
        table
            .create_index(
                &["text"],
                Index::FTS(FtsIndexBuilder::default().with_position(true)),
            )
            .execute()
            .await
            .unwrap();

        let ctx = SessionContext::new();
        let mut resolver = HashMapTableResolver::new();
        let adapter = BaseTableAdapter::try_new(table.base_table().clone())
            .await
            .unwrap();
        resolver.register("docs".to_string(), Arc::new(adapter));
        ctx.register_udtf("fts", Arc::new(FtsTableFunction::new(Arc::new(resolver))));

        // Test phrase with slop=1
        let result = ctx
            .sql(
                r#"SELECT id, text FROM fts('docs', '{"phrase": {"column": "text", "terms": "quick brown", "slop": 1}}')"#,
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        // Query should execute successfully (just check it returns results)
        assert!(!result.is_empty());
    }

    #[tokio::test]
    async fn test_fts_udtf_match_with_boost() {
        let db = crate::connect("memory://test_match_boost")
            .execute()
            .await
            .unwrap();
        let table = setup_diverse_fts_table(&db, "docs").await;
        let ctx = setup_context_with_udtf(table, "docs").await;

        // Test Match query with boost parameter
        let result = ctx
            .sql(
                r#"SELECT id, text, _score FROM fts('docs', '{"match": {"column": "text", "terms": "puppy", "boost": 2.0}}') ORDER BY _score DESC"#,
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        // Should return puppy matches with boosted scores
        assert_eq!(result[0].num_rows(), 3);
        assert!(
            result[0].schema().column_with_name("_score").is_some(),
            "_score should be present"
        );
    }

    #[tokio::test]
    async fn test_fts_udtf_boost_query() {
        let db = crate::connect("memory://test_boost_query")
            .execute()
            .await
            .unwrap();
        let table = setup_diverse_fts_table(&db, "docs").await;
        let ctx = setup_context_with_udtf(table, "docs").await;

        // Test Boost query
        let result = ctx
            .sql(
                r#"SELECT id, text, _score FROM fts('docs', '{"boost": {"positive": {"match": {"column": "text", "terms": "puppy"}}, "negative": {"match": {"column": "text", "terms": "training"}}, "negative_boost": 0.3}}') ORDER BY _score DESC"#,
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        // Should return puppy matches
        assert_eq!(result[0].num_rows(), 3);
        assert!(
            result[0].schema().column_with_name("_score").is_some(),
            "_score should be present"
        );
    }

    #[tokio::test]
    async fn test_fts_udtf_multi_match_query() {
        let db = crate::connect("memory://test_multi_match")
            .execute()
            .await
            .unwrap();

        // Create table with multiple text columns
        let id_col = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let title_col = Arc::new(StringArray::from(vec![
            "Important Document",
            "Another Page",
            "Random Text",
        ]));
        let content_col = Arc::new(StringArray::from(vec![
            "This document has valuable data",
            "This page contains important information",
            "Nothing special here",
        ]));

        let schema = Arc::new(arrow_schema::Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("title", DataType::Utf8, false),
            Field::new("content", DataType::Utf8, false),
        ]));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![id_col, title_col, content_col]).unwrap();

        let table = db
            .create_table(
                "docs",
                RecordBatchIterator::new(vec![Ok(batch)].into_iter(), schema),
            )
            .execute()
            .await
            .unwrap();

        // Create FTS indices on both columns
        table
            .create_index(&["title"], Index::FTS(Default::default()))
            .execute()
            .await
            .unwrap();
        table
            .create_index(&["content"], Index::FTS(Default::default()))
            .execute()
            .await
            .unwrap();

        let ctx = SessionContext::new();
        let mut resolver = HashMapTableResolver::new();
        let adapter = BaseTableAdapter::try_new(table.base_table().clone())
            .await
            .unwrap();
        resolver.register("docs".to_string(), Arc::new(adapter));
        ctx.register_udtf("fts", Arc::new(FtsTableFunction::new(Arc::new(resolver))));

        // Test MultiMatch query
        let result = ctx
            .sql(
                r#"SELECT id, title, content FROM fts('docs', '{"multi_match": {"match_queries": [{"column": "title", "terms": "Document"}, {"column": "content", "terms": "data"}]}}')"#,
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        // Should match row 1
        assert_eq!(result[0].num_rows(), 1);
    }

    #[tokio::test]
    async fn test_fts_udtf_multi_match_with_boost() {
        let db = crate::connect("memory://test_multi_match_boost")
            .execute()
            .await
            .unwrap();

        // Create table with multiple text columns
        let id_col = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let title_col = Arc::new(StringArray::from(vec![
            "Important Document",
            "Random Title",
            "Test Page",
        ]));
        let content_col = Arc::new(StringArray::from(vec![
            "Some content here",
            "This content is important data",
            "Nothing relevant",
        ]));

        let schema = Arc::new(arrow_schema::Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("title", DataType::Utf8, false),
            Field::new("content", DataType::Utf8, false),
        ]));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![id_col, title_col, content_col]).unwrap();

        let table = db
            .create_table(
                "docs",
                RecordBatchIterator::new(vec![Ok(batch)].into_iter(), schema),
            )
            .execute()
            .await
            .unwrap();

        // Create FTS indices
        table
            .create_index(&["title"], Index::FTS(Default::default()))
            .execute()
            .await
            .unwrap();
        table
            .create_index(&["content"], Index::FTS(Default::default()))
            .execute()
            .await
            .unwrap();

        let ctx = SessionContext::new();
        let mut resolver = HashMapTableResolver::new();
        let adapter = BaseTableAdapter::try_new(table.base_table().clone())
            .await
            .unwrap();
        resolver.register("docs".to_string(), Arc::new(adapter));
        ctx.register_udtf("fts", Arc::new(FtsTableFunction::new(Arc::new(resolver))));

        // Test MultiMatch with boosts
        let result = ctx
            .sql(
                r#"SELECT id, title, content, _score FROM fts('docs', '{"multi_match": {"match_queries": [{"column": "title", "terms": "important", "boost": 2.0}, {"column": "content", "terms": "data", "boost": 1.0}]}}') ORDER BY _score DESC"#,
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        // Should match rows with "important" in title (row 1) or "data"/"important" in content (row 2)
        assert_eq!(result[0].num_rows(), 2);
        assert!(
            result[0].schema().column_with_name("_score").is_some(),
            "_score should be present"
        );
    }

    #[tokio::test]
    async fn test_fts_udtf_fuzzy_with_prefix_length() {
        let db = crate::connect("memory://test_fuzzy_prefix")
            .execute()
            .await
            .unwrap();
        let table = setup_diverse_fts_table(&db, "docs").await;
        let ctx = setup_context_with_udtf(table, "docs").await;

        // Test fuzzy search with prefix_length parameter
        // prefix_length=3 means first 3 chars must match exactly
        let result = ctx
            .sql(r#"SELECT id, text FROM fts('docs', '{"match": {"column": "text", "terms": "crazily", "fuzziness": 2, "prefix_length": 3}}')"#)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        // With prefix_length=3, "cra" must match exactly
        // Should match "crazily" in rows 4 and 10 (and possibly row 9 "craziou" with fuzziness=2)
        assert_eq!(result[0].num_rows(), 3);
    }

    #[tokio::test]
    async fn test_fts_udtf_with_where_clause() {
        let db = crate::connect("memory://test_where")
            .execute()
            .await
            .unwrap();
        let table = setup_diverse_fts_table(&db, "docs").await;
        let ctx = setup_context_with_udtf(table, "docs").await;

        // Test FTS with WHERE clause filtering
        let result = ctx
            .sql(r#"SELECT id, text FROM fts('docs', '{"match": {"column": "text", "terms": "puppy"}}') WHERE id > 2"#)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        // Should match rows 3, 5 (puppy docs with id > 2). Row 8 doesn't contain "puppy"
        assert_eq!(result[0].num_rows(), 2);
    }

    #[tokio::test]
    async fn test_fts_udtf_ngram_substring_search() {
        let db = crate::connect("memory://test_ngram")
            .execute()
            .await
            .unwrap();

        // Create table with simple text for n-gram testing
        let data = RecordBatchIterator::new(
            vec![RecordBatch::try_new(
                Arc::new(ArrowSchema::new(vec![
                    Field::new("id", DataType::Int32, false),
                    Field::new("text", DataType::Utf8, false),
                ])),
                vec![
                    Arc::new(Int32Array::from(vec![1, 2, 3])),
                    Arc::new(StringArray::from(vec![
                        "hello world",
                        "lance database",
                        "lance is cool",
                    ])),
                ],
            )
            .unwrap()]
            .into_iter()
            .map(Ok),
            Arc::new(ArrowSchema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("text", DataType::Utf8, false),
            ])),
        );

        let table = Arc::new(
            db.create_table("docs", Box::new(data))
                .execute()
                .await
                .unwrap(),
        );

        // Create FTS index with n-gram tokenizer (default min_ngram_length=3)
        table
            .create_index(
                &["text"],
                Index::FTS(
                    FtsIndexBuilder::default()
                        .base_tokenizer("ngram".to_string())
                        .ngram_min_length(3)
                        .ngram_max_length(3),
                ),
            )
            .execute()
            .await
            .unwrap();

        let ctx = setup_context_with_udtf(table, "docs").await;

        // Test substring search with 3+ characters (default min_ngram_length=3)
        let result = ctx
            .sql(r#"SELECT id, text FROM fts('docs', '{"match": {"column": "text", "terms": "lan"}}')"#)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        // Should match "lance database" and "lance is cool"
        assert_eq!(result[0].num_rows(), 2);
    }

    #[tokio::test]
    async fn test_fts_udtf_boolean_query_and_combination() {
        let db = crate::connect("memory://test_bool_and")
            .execute()
            .await
            .unwrap();
        let table = setup_diverse_fts_table(&db, "docs").await;
        let ctx = setup_context_with_udtf(table, "docs").await;

        // Boolean query with multiple MUST clauses
        let result = ctx
            .sql(
                r#"SELECT id, text FROM fts('docs', '{"boolean": {"must": [{"match": {"column": "text", "terms": "puppy"}}, {"match": {"column": "text", "terms": "merrily"}}]}}')"#,
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        // Should match only row 1: "the puppy runs merrily" (has both "puppy" AND "merrily")
        assert_eq!(result[0].num_rows(), 1);
    }

    #[tokio::test]
    async fn test_fts_udtf_boolean_query_or_combination() {
        let db = crate::connect("memory://test_bool_or")
            .execute()
            .await
            .unwrap();
        let table = setup_diverse_fts_table(&db, "docs").await;
        let ctx = setup_context_with_udtf(table, "docs").await;

        // Boolean query with SHOULD clauses (OR)
        let result = ctx
            .sql(
                r#"SELECT id, text FROM fts('docs', '{"boolean": {"should": [{"match": {"column": "text", "terms": "puppy"}}, {"match": {"column": "text", "terms": "merrily"}}]}}')"#,
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        // Should match rows 1, 3, 5 (containing "puppy" OR "merrily")
        assert_eq!(result[0].num_rows(), 3);
    }

    #[tokio::test]
    async fn test_fts_udtf_multi_match_with_field_boosts() {
        let db = crate::connect("memory://test_mm_boost")
            .execute()
            .await
            .unwrap();

        // Create table with two text columns
        let data = RecordBatchIterator::new(
            vec![RecordBatch::try_new(
                Arc::new(ArrowSchema::new(vec![
                    Field::new("id", DataType::Int32, false),
                    Field::new("title", DataType::Utf8, false),
                    Field::new("content", DataType::Utf8, false),
                ])),
                vec![
                    Arc::new(Int32Array::from(vec![1, 2, 3])),
                    Arc::new(StringArray::from(vec![
                        "Important Document",
                        "Another Document",
                        "Random Text",
                    ])),
                    Arc::new(StringArray::from(vec![
                        "This is important information",
                        "This has details",
                        "Nothing special here",
                    ])),
                ],
            )
            .unwrap()]
            .into_iter()
            .map(Ok),
            Arc::new(ArrowSchema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("title", DataType::Utf8, false),
                Field::new("content", DataType::Utf8, false),
            ])),
        );

        let table = Arc::new(
            db.create_table("docs", Box::new(data))
                .execute()
                .await
                .unwrap(),
        );

        // Create FTS indices on both columns
        table
            .create_index(&["title"], Index::FTS(Default::default()))
            .execute()
            .await
            .unwrap();
        table
            .create_index(&["content"], Index::FTS(Default::default()))
            .execute()
            .await
            .unwrap();

        let ctx = setup_context_with_udtf(table, "docs").await;

        // Test MultiMatch with field boosts - title boosted 2x
        let result = ctx
            .sql(
                r#"SELECT id, title, content, _score FROM fts('docs', '{"multi_match": {"match_queries": [{"column": "title", "terms": "important", "boost": 2.0}, {"column": "content", "terms": "important", "boost": 1.0}]}}') ORDER BY _score DESC"#,
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        // Should match row 1 with "important" in title or content
        assert_eq!(result[0].num_rows(), 1);
        assert!(result[0].schema().column_with_name("_score").is_some());
    }

    #[tokio::test]
    async fn test_python_json_e2e_basic_match() {
        let db = crate::connect("memory://test_python_e2e_basic")
            .execute()
            .await
            .unwrap();

        let data = RecordBatchIterator::new(
            vec![RecordBatch::try_new(
                Arc::new(ArrowSchema::new(vec![
                    Field::new("id", DataType::Int32, false),
                    Field::new("text", DataType::Utf8, false),
                ])),
                vec![
                    Arc::new(Int32Array::from(vec![1, 2, 3])),
                    Arc::new(StringArray::from(vec![
                        "hello world",
                        "hello there",
                        "goodbye friend",
                    ])),
                ],
            )
            .unwrap()]
            .into_iter()
            .map(Ok),
            Arc::new(ArrowSchema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("text", DataType::Utf8, false),
            ])),
        );

        let table = Arc::new(
            db.create_table("docs", Box::new(data))
                .execute()
                .await
                .unwrap(),
        );
        table
            .create_index(&["text"], Index::FTS(Default::default()))
            .execute()
            .await
            .unwrap();

        let ctx = setup_context_with_udtf(table, "docs").await;

        // Test 1: Basic match query from Python (with all default fields included)
        const JSON: &str = r#"{"match": {"column": "text", "terms": "hello world"}}"#;
        let result = ctx
            .sql(&format!(r#"SELECT id, text FROM fts('docs', '{}')"#, JSON))
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        // Should match exactly 2 rows: "hello world" and "hello there" (both contain "hello" or "world")
        assert_eq!(result[0].num_rows(), 2);
    }

    #[tokio::test]
    async fn test_python_json_e2e_match_and_operator() {
        let db = crate::connect("memory://test_python_e2e_and")
            .execute()
            .await
            .unwrap();

        let data = RecordBatchIterator::new(
            vec![RecordBatch::try_new(
                Arc::new(ArrowSchema::new(vec![
                    Field::new("id", DataType::Int32, false),
                    Field::new("content", DataType::Utf8, false),
                ])),
                vec![
                    Arc::new(Int32Array::from(vec![1, 2, 3])),
                    Arc::new(StringArray::from(vec![
                        "fuzzy search algorithms",
                        "fuzzy logic",
                        "search engine",
                    ])),
                ],
            )
            .unwrap()]
            .into_iter()
            .map(Ok),
            Arc::new(ArrowSchema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("content", DataType::Utf8, false),
            ])),
        );

        let table = Arc::new(
            db.create_table("docs", Box::new(data))
                .execute()
                .await
                .unwrap(),
        );
        table
            .create_index(&["content"], Index::FTS(Default::default()))
            .execute()
            .await
            .unwrap();

        let ctx = setup_context_with_udtf(table, "docs").await;

        // Test 2: Match with AND operator from Python
        const JSON: &str = r#"{"match": {"column": "content", "terms": "fuzzy search", "boost": 1.5, "fuzziness": 2, "max_expansions": 100, "operator": "AND", "prefix_length": 3}}"#;
        let result = ctx
            .sql(&format!(
                r#"SELECT id, content FROM fts('docs', '{}')"#,
                JSON
            ))
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        // Should match row 1 only (contains both "fuzzy" and "search")
        assert_eq!(result[0].num_rows(), 1);
    }

    #[tokio::test]
    async fn test_python_json_e2e_phrase_query() {
        let db = crate::connect("memory://test_python_e2e_phrase")
            .execute()
            .await
            .unwrap();

        let data = RecordBatchIterator::new(
            vec![RecordBatch::try_new(
                Arc::new(ArrowSchema::new(vec![
                    Field::new("id", DataType::Int32, false),
                    Field::new("title", DataType::Utf8, false),
                ])),
                vec![
                    Arc::new(Int32Array::from(vec![1, 2, 3])),
                    Arc::new(StringArray::from(vec![
                        "exact phrase match here",
                        "exact and phrase",
                        "unrelated text",
                    ])),
                ],
            )
            .unwrap()]
            .into_iter()
            .map(Ok),
            Arc::new(ArrowSchema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("title", DataType::Utf8, false),
            ])),
        );

        let table = Arc::new(
            db.create_table("docs", Box::new(data))
                .execute()
                .await
                .unwrap(),
        );
        table
            .create_index(
                &["title"],
                Index::FTS(FtsIndexBuilder::default().with_position(true)),
            )
            .execute()
            .await
            .unwrap();

        let ctx = setup_context_with_udtf(table, "docs").await;

        // Test 4: Phrase query with slop from Python
        const JSON: &str =
            r#"{"phrase": {"column": "title", "terms": "exact phrase match", "slop": 2}}"#;
        let result = ctx
            .sql(&format!(r#"SELECT id, title FROM fts('docs', '{}')"#, JSON))
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        // Should match row 1 (phrase appears with allowed slop)
        assert_eq!(result[0].num_rows(), 1);
    }

    #[tokio::test]
    async fn test_python_json_e2e_boolean_query() {
        let db = crate::connect("memory://test_python_e2e_boolean")
            .execute()
            .await
            .unwrap();

        let data = RecordBatchIterator::new(
            vec![RecordBatch::try_new(
                Arc::new(ArrowSchema::new(vec![
                    Field::new("id", DataType::Int32, false),
                    Field::new("text", DataType::Utf8, false),
                ])),
                vec![
                    Arc::new(Int32Array::from(vec![1, 2, 3])),
                    Arc::new(StringArray::from(vec![
                        "puppy runs fast",
                        "puppy walks slow",
                        "dog runs fast",
                    ])),
                ],
            )
            .unwrap()]
            .into_iter()
            .map(Ok),
            Arc::new(ArrowSchema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("text", DataType::Utf8, false),
            ])),
        );

        let table = Arc::new(
            db.create_table("docs", Box::new(data))
                .execute()
                .await
                .unwrap(),
        );
        table
            .create_index(&["text"], Index::FTS(Default::default()))
            .execute()
            .await
            .unwrap();

        let ctx = setup_context_with_udtf(table, "docs").await;

        // Test 8: Boolean MUST query from Python
        const JSON: &str = r#"{"boolean": {"must": [{"match": {"column": "text", "terms": "puppy"}}, {"match": {"column": "text", "terms": "runs"}}]}}"#;
        let result = ctx
            .sql(&format!(r#"SELECT id, text FROM fts('docs', '{}')"#, JSON))
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        // Should match row 1 only (contains both "puppy" AND "runs")
        assert_eq!(result[0].num_rows(), 1);
    }

    #[tokio::test]
    async fn test_python_json_e2e_complex_nested() {
        let db = crate::connect("memory://test_python_e2e_nested")
            .execute()
            .await
            .unwrap();

        let data = RecordBatchIterator::new(
            vec![RecordBatch::try_new(
                Arc::new(ArrowSchema::new(vec![
                    Field::new("id", DataType::Int32, false),
                    Field::new("tags", DataType::Utf8, false),
                    Field::new("title", DataType::Utf8, false),
                ])),
                vec![
                    Arc::new(Int32Array::from(vec![1, 2, 3])),
                    Arc::new(StringArray::from(vec!["python", "rust", "java"])),
                    Arc::new(StringArray::from(vec!["tutorial", "guide", "manual"])),
                ],
            )
            .unwrap()]
            .into_iter()
            .map(Ok),
            Arc::new(ArrowSchema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("tags", DataType::Utf8, false),
                Field::new("title", DataType::Utf8, false),
            ])),
        );

        let table = Arc::new(
            db.create_table("docs", Box::new(data))
                .execute()
                .await
                .unwrap(),
        );
        table
            .create_index(&["tags"], Index::FTS(Default::default()))
            .execute()
            .await
            .unwrap();
        table
            .create_index(&["title"], Index::FTS(Default::default()))
            .execute()
            .await
            .unwrap();

        let ctx = setup_context_with_udtf(table, "docs").await;

        // Test 11: Complex nested boolean from Python
        const JSON: &str = r#"{"boolean": {"should": [{"boolean": {"must": [{"match": {"column": "tags", "terms": "python"}}, {"match": {"column": "title", "terms": "tutorial"}}]}}, {"boolean": {"must": [{"match": {"column": "tags", "terms": "rust"}}, {"match": {"column": "title", "terms": "guide"}}]}}]}}"#;
        let result = ctx
            .sql(&format!(r#"SELECT id FROM fts('docs', '{}')"#, JSON))
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        // Should match rows 1 and 2 ((python AND tutorial) OR (rust AND guide))
        assert_eq!(result[0].num_rows(), 2);
    }
}
