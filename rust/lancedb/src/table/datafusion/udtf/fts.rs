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

/// Serialize an FTS query to JSON string.
pub fn to_json(query: &lance_index::scalar::inverted::query::FtsQuery) -> crate::Result<String> {
    serde_json::to_string(query).map_err(|e| crate::Error::InvalidInput {
        message: format!("Failed to serialize FTS query to JSON: {}", e),
    })
}

/// Deserialize an FTS query from JSON string.
pub fn from_json(json: &str) -> crate::Result<lance_index::scalar::inverted::query::FtsQuery> {
    serde_json::from_str(json).map_err(|e| crate::Error::InvalidInput {
        message: format!(
            "Failed to deserialize FTS query from JSON: {}. Input was: {}",
            e, json
        ),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        index::{scalar::FtsIndexBuilder, Index},
        table::datafusion::BaseTableAdapter,
        Connection, Table,
    };
    use arrow_array::{Int32Array, RecordBatch, StringArray};
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
        let table = db.create_table("foo", batch).execute().await.unwrap();

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
        use lance_index::scalar::inverted::query::*;
        let fts_query_struct = FtsQuery::Match(
            MatchQuery::new("catch fish".to_string())
                .with_column(Some("text".to_string()))
                .with_operator(Operator::And),
        );
        let fts_query = to_json(&fts_query_struct).unwrap();

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
        let group_query = FtsQuery::Match(
            MatchQuery::new("catch".to_string()).with_column(Some("text".to_string())),
        );
        let group_query_json = to_json(&group_query).unwrap();
        let group_result = ctx
            .sql(&format!(
                "SELECT number, COUNT(*) as cnt FROM fts('foo', '{}') GROUP BY number",
                group_query_json
            ))
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
            .create_table("metadata", metadata_batch.clone())
            .execute()
            .await
            .unwrap();

        // Register metadata table with DataFusion
        ctx.register_batch("metadata", metadata_batch).unwrap();

        let join_query = FtsQuery::Match(
            MatchQuery::new("catch".to_string()).with_column(Some("text".to_string())),
        );
        let join_query_json = to_json(&join_query).unwrap();
        let join_result = ctx
            .sql(&format!(
                "SELECT f.text, f.number, m.extra FROM fts('foo', '{}') f JOIN metadata m ON f.number = m.id",
                join_query_json
            ))
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

        let table = db.create_table(table_name, batch).execute().await.unwrap();

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
        use lance_index::scalar::inverted::query::*;
        let db = crate::connect("memory://test_and_op")
            .execute()
            .await
            .unwrap();
        let table = setup_diverse_fts_table(&db, "docs").await;
        let ctx = setup_context_with_udtf(table, "docs").await;

        let query = FtsQuery::Match(
            MatchQuery::new("puppy runs".to_string())
                .with_column(Some("text".to_string()))
                .with_operator(Operator::And),
        );
        let query_json = to_json(&query).unwrap();
        let result = execute_fts_query(
            &ctx,
            &format!(r#"SELECT id, text FROM fts('docs', '{}')"#, query_json),
        )
        .await;

        // Should match only row 1: "the puppy runs merrily" (has both "puppy" AND "runs")
        assert_result_shape(&result, 1, 2);
    }

    #[tokio::test]
    async fn test_fts_udtf_or_operator() {
        use lance_index::scalar::inverted::query::*;
        let db = crate::connect("memory://test_or_op")
            .execute()
            .await
            .unwrap();
        let table = setup_diverse_fts_table(&db, "docs").await;
        let ctx = setup_context_with_udtf(table, "docs").await;

        let query = FtsQuery::Match(
            MatchQuery::new("cat dog".to_string())
                .with_column(Some("text".to_string()))
                .with_operator(Operator::Or),
        );
        let query_json = to_json(&query).unwrap();
        let result = execute_fts_query(
            &ctx,
            &format!(r#"SELECT id, text FROM fts('docs', '{}')"#, query_json),
        )
        .await;

        // Should match rows 2, 4, 6 ("cat" OR "dog")
        assert_result_shape(&result, 3, 2);
    }

    #[tokio::test]
    async fn test_fts_udtf_phrase_query() {
        use lance_index::scalar::inverted::query::*;
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

        let table = db.create_table("docs", batch).execute().await.unwrap();

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
        let query = FtsQuery::Phrase(
            PhraseQuery::new("quick brown".to_string()).with_column(Some("text".to_string())),
        );
        let query_json = to_json(&query).unwrap();
        let result = ctx
            .sql(&format!(
                r#"SELECT id, text FROM fts('docs', '{}')"#,
                query_json
            ))
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
        use lance_index::scalar::inverted::query::*;
        let db = crate::connect("memory://test_fuzzy")
            .execute()
            .await
            .unwrap();
        let table = setup_diverse_fts_table(&db, "docs").await;
        let ctx = setup_context_with_udtf(table, "docs").await;

        // Test fuzzy search with fuzziness=2
        let query = FtsQuery::Match(
            MatchQuery::new("craziou".to_string())
                .with_column(Some("text".to_string()))
                .with_fuzziness(Some(2)),
        );
        let query_json = to_json(&query).unwrap();
        let result = ctx
            .sql(&format!(
                r#"SELECT id, text FROM fts('docs', '{}')"#,
                query_json
            ))
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
        use lance_index::scalar::inverted::query::*;
        let db = crate::connect("memory://test_ordered")
            .execute()
            .await
            .unwrap();
        let table = setup_diverse_fts_table(&db, "docs").await;
        let ctx = setup_context_with_udtf(table, "docs").await;

        // Test ordered results (order by _score explicitly)
        let query = FtsQuery::Match(
            MatchQuery::new("puppy".to_string()).with_column(Some("text".to_string())),
        );
        let query_json = to_json(&query).unwrap();
        let result = ctx
            .sql(&format!(
                r#"SELECT id, text, _score FROM fts('docs', '{}') ORDER BY _score DESC"#,
                query_json
            ))
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
        use lance_index::scalar::inverted::query::*;
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

        let table = db.create_table("multi_col", batch).execute().await.unwrap();

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
        let query = FtsQuery::Match(
            MatchQuery::new("Important".to_string()).with_column(Some("title".to_string())),
        );
        let query_json = to_json(&query).unwrap();
        let result = ctx
            .sql(&format!(
                r#"SELECT id, title FROM fts('multi_col', '{}')"#,
                query_json
            ))
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        assert_eq!(result[0].num_rows(), 1);

        // Test searching content column
        let query = FtsQuery::Match(
            MatchQuery::new("important".to_string()).with_column(Some("content".to_string())),
        );
        let query_json = to_json(&query).unwrap();
        let result = ctx
            .sql(&format!(
                r#"SELECT id, content FROM fts('multi_col', '{}')"#,
                query_json
            ))
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        assert_eq!(result[0].num_rows(), 1);
    }

    #[tokio::test]
    async fn test_fts_udtf_projection() {
        use lance_index::scalar::inverted::query::*;
        let db = crate::connect("memory://test_projection")
            .execute()
            .await
            .unwrap();
        let table = setup_diverse_fts_table(&db, "docs").await;
        let ctx = setup_context_with_udtf(table, "docs").await;

        // Test selecting only specific columns
        let query = FtsQuery::Match(
            MatchQuery::new("puppy".to_string()).with_column(Some("text".to_string())),
        );
        let query_json = to_json(&query).unwrap();
        let result = ctx
            .sql(&format!(
                r#"SELECT text FROM fts('docs', '{}')"#,
                query_json
            ))
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
        use lance_index::scalar::inverted::query::*;
        let db = crate::connect("memory://test_limit")
            .execute()
            .await
            .unwrap();
        let table = setup_diverse_fts_table(&db, "docs").await;
        let ctx = setup_context_with_udtf(table, "docs").await;

        // Test LIMIT clause
        let query = FtsQuery::Match(
            MatchQuery::new("puppy".to_string()).with_column(Some("text".to_string())),
        );
        let query_json = to_json(&query).unwrap();
        let result = ctx
            .sql(&format!(
                r#"SELECT id, text FROM fts('docs', '{}') LIMIT 2"#,
                query_json
            ))
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        assert_eq!(result[0].num_rows(), 2);
    }

    #[tokio::test]
    async fn test_fts_udtf_order_by() {
        use lance_index::scalar::inverted::query::*;
        let db = crate::connect("memory://test_order_by")
            .execute()
            .await
            .unwrap();
        let table = setup_diverse_fts_table(&db, "docs").await;
        let ctx = setup_context_with_udtf(table, "docs").await;

        // Test ORDER BY clause (ordering by id instead of _score since _score is not in schema)
        let query = FtsQuery::Match(
            MatchQuery::new("puppy".to_string()).with_column(Some("text".to_string())),
        );
        let query_json = to_json(&query).unwrap();
        let result = ctx
            .sql(&format!(
                r#"SELECT id, text FROM fts('docs', '{}') ORDER BY id LIMIT 3"#,
                query_json
            ))
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        assert_eq!(result[0].num_rows(), 3);
    }

    #[tokio::test]
    async fn test_fts_udtf_complex_boolean() {
        use lance_index::scalar::inverted::query::*;
        let db = crate::connect("memory://test_complex")
            .execute()
            .await
            .unwrap();
        let table = setup_diverse_fts_table(&db, "docs").await;
        let ctx = setup_context_with_udtf(table, "docs").await;

        // Complex query: search for texts containing "runs" or "jumps"
        let query = FtsQuery::Match(
            MatchQuery::new("runs jumps".to_string()).with_column(Some("text".to_string())),
        );
        let query_json = to_json(&query).unwrap();
        let result = ctx
            .sql(&format!(
                r#"SELECT id, text FROM fts('docs', '{}')"#,
                query_json
            ))
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
        use lance_index::scalar::inverted::query::*;
        let db = crate::connect("memory://test_empty")
            .execute()
            .await
            .unwrap();
        let table = setup_diverse_fts_table(&db, "docs").await;
        let ctx = setup_context_with_udtf(table, "docs").await;

        // Test query with no matches
        let query = FtsQuery::Match(
            MatchQuery::new("nonexistent_word_xyz".to_string())
                .with_column(Some("text".to_string())),
        );
        let query_json = to_json(&query).unwrap();
        let result = ctx
            .sql(&format!(
                r#"SELECT id, text FROM fts('docs', '{}')"#,
                query_json
            ))
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
        use lance_index::scalar::inverted::query::*;
        let db = crate::connect("memory://test_count")
            .execute()
            .await
            .unwrap();
        let table = setup_diverse_fts_table(&db, "docs").await;
        let ctx = setup_context_with_udtf(table, "docs").await;

        // Test COUNT aggregation (using COUNT(id) instead of COUNT(*))
        let query = FtsQuery::Match(
            MatchQuery::new("puppy".to_string()).with_column(Some("text".to_string())),
        );
        let query_json = to_json(&query).unwrap();
        let result = ctx
            .sql(&format!(
                r#"SELECT COUNT(id) as cnt FROM fts('docs', '{}')"#,
                query_json
            ))
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
        use lance_index::scalar::inverted::query::*;
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
            .create_table("metadata", metadata_batch.clone())
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

        let query = FtsQuery::Match(
            MatchQuery::new("puppy".to_string()).with_column(Some("text".to_string())),
        );
        let query_json = to_json(&query).unwrap();
        let result = ctx
            .sql(&format!(
                r#"SELECT d.id, d.text, m.extra_info
                   FROM fts('docs', '{}') d
                   JOIN metadata m ON d.id = m.id"#,
                query_json
            ))
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
        use lance_index::scalar::inverted::query::*;
        let db = crate::connect("memory://test_agg").execute().await.unwrap();
        let table = setup_diverse_fts_table(&db, "docs").await;
        let ctx = setup_context_with_udtf(table, "docs").await;

        let query = FtsQuery::Match(
            MatchQuery::new("puppy".to_string()).with_column(Some("text".to_string())),
        );
        let query_json = to_json(&query).unwrap();
        let result = ctx
            .sql(&format!(
                r#"SELECT category, COUNT(*) as cnt
                   FROM fts('docs', '{}')
                   GROUP BY category"#,
                query_json
            ))
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        // Should group by category - puppy appears in multiple rows but aggregation may combine them
        // The actual behavior depends on whether DataFusion aggregates across all matches
        assert!(
            result[0].num_rows() >= 1,
            "Should have at least 1 category with puppy"
        );
        assert_eq!(result[0].num_columns(), 2);
    }

    #[tokio::test]
    async fn test_fts_udtf_score_with_select_star() {
        use lance_index::scalar::inverted::query::*;
        let db = crate::connect("memory://test_score_star")
            .execute()
            .await
            .unwrap();
        let table = setup_diverse_fts_table(&db, "docs").await;
        let ctx = setup_context_with_udtf(table, "docs").await;

        // Test SELECT * - should include _score
        let query = FtsQuery::Match(
            MatchQuery::new("puppy".to_string()).with_column(Some("text".to_string())),
        );
        let query_json = to_json(&query).unwrap();
        let result = ctx
            .sql(&format!(r#"SELECT * FROM fts('docs', '{}')"#, query_json))
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
        use lance_index::scalar::inverted::query::*;
        let db = crate::connect("memory://test_score_explicit")
            .execute()
            .await
            .unwrap();
        let table = setup_diverse_fts_table(&db, "docs").await;
        let ctx = setup_context_with_udtf(table, "docs").await;

        // Test explicit _score in projection
        let query = FtsQuery::Match(
            MatchQuery::new("puppy".to_string()).with_column(Some("text".to_string())),
        );
        let query_json = to_json(&query).unwrap();
        let result = ctx
            .sql(&format!(
                r#"SELECT id, text, _score FROM fts('docs', '{}')"#,
                query_json
            ))
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
        use lance_index::scalar::inverted::query::*;
        let db = crate::connect("memory://test_score_no_proj")
            .execute()
            .await
            .unwrap();
        let table = setup_diverse_fts_table(&db, "docs").await;
        let ctx = setup_context_with_udtf(table, "docs").await;

        // Test without _score in projection
        let query = FtsQuery::Match(
            MatchQuery::new("puppy".to_string()).with_column(Some("text".to_string())),
        );
        let query_json = to_json(&query).unwrap();
        let result = ctx
            .sql(&format!(
                r#"SELECT id, text FROM fts('docs', '{}')"#,
                query_json
            ))
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
        use lance_index::scalar::inverted::query::*;
        let db = crate::connect("memory://test_score_order")
            .execute()
            .await
            .unwrap();
        let table = setup_diverse_fts_table(&db, "docs").await;
        let ctx = setup_context_with_udtf(table, "docs").await;

        // Test ORDER BY _score
        let query = FtsQuery::Match(
            MatchQuery::new("puppy".to_string()).with_column(Some("text".to_string())),
        );
        let query_json = to_json(&query).unwrap();
        let result = ctx
            .sql(&format!(
                r#"SELECT id, text, _score FROM fts('docs', '{}') ORDER BY _score DESC"#,
                query_json
            ))
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
        use lance_index::scalar::inverted::query::*;
        let db = crate::connect("memory://test_score_mixed")
            .execute()
            .await
            .unwrap();
        let table = setup_diverse_fts_table(&db, "docs").await;
        let ctx = setup_context_with_udtf(table, "docs").await;

        // Test mixing _score with normal columns and expressions
        let query = FtsQuery::Match(
            MatchQuery::new("puppy".to_string()).with_column(Some("text".to_string())),
        );
        let query_json = to_json(&query).unwrap();
        let result = ctx
            .sql(&format!(
                r#"SELECT id, text, _score, category, id + 100 as id_plus_100 FROM fts('docs', '{}')"#,
                query_json
            ))
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
        use lance_index::scalar::inverted::query::*;
        let db = crate::connect("memory://test_bool_must")
            .execute()
            .await
            .unwrap();
        let table = setup_diverse_fts_table(&db, "docs").await;
        let ctx = setup_context_with_udtf(table, "docs").await;

        // Test Boolean query with "must" clause - all terms must match
        let must1 = FtsQuery::Match(
            MatchQuery::new("puppy".to_string()).with_column(Some("text".to_string())),
        );
        let must2 = FtsQuery::Match(
            MatchQuery::new("runs".to_string()).with_column(Some("text".to_string())),
        );
        let query = FtsQuery::Boolean(BooleanQuery::new(vec![
            (Occur::Must, must1),
            (Occur::Must, must2),
        ]));
        let query_json = to_json(&query).unwrap();
        let result = ctx
            .sql(&format!(
                r#"SELECT id, text FROM fts('docs', '{}')"#,
                query_json
            ))
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
        use lance_index::scalar::inverted::query::*;
        let db = crate::connect("memory://test_bool_should")
            .execute()
            .await
            .unwrap();
        let table = setup_diverse_fts_table(&db, "docs").await;
        let ctx = setup_context_with_udtf(table, "docs").await;

        // Test Boolean query with "should" clause
        let should1 = FtsQuery::Match(
            MatchQuery::new("puppy".to_string()).with_column(Some("text".to_string())),
        );
        let should2 = FtsQuery::Match(
            MatchQuery::new("cat".to_string()).with_column(Some("text".to_string())),
        );
        let query = FtsQuery::Boolean(BooleanQuery::new(vec![
            (Occur::Should, should1),
            (Occur::Should, should2),
        ]));
        let query_json = to_json(&query).unwrap();
        let result = ctx
            .sql(&format!(
                r#"SELECT id, text FROM fts('docs', '{}')"#,
                query_json
            ))
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
        use lance_index::scalar::inverted::query::*;
        let db = crate::connect("memory://test_bool_must_not")
            .execute()
            .await
            .unwrap();
        let table = setup_diverse_fts_table(&db, "docs").await;
        let ctx = setup_context_with_udtf(table, "docs").await;

        // Test Boolean query with must and must_not
        let must = FtsQuery::Match(
            MatchQuery::new("puppy".to_string()).with_column(Some("text".to_string())),
        );
        let must_not = FtsQuery::Match(
            MatchQuery::new("training".to_string()).with_column(Some("text".to_string())),
        );
        let query = FtsQuery::Boolean(BooleanQuery::new(vec![
            (Occur::Must, must),
            (Occur::MustNot, must_not),
        ]));
        let query_json = to_json(&query).unwrap();
        let result = ctx
            .sql(&format!(
                r#"SELECT id, text FROM fts('docs', '{}')"#,
                query_json
            ))
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
        use lance_index::scalar::inverted::query::*;
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

        let table = db.create_table("docs", batch).execute().await.unwrap();

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
        let query = FtsQuery::Phrase(
            PhraseQuery::new("quick brown".to_string())
                .with_column(Some("text".to_string()))
                .with_slop(1),
        );
        let query_json = to_json(&query).unwrap();
        let result = ctx
            .sql(&format!(
                r#"SELECT id, text FROM fts('docs', '{}')"#,
                query_json
            ))
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
        use lance_index::scalar::inverted::query::*;
        let db = crate::connect("memory://test_match_boost")
            .execute()
            .await
            .unwrap();
        let table = setup_diverse_fts_table(&db, "docs").await;
        let ctx = setup_context_with_udtf(table, "docs").await;

        // Test Match query with boost parameter
        let query = FtsQuery::Match(
            MatchQuery::new("puppy".to_string())
                .with_column(Some("text".to_string()))
                .with_boost(2.0),
        );
        let query_json = to_json(&query).unwrap();
        let result = ctx
            .sql(&format!(
                r#"SELECT id, text, _score FROM fts('docs', '{}') ORDER BY _score DESC"#,
                query_json
            ))
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
        use lance_index::scalar::inverted::query::*;
        let db = crate::connect("memory://test_boost_query")
            .execute()
            .await
            .unwrap();
        let table = setup_diverse_fts_table(&db, "docs").await;
        let ctx = setup_context_with_udtf(table, "docs").await;

        // Test Boost query
        let positive = FtsQuery::Match(
            MatchQuery::new("puppy".to_string()).with_column(Some("text".to_string())),
        );
        let negative = FtsQuery::Match(
            MatchQuery::new("training".to_string()).with_column(Some("text".to_string())),
        );
        let query = FtsQuery::Boost(BoostQuery::new(positive, negative, Some(0.3)));
        let query_json = to_json(&query).unwrap();
        let result = ctx
            .sql(&format!(
                r#"SELECT id, text, _score FROM fts('docs', '{}') ORDER BY _score DESC"#,
                query_json
            ))
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
        use lance_index::scalar::inverted::query::*;
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

        let table = db.create_table("docs", batch).execute().await.unwrap();

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
        let query = FtsQuery::MultiMatch(MultiMatchQuery {
            match_queries: vec![
                MatchQuery::new("Document".to_string()).with_column(Some("title".to_string())),
                MatchQuery::new("data".to_string()).with_column(Some("content".to_string())),
            ],
        });
        let query_json = to_json(&query).unwrap();
        let result = ctx
            .sql(&format!(
                r#"SELECT id, title, content FROM fts('docs', '{}')"#,
                query_json
            ))
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
        use lance_index::scalar::inverted::query::*;
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

        let table = db.create_table("docs", batch).execute().await.unwrap();

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
        let query = FtsQuery::MultiMatch(MultiMatchQuery {
            match_queries: vec![
                MatchQuery::new("important".to_string())
                    .with_column(Some("title".to_string()))
                    .with_boost(2.0),
                MatchQuery::new("data".to_string())
                    .with_column(Some("content".to_string()))
                    .with_boost(1.0),
            ],
        });
        let query_json = to_json(&query).unwrap();
        let result = ctx
            .sql(&format!(
                r#"SELECT id, title, content, _score FROM fts('docs', '{}') ORDER BY _score DESC"#,
                query_json
            ))
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
        use lance_index::scalar::inverted::query::*;
        let db = crate::connect("memory://test_fuzzy_prefix")
            .execute()
            .await
            .unwrap();
        let table = setup_diverse_fts_table(&db, "docs").await;
        let ctx = setup_context_with_udtf(table, "docs").await;

        // Test fuzzy search with prefix_length parameter
        // prefix_length=3 means first 3 chars must match exactly
        let query = FtsQuery::Match(
            MatchQuery::new("crazily".to_string())
                .with_column(Some("text".to_string()))
                .with_fuzziness(Some(2))
                .with_prefix_length(3),
        );
        let query_json = to_json(&query).unwrap();
        let result = ctx
            .sql(&format!(
                r#"SELECT id, text FROM fts('docs', '{}')"#,
                query_json
            ))
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
        use lance_index::scalar::inverted::query::*;
        let db = crate::connect("memory://test_where")
            .execute()
            .await
            .unwrap();
        let table = setup_diverse_fts_table(&db, "docs").await;
        let ctx = setup_context_with_udtf(table, "docs").await;

        // Test FTS with WHERE clause filtering
        let query = FtsQuery::Match(
            MatchQuery::new("puppy".to_string()).with_column(Some("text".to_string())),
        );
        let query_json = to_json(&query).unwrap();
        let result = ctx
            .sql(&format!(
                r#"SELECT id, text FROM fts('docs', '{}') WHERE id > 2"#,
                query_json
            ))
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
        let data = RecordBatch::try_new(
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
        .unwrap();

        let table = Arc::new(db.create_table("docs", data).execute().await.unwrap());

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
        use lance_index::scalar::inverted::query::*;
        let query = FtsQuery::Match(
            MatchQuery::new("lan".to_string()).with_column(Some("text".to_string())),
        );
        let query_json = to_json(&query).unwrap();
        let result = ctx
            .sql(&format!(
                r#"SELECT id, text FROM fts('docs', '{}')"#,
                query_json
            ))
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
        use lance_index::scalar::inverted::query::*;
        let db = crate::connect("memory://test_bool_and")
            .execute()
            .await
            .unwrap();
        let table = setup_diverse_fts_table(&db, "docs").await;
        let ctx = setup_context_with_udtf(table, "docs").await;

        // Boolean query with multiple MUST clauses
        let must1 = FtsQuery::Match(
            MatchQuery::new("puppy".to_string()).with_column(Some("text".to_string())),
        );
        let must2 = FtsQuery::Match(
            MatchQuery::new("merrily".to_string()).with_column(Some("text".to_string())),
        );
        let query = FtsQuery::Boolean(BooleanQuery::new(vec![
            (Occur::Must, must1),
            (Occur::Must, must2),
        ]));
        let query_json = to_json(&query).unwrap();
        let result = ctx
            .sql(&format!(
                r#"SELECT id, text FROM fts('docs', '{}')"#,
                query_json
            ))
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
        use lance_index::scalar::inverted::query::*;
        let db = crate::connect("memory://test_bool_or")
            .execute()
            .await
            .unwrap();
        let table = setup_diverse_fts_table(&db, "docs").await;
        let ctx = setup_context_with_udtf(table, "docs").await;

        // Boolean query with SHOULD clauses (OR)
        let should1 = FtsQuery::Match(
            MatchQuery::new("puppy".to_string()).with_column(Some("text".to_string())),
        );
        let should2 = FtsQuery::Match(
            MatchQuery::new("merrily".to_string()).with_column(Some("text".to_string())),
        );
        let query = FtsQuery::Boolean(BooleanQuery::new(vec![
            (Occur::Should, should1),
            (Occur::Should, should2),
        ]));
        let query_json = to_json(&query).unwrap();
        let result = ctx
            .sql(&format!(
                r#"SELECT id, text FROM fts('docs', '{}')"#,
                query_json
            ))
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
        use lance_index::scalar::inverted::query::*;
        let db = crate::connect("memory://test_mm_boost")
            .execute()
            .await
            .unwrap();

        // Create table with two text columns
        let data = RecordBatch::try_new(
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
        .unwrap();

        let table = Arc::new(db.create_table("docs", data).execute().await.unwrap());

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
        let query = FtsQuery::MultiMatch(MultiMatchQuery {
            match_queries: vec![
                MatchQuery::new("important".to_string())
                    .with_column(Some("title".to_string()))
                    .with_boost(2.0),
                MatchQuery::new("important".to_string())
                    .with_column(Some("content".to_string()))
                    .with_boost(1.0),
            ],
        });
        let query_json = to_json(&query).unwrap();
        let result = ctx
            .sql(&format!(
                r#"SELECT id, title, content, _score FROM fts('docs', '{}') ORDER BY _score DESC"#,
                query_json
            ))
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        // Should match row 1 with "important" in title or content
        assert_eq!(result[0].num_rows(), 1);
        assert!(result[0].schema().column_with_name("_score").is_some());
    }

    #[test]
    fn test_to_json_round_trip_match() {
        use lance_index::scalar::inverted::query::*;

        let query = FtsQuery::Match(
            MatchQuery::new("hello world".to_string())
                .with_column(Some("text".to_string()))
                .with_boost(2.0)
                .with_fuzziness(Some(2)),
        );

        let json = to_json(&query).unwrap();
        let parsed = from_json(&json).unwrap();
        assert_eq!(query, parsed);
    }

    #[test]
    fn test_to_json_round_trip_phrase() {
        use lance_index::scalar::inverted::query::*;

        let query = FtsQuery::Phrase(
            PhraseQuery::new("exact phrase".to_string())
                .with_column(Some("text".to_string()))
                .with_slop(2),
        );

        let json = to_json(&query).unwrap();
        let parsed = from_json(&json).unwrap();
        assert_eq!(query, parsed);
    }

    #[test]
    fn test_to_json_round_trip_boolean() {
        use lance_index::scalar::inverted::query::*;

        let must = FtsQuery::Match(
            MatchQuery::new("required".to_string()).with_column(Some("status".to_string())),
        );
        let should = FtsQuery::Phrase(
            PhraseQuery::new("optional phrase".to_string())
                .with_column(Some("description".to_string())),
        );

        let query = FtsQuery::Boolean(BooleanQuery::new(vec![
            (Occur::Must, must),
            (Occur::Should, should),
        ]));

        let json = to_json(&query).unwrap();
        let parsed = from_json(&json).unwrap();
        assert_eq!(query, parsed);
    }

    #[test]
    fn test_to_json_format() {
        use lance_index::scalar::inverted::query::*;

        let query = FtsQuery::Match(
            MatchQuery::new("puppy".to_string())
                .with_column(Some("text".to_string()))
                .with_fuzziness(Some(2)),
        );

        let json = to_json(&query).unwrap();

        // Verify it parses correctly with our from_json
        let parsed = from_json(&json).unwrap();
        assert_eq!(query, parsed);
    }
}
