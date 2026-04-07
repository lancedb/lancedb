// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Hybrid search table function for DataFusion SQL integration.
//!
//! Combines vector similarity search with full-text search:
//! ```sql
//! SELECT * FROM hybrid_search(
//!     'my_table',
//!     '[0.1, 0.2, 0.3]',
//!     '{"match": {"column": "text", "terms": "search query"}}',
//!     10
//! )
//! ```

use std::sync::Arc;

use arrow_array::Array;
use datafusion::catalog::TableFunctionImpl;
use datafusion_catalog::TableProvider;
use datafusion_common::{DataFusionError, Result as DataFusionResult, plan_err};
use datafusion_expr::Expr;
use lance_index::scalar::FullTextSearchQuery;

use super::fts::from_json as fts_from_json;
use super::{SearchQuery, TableResolver, extract_int_literal, extract_string_literal};
use crate::table::datafusion::VectorSearchParams;

/// Default number of results for hybrid search when top_k is not specified.
const DEFAULT_TOP_K: usize = 10;

/// Hybrid search table function combining vector and full-text search.
///
/// Accepts 3-4 parameters: `hybrid_search(table_name, query_vector_json, fts_query_json [, top_k])`
///
/// - `table_name`: Name of the table to search
/// - `query_vector_json`: JSON array of float values, e.g. `'[0.1, 0.2, 0.3]'`
/// - `fts_query_json`: FTS query as JSON, e.g. `'{"match": {"column": "text", "terms": "query"}}'`
/// - `top_k` (optional): Number of results to return (default: 10)
#[derive(Debug)]
pub struct HybridSearchTableFunction {
    resolver: Arc<dyn TableResolver>,
}

impl HybridSearchTableFunction {
    pub fn new(resolver: Arc<dyn TableResolver>) -> Self {
        Self { resolver }
    }
}

impl TableFunctionImpl for HybridSearchTableFunction {
    fn call(&self, exprs: &[Expr]) -> DataFusionResult<Arc<dyn TableProvider>> {
        if exprs.len() < 3 || exprs.len() > 4 {
            return plan_err!(
                "hybrid_search() requires 3-4 parameters: hybrid_search(table_name, query_vector_json, fts_query_json [, top_k])"
            );
        }

        let table_name = extract_string_literal(&exprs[0], "table_name")?;
        let vector_json = extract_string_literal(&exprs[1], "query_vector_json")?;
        let fts_json = extract_string_literal(&exprs[2], "fts_query_json")?;

        let top_k = if exprs.len() == 4 {
            extract_int_literal(&exprs[3], "top_k")?
        } else {
            DEFAULT_TOP_K
        };

        let query_vector = parse_vector_json(&vector_json)?;
        let fts_query = parse_fts_query(&fts_json)?;

        let vector_params = VectorSearchParams {
            query_vector: query_vector as Arc<dyn Array>,
            column: None,
            top_k,
            distance_type: None,
            nprobes: None,
            ef: None,
            refine_factor: None,
        };

        self.resolver.resolve_table(
            &table_name,
            Some(SearchQuery::Hybrid {
                fts: fts_query,
                vector: vector_params,
            }),
        )
    }
}

fn parse_vector_json(json: &str) -> DataFusionResult<Arc<arrow_array::Float32Array>> {
    super::vector_search::parse_vector_json(json)
}

fn parse_fts_query(json: &str) -> DataFusionResult<FullTextSearchQuery> {
    let query = fts_from_json(json).map_err(|e| {
        DataFusionError::Plan(format!(
            "Invalid FTS query JSON: {}. Expected format: {{\"match\": {{\"column\": \"text\", \"terms\": \"query\"}} }}",
            e
        ))
    })?;
    Ok(FullTextSearchQuery::new_query(query))
}

#[cfg(test)]
mod tests {
    use super::super::fts::to_json;
    use super::super::{SearchQuery, TableResolver};
    use super::*;
    use crate::{index::Index, table::datafusion::BaseTableAdapter};
    use arrow_array::FixedSizeListArray;
    use arrow_array::{Float32Array, Int32Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema as ArrowSchema};
    use datafusion::prelude::SessionContext;
    #[allow(unused_imports)]
    use lance_arrow::FixedSizeListArrayExt;

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
            search: Option<SearchQuery>,
        ) -> DataFusionResult<Arc<dyn TableProvider>> {
            let table_provider = self
                .tables
                .get(name)
                .cloned()
                .ok_or_else(|| DataFusionError::Plan(format!("Table '{}' not found", name)))?;

            let Some(search) = search else {
                return Ok(table_provider);
            };

            let base_adapter = table_provider
                .as_any()
                .downcast_ref::<BaseTableAdapter>()
                .ok_or_else(|| {
                    DataFusionError::Internal(
                        "Expected BaseTableAdapter but got different type".to_string(),
                    )
                })?;

            match search {
                SearchQuery::Fts(fts_query) => Ok(Arc::new(base_adapter.with_fts_query(fts_query))),
                SearchQuery::Vector(vector_query) => {
                    Ok(Arc::new(base_adapter.with_vector_query(vector_query)))
                }
                SearchQuery::Hybrid { fts, vector } => {
                    Ok(Arc::new(base_adapter.with_hybrid_query(fts, vector)))
                }
            }
        }
    }

    #[tokio::test]
    async fn test_hybrid_search_udtf() {
        let dim = 4i32;
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("text", DataType::Utf8, false),
            Field::new(
                "vector",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), dim),
                true,
            ),
        ]));

        let ids = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let texts = StringArray::from(vec![
            "the quick brown fox",
            "jumps over the lazy dog",
            "a quick red fox runs",
            "the dog sleeps all day",
            "a brown fox and a quick dog",
        ]);
        let flat_values = Float32Array::from(vec![
            1.0, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0, 0.9, 0.1, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.5,
            0.5, 0.0, 0.0,
        ]);
        let vector_array = FixedSizeListArray::try_new_from_values(flat_values, dim).unwrap();

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(ids), Arc::new(texts), Arc::new(vector_array)],
        )
        .unwrap();

        let db = crate::connect("memory://test_hybrid")
            .execute()
            .await
            .unwrap();
        let table = db.create_table("docs", batch).execute().await.unwrap();

        // Create FTS index on text column
        table
            .create_index(&["text"], Index::FTS(Default::default()))
            .execute()
            .await
            .unwrap();

        let ctx = SessionContext::new();
        let mut resolver = HashMapTableResolver::new();
        let adapter = BaseTableAdapter::try_new(table.base_table().clone())
            .await
            .unwrap();
        resolver.register("docs".to_string(), Arc::new(adapter));

        let resolver = Arc::new(resolver);
        ctx.register_udtf(
            "hybrid_search",
            Arc::new(HybridSearchTableFunction::new(resolver.clone())),
        );

        // Run hybrid search: vector close to [1,0,0,0] AND FTS for "fox"
        use lance_index::scalar::inverted::query::*;
        let fts_query_struct = FtsQuery::Match(
            MatchQuery::new("fox".to_string()).with_column(Some("text".to_string())),
        );
        let fts_json = to_json(&fts_query_struct).unwrap();

        let query = format!(
            "SELECT * FROM hybrid_search('docs', '[1.0, 0.0, 0.0, 0.0]', '{}', 5)",
            fts_json
        );

        let df = ctx.sql(&query).await.unwrap();
        let results = df.collect().await.unwrap();

        assert!(!results.is_empty());
        let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
        assert!(total_rows > 0, "Should have at least one result");

        // Check schema has the expected columns
        let result_schema = results[0].schema();
        assert!(result_schema.column_with_name("id").is_some());
        assert!(result_schema.column_with_name("text").is_some());
        assert!(result_schema.column_with_name("vector").is_some());
    }
}
