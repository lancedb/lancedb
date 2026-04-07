// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Vector search table function for DataFusion SQL integration.
//!
//! Enables vector similarity search via SQL:
//! ```sql
//! SELECT * FROM vector_search('my_table', '[0.1, 0.2, 0.3, ...]', 10)
//! ```

use std::sync::Arc;

use arrow_array::{Array, Float32Array};
use datafusion::catalog::TableFunctionImpl;
use datafusion_catalog::TableProvider;
use datafusion_common::{DataFusionError, Result as DataFusionResult, plan_err};
use datafusion_expr::Expr;

use super::{SearchQuery, TableResolver, extract_int_literal, extract_string_literal};
use crate::table::datafusion::VectorSearchParams;

/// Default number of results for vector search when top_k is not specified.
const DEFAULT_TOP_K: usize = 10;

/// Vector search table function for LanceDB tables.
///
/// Accepts 2-3 parameters: `vector_search(table_name, query_vector_json [, top_k])`
///
/// - `table_name`: Name of the table to search
/// - `query_vector_json`: JSON array of float values, e.g. `'[0.1, 0.2, 0.3]'`
/// - `top_k` (optional): Number of results to return (default: 10)
#[derive(Debug)]
pub struct VectorSearchTableFunction {
    resolver: Arc<dyn TableResolver>,
}

impl VectorSearchTableFunction {
    pub fn new(resolver: Arc<dyn TableResolver>) -> Self {
        Self { resolver }
    }
}

impl TableFunctionImpl for VectorSearchTableFunction {
    fn call(&self, exprs: &[Expr]) -> DataFusionResult<Arc<dyn TableProvider>> {
        if exprs.len() < 2 || exprs.len() > 3 {
            return plan_err!(
                "vector_search() requires 2-3 parameters: vector_search(table_name, query_vector_json [, top_k])"
            );
        }

        let table_name = extract_string_literal(&exprs[0], "table_name")?;
        let vector_json = extract_string_literal(&exprs[1], "query_vector_json")?;

        let top_k = if exprs.len() == 3 {
            extract_int_literal(&exprs[2], "top_k")?
        } else {
            DEFAULT_TOP_K
        };

        let query_vector = parse_vector_json(&vector_json)?;

        let params = VectorSearchParams {
            query_vector: query_vector as Arc<dyn Array>,
            column: None,
            top_k,
            distance_type: None,
            nprobes: None,
            ef: None,
            refine_factor: None,
        };

        self.resolver
            .resolve_table(&table_name, Some(SearchQuery::Vector(params)))
    }
}

/// Parse a JSON array of floats into an Arrow Float32Array for vector search.
///
/// Input format: `"[0.1, 0.2, 0.3, ...]"`
///
/// Returns a Float32Array whose length equals the vector dimension.
/// This is the format expected by LanceDB's vector search internals.
pub(crate) fn parse_vector_json(json: &str) -> DataFusionResult<Arc<Float32Array>> {
    let values: Vec<f32> = serde_json::from_str(json).map_err(|e| {
        DataFusionError::Plan(format!(
            "Invalid vector JSON: {}. Expected format: [0.1, 0.2, 0.3, ...]",
            e
        ))
    })?;

    if values.is_empty() {
        return Err(DataFusionError::Plan(
            "Vector must not be empty".to_string(),
        ));
    }

    Ok(Arc::new(Float32Array::from(values)))
}

#[cfg(test)]
mod tests {
    use super::super::{SearchQuery, TableResolver};
    use super::*;
    use crate::table::datafusion::BaseTableAdapter;
    use arrow_array::{FixedSizeListArray, Float32Array, Int32Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema as ArrowSchema};
    use datafusion::prelude::SessionContext;
    #[allow(unused_imports)]
    use lance_arrow::FixedSizeListArrayExt;

    /// Resolver that looks up tables in a HashMap and applies search queries
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

    fn make_test_data() -> (Arc<ArrowSchema>, RecordBatch) {
        let dim = 4;
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                "vector",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), dim),
                true,
            ),
        ]));

        let ids = Int32Array::from(vec![1, 2, 3, 4, 5]);
        // Create vectors: [1,0,0,0], [0,1,0,0], [0,0,1,0], [0,0,0,1], [1,1,0,0]
        let flat_values = Float32Array::from(vec![
            1.0, 0.0, 0.0, 0.0, // vec 1
            0.0, 1.0, 0.0, 0.0, // vec 2
            0.0, 0.0, 1.0, 0.0, // vec 3
            0.0, 0.0, 0.0, 1.0, // vec 4
            1.0, 1.0, 0.0, 0.0, // vec 5
        ]);
        let vector_array = FixedSizeListArray::try_new_from_values(flat_values, dim).unwrap();

        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(ids), Arc::new(vector_array)])
                .unwrap();

        (schema, batch)
    }

    #[tokio::test]
    async fn test_vector_search_udtf() {
        let (_schema, batch) = make_test_data();

        let db = crate::connect("memory://test_vec").execute().await.unwrap();
        let table = db.create_table("vectors", batch).execute().await.unwrap();

        // No index needed — vector search works with brute-force scan on small tables

        // Setup DataFusion context
        let ctx = SessionContext::new();
        let mut resolver = HashMapTableResolver::new();
        let adapter = BaseTableAdapter::try_new(table.base_table().clone())
            .await
            .unwrap();
        resolver.register("vectors".to_string(), Arc::new(adapter));

        let udtf = VectorSearchTableFunction::new(Arc::new(resolver));
        ctx.register_udtf("vector_search", Arc::new(udtf));

        // Search for vectors close to [1, 0, 0, 0]
        let query = "SELECT * FROM vector_search('vectors', '[1.0, 0.0, 0.0, 0.0]', 3)";
        let df = ctx.sql(query).await.unwrap();
        let results = df.collect().await.unwrap();

        assert!(!results.is_empty());
        let batch = &results[0];

        // Should have id, vector, _distance columns
        assert!(batch.schema().column_with_name("id").is_some());
        assert!(batch.schema().column_with_name("vector").is_some());
        assert!(
            batch.schema().column_with_name("_distance").is_some(),
            "_distance column should be present"
        );

        // Should return at most 3 results
        let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
        assert!(total_rows <= 3);
        assert!(total_rows > 0);
    }

    #[tokio::test]
    async fn test_vector_search_default_top_k() {
        let (_, batch) = make_test_data();

        let db = crate::connect("memory://test_vec_default")
            .execute()
            .await
            .unwrap();
        let table = db.create_table("vectors", batch).execute().await.unwrap();

        let ctx = SessionContext::new();
        let mut resolver = HashMapTableResolver::new();
        let adapter = BaseTableAdapter::try_new(table.base_table().clone())
            .await
            .unwrap();
        resolver.register("vectors".to_string(), Arc::new(adapter));

        let udtf = VectorSearchTableFunction::new(Arc::new(resolver));
        ctx.register_udtf("vector_search", Arc::new(udtf));

        // No top_k parameter — should default to 10
        let query = "SELECT * FROM vector_search('vectors', '[1.0, 0.0, 0.0, 0.0]')";
        let df = ctx.sql(query).await.unwrap();
        let results = df.collect().await.unwrap();

        let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
        // We only have 5 rows, so we should get all 5 back
        assert_eq!(total_rows, 5);
    }

    #[test]
    fn test_parse_vector_json() {
        let result = parse_vector_json("[1.0, 2.0, 3.0]").unwrap();
        assert_eq!(result.len(), 3); // 3-dimensional vector

        // Empty vector should fail
        assert!(parse_vector_json("[]").is_err());

        // Invalid JSON should fail
        assert!(parse_vector_json("not json").is_err());
    }
}
