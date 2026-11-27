// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

/// Mock implementations for testing
#[cfg(test)]
pub mod test_mocks {
    use crate::sql_executor::{SqlQueryExecutor, SqlResult};
    use anyhow::Result;
    use arrow_array::RecordBatch;
    use arrow_schema::{DataType, Field, Schema};
    use async_trait::async_trait;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    /// Mock SQL executor for testing
    pub struct MockSqlExecutor {
        pub is_connected: bool,
        pub queries: Arc<Mutex<Vec<String>>>,
        pub query_results: HashMap<String, Vec<RecordBatch>>,
        /// URLs passed to `connect`, in call order.
        pub connected_urls: Arc<Mutex<Vec<String>>>,
    }

    impl Default for MockSqlExecutor {
        fn default() -> Self {
            Self::new()
        }
    }

    impl MockSqlExecutor {
        pub fn new() -> Self {
            Self {
                is_connected: false,
                queries: Arc::new(Mutex::new(Vec::new())),
                query_results: HashMap::new(),
                connected_urls: Arc::new(Mutex::new(Vec::new())),
            }
        }

        pub fn set_query_result(&mut self, query: &str, result: Vec<RecordBatch>) {
            self.query_results.insert(query.to_string(), result);
        }

        pub fn get_executed_queries(&self) -> Vec<String> {
            self.queries.lock().unwrap().clone()
        }

        pub fn get_connected_urls(&self) -> Vec<String> {
            self.connected_urls.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl SqlQueryExecutor for MockSqlExecutor {
        async fn execute_query(&mut self, query: &str) -> Result<SqlResult> {
            self.queries.lock().unwrap().push(query.to_string());

            if let Some(result) = self.query_results.get(query) {
                Ok(SqlResult::Query(result.clone()))
            } else {
                // Return empty result by default
                Ok(SqlResult::Query(vec![]))
            }
        }

        async fn is_connected(&self) -> bool {
            self.is_connected
        }

        fn connection_info(&self) -> Option<String> {
            if self.is_connected {
                Some("Mock SQL connection".to_string())
            } else {
                None
            }
        }

        async fn connect(&mut self, server_url: &str) -> Result<()> {
            self.connected_urls
                .lock()
                .unwrap()
                .push(server_url.to_string());
            self.is_connected = true;
            Ok(())
        }

        fn set_database(&mut self, _database: String) {
            // Mock implementation - no-op
        }
    }

    /// Helper function to create a simple test schema
    pub fn create_test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("value", DataType::Float64, true),
        ]))
    }

    /// Helper function to create test RecordBatch
    pub fn create_test_batch() -> RecordBatch {
        use arrow_array::{Float64Array, Int64Array, StringArray};

        let id_array = Int64Array::from(vec![1, 2, 3]);
        let name_array = StringArray::from(vec!["Alice", "Bob", "Charlie"]);
        let value_array = Float64Array::from(vec![Some(10.5), None, Some(20.0)]);

        RecordBatch::try_new(
            create_test_schema(),
            vec![
                Arc::new(id_array),
                Arc::new(name_array),
                Arc::new(value_array),
            ],
        )
        .unwrap()
    }
}
