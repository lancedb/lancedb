// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Namespace-based database implementation that delegates table management to lance-namespace

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use lance_namespace::{
    models::{
        CreateNamespaceRequest, CreateNamespaceResponse, DeclareTableRequest,
        DescribeNamespaceRequest, DescribeNamespaceResponse, DescribeTableRequest,
        DropNamespaceRequest, DropNamespaceResponse, DropTableRequest, ListNamespacesRequest,
        ListNamespacesResponse, ListTablesRequest, ListTablesResponse,
    },
    LanceNamespace,
};
use lance_namespace_impls::ConnectBuilder;

use crate::database::ReadConsistency;
use crate::error::{Error, Result};
use crate::table::NativeTable;

use super::{
    BaseTable, CloneTableRequest, CreateTableMode, CreateTableRequest as DbCreateTableRequest,
    Database, OpenTableRequest, TableNamesRequest,
};

/// A database implementation that uses lance-namespace for table management
pub struct LanceNamespaceDatabase {
    namespace: Arc<dyn LanceNamespace>,
    // Storage options to be inherited by tables
    storage_options: HashMap<String, String>,
    // Read consistency interval for tables
    read_consistency_interval: Option<std::time::Duration>,
    // Optional session for object stores and caching
    session: Option<Arc<lance::session::Session>>,
    // database URI
    uri: String,
    // Whether to enable server-side query execution
    server_side_query_enabled: bool,
}

impl LanceNamespaceDatabase {
    pub async fn connect(
        ns_impl: &str,
        ns_properties: HashMap<String, String>,
        storage_options: HashMap<String, String>,
        read_consistency_interval: Option<std::time::Duration>,
        session: Option<Arc<lance::session::Session>>,
        server_side_query_enabled: bool,
    ) -> Result<Self> {
        let mut builder = ConnectBuilder::new(ns_impl);
        for (key, value) in ns_properties.clone() {
            builder = builder.property(key, value);
        }
        if let Some(ref sess) = session {
            builder = builder.session(sess.clone());
        }
        let namespace = builder.connect().await.map_err(|e| Error::InvalidInput {
            message: format!("Failed to connect to namespace: {:?}", e),
        })?;

        Ok(Self {
            namespace,
            storage_options,
            read_consistency_interval,
            session,
            uri: format!("namespace://{}", ns_impl),
            server_side_query_enabled,
        })
    }
}

impl std::fmt::Debug for LanceNamespaceDatabase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LanceNamespaceDatabase")
            .field("storage_options", &self.storage_options)
            .field("read_consistency_interval", &self.read_consistency_interval)
            .field("server_side_query_enabled", &self.server_side_query_enabled)
            .finish()
    }
}

impl std::fmt::Display for LanceNamespaceDatabase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LanceNamespaceDatabase")
    }
}

#[async_trait]
impl Database for LanceNamespaceDatabase {
    fn uri(&self) -> &str {
        &self.uri
    }

    async fn read_consistency(&self) -> Result<ReadConsistency> {
        if let Some(read_consistency_inverval) = self.read_consistency_interval {
            if read_consistency_inverval.is_zero() {
                Ok(ReadConsistency::Strong)
            } else {
                Ok(ReadConsistency::Eventual(read_consistency_inverval))
            }
        } else {
            Ok(ReadConsistency::Manual)
        }
    }

    async fn list_namespaces(
        &self,
        request: ListNamespacesRequest,
    ) -> Result<ListNamespacesResponse> {
        Ok(self.namespace.list_namespaces(request).await?)
    }

    async fn create_namespace(
        &self,
        request: CreateNamespaceRequest,
    ) -> Result<CreateNamespaceResponse> {
        Ok(self.namespace.create_namespace(request).await?)
    }

    async fn drop_namespace(&self, request: DropNamespaceRequest) -> Result<DropNamespaceResponse> {
        Ok(self.namespace.drop_namespace(request).await?)
    }

    async fn describe_namespace(
        &self,
        request: DescribeNamespaceRequest,
    ) -> Result<DescribeNamespaceResponse> {
        Ok(self.namespace.describe_namespace(request).await?)
    }

    async fn table_names(&self, request: TableNamesRequest) -> Result<Vec<String>> {
        let ns_request = ListTablesRequest {
            identity: None,
            context: None,
            id: Some(request.namespace),
            page_token: request.start_after,
            limit: request.limit.map(|l| l as i32),
        };

        let response = self.namespace.list_tables(ns_request).await?;

        Ok(response.tables)
    }

    async fn list_tables(&self, request: ListTablesRequest) -> Result<ListTablesResponse> {
        Ok(self.namespace.list_tables(request).await?)
    }

    async fn create_table(&self, request: DbCreateTableRequest) -> Result<Arc<dyn BaseTable>> {
        let mut table_id = request.namespace.clone();
        table_id.push(request.name.clone());
        let describe_request = DescribeTableRequest {
            identity: None,
            context: None,
            id: Some(table_id.clone()),
            version: None,
            with_table_uri: None,
            load_detailed_metadata: None,
            vend_credentials: None,
        };

        let describe_result = self.namespace.describe_table(describe_request).await;

        match request.mode {
            CreateTableMode::Create => {
                if describe_result.is_ok() {
                    return Err(Error::TableAlreadyExists {
                        name: request.name.clone(),
                    });
                }
            }
            CreateTableMode::Overwrite => {
                if describe_result.is_ok() {
                    // Drop the existing table - must succeed
                    let drop_request = DropTableRequest {
                        identity: None,
                        context: None,
                        id: Some(table_id.clone()),
                    };
                    self.namespace
                        .drop_table(drop_request)
                        .await
                        .map_err(|e| Error::Runtime {
                            message: format!("Failed to drop existing table for overwrite: {}", e),
                        })?;
                }
            }
            CreateTableMode::ExistOk(_) => {
                if describe_result.is_ok() {
                    let native_table = NativeTable::open_from_namespace(
                        self.namespace.clone(),
                        &request.name,
                        request.namespace.clone(),
                        None,
                        None,
                        self.read_consistency_interval,
                        self.server_side_query_enabled,
                        self.session.clone(),
                    )
                    .await?;

                    return Ok(Arc::new(native_table));
                }
            }
        }

        let mut table_id = request.namespace.clone();
        table_id.push(request.name.clone());

        let declare_request = DeclareTableRequest {
            identity: None,
            context: None,
            id: Some(table_id.clone()),
            location: None,
            vend_credentials: None,
        };

        let declare_response = self
            .namespace
            .declare_table(declare_request)
            .await
            .map_err(|e| Error::Runtime {
                message: format!("Failed to declare table: {}", e),
            })?;

        let location = declare_response.location.ok_or_else(|| Error::Runtime {
            message: "Table location is missing from declare_table response".to_string(),
        })?;

        let native_table = NativeTable::create_from_namespace(
            self.namespace.clone(),
            &location,
            &request.name,
            request.namespace.clone(),
            request.data,
            None, // write_store_wrapper not used for namespace connections
            request.write_options.lance_write_params,
            self.read_consistency_interval,
            self.server_side_query_enabled,
            self.session.clone(),
        )
        .await?;

        Ok(Arc::new(native_table))
    }

    async fn open_table(&self, request: OpenTableRequest) -> Result<Arc<dyn BaseTable>> {
        let native_table = NativeTable::open_from_namespace(
            self.namespace.clone(),
            &request.name,
            request.namespace.clone(),
            None, // write_store_wrapper not used for namespace connections
            request.lance_read_params,
            self.read_consistency_interval,
            self.server_side_query_enabled,
            self.session.clone(),
        )
        .await?;

        Ok(Arc::new(native_table))
    }

    async fn clone_table(&self, _request: CloneTableRequest) -> Result<Arc<dyn BaseTable>> {
        Err(Error::NotSupported {
            message: "clone_table is not supported for namespace connections".to_string(),
        })
    }

    async fn rename_table(
        &self,
        _cur_name: &str,
        _new_name: &str,
        _cur_namespace: &[String],
        _new_namespace: &[String],
    ) -> Result<()> {
        Err(Error::NotSupported {
            message: "rename_table is not supported for namespace connections".to_string(),
        })
    }

    async fn drop_table(&self, name: &str, namespace: &[String]) -> Result<()> {
        let mut table_id = namespace.to_vec();
        table_id.push(name.to_string());

        let drop_request = DropTableRequest {
            identity: None,
            context: None,
            id: Some(table_id),
        };
        self.namespace
            .drop_table(drop_request)
            .await
            .map_err(|e| Error::Runtime {
                message: format!("Failed to drop table: {}", e),
            })?;

        Ok(())
    }

    #[allow(deprecated)]
    async fn drop_all_tables(&self, namespace: &[String]) -> Result<()> {
        let tables = self
            .table_names(TableNamesRequest {
                namespace: namespace.to_vec(),
                start_after: None,
                limit: None,
            })
            .await?;

        for table in tables {
            self.drop_table(&table, namespace).await?;
        }

        Ok(())
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    async fn namespace_client(&self) -> Result<Arc<dyn LanceNamespace>> {
        Ok(self.namespace.clone())
    }
}

#[cfg(test)]
#[cfg(not(windows))] // TODO: support windows for lance-namespace
mod tests {
    use super::*;
    use crate::connect_namespace;
    use crate::query::ExecutableQuery;
    use arrow_array::{Int32Array, RecordBatch, RecordBatchIterator, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use futures::TryStreamExt;
    use tempfile::tempdir;

    /// Helper function to create test data
    fn create_test_data() -> RecordBatchIterator<
        std::vec::IntoIter<std::result::Result<RecordBatch, arrow_schema::ArrowError>>,
    > {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let id_array = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let name_array = StringArray::from(vec!["Alice", "Bob", "Charlie", "David", "Eve"]);

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(id_array), Arc::new(name_array)],
        )
        .unwrap();
        RecordBatchIterator::new(vec![std::result::Result::Ok(batch)].into_iter(), schema)
    }

    #[tokio::test]
    async fn test_namespace_connection_simple() {
        // Test that namespace connections work with simple connect_namespace(impl_type, properties)
        let tmp_dir = tempdir().unwrap();
        let root_path = tmp_dir.path().to_str().unwrap().to_string();

        let mut properties = HashMap::new();
        properties.insert("root".to_string(), root_path);

        // This should succeed with directory-based namespace
        let result = connect_namespace("dir", properties).execute().await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_namespace_connection_with_storage_options() {
        // Test namespace connections with storage options
        let tmp_dir = tempdir().unwrap();
        let root_path = tmp_dir.path().to_str().unwrap().to_string();

        let mut properties = HashMap::new();
        properties.insert("root".to_string(), root_path);

        // This should succeed with directory-based namespace and storage options
        let result = connect_namespace("dir", properties)
            .storage_option("timeout", "30s")
            .execute()
            .await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_namespace_connection_with_all_options() {
        use crate::embeddings::MemoryRegistry;
        use std::time::Duration;

        // Test namespace connections with all configuration options
        let tmp_dir = tempdir().unwrap();
        let root_path = tmp_dir.path().to_str().unwrap().to_string();

        let mut properties = HashMap::new();
        properties.insert("root".to_string(), root_path);

        let embedding_registry = Arc::new(MemoryRegistry::new());
        let session = Arc::new(lance::session::Session::default());

        // Test with all options set
        let result = connect_namespace("dir", properties)
            .storage_option("timeout", "30s")
            .storage_options([("cache_size", "1gb"), ("region", "us-east-1")])
            .read_consistency_interval(Duration::from_secs(5))
            .embedding_registry(embedding_registry.clone())
            .session(session.clone())
            .execute()
            .await;

        assert!(result.is_ok());

        let conn = result.unwrap();

        // Verify embedding registry is set correctly
        assert!(std::ptr::eq(
            conn.embedding_registry() as *const _,
            embedding_registry.as_ref() as *const _
        ));
    }

    #[tokio::test]
    async fn test_namespace_create_table_basic() {
        // Setup: Create a temporary directory for the namespace
        let tmp_dir = tempdir().unwrap();
        let root_path = tmp_dir.path().to_str().unwrap().to_string();

        // Connect to namespace using DirectoryNamespace
        let mut properties = HashMap::new();
        properties.insert("root".to_string(), root_path);

        let conn = connect_namespace("dir", properties)
            .execute()
            .await
            .expect("Failed to connect to namespace");

        // Create a child namespace first
        conn.create_namespace(CreateNamespaceRequest {
            identity: None,
            context: None,
            id: Some(vec!["test_ns".into()]),
            mode: None,
            properties: None,
        })
        .await
        .expect("Failed to create namespace");

        // Test: Create a table in the child namespace
        let test_data = create_test_data();
        let table = conn
            .create_table("test_table", test_data)
            .namespace(vec!["test_ns".into()])
            .execute()
            .await
            .expect("Failed to create table");

        // Verify: Table was created and can be queried
        let results = table
            .query()
            .execute()
            .await
            .expect("Failed to query table")
            .try_collect::<Vec<_>>()
            .await
            .expect("Failed to collect results");

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].num_rows(), 5);

        // Verify: Table namespace is correct
        assert_eq!(table.namespace(), &["test_ns"]);
        assert_eq!(table.name(), "test_table");
        assert_eq!(table.id(), "test_ns$test_table");

        // Verify: Table appears in table_names for the child namespace
        let table_names = conn
            .table_names()
            .namespace(vec!["test_ns".into()])
            .execute()
            .await
            .expect("Failed to list tables");
        assert!(table_names.contains(&"test_table".to_string()));
    }

    #[tokio::test]
    async fn test_namespace_describe_table() {
        // Setup: Create a temporary directory for the namespace
        let tmp_dir = tempdir().unwrap();
        let root_path = tmp_dir.path().to_str().unwrap().to_string();

        // Connect to namespace
        let mut properties = HashMap::new();
        properties.insert("root".to_string(), root_path);

        let conn = connect_namespace("dir", properties)
            .execute()
            .await
            .expect("Failed to connect to namespace");

        // Create a child namespace first
        conn.create_namespace(CreateNamespaceRequest {
            identity: None,
            context: None,
            id: Some(vec!["test_ns".into()]),
            mode: None,
            properties: None,
        })
        .await
        .expect("Failed to create namespace");

        // Create a table in child namespace
        let test_data = create_test_data();
        let _table = conn
            .create_table("describe_test", test_data)
            .namespace(vec!["test_ns".into()])
            .execute()
            .await
            .expect("Failed to create table");

        // Test: Open the table (which internally uses describe_table)
        let opened_table = conn
            .open_table("describe_test")
            .namespace(vec!["test_ns".into()])
            .execute()
            .await
            .expect("Failed to open table");

        // Verify: Can query the opened table
        let results = opened_table
            .query()
            .execute()
            .await
            .expect("Failed to query table")
            .try_collect::<Vec<_>>()
            .await
            .expect("Failed to collect results");

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].num_rows(), 5);

        // Verify schema matches
        let schema = opened_table.schema().await.expect("Failed to get schema");
        assert_eq!(schema.fields.len(), 2);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(1).name(), "name");

        // Verify namespace and id
        assert_eq!(opened_table.namespace(), &["test_ns"]);
        assert_eq!(opened_table.id(), "test_ns$describe_test");
    }

    #[tokio::test]
    async fn test_namespace_create_table_overwrite_mode() {
        // Setup: Create a temporary directory for the namespace
        let tmp_dir = tempdir().unwrap();
        let root_path = tmp_dir.path().to_str().unwrap().to_string();

        let mut properties = HashMap::new();
        properties.insert("root".to_string(), root_path);

        let conn = connect_namespace("dir", properties)
            .execute()
            .await
            .expect("Failed to connect to namespace");

        // Create a child namespace first
        conn.create_namespace(CreateNamespaceRequest {
            identity: None,
            context: None,
            id: Some(vec!["test_ns".into()]),
            mode: None,
            properties: None,
        })
        .await
        .expect("Failed to create namespace");

        // Create initial table with 5 rows in child namespace
        let test_data1 = create_test_data();
        let _table1 = conn
            .create_table("overwrite_test", test_data1)
            .namespace(vec!["test_ns".into()])
            .execute()
            .await
            .expect("Failed to create table");

        // Create new data with 3 rows
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let id_array = Int32Array::from(vec![10, 20, 30]);
        let name_array = StringArray::from(vec!["New1", "New2", "New3"]);
        let test_data2 = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(id_array), Arc::new(name_array)],
        )
        .unwrap();

        // Test: Overwrite the table
        let table2 = conn
            .create_table(
                "overwrite_test",
                RecordBatchIterator::new(
                    vec![std::result::Result::Ok(test_data2)].into_iter(),
                    schema,
                ),
            )
            .namespace(vec!["test_ns".into()])
            .mode(CreateTableMode::Overwrite)
            .execute()
            .await
            .expect("Failed to overwrite table");

        // Verify: Table has new data (3 rows instead of 5)
        let results = table2
            .query()
            .execute()
            .await
            .expect("Failed to query table")
            .try_collect::<Vec<_>>()
            .await
            .expect("Failed to collect results");

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].num_rows(), 3);

        // Verify the data is actually the new data
        let id_col = results[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(id_col.value(0), 10);
        assert_eq!(id_col.value(1), 20);
        assert_eq!(id_col.value(2), 30);
    }

    #[tokio::test]
    async fn test_namespace_create_table_exist_ok_mode() {
        // Setup: Create a temporary directory for the namespace
        let tmp_dir = tempdir().unwrap();
        let root_path = tmp_dir.path().to_str().unwrap().to_string();

        let mut properties = HashMap::new();
        properties.insert("root".to_string(), root_path);

        let conn = connect_namespace("dir", properties)
            .execute()
            .await
            .expect("Failed to connect to namespace");

        // Create a child namespace first
        conn.create_namespace(CreateNamespaceRequest {
            identity: None,
            context: None,
            id: Some(vec!["test_ns".into()]),
            mode: None,
            properties: None,
        })
        .await
        .expect("Failed to create namespace");

        // Create initial table with test data in child namespace
        let test_data1 = create_test_data();
        let _table1 = conn
            .create_table("exist_ok_test", test_data1)
            .namespace(vec!["test_ns".into()])
            .execute()
            .await
            .expect("Failed to create table");

        // Try to create again with exist_ok mode
        let test_data2 = create_test_data();
        let table2 = conn
            .create_table("exist_ok_test", test_data2)
            .namespace(vec!["test_ns".into()])
            .mode(CreateTableMode::exist_ok(|req| req))
            .execute()
            .await
            .expect("Failed with exist_ok mode");

        // Verify: Table still has original data (5 rows)
        let results = table2
            .query()
            .execute()
            .await
            .expect("Failed to query table")
            .try_collect::<Vec<_>>()
            .await
            .expect("Failed to collect results");

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].num_rows(), 5);
    }

    #[tokio::test]
    async fn test_namespace_create_multiple_tables() {
        // Setup: Create a temporary directory for the namespace
        let tmp_dir = tempdir().unwrap();
        let root_path = tmp_dir.path().to_str().unwrap().to_string();

        let mut properties = HashMap::new();
        properties.insert("root".to_string(), root_path);

        let conn = connect_namespace("dir", properties)
            .execute()
            .await
            .expect("Failed to connect to namespace");

        // Create a child namespace first
        conn.create_namespace(CreateNamespaceRequest {
            identity: None,
            context: None,
            id: Some(vec!["test_ns".into()]),
            mode: None,
            properties: None,
        })
        .await
        .expect("Failed to create namespace");

        // Create first table in child namespace
        let test_data1 = create_test_data();
        let _table1 = conn
            .create_table("table1", test_data1)
            .namespace(vec!["test_ns".into()])
            .execute()
            .await
            .expect("Failed to create first table");

        // Create second table in child namespace
        let test_data2 = create_test_data();
        let _table2 = conn
            .create_table("table2", test_data2)
            .namespace(vec!["test_ns".into()])
            .execute()
            .await
            .expect("Failed to create second table");

        // Verify: Both tables appear in table list for the child namespace
        let table_names = conn
            .table_names()
            .namespace(vec!["test_ns".into()])
            .execute()
            .await
            .expect("Failed to list tables");

        assert!(table_names.contains(&"table1".to_string()));
        assert!(table_names.contains(&"table2".to_string()));

        // Verify: Can open both tables
        let opened_table1 = conn
            .open_table("table1")
            .namespace(vec!["test_ns".into()])
            .execute()
            .await
            .expect("Failed to open table1");

        let opened_table2 = conn
            .open_table("table2")
            .namespace(vec!["test_ns".into()])
            .execute()
            .await
            .expect("Failed to open table2");

        // Verify both tables work
        let count1 = opened_table1
            .count_rows(None)
            .await
            .expect("Failed to count rows in table1");
        assert_eq!(count1, 5);

        let count2 = opened_table2
            .count_rows(None)
            .await
            .expect("Failed to count rows in table2");
        assert_eq!(count2, 5);
    }

    #[tokio::test]
    async fn test_namespace_table_not_found() {
        // Setup: Create a temporary directory for the namespace
        let tmp_dir = tempdir().unwrap();
        let root_path = tmp_dir.path().to_str().unwrap().to_string();

        let mut properties = HashMap::new();
        properties.insert("root".to_string(), root_path);

        let conn = connect_namespace("dir", properties)
            .execute()
            .await
            .expect("Failed to connect to namespace");

        // Create a child namespace first
        conn.create_namespace(CreateNamespaceRequest {
            identity: None,
            context: None,
            id: Some(vec!["test_ns".into()]),
            mode: None,
            properties: None,
        })
        .await
        .expect("Failed to create namespace");

        // Test: Try to open a non-existent table in the child namespace
        let result = conn
            .open_table("non_existent_table")
            .namespace(vec!["test_ns".into()])
            .execute()
            .await;

        // Verify: Should return an error
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_namespace_drop_table() {
        // Setup: Create a temporary directory for the namespace
        let tmp_dir = tempdir().unwrap();
        let root_path = tmp_dir.path().to_str().unwrap().to_string();

        let mut properties = HashMap::new();
        properties.insert("root".to_string(), root_path);

        let conn = connect_namespace("dir", properties)
            .execute()
            .await
            .expect("Failed to connect to namespace");

        // Create a child namespace first
        conn.create_namespace(CreateNamespaceRequest {
            identity: None,
            context: None,
            id: Some(vec!["test_ns".into()]),
            mode: None,
            properties: None,
        })
        .await
        .expect("Failed to create namespace");

        // Create a table in child namespace
        let test_data = create_test_data();
        let _table = conn
            .create_table("drop_test", test_data)
            .namespace(vec!["test_ns".into()])
            .execute()
            .await
            .expect("Failed to create table");

        // Verify table exists in child namespace
        let table_names_before = conn
            .table_names()
            .namespace(vec!["test_ns".into()])
            .execute()
            .await
            .expect("Failed to list tables");
        assert!(table_names_before.contains(&"drop_test".to_string()));

        // Test: Drop the table
        conn.drop_table("drop_test", &["test_ns".into()])
            .await
            .expect("Failed to drop table");

        // Verify: Table no longer exists
        let table_names_after = conn
            .table_names()
            .namespace(vec!["test_ns".into()])
            .execute()
            .await
            .expect("Failed to list tables");
        assert!(!table_names_after.contains(&"drop_test".to_string()));

        // Verify: Cannot open dropped table
        let open_result = conn.open_table("drop_test").execute().await;
        assert!(open_result.is_err());
    }

    #[tokio::test]
    async fn test_table_names_at_root() {
        // Test that table_names at root (empty namespace) works correctly
        // This is a regression test for a bug where empty namespace was converted to None
        let tmp_dir = tempdir().unwrap();
        let root_path = tmp_dir.path().to_str().unwrap().to_string();

        let mut properties = HashMap::new();
        properties.insert("root".to_string(), root_path);

        let conn = connect_namespace("dir", properties)
            .execute()
            .await
            .expect("Failed to connect to namespace");

        // Create multiple tables at root namespace
        let test_data1 = create_test_data();
        let _table1 = conn
            .create_table("table1", test_data1)
            .execute()
            .await
            .expect("Failed to create table1 at root");

        let test_data2 = create_test_data();
        let _table2 = conn
            .create_table("table2", test_data2)
            .execute()
            .await
            .expect("Failed to create table2 at root");

        // List tables at root using table_names (empty namespace means root)
        let table_names = conn
            .table_names()
            .execute()
            .await
            .expect("Failed to list tables at root");

        assert!(table_names.contains(&"table1".to_string()));
        assert!(table_names.contains(&"table2".to_string()));
        assert_eq!(table_names.len(), 2);
    }
}
