// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Namespace-based database implementation that delegates table management to lance-namespace

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use lance_namespace::{
    connect as connect_namespace,
    models::{
        CreateEmptyTableRequest, CreateNamespaceRequest, DescribeTableRequest,
        DropNamespaceRequest, DropTableRequest, ListNamespacesRequest, ListTablesRequest,
    },
    LanceNamespace,
};

use crate::database::listing::{ListingDatabase, OPT_MANIFEST_ENABLED};
use crate::error::{Error, Result};
use crate::{connection::ConnectRequest, database::ReadConsistency};

use super::{
    BaseTable, CloneTableRequest, CreateNamespaceRequest as DbCreateNamespaceRequest,
    CreateTableMode, CreateTableRequest as DbCreateTableRequest, Database,
    DropNamespaceRequest as DbDropNamespaceRequest,
    ListNamespacesRequest as DbListNamespacesRequest, OpenTableRequest, TableNamesRequest,
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
}

impl LanceNamespaceDatabase {
    pub async fn connect(
        ns_impl: &str,
        ns_properties: HashMap<String, String>,
        storage_options: HashMap<String, String>,
        read_consistency_interval: Option<std::time::Duration>,
        session: Option<Arc<lance::session::Session>>,
    ) -> Result<Self> {
        let namespace = connect_namespace(ns_impl, ns_properties.clone())
            .await
            .map_err(|e| Error::InvalidInput {
                message: format!("Failed to connect to namespace: {:?}", e),
            })?;

        Ok(Self {
            namespace,
            storage_options,
            read_consistency_interval,
            session,
            uri: format!("namespace://{}", ns_impl),
        })
    }

    /// Helper method to create a ListingDatabase from a table location
    ///
    /// This method:
    /// 1. Validates that the location ends with <table_name>.lance
    /// 2. Extracts the parent directory from the location
    /// 3. Creates a ListingDatabase at that parent directory
    async fn create_listing_database(
        &self,
        table_name: &str,
        location: &str,
        additional_storage_options: Option<HashMap<String, String>>,
    ) -> Result<Arc<ListingDatabase>> {
        let expected_suffix = format!("{}.lance", table_name);
        if !location.ends_with(&expected_suffix) {
            return Err(Error::Runtime {
                message: format!(
                    "Invalid table location '{}': expected to end with '{}'",
                    location, expected_suffix
                ),
            });
        }

        let parent_dir = location
            .rsplit_once('/')
            .map(|(parent, _)| parent.to_string())
            .ok_or_else(|| Error::Runtime {
                message: format!("Invalid table location '{}': no parent directory", location),
            })?;

        let mut merged_storage_options = self.storage_options.clone();
        if let Some(opts) = additional_storage_options {
            merged_storage_options.extend(opts);
        }

        // Disable manifest for namespace-managed tables
        merged_storage_options.insert(OPT_MANIFEST_ENABLED.to_string(), "false".to_string());

        let connect_request = ConnectRequest {
            uri: parent_dir,
            options: merged_storage_options,
            read_consistency_interval: self.read_consistency_interval,
            session: self.session.clone(),
            #[cfg(feature = "remote")]
            client_config: Default::default(),
        };

        let listing_db = ListingDatabase::connect_with_options(&connect_request)
            .await
            .map_err(|e| Error::Runtime {
                message: format!("Failed to create listing database: {}", e),
            })?;

        Ok(Arc::new(listing_db))
    }
}

impl std::fmt::Debug for LanceNamespaceDatabase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LanceNamespaceDatabase")
            .field("storage_options", &self.storage_options)
            .field("read_consistency_interval", &self.read_consistency_interval)
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

    async fn list_namespaces(&self, request: DbListNamespacesRequest) -> Result<Vec<String>> {
        let ns_request = ListNamespacesRequest {
            id: if request.namespace.is_empty() {
                None
            } else {
                Some(request.namespace)
            },
            page_token: request.page_token,
            limit: request.limit.map(|l| l as i32),
        };

        let response = self
            .namespace
            .list_namespaces(ns_request)
            .await
            .map_err(|e| Error::Runtime {
                message: format!("Failed to list namespaces: {}", e),
            })?;

        Ok(response.namespaces)
    }

    async fn create_namespace(&self, request: DbCreateNamespaceRequest) -> Result<()> {
        let ns_request = CreateNamespaceRequest {
            id: if request.namespace.is_empty() {
                None
            } else {
                Some(request.namespace)
            },
            mode: None,
            properties: None,
        };

        self.namespace
            .create_namespace(ns_request)
            .await
            .map_err(|e| Error::Runtime {
                message: format!("Failed to create namespace: {}", e),
            })?;

        Ok(())
    }

    async fn drop_namespace(&self, request: DbDropNamespaceRequest) -> Result<()> {
        let ns_request = DropNamespaceRequest {
            id: if request.namespace.is_empty() {
                None
            } else {
                Some(request.namespace)
            },
            mode: None,
            behavior: None,
        };

        self.namespace
            .drop_namespace(ns_request)
            .await
            .map_err(|e| Error::Runtime {
                message: format!("Failed to drop namespace: {}", e),
            })?;

        Ok(())
    }

    async fn table_names(&self, request: TableNamesRequest) -> Result<Vec<String>> {
        let ns_request = ListTablesRequest {
            id: if request.namespace.is_empty() {
                None
            } else {
                Some(request.namespace)
            },
            page_token: request.start_after,
            limit: request.limit.map(|l| l as i32),
        };

        let response =
            self.namespace
                .list_tables(ns_request)
                .await
                .map_err(|e| Error::Runtime {
                    message: format!("Failed to list tables: {}", e),
                })?;

        Ok(response.tables)
    }

    async fn create_table(&self, request: DbCreateTableRequest) -> Result<Arc<dyn BaseTable>> {
        let mut table_id = request.namespace.clone();
        table_id.push(request.name.clone());
        let describe_request = DescribeTableRequest {
            id: Some(table_id.clone()),
            version: None,
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
                if let Ok(response) = describe_result {
                    let location = response.location.ok_or_else(|| Error::Runtime {
                        message: "Table location is missing from namespace response".to_string(),
                    })?;

                    let listing_db = self
                        .create_listing_database(&request.name, &location, response.storage_options)
                        .await?;

                    return listing_db
                        .open_table(OpenTableRequest {
                            name: request.name.clone(),
                            namespace: vec![],
                            index_cache_size: None,
                            lance_read_params: None,
                        })
                        .await;
                }
            }
        }

        let mut table_id = request.namespace.clone();
        table_id.push(request.name.clone());

        let create_empty_request = CreateEmptyTableRequest {
            id: Some(table_id),
            location: None,
            properties: if self.storage_options.is_empty() {
                None
            } else {
                Some(self.storage_options.clone())
            },
        };

        let create_empty_response = self
            .namespace
            .create_empty_table(create_empty_request)
            .await
            .map_err(|e| Error::Runtime {
                message: format!("Failed to create empty table: {}", e),
            })?;

        let location = create_empty_response
            .location
            .ok_or_else(|| Error::Runtime {
                message: "Table location is missing from create_empty_table response".to_string(),
            })?;

        let listing_db = self
            .create_listing_database(
                &request.name,
                &location,
                create_empty_response.storage_options,
            )
            .await?;

        let create_request = DbCreateTableRequest {
            name: request.name,
            namespace: vec![],
            data: request.data,
            mode: request.mode,
            write_options: request.write_options,
        };
        listing_db.create_table(create_request).await
    }

    async fn open_table(&self, request: OpenTableRequest) -> Result<Arc<dyn BaseTable>> {
        let mut table_id = request.namespace.clone();
        table_id.push(request.name.clone());

        let describe_request = DescribeTableRequest {
            id: Some(table_id),
            version: None,
        };
        let response = self
            .namespace
            .describe_table(describe_request)
            .await
            .map_err(|e| Error::Runtime {
                message: format!("Failed to describe table: {}", e),
            })?;

        let location = response.location.ok_or_else(|| Error::Runtime {
            message: "Table location is missing from namespace response".to_string(),
        })?;

        let listing_db = self
            .create_listing_database(&request.name, &location, response.storage_options)
            .await?;

        let open_request = OpenTableRequest {
            name: request.name.clone(),
            namespace: vec![],
            index_cache_size: request.index_cache_size,
            lance_read_params: request.lance_read_params,
        };
        listing_db.open_table(open_request).await
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

        let drop_request = DropTableRequest { id: Some(table_id) };
        self.namespace
            .drop_table(drop_request)
            .await
            .map_err(|e| Error::Runtime {
                message: format!("Failed to drop table: {}", e),
            })?;

        Ok(())
    }

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

        // Test: Create a table
        let test_data = create_test_data();
        let table = conn
            .create_table("test_table", test_data)
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

        // Verify: Table appears in table_names
        let table_names = conn
            .table_names()
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

        // Create a table first
        let test_data = create_test_data();
        let _table = conn
            .create_table("describe_test", test_data)
            .execute()
            .await
            .expect("Failed to create table");

        // Test: Open the table (which internally uses describe_table)
        let opened_table = conn
            .open_table("describe_test")
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

        // Create initial table with 5 rows
        let test_data1 = create_test_data();
        let _table1 = conn
            .create_table("overwrite_test", test_data1)
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

        // Create initial table with test data
        let test_data1 = create_test_data();
        let _table1 = conn
            .create_table("exist_ok_test", test_data1)
            .execute()
            .await
            .expect("Failed to create table");

        // Try to create again with exist_ok mode
        let test_data2 = create_test_data();
        let table2 = conn
            .create_table("exist_ok_test", test_data2)
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

        // Create first table
        let test_data1 = create_test_data();
        let _table1 = conn
            .create_table("table1", test_data1)
            .execute()
            .await
            .expect("Failed to create first table");

        // Create second table
        let test_data2 = create_test_data();
        let _table2 = conn
            .create_table("table2", test_data2)
            .execute()
            .await
            .expect("Failed to create second table");

        // Verify: Both tables appear in table list
        let table_names = conn
            .table_names()
            .execute()
            .await
            .expect("Failed to list tables");

        assert!(table_names.contains(&"table1".to_string()));
        assert!(table_names.contains(&"table2".to_string()));

        // Verify: Can open both tables
        let opened_table1 = conn
            .open_table("table1")
            .execute()
            .await
            .expect("Failed to open table1");

        let opened_table2 = conn
            .open_table("table2")
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

        // Test: Try to open a non-existent table
        let result = conn.open_table("non_existent_table").execute().await;

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

        // Create a table first
        let test_data = create_test_data();
        let _table = conn
            .create_table("drop_test", test_data)
            .execute()
            .await
            .expect("Failed to create table");

        // Verify table exists
        let table_names_before = conn
            .table_names()
            .execute()
            .await
            .expect("Failed to list tables");
        assert!(table_names_before.contains(&"drop_test".to_string()));

        // Test: Drop the table
        conn.drop_table("drop_test", &[])
            .await
            .expect("Failed to drop table");

        // Verify: Table no longer exists
        let table_names_after = conn
            .table_names()
            .execute()
            .await
            .expect("Failed to list tables");
        assert!(!table_names_after.contains(&"drop_test".to_string()));

        // Verify: Cannot open dropped table
        let open_result = conn.open_table("drop_test").execute().await;
        assert!(open_result.is_err());
    }
}
