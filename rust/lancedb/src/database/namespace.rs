// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Namespace-based database implementation that delegates table management to lance-namespace

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use lance_namespace::{LanceNamespace, NamespaceBuilder};
use lance_namespace_reqwest_client::models::{
    CreateEmptyTableRequest, CreateNamespaceRequest, DescribeTableRequest, DropNamespaceRequest,
    DropTableRequest, ListNamespacesRequest, ListTablesRequest,
};

use crate::connection::ConnectRequest;
use crate::database::listing::ListingDatabase;
use crate::error::{Error, Result};

use super::{
    BaseTable, CreateNamespaceRequest as DbCreateNamespaceRequest, CreateTableMode,
    CreateTableRequest as DbCreateTableRequest, Database,
    DropNamespaceRequest as DbDropNamespaceRequest,
    ListNamespacesRequest as DbListNamespacesRequest, OpenTableRequest, TableNamesRequest,
};

/// A database implementation that uses lance-namespace for table management
pub struct NamespaceBackedDatabase {
    namespace: Arc<dyn LanceNamespace>,
    // Storage options to be inherited by tables
    storage_options: HashMap<String, String>,
    // Read consistency interval for tables
    read_consistency_interval: Option<std::time::Duration>,
}

impl NamespaceBackedDatabase {
    /// Create a new namespace-backed database
    pub async fn connect(
        ns_impl: &str,
        ns_properties: HashMap<String, String>,
        storage_options: HashMap<String, String>,
        read_consistency_interval: Option<std::time::Duration>,
    ) -> Result<Self> {
        // Build the namespace using NamespaceBuilder
        let builder = NamespaceBuilder::new(ns_impl).with_properties(ns_properties);

        let namespace_box = builder.build().map_err(|e| Error::InvalidInput {
            message: format!("Failed to connect to namespace: {}", e),
        })?;

        // Convert Box<dyn LanceNamespace> to Arc<dyn LanceNamespace>
        let namespace: Arc<dyn LanceNamespace> = Arc::from(namespace_box);

        Ok(Self {
            namespace,
            storage_options,
            read_consistency_interval,
        })
    }
}

impl std::fmt::Debug for NamespaceBackedDatabase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NamespaceBackedDatabase")
            .field("storage_options", &self.storage_options)
            .field("read_consistency_interval", &self.read_consistency_interval)
            .finish()
    }
}

impl std::fmt::Display for NamespaceBackedDatabase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "NamespaceBackedDatabase")
    }
}

#[async_trait]
impl Database for NamespaceBackedDatabase {
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
        // Check mode - namespace operations don't support all modes
        match request.mode {
            CreateTableMode::Create => {}
            CreateTableMode::Overwrite => {
                // Try to drop the table first if it exists
                let mut table_id = request.namespace.clone();
                table_id.push(request.name.clone());
                let drop_request = DropTableRequest { id: Some(table_id) };
                let _ = self.namespace.drop_table(drop_request).await;
            }
            CreateTableMode::ExistOk(_) => {
                // Try to open the existing table
                let mut table_id = request.namespace.clone();
                table_id.push(request.name.clone());
                let describe_request = DescribeTableRequest {
                    id: Some(table_id),
                    version: None,
                };
                if let Ok(response) = self.namespace.describe_table(describe_request).await {
                    // Table exists, open it using ListingDatabase
                    let mut merged_storage_options = self.storage_options.clone();
                    if let Some(opts) = response.storage_options {
                        merged_storage_options.extend(opts);
                    }

                    let location = response.location.ok_or_else(|| Error::Runtime {
                        message: "Table location is missing from namespace response".to_string(),
                    })?;

                    // Get parent directory from table location
                    let parent_dir = if location.contains(".lance") {
                        location
                            .rsplit_once('/')
                            .map(|(parent, _)| parent.to_string())
                            .unwrap_or_else(|| location.clone())
                    } else {
                        location.clone()
                    };

                    // Create a ListingDatabase at the parent directory
                    let connect_request = ConnectRequest {
                        uri: parent_dir,
                        options: merged_storage_options,
                        read_consistency_interval: self.read_consistency_interval,
                        session: None,
                        #[cfg(feature = "remote")]
                        client_config: Default::default(),
                    };

                    let listing_db = Arc::new(
                        ListingDatabase::connect_with_options(&connect_request)
                            .await
                            .map_err(|e| Error::Runtime {
                                message: format!("Failed to create listing database: {}", e),
                            })?,
                    );

                    // Open the table using ListingDatabase
                    return listing_db
                        .open_table(OpenTableRequest {
                            name: request.name.clone(),
                            namespace: request.namespace.clone(),
                            index_cache_size: None,
                            lance_read_params: None,
                        })
                        .await;
                }
                // Table doesn't exist, continue with creation
            }
        }

        // Step 1: Create an empty table using namespace to get the location
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

        // Merge storage options from response
        let mut merged_storage_options = self.storage_options.clone();
        if let Some(opts) = create_empty_response.storage_options {
            merged_storage_options.extend(opts);
        }

        // Step 2: Get parent directory from table location
        // The location should be something like /path/to/db/table.lance
        // We need to extract the parent directory
        let parent_dir = if location.contains(".lance") {
            // Remove the table.lance part to get the parent directory
            location
                .rsplit_once('/')
                .map(|(parent, _)| parent.to_string())
                .unwrap_or_else(|| location.clone())
        } else {
            // If it doesn't contain .lance, assume it's already the parent directory
            location.clone()
        };

        // Step 3: Create a ListingDatabase at the parent directory
        let connect_request = ConnectRequest {
            uri: parent_dir,
            options: merged_storage_options,
            read_consistency_interval: self.read_consistency_interval,
            session: None,
            #[cfg(feature = "remote")]
            client_config: Default::default(),
        };

        let listing_db = Arc::new(
            ListingDatabase::connect_with_options(&connect_request)
                .await
                .map_err(|e| Error::Runtime {
                    message: format!("Failed to create listing database: {}", e),
                })?,
        );

        // Step 4: Use the ListingDatabase to create the table
        // Pass the request directly to the listing database
        listing_db.create_table(request).await
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

        // Merge storage options
        let mut merged_storage_options = self.storage_options.clone();
        if let Some(opts) = response.storage_options {
            merged_storage_options.extend(opts);
        }

        // Get table location
        let location = response.location.ok_or_else(|| Error::Runtime {
            message: "Table location is missing from namespace response".to_string(),
        })?;

        // Get parent directory from table location
        let parent_dir = if location.contains(".lance") {
            location
                .rsplit_once('/')
                .map(|(parent, _)| parent.to_string())
                .unwrap_or_else(|| location.clone())
        } else {
            location.clone()
        };

        // Create a ListingDatabase at the parent directory
        let connect_request = ConnectRequest {
            uri: parent_dir,
            options: merged_storage_options,
            read_consistency_interval: self.read_consistency_interval,
            session: None,
            #[cfg(feature = "remote")]
            client_config: Default::default(),
        };

        let listing_db = Arc::new(
            ListingDatabase::connect_with_options(&connect_request)
                .await
                .map_err(|e| Error::Runtime {
                    message: format!("Failed to create listing database: {}", e),
                })?,
        );

        // Open the table using ListingDatabase
        listing_db.open_table(request).await
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
