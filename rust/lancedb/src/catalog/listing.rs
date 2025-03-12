// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Catalog implementation based on a local file system.

use std::collections::HashMap;
use std::fs::create_dir_all;
use std::path::Path;
use std::sync::Arc;

use super::{
    Catalog, CreateDatabaseMode, CreateDatabaseRequest, DatabaseNamesRequest, OpenDatabaseRequest,
};
use crate::connection::ConnectRequest;
use crate::database::listing::ListingDatabase;
use crate::database::Database;
use crate::error::{CreateDirSnafu, Error, Result};
use async_trait::async_trait;
use lance::io::{ObjectStore, ObjectStoreParams, ObjectStoreRegistry};
use lance_io::local::to_local_path;
use object_store::path::Path as ObjectStorePath;
use snafu::ResultExt;

/// A catalog implementation that works by listing subfolders in a directory
///
/// The listing catalog will be created with a base folder specified by the URI.  Every subfolder
/// in this base folder will be considered a database.  These will be opened as a
/// [`crate::database::listing::ListingDatabase`]
#[derive(Debug)]
pub struct ListingCatalog {
    object_store: ObjectStore,

    uri: String,

    base_path: ObjectStorePath,

    storage_options: HashMap<String, String>,
}

impl ListingCatalog {
    /// Try to create a local directory to store the lancedb dataset
    pub fn try_create_dir(path: &str) -> core::result::Result<(), std::io::Error> {
        let path = Path::new(path);
        if !path.try_exists()? {
            create_dir_all(path)?;
        }
        Ok(())
    }

    pub fn uri(&self) -> &str {
        &self.uri
    }

    async fn open_path(path: &str) -> Result<Self> {
        let (object_store, base_path) = ObjectStore::from_path(path).unwrap();
        if object_store.is_local() {
            Self::try_create_dir(path).context(CreateDirSnafu { path })?;
        }

        Ok(Self {
            uri: path.to_string(),
            base_path,
            object_store,
            storage_options: HashMap::new(),
        })
    }

    pub async fn connect(request: &ConnectRequest) -> Result<Self> {
        let uri = &request.uri;
        let parse_res = url::Url::parse(uri);

        match parse_res {
            Ok(url) if url.scheme().len() == 1 && cfg!(windows) => Self::open_path(uri).await,
            Ok(url) => {
                let plain_uri = url.to_string();

                let registry = Arc::new(ObjectStoreRegistry::default());
                let storage_options = request.storage_options.clone();
                let os_params = ObjectStoreParams {
                    storage_options: Some(storage_options.clone()),
                    ..Default::default()
                };
                let (object_store, base_path) =
                    ObjectStore::from_uri_and_params(registry, &plain_uri, &os_params).await?;
                if object_store.is_local() {
                    Self::try_create_dir(&plain_uri).context(CreateDirSnafu { path: plain_uri })?;
                }

                Ok(Self {
                    uri: String::from(url.clone()),
                    base_path,
                    object_store,
                    storage_options,
                })
            }
            Err(_) => Self::open_path(uri).await,
        }
    }

    fn database_path(&self, name: &str) -> ObjectStorePath {
        self.base_path.child(name.replace('\\', "/"))
    }
}

#[async_trait]
impl Catalog for ListingCatalog {
    async fn database_names(&self, request: DatabaseNamesRequest) -> Result<Vec<String>> {
        let mut f = self
            .object_store
            .read_dir(self.base_path.clone())
            .await?
            .iter()
            .map(Path::new)
            .filter_map(|p| p.file_name().and_then(|s| s.to_str().map(String::from)))
            .collect::<Vec<String>>();
        f.sort();

        if let Some(start_after) = request.start_after {
            let index = f
                .iter()
                .position(|name| name.as_str() > start_after.as_str())
                .unwrap_or(f.len());
            f.drain(0..index);
        }
        if let Some(limit) = request.limit {
            f.truncate(limit as usize);
        }
        Ok(f)
    }

    async fn create_database(&self, request: CreateDatabaseRequest) -> Result<Arc<dyn Database>> {
        let db_path = self.database_path(&request.name);
        let db_path_str = to_local_path(&db_path);
        let exists = Path::new(&db_path_str).exists();

        match request.mode {
            CreateDatabaseMode::Create if exists => {
                return Err(Error::DatabaseAlreadyExists { name: request.name })
            }
            CreateDatabaseMode::Create => {
                create_dir_all(db_path.to_string()).unwrap();
            }
            CreateDatabaseMode::ExistOk => {
                if !exists {
                    create_dir_all(db_path.to_string()).unwrap();
                }
            }
            CreateDatabaseMode::Overwrite => {
                if exists {
                    self.drop_database(&request.name).await?;
                }
                create_dir_all(db_path.to_string()).unwrap();
            }
        }

        let db_uri = format!("/{}/{}", self.base_path, request.name);

        let connect_request = ConnectRequest {
            uri: db_uri,
            api_key: None,
            region: None,
            host_override: None,
            #[cfg(feature = "remote")]
            client_config: Default::default(),
            read_consistency_interval: None,
            storage_options: self.storage_options.clone(),
            object_store: None,
        };

        Ok(Arc::new(
            ListingDatabase::connect_with_options(&connect_request).await?,
        ))
    }

    async fn open_database(&self, request: OpenDatabaseRequest) -> Result<Arc<dyn Database>> {
        let db_path = self.database_path(&request.name);

        let db_path_str = to_local_path(&db_path);
        let exists = Path::new(&db_path_str).exists();
        if !exists {
            return Err(Error::DatabaseNotFound { name: request.name });
        }

        let connect_request = ConnectRequest {
            uri: db_path.to_string(),
            api_key: None,
            region: None,
            host_override: None,
            #[cfg(feature = "remote")]
            client_config: Default::default(),
            read_consistency_interval: None,
            storage_options: self.storage_options.clone(),
            object_store: None,
        };

        Ok(Arc::new(
            ListingDatabase::connect_with_options(&connect_request).await?,
        ))
    }

    async fn rename_database(&self, _old_name: &str, _new_name: &str) -> Result<()> {
        Err(Error::NotSupported {
            message: "rename_database is not supported in LanceDB OSS yet".to_string(),
        })
    }

    async fn drop_database(&self, name: &str) -> Result<()> {
        let db_path = self.database_path(name);
        self.object_store
            .remove_dir_all(db_path.clone())
            .await
            .map_err(|err| match err {
                lance::Error::NotFound { .. } => Error::DatabaseNotFound {
                    name: name.to_owned(),
                },
                _ => Error::from(err),
            })?;

        Ok(())
    }

    async fn drop_all_databases(&self) -> Result<()> {
        self.object_store
            .remove_dir_all(self.base_path.clone())
            .await?;
        Ok(())
    }
}

#[cfg(all(test, not(windows)))]
mod tests {
    use super::*;

    /// file:/// URIs with drive letters do not work correctly on Windows
    #[cfg(windows)]
    fn path_to_uri(path: PathBuf) -> String {
        path.to_str().unwrap().to_string()
    }

    #[cfg(not(windows))]
    fn path_to_uri(path: PathBuf) -> String {
        Url::from_file_path(path).unwrap().to_string()
    }

    async fn setup_catalog() -> (TempDir, ListingCatalog) {
        let tempdir = tempfile::tempdir().unwrap();
        let catalog_path = tempdir.path().join("catalog");
        std::fs::create_dir_all(&catalog_path).unwrap();

        let uri = path_to_uri(catalog_path);

        let request = ConnectRequest {
            uri: uri.clone(),
            api_key: None,
            region: None,
            host_override: None,
            #[cfg(feature = "remote")]
            client_config: Default::default(),
            storage_options: HashMap::new(),
            read_consistency_interval: None,
            object_store: None,
        };

        let catalog = ListingCatalog::connect(&request).await.unwrap();

        (tempdir, catalog)
    }

    use crate::database::{CreateTableData, CreateTableRequest, TableNamesRequest};
    use crate::table::TableDefinition;
    use arrow_schema::Field;
    use std::path::PathBuf;
    use std::sync::Arc;
    use tempfile::{tempdir, TempDir};
    use url::Url;

    #[tokio::test]
    async fn test_database_names() {
        let (_tempdir, catalog) = setup_catalog().await;

        let names = catalog
            .database_names(DatabaseNamesRequest::default())
            .await
            .unwrap();
        assert!(names.is_empty());
    }

    #[tokio::test]
    async fn test_create_database() {
        let (_tempdir, catalog) = setup_catalog().await;

        catalog
            .create_database(CreateDatabaseRequest {
                name: "db1".into(),
                mode: CreateDatabaseMode::Create,
                options: HashMap::new(),
            })
            .await
            .unwrap();

        let names = catalog
            .database_names(DatabaseNamesRequest::default())
            .await
            .unwrap();
        assert_eq!(names, vec!["db1"]);
    }

    #[tokio::test]
    async fn test_create_database_exist_ok() {
        let (_tempdir, catalog) = setup_catalog().await;

        let db1 = catalog
            .create_database(CreateDatabaseRequest {
                name: "db_exist_ok".into(),
                mode: CreateDatabaseMode::ExistOk,
                options: HashMap::new(),
            })
            .await
            .unwrap();
        let dummy_schema = Arc::new(arrow_schema::Schema::new(Vec::<Field>::default()));
        db1.create_table(CreateTableRequest {
            name: "test_table".parse().unwrap(),
            data: CreateTableData::Empty(TableDefinition::new_from_schema(dummy_schema)),
            mode: Default::default(),
            write_options: Default::default(),
        })
        .await
        .unwrap();

        let db2 = catalog
            .create_database(CreateDatabaseRequest {
                name: "db_exist_ok".into(),
                mode: CreateDatabaseMode::ExistOk,
                options: HashMap::new(),
            })
            .await
            .unwrap();

        let tables = db2.table_names(TableNamesRequest::default()).await.unwrap();
        assert_eq!(tables, vec!["test_table".to_string()]);
    }

    #[tokio::test]
    async fn test_create_database_overwrite() {
        let (_tempdir, catalog) = setup_catalog().await;

        let db = catalog
            .create_database(CreateDatabaseRequest {
                name: "db_overwrite".into(),
                mode: CreateDatabaseMode::Create,
                options: HashMap::new(),
            })
            .await
            .unwrap();
        let dummy_schema = Arc::new(arrow_schema::Schema::new(Vec::<Field>::default()));
        db.create_table(CreateTableRequest {
            name: "old_table".parse().unwrap(),
            data: CreateTableData::Empty(TableDefinition::new_from_schema(dummy_schema)),
            mode: Default::default(),
            write_options: Default::default(),
        })
        .await
        .unwrap();
        let tables = db.table_names(TableNamesRequest::default()).await.unwrap();
        assert!(!tables.is_empty());

        let new_db = catalog
            .create_database(CreateDatabaseRequest {
                name: "db_overwrite".into(),
                mode: CreateDatabaseMode::Overwrite,
                options: HashMap::new(),
            })
            .await
            .unwrap();

        let tables = new_db
            .table_names(TableNamesRequest::default())
            .await
            .unwrap();
        assert!(tables.is_empty());
    }

    #[tokio::test]
    async fn test_create_database_overwrite_non_existing() {
        let (_tempdir, catalog) = setup_catalog().await;

        catalog
            .create_database(CreateDatabaseRequest {
                name: "new_db".into(),
                mode: CreateDatabaseMode::Overwrite,
                options: HashMap::new(),
            })
            .await
            .unwrap();

        let names = catalog
            .database_names(DatabaseNamesRequest::default())
            .await
            .unwrap();
        assert!(names.contains(&"new_db".to_string()));
    }

    #[tokio::test]
    async fn test_open_database() {
        let (_tempdir, catalog) = setup_catalog().await;

        // Test open non-existent
        let result = catalog
            .open_database(OpenDatabaseRequest {
                name: "missing".into(),
                database_options: HashMap::new(),
            })
            .await;
        assert!(matches!(
            result.unwrap_err(),
            Error::DatabaseNotFound { name } if name == "missing"
        ));

        // Create and open
        catalog
            .create_database(CreateDatabaseRequest {
                name: "valid_db".into(),
                mode: CreateDatabaseMode::Create,
                options: HashMap::new(),
            })
            .await
            .unwrap();

        let db = catalog
            .open_database(OpenDatabaseRequest {
                name: "valid_db".into(),
                database_options: HashMap::new(),
            })
            .await
            .unwrap();
        assert_eq!(
            db.table_names(TableNamesRequest::default()).await.unwrap(),
            Vec::<String>::new()
        );
    }

    #[tokio::test]
    async fn test_drop_database() {
        let (_tempdir, catalog) = setup_catalog().await;

        // Create test database
        catalog
            .create_database(CreateDatabaseRequest {
                name: "to_drop".into(),
                mode: CreateDatabaseMode::Create,
                options: HashMap::new(),
            })
            .await
            .unwrap();

        let names = catalog
            .database_names(DatabaseNamesRequest::default())
            .await
            .unwrap();
        assert!(!names.is_empty());

        // Drop database
        catalog.drop_database("to_drop").await.unwrap();

        let names = catalog
            .database_names(DatabaseNamesRequest::default())
            .await
            .unwrap();
        assert!(names.is_empty());
    }

    #[tokio::test]
    async fn test_drop_all_databases() {
        let (_tempdir, catalog) = setup_catalog().await;

        catalog
            .create_database(CreateDatabaseRequest {
                name: "db1".into(),
                mode: CreateDatabaseMode::Create,
                options: HashMap::new(),
            })
            .await
            .unwrap();
        catalog
            .create_database(CreateDatabaseRequest {
                name: "db2".into(),
                mode: CreateDatabaseMode::Create,
                options: HashMap::new(),
            })
            .await
            .unwrap();

        catalog.drop_all_databases().await.unwrap();

        let names = catalog
            .database_names(DatabaseNamesRequest::default())
            .await
            .unwrap();
        assert!(names.is_empty());
    }

    #[tokio::test]
    async fn test_rename_database_unsupported() {
        let (_tempdir, catalog) = setup_catalog().await;
        let result = catalog.rename_database("old", "new").await;
        assert!(matches!(
            result.unwrap_err(),
            Error::NotSupported { message } if message.contains("rename_database")
        ));
    }

    #[tokio::test]
    async fn test_connect_local_path() {
        let tmp_dir = tempdir().unwrap();
        let path = tmp_dir.path().to_str().unwrap();

        let request = ConnectRequest {
            uri: path.to_string(),
            api_key: None,
            region: None,
            host_override: None,
            #[cfg(feature = "remote")]
            client_config: Default::default(),
            storage_options: HashMap::new(),
            read_consistency_interval: None,
            object_store: None,
        };

        let catalog = ListingCatalog::connect(&request).await.unwrap();
        assert!(catalog.object_store.is_local());
        assert_eq!(catalog.uri, path);
    }

    #[tokio::test]
    async fn test_connect_file_scheme() {
        let tmp_dir = tempdir().unwrap();
        let path = tmp_dir.path();
        let uri = path_to_uri(path.to_path_buf());

        let request = ConnectRequest {
            uri: uri.clone(),
            api_key: None,
            region: None,
            host_override: None,
            #[cfg(feature = "remote")]
            client_config: Default::default(),
            storage_options: HashMap::new(),
            read_consistency_interval: None,
            object_store: None,
        };

        let catalog = ListingCatalog::connect(&request).await.unwrap();
        assert!(catalog.object_store.is_local());
        assert_eq!(catalog.uri, uri);
    }

    #[tokio::test]
    async fn test_connect_invalid_uri_fallback() {
        let invalid_uri = "invalid:///path";
        let request = ConnectRequest {
            uri: invalid_uri.to_string(),
            api_key: None,
            region: None,
            host_override: None,
            #[cfg(feature = "remote")]
            client_config: Default::default(),
            storage_options: HashMap::new(),
            read_consistency_interval: None,
            object_store: None,
        };

        let result = ListingCatalog::connect(&request).await;
        assert!(result.is_err());
    }
}
