// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Provides the `ListingDatabase`, a simple database where tables are folders in a directory

use std::fs::create_dir_all;
use std::path::Path;
use std::{collections::HashMap, sync::Arc};

use lance::dataset::{ReadParams, WriteMode};
use lance::io::{ObjectStore, ObjectStoreParams, ObjectStoreRegistry, WrappingObjectStore};
use lance_datafusion::utils::StreamingWriteSource;
use lance_encoding::version::LanceFileVersion;
use lance_table::io::commit::commit_handler_from_url;
use object_store::local::LocalFileSystem;
use snafu::{OptionExt, ResultExt};

use crate::connection::ConnectRequest;
use crate::error::{CreateDirSnafu, Error, InvalidTableNameSnafu, Result};
use crate::io::object_store::MirroringObjectStoreWrapper;
use crate::table::NativeTable;
use crate::utils::validate_table_name;

use super::{
    BaseTable, CreateTableMode, CreateTableRequest, Database, DatabaseOptions, OpenTableRequest,
    TableNamesRequest,
};

/// File extension to indicate a lance table
pub const LANCE_FILE_EXTENSION: &str = "lance";

pub const OPT_NEW_TABLE_STORAGE_VERSION: &str = "new_table_data_storage_version";
pub const OPT_NEW_TABLE_V2_MANIFEST_PATHS: &str = "new_table_enable_v2_manifest_paths";

/// Controls how new tables should be created
#[derive(Clone, Debug, Default)]
pub struct NewTableConfig {
    /// The storage version to use for new tables
    ///
    /// If unset, then the latest stable version will be used
    pub data_storage_version: Option<LanceFileVersion>,
    /// Whether to enable V2 manifest paths for new tables
    ///
    /// V2 manifest paths are more efficient than V2 manifest paths but are not
    /// supported by old clients.
    pub enable_v2_manifest_paths: Option<bool>,
}

/// Options specific to the listing database
#[derive(Debug, Default, Clone)]
pub struct ListingDatabaseOptions {
    /// Controls what kind of Lance tables will be created by this database
    pub new_table_config: NewTableConfig,
}

impl ListingDatabaseOptions {
    fn parse_from_map(map: &HashMap<String, String>) -> Result<Self> {
        let new_table_config = NewTableConfig {
            data_storage_version: map
                .get(OPT_NEW_TABLE_STORAGE_VERSION)
                .map(|s| s.parse())
                .transpose()?,
            enable_v2_manifest_paths: map
                .get(OPT_NEW_TABLE_V2_MANIFEST_PATHS)
                .map(|s| {
                    s.parse::<bool>().map_err(|_| Error::InvalidInput {
                        message: format!(
                            "enable_v2_manifest_paths must be a boolean, received {}",
                            s
                        ),
                    })
                })
                .transpose()?,
        };
        Ok(Self { new_table_config })
    }
}

impl DatabaseOptions for ListingDatabaseOptions {
    fn serialize_into_map(&self, map: &mut HashMap<String, String>) {
        if let Some(storage_version) = &self.new_table_config.data_storage_version {
            map.insert(
                OPT_NEW_TABLE_STORAGE_VERSION.to_string(),
                storage_version.to_string(),
            );
        }
        if let Some(enable_v2_manifest_paths) = self.new_table_config.enable_v2_manifest_paths {
            map.insert(
                OPT_NEW_TABLE_V2_MANIFEST_PATHS.to_string(),
                enable_v2_manifest_paths.to_string(),
            );
        }
    }
}

/// A database that stores tables in a flat directory structure
///
/// Tables are stored as directories in the base path of the object store.
///
/// It is called a "listing database" because we use a "list directory" operation
/// to discover what tables are available.  Table names are determined from the directory
/// names.
///
/// For example, given the following directory structure:
///
/// ```text
/// /data
///  /table1.lance
///  /table2.lance
/// ```
///
/// We will have two tables named `table1` and `table2`.
#[derive(Debug)]
pub struct ListingDatabase {
    object_store: ObjectStore,
    query_string: Option<String>,

    pub(crate) uri: String,
    pub(crate) base_path: object_store::path::Path,

    // the object store wrapper to use on write path
    pub(crate) store_wrapper: Option<Arc<dyn WrappingObjectStore>>,

    read_consistency_interval: Option<std::time::Duration>,

    // Storage options to be inherited by tables created from this connection
    storage_options: HashMap<String, String>,

    // Options for tables created by this connection
    new_table_config: NewTableConfig,
}

impl std::fmt::Display for ListingDatabase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ListingDatabase(uri={}, read_consistency_interval={})",
            self.uri,
            match self.read_consistency_interval {
                None => {
                    "None".to_string()
                }
                Some(duration) => {
                    format!("{}s", duration.as_secs_f64())
                }
            }
        )
    }
}

const LANCE_EXTENSION: &str = "lance";
const ENGINE: &str = "engine";
const MIRRORED_STORE: &str = "mirroredStore";

/// A connection to LanceDB
impl ListingDatabase {
    /// Connect to a listing database
    ///
    /// The URI should be a path to a directory where the tables are stored.
    ///
    /// See [`ListingDatabaseOptions`] for options that can be set on the connection (via
    /// `storage_options`).
    pub async fn connect_with_options(request: &ConnectRequest) -> Result<Self> {
        let uri = &request.uri;
        let parse_res = url::Url::parse(uri);

        let options = ListingDatabaseOptions::parse_from_map(&request.storage_options)?;

        // TODO: pass params regardless of OS
        match parse_res {
            Ok(url) if url.scheme().len() == 1 && cfg!(windows) => {
                Self::open_path(
                    uri,
                    request.read_consistency_interval,
                    options.new_table_config,
                )
                .await
            }
            Ok(mut url) => {
                // iter thru the query params and extract the commit store param
                let mut engine = None;
                let mut mirrored_store = None;
                let mut filtered_querys = vec![];

                // WARNING: specifying engine is NOT a publicly supported feature in lancedb yet
                // THE API WILL CHANGE
                for (key, value) in url.query_pairs() {
                    if key == ENGINE {
                        engine = Some(value.to_string());
                    } else if key == MIRRORED_STORE {
                        if cfg!(windows) {
                            return Err(Error::NotSupported {
                                message: "mirrored store is not supported on windows".into(),
                            });
                        }
                        mirrored_store = Some(value.to_string());
                    } else {
                        // to owned so we can modify the url
                        filtered_querys.push((key.to_string(), value.to_string()));
                    }
                }

                // Filter out the commit store query param -- it's a lancedb param
                url.query_pairs_mut().clear();
                url.query_pairs_mut().extend_pairs(filtered_querys);
                // Take a copy of the query string so we can propagate it to lance
                let query_string = url.query().map(|s| s.to_string());
                // clear the query string so we can use the url as the base uri
                // use .set_query(None) instead of .set_query("") because the latter
                // will add a trailing '?' to the url
                url.set_query(None);

                let table_base_uri = if let Some(store) = engine {
                    static WARN_ONCE: std::sync::Once = std::sync::Once::new();
                    WARN_ONCE.call_once(|| {
                        log::warn!("Specifying engine is not a publicly supported feature in lancedb yet. THE API WILL CHANGE");
                    });
                    let old_scheme = url.scheme().to_string();
                    let new_scheme = format!("{}+{}", old_scheme, store);
                    url.to_string().replacen(&old_scheme, &new_scheme, 1)
                } else {
                    url.to_string()
                };

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

                let write_store_wrapper = match mirrored_store {
                    Some(path) => {
                        let mirrored_store = Arc::new(LocalFileSystem::new_with_prefix(path)?);
                        let wrapper = MirroringObjectStoreWrapper::new(mirrored_store);
                        Some(Arc::new(wrapper) as Arc<dyn WrappingObjectStore>)
                    }
                    None => None,
                };

                Ok(Self {
                    uri: table_base_uri,
                    query_string,
                    base_path,
                    object_store,
                    store_wrapper: write_store_wrapper,
                    read_consistency_interval: request.read_consistency_interval,
                    storage_options,
                    new_table_config: options.new_table_config,
                })
            }
            Err(_) => {
                Self::open_path(
                    uri,
                    request.read_consistency_interval,
                    options.new_table_config,
                )
                .await
            }
        }
    }

    async fn open_path(
        path: &str,
        read_consistency_interval: Option<std::time::Duration>,
        new_table_config: NewTableConfig,
    ) -> Result<Self> {
        let (object_store, base_path) = ObjectStore::from_uri(path).await?;
        if object_store.is_local() {
            Self::try_create_dir(path).context(CreateDirSnafu { path })?;
        }

        Ok(Self {
            uri: path.to_string(),
            query_string: None,
            base_path,
            object_store,
            store_wrapper: None,
            read_consistency_interval,
            storage_options: HashMap::new(),
            new_table_config,
        })
    }

    /// Try to create a local directory to store the lancedb dataset
    fn try_create_dir(path: &str) -> core::result::Result<(), std::io::Error> {
        let path = Path::new(path);
        if !path.try_exists()? {
            create_dir_all(path)?;
        }
        Ok(())
    }

    /// Get the URI of a table in the database.
    fn table_uri(&self, name: &str) -> Result<String> {
        validate_table_name(name)?;

        let path = Path::new(&self.uri);
        let table_uri = path.join(format!("{}.{}", name, LANCE_FILE_EXTENSION));

        let mut uri = table_uri
            .as_path()
            .to_str()
            .context(InvalidTableNameSnafu {
                name,
                reason: "Name is not valid URL",
            })?
            .to_string();

        // If there are query string set on the connection, propagate to lance
        if let Some(query) = self.query_string.as_ref() {
            uri.push('?');
            uri.push_str(query.as_str());
        }

        Ok(uri)
    }
}

#[async_trait::async_trait]
impl Database for ListingDatabase {
    async fn table_names(&self, request: TableNamesRequest) -> Result<Vec<String>> {
        let mut f = self
            .object_store
            .read_dir(self.base_path.clone())
            .await?
            .iter()
            .map(Path::new)
            .filter(|path| {
                let is_lance = path
                    .extension()
                    .and_then(|e| e.to_str())
                    .map(|e| e == LANCE_EXTENSION);
                is_lance.unwrap_or(false)
            })
            .filter_map(|p| p.file_stem().and_then(|s| s.to_str().map(String::from)))
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

    async fn create_table(&self, mut request: CreateTableRequest) -> Result<Arc<dyn BaseTable>> {
        let table_uri = self.table_uri(&request.name)?;
        // Inherit storage options from the connection
        let storage_options = request
            .write_options
            .lance_write_params
            .get_or_insert_with(Default::default)
            .store_params
            .get_or_insert_with(Default::default)
            .storage_options
            .get_or_insert_with(Default::default);
        for (key, value) in self.storage_options.iter() {
            if !storage_options.contains_key(key) {
                storage_options.insert(key.clone(), value.clone());
            }
        }

        let storage_options = storage_options.clone();

        let mut write_params = request.write_options.lance_write_params.unwrap_or_default();

        if let Some(storage_version) = &self.new_table_config.data_storage_version {
            write_params.data_storage_version = Some(*storage_version);
        } else {
            // Allow the user to override the storage version via storage options (backwards compatibility)
            if let Some(data_storage_version) = storage_options.get(OPT_NEW_TABLE_STORAGE_VERSION) {
                write_params.data_storage_version = Some(data_storage_version.parse()?);
            }
        }
        if let Some(enable_v2_manifest_paths) = self.new_table_config.enable_v2_manifest_paths {
            write_params.enable_v2_manifest_paths = enable_v2_manifest_paths;
        } else {
            // Allow the user to override the storage version via storage options (backwards compatibility)
            if let Some(enable_v2_manifest_paths) = storage_options
                .get(OPT_NEW_TABLE_V2_MANIFEST_PATHS)
                .map(|s| s.parse::<bool>().unwrap())
            {
                write_params.enable_v2_manifest_paths = enable_v2_manifest_paths;
            }
        }

        if matches!(&request.mode, CreateTableMode::Overwrite) {
            write_params.mode = WriteMode::Overwrite;
        }

        let data_schema = request.data.arrow_schema();

        match NativeTable::create(
            &table_uri,
            &request.name,
            request.data,
            self.store_wrapper.clone(),
            Some(write_params),
            self.read_consistency_interval,
        )
        .await
        {
            Ok(table) => Ok(Arc::new(table)),
            Err(Error::TableAlreadyExists { name }) => match request.mode {
                CreateTableMode::Create => Err(Error::TableAlreadyExists { name }),
                CreateTableMode::ExistOk(callback) => {
                    let req = OpenTableRequest {
                        name: request.name.clone(),
                        index_cache_size: None,
                        lance_read_params: None,
                    };
                    let req = (callback)(req);
                    let table = self.open_table(req).await?;

                    let table_schema = table.schema().await?;

                    if table_schema != data_schema {
                        return Err(Error::Schema {
                            message: "Provided schema does not match existing table schema"
                                .to_string(),
                        });
                    }

                    Ok(table)
                }
                CreateTableMode::Overwrite => unreachable!(),
            },
            Err(err) => Err(err),
        }
    }

    async fn open_table(&self, mut request: OpenTableRequest) -> Result<Arc<dyn BaseTable>> {
        let table_uri = self.table_uri(&request.name)?;

        // Inherit storage options from the connection
        let storage_options = request
            .lance_read_params
            .get_or_insert_with(Default::default)
            .store_options
            .get_or_insert_with(Default::default)
            .storage_options
            .get_or_insert_with(Default::default);
        for (key, value) in self.storage_options.iter() {
            if !storage_options.contains_key(key) {
                storage_options.insert(key.clone(), value.clone());
            }
        }

        // Some ReadParams are exposed in the OpenTableBuilder, but we also
        // let the user provide their own ReadParams.
        //
        // If we have a user provided ReadParams use that
        // If we don't then start with the default ReadParams and customize it with
        // the options from the OpenTableBuilder
        let read_params = request.lance_read_params.unwrap_or_else(|| {
            let mut default_params = ReadParams::default();
            if let Some(index_cache_size) = request.index_cache_size {
                default_params.index_cache_size = index_cache_size as usize;
            }
            default_params
        });

        let native_table = Arc::new(
            NativeTable::open_with_params(
                &table_uri,
                &request.name,
                self.store_wrapper.clone(),
                Some(read_params),
                self.read_consistency_interval,
            )
            .await?,
        );
        Ok(native_table)
    }

    async fn rename_table(&self, _old_name: &str, _new_name: &str) -> Result<()> {
        Err(Error::NotSupported {
            message: "rename_table is not supported in LanceDB OSS".to_string(),
        })
    }

    async fn drop_table(&self, name: &str) -> Result<()> {
        let dir_name = format!("{}.{}", name, LANCE_EXTENSION);
        let full_path = self.base_path.child(dir_name.clone());
        self.object_store
            .remove_dir_all(full_path.clone())
            .await
            .map_err(|err| match err {
                // this error is not lance::Error::DatasetNotFound,
                // as the method `remove_dir_all` may be used to remove something not be a dataset
                lance::Error::NotFound { .. } => Error::TableNotFound {
                    name: name.to_owned(),
                },
                _ => Error::from(err),
            })?;

        let object_store_params = ObjectStoreParams {
            storage_options: Some(self.storage_options.clone()),
            ..Default::default()
        };
        let mut uri = self.uri.clone();
        if let Some(query_string) = &self.query_string {
            uri.push_str(&format!("?{}", query_string));
        }
        let commit_handler = commit_handler_from_url(&uri, &Some(object_store_params))
            .await
            .unwrap();
        commit_handler.delete(&full_path).await.unwrap();
        Ok(())
    }

    async fn drop_all_tables(&self) -> Result<()> {
        self.object_store
            .remove_dir_all(self.base_path.clone())
            .await?;
        Ok(())
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}
