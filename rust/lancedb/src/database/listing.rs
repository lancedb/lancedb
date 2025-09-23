// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Provides the `ListingDatabase`, a simple database where tables are folders in a directory

use std::fs::create_dir_all;
use std::path::Path;
use std::{collections::HashMap, sync::Arc};

use lance::dataset::refs::Ref;
use lance::dataset::{builder::DatasetBuilder, ReadParams, WriteMode};
use lance::io::{ObjectStore, ObjectStoreParams, WrappingObjectStore};
use lance_datafusion::utils::StreamingWriteSource;
use lance_encoding::version::LanceFileVersion;
use lance_table::io::commit::commit_handler_from_url;
use object_store::local::LocalFileSystem;
use snafu::ResultExt;

use crate::connection::ConnectRequest;
use crate::error::{CreateDirSnafu, Error, Result};
use crate::io::object_store::MirroringObjectStoreWrapper;
use crate::table::NativeTable;
use crate::utils::validate_table_name;

use super::{
    BaseTable, CloneTableRequest, CreateNamespaceRequest, CreateTableMode, CreateTableRequest,
    Database, DatabaseOptions, DropNamespaceRequest, ListNamespacesRequest, OpenTableRequest,
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
    /// Storage options configure the storage layer (e.g. S3, GCS, Azure, etc.)
    ///
    /// These are used to create/list tables and they are inherited by all tables
    /// opened by this database.
    ///
    /// See available options at <https://lancedb.github.io/lancedb/guides/storage/>
    pub storage_options: HashMap<String, String>,
}

impl ListingDatabaseOptions {
    /// Create a new builder for the listing database options
    pub fn builder() -> ListingDatabaseOptionsBuilder {
        ListingDatabaseOptionsBuilder::new()
    }

    pub(crate) fn parse_from_map(map: &HashMap<String, String>) -> Result<Self> {
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
        // We just assume that any options that are not new table config options are storage options
        let storage_options = map
            .iter()
            .filter(|(key, _)| {
                key.as_str() != OPT_NEW_TABLE_STORAGE_VERSION
                    && key.as_str() != OPT_NEW_TABLE_V2_MANIFEST_PATHS
            })
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        Ok(Self {
            new_table_config,
            storage_options,
        })
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

#[derive(Clone, Debug, Default)]
pub struct ListingDatabaseOptionsBuilder {
    options: ListingDatabaseOptions,
}

impl ListingDatabaseOptionsBuilder {
    pub fn new() -> Self {
        Self {
            options: ListingDatabaseOptions::default(),
        }
    }
}

impl ListingDatabaseOptionsBuilder {
    /// Set the storage version to use for new tables
    ///
    /// # Arguments
    ///
    /// * `data_storage_version` - The storage version to use for new tables
    pub fn data_storage_version(mut self, data_storage_version: LanceFileVersion) -> Self {
        self.options.new_table_config.data_storage_version = Some(data_storage_version);
        self
    }

    /// Enable V2 manifest paths for new tables
    ///
    /// # Arguments
    ///
    /// * `enable_v2_manifest_paths` - Whether to enable V2 manifest paths for new tables
    pub fn enable_v2_manifest_paths(mut self, enable_v2_manifest_paths: bool) -> Self {
        self.options.new_table_config.enable_v2_manifest_paths = Some(enable_v2_manifest_paths);
        self
    }

    /// Set an option for the storage layer.
    ///
    /// See available options at <https://lancedb.github.io/lancedb/guides/storage/>
    pub fn storage_option(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.options
            .storage_options
            .insert(key.into(), value.into());
        self
    }

    /// Set multiple options for the storage layer.
    ///
    /// See available options at <https://lancedb.github.io/lancedb/guides/storage/>
    pub fn storage_options(
        mut self,
        pairs: impl IntoIterator<Item = (impl Into<String>, impl Into<String>)>,
    ) -> Self {
        for (key, value) in pairs {
            self.options
                .storage_options
                .insert(key.into(), value.into());
        }
        self
    }

    /// Build the options
    pub fn build(self) -> ListingDatabaseOptions {
        self.options
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
    object_store: Arc<ObjectStore>,
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

    // Session for object stores and caching
    session: Arc<lance::session::Session>,
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

        let options = ListingDatabaseOptions::parse_from_map(&request.options)?;

        // TODO: pass params regardless of OS
        match parse_res {
            Ok(url) if url.scheme().len() == 1 && cfg!(windows) => {
                Self::open_path(
                    uri,
                    request.read_consistency_interval,
                    options.new_table_config,
                    request.session.clone(),
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

                let session = request
                    .session
                    .clone()
                    .unwrap_or_else(|| Arc::new(lance::session::Session::default()));
                let os_params = ObjectStoreParams {
                    storage_options: Some(options.storage_options.clone()),
                    ..Default::default()
                };
                let (object_store, base_path) = ObjectStore::from_uri_and_params(
                    session.store_registry(),
                    &plain_uri,
                    &os_params,
                )
                .await?;
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
                    storage_options: options.storage_options,
                    new_table_config: options.new_table_config,
                    session,
                })
            }
            Err(_) => {
                Self::open_path(
                    uri,
                    request.read_consistency_interval,
                    options.new_table_config,
                    request.session.clone(),
                )
                .await
            }
        }
    }

    async fn open_path(
        path: &str,
        read_consistency_interval: Option<std::time::Duration>,
        new_table_config: NewTableConfig,
        session: Option<Arc<lance::session::Session>>,
    ) -> Result<Self> {
        let session = session.unwrap_or_else(|| Arc::new(lance::session::Session::default()));
        let (object_store, base_path) = ObjectStore::from_uri_and_params(
            session.store_registry(),
            path,
            &ObjectStoreParams::default(),
        )
        .await?;
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
            session,
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

        let mut uri = self.uri.clone();
        // If the URI does not end with a slash, add one
        if !uri.ends_with('/') {
            uri.push('/');
        }
        // Append the table name with the lance file extension
        uri.push_str(&format!("{}.{}", name, LANCE_FILE_EXTENSION));

        // If there are query string set on the connection, propagate to lance
        if let Some(query) = self.query_string.as_ref() {
            uri.push('?');
            uri.push_str(query.as_str());
        }

        Ok(uri)
    }

    async fn drop_tables(&self, names: Vec<String>) -> Result<()> {
        let object_store_params = ObjectStoreParams {
            storage_options: Some(self.storage_options.clone()),
            ..Default::default()
        };
        let mut uri = self.uri.clone();
        if let Some(query_string) = &self.query_string {
            uri.push_str(&format!("?{}", query_string));
        }
        let commit_handler = commit_handler_from_url(&uri, &Some(object_store_params)).await?;
        for name in names {
            let dir_name = format!("{}.{}", name, LANCE_EXTENSION);
            let full_path = self.base_path.child(dir_name.clone());

            commit_handler.delete(&full_path).await?;

            self.object_store
                .remove_dir_all(full_path.clone())
                .await
                .map_err(|err| match err {
                    // this error is not lance::Error::DatasetNotFound, as the method
                    // `remove_dir_all` may be used to remove something not be a dataset
                    lance::Error::NotFound { .. } => Error::TableNotFound {
                        name: name.to_owned(),
                    },
                    _ => Error::from(err),
                })?;
        }
        Ok(())
    }

    /// Inherit storage options from the connection into the target map
    fn inherit_storage_options(&self, target: &mut HashMap<String, String>) {
        for (key, value) in self.storage_options.iter() {
            if !target.contains_key(key) {
                target.insert(key.clone(), value.clone());
            }
        }
    }

    /// Extract storage option overrides from the request
    fn extract_storage_overrides(
        &self,
        request: &CreateTableRequest,
    ) -> Result<(Option<LanceFileVersion>, Option<bool>)> {
        let storage_options = request
            .write_options
            .lance_write_params
            .as_ref()
            .and_then(|p| p.store_params.as_ref())
            .and_then(|sp| sp.storage_options.as_ref());

        let storage_version_override = storage_options
            .and_then(|opts| opts.get(OPT_NEW_TABLE_STORAGE_VERSION))
            .map(|s| s.parse::<LanceFileVersion>())
            .transpose()?;

        let v2_manifest_override = storage_options
            .and_then(|opts| opts.get(OPT_NEW_TABLE_V2_MANIFEST_PATHS))
            .map(|s| s.parse::<bool>())
            .transpose()
            .map_err(|_| Error::InvalidInput {
                message: "enable_v2_manifest_paths must be a boolean".to_string(),
            })?;

        Ok((storage_version_override, v2_manifest_override))
    }

    /// Prepare write parameters for table creation
    fn prepare_write_params(
        &self,
        request: &CreateTableRequest,
        storage_version_override: Option<LanceFileVersion>,
        v2_manifest_override: Option<bool>,
    ) -> lance::dataset::WriteParams {
        let mut write_params = request
            .write_options
            .lance_write_params
            .clone()
            .unwrap_or_default();

        // Only modify the storage options if we actually have something to
        // inherit. There is a difference between storage_options=None and
        // storage_options=Some({}). Using storage_options=None will cause the
        // connection's session store registry to be used. Supplying Some({})
        // will cause a new connection to be created, and that connection will
        // be dropped from the cache when python GCs the table object, which
        // confounds reuse across tables.
        if !self.storage_options.is_empty() {
            let storage_options = write_params
                .store_params
                .get_or_insert_with(Default::default)
                .storage_options
                .get_or_insert_with(Default::default);
            self.inherit_storage_options(storage_options);
        }

        write_params.data_storage_version = self
            .new_table_config
            .data_storage_version
            .or(storage_version_override);

        if let Some(enable_v2_manifest_paths) = self
            .new_table_config
            .enable_v2_manifest_paths
            .or(v2_manifest_override)
        {
            write_params.enable_v2_manifest_paths = enable_v2_manifest_paths;
        }

        if matches!(&request.mode, CreateTableMode::Overwrite) {
            write_params.mode = WriteMode::Overwrite;
        }

        write_params.session = Some(self.session.clone());

        write_params
    }

    /// Handle the case where table already exists based on the create mode
    async fn handle_table_exists(
        &self,
        table_name: &str,
        namespace: Vec<String>,
        mode: CreateTableMode,
        data_schema: &arrow_schema::Schema,
    ) -> Result<Arc<dyn BaseTable>> {
        match mode {
            CreateTableMode::Create => Err(Error::TableAlreadyExists {
                name: table_name.to_string(),
            }),
            CreateTableMode::ExistOk(callback) => {
                let req = OpenTableRequest {
                    name: table_name.to_string(),
                    namespace: namespace.clone(),
                    index_cache_size: None,
                    lance_read_params: None,
                };
                let req = (callback)(req);
                let table = self.open_table(req).await?;

                let table_schema = table.schema().await?;

                if table_schema.as_ref() != data_schema {
                    return Err(Error::Schema {
                        message: "Provided schema does not match existing table schema".to_string(),
                    });
                }

                Ok(table)
            }
            CreateTableMode::Overwrite => unreachable!(),
        }
    }
}

#[async_trait::async_trait]
impl Database for ListingDatabase {
    async fn list_namespaces(&self, request: ListNamespacesRequest) -> Result<Vec<String>> {
        if !request.namespace.is_empty() {
            return Err(Error::NotSupported {
                message: "Namespace operations are not supported for listing database".into(),
            });
        }

        Ok(Vec::new())
    }

    async fn create_namespace(&self, _request: CreateNamespaceRequest) -> Result<()> {
        Err(Error::NotSupported {
            message: "Namespace operations are not supported for listing database".into(),
        })
    }

    async fn drop_namespace(&self, _request: DropNamespaceRequest) -> Result<()> {
        Err(Error::NotSupported {
            message: "Namespace operations are not supported for listing database".into(),
        })
    }

    async fn table_names(&self, request: TableNamesRequest) -> Result<Vec<String>> {
        if !request.namespace.is_empty() {
            return Err(Error::NotSupported {
                message: "Namespace parameter is not supported for listing database. Only root namespace is supported.".into(),
            });
        }
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

    async fn create_table(&self, request: CreateTableRequest) -> Result<Arc<dyn BaseTable>> {
        if !request.namespace.is_empty() {
            return Err(Error::NotSupported {
                message: "Namespace parameter is not supported for listing database. Only root namespace is supported.".into(),
            });
        }
        let table_uri = self.table_uri(&request.name)?;

        let (storage_version_override, v2_manifest_override) =
            self.extract_storage_overrides(&request)?;

        let write_params =
            self.prepare_write_params(&request, storage_version_override, v2_manifest_override);

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
            Err(Error::TableAlreadyExists { .. }) => {
                self.handle_table_exists(
                    &request.name,
                    request.namespace.clone(),
                    request.mode,
                    &data_schema,
                )
                .await
            }
            Err(err) => Err(err),
        }
    }

    async fn clone_table(&self, request: CloneTableRequest) -> Result<Arc<dyn BaseTable>> {
        if !request.target_namespace.is_empty() {
            return Err(Error::NotSupported {
                message: "Namespace parameter is not supported for listing database. Only root namespace is supported.".into(),
            });
        }

        // TODO: support deep clone
        if !request.is_shallow {
            return Err(Error::NotSupported {
                message: "Deep clone is not yet implemented".to_string(),
            });
        }

        validate_table_name(&request.target_table_name)?;

        let storage_params = ObjectStoreParams {
            storage_options: Some(self.storage_options.clone()),
            ..Default::default()
        };
        let read_params = ReadParams {
            store_options: Some(storage_params.clone()),
            session: Some(self.session.clone()),
            ..Default::default()
        };

        let mut source_dataset = DatasetBuilder::from_uri(&request.source_uri)
            .with_read_params(read_params.clone())
            .load()
            .await
            .map_err(|e| Error::Lance { source: e })?;

        let version_ref = match (request.source_version, request.source_tag) {
            (Some(v), None) => Ok(Ref::Version(v)),
            (None, Some(tag)) => Ok(Ref::Tag(tag)),
            (None, None) => Ok(Ref::Version(source_dataset.version().version)),
            _ => Err(Error::InvalidInput {
                message: "Cannot specify both source_version and source_tag".to_string(),
            }),
        }?;

        let target_uri = self.table_uri(&request.target_table_name)?;
        source_dataset
            .shallow_clone(&target_uri, version_ref, storage_params)
            .await
            .map_err(|e| Error::Lance { source: e })?;

        let cloned_table = NativeTable::open_with_params(
            &target_uri,
            &request.target_table_name,
            self.store_wrapper.clone(),
            None,
            self.read_consistency_interval,
        )
        .await?;

        Ok(Arc::new(cloned_table))
    }

    async fn open_table(&self, mut request: OpenTableRequest) -> Result<Arc<dyn BaseTable>> {
        if !request.namespace.is_empty() {
            return Err(Error::NotSupported {
                message: "Namespace parameter is not supported for listing database. Only root namespace is supported.".into(),
            });
        }
        let table_uri = self.table_uri(&request.name)?;

        // Only modify the storage options if we actually have something to
        // inherit. There is a difference between storage_options=None and
        // storage_options=Some({}). Using storage_options=None will cause the
        // connection's session store registry to be used. Supplying Some({})
        // will cause a new connection to be created, and that connection will
        // be dropped from the cache when python GCs the table object, which
        // confounds reuse across tables.
        if !self.storage_options.is_empty() {
            let storage_options = request
                .lance_read_params
                .get_or_insert_with(Default::default)
                .store_options
                .get_or_insert_with(Default::default)
                .storage_options
                .get_or_insert_with(Default::default);
            self.inherit_storage_options(storage_options);
        }

        // Some ReadParams are exposed in the OpenTableBuilder, but we also
        // let the user provide their own ReadParams.
        //
        // If we have a user provided ReadParams use that
        // If we don't then start with the default ReadParams and customize it with
        // the options from the OpenTableBuilder
        let mut read_params = request.lance_read_params.unwrap_or_else(|| {
            let mut default_params = ReadParams::default();
            if let Some(index_cache_size) = request.index_cache_size {
                #[allow(deprecated)]
                default_params.index_cache_size(index_cache_size as usize);
            }
            default_params
        });
        read_params.session(self.session.clone());

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

    async fn rename_table(
        &self,
        _cur_name: &str,
        _new_name: &str,
        cur_namespace: &[String],
        new_namespace: &[String],
    ) -> Result<()> {
        if !cur_namespace.is_empty() {
            return Err(Error::NotSupported {
                message: "Namespace parameter is not supported for listing database.".into(),
            });
        }
        if !new_namespace.is_empty() {
            return Err(Error::NotSupported {
                message: "Namespace parameter is not supported for listing database.".into(),
            });
        }
        Err(Error::NotSupported {
            message: "rename_table is not supported in LanceDB OSS".into(),
        })
    }

    async fn drop_table(&self, name: &str, namespace: &[String]) -> Result<()> {
        if !namespace.is_empty() {
            return Err(Error::NotSupported {
                message: "Namespace parameter is not supported for listing database.".into(),
            });
        }
        self.drop_tables(vec![name.to_string()]).await
    }

    async fn drop_all_tables(&self, namespace: &[String]) -> Result<()> {
        // Check if namespace parameter is provided
        if !namespace.is_empty() {
            return Err(Error::NotSupported {
                message: "Namespace parameter is not supported for listing database.".into(),
            });
        }
        let tables = self.table_names(TableNamesRequest::default()).await?;
        self.drop_tables(tables).await
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection::ConnectRequest;
    use crate::database::{CreateTableData, CreateTableMode, CreateTableRequest};
    use crate::table::{Table, TableDefinition};
    use arrow_array::{Int32Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use tempfile::tempdir;

    async fn setup_database() -> (tempfile::TempDir, ListingDatabase) {
        let tempdir = tempdir().unwrap();
        let uri = tempdir.path().to_str().unwrap();

        let request = ConnectRequest {
            uri: uri.to_string(),
            #[cfg(feature = "remote")]
            client_config: Default::default(),
            options: Default::default(),
            read_consistency_interval: None,
            session: None,
        };

        let db = ListingDatabase::connect_with_options(&request)
            .await
            .unwrap();

        (tempdir, db)
    }

    #[tokio::test]
    async fn test_clone_table_basic() {
        let (_tempdir, db) = setup_database().await;

        // Create a source table with schema
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let source_table = db
            .create_table(CreateTableRequest {
                name: "source_table".to_string(),
                namespace: vec![],
                data: CreateTableData::Empty(TableDefinition::new_from_schema(schema.clone())),
                mode: CreateTableMode::Create,
                write_options: Default::default(),
            })
            .await
            .unwrap();

        // Get the source table URI
        let source_uri = db.table_uri("source_table").unwrap();

        // Clone the table
        let cloned_table = db
            .clone_table(CloneTableRequest {
                target_table_name: "cloned_table".to_string(),
                target_namespace: vec![],
                source_uri: source_uri.clone(),
                source_version: None,
                source_tag: None,
                is_shallow: true,
            })
            .await
            .unwrap();

        // Verify both tables exist
        let table_names = db.table_names(TableNamesRequest::default()).await.unwrap();
        assert!(table_names.contains(&"source_table".to_string()));
        assert!(table_names.contains(&"cloned_table".to_string()));

        // Verify schemas match
        assert_eq!(
            source_table.schema().await.unwrap(),
            cloned_table.schema().await.unwrap()
        );
    }

    #[tokio::test]
    async fn test_clone_table_with_data() {
        let (_tempdir, db) = setup_database().await;

        // Create a source table with actual data
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap();

        let reader = Box::new(arrow_array::RecordBatchIterator::new(
            vec![Ok(batch)],
            schema.clone(),
        ));

        let source_table = db
            .create_table(CreateTableRequest {
                name: "source_with_data".to_string(),
                namespace: vec![],
                data: CreateTableData::Data(reader),
                mode: CreateTableMode::Create,
                write_options: Default::default(),
            })
            .await
            .unwrap();

        let source_uri = db.table_uri("source_with_data").unwrap();

        // Clone the table
        let cloned_table = db
            .clone_table(CloneTableRequest {
                target_table_name: "cloned_with_data".to_string(),
                target_namespace: vec![],
                source_uri,
                source_version: None,
                source_tag: None,
                is_shallow: true,
            })
            .await
            .unwrap();

        // Verify data counts match
        let source_count = source_table.count_rows(None).await.unwrap();
        let cloned_count = cloned_table.count_rows(None).await.unwrap();
        assert_eq!(source_count, cloned_count);
        assert_eq!(source_count, 3);
    }

    #[tokio::test]
    async fn test_clone_table_with_storage_options() {
        let tempdir = tempdir().unwrap();
        let uri = tempdir.path().to_str().unwrap();

        // Create database with storage options
        let mut options = HashMap::new();
        options.insert("test_option".to_string(), "test_value".to_string());

        let request = ConnectRequest {
            uri: uri.to_string(),
            #[cfg(feature = "remote")]
            client_config: Default::default(),
            options: options.clone(),
            read_consistency_interval: None,
            session: None,
        };

        let db = ListingDatabase::connect_with_options(&request)
            .await
            .unwrap();

        // Create source table
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

        db.create_table(CreateTableRequest {
            name: "source".to_string(),
            namespace: vec![],
            data: CreateTableData::Empty(TableDefinition::new_from_schema(schema)),
            mode: CreateTableMode::Create,
            write_options: Default::default(),
        })
        .await
        .unwrap();

        let source_uri = db.table_uri("source").unwrap();

        // Clone should work with storage options
        let cloned = db
            .clone_table(CloneTableRequest {
                target_table_name: "cloned".to_string(),
                target_namespace: vec![],
                source_uri,
                source_version: None,
                source_tag: None,
                is_shallow: true,
            })
            .await;

        assert!(cloned.is_ok());
    }

    #[tokio::test]
    async fn test_clone_table_deep_not_supported() {
        let (_tempdir, db) = setup_database().await;

        // Create a source table
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

        db.create_table(CreateTableRequest {
            name: "source".to_string(),
            namespace: vec![],
            data: CreateTableData::Empty(TableDefinition::new_from_schema(schema)),
            mode: CreateTableMode::Create,
            write_options: Default::default(),
        })
        .await
        .unwrap();

        let source_uri = db.table_uri("source").unwrap();

        // Try deep clone (should fail)
        let result = db
            .clone_table(CloneTableRequest {
                target_table_name: "cloned".to_string(),
                target_namespace: vec![],
                source_uri,
                source_version: None,
                source_tag: None,
                is_shallow: false, // Request deep clone
            })
            .await;

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            Error::NotSupported { message } if message.contains("Deep clone")
        ));
    }

    #[tokio::test]
    async fn test_clone_table_with_namespace_not_supported() {
        let (_tempdir, db) = setup_database().await;

        // Create a source table
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

        db.create_table(CreateTableRequest {
            name: "source".to_string(),
            namespace: vec![],
            data: CreateTableData::Empty(TableDefinition::new_from_schema(schema)),
            mode: CreateTableMode::Create,
            write_options: Default::default(),
        })
        .await
        .unwrap();

        let source_uri = db.table_uri("source").unwrap();

        // Try clone with namespace (should fail for listing database)
        let result = db
            .clone_table(CloneTableRequest {
                target_table_name: "cloned".to_string(),
                target_namespace: vec!["namespace".to_string()], // Non-empty namespace
                source_uri,
                source_version: None,
                source_tag: None,
                is_shallow: true,
            })
            .await;

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            Error::NotSupported { message } if message.contains("Namespace parameter is not supported")
        ));
    }

    #[tokio::test]
    async fn test_clone_table_invalid_target_name() {
        let (_tempdir, db) = setup_database().await;

        // Create a source table
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

        db.create_table(CreateTableRequest {
            name: "source".to_string(),
            namespace: vec![],
            data: CreateTableData::Empty(TableDefinition::new_from_schema(schema)),
            mode: CreateTableMode::Create,
            write_options: Default::default(),
        })
        .await
        .unwrap();

        let source_uri = db.table_uri("source").unwrap();

        // Try clone with invalid target name
        let result = db
            .clone_table(CloneTableRequest {
                target_table_name: "invalid/name".to_string(), // Invalid name with slash
                target_namespace: vec![],
                source_uri,
                source_version: None,
                source_tag: None,
                is_shallow: true,
            })
            .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_clone_table_source_not_found() {
        let (_tempdir, db) = setup_database().await;

        // Try to clone from non-existent source
        let result = db
            .clone_table(CloneTableRequest {
                target_table_name: "cloned".to_string(),
                target_namespace: vec![],
                source_uri: "/nonexistent/table.lance".to_string(),
                source_version: None,
                source_tag: None,
                is_shallow: true,
            })
            .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_clone_table_with_version_and_tag_error() {
        let (_tempdir, db) = setup_database().await;

        // Create a source table
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

        db.create_table(CreateTableRequest {
            name: "source".to_string(),
            namespace: vec![],
            data: CreateTableData::Empty(TableDefinition::new_from_schema(schema)),
            mode: CreateTableMode::Create,
            write_options: Default::default(),
        })
        .await
        .unwrap();

        let source_uri = db.table_uri("source").unwrap();

        // Try clone with both version and tag (should fail)
        let result = db
            .clone_table(CloneTableRequest {
                target_table_name: "cloned".to_string(),
                target_namespace: vec![],
                source_uri,
                source_version: Some(1),
                source_tag: Some("v1.0".to_string()),
                is_shallow: true,
            })
            .await;

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            Error::InvalidInput { message } if message.contains("Cannot specify both source_version and source_tag")
        ));
    }

    #[tokio::test]
    async fn test_clone_table_with_specific_version() {
        let (_tempdir, db) = setup_database().await;

        // Create a source table with initial data
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Utf8, false),
        ]));

        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["a", "b"])),
            ],
        )
        .unwrap();

        let reader = Box::new(arrow_array::RecordBatchIterator::new(
            vec![Ok(batch1)],
            schema.clone(),
        ));

        let source_table = db
            .create_table(CreateTableRequest {
                name: "versioned_source".to_string(),
                namespace: vec![],
                data: CreateTableData::Data(reader),
                mode: CreateTableMode::Create,
                write_options: Default::default(),
            })
            .await
            .unwrap();

        // Get the initial version
        let initial_version = source_table.version().await.unwrap();

        // Add more data to create a new version
        let batch2 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![3, 4])),
                Arc::new(StringArray::from(vec!["c", "d"])),
            ],
        )
        .unwrap();

        let source_table_obj = Table::new(source_table.clone());
        source_table_obj
            .add(Box::new(arrow_array::RecordBatchIterator::new(
                vec![Ok(batch2)],
                schema.clone(),
            )))
            .execute()
            .await
            .unwrap();

        // Verify source table now has 4 rows
        assert_eq!(source_table.count_rows(None).await.unwrap(), 4);

        let source_uri = db.table_uri("versioned_source").unwrap();

        // Clone from the initial version (should have only 2 rows)
        let cloned_table = db
            .clone_table(CloneTableRequest {
                target_table_name: "cloned_from_version".to_string(),
                target_namespace: vec![],
                source_uri,
                source_version: Some(initial_version),
                source_tag: None,
                is_shallow: true,
            })
            .await
            .unwrap();

        // Verify cloned table has only the initial 2 rows
        assert_eq!(cloned_table.count_rows(None).await.unwrap(), 2);

        // Source table should still have 4 rows
        assert_eq!(source_table.count_rows(None).await.unwrap(), 4);
    }

    #[tokio::test]
    async fn test_clone_table_with_tag() {
        let (_tempdir, db) = setup_database().await;

        // Create a source table with initial data
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Utf8, false),
        ]));

        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["a", "b"])),
            ],
        )
        .unwrap();

        let reader = Box::new(arrow_array::RecordBatchIterator::new(
            vec![Ok(batch1)],
            schema.clone(),
        ));

        let source_table = db
            .create_table(CreateTableRequest {
                name: "tagged_source".to_string(),
                namespace: vec![],
                data: CreateTableData::Data(reader),
                mode: CreateTableMode::Create,
                write_options: Default::default(),
            })
            .await
            .unwrap();

        // Create a tag for the current version
        let source_table_obj = Table::new(source_table.clone());
        let mut tags = source_table_obj.tags().await.unwrap();
        tags.create("v1.0", source_table.version().await.unwrap())
            .await
            .unwrap();

        // Add more data after the tag
        let batch2 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![3, 4])),
                Arc::new(StringArray::from(vec!["c", "d"])),
            ],
        )
        .unwrap();

        let source_table_obj = Table::new(source_table.clone());
        source_table_obj
            .add(Box::new(arrow_array::RecordBatchIterator::new(
                vec![Ok(batch2)],
                schema.clone(),
            )))
            .execute()
            .await
            .unwrap();

        // Source table should have 4 rows
        assert_eq!(source_table.count_rows(None).await.unwrap(), 4);

        let source_uri = db.table_uri("tagged_source").unwrap();

        // Clone from the tag (should have only 2 rows)
        let cloned_table = db
            .clone_table(CloneTableRequest {
                target_table_name: "cloned_from_tag".to_string(),
                target_namespace: vec![],
                source_uri,
                source_version: None,
                source_tag: Some("v1.0".to_string()),
                is_shallow: true,
            })
            .await
            .unwrap();

        // Verify cloned table has only the tagged version's 2 rows
        assert_eq!(cloned_table.count_rows(None).await.unwrap(), 2);
    }

    #[tokio::test]
    async fn test_cloned_tables_evolve_independently() {
        let (_tempdir, db) = setup_database().await;

        // Create a source table with initial data
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Utf8, false),
        ]));

        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["a", "b"])),
            ],
        )
        .unwrap();

        let reader = Box::new(arrow_array::RecordBatchIterator::new(
            vec![Ok(batch1)],
            schema.clone(),
        ));

        let source_table = db
            .create_table(CreateTableRequest {
                name: "independent_source".to_string(),
                namespace: vec![],
                data: CreateTableData::Data(reader),
                mode: CreateTableMode::Create,
                write_options: Default::default(),
            })
            .await
            .unwrap();

        let source_uri = db.table_uri("independent_source").unwrap();

        // Clone the table
        let cloned_table = db
            .clone_table(CloneTableRequest {
                target_table_name: "independent_clone".to_string(),
                target_namespace: vec![],
                source_uri,
                source_version: None,
                source_tag: None,
                is_shallow: true,
            })
            .await
            .unwrap();

        // Both should start with 2 rows
        assert_eq!(source_table.count_rows(None).await.unwrap(), 2);
        assert_eq!(cloned_table.count_rows(None).await.unwrap(), 2);

        // Add data to the cloned table
        let batch_clone = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![3, 4, 5])),
                Arc::new(StringArray::from(vec!["c", "d", "e"])),
            ],
        )
        .unwrap();

        let cloned_table_obj = Table::new(cloned_table.clone());
        cloned_table_obj
            .add(Box::new(arrow_array::RecordBatchIterator::new(
                vec![Ok(batch_clone)],
                schema.clone(),
            )))
            .execute()
            .await
            .unwrap();

        // Add different data to the source table
        let batch_source = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![10, 11])),
                Arc::new(StringArray::from(vec!["x", "y"])),
            ],
        )
        .unwrap();

        let source_table_obj = Table::new(source_table.clone());
        source_table_obj
            .add(Box::new(arrow_array::RecordBatchIterator::new(
                vec![Ok(batch_source)],
                schema.clone(),
            )))
            .execute()
            .await
            .unwrap();

        // Verify they have evolved independently
        assert_eq!(source_table.count_rows(None).await.unwrap(), 4); // 2 + 2
        assert_eq!(cloned_table.count_rows(None).await.unwrap(), 5); // 2 + 3
    }

    #[tokio::test]
    async fn test_clone_latest_version() {
        let (_tempdir, db) = setup_database().await;

        // Create a source table with initial data
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

        let batch1 =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![1, 2]))])
                .unwrap();

        let reader = Box::new(arrow_array::RecordBatchIterator::new(
            vec![Ok(batch1)],
            schema.clone(),
        ));

        let source_table = db
            .create_table(CreateTableRequest {
                name: "latest_version_source".to_string(),
                namespace: vec![],
                data: CreateTableData::Data(reader),
                mode: CreateTableMode::Create,
                write_options: Default::default(),
            })
            .await
            .unwrap();

        // Add more data to create new versions
        for i in 0..3 {
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int32Array::from(vec![i * 10, i * 10 + 1]))],
            )
            .unwrap();

            let source_table_obj = Table::new(source_table.clone());
            source_table_obj
                .add(Box::new(arrow_array::RecordBatchIterator::new(
                    vec![Ok(batch)],
                    schema.clone(),
                )))
                .execute()
                .await
                .unwrap();
        }

        // Source should have 8 rows total (2 + 2 + 2 + 2)
        let source_count = source_table.count_rows(None).await.unwrap();
        assert_eq!(source_count, 8);

        let source_uri = db.table_uri("latest_version_source").unwrap();

        // Clone without specifying version or tag (should get latest)
        let cloned_table = db
            .clone_table(CloneTableRequest {
                target_table_name: "cloned_latest".to_string(),
                target_namespace: vec![],
                source_uri,
                source_version: None,
                source_tag: None,
                is_shallow: true,
            })
            .await
            .unwrap();

        // Cloned table should have all 8 rows from the latest version
        assert_eq!(cloned_table.count_rows(None).await.unwrap(), 8);
    }
}
