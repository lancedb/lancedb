// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Provides the `ListingDatabase`, a simple database where tables are folders in a directory

mod manifest;

use std::fs::create_dir_all;
use std::path::Path;
use std::{collections::HashMap, sync::Arc};
use std::collections::HashSet;
use lance::dataset::refs::Ref;
use lance::dataset::{builder::DatasetBuilder, ReadParams, WriteMode};
use lance::io::{ObjectStore, ObjectStoreParams, WrappingObjectStore};
use lance::session::Session;
use lance_datafusion::utils::StreamingWriteSource;
use lance_encoding::version::LanceFileVersion;
use lance_table::io::commit::commit_handler_from_url;
use object_store::local::LocalFileSystem;
use snafu::ResultExt;

use crate::connection::ConnectRequest;
use crate::database::ReadConsistency;
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
pub const OPT_MANIFEST_INLINE_OPTIMIZATION_ENABLED: &str = "manifest_inline_optimization_enabled";
pub const OPT_MANIFEST_READ_CONSISTENCY_INTERVAL: &str = "manifest_read_consistency_interval";
pub const OPT_MANIFEST_ENABLED: &str = "manifest_enabled";
pub const OPT_DIR_LISTING_ENABLED: &str = "dir_listing_enabled";

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
#[derive(Debug, Clone)]
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
    /// Manifest table configuration
    ///
    /// Controls how the manifest table (which stores metadata about tables and namespaces)
    /// behaves, including:
    /// - read_consistency_interval: How often to check for updates
    /// - inline_optimization_enabled: Whether to auto-optimize after updates
    pub manifest_config: manifest::ManifestListingDatabaseConfig,
    /// Whether to enable manifest-based listing (default: true)
    ///
    /// When enabled, the database uses the manifest table for listing tables and namespaces,
    /// with fallback to directory listing for unmigrated tables. This allows gradual migration
    /// from directory-based to manifest-based databases.
    ///
    /// When disabled, the database relies solely on directory listing.
    pub manifest_enabled: bool,
    /// Whether to enable directory-based listing fallback (default: true)
    ///
    /// When enabled, the database will fallback to directory listing for operations
    /// like table_names and open_table when manifest_enabled=true but a table is not
    /// found in the manifest. This allows gradual migration to manifest-based listing.
    ///
    /// When disabled, only the manifest table is consulted (no directory fallback).
    /// This should be set to false after running migration to improve performance.
    ///
    /// Note: manifest_enabled and dir_listing_enabled cannot both be false.
    pub dir_listing_enabled: bool,
}

impl Default for ListingDatabaseOptions {
    fn default() -> Self {
        Self {
            new_table_config: Default::default(),
            storage_options: Default::default(),
            manifest_config: Default::default(),
            manifest_enabled: true,
            dir_listing_enabled: true,
        }
    }
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

        let manifest_inline_optimization = map
            .get(OPT_MANIFEST_INLINE_OPTIMIZATION_ENABLED)
            .map(|s| {
                s.parse::<bool>().map_err(|_| Error::InvalidInput {
                    message: format!(
                        "manifest_inline_optimization must be a boolean, received {}",
                        s
                    ),
                })
            })
            .transpose()?
            .unwrap_or(true); // Default to true

        let manifest_read_consistency_interval = map
            .get(OPT_MANIFEST_READ_CONSISTENCY_INTERVAL)
            .map(|s| {
                s.parse::<u64>().map_err(|_| Error::InvalidInput {
                    message: format!(
                        "manifest_read_consistency_interval must be a number (milliseconds), received {}",
                        s
                    ),
                }).map(std::time::Duration::from_millis)
            })
            .transpose()?;

        let manifest_enabled = map
            .get(OPT_MANIFEST_ENABLED)
            .map(|s| {
                s.parse::<bool>().map_err(|_| Error::InvalidInput {
                    message: format!("manifest_enabled must be a boolean, received {}", s),
                })
            })
            .transpose()?
            .unwrap_or(true); // Default to true

        let dir_listing_enabled = map
            .get(OPT_DIR_LISTING_ENABLED)
            .map(|s| {
                s.parse::<bool>().map_err(|_| Error::InvalidInput {
                    message: format!("dir_listing_enabled must be a boolean, received {}", s),
                })
            })
            .transpose()?
            .unwrap_or(true); // Default to true

        let manifest_config = manifest::ManifestListingDatabaseConfig {
            read_consistency_interval: manifest_read_consistency_interval,
            inline_optimization_enabled: manifest_inline_optimization,
            parent_dir_listing_enabled: dir_listing_enabled,
        };

        // Validate that at least one of manifest_enabled or dir_listing_enabled is true
        if !manifest_enabled && !dir_listing_enabled {
            return Err(Error::InvalidInput {
                message: "At least one of manifest_enabled or dir_listing_enabled must be true"
                    .to_string(),
            });
        }

        // We just assume that any options that are not new table config options
        // or manifest config options are storage options
        let storage_options = map
            .iter()
            .filter(|(key, _)| {
                key.as_str() != OPT_NEW_TABLE_STORAGE_VERSION
                    && key.as_str() != OPT_NEW_TABLE_V2_MANIFEST_PATHS
                    && key.as_str() != OPT_MANIFEST_INLINE_OPTIMIZATION_ENABLED
                    && key.as_str() != OPT_MANIFEST_READ_CONSISTENCY_INTERVAL
                    && key.as_str() != OPT_MANIFEST_ENABLED
                    && key.as_str() != OPT_DIR_LISTING_ENABLED
            })
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        Ok(Self {
            new_table_config,
            storage_options,
            manifest_config,
            manifest_enabled,
            dir_listing_enabled,
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
        map.insert(
            OPT_MANIFEST_INLINE_OPTIMIZATION_ENABLED.to_string(),
            self.manifest_config.inline_optimization_enabled.to_string(),
        );
        if let Some(interval) = self.manifest_config.read_consistency_interval {
            map.insert(
                OPT_MANIFEST_READ_CONSISTENCY_INTERVAL.to_string(),
                interval.as_millis().to_string(),
            );
        }
        map.insert(
            OPT_MANIFEST_ENABLED.to_string(),
            self.manifest_enabled.to_string(),
        );
        map.insert(
            OPT_DIR_LISTING_ENABLED.to_string(),
            self.dir_listing_enabled.to_string(),
        );
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

    /// Enable or disable inline optimization of the manifest
    ///
    /// When enabled (default: true), the manifest will be automatically optimized
    /// after each update operation. This includes running compact_files and optimizing indexes.
    ///
    /// # Arguments
    ///
    /// * `enable` - Whether to enable inline optimization
    pub fn manifest_inline_optimization_enabled(mut self, enable: bool) -> Self {
        self.options.manifest_config.inline_optimization_enabled = enable;
        self
    }

    /// Set the read consistency interval for the manifest
    ///
    /// This controls how often the manifest checks for updates.
    ///
    /// # Arguments
    ///
    /// * `interval` - The read consistency interval
    pub fn manifest_read_consistency_interval(mut self, interval: std::time::Duration) -> Self {
        self.options.manifest_config.read_consistency_interval = Some(interval);
        self
    }

    /// Enable or disable manifest-based listing
    ///
    /// When enabled (default: true), the database uses the manifest table for listing
    /// tables/namespaces, with optional fallback to directory listing during migration.
    /// When disabled, the database relies solely on directory listing.
    ///
    /// # Arguments
    ///
    /// * `enable` - Whether to enable manifest migration mode
    pub fn manifest_enabled(mut self, enable: bool) -> Self {
        self.options.manifest_enabled = enable;
        self
    }

    /// Enable or disable directory listing fallback
    ///
    /// When enabled (default: true), the database will fallback to directory listing
    /// for operations like table_names and open_table when manifest_enabled=true but
    /// a table is not found in the manifest.
    ///
    /// When disabled, only the manifest table is consulted (no directory fallback).
    /// This should be set to false after running migration to improve performance.
    ///
    /// Note: manifest_enabled and dir_listing_enabled cannot both be false.
    ///
    /// # Arguments
    ///
    /// * `enable` - Whether to enable directory listing fallback
    pub fn dir_listing_enabled(mut self, enable: bool) -> Self {
        self.options.dir_listing_enabled = enable;
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
///
/// # Manifest-based Listing
///
/// When `manifest_enabled=true`, the database uses a Lance table named `__manifest`
/// (without .lance extension) to store metadata about tables and namespaces,
/// enabling namespace support and advanced table management.
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

    // Manifest-based database
    manifest_db: manifest::ManifestListingDatabase,

    // Whether manifest-based listing is enabled
    manifest_enabled: bool,

    // Whether directory-based listing fallback is enabled
    dir_listing_enabled: bool,
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
                    options.manifest_config,
                    options.manifest_enabled,
                    options.dir_listing_enabled,
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

                let manifest_db = manifest::ManifestListingDatabase::new(
                    table_base_uri.clone(),
                    query_string.clone(),
                    session.clone(),
                    options.manifest_config.clone(),
                    object_store.clone(),
                    options.storage_options.clone(),
                    options.new_table_config.clone(),
                    write_store_wrapper.clone(),
                    request.read_consistency_interval,
                )
                .await;

                Ok(Self {
                    uri: table_base_uri,
                    query_string,
                    base_path,
                    object_store,
                    store_wrapper: write_store_wrapper,
                    read_consistency_interval: request.read_consistency_interval,
                    storage_options: options.storage_options.clone(),
                    new_table_config: options.new_table_config.clone(),
                    session,
                    manifest_db,
                    manifest_enabled: options.manifest_enabled,
                    dir_listing_enabled: options.dir_listing_enabled,
                })
            }
            Err(_) => {
                Self::open_path(
                    uri,
                    request.read_consistency_interval,
                    options.new_table_config,
                    request.session.clone(),
                    options.manifest_config,
                    options.manifest_enabled,
                    options.dir_listing_enabled,
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
        manifest_config: manifest::ManifestListingDatabaseConfig,
        manifest_enabled: bool,
        dir_listing_enabled: bool,
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

        let manifest_db = manifest::ManifestListingDatabase::new(
            path.to_string(),
            None, // No query string for local path
            session.clone(),
            manifest_config,
            object_store.clone(),
            HashMap::new(), // No storage options for local path
            new_table_config.clone(),
            None, // No store wrapper for local path
            read_consistency_interval,
        )
        .await;

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
            manifest_db,
            manifest_enabled,
            dir_listing_enabled,
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

    /// Build a table URI from base URI, table name, and optional query string
    ///
    /// This is a static helper that can be used by both ListingDatabase and ManifestListingDatabase.
    ///
    /// # Arguments
    ///
    /// * `base_uri` - The base URI of the database
    /// * `name` - The table name or location path
    /// * `query_string` - Optional query string to append
    /// * `include_lance_suffix` - Whether to append the .lance extension.
    ///   Set to false when the name is already a full path (e.g., UUID or __manifest).
    pub(super) fn build_table_uri(
        base_uri: &str,
        name: &str,
        query_string: Option<&str>,
        include_lance_suffix: bool,
    ) -> Result<String> {
        // Always validate table name
        validate_table_name(name)?;

        let mut uri = base_uri.to_string();
        // If the URI does not end with a slash, add one
        if !uri.ends_with('/') {
            uri.push('/');
        }

        // Append the name/path with or without the lance file extension
        if include_lance_suffix {
            uri.push_str(&format!("{}.{}", name, LANCE_FILE_EXTENSION));
        } else {
            uri.push_str(name);
        }

        // If there are query string set on the connection, propagate to lance
        if let Some(query) = query_string {
            uri.push('?');
            uri.push_str(query);
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
    pub(super) fn extract_storage_overrides(
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
    pub(super) fn prepare_write_params(
        request: &CreateTableRequest,
        storage_version_override: Option<LanceFileVersion>,
        v2_manifest_override: Option<bool>,
        storage_options_to_inherit: &HashMap<String, String>,
        new_table_config: &NewTableConfig,
        session: Arc<Session>,
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
        if !storage_options_to_inherit.is_empty() {
            let storage_options = write_params
                .store_params
                .get_or_insert_with(Default::default)
                .storage_options
                .get_or_insert_with(Default::default);
            for (key, value) in storage_options_to_inherit {
                storage_options.insert(key.clone(), value.clone());
            }
        }

        write_params.data_storage_version = new_table_config
            .data_storage_version
            .or(storage_version_override);

        if let Some(enable_v2_manifest_paths) = new_table_config
            .enable_v2_manifest_paths
            .or(v2_manifest_override)
        {
            write_params.enable_v2_manifest_paths = enable_v2_manifest_paths;
        }

        if matches!(&request.mode, CreateTableMode::Overwrite) {
            write_params.mode = WriteMode::Overwrite;
        }

        write_params.session = Some(session);

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

    /// Migrate all directory-based tables to the manifest
    ///
    /// This performs a one-time migration of all tables found in the directory
    /// to the manifest table. After migration, you should disable migration_mode
    /// to stop checking the directory on each operation.
    ///
    /// # Returns
    ///
    /// The number of tables that were migrated.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use lancedb::database::Database;
    /// # use lancedb::connection::ConnectRequest;
    /// # use lancedb::database::listing::ListingDatabase;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let request = ConnectRequest {
    /// #     uri: "/tmp/db".to_string(),
    /// #     #[cfg(feature = "remote")]
    /// #     client_config: Default::default(),
    /// #     options: Default::default(),
    /// #     read_consistency_interval: None,
    /// #     session: None,
    /// # };
    /// let db = ListingDatabase::connect_with_options(&request).await?;
    /// let migrated_count = db.migrate().await?;
    /// println!("Migrated {} tables", migrated_count);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn migrate(&self) -> Result<usize> {
        // Get all tables with locations from manifest
        let manifest_tables = self.manifest_db.list_tables_with_location().await?;

        // Build set of locations already in manifest
        let manifest_locations: HashSet<String> = manifest_tables
            .iter()
            .map(|(_, _, location)| location.clone())
            .collect();

        // Get all tables from directory listing using the same logic as table_names
        let entries = self.object_store.read_dir(self.base_path.clone()).await?;
        let dir_tables: Vec<String> = entries
            .iter()
            .map(Path::new)
            .filter(|path| {
                path.extension()
                    .and_then(|e| e.to_str())
                    .map(|e| e == LANCE_EXTENSION)
                    .unwrap_or(false)
            })
            .filter_map(|p| p.file_stem().and_then(|s| s.to_str().map(String::from)))
            .collect();

        // Register each directory table that doesn't have overlapping location
        let mut migrated_count = 0;
        for table_name in dir_tables {
            let location = format!("{}.{}", table_name, LANCE_FILE_EXTENSION);

            // Skip if this location is already registered in manifest
            if !manifest_locations.contains(&location) {
                self.manifest_db
                    .register_table(&table_name, &[], location)
                    .await?;
                migrated_count += 1;
            }
        }

        Ok(migrated_count)
    }
}

#[async_trait::async_trait]
impl Database for ListingDatabase {
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

    async fn list_namespaces(&self, request: ListNamespacesRequest) -> Result<Vec<String>> {
        // If manifest is enabled, delegate to manifest_db
        if self.manifest_enabled {
            return self.manifest_db.list_namespaces(request).await;
        }

        // Legacy mode (no manifest): no namespace support
        if !request.namespace.is_empty() {
            return Err(Error::NotSupported {
                message: "Namespace operations require manifest table".into(),
            });
        }

        Ok(Vec::new())
    }

    async fn create_namespace(&self, request: CreateNamespaceRequest) -> Result<()> {
        // If manifest is enabled, delegate to manifest_db
        if self.manifest_enabled {
            return self.manifest_db.create_namespace(request).await;
        }

        // Directory-based listing: no namespace support
        Err(Error::NotSupported {
            message: "Namespace operations are not supported for listing database".into(),
        })
    }

    async fn drop_namespace(&self, request: DropNamespaceRequest) -> Result<()> {
        // If manifest is enabled, delegate to manifest_db
        if self.manifest_enabled {
            return self.manifest_db.drop_namespace(request).await;
        }

        // Directory-based listing: no namespace support
        Err(Error::NotSupported {
            message: "Namespace operations are not supported for listing database".into(),
        })
    }

    async fn table_names(&self, request: TableNamesRequest) -> Result<Vec<String>> {
        // If manifest is enabled, try manifest_db with optional directory merge during migration
        if self.manifest_enabled {
            match self.manifest_db.table_names(request.clone()).await {
                Ok(mut table_names) => {
                    // In migration mode, also check directory for unmigrated tables (root namespace only)
                    // Only fallback to directory listing if dir_listing_enabled is true
                    if self.dir_listing_enabled && request.namespace.is_empty() {
                        // Get all tables from directory listing using the same logic as below
                        let entries = self.object_store.read_dir(self.base_path.clone()).await?;
                        let dir_tables: Vec<String> = entries
                            .iter()
                            .map(Path::new)
                            .filter(|path| {
                                path.extension()
                                    .and_then(|e| e.to_str())
                                    .map(|e| e == LANCE_EXTENSION)
                                    .unwrap_or(false)
                            })
                            .filter_map(|p| {
                                p.file_stem().and_then(|s| s.to_str().map(String::from))
                            })
                            .collect();

                        for table in dir_tables {
                            // Add if not already in manifest
                            if !table_names.contains(&table) {
                                table_names.push(table);
                            }
                        }

                        // Re-sort and re-paginate after merging directory tables
                        table_names.sort();
                        if let Some(start_after) = &request.start_after {
                            let index = table_names
                                .iter()
                                .position(|name| name.as_str() > start_after.as_str())
                                .unwrap_or(table_names.len());
                            table_names.drain(0..index);
                        }
                        if let Some(limit) = request.limit {
                            table_names.truncate(limit as usize);
                        }
                    }

                    return Ok(table_names);
                }
                Err(Error::NotSupported { .. }) => {
                    // Continue to directory-based logic below
                }
                Err(e) => return Err(e),
            }
        }

        // Directory-based listing: no namespace support
        if !request.namespace.is_empty() {
            return Err(Error::NotSupported {
                message: "Namespace parameter is not supported unless manifest_enabled=true. Only root namespace is supported.".into(),
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
        if self.manifest_enabled {
            return self.manifest_db.create_table(request).await;
        }

        if !request.namespace.is_empty() {
            return Err(Error::NotSupported {
                message: "Namespace parameter is not supported unless manifest_enabled=true. Only root namespace is supported.".into(),
            });
        }
        let table_uri =
            Self::build_table_uri(&self.uri, &request.name, self.query_string.as_deref(), true)?;

        let (storage_version_override, v2_manifest_override) =
            Self::extract_storage_overrides(&request)?;

        let write_params = Self::prepare_write_params(
            &request,
            storage_version_override,
            v2_manifest_override,
            &self.storage_options,
            &self.new_table_config,
            self.session.clone(),
        );

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
        // If manifest is enabled, delegate to manifest_db
        if self.manifest_enabled {
            return self.manifest_db.clone_table(request).await;
        }

        // Directory-based listing: no namespace support
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
            (Some(v), None) => Ok(Ref::Version(None, Some(v))),
            (None, Some(tag)) => Ok(Ref::Tag(tag)),
            (None, None) => Ok(Ref::Version(None, Some(source_dataset.version().version))),
            _ => Err(Error::InvalidInput {
                message: "Cannot specify both source_version and source_tag".to_string(),
            }),
        }?;

        let target_uri = Self::build_table_uri(
            &self.uri,
            &request.target_table_name,
            self.query_string.as_deref(),
            true,
        )?;
        source_dataset
            .shallow_clone(&target_uri, version_ref, Some(storage_params))
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
        // If manifest is enabled, try manifest_db first with fallback to directory for unmigrated tables
        if self.manifest_enabled {
            match self.manifest_db.open_table(request.clone()).await {
                Ok(table) => return Ok(table),
                Err(Error::TableNotFound { .. }) | Err(Error::NotSupported { .. }) => {
                    // Only fallback to directory listing if dir_listing_enabled is true
                    if !self.dir_listing_enabled {
                        return Err(Error::TableNotFound {
                            name: request.name.clone(),
                        });
                    }
                    // Continue to directory-based logic below
                }
                Err(e) => return Err(e),
            }
        }

        // Directory-based listing: no namespace support
        if !request.namespace.is_empty() {
            return Err(Error::NotSupported {
                message: "Namespace parameter is not supported unless manifest_enabled=true. Only root namespace is supported.".into(),
            });
        }
        let table_uri =
            Self::build_table_uri(&self.uri, &request.name, self.query_string.as_deref(), true)?;

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
        cur_name: &str,
        new_name: &str,
        cur_namespace: &[String],
        new_namespace: &[String],
    ) -> Result<()> {
        // If manifest is enabled, delegate to manifest_db
        if self.manifest_enabled {
            return self
                .manifest_db
                .rename_table(cur_name, new_name, cur_namespace, new_namespace)
                .await;
        }

        // Directory-based listing: no rename support
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
            message: "rename_table is not supported for directory-based listing database".into(),
        })
    }

    async fn drop_table(&self, name: &str, namespace: &[String]) -> Result<()> {
        // If manifest is enabled, try manifest_db first with migration fallback
        if self.manifest_enabled {
            match self.manifest_db.drop_table(name, namespace).await {
                Ok(()) => return Ok(()),
                Err(Error::TableNotFound { .. }) => {
                    // Only fallback to directory listing if dir_listing_enabled is true
                    if !self.dir_listing_enabled {
                        return Err(Error::TableNotFound {
                            name: name.to_string(),
                        });
                    }
                    // Continue to directory-based logic below
                }
                Err(e) => return Err(e),
            }
        }

        // Directory-based listing: no namespace support
        if !namespace.is_empty() {
            return Err(Error::NotSupported {
                message: "Namespace parameter is not supported unless manifest_enabled=true."
                    .into(),
            });
        }
        self.drop_tables(vec![name.to_string()]).await
    }

    async fn drop_all_tables(&self, namespace: &[String]) -> Result<()> {
        // If manifest is enabled, delegate to manifest_db
        if self.manifest_enabled {
            return self.manifest_db.drop_all_tables(namespace).await;
        }

        // Directory-based listing: no namespace support
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

        // Configure database with manifest disabled to test directory-based behavior
        let mut options = HashMap::new();
        options.insert(OPT_MANIFEST_ENABLED.to_string(), "false".to_string());

        let request = ConnectRequest {
            uri: uri.to_string(),
            #[cfg(feature = "remote")]
            client_config: Default::default(),
            options,
            read_consistency_interval: None,
            session: None,
        };

        let db = ListingDatabase::connect_with_options(&request)
            .await
            .unwrap();

        (tempdir, db)
    }

    #[tokio::test]
    async fn test_clone_table_basic_manifest_disabled() {
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
        let source_uri =
            ListingDatabase::build_table_uri(&db.uri, "source_table", db.query_string.as_deref(), true)
                .unwrap();

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
    async fn test_clone_table_with_data_manifest_disabled() {
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

        let source_uri = ListingDatabase::build_table_uri(
            &db.uri,
            "source_with_data",
            db.query_string.as_deref(),
            true,
        )
        .unwrap();

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
    async fn test_clone_table_with_storage_options_manifest_disabled() {
        let tempdir = tempdir().unwrap();
        let uri = tempdir.path().to_str().unwrap();

        // Create database with storage options and manifest disabled
        let mut options = HashMap::new();
        options.insert("test_option".to_string(), "test_value".to_string());
        options.insert(OPT_MANIFEST_ENABLED.to_string(), "false".to_string());

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

        let source_uri =
            ListingDatabase::build_table_uri(&db.uri, "source", db.query_string.as_deref(), true)
                .unwrap();

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
    async fn test_clone_table_deep_not_supported_manifest_disabled() {
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

        let source_uri =
            ListingDatabase::build_table_uri(&db.uri, "source", db.query_string.as_deref(), true)
                .unwrap();

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
    async fn test_clone_table_namespace_not_found_manifest_disabled() {
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

        let source_uri =
            ListingDatabase::build_table_uri(&db.uri, "source", db.query_string.as_deref(), true)
                .unwrap();

        // Try clone to non-existent namespace (should fail with NotSupported in directory-based mode)
        let result = db
            .clone_table(CloneTableRequest {
                target_table_name: "cloned".to_string(),
                target_namespace: vec!["nonexistent".to_string()],
                source_uri,
                source_version: None,
                source_tag: None,
                is_shallow: true,
            })
            .await;

        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::NotSupported { .. }));
    }

    #[tokio::test]
    async fn test_clone_table_invalid_target_name_manifest_disabled() {
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

        let source_uri =
            ListingDatabase::build_table_uri(&db.uri, "source", db.query_string.as_deref(), true)
                .unwrap();

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
    async fn test_clone_table_source_not_found_manifest_disabled() {
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
    async fn test_clone_table_with_version_and_tag_error_manifest_disabled() {
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

        let source_uri =
            ListingDatabase::build_table_uri(&db.uri, "source", db.query_string.as_deref(), true)
                .unwrap();

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
    async fn test_clone_table_with_specific_version_manifest_disabled() {
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

        let db = Arc::new(db);
        let source_table_obj = Table::new(source_table.clone(), db.clone());
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

        let source_uri = ListingDatabase::build_table_uri(
            &db.uri,
            "versioned_source",
            db.query_string.as_deref(),
            true,
        )
        .unwrap();

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
    async fn test_clone_table_with_tag_manifest_disabled() {
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
        let db = Arc::new(db);
        let source_table_obj = Table::new(source_table.clone(), db.clone());
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

        let source_table_obj = Table::new(source_table.clone(), db.clone());
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

        let source_uri =
            ListingDatabase::build_table_uri(&db.uri, "tagged_source", db.query_string.as_deref(), true)
                .unwrap();

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
    async fn test_cloned_tables_evolve_independently_manifest_disabled() {
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

        let source_uri = ListingDatabase::build_table_uri(
            &db.uri,
            "independent_source",
            db.query_string.as_deref(),
            true,
        )
        .unwrap();

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

        let db = Arc::new(db);
        let cloned_table_obj = Table::new(cloned_table.clone(), db.clone());
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

        let source_table_obj = Table::new(source_table.clone(), db);
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
    async fn test_clone_latest_version_manifest_disabled() {
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
        let db = Arc::new(db);
        for i in 0..3 {
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int32Array::from(vec![i * 10, i * 10 + 1]))],
            )
            .unwrap();

            let source_table_obj = Table::new(source_table.clone(), db.clone());
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

        let source_uri = ListingDatabase::build_table_uri(
            &db.uri,
            "latest_version_source",
            db.query_string.as_deref(),
            true,
        )
        .unwrap();

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

    #[tokio::test]
    async fn test_backward_compatibility_create_with_manifest_read_without() {
        let tempdir = tempdir().unwrap();
        let uri = tempdir.path().to_str().unwrap();

        // Create a database with manifest enabled (v2)
        let mut options_v2 = HashMap::new();
        options_v2.insert(OPT_MANIFEST_ENABLED.to_string(), "true".to_string());

        let request_v2 = ConnectRequest {
            uri: uri.to_string(),
            #[cfg(feature = "remote")]
            client_config: Default::default(),
            options: options_v2,
            read_consistency_interval: None,
            session: None,
        };

        let db_v2 = ListingDatabase::connect_with_options(&request_v2)
            .await
            .unwrap();

        // Create a table with manifest enabled in root namespace
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

        db_v2
            .create_table(CreateTableRequest {
                name: "test_table".to_string(),
                namespace: vec![],
                data: CreateTableData::Data(reader),
                mode: CreateTableMode::Create,
                write_options: Default::default(),
            })
            .await
            .unwrap();

        // Create a database with manifest disabled (v1)
        let mut options_v1 = HashMap::new();
        options_v1.insert(OPT_MANIFEST_ENABLED.to_string(), "false".to_string());

        let request_v1 = ConnectRequest {
            uri: uri.to_string(),
            #[cfg(feature = "remote")]
            client_config: Default::default(),
            options: options_v1,
            read_consistency_interval: None,
            session: None,
        };

        let db_v1 = ListingDatabase::connect_with_options(&request_v1)
            .await
            .unwrap();

        // Old client should be able to list the table
        let table_names = db_v1
            .table_names(TableNamesRequest::default())
            .await
            .unwrap();
        assert!(table_names.contains(&"test_table".to_string()));

        // Old client should be able to open the table
        let table = db_v1
            .open_table(OpenTableRequest {
                name: "test_table".to_string(),
                namespace: vec![],
                index_cache_size: None,
                lance_read_params: None,
            })
            .await
            .unwrap();

        // Verify the data is accessible
        assert_eq!(table.count_rows(None).await.unwrap(), 3);
    }

    #[tokio::test]
    async fn test_forward_compatibility_create_without_manifest_read_with() {
        let tempdir = tempdir().unwrap();
        let uri = tempdir.path().to_str().unwrap();

        // Create a database with manifest disabled (v1)
        let mut options_v1 = HashMap::new();
        options_v1.insert(OPT_MANIFEST_ENABLED.to_string(), "false".to_string());

        let request_v1 = ConnectRequest {
            uri: uri.to_string(),
            #[cfg(feature = "remote")]
            client_config: Default::default(),
            options: options_v1,
            read_consistency_interval: None,
            session: None,
        };

        let db_v1 = ListingDatabase::connect_with_options(&request_v1)
            .await
            .unwrap();

        // Create a table with manifest disabled
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Utf8, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![10, 20, 30])),
                Arc::new(StringArray::from(vec!["x", "y", "z"])),
            ],
        )
        .unwrap();

        let reader = Box::new(arrow_array::RecordBatchIterator::new(
            vec![Ok(batch)],
            schema.clone(),
        ));

        db_v1
            .create_table(CreateTableRequest {
                name: "old_table".to_string(),
                namespace: vec![],
                data: CreateTableData::Data(reader),
                mode: CreateTableMode::Create,
                write_options: Default::default(),
            })
            .await
            .unwrap();

        // Create a database with manifest enabled (v2)
        let mut options_v2 = HashMap::new();
        options_v2.insert(OPT_MANIFEST_ENABLED.to_string(), "true".to_string());

        let request_v2 = ConnectRequest {
            uri: uri.to_string(),
            #[cfg(feature = "remote")]
            client_config: Default::default(),
            options: options_v2,
            read_consistency_interval: None,
            session: None,
        };

        let db_v2 = ListingDatabase::connect_with_options(&request_v2)
            .await
            .unwrap();

        // New client should be able to list the table (migration mode)
        let table_names = db_v2
            .table_names(TableNamesRequest::default())
            .await
            .unwrap();
        assert!(table_names.contains(&"old_table".to_string()));

        // New client should be able to open the table
        let table = db_v2
            .open_table(OpenTableRequest {
                name: "old_table".to_string(),
                namespace: vec![],
                index_cache_size: None,
                lance_read_params: None,
            })
            .await
            .unwrap();

        // Verify the data is accessible
        assert_eq!(table.count_rows(None).await.unwrap(), 3);
    }

    #[tokio::test]
    async fn test_old_clients_cannot_see_namespaced_tables() {
        let tempdir = tempdir().unwrap();
        let uri = tempdir.path().to_str().unwrap();

        // Create a database with manifest enabled (v2)
        let mut options_v2 = HashMap::new();
        options_v2.insert(OPT_MANIFEST_ENABLED.to_string(), "true".to_string());

        let request_v2 = ConnectRequest {
            uri: uri.to_string(),
            #[cfg(feature = "remote")]
            client_config: Default::default(),
            options: options_v2,
            read_consistency_interval: None,
            session: None,
        };

        let db_v2 = ListingDatabase::connect_with_options(&request_v2)
            .await
            .unwrap();

        // Create a namespace
        db_v2
            .create_namespace(CreateNamespaceRequest {
                namespace: vec!["myspace".to_string()],
            })
            .await
            .unwrap();

        // Create a table in the namespace
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![1, 2]))])
                .unwrap();

        let reader = Box::new(arrow_array::RecordBatchIterator::new(
            vec![Ok(batch)],
            schema.clone(),
        ));

        db_v2
            .create_table(CreateTableRequest {
                name: "namespaced_table".to_string(),
                namespace: vec!["myspace".to_string()],
                data: CreateTableData::Data(reader),
                mode: CreateTableMode::Create,
                write_options: Default::default(),
            })
            .await
            .unwrap();

        // Create a database with manifest disabled (v1)
        let mut options_v1 = HashMap::new();
        options_v1.insert(OPT_MANIFEST_ENABLED.to_string(), "false".to_string());

        let request_v1 = ConnectRequest {
            uri: uri.to_string(),
            #[cfg(feature = "remote")]
            client_config: Default::default(),
            options: options_v1,
            read_consistency_interval: None,
            session: None,
        };

        let db_v1 = ListingDatabase::connect_with_options(&request_v1)
            .await
            .unwrap();

        // Old client should NOT be able to list the namespaced table in root
        let table_names = db_v1
            .table_names(TableNamesRequest::default())
            .await
            .unwrap();
        assert!(!table_names.contains(&"namespaced_table".to_string()));

        // Old client cannot query namespaces
        let result = db_v1
            .table_names(TableNamesRequest {
                namespace: vec!["myspace".to_string()],
                start_after: None,
                limit: None,
            })
            .await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::NotSupported { .. }));
    }

    #[tokio::test]
    async fn test_old_clients_cannot_see_renamed_tables() {
        let tempdir = tempdir().unwrap();
        let uri = tempdir.path().to_str().unwrap();

        // Create a database with manifest enabled (v2)
        let mut options_v2 = HashMap::new();
        options_v2.insert(OPT_MANIFEST_ENABLED.to_string(), "true".to_string());

        let request_v2 = ConnectRequest {
            uri: uri.to_string(),
            #[cfg(feature = "remote")]
            client_config: Default::default(),
            options: options_v2,
            read_consistency_interval: None,
            session: None,
        };

        let db_v2 = ListingDatabase::connect_with_options(&request_v2)
            .await
            .unwrap();

        // Create a table with original name
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![5, 6, 7]))],
        )
        .unwrap();

        let reader = Box::new(arrow_array::RecordBatchIterator::new(
            vec![Ok(batch)],
            schema.clone(),
        ));

        db_v2
            .create_table(CreateTableRequest {
                name: "original_name".to_string(),
                namespace: vec![],
                data: CreateTableData::Data(reader),
                mode: CreateTableMode::Create,
                write_options: Default::default(),
            })
            .await
            .unwrap();

        // Rename the table using v2 database
        db_v2
            .rename_table("original_name", "new_name", &[], &[])
            .await
            .unwrap();

        // Create a database with manifest disabled (v1)
        let mut options_v1 = HashMap::new();
        options_v1.insert(OPT_MANIFEST_ENABLED.to_string(), "false".to_string());

        let request_v1 = ConnectRequest {
            uri: uri.to_string(),
            #[cfg(feature = "remote")]
            client_config: Default::default(),
            options: options_v1,
            read_consistency_interval: None,
            session: None,
        };

        let db_v1 = ListingDatabase::connect_with_options(&request_v1)
            .await
            .unwrap();

        // Old client should NOT see the renamed table because it's now stored without .lance extension
        // (hidden from directory-based listing)
        let table_names = db_v1
            .table_names(TableNamesRequest::default())
            .await
            .unwrap();
        assert!(!table_names.contains(&"original_name".to_string()));
        assert!(!table_names.contains(&"new_name".to_string()));

        // Old client cannot open table with the original name
        let result = db_v1
            .open_table(OpenTableRequest {
                name: "original_name".to_string(),
                namespace: vec![],
                index_cache_size: None,
                lance_read_params: None,
            })
            .await;
        assert!(result.is_err());

        // Old client cannot open with the new name
        let result = db_v1
            .open_table(OpenTableRequest {
                name: "new_name".to_string(),
                namespace: vec![],
                index_cache_size: None,
                lance_read_params: None,
            })
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_validation_both_flags_false() {
        let tempdir = tempdir().unwrap();
        let uri = tempdir.path().to_str().unwrap();

        // Try to create database with both flags false
        let mut options = HashMap::new();
        options.insert(OPT_MANIFEST_ENABLED.to_string(), "false".to_string());
        options.insert(OPT_DIR_LISTING_ENABLED.to_string(), "false".to_string());

        let request = ConnectRequest {
            uri: uri.to_string(),
            #[cfg(feature = "remote")]
            client_config: Default::default(),
            options,
            read_consistency_interval: None,
            session: None,
        };

        let result = ListingDatabase::connect_with_options(&request).await;
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            Error::InvalidInput { message } if message.contains("At least one of manifest_enabled or dir_listing_enabled must be true")
        ));
    }

    #[tokio::test]
    async fn test_dir_listing_disabled_prevents_fallback() {
        let tempdir = tempdir().unwrap();
        let uri = tempdir.path().to_str().unwrap();

        // Create a table with manifest disabled (directory-based mode)
        let mut options_v1 = HashMap::new();
        options_v1.insert(OPT_MANIFEST_ENABLED.to_string(), "false".to_string());

        let request_v1 = ConnectRequest {
            uri: uri.to_string(),
            #[cfg(feature = "remote")]
            client_config: Default::default(),
            options: options_v1,
            read_consistency_interval: None,
            session: None,
        };

        let db_v1 = ListingDatabase::connect_with_options(&request_v1)
            .await
            .unwrap();

        // Create a table
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();

        let reader = Box::new(arrow_array::RecordBatchIterator::new(
            vec![Ok(batch)],
            schema.clone(),
        ));

        db_v1
            .create_table(CreateTableRequest {
                name: "unmigrated_table".to_string(),
                namespace: vec![],
                data: CreateTableData::Data(reader),
                mode: CreateTableMode::Create,
                write_options: Default::default(),
            })
            .await
            .unwrap();

        // Create a new database connection with manifest enabled but dir_listing disabled
        let mut options_v2 = HashMap::new();
        options_v2.insert(OPT_MANIFEST_ENABLED.to_string(), "true".to_string());
        options_v2.insert(OPT_DIR_LISTING_ENABLED.to_string(), "false".to_string());

        let request_v2 = ConnectRequest {
            uri: uri.to_string(),
            #[cfg(feature = "remote")]
            client_config: Default::default(),
            options: options_v2,
            read_consistency_interval: None,
            session: None,
        };

        let db_v2 = ListingDatabase::connect_with_options(&request_v2)
            .await
            .unwrap();

        // Trigger manifest table creation by calling a read operation
        // (Don't actually migrate the table - we want to test the no-fallback behavior)
        // Should NOT be able to list the unmigrated table (no fallback to directory)
        let table_names = db_v2
            .table_names(TableNamesRequest::default())
            .await
            .unwrap();
        assert!(!table_names.contains(&"unmigrated_table".to_string()),
            "Table list should not contain unmigrated table when dir_listing is disabled. Found: {:?}", table_names);

        // Should NOT be able to open the unmigrated table (no fallback to directory)
        let result = db_v2
            .open_table(OpenTableRequest {
                name: "unmigrated_table".to_string(),
                namespace: vec![],
                index_cache_size: None,
                lance_read_params: None,
            })
            .await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::TableNotFound { .. }));

        // Should NOT be able to drop the unmigrated table (no fallback to directory)
        let result = db_v2.drop_table("unmigrated_table", &[]).await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::TableNotFound { .. }));

        // But with dir_listing enabled (default), it should work
        let mut options_v2_with_dir = HashMap::new();
        options_v2_with_dir.insert(OPT_MANIFEST_ENABLED.to_string(), "true".to_string());
        options_v2_with_dir.insert(OPT_DIR_LISTING_ENABLED.to_string(), "true".to_string());

        let request_v2_with_dir = ConnectRequest {
            uri: uri.to_string(),
            #[cfg(feature = "remote")]
            client_config: Default::default(),
            options: options_v2_with_dir,
            read_consistency_interval: None,
            session: None,
        };

        let db_v2_with_dir = ListingDatabase::connect_with_options(&request_v2_with_dir)
            .await
            .unwrap();

        // Should be able to list the unmigrated table (with fallback)
        let table_names = db_v2_with_dir
            .table_names(TableNamesRequest::default())
            .await
            .unwrap();
        assert!(table_names.contains(&"unmigrated_table".to_string()));

        // Should be able to open the unmigrated table (with fallback)
        let table = db_v2_with_dir
            .open_table(OpenTableRequest {
                name: "unmigrated_table".to_string(),
                namespace: vec![],
                index_cache_size: None,
                lance_read_params: None,
            })
            .await
            .unwrap();
        assert_eq!(table.count_rows(None).await.unwrap(), 3);
    }
}
