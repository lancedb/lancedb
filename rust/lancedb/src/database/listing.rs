// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Provides the `ListingDatabase`, a simple storage-only database
pub mod manifest;

use lance::dataset::refs::Ref;
use lance::dataset::{builder::DatasetBuilder, ReadParams, WriteMode};
use lance::io::{ObjectStore, ObjectStoreParams, WrappingObjectStore};
use lance::session::Session;
use lance_datafusion::utils::StreamingWriteSource;
use lance_encoding::version::LanceFileVersion;
use lance_table::io::commit::commit_handler_from_url;
use object_store::local::LocalFileSystem;
use snafu::ResultExt;
use std::collections::HashSet;
use std::fs::create_dir_all;
use std::path::Path;
use std::{collections::HashMap, sync::Arc};

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
    pub manifest_config: manifest::ManifestDatabaseConfig,
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

        let manifest_config = manifest::ManifestDatabaseConfig {
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

/// A database that stores tables in a directory.
///
/// # Manifest-based Listing and Compatibility
///
/// The `ListingDatabase` supports two modes of operation:
///
/// ## 1. Directory-based Listing (Legacy)
///
/// Tables are discovered by listing directories with `.lance` extension in the object store.
/// This is the original behavior and provides basic table management without namespace support.
///
/// **Limitations:**
/// - No namespace support (only a flat list of tables)
/// - No table renaming
/// - Requires scanning directories for each list operation
/// - Limited ability to offer more advanced database level features
/// - Layout not optimized for object store
///
/// ## 2. Manifest-based Listing (Recommended)
///
/// Tables and namespaces are tracked in a special Lance table called `__manifest` (without .lance extension).
/// This enables advanced features and better performance.
///
/// **Features:**
/// - Full namespace support with hierarchical organization
/// - Table renaming capability
/// - Efficient indexed queries for listing operations
/// - Stores additional metadata (location, base objects, etc.)
/// - Layout optimized for object store
///
/// ## Backwards Compatibility
///
/// ### Reading Old Databases
///
/// When `manifest_enabled=true` and `dir_listing_enabled=true` (migration mode), the database
/// provides backwards compatibility with existing directory-based databases:
///
/// - **List operations** merge results from both manifest and directory listing
/// - **Open table** falls back to directory listing if table not found in manifest
/// - **Drop table** falls back to directory listing if table not found in manifest
/// - New tables are created in the manifest automatically
///
/// This allows gradual migration: old tables remain accessible while new tables use the manifest.
///
/// ### Writing with Old Clients
///
/// **WARNING:** Old clients (without manifest support) can still write to the database, but:
///
/// - Tables created by old clients will NOT be registered in the manifest
/// - Migration mode (`dir_listing_enabled=true`) will discover these tables via directory listing
/// - Use the `migrate()` method to register these tables into the manifest
///
/// **Impact of mixing old and new clients:**
/// - Tables may appear inconsistently depending on which client created them
/// - Namespace operations will not see tables created by old clients
/// - After running `migrate()`, all tables will be consistent
///
/// ## Forwards Compatibility
///
/// ### Reading New Databases with Old Clients
///
/// Old clients (without manifest support) have limited visibility when reading databases created
/// by new clients:
///
/// **What old clients CAN see:**
/// - Tables in the root namespace with `.lance` extension in their location (via directory listing)
///
/// **What old clients CANNOT see:**
/// - Tables with hash-based locations (not discoverable via directory listing)
/// - Tables in nested namespaces (old clients only scan root namespace)
/// - Tables that have been renamed (location may no longer match discoverable name)
/// - The `__manifest` table itself (but it won't interfere with other operations)
///
/// **Recommendation:** If you need old client compatibility, ensure `parent_dir_listing_enabled=true`
/// when creating the manifest database. This ensures new tables use `.lance` extension and remain
/// discoverable via directory listing. Avoid using namespaces and rename operations if old clients
/// need access.
///
/// ## Migration Path
///
/// ### Phase 1: Enable Manifest (Migration Mode)
///
/// **This happens automatically when upgrading to the latest LanceDB version.**
///
/// The default configuration enables both manifest and directory listing. After upgrading,
/// simply connect to your existing database:
///
/// ```rust,no_run
/// # use lancedb::database::Database;
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
/// // Connect to existing database - migration mode is enabled by default
/// let db = ListingDatabase::connect_with_options(&request).await?;
///
/// // Both old and new tables are accessible
/// let tables = db.table_names(Default::default()).await?;
/// # Ok(())
/// # }
/// ```
///
/// **What happens:**
/// - Both old (directory-based) and new (manifest-based) tables are accessible
/// - New tables are automatically registered in manifest
/// - Old tables remain in directory-only mode until explicitly migrated
/// - No data is moved or modified
///
/// ### Phase 2: Run Migration
///
/// Once you've verified that Phase 1 is working (both old and new tables are accessible),
/// run the migration to register all existing tables in the manifest:
///
/// ```rust,no_run
/// # use lancedb::database::Database;
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
/// // Connect to the database
/// let db = ListingDatabase::connect_with_options(&request).await?;
///
/// // Run migration to register all existing tables in the manifest
/// let migrated_count = db.migrate().await?;
/// println!("Migrated {} tables to manifest", migrated_count);
/// # Ok(())
/// # }
/// ```
///
/// **What happens:**
/// - Scans directory for unmigrated tables
/// - Registers each table in the manifest with its current location
/// - Safe to run multiple times (skips already-migrated tables)
/// - Tables remain in their original locations with `.lance` extension
/// - No data is moved or modified
///
/// ### Phase 3: Disable Directory Fallback (Optional)
///
/// After confirming all tables are migrated and no old clients need directory fallback,
/// you can improve performance by disabling directory listing:
///
/// ```rust,no_run
/// # use lancedb::database::Database;
/// # use lancedb::connection::ConnectRequest;
/// # use lancedb::database::listing::ListingDatabase;
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// use std::collections::HashMap;
/// use lancedb::database::listing::OPT_DIR_LISTING_ENABLED;
///
/// let mut options = HashMap::new();
/// options.insert(OPT_DIR_LISTING_ENABLED.to_string(), "false".to_string());
///
/// let request = ConnectRequest {
///     uri: "/tmp/db".to_string(),
///     #[cfg(feature = "remote")]
///     client_config: Default::default(),
///     options,
///     read_consistency_interval: None,
///     session: None,
/// };
///
/// // Connect with directory listing disabled
/// let db = ListingDatabase::connect_with_options(&request).await?;
///
/// // Now only the manifest is consulted for all operations
/// let tables = db.table_names(Default::default()).await?;
/// # Ok(())
/// # }
/// ```
///
/// **What happens:**
/// - Only manifest is consulted for all operations (no directory scanning)
/// - Better performance for listing operations
/// - Old clients can still read existing tables but cannot discover them via listing
/// - Tables created by old clients (without manifest) will not be visible
///
/// ## Configuration Options
///
/// - `manifest_enabled`: Enable manifest-based listing (default: true)
/// - `dir_listing_enabled`: Enable directory listing fallback (default: true)
/// - `manifest_inline_optimization_enabled`: Auto-optimize manifest after updates (default: true)
/// - `manifest_read_consistency_interval`: How often to refresh manifest (default: None)
/// - `parent_dir_listing_enabled`: Whether new tables use `.lance` extension (default: true for compatibility)
///
/// ## Best Practices
///
/// 1. **New databases:** Use defaults (`manifest_enabled=true`, `dir_listing_enabled=true`)
/// 2. **Migrating existing databases:**
///    - Phase 1: Enable manifest with fallback
///    - Phase 2: Run `migrate()`
///    - Phase 3: Disable fallback after confirming migration
/// 3. **Mixed client environments:** Keep `dir_listing_enabled=true` and `parent_dir_listing_enabled=true`
/// 4. **Pure manifest environments:** Set `dir_listing_enabled=false` for best performance
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

    // Manifest-based database (None if initialization failed)
    manifest_db: Option<manifest::ManifestDatabase>,

    // Whether manifest-based listing is enabled
    manifest_enabled: bool,

    // Whether directory-based listing fallback is enabled
    dir_listing_enabled: bool,
}

/// Apply sorting and pagination to a list of names
///
/// This helper function sorts the names alphabetically and applies pagination
/// by skipping names up to and including `start_after`, then limiting the result size.
///
/// # Arguments
///
/// * `names` - Mutable vector of names to sort and paginate
/// * `start_after` - Optional name to start after (exclusive). Names are filtered to only
///   include those that come lexicographically after this value.
/// * `limit` - Optional maximum number of names to return
///
/// # Example
///
/// ```ignore
/// let mut names = vec!["table3".to_string(), "table1".to_string(), "table2".to_string()];
/// apply_pagination(&mut names, Some("table1".to_string()), Some(1));
/// assert_eq!(names, vec!["table2".to_string()]); // Sorted, skipped "table1", limited to 1
/// ```
pub(crate) fn apply_pagination(
    names: &mut Vec<String>,
    start_after: Option<String>,
    limit: Option<u32>,
) {
    apply_pagination_with_key(names, start_after.as_deref(), limit, |s| s.as_str());
}

/// Generic pagination utility that works on any type T
///
/// # Arguments
/// * `items` - The vector to paginate
/// * `start_after` - Skip items until finding one with a key greater than this value
/// * `limit` - Maximum number of items to keep
/// * `key_fn` - Function to extract the sortable/comparable key from each item (as &str)
///
/// # Example
/// ```ignore
/// struct TableInfo { name: String }
/// let mut items = vec![
///     TableInfo { name: "table3".to_string() },
///     TableInfo { name: "table1".to_string() },
/// ];
/// apply_pagination_with_key(&mut items, Some("table1"), Some(1), |t| t.name.as_str());
/// assert_eq!(items.len(), 1);
/// assert_eq!(items[0].name, "table3");
/// ```
pub(crate) fn apply_pagination_with_key<T, F>(
    items: &mut Vec<T>,
    start_after: Option<&str>,
    limit: Option<u32>,
    key_fn: F,
) where
    F: Fn(&T) -> &str,
{
    items.sort_by(|a, b| key_fn(a).cmp(key_fn(b)));
    if let Some(start_after) = start_after {
        if let Some(index) = items.iter().position(|item| key_fn(item) > start_after) {
            items.drain(0..index);
        } else {
            items.clear();
        }
    }
    if let Some(limit) = limit {
        items.truncate(limit as usize);
    }
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

                let (manifest_db, manifest_enabled) = match manifest::ManifestDatabase::new(
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
                .await
                {
                    Ok(db) => (Some(db), options.manifest_enabled),
                    Err(e) => {
                        log::warn!(
                            "Failed to initialize manifest database: {}. Falling back to directory listing only.",
                            e
                        );
                        (None, false)
                    }
                };

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
                    manifest_enabled,
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
        manifest_config: manifest::ManifestDatabaseConfig,
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

        let (manifest_db, manifest_enabled) = match manifest::ManifestDatabase::new(
            path.to_string(),
            None, // query string
            session.clone(),
            manifest_config,
            object_store.clone(),
            HashMap::new(), // storage options
            new_table_config.clone(),
            None, // store wrapper
            read_consistency_interval,
        )
        .await
        {
            Ok(db) => (Some(db), manifest_enabled),
            Err(e) => {
                log::warn!(
                    "Failed to initialize manifest database: {}. Falling back to directory listing only.",
                    e
                );
                (None, false)
            }
        };

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

    /// Inherit storage options from source into target map.
    /// Target options take precedence (not overwritten).
    pub(super) fn inherit_storage_options(
        source: &HashMap<String, String>,
        target: &mut HashMap<String, String>,
    ) {
        for (key, value) in source.iter() {
            // Only insert if the key doesn't already exist (target options take precedence)
            target.entry(key.clone()).or_insert_with(|| value.clone());
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
                // Only insert if the key doesn't already exist (table-level options should override)
                storage_options
                    .entry(key.clone())
                    .or_insert_with(|| value.clone());
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
    ///
    /// This is a static helper that can be reused by both ListingDatabase and ManifestDatabase.
    pub(super) async fn handle_table_exists<D: Database>(
        db: &D,
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
                let table = db.open_table(req).await?;

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
    /// to the manifest table.
    /// This migration can be run multiple times.
    pub async fn migrate(&self) -> Result<usize> {
        // We only care about tables in the root namespace.
        let Some(ref manifest_db) = self.manifest_db else {
            return Ok(0); // No manifest, nothing to migrate
        };
        let manifest_tables = manifest_db
            .list_tables(TableNamesRequest::default())
            .await?;
        // Collect directory names from manifest
        let manifest_locations: HashSet<String> = manifest_tables
            .iter()
            .map(|table_info| table_info.location.clone())
            .collect();
        let dir_tables = self.list_directory_tables().await?;

        // Register each directory table that doesn't have overlapping location
        // If a directory name already exists in the manifest,
        // that means the table must have already been migrated or created
        // in the manifest, so we can skip it.
        let mut migrated_count = 0;
        for table_name in dir_tables {
            // For root namespace tables, the directory name is "table_name.lance"
            let dir_name = format!("{}.{}", table_name, LANCE_FILE_EXTENSION);
            if !manifest_locations.contains(&dir_name) {
                manifest_db
                    .register_table(&table_name, &[], dir_name)
                    .await?;
                migrated_count += 1;
            }
        }

        Ok(migrated_count)
    }

    async fn list_directory_tables(&self) -> Result<Vec<String>> {
        Ok(self
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
            .collect::<Vec<String>>())
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
        if self.manifest_enabled {
            if let Some(ref manifest_db) = self.manifest_db {
                return manifest_db.list_namespaces(request).await;
            }
        }

        if !request.namespace.is_empty() {
            return Err(Error::NotSupported {
                message: "Namespace operations are not supported for listing database unless manifest_enabled is true".into(),
            });
        }

        Ok(Vec::new())
    }

    async fn create_namespace(&self, request: CreateNamespaceRequest) -> Result<()> {
        if self.manifest_enabled {
            if let Some(ref manifest_db) = self.manifest_db {
                return manifest_db.create_namespace(request).await;
            }
        }

        Err(Error::NotSupported {
            message: "Namespace operations are not supported for listing database unless manifest_enabled is true".into(),
        })
    }

    async fn drop_namespace(&self, request: DropNamespaceRequest) -> Result<()> {
        if self.manifest_enabled {
            if let Some(ref manifest_db) = self.manifest_db {
                return manifest_db.drop_namespace(request).await;
            }
        }

        Err(Error::NotSupported {
            message: "Namespace operations are not supported for listing database unless manifest_enabled is true".into(),
        })
    }

    async fn table_names(&self, request: TableNamesRequest) -> Result<Vec<String>> {
        if !request.namespace.is_empty() {
            if self.manifest_enabled {
                if let Some(ref manifest_db) = self.manifest_db {
                    return manifest_db.table_names(request.clone()).await;
                }
            }
            return Err(Error::NotSupported {
                message: "Namespace parameter is not supported for listing database unless manifest_enabled is true".into(),
            });
        }

        // When only manifest is enabled, we can directly return manifest results
        // with native pagination support
        if self.manifest_enabled && !self.dir_listing_enabled {
            if let Some(ref manifest_db) = self.manifest_db {
                return manifest_db.table_names(request).await;
            }
        }

        // When both manifest and directory listing are enabled, we need to deduplicate
        // by location to avoid listing the same table twice
        let mut f = if self.manifest_enabled && self.dir_listing_enabled {
            let mut manifest_request = request.clone();
            manifest_request.limit = None;
            manifest_request.start_after = None;
            let manifest_tables = if let Some(ref manifest_db) = self.manifest_db {
                manifest_db.list_tables(manifest_request).await?
            } else {
                vec![]
            };

            // Collect directory names from manifest
            let manifest_locations: HashSet<String> = manifest_tables
                .iter()
                .map(|table_info| table_info.location.clone())
                .collect();

            // Get table names from manifest
            let mut table_names: Vec<String> = manifest_tables
                .iter()
                .map(|table_info| table_info.name.clone())
                .collect();

            // Add directory tables that aren't already in the manifest
            let dir_tables = self.list_directory_tables().await?;
            for table_name in dir_tables {
                let dir_name = format!("{}.{}", table_name, LANCE_FILE_EXTENSION);
                if !manifest_locations.contains(&dir_name) {
                    table_names.push(table_name);
                }
            }

            table_names
        } else {
            self.list_directory_tables().await?
        };

        apply_pagination(&mut f, request.start_after, request.limit);
        Ok(f)
    }

    async fn create_table(&self, request: CreateTableRequest) -> Result<Arc<dyn BaseTable>> {
        if self.manifest_enabled {
            if let Some(ref manifest_db) = self.manifest_db {
                return match manifest_db.create_table(request).await {
                    Ok(table) => Ok(table),
                    Err(Error::Manifest { table, message }) if self.dir_listing_enabled => {
                        // To the ManifestDatabase, this table creation has failed.
                        // But to this caller ListingDatabase, the table has been successfully created
                        // in the right storage location, which means it has technically succeeded.
                        return if let Some(table) = table {
                            log::warn!("Failure in manifest operation: {message}");
                            Ok(table)
                        } else {
                            Err(Error::Manifest {
                                table: None,
                                message,
                            })
                        };
                    }
                    Err(e) => return Err(e),
                };
            }
        }

        if !request.namespace.is_empty() {
            return Err(Error::NotSupported {
                message: "Namespace parameter is not supported in listing database unless manifest_enabled is true".into(),
            });
        }
        let table_uri = self.table_uri(&request.name)?;

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
                Self::handle_table_exists(
                    self,
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
        if self.manifest_enabled {
            if let Some(ref manifest_db) = self.manifest_db {
                return match manifest_db.clone_table(request).await {
                    Ok(table) => Ok(table),
                    Err(Error::Manifest { table, message }) if self.dir_listing_enabled => {
                        // To the ManifestDatabase, this table cloning has failed.
                        // But to this caller ListingDatabase, the table has been successfully created
                        // in the right storage location, which means it has technically succeeded.
                        return if let Some(table) = table {
                            log::warn!("Failure in manifest operation: {message}");
                            Ok(table)
                        } else {
                            Err(Error::Manifest {
                                table: None,
                                message,
                            })
                        };
                    }
                    Err(e) => return Err(e),
                };
            }
        }

        if !request.target_namespace.is_empty() {
            return Err(Error::NotSupported {
                message: "Namespace parameter is not supported for listing database unless manifest_enabled is true".into(),
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

        let target_uri = self.table_uri(&request.target_table_name)?;
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
        if self.manifest_enabled {
            if let Some(ref manifest_db) = self.manifest_db {
                match manifest_db.open_table(request.clone()).await {
                    Ok(table) => return Ok(table),
                    Err(Error::TableNotFound { .. }) | Err(Error::NotSupported { .. }) => {
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
        }

        if !request.namespace.is_empty() {
            return Err(Error::NotSupported {
                message: "Namespace parameter is not supported for listing database unless manifest_enabled is true".into(),
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
            Self::inherit_storage_options(&self.storage_options, storage_options);
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
        cur_name: &str,
        new_name: &str,
        cur_namespace: &[String],
        new_namespace: &[String],
    ) -> Result<()> {
        if self.manifest_enabled {
            if let Some(ref manifest_db) = self.manifest_db {
                return manifest_db
                    .rename_table(cur_name, new_name, cur_namespace, new_namespace)
                    .await;
            }
        }

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
            message: "rename_table is not supported in LanceDB OSS without manifest_enabled=true"
                .into(),
        })
    }

    async fn drop_table(&self, name: &str, namespace: &[String]) -> Result<()> {
        if self.manifest_enabled {
            if let Some(ref manifest_db) = self.manifest_db {
                match manifest_db.drop_table(name, namespace).await {
                    Ok(()) => return Ok(()),
                    Err(Error::TableNotFound { .. }) => {
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
        }

        if !namespace.is_empty() {
            return Err(Error::NotSupported {
                message: "Namespace parameter is not supported for listing database.".into(),
            });
        }
        self.drop_tables(vec![name.to_string()]).await
    }

    async fn drop_all_tables(&self, namespace: &[String]) -> Result<()> {
        if !namespace.is_empty() {
            if self.manifest_enabled {
                if let Some(ref manifest_db) = self.manifest_db {
                    return manifest_db.drop_all_tables(namespace).await;
                }
            }
            return Err(Error::NotSupported {
                message: "Namespace parameter is not supported for listing database.".into(),
            });
        }

        if self.manifest_enabled {
            if let Some(ref manifest_db) = self.manifest_db {
                manifest_db.drop_all_tables(namespace).await?;
            }
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
    use rstest::rstest;
    use tempfile::tempdir;

    async fn setup_database(manifest_enabled: bool) -> (tempfile::TempDir, ListingDatabase) {
        let tempdir = tempdir().unwrap();
        let uri = tempdir.path().to_str().unwrap();

        // Configure database with manifest enabled/disabled
        let mut options = HashMap::new();
        options.insert(
            OPT_MANIFEST_ENABLED.to_string(),
            manifest_enabled.to_string(),
        );

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
    #[rstest]
    #[case(false)]
    #[case(true)]
    async fn test_clone_table_basic(#[case] manifest_enabled: bool) {
        let (_tempdir, db) = setup_database(manifest_enabled).await;

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
    #[rstest]
    #[case(false)]
    #[case(true)]
    async fn test_clone_table_with_data(#[case] manifest_enabled: bool) {
        let (_tempdir, db) = setup_database(manifest_enabled).await;

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
    #[rstest]
    #[case(false)]
    #[case(true)]
    async fn test_clone_table_with_storage_options(#[case] manifest_enabled: bool) {
        let tempdir = tempdir().unwrap();
        let uri = tempdir.path().to_str().unwrap();

        // Create database with storage options
        let mut options = HashMap::new();
        options.insert("test_option".to_string(), "test_value".to_string());
        options.insert(
            OPT_MANIFEST_ENABLED.to_string(),
            manifest_enabled.to_string(),
        );

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
    #[rstest]
    #[case(false)]
    #[case(true)]
    async fn test_clone_table_deep_not_supported(#[case] manifest_enabled: bool) {
        let (_tempdir, db) = setup_database(manifest_enabled).await;

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
    #[rstest]
    #[case(false)]
    #[case(true)]
    async fn test_clone_table_namespace_not_found(#[case] manifest_enabled: bool) {
        let (_tempdir, db) = setup_database(manifest_enabled).await;

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

        // Try clone to non-existent namespace
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
        let error = result.unwrap_err();
        if manifest_enabled {
            // With manifest enabled, should fail with NamespaceNotFound
            assert!(
                matches!(error, Error::NamespaceNotFound { .. }),
                "Expected NamespaceNotFound, got: {:?}",
                error
            );
        } else {
            // Without manifest, should fail with NotSupported (namespaces not supported)
            assert!(
                matches!(error, Error::NotSupported { .. }),
                "Expected NotSupported, got: {:?}",
                error
            );
        }
    }

    #[tokio::test]
    #[rstest]
    #[case(false)]
    #[case(true)]
    async fn test_clone_table_invalid_target_name(#[case] manifest_enabled: bool) {
        let (_tempdir, db) = setup_database(manifest_enabled).await;

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
    #[rstest]
    #[case(false)]
    #[case(true)]
    async fn test_clone_table_source_not_found(#[case] manifest_enabled: bool) {
        let (_tempdir, db) = setup_database(manifest_enabled).await;

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
    #[rstest]
    #[case(false)]
    #[case(true)]
    async fn test_clone_table_with_version_and_tag_error(#[case] manifest_enabled: bool) {
        let (_tempdir, db) = setup_database(manifest_enabled).await;

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
    #[rstest]
    #[case(false)]
    #[case(true)]
    async fn test_clone_table_with_specific_version(#[case] manifest_enabled: bool) {
        let (_tempdir, db) = setup_database(manifest_enabled).await;

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
    #[rstest]
    #[case(false)]
    #[case(true)]
    async fn test_clone_table_with_tag(#[case] manifest_enabled: bool) {
        let (_tempdir, db) = setup_database(manifest_enabled).await;

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
    #[rstest]
    #[case(false)]
    #[case(true)]
    async fn test_cloned_tables_evolve_independently(#[case] manifest_enabled: bool) {
        let (_tempdir, db) = setup_database(manifest_enabled).await;

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
    #[rstest]
    #[case(false)]
    #[case(true)]
    async fn test_clone_latest_version(#[case] manifest_enabled: bool) {
        let (_tempdir, db) = setup_database(manifest_enabled).await;

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
    async fn test_validation_both_manifest_and_dir_listing_flags_false() {
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
