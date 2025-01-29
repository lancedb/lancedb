// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! LanceDB Database

use std::collections::HashMap;
use std::fs::create_dir_all;
use std::path::Path;
use std::sync::Arc;

use arrow_array::{RecordBatchIterator, RecordBatchReader};
use arrow_schema::SchemaRef;
use lance::dataset::{ReadParams, WriteMode};
use lance::io::{ObjectStore, ObjectStoreParams, ObjectStoreRegistry, WrappingObjectStore};
use object_store::{aws::AwsCredential, local::LocalFileSystem};
use snafu::prelude::*;

use crate::arrow::IntoArrow;
use crate::embeddings::{
    EmbeddingDefinition, EmbeddingFunction, EmbeddingRegistry, MemoryRegistry, WithEmbeddings,
};
use crate::error::{CreateDirSnafu, Error, InvalidTableNameSnafu, Result};
use crate::io::object_store::MirroringObjectStoreWrapper;
#[cfg(feature = "remote")]
use crate::remote::client::ClientConfig;
use crate::table::{NativeTable, TableDefinition, WriteOptions};
use crate::utils::validate_table_name;
use crate::Table;
pub use lance_encoding::version::LanceFileVersion;
#[cfg(feature = "remote")]
use lance_io::object_store::StorageOptions;
use lance_table::io::commit::commit_handler_from_url;

pub const LANCE_FILE_EXTENSION: &str = "lance";

pub type TableBuilderCallback = Box<dyn FnOnce(OpenTableBuilder) -> OpenTableBuilder + Send>;

/// Describes what happens when creating a table and a table with
/// the same name already exists
pub enum CreateTableMode {
    /// If the table already exists, an error is returned
    Create,
    /// If the table already exists, it is opened.  Any provided data is
    /// ignored.  The function will be passed an OpenTableBuilder to customize
    /// how the table is opened
    ExistOk(TableBuilderCallback),
    /// If the table already exists, it is overwritten
    Overwrite,
}

impl CreateTableMode {
    pub fn exist_ok(
        callback: impl FnOnce(OpenTableBuilder) -> OpenTableBuilder + Send + 'static,
    ) -> Self {
        Self::ExistOk(Box::new(callback))
    }
}

impl Default for CreateTableMode {
    fn default() -> Self {
        Self::Create
    }
}

/// Describes what happens when a vector either contains NaN or
/// does not have enough values
#[derive(Clone, Debug, Default)]
enum BadVectorHandling {
    /// An error is returned
    #[default]
    Error,
    #[allow(dead_code)] // https://github.com/lancedb/lancedb/issues/992
    /// The offending row is droppped
    Drop,
    #[allow(dead_code)] // https://github.com/lancedb/lancedb/issues/992
    /// The invalid/missing items are replaced by fill_value
    Fill(f32),
}

/// A builder for configuring a [`Connection::table_names`] operation
pub struct TableNamesBuilder {
    parent: Arc<dyn ConnectionInternal>,
    pub(crate) start_after: Option<String>,
    pub(crate) limit: Option<u32>,
}

impl TableNamesBuilder {
    fn new(parent: Arc<dyn ConnectionInternal>) -> Self {
        Self {
            parent,
            start_after: None,
            limit: None,
        }
    }

    /// If present, only return names that come lexicographically after the supplied
    /// value.
    ///
    /// This can be combined with limit to implement pagination by setting this to
    /// the last table name from the previous page.
    pub fn start_after(mut self, start_after: impl Into<String>) -> Self {
        self.start_after = Some(start_after.into());
        self
    }

    /// The maximum number of table names to return
    pub fn limit(mut self, limit: u32) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Execute the table names operation
    pub async fn execute(self) -> Result<Vec<String>> {
        self.parent.clone().table_names(self).await
    }
}

pub struct NoData {}

impl IntoArrow for NoData {
    fn into_arrow(self) -> Result<Box<dyn arrow_array::RecordBatchReader + Send>> {
        unreachable!("NoData should never be converted to Arrow")
    }
}

/// A builder for configuring a [`Connection::create_table`] operation
pub struct CreateTableBuilder<const HAS_DATA: bool, T: IntoArrow> {
    pub(crate) parent: Arc<dyn ConnectionInternal>,
    pub(crate) name: String,
    pub(crate) data: Option<T>,
    pub(crate) mode: CreateTableMode,
    pub(crate) write_options: WriteOptions,
    pub(crate) table_definition: Option<TableDefinition>,
    pub(crate) embeddings: Vec<(EmbeddingDefinition, Arc<dyn EmbeddingFunction>)>,
    pub(crate) data_storage_version: Option<LanceFileVersion>,
    pub(crate) enable_v2_manifest_paths: Option<bool>,
}

// Builder methods that only apply when we have initial data
impl<T: IntoArrow> CreateTableBuilder<true, T> {
    fn new(parent: Arc<dyn ConnectionInternal>, name: String, data: T) -> Self {
        Self {
            parent,
            name,
            data: Some(data),
            mode: CreateTableMode::default(),
            write_options: WriteOptions::default(),
            table_definition: None,
            embeddings: Vec::new(),
            data_storage_version: None,
            enable_v2_manifest_paths: None,
        }
    }

    /// Apply the given write options when writing the initial data
    pub fn write_options(mut self, write_options: WriteOptions) -> Self {
        self.write_options = write_options;
        self
    }

    /// Execute the create table operation
    pub async fn execute(self) -> Result<Table> {
        let parent = self.parent.clone();
        let (data, builder) = self.extract_data()?;
        parent.do_create_table(builder, data).await
    }

    fn extract_data(
        mut self,
    ) -> Result<(
        Box<dyn RecordBatchReader + Send>,
        CreateTableBuilder<false, NoData>,
    )> {
        let data = self.data.take().unwrap().into_arrow()?;
        let builder = CreateTableBuilder::<false, NoData> {
            parent: self.parent,
            name: self.name,
            data: None,
            table_definition: self.table_definition,
            mode: self.mode,
            write_options: self.write_options,
            embeddings: self.embeddings,
            data_storage_version: self.data_storage_version,
            enable_v2_manifest_paths: self.enable_v2_manifest_paths,
        };
        Ok((data, builder))
    }
}

// Builder methods that only apply when we do not have initial data
impl CreateTableBuilder<false, NoData> {
    fn new(parent: Arc<dyn ConnectionInternal>, name: String, schema: SchemaRef) -> Self {
        let table_definition = TableDefinition::new_from_schema(schema);
        Self {
            parent,
            name,
            data: None,
            table_definition: Some(table_definition),
            mode: CreateTableMode::default(),
            write_options: WriteOptions::default(),
            embeddings: Vec::new(),
            data_storage_version: None,
            enable_v2_manifest_paths: None,
        }
    }

    /// Execute the create table operation
    pub async fn execute(self) -> Result<Table> {
        self.parent.clone().do_create_empty_table(self).await
    }
}

impl<const HAS_DATA: bool, T: IntoArrow> CreateTableBuilder<HAS_DATA, T> {
    /// Set the mode for creating the table
    ///
    /// This controls what happens if a table with the given name already exists
    pub fn mode(mut self, mode: CreateTableMode) -> Self {
        self.mode = mode;
        self
    }

    /// Set an option for the storage layer.
    ///
    /// Options already set on the connection will be inherited by the table,
    /// but can be overridden here.
    ///
    /// See available options at <https://lancedb.github.io/lancedb/guides/storage/>
    pub fn storage_option(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        let store_options = self
            .write_options
            .lance_write_params
            .get_or_insert(Default::default())
            .store_params
            .get_or_insert(Default::default())
            .storage_options
            .get_or_insert(Default::default());
        store_options.insert(key.into(), value.into());
        self
    }

    /// Set multiple options for the storage layer.
    ///
    /// Options already set on the connection will be inherited by the table,
    /// but can be overridden here.
    ///
    /// See available options at <https://lancedb.github.io/lancedb/guides/storage/>
    pub fn storage_options(
        mut self,
        pairs: impl IntoIterator<Item = (impl Into<String>, impl Into<String>)>,
    ) -> Self {
        let store_options = self
            .write_options
            .lance_write_params
            .get_or_insert(Default::default())
            .store_params
            .get_or_insert(Default::default())
            .storage_options
            .get_or_insert(Default::default());

        for (key, value) in pairs {
            store_options.insert(key.into(), value.into());
        }
        self
    }

    /// Set whether to use V2 manifest paths for the table. (default: false)
    ///
    /// These paths provide more efficient opening of tables with many
    /// versions on object stores.
    ///
    /// <div class="warning">Turning this on will make the dataset unreadable
    /// for older versions of LanceDB (prior to 0.10.0).</div>
    ///
    /// To migrate an existing dataset, instead use the
    /// [[NativeTable::migrate_manifest_paths_v2]].
    ///
    /// This has no effect in LanceDB Cloud.
    pub fn enable_v2_manifest_paths(mut self, use_v2_manifest_paths: bool) -> Self {
        self.enable_v2_manifest_paths = Some(use_v2_manifest_paths);
        self
    }

    /// Set the data storage version.
    ///
    /// The default is `LanceFileVersion::Stable`.
    pub fn data_storage_version(mut self, data_storage_version: LanceFileVersion) -> Self {
        self.data_storage_version = Some(data_storage_version);
        self
    }

    /// Set to true to use the v1 format for data files
    ///
    /// This is set to false by default to enable the stable format.
    /// This should only be used for experimentation and
    /// evaluation. This option may be removed in the future releases.
    #[deprecated(since = "0.9.0", note = "use data_storage_version instead")]
    pub fn use_legacy_format(mut self, use_legacy_format: bool) -> Self {
        self.data_storage_version = if use_legacy_format {
            Some(LanceFileVersion::Legacy)
        } else {
            Some(LanceFileVersion::Stable)
        };
        self
    }

    /// Add an embedding definition to the table.
    ///
    /// The `embedding_name` must match the name of an embedding function that
    /// was previously registered with the connection's [`EmbeddingRegistry`].
    pub fn add_embedding(mut self, definition: EmbeddingDefinition) -> Result<Self> {
        // Early verification of the embedding name
        let embedding_func = self
            .parent
            .embedding_registry()
            .get(&definition.embedding_name)
            .ok_or_else(|| Error::EmbeddingFunctionNotFound {
                name: definition.embedding_name.clone(),
                reason: "No embedding function found in the connection's embedding_registry"
                    .to_string(),
            })?;

        self.embeddings.push((definition, embedding_func));
        Ok(self)
    }
}

#[derive(Clone, Debug)]
pub struct OpenTableBuilder {
    pub(crate) parent: Arc<dyn ConnectionInternal>,
    pub(crate) name: String,
    index_cache_size: u32,
    lance_read_params: Option<ReadParams>,
}

impl OpenTableBuilder {
    pub(crate) fn new(parent: Arc<dyn ConnectionInternal>, name: String) -> Self {
        Self {
            parent,
            name,
            index_cache_size: 256,
            lance_read_params: None,
        }
    }

    /// Set the size of the index cache, specified as a number of entries
    ///
    /// The default value is 256
    ///
    /// The exact meaning of an "entry" will depend on the type of index:
    /// * IVF - there is one entry for each IVF partition
    /// * BTREE - there is one entry for the entire index
    ///
    /// This cache applies to the entire opened table, across all indices.
    /// Setting this value higher will increase performance on larger datasets
    /// at the expense of more RAM
    pub fn index_cache_size(mut self, index_cache_size: u32) -> Self {
        self.index_cache_size = index_cache_size;
        self
    }

    /// Advanced parameters that can be used to customize table reads
    ///
    /// If set, these will take precedence over any overlapping `OpenTableOptions` options
    pub fn lance_read_params(mut self, params: ReadParams) -> Self {
        self.lance_read_params = Some(params);
        self
    }

    /// Set an option for the storage layer.
    ///
    /// Options already set on the connection will be inherited by the table,
    /// but can be overridden here.
    ///
    /// See available options at <https://lancedb.github.io/lancedb/guides/storage/>
    pub fn storage_option(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        let storage_options = self
            .lance_read_params
            .get_or_insert(Default::default())
            .store_options
            .get_or_insert(Default::default())
            .storage_options
            .get_or_insert(Default::default());
        storage_options.insert(key.into(), value.into());
        self
    }

    /// Set multiple options for the storage layer.
    ///
    /// Options already set on the connection will be inherited by the table,
    /// but can be overridden here.
    ///
    /// See available options at <https://lancedb.github.io/lancedb/guides/storage/>
    pub fn storage_options(
        mut self,
        pairs: impl IntoIterator<Item = (impl Into<String>, impl Into<String>)>,
    ) -> Self {
        let storage_options = self
            .lance_read_params
            .get_or_insert(Default::default())
            .store_options
            .get_or_insert(Default::default())
            .storage_options
            .get_or_insert(Default::default());

        for (key, value) in pairs {
            storage_options.insert(key.into(), value.into());
        }
        self
    }

    /// Open the table
    pub async fn execute(self) -> Result<Table> {
        self.parent.clone().do_open_table(self).await
    }
}

#[async_trait::async_trait]
pub(crate) trait ConnectionInternal:
    Send + Sync + std::fmt::Debug + std::fmt::Display + 'static
{
    fn embedding_registry(&self) -> &dyn EmbeddingRegistry;
    async fn table_names(&self, options: TableNamesBuilder) -> Result<Vec<String>>;
    async fn do_create_table(
        &self,
        options: CreateTableBuilder<false, NoData>,
        data: Box<dyn RecordBatchReader + Send>,
    ) -> Result<Table>;
    async fn do_open_table(&self, options: OpenTableBuilder) -> Result<Table>;
    async fn rename_table(&self, old_name: &str, new_name: &str) -> Result<()>;
    async fn drop_table(&self, name: &str) -> Result<()>;
    async fn drop_db(&self) -> Result<()>;

    async fn do_create_empty_table(
        &self,
        options: CreateTableBuilder<false, NoData>,
    ) -> Result<Table> {
        let batches = Box::new(RecordBatchIterator::new(
            vec![],
            options.table_definition.clone().unwrap().schema.clone(),
        ));
        self.do_create_table(options, batches).await
    }
}

/// A connection to LanceDB
#[derive(Clone)]
pub struct Connection {
    uri: String,
    internal: Arc<dyn ConnectionInternal>,
}

impl std::fmt::Display for Connection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.internal)
    }
}

impl Connection {
    /// Get the URI of the connection
    pub fn uri(&self) -> &str {
        self.uri.as_str()
    }

    /// Get the names of all tables in the database
    ///
    /// The names will be returned in lexicographical order (ascending)
    ///
    /// The parameters `page_token` and `limit` can be used to paginate the results
    pub fn table_names(&self) -> TableNamesBuilder {
        TableNamesBuilder::new(self.internal.clone())
    }

    /// Create a new table from data
    ///
    /// # Parameters
    ///
    /// * `name` - The name of the table
    /// * `initial_data` - The initial data to write to the table
    pub fn create_table<T: IntoArrow>(
        &self,
        name: impl Into<String>,
        initial_data: T,
    ) -> CreateTableBuilder<true, T> {
        CreateTableBuilder::<true, T>::new(self.internal.clone(), name.into(), initial_data)
    }

    /// Create an empty table with a given schema
    ///
    /// # Parameters
    ///
    /// * `name` - The name of the table
    /// * `schema` - The schema of the table
    pub fn create_empty_table(
        &self,
        name: impl Into<String>,
        schema: SchemaRef,
    ) -> CreateTableBuilder<false, NoData> {
        CreateTableBuilder::<false, NoData>::new(self.internal.clone(), name.into(), schema)
    }

    /// Open an existing table in the database
    ///
    /// # Arguments
    /// * `name` - The name of the table
    ///
    /// # Returns
    /// Created [`TableRef`], or [`Error::TableNotFound`] if the table does not exist.
    pub fn open_table(&self, name: impl Into<String>) -> OpenTableBuilder {
        OpenTableBuilder::new(self.internal.clone(), name.into())
    }

    /// Rename a table in the database.
    ///
    /// This is only supported in LanceDB Cloud.
    pub async fn rename_table(
        &self,
        old_name: impl AsRef<str>,
        new_name: impl AsRef<str>,
    ) -> Result<()> {
        self.internal
            .rename_table(old_name.as_ref(), new_name.as_ref())
            .await
    }

    /// Drop a table in the database.
    ///
    /// # Arguments
    /// * `name` - The name of the table to drop
    pub async fn drop_table(&self, name: impl AsRef<str>) -> Result<()> {
        self.internal.drop_table(name.as_ref()).await
    }

    /// Drop the database
    ///
    /// This is the same as dropping all of the tables
    pub async fn drop_db(&self) -> Result<()> {
        self.internal.drop_db().await
    }

    /// Get the in-memory embedding registry.
    /// It's important to note that the embedding registry is not persisted across connections.
    /// So if a table contains embeddings, you will need to make sure that you are using a connection that has the same embedding functions registered
    pub fn embedding_registry(&self) -> &dyn EmbeddingRegistry {
        self.internal.embedding_registry()
    }
}

#[derive(Debug)]
pub struct ConnectBuilder {
    /// Database URI
    ///
    /// ### Accpeted URI formats
    ///
    /// - `/path/to/database` - local database on file system.
    /// - `s3://bucket/path/to/database` or `gs://bucket/path/to/database` - database on cloud object store
    /// - `db://dbname` - LanceDB Cloud
    uri: String,

    /// LanceDB Cloud API key, required if using Lance Cloud
    api_key: Option<String>,
    /// LanceDB Cloud region, required if using Lance Cloud
    region: Option<String>,
    /// LanceDB Cloud host override, only required if using an on-premises Lance Cloud instance
    host_override: Option<String>,
    #[cfg(feature = "remote")]
    client_config: ClientConfig,

    storage_options: HashMap<String, String>,

    /// The interval at which to check for updates from other processes.
    ///
    /// If None, then consistency is not checked. For performance
    /// reasons, this is the default. For strong consistency, set this to
    /// zero seconds. Then every read will check for updates from other
    /// processes. As a compromise, you can set this to a non-zero timedelta
    /// for eventual consistency. If more than that interval has passed since
    /// the last check, then the table will be checked for updates. Note: this
    /// consistency only applies to read operations. Write operations are
    /// always consistent.
    read_consistency_interval: Option<std::time::Duration>,
    embedding_registry: Option<Arc<dyn EmbeddingRegistry>>,
}

impl ConnectBuilder {
    /// Create a new [`ConnectOptions`] with the given database URI.
    pub fn new(uri: &str) -> Self {
        Self {
            uri: uri.to_string(),
            api_key: None,
            region: None,
            host_override: None,
            #[cfg(feature = "remote")]
            client_config: Default::default(),
            read_consistency_interval: None,
            storage_options: HashMap::new(),
            embedding_registry: None,
        }
    }

    pub fn api_key(mut self, api_key: &str) -> Self {
        self.api_key = Some(api_key.to_string());
        self
    }

    pub fn region(mut self, region: &str) -> Self {
        self.region = Some(region.to_string());
        self
    }

    pub fn host_override(mut self, host_override: &str) -> Self {
        self.host_override = Some(host_override.to_string());
        self
    }

    /// Set the LanceDB Cloud client configuration.
    ///
    /// ```no_run
    /// # use lancedb::connect;
    /// # use lancedb::remote::*;
    /// connect("db://my_database")
    ///    .client_config(ClientConfig {
    ///      timeout_config: TimeoutConfig {
    ///        connect_timeout: Some(std::time::Duration::from_secs(5)),
    ///        ..Default::default()
    ///      },
    ///      retry_config: RetryConfig {
    ///        retries: Some(5),
    ///        ..Default::default()
    ///      },
    ///      ..Default::default()
    ///    });
    /// ```
    #[cfg(feature = "remote")]
    pub fn client_config(mut self, config: ClientConfig) -> Self {
        self.client_config = config;
        self
    }

    /// Provide a custom [`EmbeddingRegistry`] to use for this connection.
    pub fn embedding_registry(mut self, registry: Arc<dyn EmbeddingRegistry>) -> Self {
        self.embedding_registry = Some(registry);
        self
    }

    /// [`AwsCredential`] to use when connecting to S3.
    #[deprecated(note = "Pass through storage_options instead")]
    pub fn aws_creds(mut self, aws_creds: AwsCredential) -> Self {
        self.storage_options
            .insert("aws_access_key_id".into(), aws_creds.key_id.clone());
        self.storage_options
            .insert("aws_secret_access_key".into(), aws_creds.secret_key.clone());
        if let Some(token) = &aws_creds.token {
            self.storage_options
                .insert("aws_session_token".into(), token.clone());
        }
        self
    }

    /// Set an option for the storage layer.
    ///
    /// See available options at <https://lancedb.github.io/lancedb/guides/storage/>
    pub fn storage_option(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.storage_options.insert(key.into(), value.into());
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
            self.storage_options.insert(key.into(), value.into());
        }
        self
    }

    /// The interval at which to check for updates from other processes. This
    /// only affects LanceDB OSS.
    ///
    /// If left unset, consistency is not checked. For maximum read
    /// performance, this is the default. For strong consistency, set this to
    /// zero seconds. Then every read will check for updates from other processes.
    /// As a compromise, set this to a non-zero duration for eventual consistency.
    /// If more than that duration has passed since the last read, the read will
    /// check for updates from other processes.
    ///
    /// This only affects read operations. Write operations are always
    /// consistent.
    ///
    /// LanceDB Cloud uses eventual consistency under the hood, and is not
    /// currently configurable.
    pub fn read_consistency_interval(
        mut self,
        read_consistency_interval: std::time::Duration,
    ) -> Self {
        self.read_consistency_interval = Some(read_consistency_interval);
        self
    }

    #[cfg(feature = "remote")]
    fn execute_remote(self) -> Result<Connection> {
        let region = self.region.ok_or_else(|| Error::InvalidInput {
            message: "A region is required when connecting to LanceDb Cloud".to_string(),
        })?;
        let api_key = self.api_key.ok_or_else(|| Error::InvalidInput {
            message: "An api_key is required when connecting to LanceDb Cloud".to_string(),
        })?;

        let storage_options = StorageOptions(self.storage_options.clone());
        let internal = Arc::new(crate::remote::db::RemoteDatabase::try_new(
            &self.uri,
            &api_key,
            &region,
            self.host_override,
            self.client_config,
            storage_options.into(),
        )?);
        Ok(Connection {
            internal,
            uri: self.uri,
        })
    }

    #[cfg(not(feature = "remote"))]
    fn execute_remote(self) -> Result<Connection> {
        Err(Error::Runtime {
            message: "cannot connect to LanceDb Cloud unless the 'remote' feature is enabled"
                .to_string(),
        })
    }

    /// Establishes a connection to the database
    pub async fn execute(self) -> Result<Connection> {
        if self.uri.starts_with("db") {
            self.execute_remote()
        } else {
            let internal = Arc::new(Database::connect_with_options(&self).await?);
            Ok(Connection {
                internal,
                uri: self.uri,
            })
        }
    }
}

/// Connect to a LanceDB database.
///
/// # Arguments
///
/// * `uri` - URI where the database is located, can be a local directory, supported remote cloud storage,
///           or a LanceDB Cloud database.  See [ConnectOptions::uri] for a list of accepted formats
pub fn connect(uri: &str) -> ConnectBuilder {
    ConnectBuilder::new(uri)
}

#[derive(Debug)]
struct Database {
    object_store: ObjectStore,
    query_string: Option<String>,

    pub(crate) uri: String,
    pub(crate) base_path: object_store::path::Path,

    // the object store wrapper to use on write path
    pub(crate) store_wrapper: Option<Arc<dyn WrappingObjectStore>>,

    read_consistency_interval: Option<std::time::Duration>,

    // Storage options to be inherited by tables created from this connection
    storage_options: HashMap<String, String>,
    embedding_registry: Arc<dyn EmbeddingRegistry>,
}

impl std::fmt::Display for Database {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "NativeDatabase(uri={}, read_consistency_interval={})",
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
impl Database {
    async fn connect_with_options(options: &ConnectBuilder) -> Result<Self> {
        let uri = &options.uri;
        let parse_res = url::Url::parse(uri);

        // TODO: pass params regardless of OS
        match parse_res {
            Ok(url) if url.scheme().len() == 1 && cfg!(windows) => {
                Self::open_path(
                    uri,
                    options.read_consistency_interval,
                    options.embedding_registry.clone(),
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
                let storage_options = options.storage_options.clone();
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

                let embedding_registry = options
                    .embedding_registry
                    .clone()
                    .unwrap_or_else(|| Arc::new(MemoryRegistry::new()));
                Ok(Self {
                    uri: table_base_uri,
                    query_string,
                    base_path,
                    object_store,
                    store_wrapper: write_store_wrapper,
                    read_consistency_interval: options.read_consistency_interval,
                    storage_options,
                    embedding_registry,
                })
            }
            Err(_) => {
                Self::open_path(
                    uri,
                    options.read_consistency_interval,
                    options.embedding_registry.clone(),
                )
                .await
            }
        }
    }

    async fn open_path(
        path: &str,
        read_consistency_interval: Option<std::time::Duration>,
        embedding_registry: Option<Arc<dyn EmbeddingRegistry>>,
    ) -> Result<Self> {
        let (object_store, base_path) = ObjectStore::from_uri(path).await?;
        if object_store.is_local() {
            Self::try_create_dir(path).context(CreateDirSnafu { path })?;
        }

        let embedding_registry =
            embedding_registry.unwrap_or_else(|| Arc::new(MemoryRegistry::new()));

        Ok(Self {
            uri: path.to_string(),
            query_string: None,
            base_path,
            object_store,
            store_wrapper: None,
            read_consistency_interval,
            storage_options: HashMap::new(),
            embedding_registry,
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
impl ConnectionInternal for Database {
    fn embedding_registry(&self) -> &dyn EmbeddingRegistry {
        self.embedding_registry.as_ref()
    }
    async fn table_names(&self, options: TableNamesBuilder) -> Result<Vec<String>> {
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
        if let Some(start_after) = options.start_after {
            let index = f
                .iter()
                .position(|name| name.as_str() > start_after.as_str())
                .unwrap_or(f.len());
            f.drain(0..index);
        }
        if let Some(limit) = options.limit {
            f.truncate(limit as usize);
        }
        Ok(f)
    }

    async fn do_create_table(
        &self,
        mut options: CreateTableBuilder<false, NoData>,
        data: Box<dyn RecordBatchReader + Send>,
    ) -> Result<Table> {
        let table_uri = self.table_uri(&options.name)?;
        let embedding_registry = self.embedding_registry.clone();
        // Inherit storage options from the connection
        let storage_options = options
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
        let data = if options.embeddings.is_empty() {
            data
        } else {
            Box::new(WithEmbeddings::new(data, options.embeddings))
        };

        let mut write_params = options.write_options.lance_write_params.unwrap_or_default();

        if matches!(&options.mode, CreateTableMode::Overwrite) {
            write_params.mode = WriteMode::Overwrite;
        }

        write_params.data_storage_version = options.data_storage_version;
        write_params.enable_v2_manifest_paths =
            options.enable_v2_manifest_paths.unwrap_or_default();

        let data_schema = data.schema();

        match NativeTable::create(
            &table_uri,
            &options.name,
            data,
            self.store_wrapper.clone(),
            Some(write_params),
            self.read_consistency_interval,
        )
        .await
        {
            Ok(table) => Ok(Table::new_with_embedding_registry(
                Arc::new(table),
                embedding_registry,
            )),
            Err(Error::TableAlreadyExists { name }) => match options.mode {
                CreateTableMode::Create => Err(Error::TableAlreadyExists { name }),
                CreateTableMode::ExistOk(callback) => {
                    let builder = OpenTableBuilder::new(options.parent, options.name);
                    let builder = (callback)(builder);
                    let table = builder.execute().await?;

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

    async fn do_open_table(&self, mut options: OpenTableBuilder) -> Result<Table> {
        let table_uri = self.table_uri(&options.name)?;

        // Inherit storage options from the connection
        let storage_options = options
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
        let read_params = options.lance_read_params.unwrap_or_else(|| ReadParams {
            index_cache_size: options.index_cache_size as usize,
            ..Default::default()
        });

        let native_table = Arc::new(
            NativeTable::open_with_params(
                &table_uri,
                &options.name,
                self.store_wrapper.clone(),
                Some(read_params),
                self.read_consistency_interval,
            )
            .await?,
        );
        Ok(Table::new(native_table))
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

    async fn drop_db(&self) -> Result<()> {
        self.object_store
            .remove_dir_all(self.base_path.clone())
            .await?;
        Ok(())
    }
}

#[cfg(all(test, feature = "remote"))]
mod test_utils {
    use super::*;
    impl Connection {
        pub fn new_with_handler<T>(
            handler: impl Fn(reqwest::Request) -> http::Response<T> + Clone + Send + Sync + 'static,
        ) -> Self
        where
            T: Into<reqwest::Body>,
        {
            let internal = Arc::new(crate::remote::db::RemoteDatabase::new_mock(handler));
            Self {
                internal,
                uri: "db://test".to_string(),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use arrow_schema::{DataType, Field, Schema};
    use futures::TryStreamExt;
    use lance_testing::datagen::{BatchGenerator, IncrementingInt32};
    use tempfile::tempdir;

    use crate::query::QueryBase;
    use crate::query::{ExecutableQuery, QueryExecutionOptions};

    use super::*;

    #[tokio::test]
    async fn test_connect() {
        let tmp_dir = tempdir().unwrap();
        let uri = tmp_dir.path().to_str().unwrap();
        let db = connect(uri).execute().await.unwrap();

        assert_eq!(db.uri, uri);
    }

    #[cfg(not(windows))]
    #[tokio::test]
    async fn test_connect_relative() {
        let tmp_dir = tempdir().unwrap();
        let uri = std::fs::canonicalize(tmp_dir.path().to_str().unwrap()).unwrap();

        let current_dir = std::env::current_dir().unwrap();
        let ancestors = current_dir.ancestors();
        let relative_ancestors = vec![".."; ancestors.count()];

        let relative_root = std::path::PathBuf::from(relative_ancestors.join("/"));
        let relative_uri = relative_root.join(&uri);

        let db = connect(relative_uri.to_str().unwrap())
            .execute()
            .await
            .unwrap();

        assert_eq!(db.uri, relative_uri.to_str().unwrap().to_string());
    }

    #[tokio::test]
    async fn test_table_names() {
        let tmp_dir = tempdir().unwrap();
        let mut names = Vec::with_capacity(100);
        for _ in 0..100 {
            let mut name = uuid::Uuid::new_v4().to_string();
            names.push(name.clone());
            name.push_str(".lance");
            create_dir_all(tmp_dir.path().join(&name)).unwrap();
        }
        names.sort();

        let uri = tmp_dir.path().to_str().unwrap();
        let db = connect(uri).execute().await.unwrap();
        let tables = db.table_names().execute().await.unwrap();

        assert_eq!(tables, names);

        let tables = db
            .table_names()
            .start_after(&names[30])
            .execute()
            .await
            .unwrap();

        assert_eq!(tables, names[31..]);

        let tables = db
            .table_names()
            .start_after(&names[30])
            .limit(7)
            .execute()
            .await
            .unwrap();

        assert_eq!(tables, names[31..38]);

        let tables = db.table_names().limit(7).execute().await.unwrap();

        assert_eq!(tables, names[..7]);
    }

    #[tokio::test]
    async fn test_connect_s3() {
        // let db = Database::connect("s3://bucket/path/to/database").await.unwrap();
    }

    #[tokio::test]
    async fn test_open_table() {
        let tmp_dir = tempdir().unwrap();
        let uri = tmp_dir.path().to_str().unwrap();
        let db = connect(uri).execute().await.unwrap();

        assert_eq!(db.table_names().execute().await.unwrap().len(), 0);
        // open non-exist table
        assert!(matches!(
            db.open_table("invalid_table").execute().await,
            Err(crate::Error::TableNotFound { .. })
        ));

        assert_eq!(db.table_names().execute().await.unwrap().len(), 0);

        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        db.create_empty_table("table1", schema)
            .execute()
            .await
            .unwrap();
        db.open_table("table1").execute().await.unwrap();
        let tables = db.table_names().execute().await.unwrap();
        assert_eq!(tables, vec!["table1".to_owned()]);
    }

    fn make_data() -> Box<dyn RecordBatchReader + Send + 'static> {
        let id = Box::new(IncrementingInt32::new().named("id".to_string()));
        Box::new(BatchGenerator::new().col(id).batches(10, 2000))
    }

    #[tokio::test]
    async fn test_create_table_v2() {
        let tmp_dir = tempdir().unwrap();
        let uri = tmp_dir.path().to_str().unwrap();
        let db = connect(uri).execute().await.unwrap();

        let tbl = db
            .create_table("v1_test", make_data())
            .data_storage_version(LanceFileVersion::Legacy)
            .execute()
            .await
            .unwrap();

        // In v1 the row group size will trump max_batch_length
        let batches = tbl
            .query()
            .limit(20000)
            .execute_with_options(QueryExecutionOptions {
                max_batch_length: 50000,
                ..Default::default()
            })
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(batches.len(), 20);

        let tbl = db
            .create_table("v2_test", make_data())
            .data_storage_version(LanceFileVersion::Stable)
            .execute()
            .await
            .unwrap();

        // In v2 the page size is much bigger than 50k so we should get a single batch
        let batches = tbl
            .query()
            .execute_with_options(QueryExecutionOptions {
                max_batch_length: 50000,
                ..Default::default()
            })
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert_eq!(batches.len(), 1);
    }

    #[tokio::test]
    async fn drop_table() {
        let tmp_dir = tempdir().unwrap();

        let uri = tmp_dir.path().to_str().unwrap();
        let db = connect(uri).execute().await.unwrap();

        // drop non-exist table
        assert!(matches!(
            db.drop_table("invalid_table").await,
            Err(crate::Error::TableNotFound { .. }),
        ));

        create_dir_all(tmp_dir.path().join("table1.lance")).unwrap();
        db.drop_table("table1").await.unwrap();

        let tables = db.table_names().execute().await.unwrap();
        assert_eq!(tables.len(), 0);
    }

    #[tokio::test]
    async fn test_create_table_already_exists() {
        let tmp_dir = tempdir().unwrap();
        let uri = tmp_dir.path().to_str().unwrap();
        let db = connect(uri).execute().await.unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        db.create_empty_table("test", schema.clone())
            .execute()
            .await
            .unwrap();
        // TODO: None of the open table options are "inspectable" right now but once one is we
        // should assert we are passing these options in correctly
        db.create_empty_table("test", schema)
            .mode(CreateTableMode::exist_ok(|builder| {
                builder.index_cache_size(16)
            }))
            .execute()
            .await
            .unwrap();
        let other_schema = Arc::new(Schema::new(vec![Field::new("y", DataType::Int32, false)]));
        assert!(db
            .create_empty_table("test", other_schema.clone())
            .execute()
            .await
            .is_err());
        let overwritten = db
            .create_empty_table("test", other_schema.clone())
            .mode(CreateTableMode::Overwrite)
            .execute()
            .await
            .unwrap();
        assert_eq!(other_schema, overwritten.schema().await.unwrap());
    }
}
