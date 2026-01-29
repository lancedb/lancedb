// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Functions to establish a connection to a LanceDB database

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use lance::dataset::ReadParams;
use lance_namespace::models::{
    CreateNamespaceRequest, CreateNamespaceResponse, DescribeNamespaceRequest,
    DescribeNamespaceResponse, DropNamespaceRequest, DropNamespaceResponse, ListNamespacesRequest,
    ListNamespacesResponse, ListTablesRequest, ListTablesResponse,
};
#[cfg(feature = "aws")]
use object_store::aws::AwsCredential;

use crate::connection::create_table::CreateTableBuilder;
use crate::data::scannable::Scannable;
use crate::database::listing::ListingDatabase;
use crate::database::{
    CloneTableRequest, Database, DatabaseOptions, OpenTableRequest, ReadConsistency,
    TableNamesRequest,
};
use crate::embeddings::{EmbeddingRegistry, MemoryRegistry};
use crate::error::{Error, Result};
#[cfg(feature = "remote")]
use crate::remote::{
    client::ClientConfig,
    db::{OPT_REMOTE_API_KEY, OPT_REMOTE_HOST_OVERRIDE, OPT_REMOTE_REGION},
};
use crate::Table;
use lance::io::ObjectStoreParams;
pub use lance_encoding::version::LanceFileVersion;
#[cfg(feature = "remote")]
use lance_io::object_store::StorageOptions;
use lance_io::object_store::{StorageOptionsAccessor, StorageOptionsProvider};

mod create_table;

fn merge_storage_options(
    store_params: &mut ObjectStoreParams,
    pairs: impl IntoIterator<Item = (String, String)>,
) {
    let mut options = store_params.storage_options().cloned().unwrap_or_default();
    for (key, value) in pairs {
        options.insert(key, value);
    }
    let provider = store_params
        .storage_options_accessor
        .as_ref()
        .and_then(|accessor| accessor.provider().cloned());
    let accessor = if let Some(provider) = provider {
        StorageOptionsAccessor::with_initial_and_provider(options, provider)
    } else {
        StorageOptionsAccessor::with_static_options(options)
    };
    store_params.storage_options_accessor = Some(Arc::new(accessor));
}

fn set_storage_options_provider(
    store_params: &mut ObjectStoreParams,
    provider: Arc<dyn StorageOptionsProvider>,
) {
    let accessor = match store_params.storage_options().cloned() {
        Some(options) => StorageOptionsAccessor::with_initial_and_provider(options, provider),
        None => StorageOptionsAccessor::with_provider(provider),
    };
    store_params.storage_options_accessor = Some(Arc::new(accessor));
}

/// A builder for configuring a [`Connection::table_names`] operation
pub struct TableNamesBuilder {
    parent: Arc<dyn Database>,
    request: TableNamesRequest,
}

impl TableNamesBuilder {
    fn new(parent: Arc<dyn Database>) -> Self {
        Self {
            parent,
            request: TableNamesRequest::default(),
        }
    }

    /// If present, only return names that come lexicographically after the supplied
    /// value.
    ///
    /// This can be combined with limit to implement pagination by setting this to
    /// the last table name from the previous page.
    pub fn start_after(mut self, start_after: impl Into<String>) -> Self {
        self.request.start_after = Some(start_after.into());
        self
    }

    /// The maximum number of table names to return
    pub fn limit(mut self, limit: u32) -> Self {
        self.request.limit = Some(limit);
        self
    }

    /// Set the namespace to list tables from
    pub fn namespace(mut self, namespace: Vec<String>) -> Self {
        self.request.namespace = namespace;
        self
    }

    /// Execute the table names operation
    #[allow(deprecated)]
    pub async fn execute(self) -> Result<Vec<String>> {
        self.parent.clone().table_names(self.request).await
    }
}

#[derive(Clone, Debug)]
pub struct OpenTableBuilder {
    parent: Arc<dyn Database>,
    request: OpenTableRequest,
    embedding_registry: Arc<dyn EmbeddingRegistry>,
}

impl OpenTableBuilder {
    pub(crate) fn new(
        parent: Arc<dyn Database>,
        name: String,
        embedding_registry: Arc<dyn EmbeddingRegistry>,
    ) -> Self {
        Self {
            parent,
            request: OpenTableRequest {
                name,
                namespace: vec![],
                index_cache_size: None,
                lance_read_params: None,
                location: None,
                namespace_client: None,
            },
            embedding_registry,
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
        self.request.index_cache_size = Some(index_cache_size);
        self
    }

    /// Advanced parameters that can be used to customize table reads
    ///
    /// If set, these will take precedence over any overlapping `OpenTableOptions` options
    pub fn lance_read_params(mut self, params: ReadParams) -> Self {
        self.request.lance_read_params = Some(params);
        self
    }

    /// Set an option for the storage layer.
    ///
    /// Options already set on the connection will be inherited by the table,
    /// but can be overridden here.
    ///
    /// See available options at <https://lancedb.com/docs/storage/>
    pub fn storage_option(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        let store_params = self
            .request
            .lance_read_params
            .get_or_insert(Default::default())
            .store_options
            .get_or_insert(Default::default());
        merge_storage_options(store_params, [(key.into(), value.into())]);
        self
    }

    /// Set multiple options for the storage layer.
    ///
    /// Options already set on the connection will be inherited by the table,
    /// but can be overridden here.
    ///
    /// See available options at <https://lancedb.com/docs/storage/>
    pub fn storage_options(
        mut self,
        pairs: impl IntoIterator<Item = (impl Into<String>, impl Into<String>)>,
    ) -> Self {
        let store_params = self
            .request
            .lance_read_params
            .get_or_insert(Default::default())
            .store_options
            .get_or_insert(Default::default());
        let updates = pairs
            .into_iter()
            .map(|(key, value)| (key.into(), value.into()));
        merge_storage_options(store_params, updates);
        self
    }

    /// Set the namespace for the table
    pub fn namespace(mut self, namespace: Vec<String>) -> Self {
        self.request.namespace = namespace;
        self
    }

    /// Set a custom location for the table.
    ///
    /// If not set, the database will derive a location from its URI and the table name.
    /// This is useful when integrating with namespace systems that manage table locations.
    pub fn location(mut self, location: impl Into<String>) -> Self {
        self.request.location = Some(location.into());
        self
    }

    /// Set a storage options provider for automatic credential refresh.
    ///
    /// This allows tables to automatically refresh cloud storage credentials
    /// when they expire, enabling long-running operations on remote storage.
    pub fn storage_options_provider(mut self, provider: Arc<dyn StorageOptionsProvider>) -> Self {
        let store_params = self
            .request
            .lance_read_params
            .get_or_insert(Default::default())
            .store_options
            .get_or_insert(Default::default());
        set_storage_options_provider(store_params, provider);
        self
    }

    /// Open the table
    pub async fn execute(self) -> Result<Table> {
        let table = self.parent.open_table(self.request).await?;
        Ok(Table::new_with_embedding_registry(
            table,
            self.parent,
            self.embedding_registry,
        ))
    }
}

/// Builder for cloning a table.
///
/// A shallow clone creates a new table that shares the underlying data files
/// with the source table but has its own independent manifest. Both the source
/// and cloned tables can evolve independently while initially sharing the same
/// data, deletion, and index files.
///
/// Use this builder to configure the clone operation before executing it.
pub struct CloneTableBuilder {
    parent: Arc<dyn Database>,
    request: CloneTableRequest,
}

impl CloneTableBuilder {
    fn new(parent: Arc<dyn Database>, target_table_name: String, source_uri: String) -> Self {
        Self {
            parent,
            request: CloneTableRequest::new(target_table_name, source_uri),
        }
    }

    /// Set the source version to clone from
    pub fn source_version(mut self, version: u64) -> Self {
        self.request.source_version = Some(version);
        self
    }

    /// Set the source tag to clone from
    pub fn source_tag(mut self, tag: impl Into<String>) -> Self {
        self.request.source_tag = Some(tag.into());
        self
    }

    /// Set the target namespace for the cloned table
    pub fn target_namespace(mut self, namespace: Vec<String>) -> Self {
        self.request.target_namespace = namespace;
        self
    }

    /// Set whether to perform a shallow clone (default: true)
    ///
    /// When true, the cloned table shares data files with the source table.
    /// When false, performs a deep clone (not yet implemented).
    pub fn is_shallow(mut self, is_shallow: bool) -> Self {
        self.request.is_shallow = is_shallow;
        self
    }

    /// Execute the clone operation
    pub async fn execute(self) -> Result<Table> {
        let parent = self.parent.clone();
        let table = parent.clone_table(self.request).await?;
        Ok(Table::new(table, parent))
    }
}

/// A connection to LanceDB
#[derive(Clone)]
pub struct Connection {
    internal: Arc<dyn Database>,
    embedding_registry: Arc<dyn EmbeddingRegistry>,
}

impl std::fmt::Display for Connection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.internal)
    }
}

impl Connection {
    pub fn new(
        internal: Arc<dyn Database>,
        embedding_registry: Arc<dyn EmbeddingRegistry>,
    ) -> Self {
        Self {
            internal,
            embedding_registry,
        }
    }

    /// Get the URI of the connection
    pub fn uri(&self) -> &str {
        self.internal.uri()
    }

    /// Get access to the underlying database
    pub fn database(&self) -> &Arc<dyn Database> {
        &self.internal
    }

    /// Get the names of all tables in the database
    ///
    /// The names will be returned in lexicographical order (ascending)
    ///
    /// The parameters `page_token` and `limit` can be used to paginate the results
    pub fn table_names(&self) -> TableNamesBuilder {
        TableNamesBuilder::new(self.internal.clone())
    }

    /// Create a new table from an iterator of data
    ///
    /// # Parameters
    ///
    /// * `name` - The name of the table
    /// * `initial_data` - The initial data to write to the table
    pub fn create_table<T: Scannable + 'static>(
        &self,
        name: impl Into<String>,
        initial_data: T,
    ) -> CreateTableBuilder {
        let initial_data = Box::new(initial_data);
        CreateTableBuilder::new(
            self.internal.clone(),
            self.embedding_registry.clone(),
            name.into(),
            initial_data,
        )
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
    ) -> CreateTableBuilder {
        let empty_batch = RecordBatch::new_empty(schema);
        self.create_table(name, empty_batch)
    }

    /// Open an existing table in the database
    ///
    /// # Arguments
    /// * `name` - The name of the table
    ///
    /// # Returns
    /// Created [`TableRef`], or [`Error::TableNotFound`] if the table does not exist.
    pub fn open_table(&self, name: impl Into<String>) -> OpenTableBuilder {
        OpenTableBuilder::new(
            self.internal.clone(),
            name.into(),
            self.embedding_registry.clone(),
        )
    }

    /// Clone a table in the database
    ///
    /// Creates a new table by cloning from an existing source table.
    /// By default, this performs a shallow clone where the new table shares
    /// the underlying data files with the source table.
    ///
    /// # Parameters
    /// - `target_table_name`: The name of the new table to create
    /// - `source_uri`: The URI of the source table to clone from
    ///
    /// # Returns
    /// A [`CloneTableBuilder`] that can be used to configure the clone operation
    pub fn clone_table(
        &self,
        target_table_name: impl Into<String>,
        source_uri: impl Into<String>,
    ) -> CloneTableBuilder {
        CloneTableBuilder::new(
            self.internal.clone(),
            target_table_name.into(),
            source_uri.into(),
        )
    }

    /// Rename a table in the database.
    ///
    /// This is only supported in LanceDB Cloud.
    pub async fn rename_table(
        &self,
        old_name: impl AsRef<str>,
        new_name: impl AsRef<str>,
        cur_namespace: &[String],
        new_namespace: &[String],
    ) -> Result<()> {
        self.internal
            .rename_table(
                old_name.as_ref(),
                new_name.as_ref(),
                cur_namespace,
                new_namespace,
            )
            .await
    }

    /// Get the read consistency of the connection
    pub async fn read_consistency(&self) -> Result<ReadConsistency> {
        self.internal.read_consistency().await
    }

    /// Drop a table in the database.
    ///
    /// # Arguments
    /// * `name` - The name of the table to drop
    /// * `namespace` - The namespace to drop the table from
    pub async fn drop_table(&self, name: impl AsRef<str>, namespace: &[String]) -> Result<()> {
        self.internal.drop_table(name.as_ref(), namespace).await
    }

    /// Drop the database
    ///
    /// This is the same as dropping all of the tables
    #[deprecated(since = "0.15.1", note = "Use `drop_all_tables` instead")]
    pub async fn drop_db(&self) -> Result<()> {
        self.internal.drop_all_tables(&[]).await
    }

    /// Drops all tables in the database
    ///
    /// # Arguments
    /// * `namespace` - The namespace to drop all tables from. Empty slice represents root namespace.
    pub async fn drop_all_tables(&self, namespace: &[String]) -> Result<()> {
        self.internal.drop_all_tables(namespace).await
    }

    /// List immediate child namespace names in the given namespace
    pub async fn list_namespaces(
        &self,
        request: ListNamespacesRequest,
    ) -> Result<ListNamespacesResponse> {
        self.internal.list_namespaces(request).await
    }

    /// Create a new namespace
    pub async fn create_namespace(
        &self,
        request: CreateNamespaceRequest,
    ) -> Result<CreateNamespaceResponse> {
        self.internal.create_namespace(request).await
    }

    /// Drop a namespace
    pub async fn drop_namespace(
        &self,
        request: DropNamespaceRequest,
    ) -> Result<DropNamespaceResponse> {
        self.internal.drop_namespace(request).await
    }

    /// Describe a namespace
    pub async fn describe_namespace(
        &self,
        request: DescribeNamespaceRequest,
    ) -> Result<DescribeNamespaceResponse> {
        self.internal.describe_namespace(request).await
    }

    /// Get the equivalent namespace client in the database of this connection.
    /// For LanceNamespaceDatabase, it is the underlying LanceNamespace.
    /// For ListingDatabase, it is the equivalent DirectoryNamespace.
    /// For RemoteDatabase, it is the equivalent RestNamespace.
    pub async fn namespace_client(&self) -> Result<Arc<dyn lance_namespace::LanceNamespace>> {
        self.internal.namespace_client().await
    }

    /// List tables with pagination support
    pub async fn list_tables(&self, request: ListTablesRequest) -> Result<ListTablesResponse> {
        self.internal.list_tables(request).await
    }

    /// Get the in-memory embedding registry.
    /// It's important to note that the embedding registry is not persisted across connections.
    /// So if a table contains embeddings, you will need to make sure that you are using a connection that has the same embedding functions registered
    pub fn embedding_registry(&self) -> &dyn EmbeddingRegistry {
        self.embedding_registry.as_ref()
    }
}

/// A request to connect to a database
#[derive(Clone, Debug)]
pub struct ConnectRequest {
    /// Database URI
    ///
    /// ### Accpeted URI formats
    ///
    /// - `/path/to/database` - local database on file system.
    /// - `s3://bucket/path/to/database` or `gs://bucket/path/to/database` - database on cloud object store
    /// - `db://dbname` - LanceDB Cloud
    pub uri: String,

    #[cfg(feature = "remote")]
    pub client_config: ClientConfig,

    /// Database specific options
    pub options: HashMap<String, String>,

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
    pub read_consistency_interval: Option<std::time::Duration>,

    /// Optional session for object stores and caching
    ///
    /// If provided, this session will be used instead of creating a default one.
    /// This allows for custom configuration of object store registries, caching, etc.
    pub session: Option<Arc<lance::session::Session>>,
}

#[derive(Debug)]
pub struct ConnectBuilder {
    request: ConnectRequest,
    embedding_registry: Option<Arc<dyn EmbeddingRegistry>>,
}

#[cfg(feature = "remote")]
const ENV_VARS_TO_STORAGE_OPTS: [(&str, &str); 1] =
    [("AZURE_STORAGE_ACCOUNT_NAME", "azure_storage_account_name")];

impl ConnectBuilder {
    /// Create a new [`ConnectOptions`] with the given database URI.
    pub fn new(uri: &str) -> Self {
        Self {
            request: ConnectRequest {
                uri: uri.to_string(),
                #[cfg(feature = "remote")]
                client_config: Default::default(),
                read_consistency_interval: None,
                options: HashMap::new(),
                session: None,
            },
            embedding_registry: None,
        }
    }

    /// Set the LanceDB Cloud API key.
    ///
    /// This option is only used when connecting to LanceDB Cloud (db:// URIs)
    /// and will be ignored for other URIs.
    ///
    /// # Arguments
    ///
    /// * `api_key` - The API key to use for the connection
    #[cfg(feature = "remote")]
    pub fn api_key(mut self, api_key: &str) -> Self {
        self.request
            .options
            .insert(OPT_REMOTE_API_KEY.to_string(), api_key.to_string());
        self
    }

    /// Set the LanceDB Cloud region.
    ///
    /// This option is only used when connecting to LanceDB Cloud (db:// URIs)
    /// and will be ignored for other URIs.
    ///
    /// # Arguments
    ///
    /// * `region` - The region to use for the connection
    #[cfg(feature = "remote")]
    pub fn region(mut self, region: &str) -> Self {
        self.request
            .options
            .insert(OPT_REMOTE_REGION.to_string(), region.to_string());
        self
    }

    /// Set the LanceDB Cloud host override.
    ///
    /// This option is only used when connecting to LanceDB Cloud (db:// URIs)
    /// and will be ignored for other URIs.
    ///
    /// # Arguments
    ///
    /// * `host_override` - The host override to use for the connection
    #[cfg(feature = "remote")]
    pub fn host_override(mut self, host_override: &str) -> Self {
        self.request.options.insert(
            OPT_REMOTE_HOST_OVERRIDE.to_string(),
            host_override.to_string(),
        );
        self
    }

    /// Set the database specific options
    ///
    /// See [crate::database::listing::ListingDatabaseOptions] for the options available for
    /// native LanceDB databases.
    ///
    /// See [crate::remote::db::RemoteDatabaseOptions] for the options available for
    /// LanceDB Cloud and LanceDB Enterprise.
    pub fn database_options(mut self, database_options: &dyn DatabaseOptions) -> Self {
        database_options.serialize_into_map(&mut self.request.options);
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
        self.request.client_config = config;
        self
    }

    /// Provide a custom [`EmbeddingRegistry`] to use for this connection.
    pub fn embedding_registry(mut self, registry: Arc<dyn EmbeddingRegistry>) -> Self {
        self.embedding_registry = Some(registry);
        self
    }

    /// [`AwsCredential`] to use when connecting to S3.
    #[cfg(feature = "aws")]
    #[deprecated(note = "Pass through storage_options instead")]
    pub fn aws_creds(mut self, aws_creds: AwsCredential) -> Self {
        self.request
            .options
            .insert("aws_access_key_id".into(), aws_creds.key_id.clone());
        self.request
            .options
            .insert("aws_secret_access_key".into(), aws_creds.secret_key.clone());
        if let Some(token) = &aws_creds.token {
            self.request
                .options
                .insert("aws_session_token".into(), token.clone());
        }
        self
    }

    /// Set an option for the storage layer.
    ///
    /// See available options at <https://lancedb.com/docs/storage/>
    pub fn storage_option(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.request.options.insert(key.into(), value.into());
        self
    }

    /// Set multiple options for the storage layer.
    ///
    /// See available options at <https://lancedb.com/docs/storage/>
    pub fn storage_options(
        mut self,
        pairs: impl IntoIterator<Item = (impl Into<String>, impl Into<String>)>,
    ) -> Self {
        for (key, value) in pairs {
            self.request.options.insert(key.into(), value.into());
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
        self.request.read_consistency_interval = Some(read_consistency_interval);
        self
    }

    /// Set a custom session for object stores and caching.
    ///
    /// By default, a new session with default configuration will be created.
    /// This method allows you to provide a custom session with your own
    /// configuration for object store registries, caching, etc.
    ///
    /// # Arguments
    ///
    /// * `session` - A custom session to use for this connection
    pub fn session(mut self, session: Arc<lance::session::Session>) -> Self {
        self.request.session = Some(session);
        self
    }

    #[cfg(feature = "remote")]
    fn apply_env_defaults(
        env_var_to_remote_storage_option: &[(&str, &str)],
        options: &mut HashMap<String, String>,
    ) {
        for (env_key, opt_key) in env_var_to_remote_storage_option {
            if let Ok(env_value) = std::env::var(env_key) {
                if !options.contains_key(*opt_key) {
                    options.insert((*opt_key).to_string(), env_value);
                }
            }
        }
    }

    #[cfg(feature = "remote")]
    fn execute_remote(self) -> Result<Connection> {
        use crate::remote::db::RemoteDatabaseOptions;

        let mut merged_options = self.request.options.clone();
        Self::apply_env_defaults(&ENV_VARS_TO_STORAGE_OPTS, &mut merged_options);
        let options = RemoteDatabaseOptions::parse_from_map(&merged_options)?;

        let region = options.region.ok_or_else(|| Error::InvalidInput {
            message: "A region is required when connecting to LanceDb Cloud".to_string(),
        })?;
        let api_key = options.api_key.ok_or_else(|| Error::InvalidInput {
            message: "An api_key is required when connecting to LanceDb Cloud".to_string(),
        })?;

        let storage_options = StorageOptions(options.storage_options.clone());
        let internal = Arc::new(crate::remote::db::RemoteDatabase::try_new(
            &self.request.uri,
            &api_key,
            &region,
            options.host_override,
            self.request.client_config,
            storage_options.into(),
        )?);
        Ok(Connection {
            internal,
            embedding_registry: self
                .embedding_registry
                .unwrap_or_else(|| Arc::new(MemoryRegistry::new())),
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
        if self.request.uri.starts_with("db") {
            self.execute_remote()
        } else {
            let internal = Arc::new(ListingDatabase::connect_with_options(&self.request).await?);
            Ok(Connection {
                internal,
                embedding_registry: self
                    .embedding_registry
                    .unwrap_or_else(|| Arc::new(MemoryRegistry::new())),
            })
        }
    }
}

/// Connect to a LanceDB database.
///
/// # Arguments
///
/// * `uri` - URI where the database is located, can be a local directory, supported remote cloud storage,
///   or a LanceDB Cloud database.  See [ConnectOptions::uri] for a list of accepted formats
pub fn connect(uri: &str) -> ConnectBuilder {
    ConnectBuilder::new(uri)
}

pub struct ConnectNamespaceBuilder {
    ns_impl: String,
    properties: HashMap<String, String>,
    storage_options: HashMap<String, String>,
    read_consistency_interval: Option<std::time::Duration>,
    embedding_registry: Option<Arc<dyn EmbeddingRegistry>>,
    session: Option<Arc<lance::session::Session>>,
    server_side_query_enabled: bool,
}

impl ConnectNamespaceBuilder {
    fn new(ns_impl: &str, properties: HashMap<String, String>) -> Self {
        Self {
            ns_impl: ns_impl.to_string(),
            properties,
            storage_options: HashMap::new(),
            read_consistency_interval: None,
            embedding_registry: None,
            session: None,
            server_side_query_enabled: false,
        }
    }

    /// Set an option for the storage layer.
    ///
    /// See available options at <https://lancedb.com/docs/storage/>
    pub fn storage_option(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.storage_options.insert(key.into(), value.into());
        self
    }

    /// Set multiple options for the storage layer.
    ///
    /// See available options at <https://lancedb.com/docs/storage/>
    pub fn storage_options(
        mut self,
        pairs: impl IntoIterator<Item = (impl Into<String>, impl Into<String>)>,
    ) -> Self {
        for (key, value) in pairs {
            self.storage_options.insert(key.into(), value.into());
        }
        self
    }

    /// The interval at which to check for updates from other processes.
    ///
    /// If left unset, consistency is not checked. For maximum read
    /// performance, this is the default. For strong consistency, set this to
    /// zero seconds. Then every read will check for updates from other processes.
    /// As a compromise, set this to a non-zero duration for eventual consistency.
    pub fn read_consistency_interval(
        mut self,
        read_consistency_interval: std::time::Duration,
    ) -> Self {
        self.read_consistency_interval = Some(read_consistency_interval);
        self
    }

    /// Provide a custom [`EmbeddingRegistry`] to use for this connection.
    pub fn embedding_registry(mut self, registry: Arc<dyn EmbeddingRegistry>) -> Self {
        self.embedding_registry = Some(registry);
        self
    }

    /// Set a custom session for object stores and caching.
    ///
    /// By default, a new session with default configuration will be created.
    /// This method allows you to provide a custom session with your own
    /// configuration for object store registries, caching, etc.
    pub fn session(mut self, session: Arc<lance::session::Session>) -> Self {
        self.session = Some(session);
        self
    }

    /// Enable server-side query execution.
    ///
    /// When enabled, queries will be executed on the namespace server instead of
    /// locally. This can improve performance by reducing data transfer and
    /// leveraging server-side compute resources.
    ///
    /// Default is `false` (queries executed locally).
    pub fn server_side_query(mut self, enabled: bool) -> Self {
        self.server_side_query_enabled = enabled;
        self
    }

    /// Execute the connection
    pub async fn execute(self) -> Result<Connection> {
        use crate::database::namespace::LanceNamespaceDatabase;

        let internal = Arc::new(
            LanceNamespaceDatabase::connect(
                &self.ns_impl,
                self.properties,
                self.storage_options,
                self.read_consistency_interval,
                self.session,
                self.server_side_query_enabled,
            )
            .await?,
        );

        Ok(Connection {
            internal,
            embedding_registry: self
                .embedding_registry
                .unwrap_or_else(|| Arc::new(MemoryRegistry::new())),
        })
    }
}

/// Connect to a LanceDB database through a namespace.
///
/// # Arguments
///
/// * `ns_impl` - The namespace implementation to use (e.g., "dir" for directory-based, "rest" for REST API)
/// * `properties` - Configuration properties for the namespace implementation
/// ```
pub fn connect_namespace(
    ns_impl: &str,
    properties: HashMap<String, String>,
) -> ConnectNamespaceBuilder {
    ConnectNamespaceBuilder::new(ns_impl, properties)
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
                embedding_registry: Arc::new(MemoryRegistry::new()),
            }
        }

        pub fn new_with_handler_and_config<T>(
            handler: impl Fn(reqwest::Request) -> http::Response<T> + Clone + Send + Sync + 'static,
            config: crate::remote::ClientConfig,
        ) -> Self
        where
            T: Into<reqwest::Body>,
        {
            let internal = Arc::new(crate::remote::db::RemoteDatabase::new_mock_with_config(
                handler, config,
            ));
            Self {
                internal,
                embedding_registry: Arc::new(MemoryRegistry::new()),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::RecordBatchReader;
    use arrow_schema::{DataType, Field, Schema};
    use lance_testing::datagen::{BatchGenerator, IncrementingInt32};
    use tempfile::tempdir;

    use crate::test_utils::connection::new_test_connection;

    use super::*;

    #[tokio::test]
    async fn test_connect() {
        let tc = new_test_connection().await.unwrap();
        assert_eq!(tc.connection.uri(), tc.uri);
    }

    #[cfg(feature = "remote")]
    #[test]
    fn test_apply_env_defaults() {
        let env_key = "TEST_APPLY_ENV_DEFAULTS_ENVIRONMENT_VARIABLE_ENV_KEY";
        let env_val = "TEST_APPLY_ENV_DEFAULTS_ENVIRONMENT_VARIABLE_ENV_VAL";
        let opts_key = "test_apply_env_defaults_environment_variable_opts_key";
        std::env::set_var(env_key, env_val);

        let mut options = HashMap::new();
        ConnectBuilder::apply_env_defaults(&[(env_key, opts_key)], &mut options);
        assert_eq!(Some(&env_val.to_string()), options.get(opts_key));

        options.insert(opts_key.to_string(), "EXPLICIT-VALUE".to_string());
        ConnectBuilder::apply_env_defaults(&[(env_key, opts_key)], &mut options);
        assert_eq!(Some(&"EXPLICIT-VALUE".to_string()), options.get(opts_key));
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

        assert_eq!(db.uri(), relative_uri.to_str().unwrap().to_string());
    }

    #[tokio::test]
    async fn test_table_names() {
        let tc = new_test_connection().await.unwrap();
        let db = tc.connection;
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let mut names = Vec::with_capacity(100);
        for _ in 0..100 {
            let name = uuid::Uuid::new_v4().to_string();
            names.push(name.clone());
            db.create_empty_table(name, schema.clone())
                .execute()
                .await
                .unwrap();
        }
        names.sort();
        let tables = db.table_names().limit(100).execute().await.unwrap();

        assert_eq!(tables, names);

        let tables = db
            .table_names()
            .start_after(&names[30])
            .limit(100)
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
    async fn test_open_table() {
        let tc = new_test_connection().await.unwrap();
        let db = tc.connection;

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

    #[tokio::test]
    async fn drop_table() {
        let tc = new_test_connection().await.unwrap();
        let db = tc.connection;

        if tc.is_remote {
            // All the typical endpoints such as s3:///, file-object-store:///, etc. treat drop_table
            // as idempotent.
            assert!(db.drop_table("invalid_table", &[]).await.is_ok());
        } else {
            // The behavior of drop_table when using a file:/// endpoint differs from all other
            // object providers, in that it returns an error when deleting a non-existent table.
            assert!(matches!(
                db.drop_table("invalid_table", &[]).await,
                Err(crate::Error::TableNotFound { .. }),
            ));
        }

        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        db.create_empty_table("table1", schema.clone())
            .execute()
            .await
            .unwrap();
        db.drop_table("table1", &[]).await.unwrap();

        let tables = db.table_names().execute().await.unwrap();
        assert_eq!(tables.len(), 0);
    }

    #[tokio::test]
    async fn test_clone_table() {
        let tmp_dir = tempdir().unwrap();
        let uri = tmp_dir.path().to_str().unwrap();
        let db = connect(uri).execute().await.unwrap();

        // Create a source table with some data
        let mut batch_gen = BatchGenerator::new()
            .col(Box::new(IncrementingInt32::new().named("id")))
            .col(Box::new(IncrementingInt32::new().named("value")));
        let reader: Box<dyn RecordBatchReader + Send> = Box::new(batch_gen.batches(5, 100));

        let source_table = db
            .create_table("source_table", reader)
            .execute()
            .await
            .unwrap();

        // Get the source table URI
        let source_table_path = tmp_dir.path().join("source_table.lance");
        let source_uri = source_table_path.to_str().unwrap();

        // Clone the table
        let cloned_table = db
            .clone_table("cloned_table", source_uri)
            .execute()
            .await
            .unwrap();

        // Verify the cloned table exists
        let table_names = db.table_names().execute().await.unwrap();
        assert!(table_names.contains(&"source_table".to_string()));
        assert!(table_names.contains(&"cloned_table".to_string()));

        // Verify the cloned table has the same schema
        assert_eq!(
            source_table.schema().await.unwrap(),
            cloned_table.schema().await.unwrap()
        );

        // Verify the cloned table has the same data
        let source_count = source_table.count_rows(None).await.unwrap();
        let cloned_count = cloned_table.count_rows(None).await.unwrap();
        assert_eq!(source_count, cloned_count);
    }
}
