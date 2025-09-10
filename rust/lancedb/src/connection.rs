// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Functions to establish a connection to a LanceDB database

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::RecordBatchReader;
use arrow_schema::{Field, SchemaRef};
use lance::dataset::ReadParams;
#[cfg(feature = "aws")]
use object_store::aws::AwsCredential;

use crate::arrow::{IntoArrow, IntoArrowStream, SendableRecordBatchStream};
use crate::catalog::listing::ListingCatalog;
use crate::catalog::CatalogOptions;
use crate::database::listing::{
    ListingDatabase, OPT_NEW_TABLE_STORAGE_VERSION, OPT_NEW_TABLE_V2_MANIFEST_PATHS,
};
use crate::database::{
    CreateNamespaceRequest, CreateTableData, CreateTableMode, CreateTableRequest, Database,
    DatabaseOptions, DropNamespaceRequest, ListNamespacesRequest, OpenTableRequest,
    TableNamesRequest,
};
use crate::embeddings::{
    EmbeddingDefinition, EmbeddingFunction, EmbeddingRegistry, MemoryRegistry, WithEmbeddings,
};
use crate::error::{Error, Result};
#[cfg(feature = "remote")]
use crate::remote::{
    client::ClientConfig,
    db::{OPT_REMOTE_API_KEY, OPT_REMOTE_HOST_OVERRIDE, OPT_REMOTE_REGION},
};
use crate::table::{TableDefinition, WriteOptions};
use crate::Table;
pub use lance_encoding::version::LanceFileVersion;
#[cfg(feature = "remote")]
use lance_io::object_store::StorageOptions;

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
    pub async fn execute(self) -> Result<Vec<String>> {
        self.parent.clone().table_names(self.request).await
    }
}

pub struct NoData {}

impl IntoArrow for NoData {
    fn into_arrow(self) -> Result<Box<dyn arrow_array::RecordBatchReader + Send>> {
        unreachable!("NoData should never be converted to Arrow")
    }
}

// Stores the value given from the initial CreateTableBuilder::new call
// and defers errors until `execute` is called
enum CreateTableBuilderInitialData {
    None,
    Iterator(Result<Box<dyn RecordBatchReader + Send>>),
    Stream(Result<SendableRecordBatchStream>),
}

/// A builder for configuring a [`Connection::create_table`] operation
pub struct CreateTableBuilder<const HAS_DATA: bool> {
    parent: Arc<dyn Database>,
    embeddings: Vec<(EmbeddingDefinition, Arc<dyn EmbeddingFunction>)>,
    embedding_registry: Arc<dyn EmbeddingRegistry>,
    request: CreateTableRequest,
    // This is a bit clumsy but we defer errors until `execute` is called
    // to maintain backwards compatibility
    data: CreateTableBuilderInitialData,
}

// Builder methods that only apply when we have initial data
impl CreateTableBuilder<true> {
    fn new<T: IntoArrow>(
        parent: Arc<dyn Database>,
        name: String,
        data: T,
        embedding_registry: Arc<dyn EmbeddingRegistry>,
    ) -> Self {
        let dummy_schema = Arc::new(arrow_schema::Schema::new(Vec::<Field>::default()));
        Self {
            parent,
            request: CreateTableRequest::new(
                name,
                CreateTableData::Empty(TableDefinition::new_from_schema(dummy_schema)),
            ),
            embeddings: Vec::new(),
            embedding_registry,
            data: CreateTableBuilderInitialData::Iterator(data.into_arrow()),
        }
    }

    fn new_streaming<T: IntoArrowStream>(
        parent: Arc<dyn Database>,
        name: String,
        data: T,
        embedding_registry: Arc<dyn EmbeddingRegistry>,
    ) -> Self {
        let dummy_schema = Arc::new(arrow_schema::Schema::new(Vec::<Field>::default()));
        Self {
            parent,
            request: CreateTableRequest::new(
                name,
                CreateTableData::Empty(TableDefinition::new_from_schema(dummy_schema)),
            ),
            embeddings: Vec::new(),
            embedding_registry,
            data: CreateTableBuilderInitialData::Stream(data.into_arrow()),
        }
    }

    /// Execute the create table operation
    pub async fn execute(self) -> Result<Table> {
        let embedding_registry = self.embedding_registry.clone();
        let parent = self.parent.clone();
        let request = self.into_request()?;
        Ok(Table::new_with_embedding_registry(
            parent.create_table(request).await?,
            embedding_registry,
        ))
    }

    fn into_request(self) -> Result<CreateTableRequest> {
        if self.embeddings.is_empty() {
            match self.data {
                CreateTableBuilderInitialData::Iterator(maybe_iter) => {
                    let data = maybe_iter?;
                    Ok(CreateTableRequest {
                        data: CreateTableData::Data(data),
                        ..self.request
                    })
                }
                CreateTableBuilderInitialData::None => {
                    unreachable!("No data provided for CreateTableBuilder<true>")
                }
                CreateTableBuilderInitialData::Stream(maybe_stream) => {
                    let data = maybe_stream?;
                    Ok(CreateTableRequest {
                        data: CreateTableData::StreamingData(data),
                        ..self.request
                    })
                }
            }
        } else {
            let CreateTableBuilderInitialData::Iterator(maybe_iter) = self.data else {
                return Err(Error::NotSupported { message: "Creating a table with embeddings is currently not support when the input is streaming".to_string() });
            };
            let data = maybe_iter?;
            let data = Box::new(WithEmbeddings::new(data, self.embeddings));
            Ok(CreateTableRequest {
                data: CreateTableData::Data(data),
                ..self.request
            })
        }
    }
}

// Builder methods that only apply when we do not have initial data
impl CreateTableBuilder<false> {
    fn new(
        parent: Arc<dyn Database>,
        name: String,
        schema: SchemaRef,
        embedding_registry: Arc<dyn EmbeddingRegistry>,
    ) -> Self {
        let table_definition = TableDefinition::new_from_schema(schema);
        Self {
            parent,
            request: CreateTableRequest::new(name, CreateTableData::Empty(table_definition)),
            data: CreateTableBuilderInitialData::None,
            embeddings: Vec::default(),
            embedding_registry,
        }
    }

    /// Execute the create table operation
    pub async fn execute(self) -> Result<Table> {
        Ok(Table::new(
            self.parent.clone().create_table(self.request).await?,
        ))
    }
}

impl<const HAS_DATA: bool> CreateTableBuilder<HAS_DATA> {
    /// Set the mode for creating the table
    ///
    /// This controls what happens if a table with the given name already exists
    pub fn mode(mut self, mode: CreateTableMode) -> Self {
        self.request.mode = mode;
        self
    }

    /// Apply the given write options when writing the initial data
    pub fn write_options(mut self, write_options: WriteOptions) -> Self {
        self.request.write_options = write_options;
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
            .request
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
            .request
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

    /// Add an embedding definition to the table.
    ///
    /// The `embedding_name` must match the name of an embedding function that
    /// was previously registered with the connection's [`EmbeddingRegistry`].
    pub fn add_embedding(mut self, definition: EmbeddingDefinition) -> Result<Self> {
        // Early verification of the embedding name
        let embedding_func = self
            .embedding_registry
            .get(&definition.embedding_name)
            .ok_or_else(|| Error::EmbeddingFunctionNotFound {
                name: definition.embedding_name.clone(),
                reason: "No embedding function found in the connection's embedding_registry"
                    .to_string(),
            })?;

        self.embeddings.push((definition, embedding_func));
        Ok(self)
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
    #[deprecated(since = "0.15.1", note = "Use `database_options` instead")]
    pub fn enable_v2_manifest_paths(mut self, use_v2_manifest_paths: bool) -> Self {
        let storage_options = self
            .request
            .write_options
            .lance_write_params
            .get_or_insert_with(Default::default)
            .store_params
            .get_or_insert_with(Default::default)
            .storage_options
            .get_or_insert_with(Default::default);

        storage_options.insert(
            OPT_NEW_TABLE_V2_MANIFEST_PATHS.to_string(),
            if use_v2_manifest_paths {
                "true".to_string()
            } else {
                "false".to_string()
            },
        );
        self
    }

    /// Set the data storage version.
    ///
    /// The default is `LanceFileVersion::Stable`.
    #[deprecated(since = "0.15.1", note = "Use `database_options` instead")]
    pub fn data_storage_version(mut self, data_storage_version: LanceFileVersion) -> Self {
        let storage_options = self
            .request
            .write_options
            .lance_write_params
            .get_or_insert_with(Default::default)
            .store_params
            .get_or_insert_with(Default::default)
            .storage_options
            .get_or_insert_with(Default::default);

        storage_options.insert(
            OPT_NEW_TABLE_STORAGE_VERSION.to_string(),
            data_storage_version.to_string(),
        );
        self
    }

    /// Set the namespace for the table
    pub fn namespace(mut self, namespace: Vec<String>) -> Self {
        self.request.namespace = namespace;
        self
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
    /// See available options at <https://lancedb.github.io/lancedb/guides/storage/>
    pub fn storage_option(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        let storage_options = self
            .request
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
            .request
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

    /// Set the namespace for the table
    pub fn namespace(mut self, namespace: Vec<String>) -> Self {
        self.request.namespace = namespace;
        self
    }

    /// Open the table
    pub async fn execute(self) -> Result<Table> {
        Ok(Table::new_with_embedding_registry(
            self.parent.clone().open_table(self.request).await?,
            self.embedding_registry,
        ))
    }
}

/// A connection to LanceDB
#[derive(Clone)]
pub struct Connection {
    uri: String,
    internal: Arc<dyn Database>,
    embedding_registry: Arc<dyn EmbeddingRegistry>,
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
    pub fn create_table<T: IntoArrow>(
        &self,
        name: impl Into<String>,
        initial_data: T,
    ) -> CreateTableBuilder<true> {
        CreateTableBuilder::<true>::new(
            self.internal.clone(),
            name.into(),
            initial_data,
            self.embedding_registry.clone(),
        )
    }

    /// Create a new table from a stream of data
    ///
    /// # Parameters
    ///
    /// * `name` - The name of the table
    /// * `initial_data` - The initial data to write to the table
    pub fn create_table_streaming<T: IntoArrowStream>(
        &self,
        name: impl Into<String>,
        initial_data: T,
    ) -> CreateTableBuilder<true> {
        CreateTableBuilder::<true>::new_streaming(
            self.internal.clone(),
            name.into(),
            initial_data,
            self.embedding_registry.clone(),
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
    ) -> CreateTableBuilder<false> {
        CreateTableBuilder::<false>::new(
            self.internal.clone(),
            name.into(),
            schema,
            self.embedding_registry.clone(),
        )
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
    pub async fn list_namespaces(&self, request: ListNamespacesRequest) -> Result<Vec<String>> {
        self.internal.list_namespaces(request).await
    }

    /// Create a new namespace
    pub async fn create_namespace(&self, request: CreateNamespaceRequest) -> Result<()> {
        self.internal.create_namespace(request).await
    }

    /// Drop a namespace
    pub async fn drop_namespace(&self, request: DropNamespaceRequest) -> Result<()> {
        self.internal.drop_namespace(request).await
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

    /// Database/Catalog specific options
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
    /// See available options at <https://lancedb.github.io/lancedb/guides/storage/>
    pub fn storage_option(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.request.options.insert(key.into(), value.into());
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
    fn execute_remote(self) -> Result<Connection> {
        use crate::remote::db::RemoteDatabaseOptions;

        let options = RemoteDatabaseOptions::parse_from_map(&self.request.options)?;

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
            uri: self.request.uri,
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
                uri: self.request.uri,
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

/// A builder for configuring a connection to a LanceDB catalog
#[derive(Debug)]
pub struct CatalogConnectBuilder {
    request: ConnectRequest,
}

impl CatalogConnectBuilder {
    /// Create a new [`CatalogConnectBuilder`] with the given catalog URI.
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
        }
    }

    pub fn catalog_options(mut self, catalog_options: &dyn CatalogOptions) -> Self {
        catalog_options.serialize_into_map(&mut self.request.options);
        self
    }

    /// Establishes a connection to the catalog
    pub async fn execute(self) -> Result<Arc<ListingCatalog>> {
        let catalog = ListingCatalog::connect(&self.request).await?;
        Ok(Arc::new(catalog))
    }
}

/// Connect to a LanceDB catalog.
///
/// A catalog is a container for databases, which in turn are containers for tables.
///
/// # Arguments
///
/// * `uri` - URI where the catalog is located, can be a local directory or supported remote cloud storage.
pub fn connect_catalog(uri: &str) -> CatalogConnectBuilder {
    CatalogConnectBuilder::new(uri)
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
                uri: "db://test".to_string(),
                embedding_registry: Arc::new(MemoryRegistry::new()),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs::create_dir_all;

    use crate::catalog::{Catalog, DatabaseNamesRequest, OpenDatabaseRequest};
    use crate::database::listing::{ListingDatabaseOptions, NewTableConfig};
    use crate::query::QueryBase;
    use crate::query::{ExecutableQuery, QueryExecutionOptions};
    use arrow::compute::concat_batches;
    use arrow_array::RecordBatchReader;
    use arrow_schema::{DataType, Field, Schema};
    use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
    use futures::{stream, TryStreamExt};
    use lance::error::{ArrowResult, DataFusionResult};
    use lance_testing::datagen::{BatchGenerator, IncrementingInt32};
    use tempfile::tempdir;

    use crate::arrow::SimpleRecordBatchStream;

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
        let db = connect(uri)
            .database_options(&ListingDatabaseOptions {
                new_table_config: NewTableConfig {
                    data_storage_version: Some(LanceFileVersion::Legacy),
                    ..Default::default()
                },
                ..Default::default()
            })
            .execute()
            .await
            .unwrap();

        let tbl = db
            .create_table("v1_test", make_data())
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

        let db = connect(uri)
            .database_options(&ListingDatabaseOptions {
                new_table_config: NewTableConfig {
                    data_storage_version: Some(LanceFileVersion::Stable),
                    ..Default::default()
                },
                ..Default::default()
            })
            .execute()
            .await
            .unwrap();

        let tbl = db
            .create_table("v2_test", make_data())
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
    async fn test_create_table_streaming() {
        let tmp_dir = tempdir().unwrap();

        let uri = tmp_dir.path().to_str().unwrap();
        let db = connect(uri).execute().await.unwrap();

        let batches = make_data().collect::<ArrowResult<Vec<_>>>().unwrap();

        let schema = batches.first().unwrap().schema();
        let one_batch = concat_batches(&schema, batches.iter()).unwrap();

        let ldb_stream = stream::iter(batches.clone().into_iter().map(Result::Ok));
        let ldb_stream: SendableRecordBatchStream =
            Box::pin(SimpleRecordBatchStream::new(ldb_stream, schema.clone()));

        let tbl1 = db
            .create_table_streaming("one", ldb_stream)
            .execute()
            .await
            .unwrap();

        let df_stream = stream::iter(batches.into_iter().map(DataFusionResult::Ok));
        let df_stream: datafusion_physical_plan::SendableRecordBatchStream =
            Box::pin(RecordBatchStreamAdapter::new(schema.clone(), df_stream));

        let tbl2 = db
            .create_table_streaming("two", df_stream)
            .execute()
            .await
            .unwrap();

        let tbl1_data = tbl1
            .query()
            .execute()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        let tbl1_data = concat_batches(&schema, tbl1_data.iter()).unwrap();
        assert_eq!(tbl1_data, one_batch);

        let tbl2_data = tbl2
            .query()
            .execute()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        let tbl2_data = concat_batches(&schema, tbl2_data.iter()).unwrap();
        assert_eq!(tbl2_data, one_batch);
    }

    #[tokio::test]
    async fn drop_table() {
        let tmp_dir = tempdir().unwrap();

        let uri = tmp_dir.path().to_str().unwrap();
        let db = connect(uri).execute().await.unwrap();

        // drop non-exist table
        assert!(matches!(
            db.drop_table("invalid_table", &[]).await,
            Err(crate::Error::TableNotFound { .. }),
        ));

        create_dir_all(tmp_dir.path().join("table1.lance")).unwrap();
        db.drop_table("table1", &[]).await.unwrap();

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
            .mode(CreateTableMode::exist_ok(|mut req| {
                req.index_cache_size = Some(16);
                req
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

    #[tokio::test]
    async fn test_connect_catalog() {
        let tmp_dir = tempdir().unwrap();
        let uri = tmp_dir.path().to_str().unwrap();
        let catalog = connect_catalog(uri).execute().await.unwrap();

        // Verify that we can get the uri from the catalog
        let catalog_uri = catalog.uri();
        assert_eq!(catalog_uri, uri);

        // Check that the catalog is initially empty
        let dbs = catalog
            .database_names(DatabaseNamesRequest::default())
            .await
            .unwrap();
        assert_eq!(dbs.len(), 0);
    }

    #[tokio::test]
    #[cfg(not(windows))]
    async fn test_catalog_create_database() {
        let tmp_dir = tempdir().unwrap();
        let uri = tmp_dir.path().to_str().unwrap();
        let catalog = connect_catalog(uri).execute().await.unwrap();

        let db_name = "test_db";
        catalog
            .create_database(crate::catalog::CreateDatabaseRequest {
                name: db_name.to_string(),
                mode: Default::default(),
                options: Default::default(),
            })
            .await
            .unwrap();

        let dbs = catalog
            .database_names(DatabaseNamesRequest::default())
            .await
            .unwrap();
        assert_eq!(dbs.len(), 1);
        assert_eq!(dbs[0], db_name);

        let db = catalog
            .open_database(OpenDatabaseRequest {
                name: db_name.to_string(),
                database_options: HashMap::new(),
            })
            .await
            .unwrap();

        let tables = db.table_names(Default::default()).await.unwrap();
        assert_eq!(tables.len(), 0);
    }

    #[tokio::test]
    #[cfg(not(windows))]
    async fn test_catalog_drop_database() {
        let tmp_dir = tempdir().unwrap();
        let uri = tmp_dir.path().to_str().unwrap();
        let catalog = connect_catalog(uri).execute().await.unwrap();

        // Create and then drop a database
        let db_name = "test_db_to_drop";
        catalog
            .create_database(crate::catalog::CreateDatabaseRequest {
                name: db_name.to_string(),
                mode: Default::default(),
                options: Default::default(),
            })
            .await
            .unwrap();

        let dbs = catalog
            .database_names(DatabaseNamesRequest::default())
            .await
            .unwrap();
        assert_eq!(dbs.len(), 1);

        catalog.drop_database(db_name).await.unwrap();

        let dbs_after = catalog
            .database_names(DatabaseNamesRequest::default())
            .await
            .unwrap();
        assert_eq!(dbs_after.len(), 0);
    }
}
