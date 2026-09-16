// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Catalogs manage databases. A remote catalog is the root namespace of a server.
//!
//! ```
//! # #[cfg(feature = "remote")]
//! # async fn example() -> lancedb::Result<()> {
//! let catalog = lancedb::connect_catalog("https://my-server.example")
//!     .api_key("my-api-key")
//!     .execute().await?;
//! let database = catalog.create_database("analytics").await?;
//! # Ok(())
//! # }
//! ```

use std::fmt;
use std::sync::Arc;

use crate::Result;
use crate::connection::Connection;
use crate::database::Database;
use crate::embeddings::{EmbeddingRegistry, MemoryRegistry};

/// Options for creating a database. By default an existing name is an error.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct CreateDatabaseRequest {
    /// Logical database name, including any literal slashes.
    pub name: String,
    /// Open the existing database if it is already registered.
    pub exist_ok: bool,
}

impl CreateDatabaseRequest {
    /// Initialize a request with the default behavior.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            exist_ok: false,
        }
    }

    /// Open an existing database instead of failing if its name already exists.
    pub fn exist_ok(mut self, value: bool) -> Self {
        self.exist_ok = value;
        self
    }
}

impl<T: Into<String>> From<T> for CreateDatabaseRequest {
    fn from(name: T) -> Self {
        Self::new(name)
    }
}

/// Options for restricted database deletion. Tables must be removed first.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct DropDatabaseRequest {
    /// Logical database name, including any literal slashes.
    pub name: String,
    /// Succeed if the database is absent.
    pub ignore_missing: bool,
}

impl DropDatabaseRequest {
    /// Initialize a request with the default behavior.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            ignore_missing: false,
        }
    }

    /// Succeed when the database does not exist.
    pub fn ignore_missing(mut self, value: bool) -> Self {
        self.ignore_missing = value;
        self
    }
}

impl<T: Into<String>> From<T> for DropDatabaseRequest {
    fn from(name: T) -> Self {
        Self::new(name)
    }
}

/// Pagination options for listing databases.
#[derive(Clone, Debug, Default)]
#[non_exhaustive]
pub struct ListDatabasesRequest {
    /// Maximum number of names to return. None uses the server default.
    pub limit: Option<u32>,
    /// Opaque continuation token from a previous response. None starts a listing.
    pub page_token: Option<String>,
}

impl ListDatabasesRequest {
    /// Set the maximum page size (1 through 2147483647).
    pub fn limit(mut self, limit: u32) -> Self {
        self.limit = Some(limit);
        self
    }
    /// Resume a listing from an opaque continuation token.
    pub fn page_token(mut self, token: impl Into<String>) -> Self {
        self.page_token = Some(token.into());
        self
    }
}

/// One page of database names, relative to the catalog.
#[derive(Clone, Debug, Default)]
pub struct ListDatabasesResponse {
    /// Logical database names on this page.
    pub databases: Vec<String>,
    /// None indicates the end of the listing.
    pub page_token: Option<String>,
}

/// A backend that manages databases. Implementations own database lifecycle semantics.
#[async_trait::async_trait]
pub trait Catalog: Send + Sync + std::fmt::Debug + 'static {
    /// Catalog endpoint or location.
    fn uri(&self) -> &str;
    /// Create a database, or open it when `exist_ok` permits.
    async fn create_database(&self, request: CreateDatabaseRequest) -> Result<Arc<dyn Database>>;
    /// Drop an empty database. This must not cascade to tables.
    async fn drop_database(&self, request: DropDatabaseRequest) -> Result<()>;
    /// List a page of database names.
    async fn list_databases(&self, request: ListDatabasesRequest) -> Result<ListDatabasesResponse>;
    /// Open an existing database by its logical name.
    async fn open_database(&self, name: &str) -> Result<Arc<dyn Database>>;
}

/// A catalog connection that returns ordinary LanceDB database connections.
#[derive(Clone)]
pub struct CatalogConnection {
    catalog: Arc<dyn Catalog>,
    embedding_registry: Arc<dyn EmbeddingRegistry>,
}

impl fmt::Debug for CatalogConnection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CatalogConnection")
            .field("uri", &self.uri())
            .finish_non_exhaustive()
    }
}

impl CatalogConnection {
    /// Wrap a catalog implementation using the default in-memory embedding registry.
    pub fn new(catalog: Arc<dyn Catalog>) -> Self {
        Self {
            catalog,
            embedding_registry: Arc::new(MemoryRegistry::new()),
        }
    }

    /// Provide the registry used by databases opened through this connection.
    pub fn with_embedding_registry(mut self, registry: Arc<dyn EmbeddingRegistry>) -> Self {
        self.embedding_registry = registry;
        self
    }

    /// The catalog endpoint or location.
    pub fn uri(&self) -> &str {
        self.catalog.uri()
    }
    /// Access the underlying backend.
    pub fn catalog(&self) -> &Arc<dyn Catalog> {
        &self.catalog
    }

    /// Create a database; pass a name or [`CreateDatabaseRequest`] for additional options.
    pub async fn create_database(
        &self,
        request: impl Into<CreateDatabaseRequest>,
    ) -> Result<Connection> {
        Ok(Connection::new(
            self.catalog.create_database(request.into()).await?,
            self.embedding_registry.clone(),
        ))
    }

    /// Drop an empty database; pass a name or [`DropDatabaseRequest`] for additional options.
    pub async fn drop_database(&self, request: impl Into<DropDatabaseRequest>) -> Result<()> {
        self.catalog.drop_database(request.into()).await
    }

    /// List a page of databases. Pass [`ListDatabasesRequest::default`] for the first page.
    pub async fn list_databases(
        &self,
        request: ListDatabasesRequest,
    ) -> Result<ListDatabasesResponse> {
        self.catalog.list_databases(request).await
    }

    /// Open a database without creating it if it is missing.
    pub async fn open_database(&self, name: impl AsRef<str>) -> Result<Connection> {
        Ok(Connection::new(
            self.catalog.open_database(name.as_ref()).await?,
            self.embedding_registry.clone(),
        ))
    }
}

/// Configure a connection to a remote catalog.
#[cfg(feature = "remote")]
#[derive(Debug)]
pub struct ConnectCatalogBuilder {
    endpoint: String,
    options: crate::remote::RemoteCatalogOptions,
}

#[cfg(feature = "remote")]
impl ConnectCatalogBuilder {
    /// Start configuring a remote HTTP(S) catalog connection.
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            options: Default::default(),
        }
    }
    /// Authenticate with an API key.
    pub fn api_key(mut self, key: impl Into<String>) -> Self {
        self.options.api_key = Some(key.into());
        self
    }
    /// Configure headers, TLS, timeouts, and other shared client settings.
    pub fn client_config(mut self, config: crate::remote::ClientConfig) -> Self {
        self.options.client_config = config;
        self
    }
    /// Configure table read consistency for opened databases.
    pub fn read_consistency_interval(mut self, interval: std::time::Duration) -> Self {
        self.options.read_consistency_interval = Some(interval);
        self
    }
    /// Authenticate using OAuth; mutually exclusive with API keys and header providers.
    pub fn oauth_config(mut self, config: crate::remote::OAuthConfig) -> Self {
        self.options.oauth_config = Some(config);
        self
    }
    /// Connect to the server's root namespace. Database-scoped headers are omitted.
    pub async fn execute(self) -> Result<CatalogConnection> {
        Ok(CatalogConnection::new(Arc::new(
            crate::remote::RemoteCatalog::try_new(&self.endpoint, self.options)?,
        )))
    }
}
