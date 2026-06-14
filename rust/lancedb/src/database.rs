// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! The database module defines the `Database` trait and related types.
//!
//! A "database" is a generic concept for something that manages tables and their metadata.
//!
//! We provide a basic implementation of a database that requires no additional infrastructure
//! and is based off listing directories in a filesystem.
//!
//! Users may want to provider their own implementations for a variety of reasons:
//!  * Tables may be arranged in a different order on the S3 filesystem
//!  * Tables may be managed by some kind of independent application (e.g. some database)
//!  * Tables may be managed by a database system (e.g. Postgres)
//!  * A custom table implementation (e.g. remote table, etc.) may be used

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use lance::dataset::ReadParams;
use lance_namespace::LanceNamespace;
use lance_namespace::models::{
    CreateNamespaceRequest, CreateNamespaceResponse, DescribeNamespaceRequest,
    DescribeNamespaceResponse, DropNamespaceRequest, DropNamespaceResponse, ListNamespacesRequest,
    ListNamespacesResponse, ListTablesRequest, ListTablesResponse,
};

use crate::data::scannable::Scannable;
use crate::error::{Error, Result};
use crate::table::{BaseTable, WriteOptions};

pub mod listing;
pub mod namespace;
pub(crate) mod read_freshness;

pub trait DatabaseOptions {
    fn serialize_into_map(&self, map: &mut HashMap<String, String>);
}

/// A request to list names of tables in the database (deprecated, use ListTablesRequest)
#[derive(Clone, Debug, Default)]
pub struct TableNamesRequest {
    /// The namespace path to list tables in. Empty list represents root namespace.
    pub namespace_path: Vec<String>,
    /// If present, only return names that come lexicographically after the supplied
    /// value.
    ///
    /// This can be combined with limit to implement pagination by setting this to
    /// the last table name from the previous page.
    pub start_after: Option<String>,
    /// The maximum number of table names to return
    pub limit: Option<u32>,
}

/// A request to open a table
#[derive(Clone)]
pub struct OpenTableRequest {
    pub name: String,
    /// The namespace path to open the table from. Empty list represents root namespace.
    pub namespace_path: Vec<String>,
    pub index_cache_size: Option<u32>,
    pub lance_read_params: Option<ReadParams>,
    /// Optional custom location for the table. If not provided, the database will
    /// derive a location based on its URI and the table name.
    pub location: Option<String>,
    /// Optional namespace client for server-side query execution.
    /// When set, queries will be executed on the namespace server instead of locally.
    pub namespace_client: Option<Arc<dyn LanceNamespace>>,
    /// Whether managed versioning is enabled for this table.
    /// When Some(true), the table will use namespace-managed commits instead of local commits.
    /// When None and namespace_client is provided, the value will be fetched from the namespace.
    pub managed_versioning: Option<bool>,
}

impl std::fmt::Debug for OpenTableRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OpenTableRequest")
            .field("name", &self.name)
            .field("namespace_path", &self.namespace_path)
            .field("index_cache_size", &self.index_cache_size)
            .field("lance_read_params", &self.lance_read_params)
            .field("location", &self.location)
            .field("namespace_client", &self.namespace_client)
            .field("managed_versioning", &self.managed_versioning)
            .finish()
    }
}

pub type TableBuilderCallback = Box<dyn FnOnce(OpenTableRequest) -> OpenTableRequest + Send>;

/// Describes what happens when creating a table and a table with
/// the same name already exists
#[derive(Default)]
pub enum CreateTableMode {
    /// If the table already exists, an error is returned
    #[default]
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
        callback: impl FnOnce(OpenTableRequest) -> OpenTableRequest + Send + 'static,
    ) -> Self {
        Self::ExistOk(Box::new(callback))
    }
}

/// A request to create a table
pub struct CreateTableRequest {
    /// The name of the new table
    pub name: String,
    /// The namespace path to create the table in. Empty list represents root namespace.
    pub namespace_path: Vec<String>,
    /// Initial data to write to the table, can be empty.
    pub data: Box<dyn Scannable>,
    /// The mode to use when creating the table
    pub mode: CreateTableMode,
    /// Options to use when writing data (only used if `data` is not None)
    pub write_options: WriteOptions,
    /// Optional custom location for the table. If not provided, the database will
    /// derive a location based on its URI and the table name.
    pub location: Option<String>,
    /// Optional namespace client for server-side query execution.
    /// When set, queries will be executed on the namespace server instead of locally.
    pub namespace_client: Option<Arc<dyn LanceNamespace>>,
}

impl CreateTableRequest {
    pub fn new(name: String, data: Box<dyn Scannable>) -> Self {
        Self {
            name,
            namespace_path: vec![],
            data,
            mode: CreateTableMode::default(),
            write_options: WriteOptions::default(),
            location: None,
            namespace_client: None,
        }
    }
}

/// Request to clone a table from a source table.
///
/// A shallow clone creates a new table that shares the underlying data files
/// with the source table but has its own independent manifest. This allows
/// both the source and cloned tables to evolve independently while initially
/// sharing the same data, deletion, and index files.
#[derive(Clone, Debug)]
pub struct CloneTableRequest {
    /// The name of the target table to create
    pub target_table_name: String,
    /// The namespace path for the target table. Empty list represents root namespace.
    pub target_namespace_path: Vec<String>,
    /// The URI of the source table to clone from.
    pub source_uri: String,
    /// Optional version of the source table to clone.
    pub source_version: Option<u64>,
    /// Optional tag of the source table to clone.
    pub source_tag: Option<String>,
    /// Whether to perform a shallow clone (true) or deep clone (false). Defaults to true.
    /// Currently only shallow clone is supported.
    pub is_shallow: bool,
    /// Optional namespace client for managed versioning support.
    /// When set, enables the commit handler to track table versions through the namespace.
    pub namespace_client: Option<Arc<dyn LanceNamespace>>,
}

impl CloneTableRequest {
    pub fn new(target_table_name: String, source_uri: String) -> Self {
        Self {
            target_table_name,
            target_namespace_path: vec![],
            source_uri,
            source_version: None,
            source_tag: None,
            is_shallow: true,
            namespace_client: None,
        }
    }
}

/// How long until a change is reflected from one Table instance to another
///
/// Tables are always internally consistent.  If a write method is called on
/// a table instance it will be immediately visible in that same table instance.
pub enum ReadConsistency {
    /// Changes will not be automatically propagated until the checkout_latest
    /// method is called on the target table
    Manual,
    /// Changes will be propagated automatically within the given duration
    Eventual(Duration),
    /// Changes are immediately visible in target tables
    Strong,
}

/// A request to register a UDF (CREATE FUNCTION).
///
/// Functions are first-class database objects, decoupled from any
/// column; computed columns and materialized views reference them by
/// name. Server-backed feature (LanceDB Enterprise / Cloud).
#[derive(Debug, Clone)]
pub struct CreateFunctionRequest {
    /// Function name.
    pub name: String,
    /// Implementation language (currently "python").
    pub language: String,
    /// SQL return type, e.g. `FLOAT`, `FLOAT[1536]`,
    /// `STRUCT(a FLOAT, b VARCHAR)`, `TABLE(chunk VARCHAR, idx INT)`.
    pub return_type: String,
    /// Function body: source text, or base64 cloudpickle bytes when
    /// `options["body_format"] = "cloudpickle"`.
    pub body: String,
    /// Options: input_columns, pip, num_gpus, batch_size, timeout,
    /// error_policy, docker_image, body_format, ...
    pub options: HashMap<String, String>,
}

/// A registered function, as returned by `list_functions`.
#[derive(Debug, Clone)]
pub struct FunctionInfo {
    pub name: String,
    pub language: String,
    pub return_type: String,
    pub description: String,
}

/// A request to create a materialized view (CREATE MATERIALIZED VIEW).
#[derive(Debug, Clone)]
pub struct CreateMaterializedViewRequest {
    /// View name.
    pub name: String,
    /// The view's SELECT statement, e.g.
    /// `SELECT id, embed(body) AS vec FROM articles WHERE id > 1`.
    /// Bare columns project through; function-call columns compute via
    /// registered UDFs (a RETURNS TABLE function makes a row-expanding
    /// chunker view).
    pub query: String,
    /// Refresh automatically when the source table changes.
    pub auto_refresh: bool,
    /// Register the definition only; skip the initial population.
    pub with_no_data: bool,
    /// Optional source column to partition the view's table function on. If the
    /// column has an IVF vector index the server partitions by its clusters
    /// (image-dedup style); otherwise it groups by distinct value.
    pub partition_by: Option<String>,
}

impl CreateMaterializedViewRequest {
    pub fn new(name: impl Into<String>, query: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            query: query.into(),
            auto_refresh: false,
            with_no_data: false,
            partition_by: None,
        }
    }
}

/// A request to refresh a materialized view.
#[derive(Debug, Clone)]
pub struct RefreshMaterializedViewRequest {
    /// View name.
    pub name: String,
    /// Pin the refresh to a source-table version; latest when absent.
    pub src_version: Option<u64>,
    /// Initial worker count.
    pub num_workers: Option<u32>,
    /// Elastic worker ceiling.
    pub max_workers: Option<u32>,
}

impl RefreshMaterializedViewRequest {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            src_version: None,
            num_workers: None,
            max_workers: None,
        }
    }
}

/// A registered materialized view definition, as returned by
/// `list_materialized_views`.
#[derive(Debug, Clone)]
pub struct MaterializedViewInfo {
    pub name: String,
    pub source_table: String,
    /// Source columns projected through.
    pub projection: Vec<String>,
    /// `alias=expression` per UDF-computed column.
    pub udf_columns: Vec<String>,
    pub filter: Option<String>,
    pub auto_refresh: bool,
}

/// A row from `list_jobs`: one inflight server-side job (index build,
/// compaction, column refresh, view refresh, ...).
#[derive(Debug, Clone)]
pub struct JobInfo {
    pub table: String,
    pub job_id: String,
    pub job_type: String,
    /// Lifecycle state: "running", "cancelling", or "stale".
    pub state: String,
    pub column: Option<String>,
    pub age_seconds: Option<i64>,
    pub command: Option<String>,
    pub units_done: Option<i64>,
    pub units_total: Option<i64>,
    /// Whether the job's final commit has completed (output visible).
    pub committed: bool,
    pub rows_skipped: u64,
    pub error: Option<String>,
}

/// The plan a `REFRESH MATERIALIZED VIEW` would execute, as returned by
/// `explain_refresh_materialized_view` (EXPLAIN REFRESH). No work is run.
#[derive(Debug, Clone)]
pub struct MvRefreshPlan {
    pub table_name: String,
    /// Whether a refresh would do anything (rebuild or non-empty units).
    pub has_work: bool,
    pub source_version: u64,
    pub last_refreshed_version: Option<u64>,
    pub full_refresh: bool,
    /// Source changed non-append-only since the last refresh -> rebuild.
    pub rebuild: bool,
    /// Number of row-range work units the refresh would process.
    pub units_total: u64,
}

fn not_supported<T>(what: &str) -> Result<T> {
    Err(Error::NotSupported {
        message: format!("{} is not supported by this database", what),
    })
}

/// The `Database` trait defines the interface for database implementations.
///
/// A database is responsible for managing tables and their metadata.
#[async_trait::async_trait]
pub trait Database:
    Send + Sync + std::any::Any + std::fmt::Debug + std::fmt::Display + 'static
{
    /// Get the uri of the database
    fn uri(&self) -> &str;
    /// Get the read consistency of the database
    async fn read_consistency(&self) -> Result<ReadConsistency>;
    /// List immediate child namespace names in the given namespace
    async fn list_namespaces(
        &self,
        request: ListNamespacesRequest,
    ) -> Result<ListNamespacesResponse>;
    /// Create a new namespace
    async fn create_namespace(
        &self,
        request: CreateNamespaceRequest,
    ) -> Result<CreateNamespaceResponse>;
    /// Drop a namespace
    async fn drop_namespace(&self, request: DropNamespaceRequest) -> Result<DropNamespaceResponse>;
    /// Describe a namespace (get its properties)
    async fn describe_namespace(
        &self,
        request: DescribeNamespaceRequest,
    ) -> Result<DescribeNamespaceResponse>;
    /// List the names of tables in the database
    ///
    /// # Deprecated
    /// Use `list_tables` instead for pagination support
    #[deprecated(note = "Use list_tables instead")]
    async fn table_names(&self, request: TableNamesRequest) -> Result<Vec<String>>;
    /// List tables in the database with pagination support
    async fn list_tables(&self, request: ListTablesRequest) -> Result<ListTablesResponse>;
    /// Create a table in the database
    async fn create_table(&self, request: CreateTableRequest) -> Result<Arc<dyn BaseTable>>;
    /// Clone a table in the database.
    ///
    /// Creates a shallow clone of the source table, sharing underlying data files
    /// but with an independent manifest. Both tables can evolve separately after cloning.
    ///
    /// See [`CloneTableRequest`] for detailed documentation and examples.
    async fn clone_table(&self, request: CloneTableRequest) -> Result<Arc<dyn BaseTable>>;

    // -- Derived compute: functions, materialized views, jobs -------------
    //
    // Server-backed features (LanceDB Enterprise / Cloud). The defaults
    // return NotSupported; the remote database overrides them. Local
    // single-node implementations are planned.

    /// Register a UDF (CREATE FUNCTION).
    async fn create_function(&self, _request: CreateFunctionRequest) -> Result<()> {
        not_supported("create_function")
    }
    /// List registered functions (SHOW FUNCTIONS).
    async fn list_functions(&self) -> Result<Vec<FunctionInfo>> {
        not_supported("list_functions")
    }
    /// Drop a registered function (DROP FUNCTION).
    async fn drop_function(&self, _name: &str) -> Result<()> {
        not_supported("drop_function")
    }
    /// Create a materialized view (CREATE MATERIALIZED VIEW). Returns
    /// the initial-population job id, absent when `with_no_data`.
    async fn create_materialized_view(
        &self,
        _request: CreateMaterializedViewRequest,
    ) -> Result<Option<String>> {
        not_supported("create_materialized_view")
    }
    /// Refresh a materialized view; returns the refresh job id.
    async fn refresh_materialized_view(
        &self,
        _request: RefreshMaterializedViewRequest,
    ) -> Result<String> {
        not_supported("refresh_materialized_view")
    }
    /// Plan a materialized-view refresh without submitting work
    /// (EXPLAIN REFRESH). `full` plans a full rebuild (incremental
    /// planning requires stable row IDs on the source).
    async fn explain_refresh_materialized_view(
        &self,
        _name: &str,
        _full: bool,
        _src_version: Option<u64>,
    ) -> Result<MvRefreshPlan> {
        not_supported("explain_refresh_materialized_view")
    }
    /// Update a materialized view's options (ALTER MATERIALIZED VIEW).
    async fn alter_materialized_view(&self, _name: &str, _auto_refresh: bool) -> Result<()> {
        not_supported("alter_materialized_view")
    }
    /// Drop a materialized view definition (DROP MATERIALIZED VIEW).
    async fn drop_materialized_view(&self, _name: &str) -> Result<()> {
        not_supported("drop_materialized_view")
    }
    /// List registered materialized view definitions.
    async fn list_materialized_views(&self) -> Result<Vec<MaterializedViewInfo>> {
        not_supported("list_materialized_views")
    }
    /// List inflight server-side jobs across the database's tables.
    async fn list_jobs(&self) -> Result<Vec<JobInfo>> {
        not_supported("list_jobs")
    }
    /// Cancel an inflight server-side job by id. Returns true if a
    /// matching inflight job was found and flagged for cancellation,
    /// false if none was inflight (best-effort, like SQL `CANCEL JOB`).
    async fn cancel_job(&self, _job_id: &str) -> Result<bool> {
        not_supported("cancel_job")
    }

    /// Open a table in the database
    async fn open_table(&self, request: OpenTableRequest) -> Result<Arc<dyn BaseTable>>;
    /// Rename a table in the database
    async fn rename_table(
        &self,
        cur_name: &str,
        new_name: &str,
        cur_namespace_path: &[String],
        new_namespace_path: &[String],
    ) -> Result<()>;
    /// Drop a table in the database
    async fn drop_table(&self, name: &str, namespace_path: &[String]) -> Result<()>;
    /// Drop all tables in the database
    async fn drop_all_tables(&self, namespace_path: &[String]) -> Result<()>;
    fn as_any(&self) -> &dyn std::any::Any;

    /// Get the equivalent namespace client of this database
    /// For LanceNamespaceDatabase, it is the underlying LanceNamespace.
    /// For ListingDatabase, it is the equivalent DirectoryNamespace.
    /// For RemoteDatabase, it is the equivalent RestNamespace.
    async fn namespace_client(&self) -> Result<Arc<dyn LanceNamespace>>;

    /// Get the configuration for constructing an equivalent namespace client.
    /// Returns (impl_type, properties) where:
    /// - impl_type: "dir" for DirectoryNamespace, "rest" for RestNamespace
    /// - properties: configuration properties for the namespace
    ///
    /// This is useful for Python bindings where we want to return a Python
    /// namespace object rather than a Rust trait object.
    async fn namespace_client_config(&self) -> Result<(String, HashMap<String, String>)>;
}
