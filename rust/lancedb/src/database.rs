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

use arrow_array::RecordBatch;

use lance::dataset::ReadParams;
use lance_namespace::LanceNamespace;
use lance_namespace::models::{
    CreateNamespaceRequest, CreateNamespaceResponse, DescribeNamespaceRequest,
    DescribeNamespaceResponse, DropNamespaceRequest, DropNamespaceResponse, ListNamespacesRequest,
    ListNamespacesResponse, ListTablesRequest, ListTablesResponse,
};

use crate::data::scannable::Scannable;
use crate::error::Result;
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

/// A row from [`Database::list_jobs`]: one server-side job (index build,
/// compaction, column refresh, ...).
#[derive(Debug, Clone)]
pub struct JobInfo {
    /// The job id -- what [`Database::get_job`] and [`Database::cancel_job`]
    /// accept.
    pub job_id: String,
    /// The table the job runs against, without URI or namespace.
    pub table: String,
    pub job_type: String,
    /// Lifecycle state: "running", "finished", "failed", or "cancelled".
    pub state: String,
    /// When the job was created, in milliseconds since the epoch.
    pub created_at_millis: i64,
}

/// A described job from [`Database::get_job`]: lifecycle state plus the
/// job-type-specific specification.
#[derive(Debug, Clone)]
pub struct JobDescription {
    pub job_id: String,
    pub job_type: String,
    /// Lifecycle state: "running", "finished", "failed", or "cancelled".
    pub state: String,
    /// When the job was created, in milliseconds since the epoch.
    pub creation_ms: i64,
    /// The job-type-specific specification. Null when the server omits it.
    pub spec: serde_json::Value,
    /// Why the job failed, when the job is failed and the server reports a
    /// reason.
    pub failure: Option<crate::error::JobFailure>,
}

fn job_op_not_supported<T>(what: &str) -> Result<T> {
    Err(crate::error::Error::NotSupported {
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
    /// A [`crate::job::Job`] handle for a server-side job by id, suitable for
    /// waiting on or cancelling the job. The handle is constructed without a
    /// server round trip; an unknown id surfaces when the handle is used.
    fn job(&self, _job_id: &str) -> Result<crate::job::Job> {
        job_op_not_supported("job")
    }
    /// List server-side jobs across the database's tables.
    async fn list_jobs(&self) -> Result<Vec<JobInfo>> {
        job_op_not_supported("list_jobs")
    }
    /// Describe a single job by id. `None` when the server has no such job.
    async fn get_job(&self, _job_id: &str) -> Result<Option<JobDescription>> {
        job_op_not_supported("get_job")
    }
    /// Request cancellation of a job by id. Returns true if the server
    /// accepted the cancellation, false if no such job exists. Cancelling an
    /// already-terminal job is a no-op success.
    async fn cancel_job(&self, _job_id: &str) -> Result<bool> {
        job_op_not_supported("cancel_job")
    }
    /// The lifecycle event history of a job (all jobs when `job_id` is
    /// `None`), as recorded Arrow batches.
    async fn job_history(&self, _job_id: Option<&str>) -> Result<Vec<RecordBatch>> {
        job_op_not_supported("job_history")
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
    /// Start dropping a table and return a handle to the cleanup job.
    ///
    /// Backends without asynchronous cleanup complete the drop before
    /// returning an already-finished job.
    async fn drop_table_async(
        &self,
        name: &str,
        namespace_path: &[String],
    ) -> Result<crate::job::Job> {
        self.drop_table(name, namespace_path).await?;
        Ok(crate::job::Job::new_done())
    }
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
