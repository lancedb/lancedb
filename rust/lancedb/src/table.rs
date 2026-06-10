// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! LanceDB Table APIs

use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion_execution::TaskContext;
use datafusion_expr::Expr;
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::display::DisplayableExecutionPlan;
use futures::StreamExt;
use futures::stream::FuturesUnordered;
pub use lance::dataset::ColumnAlteration;
pub use lance::dataset::NewColumnTransform;
pub use lance::dataset::ReadParams;
pub use lance::dataset::Version;
use lance::dataset::WriteMode;
use lance::dataset::builder::DatasetBuilder;
use lance::dataset::{InsertBuilder, WriteParams};
use lance::index::DatasetIndexExt;
use lance::index::vector::VectorIndexParams;
use lance::index::vector::utils::infer_vector_dim;
use lance::io::{ObjectStoreParams, WrappingObjectStore};
use lance_datafusion::utils::StreamingWriteSource;
use lance_index::IndexType;
use lance_index::scalar::{BuiltinIndexType, ScalarIndexParams};
use lance_index::vector::bq::RQBuildParams;
use lance_index::vector::hnsw::builder::HnswBuildParams;
use lance_index::vector::ivf::IvfBuildParams;
use lance_index::vector::pq::PQBuildParams;
use lance_index::vector::sq::builder::SQBuildParams;
use lance_io::object_store::{LanceNamespaceStorageOptionsProvider, StorageOptionsAccessor};
pub use query::AnyQuery;

use lance::io::commit::namespace_manifest::LanceNamespaceExternalManifestStore;
use lance_namespace::LanceNamespace;
use lance_namespace::error::NamespaceError;
use lance_namespace::models::DescribeTableRequest;
use lance_table::format::Manifest;
use lance_table::io::commit::CommitHandler;
use lance_table::io::commit::ManifestNamingScheme;
use lance_table::io::commit::external_manifest::ExternalManifestCommitHandler;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::format;
use std::path::Path;
use std::sync::Arc;

use crate::connection::NamespaceClientPushdownOperation;

use crate::data::scannable::{PeekedScannable, Scannable, estimate_write_partitions};
use crate::database::Database;
use crate::embeddings::{EmbeddingDefinition, EmbeddingRegistry, MemoryRegistry};
use crate::error::{Error, Result};
use crate::index::IndexStatistics;
use crate::index::vector::VectorIndex;
use crate::index::{Index, IndexBuilder, vector::suggested_num_sub_vectors};
use crate::index::{IndexConfig, IndexStatisticsImpl};
use crate::query::{IntoQueryVector, Query, QueryExecutionOptions, TakeQuery, VectorQuery};
use crate::table::datafusion::insert::InsertExec;
use crate::utils::{
    PatchReadParam, PatchWriteParam, supported_bitmap_data_type, supported_btree_data_type,
    supported_fm_data_type, supported_fts_data_type, supported_label_list_data_type,
    supported_vector_data_type,
};

use self::dataset::DatasetConsistencyWrapper;
use self::merge::MergeInsertBuilder;

mod add_data;
pub mod datafusion;
pub(crate) mod dataset;
pub mod delete;
pub mod merge;
pub mod optimize;
mod primary_key;
pub mod query;
pub mod schema_evolution;
pub mod update;
pub mod write_progress;
use crate::index::waiter::wait_for_index;
#[cfg(feature = "remote")]
pub(crate) use add_data::PreprocessingOutput;
pub use add_data::{AddDataBuilder, AddDataMode, AddResult, NaNVectorBehavior};
pub use chrono::Duration;
pub use delete::DeleteResult;
use futures::future::join_all;
pub use lance::dataset::refs::{BranchContents, Ref, TagContents, Tags as LanceTags};
pub use lance::dataset::scanner::DatasetRecordBatchStream;
use lance::dataset::statistics::DatasetStatisticsExt;
pub use lance_index::optimize::OptimizeOptions;
pub use optimize::{CompactionOptions, OptimizeAction, OptimizeStats};
pub use schema_evolution::{
    AddColumnsResult, AlterColumnsResult, DropColumnsResult, FieldMetadataUpdate,
    UpdateFieldMetadataResult,
};
use serde_with::skip_serializing_none;
pub use update::{UpdateBuilder, UpdateResult};

/// Walk a boxed error chain to find the innermost `NamespaceError`.
///
/// Callers like `DatasetBuilder::from_namespace` re-wrap their inner namespace error
/// inside a fresh `lance::Error::Namespace`, so a single downcast at the top level
/// won't find it. This walks `.source()` to unwrap arbitrarily nested layers.
fn find_namespace_error<'a>(
    err: &'a (dyn std::error::Error + 'static),
) -> Option<&'a NamespaceError> {
    let mut current: Option<&(dyn std::error::Error + 'static)> = Some(err);
    while let Some(e) = current {
        if let Some(ns_err) = e.downcast_ref::<NamespaceError>() {
            return Some(ns_err);
        }
        current = e.source();
    }
    None
}

/// Map a `lance::Error` coming from a `lance-namespace` call into a `lancedb::Error`,
/// preserving the fine-grained namespace error code (e.g. `TableNotFound`,
/// `TableAlreadyExists`). Errors that aren't recognized namespace error variants fall
/// through to a generic runtime error rather than `TableNotFound`/`TableAlreadyExists`.
pub(crate) fn map_namespace_lance_error(err: lance::Error, table_name: &str) -> Error {
    if let Some(code) = find_namespace_error(&err).map(NamespaceError::code) {
        match code {
            lance_namespace::error::ErrorCode::TableNotFound => {
                return Error::TableNotFound {
                    name: table_name.to_string(),
                    source: Box::new(err),
                };
            }
            lance_namespace::error::ErrorCode::TableAlreadyExists => {
                return Error::TableAlreadyExists {
                    name: table_name.to_string(),
                };
            }
            _ => {}
        }
    }
    match err {
        lance::Error::Namespace { source, .. } => Error::Runtime {
            message: format!("Namespace error: {}", source),
        },
        other => other.into(),
    }
}

/// Defines the type of column
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ColumnKind {
    /// Columns populated by data from the user (this is the most common case)
    Physical,
    /// Columns populated by applying an embedding function to the input
    Embedding(EmbeddingDefinition),
}

/// Defines a column in a table
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDefinition {
    /// The source of the column data
    pub kind: ColumnKind,
}

#[derive(Debug, Clone)]
pub struct TableDefinition {
    pub column_definitions: Vec<ColumnDefinition>,
    pub schema: SchemaRef,
}

impl TableDefinition {
    pub fn new(schema: SchemaRef, column_definitions: Vec<ColumnDefinition>) -> Self {
        Self {
            column_definitions,
            schema,
        }
    }

    pub fn new_from_schema(schema: SchemaRef) -> Self {
        let column_definitions = schema
            .fields()
            .iter()
            .map(|_| ColumnDefinition {
                kind: ColumnKind::Physical,
            })
            .collect();
        Self::new(schema, column_definitions)
    }

    pub fn try_from_rich_schema(schema: SchemaRef) -> Result<Self> {
        let column_definitions = schema.metadata.get("lancedb::column_definitions");
        if let Some(column_definitions) = column_definitions {
            let column_definitions: Vec<ColumnDefinition> =
                serde_json::from_str(column_definitions).map_err(|e| Error::Runtime {
                    message: format!("Failed to deserialize column definitions: {}", e),
                })?;
            Ok(Self::new(schema, column_definitions))
        } else {
            let column_definitions = schema
                .fields()
                .iter()
                .map(|_| ColumnDefinition {
                    kind: ColumnKind::Physical,
                })
                .collect();
            Ok(Self::new(schema, column_definitions))
        }
    }

    pub fn into_rich_schema(self) -> SchemaRef {
        // We have full control over the structure of column definitions.  This should
        // not fail, except for a bug
        let lancedb_metadata = serde_json::to_string(&self.column_definitions).unwrap();
        let mut schema_with_metadata = (*self.schema).clone();
        schema_with_metadata
            .metadata
            .insert("lancedb::column_definitions".to_string(), lancedb_metadata);
        Arc::new(schema_with_metadata)
    }
}

/// Describes what happens when a vector either contains NaN or
/// does not have enough values
#[derive(Clone, Debug, Default)]
#[allow(dead_code)] // https://github.com/lancedb/lancedb/issues/992
enum BadVectorHandling {
    /// An error is returned
    #[default]
    Error,
    /// The offending row is droppped
    Drop,
    /// The invalid/missing items are replaced by fill_value
    Fill(f32),
    /// The invalid items are replaced by NULL
    None,
}

/// Options to use when writing data
#[derive(Clone, Debug, Default)]
pub struct WriteOptions {
    // Coming soon: https://github.com/lancedb/lancedb/issues/992
    // /// What behavior to take if the data contains invalid vectors
    // pub on_bad_vectors: BadVectorHandling,
    /// Advanced parameters that can be used to customize table creation
    ///
    /// Overlapping `OpenTableBuilder` options (e.g. [AddDataBuilder::mode]) will take
    /// precedence over their counterparts in `WriteOptions` (e.g. [WriteParams::mode]).
    pub lance_write_params: Option<WriteParams>,
}

/// Filters that can be used to limit the rows returned by a query
pub enum Filter {
    /// A SQL filter string
    Sql(String),
    /// A Datafusion logical expression
    Datafusion(Expr),
}

/// A predicate for filtering rows in delete operations.
///
/// Accepts either a SQL string or a DataFusion [`Expr`]. Use the [`From`]
/// implementations to convert from `&str` or `&Expr` automatically.
/// See [`Table::delete`] for usage examples.
pub enum Predicate<'a> {
    /// A SQL predicate string
    String(&'a str),
    /// A DataFusion logical expression
    Expr(&'a Expr),
}

impl<'a> From<&'a str> for Predicate<'a> {
    fn from(s: &'a str) -> Self {
        Predicate::String(s)
    }
}

impl<'a> From<&'a String> for Predicate<'a> {
    fn from(s: &'a String) -> Self {
        Predicate::String(s.as_str())
    }
}

impl<'a> From<&'a Expr> for Predicate<'a> {
    fn from(e: &'a Expr) -> Self {
        Predicate::Expr(e)
    }
}

#[async_trait]
pub trait Tags: Send + Sync {
    /// List the tags of the table.
    async fn list(&self) -> Result<HashMap<String, TagContents>>;

    /// Get the version of the table referenced by a tag.
    async fn get_version(&self, tag: &str) -> Result<u64>;

    /// Create a new tag for the given version of the table.
    async fn create(&mut self, tag: &str, version: u64) -> Result<()>;

    /// Delete a tag from the table.
    async fn delete(&mut self, tag: &str) -> Result<()>;

    /// Update an existing tag to point to a new version of the table.
    async fn update(&mut self, tag: &str, version: u64) -> Result<()>;
}

pub use self::merge::MergeResult;

/// Specification selecting Lance's MemWAL LSM-style write path for
/// `merge_insert`.
///
/// Construct via [`LsmWriteSpec::bucket`], [`LsmWriteSpec::identity`], or
/// [`LsmWriteSpec::unsharded`], then optionally chain
/// [`LsmWriteSpec::with_maintained_indexes`] (indexes the MemWAL keeps up to
/// date) and [`LsmWriteSpec::with_writer_config_defaults`] (default
/// `ShardWriter` configuration recorded in the MemWAL index).
///
/// Install a spec with [`Table::set_lsm_write_spec`] and remove it with
/// [`Table::unset_lsm_write_spec`]. The actual `merge_insert` dispatch
/// onto the MemWAL writer is a follow-up.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum LsmWriteSpec {
    /// Hash-bucket sharding by a scalar column.
    ///
    /// `column` must be a non-nested column with a supported scalar type.
    /// `num_buckets` must be in `[1, 1024]`.
    /// Iceberg-compatible Murmur3-x86-32 (seed 0) is used so each row's
    /// `bucket(column, num_buckets)` value is stable across processes.
    Bucket {
        column: String,
        num_buckets: u32,
        /// Names of indexes (already created on the table) that the
        /// MemWAL should maintain in-memory as rows are appended.
        maintained_indexes: Vec<String>,
        /// Default `ShardWriter` configuration recorded in the MemWAL index.
        writer_config_defaults: HashMap<String, String>,
    },
    /// Identity sharding — shard by the raw value of `column`.
    ///
    /// Use this when the data is already partitioned by `column`; each
    /// distinct value of `column` becomes its own shard.
    Identity {
        column: String,
        /// Names of indexes (already created on the table) that the
        /// MemWAL should maintain in-memory as rows are appended.
        maintained_indexes: Vec<String>,
        /// Default `ShardWriter` configuration recorded in the MemWAL index.
        writer_config_defaults: HashMap<String, String>,
    },
    /// No sharding — every `merge_insert` call writes to a single MemWAL shard.
    Unsharded {
        /// Names of indexes (already created on the table) that the
        /// MemWAL should maintain in-memory as rows are appended.
        maintained_indexes: Vec<String>,
        /// Default `ShardWriter` configuration recorded in the MemWAL index.
        writer_config_defaults: HashMap<String, String>,
    },
}

impl LsmWriteSpec {
    /// Construct a hash-bucket sharding spec with no maintained indexes.
    pub fn bucket(column: impl Into<String>, num_buckets: u32) -> Self {
        Self::Bucket {
            column: column.into(),
            num_buckets,
            maintained_indexes: Vec::new(),
            writer_config_defaults: HashMap::new(),
        }
    }

    /// Construct an identity-sharding spec (shard by the raw value of
    /// `column`) with no maintained indexes.
    ///
    /// `column` must be a deterministic function of the unenforced primary
    /// key: every row with a given primary key must always produce the same
    /// `column` value. MemWAL dedups upserts by primary key but tracks
    /// generations per shard, so if the same key is written with two
    /// different `column` values its versions land in different shards and a
    /// stale value can win. Typically `column` is the primary key itself, or
    /// a stable attribute of it (e.g. a tenant id).
    pub fn identity(column: impl Into<String>) -> Self {
        Self::Identity {
            column: column.into(),
            maintained_indexes: Vec::new(),
            writer_config_defaults: HashMap::new(),
        }
    }

    /// Construct an unsharded spec with no maintained indexes.
    pub fn unsharded() -> Self {
        Self::Unsharded {
            maintained_indexes: Vec::new(),
            writer_config_defaults: HashMap::new(),
        }
    }

    /// Replace the list of indexes the MemWAL should keep up to date as
    /// rows are appended. Each name must reference an index that already
    /// exists on the table at the time `set_lsm_write_spec` is called.
    pub fn with_maintained_indexes<I, S>(mut self, indexes: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let v: Vec<String> = indexes.into_iter().map(Into::into).collect();
        match &mut self {
            Self::Bucket {
                maintained_indexes, ..
            }
            | Self::Identity {
                maintained_indexes, ..
            }
            | Self::Unsharded {
                maintained_indexes, ..
            } => *maintained_indexes = v,
        }
        self
    }

    /// Replace the default `ShardWriter` configuration recorded in the MemWAL
    /// index, so every writer starts from the same defaults. Keys are
    /// `ShardWriter` config field names (`Duration` knobs use a `_ms` suffix);
    /// values are their string encodings.
    pub fn with_writer_config_defaults<I, K, V>(mut self, defaults: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        let m: HashMap<String, String> = defaults
            .into_iter()
            .map(|(k, v)| (k.into(), v.into()))
            .collect();
        match &mut self {
            Self::Bucket {
                writer_config_defaults,
                ..
            }
            | Self::Identity {
                writer_config_defaults,
                ..
            }
            | Self::Unsharded {
                writer_config_defaults,
                ..
            } => *writer_config_defaults = m,
        }
        self
    }

    /// Borrow the list of index names this spec asks MemWAL to maintain.
    pub fn maintained_indexes(&self) -> &[String] {
        match self {
            Self::Bucket {
                maintained_indexes, ..
            }
            | Self::Identity {
                maintained_indexes, ..
            }
            | Self::Unsharded {
                maintained_indexes, ..
            } => maintained_indexes,
        }
    }

    /// Borrow the default `ShardWriter` configuration recorded by this spec.
    pub fn writer_config_defaults(&self) -> &HashMap<String, String> {
        match self {
            Self::Bucket {
                writer_config_defaults,
                ..
            }
            | Self::Identity {
                writer_config_defaults,
                ..
            }
            | Self::Unsharded {
                writer_config_defaults,
                ..
            } => writer_config_defaults,
        }
    }
}

/// A trait for anything "table-like".  This is used for both native tables (which target
/// Lance datasets) and remote tables (which target LanceDB cloud)
///
/// This trait is still EXPERIMENTAL and subject to change in the future
#[async_trait]
pub trait BaseTable: std::fmt::Display + std::fmt::Debug + Send + Sync {
    /// Get a reference to std::any::Any
    fn as_any(&self) -> &dyn std::any::Any;
    /// Get the name of the table.
    fn name(&self) -> &str;
    /// Get the namespace of the table.
    fn namespace(&self) -> &[String];
    /// Get the id of the table
    ///
    /// This is the namespace of the table concatenated with the name
    /// separated by $
    fn id(&self) -> &str;
    /// Get the arrow [Schema] of the table.
    async fn schema(&self) -> Result<SchemaRef>;
    /// Count the number of rows in this table.
    async fn count_rows(&self, filter: Option<Filter>) -> Result<usize>;
    /// Create a physical plan for the query.
    async fn create_plan(
        &self,
        query: &AnyQuery,
        options: QueryExecutionOptions,
    ) -> Result<Arc<dyn ExecutionPlan>>;
    /// Execute a query and return the results as a stream of RecordBatches.
    async fn query(
        &self,
        query: &AnyQuery,
        options: QueryExecutionOptions,
    ) -> Result<DatasetRecordBatchStream>;
    /// Explain the plan for a query.
    async fn explain_plan(&self, query: &AnyQuery, verbose: bool) -> Result<String> {
        let plan = self.create_plan(query, Default::default()).await?;
        let display = DisplayableExecutionPlan::new(plan.as_ref());

        Ok(format!("{}", display.indent(verbose)))
    }
    async fn analyze_plan(
        &self,
        query: &AnyQuery,
        options: QueryExecutionOptions,
    ) -> Result<String>;

    /// Add new records to the table.
    async fn add(&self, add: AddDataBuilder) -> Result<AddResult>;
    /// Delete rows from the table matching the given [`Predicate`].
    async fn delete(&self, predicate: Predicate<'_>) -> Result<DeleteResult>;
    /// Update rows in the table.
    async fn update(&self, update: UpdateBuilder) -> Result<UpdateResult>;
    /// Create an index on the provided column(s).
    async fn create_index(&self, index: IndexBuilder) -> Result<()>;
    /// List the indices on the table.
    async fn list_indices(&self) -> Result<Vec<IndexConfig>>;
    /// Drop an index from the table.
    async fn drop_index(&self, name: &str) -> Result<()>;
    /// Prewarm an index in the table.
    async fn prewarm_index(&self, name: &str) -> Result<()>;
    /// Prewarm data for the table.
    ///
    /// Currently only supported on remote tables.
    /// If `columns` is `None`, all columns are prewarmed.
    async fn prewarm_data(&self, columns: Option<Vec<String>>) -> Result<()>;
    /// Get statistics about the index.
    async fn index_stats(&self, index_name: &str) -> Result<Option<IndexStatistics>>;
    /// Merge insert new records into the table.
    async fn merge_insert(
        &self,
        params: MergeInsertBuilder,
        new_data: Box<dyn RecordBatchReader + Send>,
    ) -> Result<MergeResult>;
    /// Set the unenforced primary key for the table to a single column.
    ///
    /// "Unenforced" means LanceDB does not check uniqueness on writes; the
    /// column is recorded in the schema as the primary key for use by
    /// features such as `merge_insert`. Only single-column primary keys are
    /// supported, and the key cannot be changed once set.
    ///
    /// The default implementation returns `NotSupported`; table types
    /// backed by a Lance dataset override it.
    async fn set_unenforced_primary_key(&self, _columns: &[&str]) -> Result<()> {
        Err(Error::NotSupported {
            message: "set_unenforced_primary_key is not supported on this table type".into(),
        })
    }
    /// Install an [`LsmWriteSpec`] on this table.
    ///
    /// The spec selects Lance's MemWAL LSM-style write path for future
    /// `merge_insert` calls.
    ///
    /// The default implementation returns `NotSupported`. Implementations
    /// that support the MemWAL LSM write path must override this.
    async fn set_lsm_write_spec(&self, _spec: LsmWriteSpec) -> Result<()> {
        Err(Error::NotSupported {
            message: "set_lsm_write_spec is not supported on this table type".into(),
        })
    }
    /// Remove the [`LsmWriteSpec`] from this table.
    ///
    /// This is a no-op if no spec is currently set.
    ///
    /// The default implementation returns `NotSupported`. Implementations
    /// that support the MemWAL LSM write path must override this.
    async fn unset_lsm_write_spec(&self) -> Result<()> {
        Err(Error::NotSupported {
            message: "unset_lsm_write_spec is not supported on this table type".into(),
        })
    }
    /// Drain and close any cached MemWAL shard writers for this table.
    ///
    /// The default implementation is a no-op; table types that maintain
    /// MemWAL shard writers override it.
    async fn close_lsm_writers(&self) -> Result<()> {
        Ok(())
    }
    /// Gets the table tag manager.
    async fn tags(&self) -> Result<Box<dyn Tags + '_>>;
    /// Optimize the dataset.
    async fn optimize(&self, action: OptimizeAction) -> Result<OptimizeStats>;
    /// Add columns to the table.
    async fn add_columns(
        &self,
        transforms: NewColumnTransform,
        read_columns: Option<Vec<String>>,
    ) -> Result<AddColumnsResult>;
    /// Alter columns in the table.
    async fn alter_columns(&self, alterations: &[ColumnAlteration]) -> Result<AlterColumnsResult>;
    /// Drop columns from the table.
    async fn drop_columns(&self, columns: &[&str]) -> Result<DropColumnsResult>;
    /// Get the version of the table.
    async fn version(&self) -> Result<u64>;
    /// Checkout a specific version of the table.
    async fn checkout(&self, version: u64) -> Result<()>;
    /// Checkout a table version referenced by a tag.
    /// Tags provide a human-readable way to reference specific versions of the table.
    async fn checkout_tag(&self, tag: &str) -> Result<()>;
    /// Checkout the latest version of the table.
    async fn checkout_latest(&self) -> Result<()>;
    /// Restore the table to the currently checked out version.
    async fn restore(&self) -> Result<()>;
    /// List the versions of the table.
    async fn list_versions(&self) -> Result<Vec<Version>>;
    /// Create a new branch from `from` and return a handle scoped to it.
    async fn create_branch(
        &self,
        name: &str,
        from: lance::dataset::refs::Ref,
    ) -> Result<Arc<dyn BaseTable>>;
    /// Check out an existing branch and return a handle scoped to it.
    async fn checkout_branch(&self, name: &str) -> Result<Arc<dyn BaseTable>>;
    /// Check out an existing branch at an optional version, returning a handle.
    ///
    /// `None` tracks the branch's latest; `Some(v)` pins it to that version
    /// (read-only). The default implementation composes [`Self::checkout_branch`]
    /// and [`Self::checkout`]; implementations may override it to resolve the
    /// `(branch, version)` coordinate in a single manifest read.
    async fn checkout_branch_version(
        &self,
        name: &str,
        version: Option<u64>,
    ) -> Result<Arc<dyn BaseTable>> {
        let branch = self.checkout_branch(name).await?;
        if let Some(version) = version {
            branch.checkout(version).await?;
        }
        Ok(branch)
    }
    /// List the branches of the table.
    async fn list_branches(&self) -> Result<HashMap<String, BranchContents>>;
    /// Delete a branch.
    async fn delete_branch(&self, name: &str) -> Result<()>;
    /// The branch this handle is scoped to, or `None` for `main`.
    fn current_branch(&self) -> Option<String>;
    /// Get the table definition.
    async fn table_definition(&self) -> Result<TableDefinition>;
    /// Get the table URI (storage location)
    async fn uri(&self) -> Result<String>;
    /// Get the storage options used when opening this table, if any.
    #[deprecated(since = "0.25.0", note = "Use initial_storage_options() instead")]
    async fn storage_options(&self) -> Option<HashMap<String, String>>;
    /// Get the initial storage options that were passed in when opening this table.
    ///
    /// For dynamically refreshed options (e.g., credential vending), use [`Self::latest_storage_options`].
    async fn initial_storage_options(&self) -> Option<HashMap<String, String>>;
    /// Get the latest storage options, refreshing from provider if configured.
    ///
    /// Returns `Ok(Some(options))` if storage options are available (static or refreshed),
    /// `Ok(None)` if no storage options were configured, or `Err(...)` if refresh failed.
    async fn latest_storage_options(&self) -> Result<Option<HashMap<String, String>>>;
    /// Poll until the columns are fully indexed. Will return Error::Timeout if the columns
    /// are not fully indexed within the timeout.
    async fn wait_for_index(
        &self,
        index_names: &[&str],
        timeout: std::time::Duration,
    ) -> Result<()>;
    /// Get statistics on the table
    async fn stats(&self) -> Result<TableStatistics>;
    /// Create an ExecutionPlan for inserting data into the table.
    ///
    /// This is used by the DataFusion TableProvider implementation to support
    /// INSERT INTO statements.
    async fn create_insert_exec(
        &self,
        _input: Arc<dyn datafusion_physical_plan::ExecutionPlan>,
        _write_params: WriteParams,
    ) -> Result<Arc<dyn datafusion_physical_plan::ExecutionPlan>> {
        Err(Error::NotSupported {
            message: "create_insert_exec not implemented".to_string(),
        })
    }
    /// Update per-field metadata. Merges into existing metadata by default;
    /// [`FieldMetadataUpdate::remove`] deletes a key and
    /// [`FieldMetadataUpdate::replace`] swaps the field's whole map.
    ///
    /// The default returns `NotSupported`; Lance-backed and remote tables override it.
    async fn update_field_metadata(
        &self,
        _updates: &[FieldMetadataUpdate],
    ) -> Result<UpdateFieldMetadataResult> {
        Err(Error::NotSupported {
            message: "update_field_metadata is not supported on this table type".into(),
        })
    }
}

/// A Table is a collection of strong typed Rows.
///
/// The type of the each row is defined in Apache Arrow [Schema].
#[derive(Clone, Debug)]
pub struct Table {
    inner: Arc<dyn BaseTable>,
    database: Option<Arc<dyn Database>>,
    embedding_registry: Arc<dyn EmbeddingRegistry>,
}

#[cfg(all(test, feature = "remote"))]
mod test_utils {
    use super::*;

    impl Table {
        pub fn new_with_handler<T>(
            name: impl Into<String>,
            handler: impl Fn(reqwest::Request) -> http::Response<T> + Clone + Send + Sync + 'static,
        ) -> Self
        where
            T: Into<reqwest::Body>,
        {
            let inner = Arc::new(crate::remote::table::RemoteTable::new_mock(
                name.into(),
                handler.clone(),
                None,
            ));
            let database = Arc::new(crate::remote::db::RemoteDatabase::new_mock(handler));
            Self {
                inner,
                database: Some(database),
                // Registry is unused.
                embedding_registry: Arc::new(MemoryRegistry::new()),
            }
        }

        pub fn new_with_handler_and_interval<T>(
            name: impl Into<String>,
            handler: impl Fn(reqwest::Request) -> http::Response<T> + Clone + Send + Sync + 'static,
            read_consistency_interval: Option<std::time::Duration>,
        ) -> Self
        where
            T: Into<reqwest::Body>,
        {
            let inner = Arc::new(
                crate::remote::table::RemoteTable::new_mock_with_consistency_interval(
                    name.into(),
                    handler.clone(),
                    read_consistency_interval,
                ),
            );
            let database = Arc::new(crate::remote::db::RemoteDatabase::new_mock(handler));
            Self {
                inner,
                database: Some(database),
                // Registry is unused.
                embedding_registry: Arc::new(MemoryRegistry::new()),
            }
        }

        pub fn new_with_handler_version<T>(
            name: impl Into<String>,
            version: semver::Version,
            handler: impl Fn(reqwest::Request) -> http::Response<T> + Clone + Send + Sync + 'static,
        ) -> Self
        where
            T: Into<reqwest::Body>,
        {
            let inner = Arc::new(crate::remote::table::RemoteTable::new_mock(
                name.into(),
                handler.clone(),
                Some(version),
            ));
            let database = Arc::new(crate::remote::db::RemoteDatabase::new_mock(handler));
            Self {
                inner,
                database: Some(database),
                // Registry is unused.
                embedding_registry: Arc::new(MemoryRegistry::new()),
            }
        }

        pub fn new_with_handler_and_config<T>(
            name: impl Into<String>,
            handler: impl Fn(reqwest::Request) -> http::Response<T> + Clone + Send + Sync + 'static,
            config: crate::remote::ClientConfig,
        ) -> Self
        where
            T: Into<reqwest::Body>,
        {
            let inner = Arc::new(crate::remote::table::RemoteTable::new_mock_with_config(
                name.into(),
                handler.clone(),
                config.clone(),
            ));
            let database = Arc::new(crate::remote::db::RemoteDatabase::new_mock_with_config(
                handler, config,
            ));
            Self {
                inner,
                database: Some(database),
                // Registry is unused.
                embedding_registry: Arc::new(MemoryRegistry::new()),
            }
        }

        pub fn new_with_handler_version_and_config<T>(
            name: impl Into<String>,
            version: semver::Version,
            handler: impl Fn(reqwest::Request) -> http::Response<T> + Clone + Send + Sync + 'static,
            config: crate::remote::ClientConfig,
        ) -> Self
        where
            T: Into<reqwest::Body>,
        {
            let inner = Arc::new(
                crate::remote::table::RemoteTable::new_mock_with_version_and_config(
                    name.into(),
                    handler.clone(),
                    Some(version),
                    config.clone(),
                ),
            );
            let database = Arc::new(crate::remote::db::RemoteDatabase::new_mock_with_config(
                handler, config,
            ));
            Self {
                inner,
                database: Some(database),
                // Registry is unused.
                embedding_registry: Arc::new(MemoryRegistry::new()),
            }
        }
    }
}

impl std::fmt::Display for Table {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.inner)
    }
}

impl From<Arc<dyn BaseTable>> for Table {
    fn from(inner: Arc<dyn BaseTable>) -> Self {
        Self {
            inner,
            database: None,
            embedding_registry: Arc::new(MemoryRegistry::new()),
        }
    }
}

impl Table {
    pub fn new(inner: Arc<dyn BaseTable>, database: Arc<dyn Database>) -> Self {
        Self {
            inner,
            database: Some(database),
            embedding_registry: Arc::new(MemoryRegistry::new()),
        }
    }

    pub fn base_table(&self) -> &Arc<dyn BaseTable> {
        &self.inner
    }

    pub fn database(&self) -> &Arc<dyn Database> {
        self.database.as_ref().unwrap()
    }

    pub fn embedding_registry(&self) -> &Arc<dyn EmbeddingRegistry> {
        &self.embedding_registry
    }

    pub(crate) fn new_with_embedding_registry(
        inner: Arc<dyn BaseTable>,
        database: Arc<dyn Database>,
        embedding_registry: Arc<dyn EmbeddingRegistry>,
    ) -> Self {
        Self {
            inner,
            database: Some(database),
            embedding_registry,
        }
    }

    /// Cast as [`NativeTable`], or return None it if is not a [`NativeTable`].
    ///
    /// Warning: This function will be removed soon (features exclusive to NativeTable
    ///          will be added to Table)
    pub fn as_native(&self) -> Option<&NativeTable> {
        self.inner.as_native()
    }

    /// Get the name of the table.
    pub fn name(&self) -> &str {
        self.inner.name()
    }

    /// Get the namespace of the table.
    pub fn namespace(&self) -> &[String] {
        self.inner.namespace()
    }

    /// Get the ID of the table (namespace + name joined by '$').
    pub fn id(&self) -> &str {
        self.inner.id()
    }

    /// Get the dataset of the table if it is a native table
    ///
    /// Returns None otherwise
    pub fn dataset(&self) -> Option<&dataset::DatasetConsistencyWrapper> {
        self.inner.as_native().map(|t| &t.dataset)
    }

    /// Get the arrow [Schema] of the table.
    pub async fn schema(&self) -> Result<SchemaRef> {
        self.inner.schema().await
    }

    /// Count the number of rows in this dataset.
    ///
    /// # Arguments
    ///
    /// * `filter` if present, only count rows matching the filter
    pub async fn count_rows(&self, filter: Option<String>) -> Result<usize> {
        self.inner.count_rows(filter.map(Filter::Sql)).await
    }

    /// Insert new records into this Table
    ///
    /// # Arguments
    ///
    /// * `data` data to be added to the Table
    /// * `options` options to control how data is added
    pub fn add<T: Scannable + 'static>(&self, data: T) -> AddDataBuilder {
        AddDataBuilder::new(
            self.inner.clone(),
            Box::new(data),
            Some(self.embedding_registry.clone()),
        )
    }

    /// Update existing records in the Table
    ///
    /// An update operation can be used to adjust existing values.  Use the
    /// returned builder to specify which columns to update.  The new value
    /// can be a literal value (e.g. replacing nulls with some default value)
    /// or an expression applied to the old value (e.g. incrementing a value)
    ///
    /// An optional condition can be specified (e.g. "only update if the old
    /// value is 0")
    ///
    /// Note: if your condition is something like "some_id_column == 7" and
    /// you are updating many rows (with different ids) then you will get
    /// better performance with a single [`merge_insert`] call instead of
    /// repeatedly calilng this method.
    pub fn update(&self) -> UpdateBuilder {
        UpdateBuilder::new(self.inner.clone())
    }

    /// Delete the rows from table that match the predicate.
    ///
    /// # Arguments
    /// - `predicate` - A SQL string (`&str`) or DataFusion expression (`&Expr`)
    ///   that selects the rows to delete.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use std::sync::Arc;
    /// # use arrow_array::{FixedSizeListArray, types::Float32Type, RecordBatch,
    /// #   RecordBatchIterator, Int32Array};
    /// # use arrow_schema::{Schema, Field, DataType};
    /// use datafusion_expr::{col, lit};
    /// # tokio::runtime::Runtime::new().unwrap().block_on(async {
    /// let tmpdir = tempfile::tempdir().unwrap();
    /// let db = lancedb::connect(tmpdir.path().to_str().unwrap())
    ///     .execute()
    ///     .await
    ///     .unwrap();
    /// let schema = Arc::new(Schema::new(vec![
    ///     Field::new("id", DataType::Int32, false),
    ///     Field::new("vector", DataType::FixedSizeList(
    ///         Arc::new(Field::new("item", DataType::Float32, true)), 128), true),
    /// ]));
    /// let data = RecordBatch::try_new(
    ///     schema.clone(),
    ///     vec![
    ///         Arc::new(Int32Array::from_iter_values(0..10)),
    ///         Arc::new(
    ///             FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
    ///                 (0..10).map(|_| Some(vec![Some(1.0); 128])),
    ///                 128,
    ///             ),
    ///         ),
    ///     ],
    /// )
    /// .unwrap();
    /// let tbl = db
    ///     .create_table("delete_test", data)
    ///     .execute()
    ///     .await
    ///     .unwrap();
    ///
    /// // Using a SQL string:
    /// tbl.delete("id > 5").await.unwrap();
    ///
    /// // Using a DataFusion expression:
    /// let expr = col("id").lt(lit(4));
    /// tbl.delete(&expr).await.unwrap();
    /// # });
    /// ```
    pub async fn delete(&self, predicate: impl Into<Predicate<'_>>) -> Result<DeleteResult> {
        self.inner.delete(predicate.into()).await
    }

    /// Create an index on the provided column(s).
    ///
    /// Indices are used to speed up searches and are often needed when the size of the table
    /// becomes large (the exact size depends on many factors but somewhere between 100K rows
    /// and 1M rows is a good rule of thumb)
    ///
    /// There are a variety of indices available.  They are described more in
    /// [`crate::index::Index`].  The simplest thing to do is to use `index::Index::Auto` which
    /// will attempt to create the most useful index based on the column type and column
    /// statistics. `BTree` index is created by default for numeric, temporal, and
    /// string columns.
    ///
    /// Once an index is created it will remain until the data is overwritten (e.g. an
    /// add operation with mode overwrite) or the indexed column is dropped.
    ///
    /// Indices are not automatically updated with new data.  If you add new data to the
    /// table then the index will not include the new rows.  However, a table search will
    /// still consider the unindexed rows.  Searches will issue both an indexed search (on
    /// the data covered by the index) and a flat search (on the unindexed data) and the
    /// results will be combined.
    ///
    /// If there is enough unindexed data then the flat search will become slow and the index
    /// should be optimized.  Optimizing an index will add any unindexed data to the existing
    /// index without rerunning the full index creation process.  For more details see
    /// [Table::optimize].
    ///
    /// Note: Multi-column (composite) indices are not currently supported.  However, they will
    /// be supported in the future and the API is designed to be compatible with them.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use std::sync::Arc;
    /// # use arrow_array::{FixedSizeListArray, types::Float32Type, RecordBatch,
    /// #   RecordBatchIterator, Int32Array};
    /// # use arrow_schema::{Schema, Field, DataType};
    /// # tokio::runtime::Runtime::new().unwrap().block_on(async {
    /// use lancedb::index::Index;
    /// let tmpdir = tempfile::tempdir().unwrap();
    /// let db = lancedb::connect(tmpdir.path().to_str().unwrap())
    ///     .execute()
    ///     .await
    ///     .unwrap();
    /// # let tbl = db.open_table("idx_test").execute().await.unwrap();
    /// // Create IVF PQ index on the "vector" column by default.
    /// tbl.create_index(&["vector"], Index::Auto)
    ///    .execute()
    ///    .await
    ///    .unwrap();
    /// // Create a BTree index on the "id" column.
    /// tbl.create_index(&["id"], Index::Auto)
    ///     .execute()
    ///     .await
    ///     .unwrap();
    /// // Create a LabelList index on the "tags" column.
    /// tbl.create_index(&["tags"], Index::LabelList(Default::default()))
    ///     .execute()
    ///     .await
    ///     .unwrap();
    /// # });
    /// ```
    pub fn create_index(&self, columns: &[impl AsRef<str>], index: Index) -> IndexBuilder {
        IndexBuilder::new(
            self.inner.clone(),
            columns
                .iter()
                .map(|val| val.as_ref().to_string())
                .collect::<Vec<_>>(),
            index,
        )
    }

    /// See [Table::create_index]
    /// For remote tables, this allows an optional wait_timeout to poll until asynchronous indexing is complete
    pub fn create_index_with_timeout(
        &self,
        columns: &[impl AsRef<str>],
        index: Index,
        wait_timeout: Option<std::time::Duration>,
    ) -> IndexBuilder {
        let mut builder = IndexBuilder::new(
            self.inner.clone(),
            columns
                .iter()
                .map(|val| val.as_ref().to_string())
                .collect::<Vec<_>>(),
            index,
        );
        if let Some(timeout) = wait_timeout {
            builder = builder.wait_timeout(timeout);
        }
        builder
    }

    /// Create a builder for a merge insert operation
    ///
    /// This operation can add rows, update rows, and remove rows all in a single
    /// transaction. It is a very generic tool that can be used to create
    /// behaviors like "insert if not exists", "update or insert (i.e. upsert)",
    /// or even replace a portion of existing data with new data (e.g. replace
    /// all data where month="january")
    ///
    /// The merge insert operation works by combining new data from a
    /// **source table** with existing data in a **target table** by using a
    /// join.  There are three categories of records.
    ///
    /// "Matched" records are records that exist in both the source table and
    /// the target table. "Not matched" records exist only in the source table
    /// (e.g. these are new data) "Not matched by source" records exist only
    /// in the target table (this is old data)
    ///
    /// The builder returned by this method can be used to customize what
    /// should happen for each category of data.
    ///
    /// Please note that the data may appear to be reordered as part of this
    /// operation.  This is because updated rows will be deleted from the
    /// dataset and then reinserted at the end with the new values.
    ///
    /// # Arguments
    ///
    /// * `on` One or more columns to join on.  This is how records from the
    ///   source table and target table are matched.  Typically this is some
    ///   kind of key or id column.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use std::sync::Arc;
    /// # use arrow_array::{FixedSizeListArray, types::Float32Type, RecordBatch,
    /// #   RecordBatchIterator, Int32Array};
    /// # use arrow_schema::{Schema, Field, DataType};
    /// # tokio::runtime::Runtime::new().unwrap().block_on(async {
    /// let tmpdir = tempfile::tempdir().unwrap();
    /// let db = lancedb::connect(tmpdir.path().to_str().unwrap())
    ///     .execute()
    ///     .await
    ///     .unwrap();
    /// # let tbl = db.open_table("idx_test").execute().await.unwrap();
    /// # let schema = Arc::new(Schema::new(vec![
    /// #  Field::new("id", DataType::Int32, false),
    /// #  Field::new("vector", DataType::FixedSizeList(
    /// #    Arc::new(Field::new("item", DataType::Float32, true)), 128), true),
    /// # ]));
    /// let new_data = RecordBatchIterator::new(
    ///     vec![RecordBatch::try_new(
    ///         schema.clone(),
    ///         vec![
    ///             Arc::new(Int32Array::from_iter_values(0..10)),
    ///             Arc::new(
    ///                 FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
    ///                     (0..10).map(|_| Some(vec![Some(1.0); 128])),
    ///                     128,
    ///                 ),
    ///             ),
    ///         ],
    ///     )
    ///     .unwrap()]
    ///     .into_iter()
    ///     .map(Ok),
    ///     schema.clone(),
    /// );
    /// // Perform an upsert operation
    /// let mut merge_insert = tbl.merge_insert(&["id"]);
    /// merge_insert
    ///     .when_matched_update_all(None)
    ///     .when_not_matched_insert_all();
    /// merge_insert.execute(Box::new(new_data)).await.unwrap();
    /// # });
    /// ```
    pub fn merge_insert(&self, on: &[&str]) -> MergeInsertBuilder {
        MergeInsertBuilder::new(
            self.inner.clone(),
            on.iter().map(|s| s.to_string()).collect(),
        )
    }

    /// Create a [`Query`] Builder.
    ///
    /// Queries allow you to search your existing data.  By default the query will
    /// return all the data in the table in no particular order.  The builder
    /// returned by this method can be used to control the query using filtering,
    /// vector similarity, sorting, and more.
    ///
    /// Note: By default, all columns are returned.  For best performance, you should
    /// only fetch the columns you need.  See [`Query::select_with_projection`] for
    /// more details.
    ///
    /// When appropriate, various indices and statistics will be used to accelerate
    /// the query.
    ///
    /// # Examples
    ///
    /// ## Vector search
    ///
    /// This example will find the 10 rows whose value in the "vector" column are
    /// closest to the query vector [1.0, 2.0, 3.0].  If an index has been created
    /// on the "vector" column then this will perform an ANN search.
    ///
    /// The [`Query::refine_factor`] and [`Query::nprobes`] methods are used to
    /// control the recall / latency tradeoff of the search.
    ///
    /// ```no_run
    /// # use arrow_array::RecordBatch;
    /// # use futures::TryStreamExt;
    /// # tokio::runtime::Runtime::new().unwrap().block_on(async {
    /// # let conn = lancedb::connect("/tmp").execute().await.unwrap();
    /// # let tbl = conn.open_table("tbl").execute().await.unwrap();
    /// use crate::lancedb::Table;
    /// use crate::lancedb::query::ExecutableQuery;
    /// let stream = tbl
    ///     .query()
    ///     .nearest_to(&[1.0, 2.0, 3.0])
    ///     .unwrap()
    ///     .refine_factor(5)
    ///     .nprobes(10)
    ///     .execute()
    ///     .await
    ///     .unwrap();
    /// let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
    /// # });
    /// ```
    ///
    /// ## SQL-style filter
    ///
    /// This query will return up to 1000 rows whose value in the `id` column
    /// is greater than 5.  LanceDb supports a broad set of filtering functions.
    ///
    /// ```no_run
    /// # use arrow_array::RecordBatch;
    /// # use futures::TryStreamExt;
    /// # tokio::runtime::Runtime::new().unwrap().block_on(async {
    /// # let conn = lancedb::connect("/tmp").execute().await.unwrap();
    /// # let tbl = conn.open_table("tbl").execute().await.unwrap();
    /// use crate::lancedb::Table;
    /// use crate::lancedb::query::{ExecutableQuery, QueryBase};
    /// let stream = tbl
    ///     .query()
    ///     .only_if("id > 5")
    ///     .limit(1000)
    ///     .execute()
    ///     .await
    ///     .unwrap();
    /// let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
    /// # });
    /// ```
    ///
    /// ## Full scan
    ///
    /// This query will return everything in the table in no particular
    /// order.
    ///
    /// ```no_run
    /// # use arrow_array::RecordBatch;
    /// # use futures::TryStreamExt;
    /// # tokio::runtime::Runtime::new().unwrap().block_on(async {
    /// # let conn = lancedb::connect("/tmp").execute().await.unwrap();
    /// # let tbl = conn.open_table("tbl").execute().await.unwrap();
    /// use crate::lancedb::Table;
    /// use crate::lancedb::query::ExecutableQuery;
    /// let stream = tbl.query().execute().await.unwrap();
    /// let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
    /// # });
    /// ```
    pub fn query(&self) -> Query {
        Query::new(self.inner.clone())
    }

    /// Extract rows from the dataset using dataset offsets.
    ///
    /// Dataset offsets are 0-indexed and relative to the current version of the table.
    /// They are not stable.  A row with an offset of N may have a different offset in a
    /// different version of the table (e.g. if an earlier row is deleted).
    ///
    /// Offsets are useful for sampling as the set of all valid offsets is easily
    /// known in advance to be [0, len(table)).
    ///
    /// No guarantees are made regarding the order in which results are returned.  If you
    /// desire an output order that matches the order of the given offsets, you will need
    /// to add the row offset column to the output and align it yourself.
    ///
    /// Parameters
    /// ----------
    /// offsets: list[int]
    ///     The offsets to take.
    ///
    /// Returns
    /// -------
    /// pa.RecordBatch
    ///     A record batch containing the rows at the given offsets.
    pub fn take_offsets(&self, offsets: Vec<u64>) -> TakeQuery {
        TakeQuery::from_offsets(self.inner.clone(), offsets)
    }

    /// Extract rows from the dataset using row ids.
    ///
    /// Row ids are not stable and are relative to the current version of the table.
    /// They can change due to compaction and updates.
    ///
    /// Even so, row ids are more stable than offsets and can be useful in some situations.
    ///
    /// There is an ongoing effort to make row ids stable which is tracked at
    /// https://github.com/lancedb/lancedb/issues/1120
    ///
    /// No guarantees are made regarding the order in which results are returned.  If you
    /// desire an output order that matches the order of the given ids, you will need
    /// to add the row id column to the output and align it yourself.
    /// Parameters
    /// ----------
    /// row_ids: list[int]
    ///     The row ids to take.
    ///
    pub fn take_row_ids(&self, row_ids: Vec<u64>) -> TakeQuery {
        TakeQuery::from_row_ids(self.inner.clone(), row_ids)
    }

    /// Search the table with a given query vector.
    ///
    /// This is a convenience method for preparing a vector query and
    /// is the same thing as calling `nearest_to` on the builder returned
    /// by `query`.  See [`Query::nearest_to`] for more details.
    pub fn vector_search(&self, query: impl IntoQueryVector) -> Result<VectorQuery> {
        self.query().nearest_to(query)
    }

    /// Optimize the on-disk data and indices for better performance.
    ///
    /// Modeled after ``VACUUM`` in PostgreSQL.
    ///
    /// Optimization is discussed in more detail in the [OptimizeAction] documentation
    /// and covers three operations:
    ///
    ///  * Compaction: Merges small files into larger ones
    ///  * Prune: Removes old versions of the dataset
    ///  * Index: Optimizes the indices, adding new data to existing indices
    ///
    /// The frequency an application should call optimize is based on the frequency of
    /// data modifications.  If data is frequently added, deleted, or updated then
    /// optimize should be run frequently.  A good rule of thumb is to run optimize if
    /// you have added or modified 100,000 or more records or run more than 20 data
    /// modification operations.
    pub async fn optimize(&self, action: OptimizeAction) -> Result<OptimizeStats> {
        self.inner.optimize(action).await
    }

    /// Add new columns to the table, providing values to fill in.
    pub async fn add_columns(
        &self,
        transforms: NewColumnTransform,
        read_columns: Option<Vec<String>>,
    ) -> Result<AddColumnsResult> {
        self.inner.add_columns(transforms, read_columns).await
    }

    /// Change a column's name or nullability.
    pub async fn alter_columns(
        &self,
        alterations: &[ColumnAlteration],
    ) -> Result<AlterColumnsResult> {
        self.inner.alter_columns(alterations).await
    }

    /// Update per-field metadata (merges by default).
    pub async fn update_field_metadata(
        &self,
        updates: &[FieldMetadataUpdate],
    ) -> Result<UpdateFieldMetadataResult> {
        self.inner.update_field_metadata(updates).await
    }

    /// Remove columns from the table.
    pub async fn drop_columns(&self, columns: &[&str]) -> Result<DropColumnsResult> {
        self.inner.drop_columns(columns).await
    }

    /// Set the unenforced primary key for this table to a single column.
    ///
    /// "Unenforced" means LanceDB does not check uniqueness on writes; the
    /// column is recorded in the schema as the primary key so that features
    /// such as `merge_insert` can use it.
    ///
    /// Only single-column primary keys are supported, and the key cannot be
    /// changed once set — calling this on a table that already has an
    /// unenforced primary key fails. `columns` is an iterable for binding
    /// ergonomics but must yield exactly one column:
    ///
    /// - `table.set_unenforced_primary_key(["id"])`
    pub async fn set_unenforced_primary_key<I, S>(&self, columns: I) -> Result<()>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let owned: Vec<String> = columns.into_iter().map(Into::into).collect();
        let borrowed: Vec<&str> = owned.iter().map(String::as_str).collect();
        self.inner.set_unenforced_primary_key(&borrowed).await
    }

    /// Install an [`LsmWriteSpec`] on this table, selecting Lance's MemWAL
    /// LSM-style write path for future `merge_insert` calls.
    ///
    /// [`LsmWriteSpec`] chooses one of three sharding strategies:
    ///
    /// - [`LsmWriteSpec::bucket`] — hash-bucket writes by a scalar column.
    /// - [`LsmWriteSpec::identity`] — shard by the raw value of a scalar column.
    /// - [`LsmWriteSpec::unsharded`] — route every write to a single shard.
    ///
    /// # Example
    ///
    /// ```
    /// # use lancedb::table::{LsmWriteSpec, Table};
    /// # async fn example(table: &Table) -> Result<(), Box<dyn std::error::Error>> {
    /// table
    ///     .set_lsm_write_spec(
    ///         LsmWriteSpec::bucket("id", 16).with_maintained_indexes(["id_idx"]),
    ///     )
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn set_lsm_write_spec(&self, spec: LsmWriteSpec) -> Result<()> {
        self.inner.set_lsm_write_spec(spec).await
    }

    /// Remove the [`LsmWriteSpec`] from this table, reverting to the standard
    /// `merge_insert` write path.
    ///
    /// Errors if no spec is currently set.
    pub async fn unset_lsm_write_spec(&self) -> Result<()> {
        self.inner.unset_lsm_write_spec().await
    }

    /// Drain and close any cached MemWAL shard writers held for this table.
    ///
    /// When an [`LsmWriteSpec`] is installed, `merge_insert` opens MemWAL shard
    /// writers and caches them for reuse across calls. This closes them,
    /// flushing pending data; writers reopen lazily on the next `merge_insert`.
    /// It is a no-op when no writers are cached.
    pub async fn close_lsm_writers(&self) -> Result<()> {
        self.inner.close_lsm_writers().await
    }

    /// Retrieve the version of the table
    ///
    /// LanceDb supports versioning.  Every operation that modifies the table increases
    /// version.  As long as a version hasn't been deleted you can `[Self::checkout]` that
    /// version to view the data at that point.  In addition, you can `[Self::restore]` the
    /// version to replace the current table with a previous version.
    pub async fn version(&self) -> Result<u64> {
        self.inner.version().await
    }

    /// Checks out a specific version of the Table
    ///
    /// Any read operation on the table will now access the data at the checked out version.
    /// As a consequence, calling this method will disable any read consistency interval
    /// that was previously set.
    ///
    /// This is a read-only operation that turns the table into a sort of "view"
    /// or "detached head".  Other table instances will not be affected.  To make the change
    /// permanent you can use the `[Self::restore]` method.
    ///
    /// Any operation that modifies the table will fail while the table is in a checked
    /// out state.
    ///
    /// To return the table to a normal state use `[Self::checkout_latest]`
    pub async fn checkout(&self, version: u64) -> Result<()> {
        self.inner.checkout(version).await
    }

    /// Checks out a specific version of the Table by tag
    ///
    /// Any read operation on the table will now access the data at the version referenced by the tag.
    /// As a consequence, calling this method will disable any read consistency interval
    /// that was previously set.
    ///
    /// This is a read-only operation that turns the table into a sort of "view"
    /// or "detached head".  Other table instances will not be affected.  To make the change
    /// permanent you can use the `[Self::restore]` method.
    ///
    /// Any operation that modifies the table will fail while the table is in a checked
    /// out state.
    ///
    /// To return the table to a normal state use `[Self::checkout_latest]`
    pub async fn checkout_tag(&self, tag: &str) -> Result<()> {
        self.inner.checkout_tag(tag).await
    }

    /// Ensures the table is pointing at the latest version
    ///
    /// This can be used to manually update a table when the read_consistency_interval is None
    /// It can also be used to undo a `[Self::checkout]` operation
    pub async fn checkout_latest(&self) -> Result<()> {
        self.inner.checkout_latest().await
    }

    /// Restore the table to the currently checked out version
    ///
    /// This operation will fail if checkout has not been called previously
    ///
    /// This operation will overwrite the latest version of the table with a
    /// previous version.  Any changes made since the checked out version will
    /// no longer be visible.
    ///
    /// Once the operation concludes the table will no longer be in a checked
    /// out state and the read_consistency_interval, if any, will apply.
    pub async fn restore(&self) -> Result<()> {
        self.inner.restore().await
    }

    /// List all the versions of the table
    pub async fn list_versions(&self) -> Result<Vec<Version>> {
        self.inner.list_versions().await
    }

    /// List all indices that have been created with [`Self::create_index`]
    pub async fn list_indices(&self) -> Result<Vec<IndexConfig>> {
        self.inner.list_indices().await
    }

    /// Get the table URI (storage location)
    ///
    /// Returns the full storage location of the table (e.g., S3/GCS path).
    /// For remote tables, this fetches the location from the server via describe.
    pub async fn uri(&self) -> Result<String> {
        self.inner.uri().await
    }

    /// Get the storage options used when opening this table, if any.
    ///
    /// Warning: This is an internal API and the return value is subject to change.
    #[deprecated(since = "0.25.0", note = "Use initial_storage_options() instead")]
    pub async fn storage_options(&self) -> Option<HashMap<String, String>> {
        #[allow(deprecated)]
        self.inner.storage_options().await
    }

    /// Get the initial storage options that were passed in when opening this table.
    ///
    /// For dynamically refreshed options (e.g., credential vending), use [`Self::latest_storage_options`].
    ///
    /// Warning: This is an internal API and the return value is subject to change.
    pub async fn initial_storage_options(&self) -> Option<HashMap<String, String>> {
        self.inner.initial_storage_options().await
    }

    /// Get the latest storage options, refreshing from provider if configured.
    ///
    /// This method is useful for credential vending scenarios where storage options
    /// may be refreshed dynamically. If no dynamic provider is configured, this
    /// returns the initial static options.
    ///
    /// Warning: This is an internal API and the return value is subject to change.
    pub async fn latest_storage_options(&self) -> Result<Option<HashMap<String, String>>> {
        self.inner.latest_storage_options().await
    }

    /// Get statistics about an index.
    /// Returns None if the index does not exist.
    pub async fn index_stats(
        &self,
        index_name: impl AsRef<str>,
    ) -> Result<Option<IndexStatistics>> {
        self.inner.index_stats(index_name.as_ref()).await
    }

    /// Drop an index from the table.
    ///
    /// Note: This is not yet available in LanceDB cloud.
    ///
    /// This does not delete the index from disk, it just removes it from the table.
    /// To delete the index, run [`Self::optimize()`] after dropping the index.
    ///
    /// Use [`Self::list_indices()`] to find the names of the indices.
    pub async fn drop_index(&self, name: &str) -> Result<()> {
        self.inner.drop_index(name).await
    }

    /// Prewarm an index in the table.
    ///
    /// This is a hint to the database that the index will be accessed in the
    /// future and should be loaded into memory if possible.  This can reduce
    /// cold-start latency for subsequent queries.
    ///
    /// This call initiates prewarming and returns once the request is accepted.
    /// It is idempotent and safe to call from multiple clients concurrently.
    ///
    /// It is generally wasteful to call this if the index does not fit into the
    /// available cache.  Not all index types support prewarming; unsupported
    /// indices will silently ignore the request.
    ///
    /// Use [`Self::list_indices()`] to find the names of the indices.
    pub async fn prewarm_index(&self, name: &str) -> Result<()> {
        self.inner.prewarm_index(name).await
    }

    /// Prewarm data for the table.
    ///
    /// This is a hint to the database that the given columns will be accessed in
    /// the future and the database should prefetch the data if possible.  This
    /// can reduce cold-start latency for subsequent queries.  Currently only
    /// supported on remote tables.
    ///
    /// This call initiates prewarming and returns once the request is accepted.
    /// It is idempotent and safe to call from multiple clients concurrently —
    /// calling it on already-prewarmed columns is a no-op on the server.
    ///
    /// This operation has a large upfront cost but can speed up future queries
    /// that need to fetch the given columns.  Large columns such as embeddings
    /// or binary data may not be practical to prewarm.  This feature is intended
    /// for workloads that issue many queries against the same columns.
    ///
    /// If `columns` is `None`, all columns are prewarmed.
    pub async fn prewarm_data(&self, columns: Option<Vec<String>>) -> Result<()> {
        self.inner.prewarm_data(columns).await
    }

    /// Poll until the columns are fully indexed. Will return Error::Timeout if the columns
    /// are not fully indexed within the timeout.
    pub async fn wait_for_index(
        &self,
        index_names: &[&str],
        timeout: std::time::Duration,
    ) -> Result<()> {
        self.inner.wait_for_index(index_names, timeout).await
    }

    /// Get the tags manager.
    pub async fn tags(&self) -> Result<Box<dyn Tags + '_>> {
        self.inner.tags().await
    }

    /// Create a new branch from `from` (a version, tag, or branch)
    pub async fn create_branch(
        &self,
        name: &str,
        from: impl Into<lance::dataset::refs::Ref>,
    ) -> Result<Self> {
        let inner = self.inner.create_branch(name, from.into()).await?;
        Ok(Self {
            inner,
            database: self.database.clone(),
            embedding_registry: self.embedding_registry.clone(),
        })
    }

    /// Check out an existing branch and return a handle scoped to it.
    ///
    /// With `version` set, the returned handle is pinned to that version of the
    /// branch: a read-only, detached view (as with [`Self::checkout`]). With
    /// `version` as `None` it tracks the branch's latest and stays writable.
    ///
    /// ```
    /// # use lancedb::Table;
    /// # async fn f(table: &Table) -> Result<(), Box<dyn std::error::Error>> {
    /// let exp_at_v3 = table.checkout_branch("exp", Some(3)).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn checkout_branch(&self, name: &str, version: Option<u64>) -> Result<Self> {
        let inner = self.inner.checkout_branch_version(name, version).await?;
        Ok(Self {
            inner,
            database: self.database.clone(),
            embedding_registry: self.embedding_registry.clone(),
        })
    }

    /// List the branches of the table.
    pub async fn list_branches(&self) -> Result<HashMap<String, BranchContents>> {
        self.inner.list_branches().await
    }

    /// Delete a branch.
    pub async fn delete_branch(&self, name: &str) -> Result<()> {
        self.inner.delete_branch(name).await
    }

    /// The branch this handle is scoped to, or `None` for `main`.
    pub fn current_branch(&self) -> Option<String> {
        self.inner.current_branch()
    }

    /// Retrieve statistics on the table
    pub async fn stats(&self) -> Result<TableStatistics> {
        self.inner.stats().await
    }
}

pub struct NativeTags {
    dataset: dataset::DatasetConsistencyWrapper,
}
#[async_trait]
impl Tags for NativeTags {
    async fn list(&self) -> Result<HashMap<String, TagContents>> {
        let dataset = self.dataset.get().await?;
        Ok(dataset.tags().list().await?)
    }

    async fn get_version(&self, tag: &str) -> Result<u64> {
        let dataset = self.dataset.get().await?;
        Ok(dataset.tags().get_version(tag).await?)
    }

    async fn create(&mut self, tag: &str, version: u64) -> Result<()> {
        let dataset = self.dataset.get().await?;
        dataset.tags().create(tag, version).await?;
        Ok(())
    }

    async fn delete(&mut self, tag: &str) -> Result<()> {
        let dataset = self.dataset.get().await?;
        dataset.tags().delete(tag).await?;
        Ok(())
    }

    async fn update(&mut self, tag: &str, version: u64) -> Result<()> {
        let dataset = self.dataset.get().await?;
        dataset.tags().update(tag, version).await?;
        Ok(())
    }
}

pub trait NativeTableExt {
    /// Cast as [`NativeTable`], or return None it if is not a [`NativeTable`].
    fn as_native(&self) -> Option<&NativeTable>;
}

impl NativeTableExt for Arc<dyn BaseTable> {
    fn as_native(&self) -> Option<&NativeTable> {
        self.as_any().downcast_ref::<NativeTable>()
    }
}

/// A table in a LanceDB database.
#[derive(Clone)]
pub struct NativeTable {
    name: String,
    namespace: Vec<String>,
    id: String,
    uri: String,
    pub(crate) dataset: dataset::DatasetConsistencyWrapper,
    // This comes from the connection options. We store here so we can pass down
    // to the dataset when we recreate it (for example, in checkout_latest).
    read_consistency_interval: Option<std::time::Duration>,
    // Optional namespace client for namespace operations (e.g., managed versioning).
    // pub(crate) so query.rs can access the field for server-side query execution.
    pub(crate) namespace_client: Option<Arc<dyn LanceNamespace>>,
    // Operations to push down to the namespace server.
    // pub(crate) so query.rs can access the field for server-side query execution.
    pub(crate) pushdown_operations: HashSet<NamespaceClientPushdownOperation>,
}

impl std::fmt::Debug for NativeTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeTable")
            .field("name", &self.name)
            .field("namespace", &self.namespace)
            .field("id", &self.id)
            .field("uri", &self.uri)
            .field("read_consistency_interval", &self.read_consistency_interval)
            .field("namespace_client", &self.namespace_client)
            .field("pushdown_operations", &self.pushdown_operations)
            .finish()
    }
}

impl std::fmt::Display for NativeTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "NativeTable({}, uri={}, read_consistency_interval={})",
            self.name,
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

impl NativeTable {
    /// Opens an existing Table
    ///
    /// # Arguments
    ///
    /// * `uri` - The uri to a [NativeTable]
    /// * `name` - The table name
    ///
    /// # Returns
    ///
    /// * A [NativeTable] object.
    pub async fn open(uri: &str) -> Result<Self> {
        let name = Self::get_table_name(uri)?;
        Self::open_with_params(
            uri,
            &name,
            vec![],
            None,
            None,
            None,
            None,
            HashSet::new(),
            None,
        )
        .await
    }

    /// Opens an existing Table
    ///
    /// # Arguments
    ///
    /// * `base_path` - The base path where the table is located
    /// * `name` The Table name
    /// * `params` The [ReadParams] to use when opening the table
    /// * `namespace_client` - Optional namespace client for namespace operations
    /// * `pushdown_operations` - Operations to push down to the namespace server
    /// * `managed_versioning` - Whether managed versioning is enabled. If None and namespace_client
    ///   is provided, the value will be fetched via describe_table.
    ///
    /// # Returns
    ///
    /// * A [NativeTable] object.
    #[allow(clippy::too_many_arguments)]
    pub async fn open_with_params(
        uri: &str,
        name: &str,
        namespace: Vec<String>,
        write_store_wrapper: Option<Arc<dyn WrappingObjectStore>>,
        params: Option<ReadParams>,
        read_consistency_interval: Option<std::time::Duration>,
        namespace_client: Option<Arc<dyn LanceNamespace>>,
        pushdown_operations: HashSet<NamespaceClientPushdownOperation>,
        managed_versioning: Option<bool>,
    ) -> Result<Self> {
        let params = params.unwrap_or_default();
        // patch the params if we have a write store wrapper
        let params = match write_store_wrapper.clone() {
            Some(wrapper) => params.patch_with_store_wrapper(wrapper)?,
            None => params,
        };

        // Build table_id from namespace + name
        let mut table_id = namespace.clone();
        table_id.push(name.to_string());

        // Determine if managed_versioning is enabled
        // Use the provided value if available, otherwise query the namespace
        let managed_versioning = match managed_versioning {
            Some(value) => value,
            None if namespace_client.is_some() => {
                let ns_client = namespace_client.as_ref().unwrap();
                let describe_request = DescribeTableRequest {
                    id: Some(table_id.clone()),
                    ..Default::default()
                };
                let response = ns_client
                    .describe_table(describe_request)
                    .await
                    .map_err(|e| Error::Runtime {
                        message: format!(
                            "Failed to describe table via namespace client: {}. \
                             If you don't need managed versioning, don't pass namespace_client.",
                            e
                        ),
                    })?;
                response.managed_versioning == Some(true)
            }
            None => false,
        };

        let mut builder = DatasetBuilder::from_uri(uri).with_read_params(params);

        // Set up commit handler when managed_versioning is enabled
        if managed_versioning && let Some(ref ns_client) = namespace_client {
            let external_store = LanceNamespaceExternalManifestStore::for_table_uri(
                ns_client.clone(),
                table_id.clone(),
                uri,
            )?;
            let commit_handler: Arc<dyn CommitHandler> = Arc::new(ExternalManifestCommitHandler {
                external_manifest_store: Arc::new(external_store),
            });
            builder = builder.with_commit_handler(commit_handler);
        }

        let dataset = builder.load().await.map_err(|e| match e {
            lance::Error::DatasetNotFound { .. } => Error::TableNotFound {
                name: name.to_string(),
                source: Box::new(e),
            },
            e => e.into(),
        })?;

        let dataset = DatasetConsistencyWrapper::new_latest(dataset, read_consistency_interval);
        let id = Self::build_id(&namespace, name);

        Ok(Self {
            name: name.to_string(),
            namespace,
            id,
            uri: uri.to_string(),
            dataset,
            read_consistency_interval,
            namespace_client,
            pushdown_operations,
        })
    }

    /// Set the namespace client for server-side query execution.
    ///
    /// When set, queries will be executed on the namespace server instead of locally.
    pub fn with_namespace_client(mut self, namespace_client: Arc<dyn LanceNamespace>) -> Self {
        self.namespace_client = Some(namespace_client);
        self
    }

    /// Build a sibling `NativeTable` with the same identity but a different
    /// (independent) dataset wrapper — used to hand out branch-scoped handles.
    fn with_dataset(&self, dataset: dataset::DatasetConsistencyWrapper) -> Self {
        Self {
            name: self.name.clone(),
            namespace: self.namespace.clone(),
            id: self.id.clone(),
            uri: self.uri.clone(),
            dataset,
            read_consistency_interval: self.read_consistency_interval,
            namespace_client: self.namespace_client.clone(),
            pushdown_operations: self.pushdown_operations.clone(),
        }
    }

    fn validate_branch_name(name: &str, field: &str) -> Result<()> {
        if name.is_empty() {
            return Err(Error::InvalidInput {
                message: format!("{field} must be a non-empty string"),
            });
        }
        Ok(())
    }

    /// Opens an existing Table using a namespace client.
    ///
    /// This method uses `DatasetBuilder::from_namespace` to open the table, which
    /// automatically fetches the table location and storage options from the namespace.
    /// This eliminates the need to pre-fetch and merge storage options before opening.
    ///
    /// # Arguments
    ///
    /// * `namespace_client` - The namespace client to use for fetching table metadata
    /// * `name` - The table name
    /// * `namespace` - The namespace path (e.g., vec!["parent", "child"])
    /// * `write_store_wrapper` - Optional wrapper for the object store on write path
    /// * `params` - Optional read parameters
    /// * `read_consistency_interval` - Optional interval for read consistency
    /// * `pushdown_operations` - Operations to push down to the namespace server.
    ///   When `QueryTable` is included, queries will be executed on the namespace server.
    /// * `session` - Optional session for object stores and caching
    ///
    /// # Returns
    ///
    /// * A [NativeTable] object.
    #[allow(clippy::too_many_arguments)]
    pub async fn open_from_namespace(
        namespace_client: Arc<dyn LanceNamespace>,
        name: &str,
        namespace: Vec<String>,
        write_store_wrapper: Option<Arc<dyn WrappingObjectStore>>,
        params: Option<ReadParams>,
        read_consistency_interval: Option<std::time::Duration>,
        pushdown_operations: HashSet<NamespaceClientPushdownOperation>,
        session: Option<Arc<lance::session::Session>>,
    ) -> Result<Self> {
        let mut params = params.unwrap_or_default();

        // Set the session in read params
        if let Some(sess) = session {
            params.session(sess);
        }

        // patch the params if we have a write store wrapper
        let params = match write_store_wrapper.clone() {
            Some(wrapper) => params.patch_with_store_wrapper(wrapper)?,
            None => params,
        };

        // Build table_id from namespace + name
        let mut table_id = namespace.clone();
        table_id.push(name.to_string());

        // Use DatasetBuilder::from_namespace which automatically fetches location
        // and storage options from the namespace
        let builder = DatasetBuilder::from_namespace(namespace_client.clone(), table_id)
            .await
            .map_err(|e| map_namespace_lance_error(e, name))?;

        let dataset = builder
            .with_read_params(params)
            .load()
            .await
            .map_err(|e| match e {
                lance::Error::DatasetNotFound { .. } => Error::TableNotFound {
                    name: name.to_string(),
                    source: Box::new(e),
                },
                e => e.into(),
            })?;

        let uri = dataset.uri().to_string();
        let dataset = DatasetConsistencyWrapper::new_latest(dataset, read_consistency_interval);
        let id = Self::build_id(&namespace, name);

        let stored_namespace_client =
            if pushdown_operations.contains(&NamespaceClientPushdownOperation::QueryTable) {
                Some(namespace_client)
            } else {
                None
            };

        Ok(Self {
            name: name.to_string(),
            namespace,
            id,
            uri,
            dataset,
            read_consistency_interval,
            namespace_client: stored_namespace_client,
            pushdown_operations,
        })
    }

    fn get_table_name(uri: &str) -> Result<String> {
        let path = Path::new(uri);
        let name = path
            .file_stem()
            .ok_or(Error::TableNotFound {
                name: uri.to_string(),
                source: format!("Could not extract table name from URI: '{}'", uri).into(),
            })?
            .to_str()
            .ok_or(Error::InvalidTableName {
                name: uri.to_string(),
                reason: "Table name is not valid URL".to_string(),
            })?;
        Ok(name.to_string())
    }

    fn build_id(namespace: &[String], name: &str) -> String {
        if namespace.is_empty() {
            name.to_string()
        } else {
            let mut parts = namespace.to_vec();
            parts.push(name.to_string());
            parts.join("$")
        }
    }

    /// Creates a new Table
    ///
    /// # Arguments
    ///
    /// * `uri` - The URI to the table. When namespace is not empty, the caller must
    ///   provide an explicit URI (location) rather than deriving it from the table name.
    /// * `name` The Table name
    /// * `namespace` - The namespace path. When non-empty, an explicit URI must be provided.
    /// * `batches` RecordBatch to be saved in the database.
    /// * `params` - Write parameters.
    /// * `namespace_client` - Optional namespace client for namespace operations
    /// * `pushdown_operations` - Operations to push down to the namespace server
    ///
    /// # Returns
    ///
    /// * A [TableImpl] object.
    #[allow(clippy::too_many_arguments)]
    pub async fn create(
        uri: &str,
        name: &str,
        namespace: Vec<String>,
        batches: impl StreamingWriteSource,
        write_store_wrapper: Option<Arc<dyn WrappingObjectStore>>,
        params: Option<WriteParams>,
        read_consistency_interval: Option<std::time::Duration>,
        namespace_client: Option<Arc<dyn LanceNamespace>>,
        pushdown_operations: HashSet<NamespaceClientPushdownOperation>,
    ) -> Result<Self> {
        // Default params uses format v1.
        let params = params.unwrap_or(WriteParams {
            ..Default::default()
        });
        // patch the params if we have a write store wrapper
        let params = match write_store_wrapper.clone() {
            Some(wrapper) => params.patch_with_store_wrapper(wrapper)?,
            None => params,
        };

        let insert_builder = InsertBuilder::new(uri).with_params(&params);
        let dataset = insert_builder
            .execute_stream(batches)
            .await
            .map_err(|e| match e {
                lance::Error::DatasetAlreadyExists { .. } => Error::TableAlreadyExists {
                    name: name.to_string(),
                },
                e => e.into(),
            })?;

        let id = Self::build_id(&namespace, name);

        Ok(Self {
            name: name.to_string(),
            namespace,
            id,
            uri: uri.to_string(),
            dataset: DatasetConsistencyWrapper::new_latest(dataset, read_consistency_interval),
            read_consistency_interval,
            namespace_client,
            pushdown_operations,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn create_empty(
        uri: &str,
        name: &str,
        namespace: Vec<String>,
        schema: SchemaRef,
        write_store_wrapper: Option<Arc<dyn WrappingObjectStore>>,
        params: Option<WriteParams>,
        read_consistency_interval: Option<std::time::Duration>,
        namespace_client: Option<Arc<dyn LanceNamespace>>,
        pushdown_operations: HashSet<NamespaceClientPushdownOperation>,
    ) -> Result<Self> {
        let data: Box<dyn Scannable> = Box::new(RecordBatch::new_empty(schema));
        Self::create(
            uri,
            name,
            namespace,
            data,
            write_store_wrapper,
            params,
            read_consistency_interval,
            namespace_client,
            pushdown_operations,
        )
        .await
    }

    /// Creates a new Table using a namespace client for storage options.
    ///
    /// This method sets up a `StorageOptionsProvider` from the namespace client,
    /// enabling automatic credential refresh for cloud storage. The namespace
    /// is used for:
    /// 1. Setting up storage options provider for credential vending
    /// 2. Optionally enabling server-side query execution
    ///
    /// # Arguments
    ///
    /// * `namespace_client` - The namespace client to use for storage options
    /// * `uri` - The URI to the table (obtained from create_empty_table response)
    /// * `name` - The table name
    /// * `namespace` - The namespace path (e.g., vec!["parent", "child"])
    /// * `batches` - RecordBatch to be saved in the database
    /// * `write_store_wrapper` - Optional wrapper for the object store on write path
    /// * `params` - Optional write parameters
    /// * `read_consistency_interval` - Optional interval for read consistency
    /// * `pushdown_operations` - Operations to push down to the namespace server
    ///
    /// # Returns
    ///
    /// * A [NativeTable] object.
    #[allow(clippy::too_many_arguments)]
    pub async fn create_from_namespace(
        namespace_client: Arc<dyn LanceNamespace>,
        uri: &str,
        name: &str,
        namespace: Vec<String>,
        batches: impl StreamingWriteSource,
        write_store_wrapper: Option<Arc<dyn WrappingObjectStore>>,
        params: Option<WriteParams>,
        read_consistency_interval: Option<std::time::Duration>,
        pushdown_operations: HashSet<NamespaceClientPushdownOperation>,
        session: Option<Arc<lance::session::Session>>,
    ) -> Result<Self> {
        // Build table_id from namespace + name for the storage options provider
        let mut table_id = namespace.clone();
        table_id.push(name.to_string());

        // Set up storage options provider from namespace
        let storage_options_provider = Arc::new(LanceNamespaceStorageOptionsProvider::new(
            namespace_client.clone(),
            table_id,
        ));

        // Start with provided params or defaults
        let mut params = params.unwrap_or_default();

        // Set the session in write params
        if let Some(sess) = session {
            params.session = Some(sess);
        }

        // Ensure store_params exists and set the storage options provider
        let store_params = params
            .store_params
            .get_or_insert_with(ObjectStoreParams::default);
        let accessor = match store_params.storage_options().cloned() {
            Some(options) => {
                StorageOptionsAccessor::with_initial_and_provider(options, storage_options_provider)
            }
            None => StorageOptionsAccessor::with_provider(storage_options_provider),
        };
        store_params.storage_options_accessor = Some(Arc::new(accessor));

        // Patch the params if we have a write store wrapper
        let params = match write_store_wrapper.clone() {
            Some(wrapper) => params.patch_with_store_wrapper(wrapper)?,
            None => params,
        };

        let insert_builder = InsertBuilder::new(uri).with_params(&params);
        let dataset = insert_builder
            .execute_stream(batches)
            .await
            .map_err(|e| match e {
                lance::Error::DatasetAlreadyExists { .. } => Error::TableAlreadyExists {
                    name: name.to_string(),
                },
                e => e.into(),
            })?;

        let id = Self::build_id(&namespace, name);

        let stored_namespace_client =
            if pushdown_operations.contains(&NamespaceClientPushdownOperation::QueryTable) {
                Some(namespace_client)
            } else {
                None
            };

        Ok(Self {
            name: name.to_string(),
            namespace,
            id,
            uri: uri.to_string(),
            dataset: DatasetConsistencyWrapper::new_latest(dataset, read_consistency_interval),
            read_consistency_interval,
            namespace_client: stored_namespace_client,
            pushdown_operations,
        })
    }

    /// Merge new data into this table.
    pub async fn merge(
        &mut self,
        batches: impl RecordBatchReader + Send + 'static,
        left_on: &str,
        right_on: &str,
    ) -> Result<()> {
        self.dataset.ensure_mutable()?;
        let mut dataset = (*self.dataset.get().await?).clone();
        dataset.merge(batches, left_on, right_on).await?;
        self.dataset.update(dataset);
        Ok(())
    }

    // TODO: why are these individual methods and not some single "get_stats" method?
    pub async fn count_fragments(&self) -> Result<usize> {
        Ok(self.dataset.get().await?.count_fragments())
    }

    pub async fn count_deleted_rows(&self) -> Result<usize> {
        Ok(self.dataset.get().await?.count_deleted_rows().await?)
    }

    pub async fn num_small_files(&self, max_rows_per_group: usize) -> Result<usize> {
        Ok(self
            .dataset
            .get()
            .await?
            .num_small_files(max_rows_per_group)
            .await)
    }

    pub async fn load_indices(&self) -> Result<Vec<VectorIndex>> {
        let dataset = self.dataset.get().await?;
        let mf = dataset.manifest();
        let indices = dataset.load_indices().await?;
        Ok(indices
            .iter()
            .map(|i| VectorIndex::new_from_format(mf, i))
            .collect())
    }

    // Helper to validate index type compatibility with field data type
    fn validate_index_type(
        field: &Field,
        index_name: &str,
        supported_fn: impl Fn(&DataType) -> bool,
    ) -> Result<()> {
        if !supported_fn(field.data_type()) {
            return Err(Error::Schema {
                message: format!(
                    "A {} index cannot be created on the field `{}` which has data type {}",
                    index_name,
                    field.name(),
                    field.data_type()
                ),
            });
        }
        Ok(())
    }

    // Helper to build IVF params honoring table options.
    fn build_ivf_params(
        num_partitions: Option<u32>,
        target_partition_size: Option<u32>,
        sample_rate: u32,
        max_iterations: u32,
    ) -> IvfBuildParams {
        let mut ivf_params = match (num_partitions, target_partition_size) {
            (Some(num_partitions), _) => IvfBuildParams::new(num_partitions as usize),
            (None, Some(target_partition_size)) => {
                IvfBuildParams::with_target_partition_size(target_partition_size as usize)
            }
            (None, None) => IvfBuildParams::default(),
        };
        ivf_params.sample_rate = sample_rate as usize;
        ivf_params.max_iters = max_iterations as usize;
        ivf_params
    }

    // Helper to get num_sub_vectors with default calculation
    fn get_num_sub_vectors(provided: Option<u32>, dim: u32, num_bits: Option<u32>) -> u32 {
        if let Some(provided) = provided {
            return provided;
        }
        let suggested = suggested_num_sub_vectors(dim);
        if num_bits.is_some_and(|num_bits| num_bits == 4) && !suggested.is_multiple_of(2) {
            // num_sub_vectors must be even when 4 bits are used
            suggested + 1
        } else {
            suggested
        }
    }

    // Helper to extract vector dimension from field
    fn get_vector_dimension(field: &Field) -> Result<u32> {
        match field.data_type() {
            arrow_schema::DataType::FixedSizeList(_, n) => Ok(*n as u32),
            _ => Ok(infer_vector_dim(field.data_type())? as u32),
        }
    }

    fn resolve_index_field(
        schema: &lance_core::datatypes::Schema,
        column: &str,
    ) -> Result<(String, Field)> {
        lance_core::datatypes::parse_field_path(column).map_err(|e| Error::InvalidInput {
            message: format!("Invalid field path `{}`: {}", column, e),
        })?;

        let field_path = schema
            .resolve_case_insensitive(column)
            .ok_or_else(|| Error::Schema {
                message: format!(
                    "Field path `{}` not found in schema. Available field paths: {}",
                    column,
                    schema.field_paths().join(", ")
                ),
            })?;
        let field = field_path.last().expect("field path should be non-empty");
        let path_segments = field_path
            .iter()
            .map(|field| field.name.as_str())
            .collect::<Vec<_>>();
        let canonical_path = lance_core::datatypes::format_field_path(&path_segments);

        Ok((canonical_path, Field::from(*field)))
    }

    // Convert LanceDB Index to Lance IndexParams
    async fn make_index_params(
        &self,
        field: &Field,
        index_opts: Index,
    ) -> Result<Box<dyn lance::index::IndexParams>> {
        match index_opts {
            Index::Auto => {
                if supported_vector_data_type(field.data_type()) {
                    // Use IvfPq as the default for auto vector indices
                    let dim = Self::get_vector_dimension(field)?;
                    let ivf_params = lance_index::vector::ivf::IvfBuildParams::default();
                    let num_sub_vectors = Self::get_num_sub_vectors(None, dim, None);
                    let pq_params =
                        lance_index::vector::pq::PQBuildParams::new(num_sub_vectors as usize, 8);
                    let lance_idx_params =
                        lance::index::vector::VectorIndexParams::with_ivf_pq_params(
                            lance_linalg::distance::MetricType::L2,
                            ivf_params,
                            pq_params,
                        );
                    Ok(Box::new(lance_idx_params))
                } else if supported_btree_data_type(field.data_type()) {
                    Ok(Box::new(ScalarIndexParams::for_builtin(
                        BuiltinIndexType::BTree,
                    )))
                } else {
                    Err(Error::InvalidInput {
                        message: format!(
                            "there are no indices supported for the field `{}` with the data type {}",
                            field.name(),
                            field.data_type()
                        ),
                    })?
                }
            }
            Index::BTree(_) => {
                Self::validate_index_type(field, "BTree", supported_btree_data_type)?;
                Ok(Box::new(ScalarIndexParams::for_builtin(
                    BuiltinIndexType::BTree,
                )))
            }
            Index::Bitmap(_) => {
                Self::validate_index_type(field, "Bitmap", supported_bitmap_data_type)?;
                Ok(Box::new(ScalarIndexParams::for_builtin(
                    BuiltinIndexType::Bitmap,
                )))
            }
            Index::LabelList(_) => {
                Self::validate_index_type(field, "LabelList", supported_label_list_data_type)?;
                Ok(Box::new(ScalarIndexParams::for_builtin(
                    BuiltinIndexType::LabelList,
                )))
            }
            Index::Fm(_) => {
                Self::validate_index_type(field, "FM", supported_fm_data_type)?;
                Ok(Box::new(ScalarIndexParams::for_builtin(
                    BuiltinIndexType::Fm,
                )))
            }
            Index::FTS(fts_opts) => {
                Self::validate_index_type(field, "FTS", supported_fts_data_type)?;
                Ok(Box::new(fts_opts))
            }
            Index::IvfFlat(index) => {
                Self::validate_index_type(field, "IVF Flat", supported_vector_data_type)?;
                let ivf_params = Self::build_ivf_params(
                    index.num_partitions,
                    index.target_partition_size,
                    index.sample_rate,
                    index.max_iterations,
                );
                let lance_idx_params =
                    VectorIndexParams::with_ivf_flat_params(index.distance_type.into(), ivf_params);
                Ok(Box::new(lance_idx_params))
            }
            Index::IvfSq(index) => {
                Self::validate_index_type(field, "IVF SQ", supported_vector_data_type)?;
                let ivf_params = Self::build_ivf_params(
                    index.num_partitions,
                    index.target_partition_size,
                    index.sample_rate,
                    index.max_iterations,
                );
                let sq_params = SQBuildParams {
                    sample_rate: index.sample_rate as usize,
                    ..Default::default()
                };
                let lance_idx_params = VectorIndexParams::with_ivf_sq_params(
                    index.distance_type.into(),
                    ivf_params,
                    sq_params,
                );
                Ok(Box::new(lance_idx_params))
            }
            Index::IvfPq(index) => {
                Self::validate_index_type(field, "IVF PQ", supported_vector_data_type)?;
                let dim = Self::get_vector_dimension(field)?;
                let ivf_params = Self::build_ivf_params(
                    index.num_partitions,
                    index.target_partition_size,
                    index.sample_rate,
                    index.max_iterations,
                );
                let num_sub_vectors =
                    Self::get_num_sub_vectors(index.num_sub_vectors, dim, index.num_bits);
                let num_bits = index.num_bits.unwrap_or(8) as usize;
                let mut pq_params = PQBuildParams::new(num_sub_vectors as usize, num_bits);
                pq_params.max_iters = index.max_iterations as usize;
                let lance_idx_params = VectorIndexParams::with_ivf_pq_params(
                    index.distance_type.into(),
                    ivf_params,
                    pq_params,
                );
                Ok(Box::new(lance_idx_params))
            }
            Index::IvfRq(index) => {
                Self::validate_index_type(field, "IVF RQ", supported_vector_data_type)?;
                let ivf_params = Self::build_ivf_params(
                    index.num_partitions,
                    index.target_partition_size,
                    index.sample_rate,
                    index.max_iterations,
                );
                let rq_params = RQBuildParams::new(index.num_bits.unwrap_or(1) as u8);
                let lance_idx_params = VectorIndexParams::with_ivf_rq_params(
                    index.distance_type.into(),
                    ivf_params,
                    rq_params,
                );
                Ok(Box::new(lance_idx_params))
            }
            Index::IvfHnswPq(index) => {
                Self::validate_index_type(field, "IVF HNSW PQ", supported_vector_data_type)?;
                let dim = Self::get_vector_dimension(field)?;
                let ivf_params = Self::build_ivf_params(
                    index.num_partitions,
                    index.target_partition_size,
                    index.sample_rate,
                    index.max_iterations,
                );
                let num_sub_vectors =
                    Self::get_num_sub_vectors(index.num_sub_vectors, dim, index.num_bits);
                let hnsw_params = HnswBuildParams::default()
                    .num_edges(index.m as usize)
                    .ef_construction(index.ef_construction as usize);
                let pq_params = PQBuildParams::new(
                    num_sub_vectors as usize,
                    index.num_bits.unwrap_or(8) as usize,
                );
                let lance_idx_params = VectorIndexParams::with_ivf_hnsw_pq_params(
                    index.distance_type.into(),
                    ivf_params,
                    hnsw_params,
                    pq_params,
                );
                Ok(Box::new(lance_idx_params))
            }
            Index::IvfHnswSq(index) => {
                Self::validate_index_type(field, "IVF HNSW SQ", supported_vector_data_type)?;
                let ivf_params = Self::build_ivf_params(
                    index.num_partitions,
                    index.target_partition_size,
                    index.sample_rate,
                    index.max_iterations,
                );
                let hnsw_params = HnswBuildParams::default()
                    .num_edges(index.m as usize)
                    .ef_construction(index.ef_construction as usize);
                let sq_params = SQBuildParams {
                    sample_rate: index.sample_rate as usize,
                    ..Default::default()
                };
                let lance_idx_params = VectorIndexParams::with_ivf_hnsw_sq_params(
                    index.distance_type.into(),
                    ivf_params,
                    hnsw_params,
                    sq_params,
                );
                Ok(Box::new(lance_idx_params))
            }
            Index::IvfHnswFlat(index) => {
                Self::validate_index_type(field, "IVF HNSW FLAT", supported_vector_data_type)?;
                let ivf_params = Self::build_ivf_params(
                    index.num_partitions,
                    index.target_partition_size,
                    index.sample_rate,
                    index.max_iterations,
                );
                let hnsw_params = HnswBuildParams::default()
                    .num_edges(index.m as usize)
                    .ef_construction(index.ef_construction as usize);
                let lance_idx_params = VectorIndexParams::ivf_hnsw(
                    index.distance_type.into(),
                    ivf_params,
                    hnsw_params,
                );
                Ok(Box::new(lance_idx_params))
            }
        }
    }

    // Helper method to get the correct IndexType based on the Index variant and field data type
    fn get_index_type_for_field(&self, field: &Field, index: &Index) -> IndexType {
        match index {
            Index::Auto => {
                if supported_vector_data_type(field.data_type()) {
                    IndexType::Vector
                } else if supported_btree_data_type(field.data_type()) {
                    IndexType::BTree
                } else {
                    // This should not happen since make_index_params would have failed
                    IndexType::BTree
                }
            }
            Index::BTree(_) => IndexType::BTree,
            Index::Bitmap(_) => IndexType::Bitmap,
            Index::LabelList(_) => IndexType::LabelList,
            Index::Fm(_) => IndexType::Fm,
            Index::FTS(_) => IndexType::Inverted,
            Index::IvfFlat(_)
            | Index::IvfSq(_)
            | Index::IvfPq(_)
            | Index::IvfRq(_)
            | Index::IvfHnswPq(_)
            | Index::IvfHnswSq(_)
            | Index::IvfHnswFlat(_) => IndexType::Vector,
        }
    }

    /// Check whether the table uses V2 manifest paths.
    ///
    /// See [Self::migrate_manifest_paths_v2] and [ManifestNamingScheme] for
    /// more information.
    pub async fn uses_v2_manifest_paths(&self) -> Result<bool> {
        let dataset = self.dataset.get().await?;
        Ok(dataset.manifest_location().naming_scheme == ManifestNamingScheme::V2)
    }

    /// Migrate the table to use the new manifest path scheme.
    ///
    /// This function will rename all V1 manifests to V2 manifest paths.
    /// These paths provide more efficient opening of datasets with many versions
    /// on object stores.
    ///
    /// This function is idempotent, and can be run multiple times without
    /// changing the state of the object store.
    ///
    /// However, it should not be run while other concurrent operations are happening.
    /// And it should also run until completion before resuming other operations.
    ///
    /// You can use [Self::uses_v2_manifest_paths] to check if the table is already
    /// using V2 manifest paths.
    pub async fn migrate_manifest_paths_v2(&self) -> Result<()> {
        self.dataset.ensure_mutable()?;
        let mut dataset = (*self.dataset.get().await?).clone();
        dataset.migrate_manifest_paths_v2().await?;
        self.dataset.update(dataset);
        Ok(())
    }

    /// Get the table manifest
    pub async fn manifest(&self) -> Result<Manifest> {
        let dataset = self.dataset.get().await?;
        Ok(dataset.manifest().clone())
    }

    /// Update key-value pairs in config.
    pub async fn update_config(
        &self,
        upsert_values: impl IntoIterator<Item = (String, String)>,
    ) -> Result<()> {
        self.dataset.ensure_mutable()?;
        let mut dataset = (*self.dataset.get().await?).clone();
        dataset.update_config(upsert_values).await?;
        self.dataset.update(dataset);
        Ok(())
    }

    /// Delete keys from the config
    pub async fn delete_config_keys(&self, delete_keys: &[&str]) -> Result<()> {
        self.dataset.ensure_mutable()?;
        let mut dataset = (*self.dataset.get().await?).clone();
        // TODO: update this when we implement metadata APIs
        #[allow(deprecated)]
        dataset.delete_config_keys(delete_keys).await?;
        self.dataset.update(dataset);
        Ok(())
    }

    /// Update schema metadata
    pub async fn replace_schema_metadata(
        &self,
        upsert_values: impl IntoIterator<Item = (String, String)>,
    ) -> Result<()> {
        self.dataset.ensure_mutable()?;
        let mut dataset = (*self.dataset.get().await?).clone();
        // TODO: update this when we implement metadata APIs
        #[allow(deprecated)]
        dataset.replace_schema_metadata(upsert_values).await?;
        self.dataset.update(dataset);
        Ok(())
    }

    /// Update field metadata
    ///
    /// # Arguments:
    /// * `new_values` - An iterator of tuples where the first element is the
    ///   field id and the second element is a hashmap of metadata key-value
    ///   pairs.
    ///
    #[deprecated(since = "0.33.1", note = "Use `update_field_metadata` instead")]
    pub async fn replace_field_metadata(
        &self,
        new_values: impl IntoIterator<Item = (u32, HashMap<String, String>)>,
    ) -> Result<()> {
        self.dataset.ensure_mutable()?;
        let mut dataset = (*self.dataset.get().await?).clone();
        dataset.replace_field_metadata(new_values).await?;
        self.dataset.update(dataset);
        Ok(())
    }
}

#[async_trait::async_trait]
impl BaseTable for NativeTable {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        self.name.as_str()
    }

    fn namespace(&self) -> &[String] {
        &self.namespace
    }

    fn id(&self) -> &str {
        &self.id
    }

    async fn version(&self) -> Result<u64> {
        Ok(self.dataset.get().await?.version().version)
    }

    async fn checkout(&self, version: u64) -> Result<()> {
        self.dataset.as_time_travel(version).await
    }

    async fn checkout_tag(&self, tag: &str) -> Result<()> {
        self.dataset.as_time_travel(tag).await
    }

    async fn checkout_latest(&self) -> Result<()> {
        self.dataset.as_latest().await?;
        self.dataset.reload().await
    }

    async fn create_branch(
        &self,
        name: &str,
        from: lance::dataset::refs::Ref,
    ) -> Result<Arc<dyn BaseTable>> {
        Self::validate_branch_name(name, "branch name")?;
        if let lance::dataset::refs::Ref::Version(Some(from_branch), _) = &from {
            Self::validate_branch_name(from_branch, "from_ref")?;
        }
        let mut ds = (*self.dataset.get().await?).clone();
        let branch_ds = ds.create_branch(name, from, None).await?;
        let dataset = dataset::DatasetConsistencyWrapper::new_latest(
            branch_ds,
            self.read_consistency_interval,
        );
        Ok(Arc::new(self.with_dataset(dataset)))
    }

    async fn checkout_branch(&self, name: &str) -> Result<Arc<dyn BaseTable>> {
        Self::validate_branch_name(name, "branch name")?;
        let branch_ds = self.dataset.get().await?.checkout_branch(name).await?;
        let dataset = dataset::DatasetConsistencyWrapper::new_latest(
            branch_ds,
            self.read_consistency_interval,
        );
        Ok(Arc::new(self.with_dataset(dataset)))
    }

    async fn checkout_branch_version(
        &self,
        name: &str,
        version: Option<u64>,
    ) -> Result<Arc<dyn BaseTable>> {
        let Some(version) = version else {
            return self.checkout_branch(name).await;
        };
        Self::validate_branch_name(name, "branch name")?;
        // Resolve (branch, version) in a single manifest read.
        let branch_ds = self
            .dataset
            .get()
            .await?
            .checkout_version((name, version))
            .await?;
        let dataset = dataset::DatasetConsistencyWrapper::new_time_travel(
            branch_ds,
            self.read_consistency_interval,
        );
        Ok(Arc::new(self.with_dataset(dataset)))
    }

    async fn list_branches(&self) -> Result<HashMap<String, BranchContents>> {
        Ok(self.dataset.get().await?.list_branches().await?)
    }

    async fn delete_branch(&self, name: &str) -> Result<()> {
        Self::validate_branch_name(name, "branch name")?;
        let mut ds = (*self.dataset.get().await?).clone();
        ds.delete_branch(name).await?;
        Ok(())
    }

    fn current_branch(&self) -> Option<String> {
        self.dataset.current_branch()
    }

    async fn list_versions(&self) -> Result<Vec<Version>> {
        Ok(self.dataset.get().await?.versions().await?)
    }

    async fn restore(&self) -> Result<()> {
        let version = self
            .dataset
            .time_travel_version()
            .ok_or_else(|| Error::InvalidInput {
                message: "you must run checkout before running restore".to_string(),
            })?;
        {
            // restore is the only "write" operation allowed in time travel mode
            let mut dataset = (*self.dataset.get().await?).clone();
            debug_assert_eq!(dataset.version().version, version);
            dataset.restore().await?;
        }
        self.dataset.as_latest().await?;
        Ok(())
    }

    async fn schema(&self) -> Result<SchemaRef> {
        let lance_schema = self.dataset.get().await?.schema().clone();
        Ok(Arc::new(Schema::from(&lance_schema)))
    }

    async fn table_definition(&self) -> Result<TableDefinition> {
        let schema = self.schema().await?;
        TableDefinition::try_from_rich_schema(schema)
    }

    async fn count_rows(&self, filter: Option<Filter>) -> Result<usize> {
        let dataset = self.dataset.get().await?;
        match filter {
            None => Ok(dataset.count_rows(None).await?),
            Some(Filter::Sql(sql)) => Ok(dataset.count_rows(Some(sql)).await?),
            Some(Filter::Datafusion(_)) => Err(Error::NotSupported {
                message: "Datafusion filters are not yet supported".to_string(),
            }),
        }
    }

    async fn add(&self, mut add: AddDataBuilder) -> Result<AddResult> {
        let table_def = self.table_definition().await?;

        self.dataset.ensure_mutable()?;
        let ds_wrapper = self.dataset.clone();
        let ds = self.dataset.get().await?;

        let table_schema = Schema::from(&ds.schema().clone());

        let num_partitions = if let Some(parallelism) = add.write_parallelism {
            parallelism
        } else {
            // Peek at the first batch to estimate a good partition count for
            // write parallelism.
            let mut peeked = PeekedScannable::new(add.data);
            let n = if let Some(first_batch) = peeked.peek().await {
                let max_partitions = lance_core::utils::tokio::get_num_compute_intensive_cpus();
                estimate_write_partitions(
                    first_batch.get_array_memory_size(),
                    first_batch.num_rows(),
                    peeked.num_rows(),
                    max_partitions,
                )
            } else {
                1
            };
            add.data = Box::new(peeked);
            n
        };

        let output = add.into_plan(&table_schema, &table_def)?;

        let lance_params = output
            .write_options
            .lance_write_params
            .unwrap_or(WriteParams {
                mode: match output.mode {
                    AddDataMode::Append => WriteMode::Append,
                    AddDataMode::Overwrite => WriteMode::Overwrite,
                },
                ..Default::default()
            });

        // Repartition for write parallelism if beneficial.
        let plan = if num_partitions > 1 {
            Arc::new(
                datafusion_physical_plan::repartition::RepartitionExec::try_new(
                    output.plan,
                    datafusion_physical_plan::Partitioning::RoundRobinBatch(num_partitions),
                )?,
            ) as Arc<dyn ExecutionPlan>
        } else {
            output.plan
        };

        let insert_exec = Arc::new(InsertExec::new(ds_wrapper.clone(), ds, plan, lance_params));

        let tracker_for_tasks = output.tracker.clone();
        if let Some(ref t) = tracker_for_tasks {
            t.set_total_tasks(num_partitions);
        }
        let _finish = write_progress::FinishOnDrop(output.tracker);

        // Execute all partitions in parallel.
        let task_ctx = Arc::new(TaskContext::default());
        let handles = FuturesUnordered::new();
        for partition in 0..num_partitions {
            let exec = insert_exec.clone();
            let ctx = task_ctx.clone();
            let tracker = tracker_for_tasks.clone();
            handles.push(tokio::spawn(async move {
                let _guard = tracker.as_ref().map(|t| t.track_task());
                let mut stream = exec
                    .execute(partition, ctx)
                    .map_err(|e| -> Error { e.into() })?;
                while let Some(batch) = stream.next().await {
                    batch.map_err(|e| -> Error { e.into() })?;
                }
                Ok::<_, Error>(())
            }));
        }
        for handle in handles {
            handle.await.map_err(|e| Error::Runtime {
                message: format!("Insert task panicked: {}", e),
            })??;
        }

        let version = ds_wrapper.get().await?.manifest().version;
        Ok(AddResult { version })
    }

    async fn create_index(&self, opts: IndexBuilder) -> Result<()> {
        if opts.columns.len() != 1 {
            return Err(Error::Schema {
                message: "Multi-column (composite) indices are not yet supported".to_string(),
            });
        }
        self.dataset.ensure_mutable()?;
        let mut dataset = (*self.dataset.get().await?).clone();
        let (column, field) = Self::resolve_index_field(dataset.schema(), &opts.columns[0])?;

        let lance_idx_params = self.make_index_params(&field, opts.index.clone()).await?;
        let index_type = self.get_index_type_for_field(&field, &opts.index);
        let columns = [column.as_str()];
        let mut builder = dataset
            .create_index_builder(&columns, index_type, lance_idx_params.as_ref())
            .train(opts.train)
            .replace(opts.replace);

        if let Some(name) = opts.name {
            builder = builder.name(name);
        }
        builder.await?;
        self.dataset.update(dataset);
        Ok(())
    }

    async fn drop_index(&self, index_name: &str) -> Result<()> {
        self.dataset.ensure_mutable()?;
        let mut dataset = (*self.dataset.get().await?).clone();
        dataset.drop_index(index_name).await?;
        self.dataset.update(dataset);
        Ok(())
    }

    async fn prewarm_index(&self, index_name: &str) -> Result<()> {
        let dataset = self.dataset.get().await?;
        Ok(dataset.prewarm_index(index_name).await?)
    }

    async fn prewarm_data(&self, _columns: Option<Vec<String>>) -> Result<()> {
        Err(Error::NotSupported {
            message: "prewarm_data is currently only supported on remote tables.".into(),
        })
    }

    async fn update(&self, update: UpdateBuilder) -> Result<UpdateResult> {
        // Delegate to the submodule implementation
        update::execute_update(self, update).await
    }

    async fn create_plan(
        &self,
        query: &AnyQuery,
        options: QueryExecutionOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        query::create_plan(self, query, options).await
    }

    async fn query(
        &self,
        query: &AnyQuery,
        options: QueryExecutionOptions,
    ) -> Result<DatasetRecordBatchStream> {
        query::execute_query(self, query, options).await
    }

    async fn analyze_plan(
        &self,
        query: &AnyQuery,
        options: QueryExecutionOptions,
    ) -> Result<String> {
        query::analyze_query_plan(self, query, options).await
    }

    async fn merge_insert(
        &self,
        params: MergeInsertBuilder,
        new_data: Box<dyn RecordBatchReader + Send>,
    ) -> Result<MergeResult> {
        merge::execute_merge_insert(self, params, new_data).await
    }

    async fn set_unenforced_primary_key(&self, columns: &[&str]) -> Result<()> {
        primary_key::set_unenforced_primary_key(self, columns).await
    }

    async fn set_lsm_write_spec(&self, spec: LsmWriteSpec) -> Result<()> {
        merge::lsm::set_lsm_write_spec(self, spec).await
    }

    async fn unset_lsm_write_spec(&self) -> Result<()> {
        merge::lsm::unset_lsm_write_spec(self).await
    }

    async fn close_lsm_writers(&self) -> Result<()> {
        merge::lsm::close_lsm_writers(self).await
    }

    /// Delete rows from the table
    async fn delete(&self, predicate: Predicate<'_>) -> Result<DeleteResult> {
        delete::execute_delete(self, predicate).await
    }

    async fn tags(&self) -> Result<Box<dyn Tags + '_>> {
        Ok(Box::new(NativeTags {
            dataset: self.dataset.clone(),
        }))
    }

    async fn optimize(&self, action: OptimizeAction) -> Result<OptimizeStats> {
        // Delegate to the submodule implementation
        optimize::execute_optimize(self, action).await
    }

    async fn add_columns(
        &self,
        transforms: NewColumnTransform,
        read_columns: Option<Vec<String>>,
    ) -> Result<AddColumnsResult> {
        schema_evolution::execute_add_columns(self, transforms, read_columns).await
    }

    async fn alter_columns(&self, alterations: &[ColumnAlteration]) -> Result<AlterColumnsResult> {
        schema_evolution::execute_alter_columns(self, alterations).await
    }

    async fn update_field_metadata(
        &self,
        updates: &[FieldMetadataUpdate],
    ) -> Result<UpdateFieldMetadataResult> {
        schema_evolution::execute_update_field_metadata(self, updates).await
    }

    async fn drop_columns(&self, columns: &[&str]) -> Result<DropColumnsResult> {
        schema_evolution::execute_drop_columns(self, columns).await
    }

    async fn list_indices(&self) -> Result<Vec<IndexConfig>> {
        let dataset = self.dataset.get().await?;
        let indices = dataset
            .describe_indices(None)
            .await?
            .into_iter()
            .filter_map(|idx_desc| {
                let index_type: crate::index::IndexType = match idx_desc.index_type().parse() {
                    Ok(index_type) => index_type,
                    Err(e) => {
                        log::warn!(
                            "Failed to parse index type for index {}: {}",
                            idx_desc.name(),
                            e
                        );
                        return None;
                    }
                };

                let field_ids = idx_desc.field_ids();
                let mut columns = Vec::with_capacity(field_ids.len());
                for field_id in field_ids {
                    let field_path = match dataset.schema().field_path(*field_id as i32) {
                        Ok(field_path) => field_path,
                        Err(e) => {
                            log::warn!(
                                "Failed to resolve field path for index {} field id {}: {}",
                                idx_desc.name(),
                                field_id,
                                e
                            );
                            return None;
                        }
                    };
                    columns.push(field_path);
                }

                Some(IndexConfig {
                    name: idx_desc.name().to_string(),
                    index_type,
                    columns,
                })
            })
            .collect();
        Ok(indices)
    }

    async fn uri(&self) -> Result<String> {
        Ok(self.uri.clone())
    }

    async fn storage_options(&self) -> Option<HashMap<String, String>> {
        self.initial_storage_options().await
    }

    async fn initial_storage_options(&self) -> Option<HashMap<String, String>> {
        self.dataset
            .get()
            .await
            .ok()
            .and_then(|dataset| dataset.initial_storage_options().cloned())
    }

    async fn latest_storage_options(&self) -> Result<Option<HashMap<String, String>>> {
        let dataset = self.dataset.get().await?;
        Ok(dataset.latest_storage_options().await?.map(|o| o.0))
    }

    async fn index_stats(&self, index_name: &str) -> Result<Option<IndexStatistics>> {
        let stats = match self
            .dataset
            .get()
            .await?
            .index_statistics(index_name.as_ref())
            .await
        {
            Ok(stats) => stats,
            Err(lance_core::Error::IndexNotFound { .. }) => return Ok(None),
            Err(e) => return Err(Error::from(e)),
        };

        let mut stats: IndexStatisticsImpl =
            serde_json::from_str(&stats).map_err(|e| Error::InvalidInput {
                message: format!("error deserializing index statistics: {}", e),
            })?;

        let first_index = stats.indices.pop().ok_or_else(|| Error::InvalidInput {
            message: "index statistics is empty".to_string(),
        })?;
        // Index type should be present at one of the levels.
        let index_type =
            stats
                .index_type
                .or(first_index.index_type)
                .ok_or_else(|| Error::InvalidInput {
                    message: "index statistics was missing index type".to_string(),
                })?;
        Ok(Some(IndexStatistics {
            num_indexed_rows: stats.num_indexed_rows,
            num_unindexed_rows: stats.num_unindexed_rows,
            index_type,
            distance_type: first_index.metric_type,
            num_indices: stats.num_indices,
        }))
    }

    /// Poll until the columns are fully indexed. Will return Error::Timeout if the columns
    /// are not fully indexed within the timeout.
    async fn wait_for_index(
        &self,
        index_names: &[&str],
        timeout: std::time::Duration,
    ) -> Result<()> {
        wait_for_index(self, index_names, timeout).await
    }

    async fn stats(&self) -> Result<TableStatistics> {
        let num_rows = self.count_rows(None).await?;
        let num_indices = self.list_indices().await?.len();
        let ds = self.dataset.get().await?;
        let ds_clone = (*ds).clone();
        let ds_stats = Arc::new(ds_clone).calculate_data_stats().await?;
        let total_bytes = ds_stats.fields.iter().map(|f| f.bytes_on_disk).sum::<u64>() as usize;

        let frags = ds.get_fragments();
        let mut sorted_sizes = join_all(
            frags
                .iter()
                .map(|frag| async move { frag.physical_rows().await.unwrap_or(0) }),
        )
        .await;
        sorted_sizes.sort();

        let small_frag_threshold = 100000;
        let num_fragments = sorted_sizes.len();
        let num_small_fragments = sorted_sizes
            .iter()
            .filter(|&&size| size < small_frag_threshold)
            .count();

        let p25 = *sorted_sizes.get(num_fragments / 4).unwrap_or(&0);
        let p50 = *sorted_sizes.get(num_fragments / 2).unwrap_or(&0);
        let p75 = *sorted_sizes.get(num_fragments * 3 / 4).unwrap_or(&0);
        let p99 = *sorted_sizes.get(num_fragments * 99 / 100).unwrap_or(&0);
        let min = sorted_sizes.first().copied().unwrap_or(0);
        let max = sorted_sizes.last().copied().unwrap_or(0);
        let mean = sorted_sizes
            .iter()
            .copied()
            .sum::<usize>()
            .checked_div(num_fragments)
            .unwrap_or(0);

        let frag_stats = FragmentStatistics {
            num_fragments,
            num_small_fragments,
            lengths: FragmentSummaryStats {
                min,
                max,
                mean,
                p25,
                p50,
                p75,
                p99,
            },
        };
        let stats = TableStatistics {
            total_bytes,
            num_rows,
            num_indices,
            fragment_stats: frag_stats,
        };
        Ok(stats)
    }

    async fn create_insert_exec(
        &self,
        input: Arc<dyn datafusion_physical_plan::ExecutionPlan>,
        write_params: WriteParams,
    ) -> Result<Arc<dyn datafusion_physical_plan::ExecutionPlan>> {
        let ds = self.dataset.get().await?;
        let dataset = Arc::new((*ds).clone());
        Ok(Arc::new(datafusion::insert::InsertExec::new(
            self.dataset.clone(),
            dataset,
            input,
            write_params,
        )))
    }
}

#[skip_serializing_none]
#[derive(Debug, Deserialize, PartialEq)]
pub struct TableStatistics {
    /// The total number of bytes in the table
    pub total_bytes: usize,

    /// The number of rows in the table
    pub num_rows: usize,

    /// The number of indices in the table
    pub num_indices: usize,

    /// Statistics on table fragments
    pub fragment_stats: FragmentStatistics,
}

#[skip_serializing_none]
#[derive(Debug, Deserialize, PartialEq)]
pub struct FragmentStatistics {
    /// The number of fragments in the table
    pub num_fragments: usize,

    /// The number of uncompacted fragments in the table
    pub num_small_fragments: usize,

    /// Statistics on the number of rows in the table fragments
    pub lengths: FragmentSummaryStats,
    // todo: add size statistics
    // /// Statistics on the number of bytes in the table fragments
    // sizes: FragmentStats,
}

#[skip_serializing_none]
#[derive(Debug, Deserialize, PartialEq)]
pub struct FragmentSummaryStats {
    pub min: usize,
    pub max: usize,
    pub mean: usize,
    pub p25: usize,
    pub p50: usize,
    pub p75: usize,
    pub p99: usize,
}

#[cfg(test)]
#[allow(deprecated)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    use arrow_array::{
        Array, ArrayRef, BooleanArray, FixedSizeListArray, Int32Array, LargeStringArray,
        RecordBatch, RecordBatchIterator, RecordBatchReader, StringArray, StructArray,
        builder::{LargeListBuilder, ListBuilder, StringBuilder},
    };
    use arrow_array::{BinaryArray, LargeBinaryArray};
    use arrow_data::ArrayDataBuilder;
    use arrow_schema::{DataType, Field, Schema};
    use futures::TryStreamExt;
    use lance::Dataset;
    use lance::io::{ObjectStoreParams, WrappingObjectStore};
    use lance_core::datatypes::LANCE_UNENFORCED_PRIMARY_KEY_POSITION;
    use tempfile::tempdir;

    use super::*;
    use crate::connect;
    use crate::connection::ConnectBuilder;
    use crate::index::scalar::{BTreeIndexBuilder, BitmapIndexBuilder, FmIndexBuilder};
    use crate::index::vector::{IvfHnswPqIndexBuilder, IvfHnswSqIndexBuilder};
    use crate::query::Select;
    use crate::query::{ExecutableQuery, QueryBase};
    use crate::test_utils::connection::new_test_connection;
    use lance_index::scalar::FullTextSearchQuery;
    #[tokio::test]
    async fn test_open() {
        let tmp_dir = tempdir().unwrap();
        let dataset_path = tmp_dir.path().join("test.lance");

        let batch = make_test_batches();
        let reader = RecordBatchIterator::new(vec![Ok(batch.clone())], batch.schema());
        Dataset::write(reader, dataset_path.to_str().unwrap(), None)
            .await
            .unwrap();

        let table = NativeTable::open(dataset_path.to_str().unwrap())
            .await
            .unwrap();

        assert_eq!(table.name, "test")
    }

    #[tokio::test]
    async fn test_open_not_found() {
        let tmp_dir = tempdir().unwrap();
        let uri = tmp_dir.path().to_str().unwrap();
        let table = NativeTable::open(uri).await;
        assert!(matches!(table.unwrap_err(), Error::TableNotFound { .. }));
    }

    #[test]
    #[cfg(not(windows))]
    fn test_object_store_path() {
        use std::path::Path as StdPath;
        let p = StdPath::new("s3://bucket/path/to/file");
        let c = p.join("subfile");
        assert_eq!(c.to_str().unwrap(), "s3://bucket/path/to/file/subfile");
    }

    #[tokio::test]
    async fn test_count_rows() {
        let tmp_dir = tempdir().unwrap();
        let uri = tmp_dir.path().to_str().unwrap();

        let batch = make_test_batches();
        let reader: Box<dyn RecordBatchReader + Send> = Box::new(RecordBatchIterator::new(
            vec![Ok(batch.clone())],
            batch.schema(),
        ));
        let table = NativeTable::create(
            uri,
            "test",
            vec![],
            reader,
            None,
            None,
            None,
            None,
            HashSet::new(),
        )
        .await
        .unwrap();

        assert_eq!(table.count_rows(None).await.unwrap(), 10);
        assert_eq!(
            table
                .count_rows(Some(Filter::Sql("i >= 5".to_string())))
                .await
                .unwrap(),
            5
        );
    }

    #[derive(Default, Debug)]
    struct NoOpCacheWrapper {
        called: AtomicBool,
    }

    impl NoOpCacheWrapper {
        fn called(&self) -> bool {
            self.called.load(Ordering::Relaxed)
        }
    }

    impl WrappingObjectStore for NoOpCacheWrapper {
        fn wrap(
            &self,
            _store_prefix: &str,
            original: Arc<dyn object_store::ObjectStore>,
        ) -> Arc<dyn object_store::ObjectStore> {
            self.called.store(true, Ordering::Relaxed);
            original
        }
    }

    #[tokio::test]
    async fn test_open_table_options() {
        let tmp_dir = tempdir().unwrap();
        let dataset_path = tmp_dir.path().join("test.lance");
        let uri = dataset_path.to_str().unwrap();
        let conn = connect(uri).execute().await.unwrap();

        let batches = make_test_batches();

        conn.create_table("my_table", batches)
            .execute()
            .await
            .unwrap();

        let wrapper = Arc::new(NoOpCacheWrapper::default());

        let object_store_params = ObjectStoreParams {
            object_store_wrapper: Some(wrapper.clone()),
            ..Default::default()
        };
        let param = ReadParams {
            store_options: Some(object_store_params),
            ..Default::default()
        };
        assert!(!wrapper.called());
        conn.open_table("my_table")
            .lance_read_params(param)
            .execute()
            .await
            .unwrap();
        assert!(wrapper.called());
    }

    fn make_test_batches() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("i", DataType::Int32, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from_iter_values(0..10))]).unwrap()
    }

    #[tokio::test]
    async fn test_tags() {
        let tmp_dir = tempdir().unwrap();
        let uri = tmp_dir.path().to_str().unwrap();

        let conn = ConnectBuilder::new(uri)
            .read_consistency_interval(Duration::from_secs(0))
            .execute()
            .await
            .unwrap();
        let table = conn
            .create_table("my_table", some_sample_data())
            .execute()
            .await
            .unwrap();
        assert_eq!(table.version().await.unwrap(), 1);
        table.add(some_sample_data()).execute().await.unwrap();
        assert_eq!(table.version().await.unwrap(), 2);
        let mut tags_manager = table.tags().await.unwrap();
        let tags = tags_manager.list().await.unwrap();
        assert!(tags.is_empty(), "Tags should be empty initially");
        let tag1 = "tag1";
        tags_manager.create(tag1, 1).await.unwrap();
        assert_eq!(tags_manager.get_version(tag1).await.unwrap(), 1);
        let tags = tags_manager.list().await.unwrap();
        assert_eq!(tags.len(), 1);
        assert!(tags.contains_key(tag1));
        assert_eq!(tags.get(tag1).unwrap().version, 1);
        tags_manager.create("tag2", 2).await.unwrap();
        assert_eq!(tags_manager.get_version("tag2").await.unwrap(), 2);
        let tags = tags_manager.list().await.unwrap();
        assert_eq!(tags.len(), 2);
        assert!(tags.contains_key(tag1));
        assert_eq!(tags.get(tag1).unwrap().version, 1);
        assert!(tags.contains_key("tag2"));
        assert_eq!(tags.get("tag2").unwrap().version, 2);
        // Test update and delete
        table.add(some_sample_data()).execute().await.unwrap();
        tags_manager.update(tag1, 3).await.unwrap();
        assert_eq!(tags_manager.get_version(tag1).await.unwrap(), 3);
        tags_manager.delete("tag2").await.unwrap();
        let tags = tags_manager.list().await.unwrap();
        assert_eq!(tags.len(), 1);
        assert!(tags.contains_key(tag1));
        assert_eq!(tags.get(tag1).unwrap().version, 3);
        // Test checkout tag
        table.add(some_sample_data()).execute().await.unwrap();
        assert_eq!(table.version().await.unwrap(), 4);
        table.checkout_tag(tag1).await.unwrap();
        assert_eq!(table.version().await.unwrap(), 3);
        table.checkout_latest().await.unwrap();
        assert_eq!(table.version().await.unwrap(), 4);
    }

    #[tokio::test]
    async fn test_branches() {
        let tmp_dir = tempdir().unwrap();
        let uri = tmp_dir.path().to_str().unwrap();

        let conn = ConnectBuilder::new(uri)
            .read_consistency_interval(Duration::from_secs(0))
            .execute()
            .await
            .unwrap();

        // main: one row at v1
        let table = conn
            .create_table("my_table", some_sample_data())
            .execute()
            .await
            .unwrap();
        assert_eq!(table.count_rows(None).await.unwrap(), 1);
        assert_eq!(table.current_branch(), None);
        let main_version = table.version().await.unwrap();

        // branch off main's current version; it starts with main's data
        let branch = table.create_branch("exp", main_version).await.unwrap();
        assert_eq!(branch.current_branch().as_deref(), Some("exp"));
        assert_eq!(branch.count_rows(None).await.unwrap(), 1);

        // writes on the branch are isolated from main
        branch.add(some_sample_data()).execute().await.unwrap();
        assert_eq!(branch.count_rows(None).await.unwrap(), 2);
        assert_eq!(
            table.count_rows(None).await.unwrap(),
            1,
            "main must be untouched by branch writes"
        );

        // the branch shows up in the listing
        let branches = table.list_branches().await.unwrap();
        assert!(branches.contains_key("exp"));

        // checking out the branch from the main handle sees the branch's latest data
        let checked_out = table.checkout_branch("exp", None).await.unwrap();
        assert_eq!(checked_out.current_branch().as_deref(), Some("exp"));
        assert_eq!(checked_out.count_rows(None).await.unwrap(), 2);

        // open_table(...).branch(...) opens directly onto the branch
        let opened = conn
            .open_table("my_table")
            .branch("exp")
            .execute()
            .await
            .unwrap();
        assert_eq!(opened.current_branch().as_deref(), Some("exp"));
        assert_eq!(opened.count_rows(None).await.unwrap(), 2);

        // delete removes it from the listing
        table.delete_branch("exp").await.unwrap();
        let branches = table.list_branches().await.unwrap();
        assert!(!branches.contains_key("exp"));
    }

    #[tokio::test]
    async fn test_branch_version_checkout() {
        let tmp_dir = tempdir().unwrap();
        let uri = tmp_dir.path().to_str().unwrap();

        let conn = ConnectBuilder::new(uri)
            .read_consistency_interval(Duration::from_secs(0))
            .execute()
            .await
            .unwrap();

        // main: a single fork-point row (i = 0)
        let table = conn
            .create_table("my_table", sample_rows(vec![0]))
            .execute()
            .await
            .unwrap();
        let fork_point = table.version().await.unwrap();

        // Fork "exp", then advance exp AND main independently past the fork so
        // they diverge while sharing version numbers.
        let branch = table.create_branch("exp", fork_point).await.unwrap();
        let exp_fork = branch.version().await.unwrap(); // exp's shallow-clone version
        branch.add(sample_rows(vec![1])).execute().await.unwrap(); // exp: {0, 1}
        let exp_v2 = branch.version().await.unwrap();
        branch.add(sample_rows(vec![2])).execute().await.unwrap(); // exp HEAD: {0, 1, 2}

        // main's own commit reaches the SAME version number with different data
        table
            .add(sample_rows(vec![100, 101, 102]))
            .execute()
            .await
            .unwrap(); // main HEAD: {0, 100, 101, 102}
        let main_v2 = table.version().await.unwrap();
        assert_eq!(
            exp_v2, main_v2,
            "branch and main must share the version number for this test to mean anything"
        );

        // Open exp at the shared version. The data must be exp's, not main's:
        // count alone cannot prove this (main@v2 differs), so assert provenance
        // by content.
        let pinned = conn
            .open_table("my_table")
            .branch("exp")
            .version(exp_v2)
            .execute()
            .await
            .unwrap();
        assert_eq!(pinned.current_branch().as_deref(), Some("exp"));
        // isolated from exp's HEAD (3 rows) and from main@v2 (4 rows)
        assert_eq!(pinned.count_rows(None).await.unwrap(), 2);
        // exp's post-fork row is visible; main's divergent rows are not
        assert_eq!(
            pinned.count_rows(Some("i = 1".to_string())).await.unwrap(),
            1
        );
        assert_eq!(
            pinned
                .count_rows(Some("i = 100".to_string()))
                .await
                .unwrap(),
            0
        );

        // the same coordinate is reachable directly via checkout_branch(name, version)
        let pinned_direct = table.checkout_branch("exp", Some(exp_v2)).await.unwrap();
        assert_eq!(pinned_direct.current_branch().as_deref(), Some("exp"));
        assert_eq!(pinned_direct.count_rows(None).await.unwrap(), 2);

        // the HEADs are unaffected
        let head = conn
            .open_table("my_table")
            .branch("exp")
            .execute()
            .await
            .unwrap();
        assert_eq!(head.count_rows(None).await.unwrap(), 3);
        assert_eq!(table.count_rows(None).await.unwrap(), 4);

        // a pinned version is a detached head: writes are rejected
        assert!(pinned.add(sample_rows(vec![9])).execute().await.is_err());

        // version-only (no branch) time-travels main itself: its fork-point
        // version holds only main's first row, and the shared version number
        // resolves to main's data, not the branch's ("opens main at the version")
        let old_main = conn
            .open_table("my_table")
            .version(fork_point)
            .execute()
            .await
            .unwrap();
        assert_eq!(old_main.current_branch(), None);
        assert_eq!(old_main.count_rows(None).await.unwrap(), 1);
        let shared_on_main = conn
            .open_table("my_table")
            .version(exp_v2)
            .execute()
            .await
            .unwrap();
        assert_eq!(shared_on_main.current_branch(), None);
        assert_eq!(shared_on_main.count_rows(None).await.unwrap(), 4);

        // a nonexistent version is rejected
        assert!(
            conn.open_table("my_table")
                .version(9999)
                .execute()
                .await
                .is_err()
        );

        // a nonexistent version on a branch is rejected too: this resolves on
        // the branch's path, a distinct miss from the main lookup above
        assert!(
            conn.open_table("my_table")
                .branch("exp")
                .version(9999)
                .execute()
                .await
                .is_err()
        );

        // opening the branch at its fork point (the shallow-clone manifest)
        // shows just the cloned state: main's fork-point row
        let exp_at_fork = conn
            .open_table("my_table")
            .branch("exp")
            .version(exp_fork)
            .execute()
            .await
            .unwrap();
        assert_eq!(exp_at_fork.current_branch().as_deref(), Some("exp"));
        assert_eq!(exp_at_fork.count_rows(None).await.unwrap(), 1);

        // checkout_latest re-attaches the pinned handle to the BRANCH's HEAD
        // (writable again), not main's HEAD, and not staying pinned
        pinned.checkout_latest().await.unwrap();
        assert_eq!(pinned.current_branch().as_deref(), Some("exp"));
        assert_eq!(pinned.count_rows(None).await.unwrap(), 3); // exp HEAD, not main's 4
        pinned.add(sample_rows(vec![3])).execute().await.unwrap();
        assert_eq!(pinned.count_rows(None).await.unwrap(), 4); // writable again
    }

    #[tokio::test]
    async fn test_branch_version_two_branches() {
        let tmp_dir = tempdir().unwrap();
        let uri = tmp_dir.path().to_str().unwrap();
        let conn = ConnectBuilder::new(uri)
            .read_consistency_interval(Duration::from_secs(0))
            .execute()
            .await
            .unwrap();

        let table = conn
            .create_table("my_table", sample_rows(vec![0]))
            .execute()
            .await
            .unwrap();
        let fork_point = table.version().await.unwrap();

        // two branches off the same point, each advanced once so they reach the
        // SAME version number with divergent data
        let exp1 = table.create_branch("exp1", fork_point).await.unwrap();
        let exp2 = table.create_branch("exp2", fork_point).await.unwrap();
        exp1.add(sample_rows(vec![10])).execute().await.unwrap();
        exp2.add(sample_rows(vec![20])).execute().await.unwrap();
        let v1 = exp1.version().await.unwrap();
        let v2 = exp2.version().await.unwrap();
        assert_eq!(v1, v2, "both branches must reach the same version number");

        // that shared version number resolves to each branch's own data
        let at1 = table.checkout_branch("exp1", Some(v1)).await.unwrap();
        assert_eq!(at1.count_rows(Some("i = 10".to_string())).await.unwrap(), 1);
        assert_eq!(at1.count_rows(Some("i = 20".to_string())).await.unwrap(), 0);
        let at2 = table.checkout_branch("exp2", Some(v2)).await.unwrap();
        assert_eq!(at2.count_rows(Some("i = 20".to_string())).await.unwrap(), 1);
        assert_eq!(at2.count_rows(Some("i = 10".to_string())).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn test_branch_name_validation() {
        let tmp_dir = tempdir().unwrap();
        let uri = tmp_dir.path().to_str().unwrap();
        let conn = ConnectBuilder::new(uri).execute().await.unwrap();
        let table = conn
            .create_table("my_table", some_sample_data())
            .execute()
            .await
            .unwrap();

        // every entry point rejects an empty name instead of passing it down
        assert!(matches!(
            table.create_branch("", 1u64).await,
            Err(Error::InvalidInput { .. })
        ));
        assert!(matches!(
            table.checkout_branch("", None).await,
            Err(Error::InvalidInput { .. })
        ));
        assert!(matches!(
            table.delete_branch("").await,
            Err(Error::InvalidInput { .. })
        ));
        // an empty source branch is rejected too
        assert!(matches!(
            table
                .create_branch(
                    "ok",
                    lance::dataset::refs::Ref::Version(Some(String::new()), None)
                )
                .await,
            Err(Error::InvalidInput { .. })
        ));
    }

    #[tokio::test]
    async fn test_branch_handle_tracks_concurrent_writes() {
        let tmp_dir = tempdir().unwrap();
        let uri = tmp_dir.path().to_str().unwrap();

        // interval = 0 so every read checks storage for new commits
        let conn = ConnectBuilder::new(uri)
            .read_consistency_interval(Duration::from_secs(0))
            .execute()
            .await
            .unwrap();
        let table = conn
            .create_table("my_table", some_sample_data())
            .execute()
            .await
            .unwrap();
        let v1 = table.version().await.unwrap();

        // two independent handles on the same branch
        let writer = table.create_branch("exp", v1).await.unwrap();
        let reader = conn
            .open_table("my_table")
            .branch("exp")
            .execute()
            .await
            .unwrap();
        assert_eq!(reader.count_rows(None).await.unwrap(), 1);

        // a concurrent write on the branch is visible to the other handle, which
        // tracks the branch's HEAD (not main's)
        writer.add(some_sample_data()).execute().await.unwrap();
        assert_eq!(reader.count_rows(None).await.unwrap(), 2);
        // main is untouched
        assert_eq!(table.count_rows(None).await.unwrap(), 1);
    }

    #[tokio::test]
    async fn test_branch_handle_without_consistency_interval_is_pinned() {
        let tmp_dir = tempdir().unwrap();
        let uri = tmp_dir.path().to_str().unwrap();

        // default interval (None): handles do not auto-refresh
        let conn = ConnectBuilder::new(uri).execute().await.unwrap();
        let table = conn
            .create_table("my_table", some_sample_data())
            .execute()
            .await
            .unwrap();
        let v1 = table.version().await.unwrap();

        let writer = table.create_branch("exp", v1).await.unwrap();
        let reader = conn
            .open_table("my_table")
            .branch("exp")
            .execute()
            .await
            .unwrap();
        assert_eq!(reader.count_rows(None).await.unwrap(), 1);

        // without a consistency interval the reader stays on the version it
        // opened, exactly like a main-branch handle...
        writer.add(some_sample_data()).execute().await.unwrap();
        assert_eq!(reader.count_rows(None).await.unwrap(), 1);

        // ...until it explicitly refreshes
        reader.checkout_latest().await.unwrap();
        assert_eq!(reader.count_rows(None).await.unwrap(), 2);
    }

    #[tokio::test]
    async fn test_create_index() {
        use arrow_array::RecordBatch;
        use arrow_schema::{DataType, Field, Schema as ArrowSchema};
        use rand;
        use std::iter::repeat_with;

        use arrow_array::Float32Array;

        let tmp_dir = tempdir().unwrap();
        let uri = tmp_dir.path().to_str().unwrap();
        let conn = connect(uri).execute().await.unwrap();

        let dimension = 16;
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "embeddings",
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, true)),
                dimension,
            ),
            false,
        )]));

        let float_arr = Float32Array::from(
            repeat_with(rand::random::<f32>)
                .take(512 * dimension as usize)
                .collect::<Vec<f32>>(),
        );

        let vectors = Arc::new(create_fixed_size_list(float_arr, dimension).unwrap());
        let batch = RecordBatch::try_new(schema.clone(), vec![vectors.clone()]).unwrap();

        let table = conn.create_table("test", batch).execute().await.unwrap();

        assert_eq!(table.index_stats("my_index").await.unwrap(), None);

        table
            .create_index(&["embeddings"], Index::Auto)
            .execute()
            .await
            .unwrap();

        let index_configs = table.list_indices().await.unwrap();
        assert_eq!(index_configs.len(), 1);
        let index = index_configs.into_iter().next().unwrap();
        assert_eq!(index.index_type, crate::index::IndexType::IvfPq);
        assert_eq!(index.columns, vec!["embeddings".to_string()]);
        assert_eq!(table.count_rows(None).await.unwrap(), 512);
        assert_eq!(table.name(), "test");

        let indices = table.as_native().unwrap().load_indices().await.unwrap();
        let index_name = &indices[0].index_name;
        let stats = table.index_stats(index_name).await.unwrap().unwrap();
        assert_eq!(stats.num_indexed_rows, 512);
        assert_eq!(stats.num_unindexed_rows, 0);
        assert_eq!(stats.index_type, crate::index::IndexType::IvfPq);
        assert_eq!(stats.distance_type, Some(crate::DistanceType::L2));

        table.drop_index(index_name).await.unwrap();
        assert_eq!(table.list_indices().await.unwrap().len(), 0);
    }

    #[tokio::test]
    async fn test_dynamic_select() {
        let tc = new_test_connection().await.unwrap();
        let db = tc.connection;

        let table = db
            .create_table("test", some_sample_data())
            .execute()
            .await
            .unwrap();

        let query = table.query().select(Select::dynamic(&[("i_alias", "i")]));

        let result = query.execute().await;
        let batches = result
            .expect("should have result")
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        for batch in batches {
            assert!(batch.column_by_name("i_alias").is_some());
        }
    }

    #[tokio::test]
    async fn test_ivf_pq_uses_default_partition_size_for_num_partitions() {
        use arrow_array::{Float32Array, RecordBatch};
        use arrow_schema::{DataType, Field, Schema as ArrowSchema};

        use crate::index::vector::IvfPqIndexBuilder;

        let tmp_dir = tempdir().unwrap();
        let uri = tmp_dir.path().to_str().unwrap();
        let conn = connect(uri).execute().await.unwrap();

        const PARTITION_SIZE: usize = 8192;
        let num_rows = PARTITION_SIZE * 2;
        let dimension = 8usize;
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "embeddings",
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, true)),
                dimension as i32,
            ),
            false,
        )]));

        let float_arr =
            Float32Array::from_iter_values((0..(num_rows * dimension)).map(|v| v as f32));
        let vectors = Arc::new(create_fixed_size_list(float_arr, dimension as i32).unwrap());
        let batch = RecordBatch::try_new(schema.clone(), vec![vectors]).unwrap();

        let table = conn.create_table("test", batch).execute().await.unwrap();
        let native_table = table.as_native().unwrap();
        let builder = IvfPqIndexBuilder::default();
        table
            .create_index(&["embeddings"], Index::IvfPq(builder))
            .execute()
            .await
            .unwrap();
        table
            .wait_for_index(&["embeddings_idx"], std::time::Duration::from_secs(30))
            .await
            .unwrap();

        use lance::index::DatasetIndexInternalExt;
        use lance::index::vector::ivf::v2::IvfPq as LanceIvfPq;
        use lance_index::metrics::NoOpMetricsCollector;
        use lance_index::vector::VectorIndex as LanceVectorIndex;

        let indices = native_table.load_indices().await.unwrap();
        let index_uuid = uuid::Uuid::parse_str(&indices[0].index_uuid).unwrap();

        let dataset_guard = native_table.dataset.get().await.unwrap();
        let dataset = (*dataset_guard).clone();
        drop(dataset_guard);

        let lance_index = dataset
            .open_vector_index("embeddings", &index_uuid, &NoOpMetricsCollector)
            .await
            .unwrap();
        let ivf_index = lance_index
            .as_any()
            .downcast_ref::<LanceIvfPq>()
            .expect("expected IvfPq index");
        let partition_count = ivf_index.ivf_model().num_partitions();

        let expected_partitions = num_rows / PARTITION_SIZE;
        assert_eq!(partition_count, expected_partitions);
    }

    #[tokio::test]
    async fn test_create_index_ivf_hnsw_sq() {
        use arrow_array::RecordBatch;
        use arrow_schema::{DataType, Field, Schema as ArrowSchema};
        use rand;
        use std::iter::repeat_with;

        use arrow_array::Float32Array;

        let tmp_dir = tempdir().unwrap();
        let uri = tmp_dir.path().to_str().unwrap();
        let conn = connect(uri).execute().await.unwrap();

        let dimension = 16;
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "embeddings",
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, true)),
                dimension,
            ),
            false,
        )]));

        let float_arr = Float32Array::from(
            repeat_with(rand::random::<f32>)
                .take(512 * dimension as usize)
                .collect::<Vec<f32>>(),
        );

        let vectors = Arc::new(create_fixed_size_list(float_arr, dimension).unwrap());
        let batch = RecordBatch::try_new(schema.clone(), vec![vectors.clone()]).unwrap();

        let table = conn.create_table("test", batch).execute().await.unwrap();

        let stats = table.index_stats("my_index").await.unwrap();
        assert!(stats.is_none());

        let index = IvfHnswSqIndexBuilder::default();
        table
            .create_index(&["embeddings"], Index::IvfHnswSq(index))
            .execute()
            .await
            .unwrap();

        let index_configs = table.list_indices().await.unwrap();
        assert_eq!(index_configs.len(), 1);
        let index = index_configs.into_iter().next().unwrap();
        assert_eq!(index.index_type, crate::index::IndexType::IvfHnswSq);
        assert_eq!(index.columns, vec!["embeddings".to_string()]);
        assert_eq!(table.count_rows(None).await.unwrap(), 512);
        assert_eq!(table.name(), "test");

        let indices = table.as_native().unwrap().load_indices().await.unwrap();
        let index_name = &indices[0].index_name;
        let stats = table.index_stats(index_name).await.unwrap().unwrap();
        assert_eq!(stats.num_indexed_rows, 512);
        assert_eq!(stats.num_unindexed_rows, 0);
    }

    #[tokio::test]
    async fn test_create_index_ivf_hnsw_pq() {
        use arrow_array::RecordBatch;
        use arrow_schema::{DataType, Field, Schema as ArrowSchema};
        use rand;
        use std::iter::repeat_with;

        use arrow_array::Float32Array;

        let tmp_dir = tempdir().unwrap();
        let uri = tmp_dir.path().to_str().unwrap();
        let conn = connect(uri).execute().await.unwrap();

        let dimension = 16;
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "embeddings",
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, true)),
                dimension,
            ),
            false,
        )]));

        let float_arr = Float32Array::from(
            repeat_with(rand::random::<f32>)
                .take(512 * dimension as usize)
                .collect::<Vec<f32>>(),
        );

        let vectors = Arc::new(create_fixed_size_list(float_arr, dimension).unwrap());
        let batch = RecordBatch::try_new(schema.clone(), vec![vectors.clone()]).unwrap();

        let table = conn.create_table("test", batch).execute().await.unwrap();
        let stats = table.index_stats("my_index").await.unwrap();
        assert!(stats.is_none());

        let index = IvfHnswPqIndexBuilder::default();
        table
            .create_index(&["embeddings"], Index::IvfHnswPq(index))
            .execute()
            .await
            .unwrap();
        table
            .wait_for_index(&["embeddings_idx"], Duration::from_millis(10))
            .await
            .unwrap();
        let index_configs = table.list_indices().await.unwrap();
        assert_eq!(index_configs.len(), 1);
        let index = index_configs.into_iter().next().unwrap();
        assert_eq!(index.index_type, crate::index::IndexType::IvfHnswPq);
        assert_eq!(index.columns, vec!["embeddings".to_string()]);
        assert_eq!(table.count_rows(None).await.unwrap(), 512);
        assert_eq!(table.name(), "test");

        let indices: Vec<VectorIndex> = table.as_native().unwrap().load_indices().await.unwrap();
        let index_name = &indices[0].index_name;
        let stats = table.index_stats(index_name).await.unwrap().unwrap();
        assert_eq!(stats.num_indexed_rows, 512);
        assert_eq!(stats.num_unindexed_rows, 0);
    }

    #[tokio::test]
    async fn test_create_index_ivf_hnsw_flat() {
        use arrow_array::RecordBatch;
        use arrow_schema::{DataType, Field, Schema as ArrowSchema};
        use rand;
        use std::iter::repeat_with;

        use crate::index::vector::IvfHnswFlatIndexBuilder;
        use arrow_array::Float32Array;

        let tmp_dir = tempdir().unwrap();
        let uri = tmp_dir.path().to_str().unwrap();
        let conn = connect(uri).execute().await.unwrap();

        let dimension = 16;
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "embeddings",
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, true)),
                dimension,
            ),
            false,
        )]));

        let float_arr = Float32Array::from(
            repeat_with(rand::random::<f32>)
                .take(512 * dimension as usize)
                .collect::<Vec<f32>>(),
        );

        let vectors = Arc::new(create_fixed_size_list(float_arr, dimension).unwrap());
        let batch = RecordBatch::try_new(schema.clone(), vec![vectors.clone()]).unwrap();

        let table = conn.create_table("test", batch).execute().await.unwrap();

        let index = IvfHnswFlatIndexBuilder::default();
        table
            .create_index(&["embeddings"], Index::IvfHnswFlat(index))
            .execute()
            .await
            .unwrap();

        let index_configs = table.list_indices().await.unwrap();
        assert_eq!(index_configs.len(), 1);
        let index = index_configs.into_iter().next().unwrap();
        assert_eq!(index.index_type, crate::index::IndexType::IvfHnswFlat);
        assert_eq!(index.columns, vec!["embeddings".to_string()]);
        assert_eq!(table.count_rows(None).await.unwrap(), 512);
    }

    fn create_fixed_size_list<T: Array>(values: T, list_size: i32) -> Result<FixedSizeListArray> {
        let list_type = DataType::FixedSizeList(
            Arc::new(Field::new("item", values.data_type().clone(), true)),
            list_size,
        );
        let data = ArrayDataBuilder::new(list_type)
            .len(values.len() / list_size as usize)
            .add_child_data(values.into_data())
            .build()
            .unwrap();

        Ok(FixedSizeListArray::from(data))
    }

    fn some_sample_data() -> Box<dyn arrow_array::RecordBatchReader + Send> {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("i", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![1]))],
        )
        .unwrap();
        let schema = batch.schema().clone();
        let batch = Ok(batch);

        Box::new(RecordBatchIterator::new(vec![batch], schema))
    }

    /// A single-batch reader holding the given `i` (Int32) values. Lets a test
    /// write distinguishable rows so it can assert data provenance, not row count.
    fn sample_rows(values: Vec<i32>) -> Box<dyn arrow_array::RecordBatchReader + Send> {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("i", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(values))],
        )
        .unwrap();
        let schema = batch.schema().clone();

        Box::new(RecordBatchIterator::new(vec![Ok(batch)], schema))
    }

    #[tokio::test]
    async fn test_create_scalar_index() {
        let tmp_dir = tempdir().unwrap();
        let uri = tmp_dir.path().to_str().unwrap();

        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("i", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![1]))],
        )
        .unwrap();
        let conn = ConnectBuilder::new(uri).execute().await.unwrap();
        let table = conn
            .create_table("my_table", batch.clone())
            .execute()
            .await
            .unwrap();

        // Can create an index on a scalar column (will default to btree)
        table
            .create_index(&["i"], Index::Auto)
            .execute()
            .await
            .unwrap();
        table
            .wait_for_index(&["i_idx"], Duration::from_millis(10))
            .await
            .unwrap();
        let index_configs = table.list_indices().await.unwrap();
        assert_eq!(index_configs.len(), 1);
        let index = index_configs.into_iter().next().unwrap();
        assert_eq!(index.index_type, crate::index::IndexType::BTree);
        assert_eq!(index.columns, vec!["i".to_string()]);

        // Can also specify btree
        table
            .create_index(&["i"], Index::BTree(BTreeIndexBuilder::default()))
            .execute()
            .await
            .unwrap();

        let index_configs = table.list_indices().await.unwrap();
        assert_eq!(index_configs.len(), 1);
        let index = index_configs.into_iter().next().unwrap();
        assert_eq!(index.index_type, crate::index::IndexType::BTree);
        assert_eq!(index.columns, vec!["i".to_string()]);

        let indices = table.as_native().unwrap().load_indices().await.unwrap();
        let index_name = &indices[0].index_name;
        let stats = table.index_stats(index_name).await.unwrap().unwrap();
        assert_eq!(stats.num_indexed_rows, 1);
        assert_eq!(stats.num_unindexed_rows, 0);
    }

    #[tokio::test]
    async fn test_create_fm_index() {
        let tmp_dir = tempdir().unwrap();
        let uri = tmp_dir.path().to_str().unwrap();

        // FM-Index accelerates substring search, so it applies to a string column.
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("text", DataType::Utf8, false)])),
            vec![Arc::new(StringArray::from(vec!["hello world"]))],
        )
        .unwrap();
        let conn = ConnectBuilder::new(uri).execute().await.unwrap();
        let table = conn
            .create_table("my_table", batch.clone())
            .execute()
            .await
            .unwrap();

        table
            .create_index(&["text"], Index::Fm(FmIndexBuilder::default()))
            .execute()
            .await
            .unwrap();
        table
            .wait_for_index(&["text_idx"], Duration::from_millis(10))
            .await
            .unwrap();

        let index_configs = table.list_indices().await.unwrap();
        assert_eq!(index_configs.len(), 1);
        let index = index_configs.into_iter().next().unwrap();
        assert_eq!(index.index_type, crate::index::IndexType::Fm);
        assert_eq!(index.columns, vec!["text".to_string()]);

        // The committed FM-Index must answer a substring `contains` query.
        let count = table
            .query()
            .only_if("contains(text, 'world')")
            .execute()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .iter()
            .map(|b| b.num_rows())
            .sum::<usize>();
        assert_eq!(count, 1);
    }

    #[tokio::test]
    async fn test_create_index_nested_field_paths() {
        let tmp_dir = tempdir().unwrap();
        let uri = tmp_dir.path().to_str().unwrap();
        let conn = ConnectBuilder::new(uri).execute().await.unwrap();

        let num_rows = 512;
        let dimension = 8;

        let row_id = Arc::new(Int32Array::from_iter_values(0..num_rows)) as ArrayRef;
        let row_dash_id = Arc::new(Int32Array::from_iter_values(0..num_rows)) as ArrayRef;
        let top_user_id = Arc::new(Int32Array::from_iter_values(0..num_rows)) as ArrayRef;

        let metadata = Arc::new(StructArray::from(vec![(
            Arc::new(Field::new("user_id", DataType::Int32, false)),
            Arc::new(Int32Array::from_iter_values(0..num_rows)) as ArrayRef,
        )]));

        let mixed_case_metadata = Arc::new(StructArray::from(vec![(
            Arc::new(Field::new("userId", DataType::Int32, false)),
            Arc::new(Int32Array::from_iter_values(0..num_rows)) as ArrayRef,
        )]));

        let vector_values = arrow_array::Float32Array::from_iter_values(
            (0..num_rows * dimension).map(|v| v as f32),
        );
        let embeddings =
            Arc::new(create_fixed_size_list(vector_values, dimension).unwrap()) as ArrayRef;
        let image = Arc::new(StructArray::from(vec![(
            Arc::new(Field::new(
                "embedding",
                embeddings.data_type().clone(),
                false,
            )),
            embeddings,
        )]));

        let payload = Arc::new(StructArray::from(vec![(
            Arc::new(Field::new("text", DataType::Utf8, false)),
            Arc::new(StringArray::from_iter_values(
                (0..num_rows).map(|i| format!("document {}", i)),
            )) as ArrayRef,
        )]));

        let meta_data = Arc::new(StructArray::from(vec![(
            Arc::new(Field::new("user-id", DataType::Int32, false)),
            Arc::new(Int32Array::from_iter_values(0..num_rows)) as ArrayRef,
        )]));

        let literal = Arc::new(StructArray::from(vec![(
            Arc::new(Field::new("a.b", DataType::Int32, false)),
            Arc::new(Int32Array::from_iter_values(0..num_rows)) as ArrayRef,
        )]));

        let schema = Arc::new(Schema::new(vec![
            Field::new("rowId", DataType::Int32, false),
            Field::new("row-id", DataType::Int32, false),
            Field::new("userId", DataType::Int32, false),
            Field::new("metadata", metadata.data_type().clone(), false),
            Field::new("MetaData", mixed_case_metadata.data_type().clone(), false),
            Field::new("image", image.data_type().clone(), false),
            Field::new("payload", payload.data_type().clone(), false),
            Field::new("meta-data", meta_data.data_type().clone(), false),
            Field::new("literal", literal.data_type().clone(), false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                row_id,
                row_dash_id,
                top_user_id,
                metadata,
                mixed_case_metadata,
                image,
                payload,
                meta_data,
                literal,
            ],
        )
        .unwrap();

        let table = conn
            .create_table("nested_index_paths", batch)
            .execute()
            .await
            .unwrap();

        table
            .create_index(
                &["metadata.user_id"],
                Index::BTree(BTreeIndexBuilder::default()),
            )
            .name("metadata_user_id_idx".to_string())
            .execute()
            .await
            .unwrap();
        table
            .create_index(&["rowId"], Index::BTree(BTreeIndexBuilder::default()))
            .name("row_id_idx".to_string())
            .execute()
            .await
            .unwrap();
        table
            .create_index(&["`row-id`"], Index::BTree(BTreeIndexBuilder::default()))
            .name("row_dash_id_idx".to_string())
            .execute()
            .await
            .unwrap();
        table
            .create_index(&["userId"], Index::BTree(BTreeIndexBuilder::default()))
            .name("top_user_id_idx".to_string())
            .execute()
            .await
            .unwrap();
        table
            .create_index(
                &["MetaData.userId"],
                Index::BTree(BTreeIndexBuilder::default()),
            )
            .name("mixed_case_metadata_user_id_idx".to_string())
            .execute()
            .await
            .unwrap();
        table
            .create_index(&["image.embedding"], Index::Auto)
            .name("image_embedding_idx".to_string())
            .execute()
            .await
            .unwrap();
        table
            .create_index(&["payload.text"], Index::FTS(Default::default()))
            .name("payload_text_idx".to_string())
            .execute()
            .await
            .unwrap();
        table
            .create_index(
                &["`meta-data`.`user-id`"],
                Index::BTree(BTreeIndexBuilder::default()),
            )
            .name("escaped_names_idx".to_string())
            .execute()
            .await
            .unwrap();
        table
            .create_index(
                &["literal.`a.b`"],
                Index::BTree(BTreeIndexBuilder::default()),
            )
            .name("literal_dot_idx".to_string())
            .execute()
            .await
            .unwrap();

        let mut index_configs = table.list_indices().await.unwrap();
        index_configs.sort_by(|left, right| left.name.cmp(&right.name));

        let indexed_columns = index_configs
            .iter()
            .map(|index| {
                (
                    index.name.as_str(),
                    index.columns.as_slice(),
                    index.index_type.clone(),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(
            indexed_columns,
            vec![
                (
                    "escaped_names_idx",
                    &["`meta-data`.`user-id`".to_string()][..],
                    crate::index::IndexType::BTree,
                ),
                (
                    "image_embedding_idx",
                    &["image.embedding".to_string()][..],
                    crate::index::IndexType::IvfPq,
                ),
                (
                    "literal_dot_idx",
                    &["literal.`a.b`".to_string()][..],
                    crate::index::IndexType::BTree,
                ),
                (
                    "metadata_user_id_idx",
                    &["metadata.user_id".to_string()][..],
                    crate::index::IndexType::BTree,
                ),
                (
                    "mixed_case_metadata_user_id_idx",
                    &["MetaData.userId".to_string()][..],
                    crate::index::IndexType::BTree,
                ),
                (
                    "payload_text_idx",
                    &["payload.text".to_string()][..],
                    crate::index::IndexType::FTS,
                ),
                (
                    "row_dash_id_idx",
                    &["`row-id`".to_string()][..],
                    crate::index::IndexType::BTree,
                ),
                (
                    "row_id_idx",
                    &["rowId".to_string()][..],
                    crate::index::IndexType::BTree,
                ),
                (
                    "top_user_id_idx",
                    &["userId".to_string()][..],
                    crate::index::IndexType::BTree,
                ),
            ]
        );

        let vector_results = table
            .query()
            .nearest_to(&[0.0; 8])
            .unwrap()
            .column("image.embedding")
            .limit(1)
            .execute()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(
            vector_results
                .iter()
                .map(|batch| batch.num_rows())
                .sum::<usize>(),
            1
        );

        let default_vector_results = table
            .query()
            .nearest_to(&[0.0; 8])
            .unwrap()
            .limit(1)
            .execute()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(
            default_vector_results
                .iter()
                .map(|batch| batch.num_rows())
                .sum::<usize>(),
            1
        );

        let fts_results = table
            .query()
            .full_text_search(FullTextSearchQuery::new("document".to_string()))
            .limit(5)
            .execute()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert!(!fts_results.is_empty());

        let filtered_results = table
            .query()
            .only_if("metadata.user_id = 42")
            .limit(1)
            .execute()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(
            filtered_results
                .iter()
                .map(|batch| batch.num_rows())
                .sum::<usize>(),
            1
        );
    }

    #[tokio::test]
    async fn test_create_bitmap_index() {
        let tmp_dir = tempdir().unwrap();
        let uri = tmp_dir.path().to_str().unwrap();

        let conn = ConnectBuilder::new(uri).execute().await.unwrap();

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("category", DataType::Utf8, true),
            Field::new("large_category", DataType::LargeUtf8, true),
            Field::new("is_active", DataType::Boolean, true),
            Field::new("data", DataType::Binary, true),
            Field::new("large_data", DataType::LargeBinary, true),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from_iter_values(0..100)),
                Arc::new(StringArray::from_iter_values(
                    (0..100).map(|i| format!("category_{}", i % 5)),
                )),
                Arc::new(LargeStringArray::from_iter_values(
                    (0..100).map(|i| format!("large_category_{}", i % 5)),
                )),
                Arc::new(BooleanArray::from_iter((0..100).map(|i| Some(i % 2 == 0)))),
                Arc::new(BinaryArray::from_iter_values(
                    (0_u32..100).map(|i| i.to_le_bytes()),
                )),
                Arc::new(LargeBinaryArray::from_iter_values(
                    (0_u32..100).map(|i| i.to_le_bytes()),
                )),
            ],
        )
        .unwrap();

        let table = conn
            .create_table("test_bitmap", batch.clone())
            .execute()
            .await
            .unwrap();

        // Create bitmap index on the "category" column
        table
            .create_index(&["category"], Index::Bitmap(Default::default()))
            .execute()
            .await
            .unwrap();

        // Create bitmap index on the "is_active" column
        table
            .create_index(&["is_active"], Index::Bitmap(Default::default()))
            .execute()
            .await
            .unwrap();

        // Create bitmap index on the "data" column
        table
            .create_index(&["data"], Index::Bitmap(Default::default()))
            .execute()
            .await
            .unwrap();

        // Create bitmap index on the "large_data" column
        table
            .create_index(&["large_data"], Index::Bitmap(Default::default()))
            .execute()
            .await
            .unwrap();

        // Create bitmap index on the "large_category" column
        table
            .create_index(&["large_category"], Index::Bitmap(Default::default()))
            .execute()
            .await
            .unwrap();

        // Verify the index was created
        let index_configs = table.list_indices().await.unwrap();
        assert_eq!(index_configs.len(), 5);

        // list_indices returns indices in alphabetical order by name
        let mut configs_iter = index_configs.into_iter();
        let index = configs_iter.next().unwrap();
        assert_eq!(index.index_type, crate::index::IndexType::Bitmap);
        assert_eq!(index.columns, vec!["category".to_string()]);

        let index = configs_iter.next().unwrap();
        assert_eq!(index.index_type, crate::index::IndexType::Bitmap);
        assert_eq!(index.columns, vec!["data".to_string()]);

        let index = configs_iter.next().unwrap();
        assert_eq!(index.index_type, crate::index::IndexType::Bitmap);
        assert_eq!(index.columns, vec!["is_active".to_string()]);

        let index = configs_iter.next().unwrap();
        assert_eq!(index.index_type, crate::index::IndexType::Bitmap);
        assert_eq!(index.columns, vec!["large_category".to_string()]);

        let index = configs_iter.next().unwrap();
        assert_eq!(index.index_type, crate::index::IndexType::Bitmap);
        assert_eq!(index.columns, vec!["large_data".to_string()]);
    }

    #[tokio::test]
    async fn test_create_label_list_index() {
        let tmp_dir = tempdir().unwrap();
        let uri = tmp_dir.path().to_str().unwrap();

        let conn = ConnectBuilder::new(uri).execute().await.unwrap();

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                "tags",
                DataType::List(Field::new("item", DataType::Utf8, true).into()),
                true,
            ),
        ]));

        const TAGS: [&str; 3] = ["cat", "dog", "fish"];

        let values_builder = StringBuilder::new();
        let mut builder = ListBuilder::new(values_builder);
        for i in 0..120 {
            builder.values().append_value(TAGS[i % 3]);
            if i % 3 == 0 {
                builder.append(true)
            }
        }
        let tags = Arc::new(builder.finish());

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from_iter_values(0..40)), tags],
        )
        .unwrap();

        let table = conn
            .create_table("test_bitmap", batch.clone())
            .execute()
            .await
            .unwrap();

        // Can not create btree or bitmap index on list column
        assert!(
            table
                .create_index(&["tags"], Index::BTree(Default::default()))
                .execute()
                .await
                .is_err()
        );
        assert!(
            table
                .create_index(&["tags"], Index::Bitmap(Default::default()))
                .execute()
                .await
                .is_err()
        );

        // Create bitmap index on the "category" column
        table
            .create_index(&["tags"], Index::LabelList(Default::default()))
            .execute()
            .await
            .unwrap();

        // Verify the index was created
        let index_configs = table.list_indices().await.unwrap();
        assert_eq!(index_configs.len(), 1);
        let index = index_configs.into_iter().next().unwrap();
        assert_eq!(index.index_type, crate::index::IndexType::LabelList);
        assert_eq!(index.columns, vec!["tags".to_string()]);
    }

    #[tokio::test]
    async fn test_create_label_list_index_on_large_list() {
        let tmp_dir = tempdir().unwrap();
        let uri = tmp_dir.path().to_str().unwrap();

        let conn = ConnectBuilder::new(uri).execute().await.unwrap();

        let schema = Arc::new(Schema::new(vec![Field::new(
            "tags",
            DataType::LargeList(Field::new("item", DataType::Utf8, true).into()),
            true,
        )]));

        const TAGS: [&str; 3] = ["cat", "dog", "fish"];

        let values_builder = StringBuilder::new();
        let mut builder = LargeListBuilder::new(values_builder);
        for i in 0..120 {
            builder.values().append_value(TAGS[i % 3]);
            if i % 3 == 0 {
                builder.append(true)
            }
        }
        let tags = Arc::new(builder.finish());

        let batch = RecordBatch::try_new(schema, vec![tags]).unwrap();

        let table = conn
            .create_table("test_large_list_label_list", batch)
            .execute()
            .await
            .unwrap();

        table
            .create_index(&["tags"], Index::LabelList(Default::default()))
            .execute()
            .await
            .unwrap();

        let index_configs = table.list_indices().await.unwrap();
        assert_eq!(index_configs.len(), 1);
        let index = index_configs.into_iter().next().unwrap();
        assert_eq!(index.index_type, crate::index::IndexType::LabelList);
        assert_eq!(index.columns, vec!["tags".to_string()]);
    }

    #[tokio::test]
    async fn test_create_inverted_index() {
        let tmp_dir = tempdir().unwrap();
        let uri = tmp_dir.path().to_str().unwrap();

        let conn = ConnectBuilder::new(uri).execute().await.unwrap();
        const WORDS: [&str; 3] = ["cat", "dog", "fish"];
        let mut text_builder = StringBuilder::new();
        let num_rows = 120;
        for i in 0..num_rows {
            text_builder.append_value(WORDS[i % 3]);
        }
        let text = Arc::new(text_builder.finish());

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("text", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from_iter_values(0..num_rows as i32)),
                text,
            ],
        )
        .unwrap();

        let table = conn
            .create_table("test_bitmap", batch.clone())
            .execute()
            .await
            .unwrap();

        table
            .create_index(&["text"], Index::FTS(Default::default()))
            .execute()
            .await
            .unwrap();
        let index_configs = table.list_indices().await.unwrap();
        assert_eq!(index_configs.len(), 1);
        let index = index_configs.into_iter().next().unwrap();
        assert_eq!(index.index_type, crate::index::IndexType::FTS);
        assert_eq!(index.columns, vec!["text".to_string()]);
        assert_eq!(index.name, "text_idx");

        let stats = table.index_stats("text_idx").await.unwrap().unwrap();
        assert_eq!(stats.num_indexed_rows, num_rows);
        assert_eq!(stats.num_unindexed_rows, 0);
        assert_eq!(stats.index_type, crate::index::IndexType::FTS);
        assert_eq!(stats.distance_type, None);

        // Make sure we can call prewarm without error
        table.prewarm_index("text_idx").await.unwrap();
    }

    // Windows does not support precise sleep durations due to timer resolution limitations.
    #[cfg(not(target_os = "windows"))]
    #[tokio::test]
    async fn test_read_consistency_interval() {
        let intervals = vec![
            None,
            Some(0),
            Some(100), // 100 ms
        ];

        for interval in intervals {
            let data = some_sample_data();

            let tmp_dir = tempdir().unwrap();
            let uri = tmp_dir.path().to_str().unwrap();

            let conn1 = ConnectBuilder::new(uri).execute().await.unwrap();
            let table1 = conn1
                .create_empty_table("my_table", RecordBatchReader::schema(&data))
                .execute()
                .await
                .unwrap();

            let mut conn2 = ConnectBuilder::new(uri);
            if let Some(interval) = interval {
                conn2 = conn2.read_consistency_interval(std::time::Duration::from_millis(interval));
            }
            let conn2 = conn2.execute().await.unwrap();
            let table2 = conn2.open_table("my_table").execute().await.unwrap();

            assert_eq!(table1.count_rows(None).await.unwrap(), 0);
            assert_eq!(table2.count_rows(None).await.unwrap(), 0);

            table1.add(data).execute().await.unwrap();
            assert_eq!(table1.count_rows(None).await.unwrap(), 1);

            match interval {
                None => {
                    assert_eq!(table2.count_rows(None).await.unwrap(), 0);
                    table2.checkout_latest().await.unwrap();
                    assert_eq!(table2.count_rows(None).await.unwrap(), 1);
                }
                Some(0) => {
                    assert_eq!(table2.count_rows(None).await.unwrap(), 1);
                }
                Some(100) => {
                    assert_eq!(table2.count_rows(None).await.unwrap(), 0);
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    assert_eq!(table2.count_rows(None).await.unwrap(), 1);
                }
                _ => unreachable!(),
            }
        }
    }

    #[tokio::test]
    async fn test_time_travel_write() {
        let tmp_dir = tempdir().unwrap();
        let uri = tmp_dir.path().to_str().unwrap();

        let conn = ConnectBuilder::new(uri)
            .read_consistency_interval(Duration::from_secs(0))
            .execute()
            .await
            .unwrap();
        let table = conn
            .create_table("my_table", some_sample_data())
            .execute()
            .await
            .unwrap();
        let version = table.version().await.unwrap();
        table.add(some_sample_data()).execute().await.unwrap();
        table.checkout(version).await.unwrap();
        assert!(table.add(some_sample_data()).execute().await.is_err())
    }

    #[tokio::test]
    async fn test_update_dataset_config() {
        let tmp_dir = tempdir().unwrap();
        let uri = tmp_dir.path().to_str().unwrap();

        let conn = ConnectBuilder::new(uri)
            .read_consistency_interval(Duration::from_secs(0))
            .execute()
            .await
            .unwrap();

        let table = conn
            .create_table("my_table", some_sample_data())
            .execute()
            .await
            .unwrap();
        let native_tbl = table.as_native().unwrap();

        let manifest = native_tbl.manifest().await.unwrap();
        let base_config_len = manifest.config.len();

        native_tbl
            .update_config(vec![("test_key1".to_string(), "test_val1".to_string())])
            .await
            .unwrap();

        let manifest = native_tbl.manifest().await.unwrap();
        assert_eq!(manifest.config.len(), 1 + base_config_len);
        assert_eq!(
            manifest.config.get("test_key1"),
            Some(&"test_val1".to_string())
        );

        native_tbl
            .update_config(vec![("test_key2".to_string(), "test_val2".to_string())])
            .await
            .unwrap();
        let manifest = native_tbl.manifest().await.unwrap();
        assert_eq!(manifest.config.len(), 2 + base_config_len);
        assert_eq!(
            manifest.config.get("test_key1"),
            Some(&"test_val1".to_string())
        );
        assert_eq!(
            manifest.config.get("test_key2"),
            Some(&"test_val2".to_string())
        );

        native_tbl
            .update_config(vec![(
                "test_key2".to_string(),
                "test_val2_update".to_string(),
            )])
            .await
            .unwrap();
        let manifest = native_tbl.manifest().await.unwrap();
        assert_eq!(manifest.config.len(), 2 + base_config_len);
        assert_eq!(
            manifest.config.get("test_key1"),
            Some(&"test_val1".to_string())
        );
        assert_eq!(
            manifest.config.get("test_key2"),
            Some(&"test_val2_update".to_string())
        );

        native_tbl.delete_config_keys(&["test_key1"]).await.unwrap();
        let manifest = native_tbl.manifest().await.unwrap();
        assert_eq!(manifest.config.len(), 1 + base_config_len);
        assert_eq!(
            manifest.config.get("test_key2"),
            Some(&"test_val2_update".to_string())
        );
    }

    #[tokio::test]
    async fn test_schema_metadata_config() {
        let tmp_dir = tempdir().unwrap();
        let uri = tmp_dir.path().to_str().unwrap();

        let conn = ConnectBuilder::new(uri)
            .read_consistency_interval(Duration::from_secs(0))
            .execute()
            .await
            .unwrap();
        let table = conn
            .create_table("my_table", some_sample_data())
            .execute()
            .await
            .unwrap();

        let native_tbl = table.as_native().unwrap();
        let schema = native_tbl.schema().await.unwrap();
        let metadata = schema.metadata();
        assert_eq!(metadata.len(), 0);

        native_tbl
            .replace_schema_metadata(vec![("test_key1".to_string(), "test_val1".to_string())])
            .await
            .unwrap();

        let schema = native_tbl.schema().await.unwrap();
        let metadata = schema.metadata();
        assert_eq!(metadata.len(), 1);
        assert_eq!(metadata.get("test_key1"), Some(&"test_val1".to_string()));

        native_tbl
            .replace_schema_metadata(vec![
                ("test_key1".to_string(), "test_val1_update".to_string()),
                ("test_key2".to_string(), "test_val2".to_string()),
            ])
            .await
            .unwrap();
        let schema = native_tbl.schema().await.unwrap();
        let metadata = schema.metadata();
        assert_eq!(metadata.len(), 2);
        assert_eq!(
            metadata.get("test_key1"),
            Some(&"test_val1_update".to_string())
        );
        assert_eq!(metadata.get("test_key2"), Some(&"test_val2".to_string()));

        native_tbl
            .replace_schema_metadata(vec![(
                "test_key2".to_string(),
                "test_val2_update".to_string(),
            )])
            .await
            .unwrap();
        let schema = native_tbl.schema().await.unwrap();
        let metadata = schema.metadata();
        assert_eq!(
            metadata.get("test_key2"),
            Some(&"test_val2_update".to_string())
        );
    }

    #[tokio::test]
    pub async fn test_field_metadata_update() {
        let tmp_dir = tempdir().unwrap();
        let uri = tmp_dir.path().to_str().unwrap();

        let conn = ConnectBuilder::new(uri)
            .read_consistency_interval(Duration::from_secs(0))
            .execute()
            .await
            .unwrap();
        let table = conn
            .create_table("my_table", some_sample_data())
            .execute()
            .await
            .unwrap();

        let native_tbl = table.as_native().unwrap();
        let schema = native_tbl.manifest().await.unwrap().schema;

        let field = schema.field("i").unwrap();
        assert_eq!(field.metadata.len(), 0);

        native_tbl
            .replace_schema_metadata(vec![(
                "test_key2".to_string(),
                "test_val2_update".to_string(),
            )])
            .await
            .unwrap();

        let schema = native_tbl.schema().await.unwrap();
        let metadata = schema.metadata();
        assert_eq!(metadata.len(), 1);
        assert_eq!(
            metadata.get("test_key2"),
            Some(&"test_val2_update".to_string())
        );

        native_tbl
            .update_field_metadata(&[
                FieldMetadataUpdate::new("i").set("test_field_key1", "test_field_val1")
            ])
            .await
            .unwrap();

        let schema = native_tbl.manifest().await.unwrap().schema;
        let field = schema.field("i").unwrap();
        assert_eq!(field.metadata.len(), 1);
        assert_eq!(
            field.metadata.get("test_field_key1"),
            Some(&"test_field_val1".to_string())
        );
    }

    #[tokio::test]
    async fn test_set_unenforced_primary_key() {
        let tmp_dir = tempdir().unwrap();
        let uri = tmp_dir.path().to_str().unwrap();

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("score", DataType::Float64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(arrow_array::Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
                Arc::new(arrow_array::Float64Array::from(vec![1.0, 2.0, 3.0])),
            ],
        )
        .unwrap();
        let reader: Box<dyn arrow_array::RecordBatchReader + Send> =
            Box::new(RecordBatchIterator::new(vec![Ok(batch)], schema.clone()));

        let conn = ConnectBuilder::new(uri)
            .read_consistency_interval(Duration::from_secs(0))
            .execute()
            .await
            .unwrap();
        let table = conn.create_table("t", reader).execute().await.unwrap();

        // Reject empty input.
        let err = table
            .set_unenforced_primary_key(Vec::<&str>::new())
            .await
            .expect_err("empty input should be rejected");
        assert!(matches!(err, Error::InvalidInput { .. }), "got {:?}", err);

        // Reject compound (multi-column) input.
        let err = table
            .set_unenforced_primary_key(["id", "name"])
            .await
            .expect_err("compound primary key should be rejected");
        assert!(matches!(err, Error::InvalidInput { .. }), "got {:?}", err);

        // Reject unknown column.
        let err = table
            .set_unenforced_primary_key(["nonexistent"])
            .await
            .expect_err("nonexistent column should be rejected");
        assert!(matches!(err, Error::InvalidInput { .. }), "got {:?}", err);

        // Reject unsupported dtype (Float64).
        let err = table
            .set_unenforced_primary_key(["score"])
            .await
            .expect_err("Float64 should be rejected");
        assert!(matches!(err, Error::InvalidInput { .. }), "got {:?}", err);

        // None of the rejected calls set a primary key.
        let lance_schema = table.as_native().unwrap().manifest().await.unwrap().schema;
        assert!(lance_schema.unenforced_primary_key().is_empty());

        // Happy path: set the primary key to "id".
        table.set_unenforced_primary_key(["id"]).await.unwrap();
        let lance_schema = table.as_native().unwrap().manifest().await.unwrap().schema;
        let pk = lance_schema.unenforced_primary_key();
        assert_eq!(pk.len(), 1);
        assert_eq!(pk[0].name, "id");
        // Position metadata is 1-indexed.
        assert_eq!(
            pk[0].metadata.get(LANCE_UNENFORCED_PRIMARY_KEY_POSITION),
            Some(&"1".to_string())
        );

        // The primary key is immutable: re-setting it is rejected, whether to
        // the same column or a different one.
        let err = table
            .set_unenforced_primary_key(["id"])
            .await
            .expect_err("re-setting the same primary key should be rejected");
        assert!(matches!(err, Error::InvalidInput { .. }), "got {:?}", err);
        let err = table
            .set_unenforced_primary_key(["name"])
            .await
            .expect_err("changing the primary key should be rejected");
        assert!(matches!(err, Error::InvalidInput { .. }), "got {:?}", err);

        // The primary key is unchanged after the rejected calls.
        let lance_schema = table.as_native().unwrap().manifest().await.unwrap().schema;
        let pk = lance_schema.unenforced_primary_key();
        assert_eq!(pk.len(), 1);
        assert_eq!(pk[0].name, "id");
    }

    #[tokio::test]
    async fn test_set_unenforced_primary_key_concurrent() {
        let tmp_dir = tempdir().unwrap();
        let uri = tmp_dir.path().to_str().unwrap();

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(arrow_array::Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let reader: Box<dyn arrow_array::RecordBatchReader + Send> =
            Box::new(RecordBatchIterator::new(vec![Ok(batch)], schema.clone()));

        // A long read-consistency interval keeps each handle pinned to the
        // version it opened, so the second handle commits against a stale
        // base — the same situation as two processes racing.
        let conn = ConnectBuilder::new(uri)
            .read_consistency_interval(Duration::from_secs(3600))
            .execute()
            .await
            .unwrap();
        conn.create_table("t", reader).execute().await.unwrap();

        let table_a = conn.open_table("t").execute().await.unwrap();
        let table_b = conn.open_table("t").execute().await.unwrap();

        // Handle A sets the primary key first.
        table_a.set_unenforced_primary_key(["id"]).await.unwrap();

        // Handle B committed against a stale base that had no primary key, so
        // its own up-front check did not see A's key. The commit itself must
        // still fail rather than silently overriding A's primary key. (The
        // cross-process race on a *different* column is caught by the Lance
        // commit layer.)
        let err = table_b
            .set_unenforced_primary_key(["id"])
            .await
            .expect_err("concurrent primary key commit on a stale base should fail");
        assert!(
            !matches!(err, Error::InvalidInput { .. }),
            "expected a commit-time conflict, not an up-front input error: {:?}",
            err
        );

        // The committed primary key is exactly what A set — no corruption.
        let fresh = conn.open_table("t").execute().await.unwrap();
        let lance_schema = fresh.as_native().unwrap().manifest().await.unwrap().schema;
        let pk = lance_schema.unenforced_primary_key();
        assert_eq!(pk.len(), 1);
        assert_eq!(pk[0].name, "id");
    }

    #[tokio::test]
    async fn test_set_lsm_write_spec() {
        use arrow_array::StringArray;
        use lance::dataset::mem_wal::DatasetMemWalExt;

        let tmp_dir = tempdir().unwrap();
        let uri = tmp_dir.path().to_str().unwrap();

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(arrow_array::Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap();
        let reader: Box<dyn arrow_array::RecordBatchReader + Send> =
            Box::new(RecordBatchIterator::new(vec![Ok(batch)], schema.clone()));

        let conn = ConnectBuilder::new(uri)
            .read_consistency_interval(Duration::from_secs(0))
            .execute()
            .await
            .unwrap();
        let table = conn.create_table("t", reader).execute().await.unwrap();

        // Reject num_buckets out of range.
        for bad in [0u32, 1025] {
            let err = table
                .set_lsm_write_spec(LsmWriteSpec::bucket("id", bad))
                .await
                .expect_err("should reject");
            assert!(matches!(err, Error::Lance { .. }), "got {:?}", err);
        }

        // Happy path: install spec; verify MemWAL details record it.
        table
            .set_lsm_write_spec(LsmWriteSpec::bucket("id", 4))
            .await
            .unwrap();

        let native_tbl = table.as_native().unwrap();
        let dataset = native_tbl.dataset.get().await.unwrap();
        let details = dataset
            .mem_wal_index_details()
            .await
            .unwrap()
            .expect("MemWAL index should be initialized");
        assert_eq!(details.num_shards, 4);
        assert_eq!(details.sharding_specs.len(), 1);
        let installed = &details.sharding_specs[0];
        assert_eq!(installed.fields.len(), 1);
        let f = &installed.fields[0];
        assert_eq!(f.transform.as_deref(), Some("bucket"));
        assert_eq!(
            f.parameters.get("num_buckets").map(String::as_str),
            Some("4")
        );
        // Bucket parameters must hold only `num_buckets`.
        assert_eq!(f.parameters.len(), 1);

        // Mutation rejected.
        let err = table
            .set_lsm_write_spec(LsmWriteSpec::bucket("id", 8))
            .await
            .expect_err("mutation should be rejected");
        assert!(matches!(err, Error::InvalidInput { .. }), "got {:?}", err);
    }

    #[tokio::test]
    async fn test_set_lsm_write_spec_unsharded() {
        use lance::dataset::mem_wal::DatasetMemWalExt;

        let tmp_dir = tempdir().unwrap();
        let uri = tmp_dir.path().to_str().unwrap();

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(arrow_array::Int64Array::from(vec![1]))],
        )
        .unwrap();
        let reader: Box<dyn arrow_array::RecordBatchReader + Send> =
            Box::new(RecordBatchIterator::new(vec![Ok(batch)], schema.clone()));
        let conn = ConnectBuilder::new(uri)
            .read_consistency_interval(Duration::from_secs(0))
            .execute()
            .await
            .unwrap();
        let table = conn.create_table("t", reader).execute().await.unwrap();

        table
            .set_lsm_write_spec(LsmWriteSpec::unsharded())
            .await
            .unwrap();

        let dataset = table.as_native().unwrap().dataset.get().await.unwrap();
        let details = dataset
            .mem_wal_index_details()
            .await
            .unwrap()
            .expect("MemWAL index should be initialized");
        assert_eq!(details.num_shards, 1);
        assert_eq!(details.sharding_specs.len(), 1);
        let f = &details.sharding_specs[0].fields[0];
        assert_eq!(f.transform.as_deref(), Some("unsharded"));
        assert!(f.source_ids.is_empty());
    }

    #[tokio::test]
    async fn test_set_lsm_write_spec_identity() {
        use lance::dataset::mem_wal::DatasetMemWalExt;

        let tmp_dir = tempdir().unwrap();
        let uri = tmp_dir.path().to_str().unwrap();

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("region", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(arrow_array::Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap();
        let reader: Box<dyn arrow_array::RecordBatchReader + Send> =
            Box::new(RecordBatchIterator::new(vec![Ok(batch)], schema.clone()));
        let conn = ConnectBuilder::new(uri)
            .read_consistency_interval(Duration::from_secs(0))
            .execute()
            .await
            .unwrap();
        let table = conn.create_table("t", reader).execute().await.unwrap();

        table
            .set_lsm_write_spec(
                LsmWriteSpec::identity("region")
                    .with_writer_config_defaults([("durable_write", "false")]),
            )
            .await
            .unwrap();

        let dataset = table.as_native().unwrap().dataset.get().await.unwrap();
        let details = dataset
            .mem_wal_index_details()
            .await
            .unwrap()
            .expect("MemWAL index should be initialized");
        // Identity sharding records an open-ended shard count.
        assert_eq!(details.num_shards, 0);
        assert_eq!(details.sharding_specs.len(), 1);
        let f = &details.sharding_specs[0].fields[0];
        assert_eq!(f.transform.as_deref(), Some("identity"));
        // Writer config defaults round-trip into the MemWAL index.
        assert_eq!(
            details
                .writer_config_defaults
                .get("durable_write")
                .map(String::as_str),
            Some("false")
        );
    }

    #[tokio::test]
    async fn test_unset_lsm_write_spec() {
        use lance::dataset::mem_wal::DatasetMemWalExt;

        let tmp_dir = tempdir().unwrap();
        let uri = tmp_dir.path().to_str().unwrap();

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(arrow_array::Int64Array::from(vec![1]))],
        )
        .unwrap();
        let reader: Box<dyn arrow_array::RecordBatchReader + Send> =
            Box::new(RecordBatchIterator::new(vec![Ok(batch)], schema.clone()));
        let conn = ConnectBuilder::new(uri)
            .read_consistency_interval(Duration::from_secs(0))
            .execute()
            .await
            .unwrap();
        let table = conn.create_table("t", reader).execute().await.unwrap();

        // unset errors when no spec is set.
        table.unset_lsm_write_spec().await.unwrap_err();

        // Install a spec, then unset it.
        table
            .set_lsm_write_spec(LsmWriteSpec::bucket("id", 4))
            .await
            .unwrap();
        {
            let dataset = table.as_native().unwrap().dataset.get().await.unwrap();
            assert!(dataset.mem_wal_index_details().await.unwrap().is_some());
        }

        table.unset_lsm_write_spec().await.unwrap();
        {
            let dataset = table.as_native().unwrap().dataset.get().await.unwrap();
            assert!(dataset.mem_wal_index_details().await.unwrap().is_none());
        }

        // A second unset errors; a fresh spec can still be installed afterwards.
        table.unset_lsm_write_spec().await.unwrap_err();
        table
            .set_lsm_write_spec(LsmWriteSpec::bucket("id", 8))
            .await
            .unwrap();
        {
            let dataset = table.as_native().unwrap().dataset.get().await.unwrap();
            assert!(dataset.mem_wal_index_details().await.unwrap().is_some());
        }
    }

    #[tokio::test]
    pub async fn test_stats() {
        let tmp_dir = tempdir().unwrap();
        let uri = tmp_dir.path().to_str().unwrap();

        let conn = ConnectBuilder::new(uri).execute().await.unwrap();

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("foo", DataType::Int32, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from_iter_values(0..100)),
                Arc::new(Int32Array::from_iter_values(0..100)),
            ],
        )
        .unwrap();

        let table = conn
            .create_table("test_stats", batch.clone())
            .execute()
            .await
            .unwrap();
        for _ in 0..10 {
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from_iter_values(0..15)),
                    Arc::new(Int32Array::from_iter_values(0..15)),
                ],
            )
            .unwrap();
            table.add(batch.clone()).execute().await.unwrap();
        }

        let empty_table = conn
            .create_table("test_stats_empty", RecordBatch::new_empty(batch.schema()))
            .execute()
            .await
            .unwrap();

        let res = table.stats().await.unwrap();
        println!("{:#?}", res);
        assert_eq!(
            res,
            TableStatistics {
                num_rows: 250,
                num_indices: 0,
                total_bytes: 2300,
                fragment_stats: FragmentStatistics {
                    num_fragments: 11,
                    num_small_fragments: 11,
                    lengths: FragmentSummaryStats {
                        min: 15,
                        max: 100,
                        mean: 22,
                        p25: 15,
                        p50: 15,
                        p75: 15,
                        p99: 100,
                    },
                },
            }
        );
        let res = empty_table.stats().await.unwrap();
        println!("{:#?}", res);
        assert_eq!(
            res,
            TableStatistics {
                num_rows: 0,
                num_indices: 0,
                total_bytes: 0,
                fragment_stats: FragmentStatistics {
                    num_fragments: 0,
                    num_small_fragments: 0,
                    lengths: FragmentSummaryStats {
                        min: 0,
                        max: 0,
                        mean: 0,
                        p25: 0,
                        p50: 0,
                        p75: 0,
                        p99: 0,
                    },
                },
            }
        )
    }

    #[tokio::test]
    pub async fn test_list_indices_skip_frag_reuse() {
        let tmp_dir = tempdir().unwrap();
        let uri = tmp_dir.path().to_str().unwrap();

        let conn = ConnectBuilder::new(uri).execute().await.unwrap();

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("foo", DataType::Int32, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from_iter_values(0..100)),
                Arc::new(Int32Array::from_iter_values(0..100)),
            ],
        )
        .unwrap();

        let table = conn
            .create_table("test_list_indices_skip_frag_reuse", batch.clone())
            .execute()
            .await
            .unwrap();

        table.add(batch.clone()).execute().await.unwrap();

        table
            .create_index(&["id"], Index::Bitmap(BitmapIndexBuilder {}))
            .execute()
            .await
            .unwrap();

        table
            .optimize(OptimizeAction::Compact {
                options: CompactionOptions {
                    target_rows_per_fragment: 2_000,
                    defer_index_remap: true,
                    ..Default::default()
                },
                remap_options: None,
            })
            .await
            .unwrap();

        let result = table.list_indices().await.unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].index_type, crate::index::IndexType::Bitmap);
    }
}
