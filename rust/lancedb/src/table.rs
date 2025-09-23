// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! LanceDB Table APIs

use arrow::array::{AsArray, FixedSizeListBuilder, Float32Builder};
use arrow::datatypes::{Float32Type, UInt8Type};
use arrow_array::{RecordBatchIterator, RecordBatchReader};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion_expr::Expr;
use datafusion_physical_plan::display::DisplayableExecutionPlan;
use datafusion_physical_plan::projection::ProjectionExec;
use datafusion_physical_plan::repartition::RepartitionExec;
use datafusion_physical_plan::union::UnionExec;
use datafusion_physical_plan::ExecutionPlan;
use futures::{FutureExt, StreamExt, TryFutureExt};
use lance::dataset::builder::DatasetBuilder;
use lance::dataset::cleanup::RemovalStats;
use lance::dataset::optimize::{compact_files, CompactionMetrics, IndexRemapperOptions};
use lance::dataset::scanner::Scanner;
pub use lance::dataset::ColumnAlteration;
pub use lance::dataset::NewColumnTransform;
pub use lance::dataset::ReadParams;
pub use lance::dataset::Version;
use lance::dataset::{
    InsertBuilder, UpdateBuilder as LanceUpdateBuilder, WhenMatched, WriteMode, WriteParams,
};
use lance::dataset::{MergeInsertBuilder as LanceMergeInsertBuilder, WhenNotMatchedBySource};
use lance::index::vector::utils::infer_vector_dim;
use lance::index::vector::VectorIndexParams;
use lance::io::WrappingObjectStore;
use lance_datafusion::exec::{analyze_plan as lance_analyze_plan, execute_plan};
use lance_datafusion::utils::StreamingWriteSource;
use lance_index::scalar::{BuiltinIndexType, ScalarIndexParams};
use lance_index::vector::hnsw::builder::HnswBuildParams;
use lance_index::vector::ivf::IvfBuildParams;
use lance_index::vector::pq::PQBuildParams;
use lance_index::vector::sq::builder::SQBuildParams;
use lance_index::DatasetIndexExt;
use lance_index::IndexType;
use lance_table::format::Manifest;
use lance_table::io::commit::ManifestNamingScheme;
use log::info;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::format;
use std::path::Path;
use std::sync::Arc;

use crate::arrow::IntoArrow;
use crate::connection::NoData;
use crate::embeddings::{EmbeddingDefinition, EmbeddingRegistry, MaybeEmbedded, MemoryRegistry};
use crate::error::{Error, Result};
use crate::index::vector::{suggested_num_partitions_for_hnsw, VectorIndex};
use crate::index::IndexStatistics;
use crate::index::{
    vector::{suggested_num_partitions, suggested_num_sub_vectors},
    Index, IndexBuilder,
};
use crate::index::{IndexConfig, IndexStatisticsImpl};
use crate::query::{
    IntoQueryVector, Query, QueryExecutionOptions, QueryFilter, QueryRequest, Select, TakeQuery,
    VectorQuery, VectorQueryRequest, DEFAULT_TOP_K,
};
use crate::utils::{
    default_vector_column, supported_bitmap_data_type, supported_btree_data_type,
    supported_fts_data_type, supported_label_list_data_type, supported_vector_data_type,
    PatchReadParam, PatchWriteParam, TimeoutStream,
};

use self::dataset::DatasetConsistencyWrapper;
use self::merge::MergeInsertBuilder;

pub mod datafusion;
pub(crate) mod dataset;
pub mod merge;

use crate::index::waiter::wait_for_index;
pub use chrono::Duration;
use futures::future::{join_all, Either};
pub use lance::dataset::optimize::CompactionOptions;
pub use lance::dataset::refs::{TagContents, Tags as LanceTags};
pub use lance::dataset::scanner::DatasetRecordBatchStream;
use lance::dataset::statistics::DatasetStatisticsExt;
use lance_index::frag_reuse::FRAG_REUSE_INDEX_NAME;
pub use lance_index::optimize::OptimizeOptions;
use serde_with::skip_serializing_none;

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

/// Optimize the dataset.
///
/// Similar to `VACUUM` in PostgreSQL, it offers different options to
/// optimize different parts of the table on disk.
///
/// By default, it optimizes everything, as [`OptimizeAction::All`].
pub enum OptimizeAction {
    /// Run all optimizations with default values
    All,
    /// Compacts files in the dataset
    ///
    /// LanceDb uses a readonly filesystem for performance and safe concurrency.  Every time
    /// new data is added it will be added into new files.  Small files
    /// can hurt both read and write performance.  Compaction will merge small files
    /// into larger ones.
    ///
    /// All operations that modify data (add, delete, update, merge insert, etc.) will create
    /// new files.  If these operations are run frequently then compaction should run frequently.
    ///
    /// If these operations are never run (search only) then compaction is not necessary.
    Compact {
        options: CompactionOptions,
        remap_options: Option<Arc<dyn IndexRemapperOptions>>,
    },
    /// Prune old version of datasets
    ///
    /// Every change in LanceDb is additive.  When data is removed from a dataset a new version is
    /// created that doesn't contain the removed data.  However, the old version, which does contain
    /// the removed data, is left in place.  This is necessary for consistency and concurrency and
    /// also enables time travel functionality like the ability to checkout an older version of the
    /// dataset to undo changes.
    ///
    /// Over time, these old versions can consume a lot of disk space.  The prune operation will
    /// remove versions of the dataset that are older than a certain age.  This will free up the
    /// space used by that old data.
    ///
    /// Once a version is pruned it can no longer be checked out.
    Prune {
        /// The duration of time to keep versions of the dataset.
        older_than: Option<Duration>,
        /// Because they may be part of an in-progress transaction, files newer than 7 days old are not deleted by default.
        /// If you are sure that there are no in-progress transactions, then you can set this to True to delete all files older than `older_than`.
        delete_unverified: Option<bool>,
        /// If true, an error will be returned if there are any old versions that are still tagged.
        error_if_tagged_old_versions: Option<bool>,
    },
    /// Optimize the indices
    ///
    /// This operation optimizes all indices in the table.  When new data is added to LanceDb
    /// it is not added to the indices.  However, it can still turn up in searches because the search
    /// function will scan both the indexed data and the unindexed data in parallel.  Over time, the
    /// unindexed data can become large enough that the search performance is slow.  This operation
    /// will add the unindexed data to the indices without rerunning the full index creation process.
    ///
    /// Optimizing an index is faster than re-training the index but it does not typically adjust the
    /// underlying model relied upon by the index.  This can eventually lead to poor search accuracy
    /// and so users may still want to occasionally retrain the index after adding a large amount of
    /// data.
    ///
    /// For example, when using IVF, an index will create clusters.  Optimizing an index assigns unindexed
    /// data to the existing clusters, but it does not move the clusters or create new clusters.
    Index(OptimizeOptions),
}

impl Default for OptimizeAction {
    fn default() -> Self {
        Self::All
    }
}

/// Statistics about the optimization.
pub struct OptimizeStats {
    /// Stats of the file compaction.
    pub compaction: Option<CompactionMetrics>,

    /// Stats of the version pruning
    pub prune: Option<RemovalStats>,
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

#[derive(Debug, Clone, Default)]
pub enum AddDataMode {
    /// Rows will be appended to the table (the default)
    #[default]
    Append,
    /// The existing table will be overwritten with the new data
    Overwrite,
}

/// A builder for configuring a [`crate::connection::Connection::create_table`] or [`Table::add`]
/// operation
pub struct AddDataBuilder<T: IntoArrow> {
    parent: Arc<dyn BaseTable>,
    pub(crate) data: T,
    pub(crate) mode: AddDataMode,
    pub(crate) write_options: WriteOptions,
    embedding_registry: Option<Arc<dyn EmbeddingRegistry>>,
}

impl<T: IntoArrow> std::fmt::Debug for AddDataBuilder<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AddDataBuilder")
            .field("parent", &self.parent)
            .field("mode", &self.mode)
            .field("write_options", &self.write_options)
            .finish()
    }
}

impl<T: IntoArrow> AddDataBuilder<T> {
    pub fn mode(mut self, mode: AddDataMode) -> Self {
        self.mode = mode;
        self
    }

    pub fn write_options(mut self, options: WriteOptions) -> Self {
        self.write_options = options;
        self
    }

    pub async fn execute(self) -> Result<AddResult> {
        let parent = self.parent.clone();
        let data = self.data.into_arrow()?;
        let without_data = AddDataBuilder::<NoData> {
            data: NoData {},
            mode: self.mode,
            parent: self.parent,
            write_options: self.write_options,
            embedding_registry: self.embedding_registry,
        };
        parent.add(without_data, data).await
    }
}

/// A builder for configuring an [`Table::update`] operation
#[derive(Debug, Clone)]
pub struct UpdateBuilder {
    parent: Arc<dyn BaseTable>,
    pub(crate) filter: Option<String>,
    pub(crate) columns: Vec<(String, String)>,
}

impl UpdateBuilder {
    fn new(parent: Arc<dyn BaseTable>) -> Self {
        Self {
            parent,
            filter: None,
            columns: Vec::new(),
        }
    }

    /// Limits the update operation to rows matching the given filter
    ///
    /// If a row does not match the filter then it will be left unchanged.
    pub fn only_if(mut self, filter: impl Into<String>) -> Self {
        self.filter = Some(filter.into());
        self
    }

    /// Specifies a column to update
    ///
    /// This method may be called multiple times to update multiple columns
    ///
    /// The `update_expr` should be an SQL expression explaining how to calculate
    /// the new value for the column.  The expression will be evaluated against the
    /// previous row's value.
    ///
    /// # Examples
    ///
    /// ```
    /// # use lancedb::Table;
    /// # async fn doctest_helper(tbl: Table) {
    ///   let mut operation = tbl.update();
    ///   // Increments the `bird_count` value by 1
    ///   operation = operation.column("bird_count", "bird_count + 1");
    ///   operation.execute().await.unwrap();
    /// # }
    /// ```
    pub fn column(
        mut self,
        column_name: impl Into<String>,
        update_expr: impl Into<String>,
    ) -> Self {
        self.columns.push((column_name.into(), update_expr.into()));
        self
    }

    /// Executes the update operation.
    /// Returns the update result
    pub async fn execute(self) -> Result<UpdateResult> {
        if self.columns.is_empty() {
            Err(Error::InvalidInput {
                message: "at least one column must be specified in an update operation".to_string(),
            })
        } else {
            self.parent.clone().update(self).await
        }
    }
}

/// Filters that can be used to limit the rows returned by a query
pub enum Filter {
    /// A SQL filter string
    Sql(String),
    /// A Datafusion logical expression
    Datafusion(Expr),
}

/// A query that can be used to search a LanceDB table
#[derive(Debug, Clone)]
pub enum AnyQuery {
    Query(QueryRequest),
    VectorQuery(VectorQueryRequest),
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct UpdateResult {
    #[serde(default)]
    pub rows_updated: u64,
    // The commit version associated with the operation.
    // A version of `0` indicates compatibility with legacy servers that do not return
    /// a commit version.
    #[serde(default)]
    pub version: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct AddResult {
    // The commit version associated with the operation.
    // A version of `0` indicates compatibility with legacy servers that do not return
    /// a commit version.
    #[serde(default)]
    pub version: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct DeleteResult {
    // The commit version associated with the operation.
    // A version of `0` indicates compatibility with legacy servers that do not return
    /// a commit version.
    #[serde(default)]
    pub version: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct MergeResult {
    // The commit version associated with the operation.
    // A version of `0` indicates compatibility with legacy servers that do not return
    /// a commit version.
    #[serde(default)]
    pub version: u64,
    /// Number of inserted rows (for user statistics)
    #[serde(default)]
    pub num_inserted_rows: u64,
    /// Number of updated rows (for user statistics)
    #[serde(default)]
    pub num_updated_rows: u64,
    /// Number of deleted rows (for user statistics)
    /// Note: This is different from internal references to 'deleted_rows', since we technically "delete" updated rows during processing.
    /// However those rows are not shared with the user.
    #[serde(default)]
    pub num_deleted_rows: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct AddColumnsResult {
    // The commit version associated with the operation.
    // A version of `0` indicates compatibility with legacy servers that do not return
    /// a commit version.
    #[serde(default)]
    pub version: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct AlterColumnsResult {
    // The commit version associated with the operation.
    // A version of `0` indicates compatibility with legacy servers that do not return
    /// a commit version.
    #[serde(default)]
    pub version: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct DropColumnsResult {
    // The commit version associated with the operation.
    // A version of `0` indicates compatibility with legacy servers that do not return
    /// a commit version.
    #[serde(default)]
    pub version: u64,
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
    async fn add(
        &self,
        add: AddDataBuilder<NoData>,
        data: Box<dyn arrow_array::RecordBatchReader + Send>,
    ) -> Result<AddResult>;
    /// Delete rows from the table.
    async fn delete(&self, predicate: &str) -> Result<DeleteResult>;
    /// Update rows in the table.
    async fn update(&self, update: UpdateBuilder) -> Result<UpdateResult>;
    /// Create an index on the provided column(s).
    async fn create_index(&self, index: IndexBuilder) -> Result<()>;
    /// List the indices on the table.
    async fn list_indices(&self) -> Result<Vec<IndexConfig>>;
    /// Drop an index from the table.
    async fn drop_index(&self, name: &str) -> Result<()>;
    /// Prewarm an index in the table
    async fn prewarm_index(&self, name: &str) -> Result<()>;
    /// Get statistics about the index.
    async fn index_stats(&self, index_name: &str) -> Result<Option<IndexStatistics>>;
    /// Merge insert new records into the table.
    async fn merge_insert(
        &self,
        params: MergeInsertBuilder,
        new_data: Box<dyn RecordBatchReader + Send>,
    ) -> Result<MergeResult>;
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
    /// Get the table definition.
    async fn table_definition(&self) -> Result<TableDefinition>;
    /// Get the table URI
    fn dataset_uri(&self) -> &str;
    /// Poll until the columns are fully indexed. Will return Error::Timeout if the columns
    /// are not fully indexed within the timeout.
    async fn wait_for_index(
        &self,
        index_names: &[&str],
        timeout: std::time::Duration,
    ) -> Result<()>;
    /// Get statistics on the table
    async fn stats(&self) -> Result<TableStatistics>;
}

/// A Table is a collection of strong typed Rows.
///
/// The type of the each row is defined in Apache Arrow [Schema].
#[derive(Clone)]
pub struct Table {
    inner: Arc<dyn BaseTable>,
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
                handler,
                None,
            ));
            Self {
                inner,
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
                handler,
                Some(version),
            ));
            Self {
                inner,
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

impl Table {
    pub fn new(inner: Arc<dyn BaseTable>) -> Self {
        Self {
            inner,
            embedding_registry: Arc::new(MemoryRegistry::new()),
        }
    }

    pub fn base_table(&self) -> &Arc<dyn BaseTable> {
        &self.inner
    }

    pub(crate) fn new_with_embedding_registry(
        inner: Arc<dyn BaseTable>,
        embedding_registry: Arc<dyn EmbeddingRegistry>,
    ) -> Self {
        Self {
            inner,
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
    /// * `batches` data to be added to the Table
    /// * `options` options to control how data is added
    pub fn add<T: IntoArrow>(&self, batches: T) -> AddDataBuilder<T> {
        AddDataBuilder {
            parent: self.inner.clone(),
            data: batches,
            mode: AddDataMode::Append,
            write_options: WriteOptions::default(),
            embedding_registry: Some(self.embedding_registry.clone()),
        }
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
    /// - `predicate` - The SQL predicate string to filter the rows to be deleted.
    ///
    /// # Example
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
    /// # let schema = Arc::new(Schema::new(vec![
    /// #  Field::new("id", DataType::Int32, false),
    /// #  Field::new("vector", DataType::FixedSizeList(
    /// #    Arc::new(Field::new("item", DataType::Float32, true)), 128), true),
    /// # ]));
    /// let batches = RecordBatchIterator::new(
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
    /// let tbl = db
    ///     .create_table("delete_test", Box::new(batches))
    ///     .execute()
    ///     .await
    ///     .unwrap();
    /// tbl.delete("id > 5").await.unwrap();
    /// # });
    /// ```
    pub async fn delete(&self, predicate: &str) -> Result<DeleteResult> {
        self.inner.delete(predicate).await
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
    /// <section class="warning">Experimental API</section>
    ///
    /// The optimization process is undergoing active development and may change.
    /// Our goal with these changes is to improve the performance of optimization and
    /// reduce the complexity.
    ///
    /// That being said, it is essential today to run optimize if you want the best
    /// performance.  It should be stable and safe to use in production, but it our
    /// hope that the API may be simplified (or not even need to be called) in the future.
    ///
    /// The frequency an application shoudl call optimize is based on the frequency of
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

    /// Remove columns from the table.
    pub async fn drop_columns(&self, columns: &[&str]) -> Result<DropColumnsResult> {
        self.inner.drop_columns(columns).await
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

    /// Get the underlying dataset URI
    ///
    /// Warning: This is an internal API and the return value is subject to change.
    pub fn dataset_uri(&self) -> &str {
        self.inner.dataset_uri()
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

    /// Prewarm an index in the table
    ///
    /// This is a hint to fully load the index into memory.  It can be used to
    /// avoid cold starts
    ///
    /// It is generally wasteful to call this if the index does not fit into the
    /// available cache.
    ///
    /// Note: This function is not yet supported on all indices, in which case it
    /// may do nothing.
    ///
    /// Use [`Self::list_indices()`] to find the names of the indices.
    pub async fn prewarm_index(&self, name: &str) -> Result<()> {
        self.inner.prewarm_index(name).await
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

    // Take many execution plans and map them into a single plan that adds
    // a query_index column and unions them.
    pub(crate) fn multi_vector_plan(
        plans: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if plans.is_empty() {
            return Err(Error::InvalidInput {
                message: "No plans provided".to_string(),
            });
        }
        // Projection to keeping all existing columns
        let first_plan = plans[0].clone();
        let project_all_columns = first_plan
            .schema()
            .fields()
            .iter()
            .enumerate()
            .map(|(i, field)| {
                let expr =
                    datafusion_physical_plan::expressions::Column::new(field.name().as_str(), i);
                let expr = Arc::new(expr) as Arc<dyn datafusion_physical_plan::PhysicalExpr>;
                (expr, field.name().clone())
            })
            .collect::<Vec<_>>();

        let projected_plans = plans
            .into_iter()
            .enumerate()
            .map(|(plan_i, plan)| {
                let query_index = datafusion_common::ScalarValue::Int32(Some(plan_i as i32));
                let query_index_expr =
                    datafusion_physical_plan::expressions::Literal::new(query_index);
                let query_index_expr =
                    Arc::new(query_index_expr) as Arc<dyn datafusion_physical_plan::PhysicalExpr>;
                let mut projections = vec![(query_index_expr, "query_index".to_string())];
                projections.extend_from_slice(&project_all_columns);
                let projection = ProjectionExec::try_new(projections, plan).unwrap();
                Arc::new(projection) as Arc<dyn datafusion_physical_plan::ExecutionPlan>
            })
            .collect::<Vec<_>>();

        let unioned = Arc::new(UnionExec::new(projected_plans));
        // We require 1 partition in the final output
        let repartitioned = RepartitionExec::try_new(
            unioned,
            datafusion_physical_plan::Partitioning::RoundRobinBatch(1),
        )
        .unwrap();
        Ok(Arc::new(repartitioned))
    }

    /// Retrieve statistics on the table
    pub async fn stats(&self) -> Result<TableStatistics> {
        self.inner.stats().await
    }
}

pub struct NativeTags {
    inner: LanceTags,
}
#[async_trait]
impl Tags for NativeTags {
    async fn list(&self) -> Result<HashMap<String, TagContents>> {
        Ok(self.inner.list().await?)
    }

    async fn get_version(&self, tag: &str) -> Result<u64> {
        Ok(self.inner.get_version(tag).await?)
    }

    async fn create(&mut self, tag: &str, version: u64) -> Result<()> {
        self.inner.create(tag, version).await?;
        Ok(())
    }

    async fn delete(&mut self, tag: &str) -> Result<()> {
        self.inner.delete(tag).await?;
        Ok(())
    }

    async fn update(&mut self, tag: &str, version: u64) -> Result<()> {
        self.inner.update(tag, version).await?;
        Ok(())
    }
}

impl From<NativeTable> for Table {
    fn from(table: NativeTable) -> Self {
        Self::new(Arc::new(table))
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
#[derive(Debug, Clone)]
pub struct NativeTable {
    name: String,
    uri: String,
    pub(crate) dataset: dataset::DatasetConsistencyWrapper,
    // This comes from the connection options. We store here so we can pass down
    // to the dataset when we recreate it (for example, in checkout_latest).
    read_consistency_interval: Option<std::time::Duration>,
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
        Self::open_with_params(uri, &name, None, None, None).await
    }

    /// Opens an existing Table
    ///
    /// # Arguments
    ///
    /// * `base_path` - The base path where the table is located
    /// * `name` The Table name
    /// * `params` The [ReadParams] to use when opening the table
    ///
    /// # Returns
    ///
    /// * A [NativeTable] object.
    pub async fn open_with_params(
        uri: &str,
        name: &str,
        write_store_wrapper: Option<Arc<dyn WrappingObjectStore>>,
        params: Option<ReadParams>,
        read_consistency_interval: Option<std::time::Duration>,
    ) -> Result<Self> {
        let params = params.unwrap_or_default();
        // patch the params if we have a write store wrapper
        let params = match write_store_wrapper.clone() {
            Some(wrapper) => params.patch_with_store_wrapper(wrapper)?,
            None => params,
        };

        let dataset = DatasetBuilder::from_uri(uri)
            .with_read_params(params)
            .load()
            .await
            .map_err(|e| match e {
                lance::Error::DatasetNotFound { .. } => Error::TableNotFound {
                    name: name.to_string(),
                },
                source => Error::Lance { source },
            })?;

        let dataset = DatasetConsistencyWrapper::new_latest(dataset, read_consistency_interval);

        Ok(Self {
            name: name.to_string(),
            uri: uri.to_string(),
            dataset,
            read_consistency_interval,
        })
    }

    fn get_table_name(uri: &str) -> Result<String> {
        let path = Path::new(uri);
        let name = path
            .file_stem()
            .ok_or(Error::TableNotFound {
                name: uri.to_string(),
            })?
            .to_str()
            .ok_or(Error::InvalidTableName {
                name: uri.to_string(),
                reason: "Table name is not valid URL".to_string(),
            })?;
        Ok(name.to_string())
    }

    /// Creates a new Table
    ///
    /// # Arguments
    ///
    /// * `uri` - The URI to the table.
    /// * `name` The Table name
    /// * `batches` RecordBatch to be saved in the database.
    /// * `params` - Write parameters.
    ///
    /// # Returns
    ///
    /// * A [TableImpl] object.
    pub async fn create(
        uri: &str,
        name: &str,
        batches: impl StreamingWriteSource,
        write_store_wrapper: Option<Arc<dyn WrappingObjectStore>>,
        params: Option<WriteParams>,
        read_consistency_interval: Option<std::time::Duration>,
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
                source => Error::Lance { source },
            })?;

        Ok(Self {
            name: name.to_string(),
            uri: uri.to_string(),
            dataset: DatasetConsistencyWrapper::new_latest(dataset, read_consistency_interval),
            read_consistency_interval,
        })
    }

    pub async fn create_empty(
        uri: &str,
        name: &str,
        schema: SchemaRef,
        write_store_wrapper: Option<Arc<dyn WrappingObjectStore>>,
        params: Option<WriteParams>,
        read_consistency_interval: Option<std::time::Duration>,
    ) -> Result<Self> {
        let batches = RecordBatchIterator::new(vec![], schema);
        Self::create(
            uri,
            name,
            batches,
            write_store_wrapper,
            params,
            read_consistency_interval,
        )
        .await
    }

    async fn optimize_indices(&self, options: &OptimizeOptions) -> Result<()> {
        info!("LanceDB: optimizing indices: {:?}", options);
        self.dataset
            .get_mut()
            .await?
            .optimize_indices(options)
            .await?;
        Ok(())
    }

    /// Merge new data into this table.
    pub async fn merge(
        &mut self,
        batches: impl RecordBatchReader + Send + 'static,
        left_on: &str,
        right_on: &str,
    ) -> Result<()> {
        self.dataset
            .get_mut()
            .await?
            .merge(batches, left_on, right_on)
            .await?;
        Ok(())
    }

    /// Remove old versions of the dataset from disk.
    ///
    /// # Arguments
    /// * `older_than` - The duration of time to keep versions of the dataset.
    /// * `delete_unverified` - Because they may be part of an in-progress
    ///   transaction, files newer than 7 days old are not deleted by default.
    ///   If you are sure that there are no in-progress transactions, then you
    ///   can set this to True to delete all files older than `older_than`.
    ///
    /// This calls into [lance::dataset::Dataset::cleanup_old_versions] and
    /// returns the result.
    async fn cleanup_old_versions(
        &self,
        older_than: Duration,
        delete_unverified: Option<bool>,
        error_if_tagged_old_versions: Option<bool>,
    ) -> Result<RemovalStats> {
        Ok(self
            .dataset
            .get_mut()
            .await?
            .cleanup_old_versions(older_than, delete_unverified, error_if_tagged_old_versions)
            .await?)
    }

    /// Compact files in the dataset.
    ///
    /// This can be run after making several small appends to optimize the table
    /// for faster reads.
    ///
    /// This calls into [lance::dataset::optimize::compact_files].
    async fn compact_files(
        &self,
        options: CompactionOptions,
        remap_options: Option<Arc<dyn IndexRemapperOptions>>,
    ) -> Result<CompactionMetrics> {
        let mut dataset_mut = self.dataset.get_mut().await?;
        let metrics = compact_files(&mut dataset_mut, options, remap_options).await?;
        Ok(metrics)
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

    // Helper to get num_partitions with default calculation
    async fn get_num_partitions(
        &self,
        provided: Option<u32>,
        for_hnsw: bool,
        dim: Option<u32>,
    ) -> Result<u32> {
        if let Some(n) = provided {
            Ok(n)
        } else {
            let row_count = self.count_rows(None).await?;
            if for_hnsw {
                Ok(suggested_num_partitions_for_hnsw(
                    row_count,
                    dim.ok_or_else(|| Error::InvalidInput {
                        message: "Vector dimension required for HNSW partitioning".to_string(),
                    })?,
                ))
            } else {
                Ok(suggested_num_partitions(row_count))
            }
        }
    }

    // Helper to get num_sub_vectors with default calculation
    fn get_num_sub_vectors(provided: Option<u32>, dim: u32) -> u32 {
        provided.unwrap_or_else(|| suggested_num_sub_vectors(dim))
    }

    // Helper to extract vector dimension from field
    fn get_vector_dimension(field: &Field) -> Result<u32> {
        match field.data_type() {
            arrow_schema::DataType::FixedSizeList(_, n) => Ok(*n as u32),
            _ => Ok(infer_vector_dim(field.data_type())? as u32),
        }
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
                    let num_partitions = self.get_num_partitions(None, false, None).await?;
                    let num_sub_vectors = Self::get_num_sub_vectors(None, dim);
                    let lance_idx_params = lance::index::vector::VectorIndexParams::ivf_pq(
                        num_partitions as usize,
                        /*num_bits=*/ 8,
                        num_sub_vectors as usize,
                        lance_linalg::distance::MetricType::L2,
                        /*max_iterations=*/ 50,
                    );
                    Ok(Box::new(lance_idx_params))
                } else if supported_btree_data_type(field.data_type()) {
                    Ok(Box::new(ScalarIndexParams::for_builtin(
                        BuiltinIndexType::BTree,
                    )))
                } else {
                    return Err(Error::InvalidInput {
                        message: format!(
                            "there are no indices supported for the field `{}` with the data type {}",
                            field.name(),
                            field.data_type()
                        ),
                    });
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
            Index::FTS(fts_opts) => {
                Self::validate_index_type(field, "FTS", supported_fts_data_type)?;
                Ok(Box::new(fts_opts))
            }
            Index::IvfFlat(index) => {
                Self::validate_index_type(field, "IVF Flat", supported_vector_data_type)?;
                let num_partitions = self
                    .get_num_partitions(index.num_partitions, false, None)
                    .await?;
                let lance_idx_params = VectorIndexParams::ivf_flat(
                    num_partitions as usize,
                    index.distance_type.into(),
                );
                Ok(Box::new(lance_idx_params))
            }
            Index::IvfPq(index) => {
                Self::validate_index_type(field, "IVF PQ", supported_vector_data_type)?;
                let dim = Self::get_vector_dimension(field)?;
                let num_partitions = self
                    .get_num_partitions(index.num_partitions, false, None)
                    .await?;
                let num_sub_vectors = Self::get_num_sub_vectors(index.num_sub_vectors, dim);
                let lance_idx_params = VectorIndexParams::ivf_pq(
                    num_partitions as usize,
                    /*num_bits=*/ 8,
                    num_sub_vectors as usize,
                    index.distance_type.into(),
                    index.max_iterations as usize,
                );
                Ok(Box::new(lance_idx_params))
            }
            Index::IvfHnswPq(index) => {
                Self::validate_index_type(field, "IVF HNSW PQ", supported_vector_data_type)?;
                let dim = Self::get_vector_dimension(field)?;
                let num_partitions = self
                    .get_num_partitions(index.num_partitions, true, Some(dim))
                    .await?;
                let num_sub_vectors = Self::get_num_sub_vectors(index.num_sub_vectors, dim);
                let mut ivf_params = IvfBuildParams::new(num_partitions as usize);
                ivf_params.sample_rate = index.sample_rate as usize;
                ivf_params.max_iters = index.max_iterations as usize;
                let hnsw_params = HnswBuildParams::default()
                    .num_edges(index.m as usize)
                    .ef_construction(index.ef_construction as usize);
                let pq_params = PQBuildParams {
                    num_sub_vectors: num_sub_vectors as usize,
                    ..Default::default()
                };
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
                let dim = Self::get_vector_dimension(field)?;
                let num_partitions = self
                    .get_num_partitions(index.num_partitions, true, Some(dim))
                    .await?;
                let mut ivf_params = IvfBuildParams::new(num_partitions as usize);
                ivf_params.sample_rate = index.sample_rate as usize;
                ivf_params.max_iters = index.max_iterations as usize;
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
            Index::FTS(_) => IndexType::Inverted,
            Index::IvfFlat(_) | Index::IvfPq(_) | Index::IvfHnswPq(_) | Index::IvfHnswSq(_) => {
                IndexType::Vector
            }
        }
    }

    async fn generic_query(
        &self,
        query: &AnyQuery,
        options: QueryExecutionOptions,
    ) -> Result<DatasetRecordBatchStream> {
        let plan = self.create_plan(query, options.clone()).await?;
        let inner = execute_plan(plan, Default::default())?;
        let inner = if let Some(timeout) = options.timeout {
            TimeoutStream::new_boxed(inner, timeout)
        } else {
            inner
        };
        Ok(DatasetRecordBatchStream::new(inner))
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
        let mut dataset = self.dataset.get_mut().await?;
        dataset.migrate_manifest_paths_v2().await?;
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
        let mut dataset = self.dataset.get_mut().await?;
        dataset.update_config(upsert_values).await?;
        Ok(())
    }

    /// Delete keys from the config
    pub async fn delete_config_keys(&self, delete_keys: &[&str]) -> Result<()> {
        let mut dataset = self.dataset.get_mut().await?;
        // TODO: update this when we implement metadata APIs
        #[allow(deprecated)]
        dataset.delete_config_keys(delete_keys).await?;
        Ok(())
    }

    /// Update schema metadata
    pub async fn replace_schema_metadata(
        &self,
        upsert_values: impl IntoIterator<Item = (String, String)>,
    ) -> Result<()> {
        let mut dataset = self.dataset.get_mut().await?;
        // TODO: update this when we implement metadata APIs
        #[allow(deprecated)]
        dataset.replace_schema_metadata(upsert_values).await?;
        Ok(())
    }

    /// Update field metadata
    ///
    /// # Arguments:
    /// * `new_values` - An iterator of tuples where the first element is the
    ///   field id and the second element is a hashmap of metadata key-value
    ///   pairs.
    ///
    pub async fn replace_field_metadata(
        &self,
        new_values: impl IntoIterator<Item = (u32, HashMap<String, String>)>,
    ) -> Result<()> {
        let mut dataset = self.dataset.get_mut().await?;
        dataset.replace_field_metadata(new_values).await?;
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
        // Native tables don't support namespaces yet, return empty slice for root namespace
        &[]
    }

    fn id(&self) -> &str {
        // For native tables, id is same as name since no namespace support
        self.name.as_str()
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
        self.dataset
            .as_latest(self.read_consistency_interval)
            .await?;
        self.dataset.reload().await
    }

    async fn list_versions(&self) -> Result<Vec<Version>> {
        Ok(self.dataset.get().await?.versions().await?)
    }

    async fn restore(&self) -> Result<()> {
        let version =
            self.dataset
                .time_travel_version()
                .await
                .ok_or_else(|| Error::InvalidInput {
                    message: "you must run checkout before running restore".to_string(),
                })?;
        {
            // Use get_mut_unchecked as restore is the only "write" operation that is allowed
            // when the table is in time travel mode.
            // Also, drop the guard after .restore because as_latest will need it
            let mut dataset = self.dataset.get_mut_unchecked().await?;
            debug_assert_eq!(dataset.version().version, version);
            dataset.restore().await?;
        }
        self.dataset
            .as_latest(self.read_consistency_interval)
            .await?;
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

    async fn add(
        &self,
        add: AddDataBuilder<NoData>,
        data: Box<dyn RecordBatchReader + Send>,
    ) -> Result<AddResult> {
        let data = Box::new(MaybeEmbedded::try_new(
            data,
            self.table_definition().await?,
            add.embedding_registry,
        )?) as Box<dyn RecordBatchReader + Send>;

        let lance_params = add.write_options.lance_write_params.unwrap_or(WriteParams {
            mode: match add.mode {
                AddDataMode::Append => WriteMode::Append,
                AddDataMode::Overwrite => WriteMode::Overwrite,
            },
            ..Default::default()
        });

        let dataset = {
            // Limited scope for the mutable borrow of self.dataset avoids deadlock.
            let ds = self.dataset.get_mut().await?;
            InsertBuilder::new(Arc::new(ds.clone()))
                .with_params(&lance_params)
                .execute_stream(data)
                .await?
        };
        let version = dataset.manifest().version;
        self.dataset.set_latest(dataset).await;
        Ok(AddResult { version })
    }

    async fn create_index(&self, opts: IndexBuilder) -> Result<()> {
        if opts.columns.len() != 1 {
            return Err(Error::Schema {
                message: "Multi-column (composite) indices are not yet supported".to_string(),
            });
        }
        let schema = self.schema().await?;

        let field = schema.field_with_name(&opts.columns[0])?;

        let lance_idx_params = self.make_index_params(field, opts.index.clone()).await?;
        let index_type = self.get_index_type_for_field(field, &opts.index);
        let columns = [field.name().as_str()];
        let mut dataset = self.dataset.get_mut().await?;
        let mut builder = dataset
            .create_index_builder(&columns, index_type, lance_idx_params.as_ref())
            .train(opts.train)
            .replace(opts.replace);

        if let Some(name) = opts.name {
            builder = builder.name(name);
        }
        builder.await?;
        Ok(())
    }

    async fn drop_index(&self, index_name: &str) -> Result<()> {
        let mut dataset = self.dataset.get_mut().await?;
        dataset.drop_index(index_name).await?;
        Ok(())
    }

    async fn prewarm_index(&self, index_name: &str) -> Result<()> {
        let dataset = self.dataset.get().await?;
        Ok(dataset.prewarm_index(index_name).await?)
    }

    async fn update(&self, update: UpdateBuilder) -> Result<UpdateResult> {
        let dataset = self.dataset.get().await?.clone();
        let mut builder = LanceUpdateBuilder::new(Arc::new(dataset));
        if let Some(predicate) = update.filter {
            builder = builder.update_where(&predicate)?;
        }

        for (column, value) in update.columns {
            builder = builder.set(column, &value)?;
        }

        let operation = builder.build()?;
        let res = operation.execute().await?;
        self.dataset
            .set_latest(res.new_dataset.as_ref().clone())
            .await;
        Ok(UpdateResult {
            rows_updated: res.rows_updated,
            version: res.new_dataset.version().version,
        })
    }

    async fn create_plan(
        &self,
        query: &AnyQuery,
        options: QueryExecutionOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let query = match query {
            AnyQuery::VectorQuery(query) => query.clone(),
            AnyQuery::Query(query) => VectorQueryRequest::from_plain_query(query.clone()),
        };

        let ds_ref = self.dataset.get().await?;
        let schema = ds_ref.schema();
        let mut column = query.column.clone();

        let mut query_vector = query.query_vector.first().cloned();
        if query.query_vector.len() > 1 {
            if column.is_none() {
                // Infer a vector column with the same dimension of the query vector.
                let arrow_schema = Schema::from(ds_ref.schema());
                column = Some(default_vector_column(
                    &arrow_schema,
                    Some(query.query_vector[0].len() as i32),
                )?);
            }
            let vector_field = schema.field(column.as_ref().unwrap()).unwrap();
            if let DataType::List(_) = vector_field.data_type() {
                // it's multivector, then the vectors should be treated as single query
                // concatenate the vectors into a FixedSizeList<FixedSizeList<_>>
                // it's also possible to concatenate the vectors into a List<FixedSizeList<_>>,
                // but FixedSizeList is more efficient and easier to construct
                let vectors = query
                    .query_vector
                    .iter()
                    .map(|arr| arr.as_ref())
                    .collect::<Vec<_>>();
                let dim = vectors[0].len();
                let mut fsl_builder = FixedSizeListBuilder::with_capacity(
                    Float32Builder::with_capacity(dim),
                    dim as i32,
                    vectors.len(),
                );
                for vec in vectors {
                    fsl_builder
                        .values()
                        .append_slice(vec.as_primitive::<Float32Type>().values());
                    fsl_builder.append(true);
                }
                query_vector = Some(Arc::new(fsl_builder.finish()));
            } else {
                // If there are multiple query vectors, create a plan for each of them and union them.
                let query_vecs = query.query_vector.clone();
                let plan_futures = query_vecs
                    .into_iter()
                    .map(|query_vector| {
                        let mut sub_query = query.clone();
                        sub_query.query_vector = vec![query_vector];
                        let options_ref = options.clone();
                        async move {
                            self.create_plan(&AnyQuery::VectorQuery(sub_query), options_ref)
                                .await
                        }
                    })
                    .collect::<Vec<_>>();
                let plans = futures::future::try_join_all(plan_futures).await?;
                return Table::multi_vector_plan(plans);
            }
        }

        let mut scanner: Scanner = ds_ref.scan();

        if let Some(query_vector) = query_vector {
            // If there is a vector query, default to limit=10 if unspecified
            let column = if let Some(col) = column {
                col
            } else {
                // Infer a vector column with the same dimension of the query vector.
                let arrow_schema = Schema::from(ds_ref.schema());
                default_vector_column(&arrow_schema, Some(query_vector.len() as i32))?
            };

            let (_, element_type) = lance::index::vector::utils::get_vector_type(schema, &column)?;
            let is_binary = matches!(element_type, DataType::UInt8);
            let top_k = query.base.limit.unwrap_or(DEFAULT_TOP_K) + query.base.offset.unwrap_or(0);
            if is_binary {
                let query_vector = arrow::compute::cast(&query_vector, &DataType::UInt8)?;
                let query_vector = query_vector.as_primitive::<UInt8Type>();
                scanner.nearest(&column, query_vector, top_k)?;
            } else {
                scanner.nearest(&column, query_vector.as_ref(), top_k)?;
            }
            scanner.minimum_nprobes(query.minimum_nprobes);
            if let Some(maximum_nprobes) = query.maximum_nprobes {
                scanner.maximum_nprobes(maximum_nprobes);
            }
        }
        scanner.limit(
            query.base.limit.map(|limit| limit as i64),
            query.base.offset.map(|offset| offset as i64),
        )?;
        if let Some(ef) = query.ef {
            scanner.ef(ef);
        }
        scanner.distance_range(query.lower_bound, query.upper_bound);
        scanner.use_index(query.use_index);
        scanner.prefilter(query.base.prefilter);
        match query.base.select {
            Select::Columns(ref columns) => {
                scanner.project(columns.as_slice())?;
            }
            Select::Dynamic(ref select_with_transform) => {
                scanner.project_with_transform(select_with_transform.as_slice())?;
            }
            Select::All => {}
        }

        if query.base.with_row_id {
            scanner.with_row_id();
        }

        scanner.batch_size(options.max_batch_length as usize);

        if query.base.fast_search {
            scanner.fast_search();
        }

        match &query.base.select {
            Select::Columns(select) => {
                scanner.project(select.as_slice())?;
            }
            Select::Dynamic(select_with_transform) => {
                scanner.project_with_transform(select_with_transform.as_slice())?;
            }
            Select::All => { /* Do nothing */ }
        }

        if let Some(filter) = &query.base.filter {
            match filter {
                QueryFilter::Sql(sql) => {
                    scanner.filter(sql)?;
                }
                QueryFilter::Substrait(substrait) => {
                    scanner.filter_substrait(substrait)?;
                }
                QueryFilter::Datafusion(expr) => {
                    scanner.filter_expr(expr.clone());
                }
            }
        }

        if let Some(fts) = &query.base.full_text_search {
            scanner.full_text_search(fts.clone())?;
        }

        if let Some(refine_factor) = query.refine_factor {
            scanner.refine(refine_factor);
        }

        if let Some(distance_type) = query.distance_type {
            scanner.distance_metric(distance_type.into());
        }

        Ok(scanner.create_plan().await?)
    }

    async fn query(
        &self,
        query: &AnyQuery,
        options: QueryExecutionOptions,
    ) -> Result<DatasetRecordBatchStream> {
        self.generic_query(query, options).await
    }

    async fn analyze_plan(
        &self,
        query: &AnyQuery,
        options: QueryExecutionOptions,
    ) -> Result<String> {
        let plan = self.create_plan(query, options).await?;
        Ok(lance_analyze_plan(plan, Default::default()).await?)
    }

    async fn merge_insert(
        &self,
        params: MergeInsertBuilder,
        new_data: Box<dyn RecordBatchReader + Send>,
    ) -> Result<MergeResult> {
        let dataset = Arc::new(self.dataset.get().await?.clone());
        let mut builder = LanceMergeInsertBuilder::try_new(dataset.clone(), params.on)?;
        match (
            params.when_matched_update_all,
            params.when_matched_update_all_filt,
        ) {
            (false, _) => builder.when_matched(WhenMatched::DoNothing),
            (true, None) => builder.when_matched(WhenMatched::UpdateAll),
            (true, Some(filt)) => builder.when_matched(WhenMatched::update_if(&dataset, &filt)?),
        };
        if params.when_not_matched_insert_all {
            builder.when_not_matched(lance::dataset::WhenNotMatched::InsertAll);
        } else {
            builder.when_not_matched(lance::dataset::WhenNotMatched::DoNothing);
        }
        if params.when_not_matched_by_source_delete {
            let behavior = if let Some(filter) = params.when_not_matched_by_source_delete_filt {
                WhenNotMatchedBySource::delete_if(dataset.as_ref(), &filter)?
            } else {
                WhenNotMatchedBySource::Delete
            };
            builder.when_not_matched_by_source(behavior);
        } else {
            builder.when_not_matched_by_source(WhenNotMatchedBySource::Keep);
        }

        let future = if let Some(timeout) = params.timeout {
            // The default retry timeout is 30s, so we pass the full timeout down
            // as well in case it is longer than that.
            let future = builder
                .retry_timeout(timeout)
                .try_build()?
                .execute_reader(new_data);
            Either::Left(tokio::time::timeout(timeout, future).map(|res| match res {
                Ok(Ok((new_dataset, stats))) => Ok((new_dataset, stats)),
                Ok(Err(e)) => Err(e.into()),
                Err(_) => Err(Error::Runtime {
                    message: "merge insert timed out".to_string(),
                }),
            }))
        } else {
            let job = builder.try_build()?;
            Either::Right(job.execute_reader(new_data).map_err(|e| e.into()))
        };
        let (new_dataset, stats) = future.await?;
        let version = new_dataset.manifest().version;
        self.dataset.set_latest(new_dataset.as_ref().clone()).await;
        Ok(MergeResult {
            version,
            num_updated_rows: stats.num_updated_rows,
            num_inserted_rows: stats.num_inserted_rows,
            num_deleted_rows: stats.num_deleted_rows,
        })
    }

    /// Delete rows from the table
    async fn delete(&self, predicate: &str) -> Result<DeleteResult> {
        let mut dataset = self.dataset.get_mut().await?;
        dataset.delete(predicate).await?;
        Ok(DeleteResult {
            version: dataset.version().version,
        })
    }

    async fn tags(&self) -> Result<Box<dyn Tags + '_>> {
        let dataset = self.dataset.get().await?;

        Ok(Box::new(NativeTags {
            inner: dataset.tags.clone(),
        }))
    }

    async fn optimize(&self, action: OptimizeAction) -> Result<OptimizeStats> {
        let mut stats = OptimizeStats {
            compaction: None,
            prune: None,
        };
        match action {
            OptimizeAction::All => {
                stats.compaction = self
                    .optimize(OptimizeAction::Compact {
                        options: CompactionOptions::default(),
                        remap_options: None,
                    })
                    .await?
                    .compaction;
                stats.prune = self
                    .optimize(OptimizeAction::Prune {
                        older_than: None,
                        delete_unverified: None,
                        error_if_tagged_old_versions: None,
                    })
                    .await?
                    .prune;
                self.optimize(OptimizeAction::Index(OptimizeOptions::default()))
                    .await?;
            }
            OptimizeAction::Compact {
                options,
                remap_options,
            } => {
                stats.compaction = Some(self.compact_files(options, remap_options).await?);
            }
            OptimizeAction::Prune {
                older_than,
                delete_unverified,
                error_if_tagged_old_versions,
            } => {
                stats.prune = Some(
                    self.cleanup_old_versions(
                        older_than.unwrap_or(Duration::try_days(7).expect("valid delta")),
                        delete_unverified,
                        error_if_tagged_old_versions,
                    )
                    .await?,
                );
            }
            OptimizeAction::Index(options) => {
                self.optimize_indices(&options).await?;
            }
        }
        Ok(stats)
    }

    async fn add_columns(
        &self,
        transforms: NewColumnTransform,
        read_columns: Option<Vec<String>>,
    ) -> Result<AddColumnsResult> {
        let mut dataset = self.dataset.get_mut().await?;
        dataset.add_columns(transforms, read_columns, None).await?;
        Ok(AddColumnsResult {
            version: dataset.version().version,
        })
    }

    async fn alter_columns(&self, alterations: &[ColumnAlteration]) -> Result<AlterColumnsResult> {
        let mut dataset = self.dataset.get_mut().await?;
        dataset.alter_columns(alterations).await?;
        Ok(AlterColumnsResult {
            version: dataset.version().version,
        })
    }

    async fn drop_columns(&self, columns: &[&str]) -> Result<DropColumnsResult> {
        let mut dataset = self.dataset.get_mut().await?;
        dataset.drop_columns(columns).await?;
        Ok(DropColumnsResult {
            version: dataset.version().version,
        })
    }

    async fn list_indices(&self) -> Result<Vec<IndexConfig>> {
        let dataset = self.dataset.get().await?;
        let indices = dataset.load_indices().await?;
        let results = futures::stream::iter(indices.as_slice()).then(|idx| async {

            // skip Lance internal indexes
            if idx.name == FRAG_REUSE_INDEX_NAME {
                return None;
            }

            let stats = match dataset.index_statistics(idx.name.as_str()).await {
                Ok(stats) => stats,
                Err(e) => {
                    log::warn!("Failed to get statistics for index {} ({}): {}", idx.name, idx.uuid, e);
                    return None;
                }
            };

            let stats: serde_json::Value = match serde_json::from_str(&stats) {
                Ok(stats) => stats,
                Err(e) => {
                    log::warn!("Failed to deserialize index statistics for index {} ({}): {}", idx.name, idx.uuid, e);
                    return None;
                }
            };

            let Some(index_type) = stats.get("index_type").and_then(|v| v.as_str()) else {
                log::warn!("Index statistics was missing 'index_type' field for index {} ({})", idx.name, idx.uuid);
                return None;
            };

            let index_type: crate::index::IndexType = match index_type.parse() {
                Ok(index_type) => index_type,
                Err(e) => {
                    log::warn!("Failed to parse index type for index {} ({}): {}", idx.name, idx.uuid, e);
                    return None;
                }
            };

            let mut columns = Vec::with_capacity(idx.fields.len());
            for field_id in &idx.fields {
                let Some(field) = dataset.schema().field_by_id(*field_id) else {
                    log::warn!("The index {} ({}) referenced a field with id {} which does not exist in the schema", idx.name, idx.uuid, field_id);
                    return None;
                };
                columns.push(field.name.clone());
            }

            let name = idx.name.clone();
            Some(IndexConfig { index_type, columns, name })
        }).collect::<Vec<_>>().await;

        Ok(results.into_iter().flatten().collect())
    }

    fn dataset_uri(&self) -> &str {
        self.uri.as_str()
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
            Err(lance::error::Error::IndexNotFound { .. }) => return Ok(None),
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
        let loss = stats
            .indices
            .iter()
            .map(|index| index.loss.unwrap_or_default())
            .sum::<f64>();

        let loss = first_index.loss.map(|first_loss| first_loss + loss);
        Ok(Some(IndexStatistics {
            num_indexed_rows: stats.num_indexed_rows,
            num_unindexed_rows: stats.num_unindexed_rows,
            index_type,
            distance_type: first_index.metric_type,
            num_indices: stats.num_indices,
            loss,
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
        let mean = if num_fragments == 0 {
            0
        } else {
            sorted_sizes.iter().copied().sum::<usize>() / num_fragments
        };

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
    use std::iter;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    use arrow_array::{
        builder::{ListBuilder, StringBuilder},
        Array, BooleanArray, Date32Array, FixedSizeListArray, Float32Array, Float64Array,
        Int32Array, Int64Array, LargeStringArray, RecordBatch, RecordBatchIterator,
        RecordBatchReader, StringArray, TimestampMillisecondArray, TimestampNanosecondArray,
        UInt32Array,
    };
    use arrow_data::ArrayDataBuilder;
    use arrow_schema::{DataType, Field, Schema, TimeUnit};
    use futures::TryStreamExt;
    use lance::dataset::WriteMode;
    use lance::io::{ObjectStoreParams, WrappingObjectStore};
    use lance::Dataset;
    use rand::Rng;
    use tempfile::tempdir;

    use super::*;
    use crate::connect;
    use crate::connection::ConnectBuilder;
    use crate::index::scalar::{BTreeIndexBuilder, BitmapIndexBuilder};
    use crate::index::vector::{IvfHnswPqIndexBuilder, IvfHnswSqIndexBuilder};
    use crate::query::{ExecutableQuery, QueryBase};

    #[tokio::test]
    async fn test_open() {
        let tmp_dir = tempdir().unwrap();
        let dataset_path = tmp_dir.path().join("test.lance");

        let batches = make_test_batches();
        Dataset::write(batches, dataset_path.to_str().unwrap(), None)
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

        let batches = make_test_batches();
        let batches = Box::new(batches) as Box<dyn RecordBatchReader + Send>;
        let table = NativeTable::create(uri, "test", batches, None, None, None)
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

    #[tokio::test]
    async fn test_add() {
        let tmp_dir = tempdir().unwrap();
        let uri = tmp_dir.path().to_str().unwrap();
        let conn = connect(uri).execute().await.unwrap();

        let batches = make_test_batches();
        let schema = batches.schema().clone();
        let table = conn.create_table("test", batches).execute().await.unwrap();
        assert_eq!(table.count_rows(None).await.unwrap(), 10);

        let new_batches = RecordBatchIterator::new(
            vec![RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int32Array::from_iter_values(100..110))],
            )
            .unwrap()]
            .into_iter()
            .map(Ok),
            schema.clone(),
        );

        table.add(new_batches).execute().await.unwrap();
        assert_eq!(table.count_rows(None).await.unwrap(), 20);
        assert_eq!(table.name(), "test");
    }

    #[tokio::test]
    async fn test_merge_insert() {
        let tmp_dir = tempdir().unwrap();
        let uri = tmp_dir.path().to_str().unwrap();
        let conn = connect(uri).execute().await.unwrap();

        // Create a dataset with i=0..10
        let batches = merge_insert_test_batches(0, 0);
        let table = conn
            .create_table("my_table", batches)
            .execute()
            .await
            .unwrap();
        assert_eq!(table.count_rows(None).await.unwrap(), 10);

        // Create new data with i=5..15
        let new_batches = Box::new(merge_insert_test_batches(5, 1));

        // Perform a "insert if not exists"
        let mut merge_insert_builder = table.merge_insert(&["i"]);
        merge_insert_builder.when_not_matched_insert_all();
        merge_insert_builder.execute(new_batches).await.unwrap();
        // Only 5 rows should actually be inserted
        assert_eq!(table.count_rows(None).await.unwrap(), 15);

        // Create new data with i=15..25 (no id matches)
        let new_batches = Box::new(merge_insert_test_batches(15, 2));
        // Perform a "bulk update" (should not affect anything)
        let mut merge_insert_builder = table.merge_insert(&["i"]);
        merge_insert_builder.when_matched_update_all(None);
        merge_insert_builder.execute(new_batches).await.unwrap();
        // No new rows should have been inserted
        assert_eq!(table.count_rows(None).await.unwrap(), 15);
        assert_eq!(
            table.count_rows(Some("age = 2".to_string())).await.unwrap(),
            0
        );

        // Conditional update that only replaces the age=0 data
        let new_batches = Box::new(merge_insert_test_batches(5, 3));
        let mut merge_insert_builder = table.merge_insert(&["i"]);
        merge_insert_builder.when_matched_update_all(Some("target.age = 0".to_string()));
        merge_insert_builder.execute(new_batches).await.unwrap();
        assert_eq!(
            table.count_rows(Some("age = 3".to_string())).await.unwrap(),
            5
        );
    }

    #[tokio::test]
    async fn test_add_overwrite() {
        let tmp_dir = tempdir().unwrap();
        let uri = tmp_dir.path().to_str().unwrap();
        let conn = connect(uri).execute().await.unwrap();

        let batches = make_test_batches();
        let schema = batches.schema().clone();
        let table = conn.create_table("test", batches).execute().await.unwrap();
        assert_eq!(table.count_rows(None).await.unwrap(), 10);

        let batches = vec![RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from_iter_values(100..110))],
        )
        .unwrap()]
        .into_iter()
        .map(Ok);

        let new_batches = RecordBatchIterator::new(batches.clone(), schema.clone());

        // Can overwrite using AddDataOptions::mode
        table
            .add(new_batches)
            .mode(AddDataMode::Overwrite)
            .execute()
            .await
            .unwrap();
        assert_eq!(table.count_rows(None).await.unwrap(), 10);
        assert_eq!(table.name(), "test");

        // Can overwrite using underlying WriteParams (which
        // take precedence over AddDataOptions::mode)

        let param: WriteParams = WriteParams {
            mode: WriteMode::Overwrite,
            ..Default::default()
        };

        let new_batches = RecordBatchIterator::new(batches.clone(), schema.clone());
        table
            .add(new_batches)
            .write_options(WriteOptions {
                lance_write_params: Some(param),
            })
            .mode(AddDataMode::Append)
            .execute()
            .await
            .unwrap();
        assert_eq!(table.count_rows(None).await.unwrap(), 10);
        assert_eq!(table.name(), "test");
    }

    #[tokio::test]
    async fn test_update_with_predicate() {
        let tmp_dir = tempdir().unwrap();
        let dataset_path = tmp_dir.path().join("test.lance");
        let uri = dataset_path.to_str().unwrap();
        let conn = connect(uri)
            .read_consistency_interval(Duration::from_secs(0))
            .execute()
            .await
            .unwrap();

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let record_batch_iter = RecordBatchIterator::new(
            vec![RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from_iter_values(0..10)),
                    Arc::new(StringArray::from_iter_values(vec![
                        "a", "b", "c", "d", "e", "f", "g", "h", "i", "j",
                    ])),
                ],
            )
            .unwrap()]
            .into_iter()
            .map(Ok),
            schema.clone(),
        );

        let table = conn
            .create_table("my_table", record_batch_iter)
            .execute()
            .await
            .unwrap();

        table
            .update()
            .only_if("id > 5")
            .column("name", "'foo'")
            .execute()
            .await
            .unwrap();

        let mut batches = table
            .query()
            .select(Select::columns(&["id", "name"]))
            .execute()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        while let Some(batch) = batches.pop() {
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>();
            let names = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>();
            for (i, name) in names.iter().enumerate() {
                let id = ids[i].unwrap();
                let name = name.unwrap();
                if id > 5 {
                    assert_eq!(name, "foo");
                } else {
                    assert_eq!(name, &format!("{}", (b'a' + id as u8) as char));
                }
            }
        }
    }

    #[tokio::test]
    async fn test_update_all_types() {
        let tmp_dir = tempdir().unwrap();
        let dataset_path = tmp_dir.path().join("test.lance");
        let uri = dataset_path.to_str().unwrap();
        let conn = connect(uri)
            .read_consistency_interval(Duration::from_secs(0))
            .execute()
            .await
            .unwrap();

        let schema = Arc::new(Schema::new(vec![
            Field::new("int32", DataType::Int32, false),
            Field::new("int64", DataType::Int64, false),
            Field::new("uint32", DataType::UInt32, false),
            Field::new("string", DataType::Utf8, false),
            Field::new("large_string", DataType::LargeUtf8, false),
            Field::new("float32", DataType::Float32, false),
            Field::new("float64", DataType::Float64, false),
            Field::new("bool", DataType::Boolean, false),
            Field::new("date32", DataType::Date32, false),
            Field::new(
                "timestamp_ns",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new(
                "timestamp_ms",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new(
                "vec_f32",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 2),
                false,
            ),
            Field::new(
                "vec_f64",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float64, true)), 2),
                false,
            ),
        ]));

        let record_batch_iter = RecordBatchIterator::new(
            vec![RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from_iter_values(0..10)),
                    Arc::new(Int64Array::from_iter_values(0..10)),
                    Arc::new(UInt32Array::from_iter_values(0..10)),
                    Arc::new(StringArray::from_iter_values(vec![
                        "a", "b", "c", "d", "e", "f", "g", "h", "i", "j",
                    ])),
                    Arc::new(LargeStringArray::from_iter_values(vec![
                        "a", "b", "c", "d", "e", "f", "g", "h", "i", "j",
                    ])),
                    Arc::new(Float32Array::from_iter_values((0..10).map(|i| i as f32))),
                    Arc::new(Float64Array::from_iter_values((0..10).map(|i| i as f64))),
                    Arc::new(Into::<BooleanArray>::into(vec![
                        true, false, true, false, true, false, true, false, true, false,
                    ])),
                    Arc::new(Date32Array::from_iter_values(0..10)),
                    Arc::new(TimestampNanosecondArray::from_iter_values(0..10)),
                    Arc::new(TimestampMillisecondArray::from_iter_values(0..10)),
                    Arc::new(
                        create_fixed_size_list(
                            Float32Array::from_iter_values((0..20).map(|i| i as f32)),
                            2,
                        )
                        .unwrap(),
                    ),
                    Arc::new(
                        create_fixed_size_list(
                            Float64Array::from_iter_values((0..20).map(|i| i as f64)),
                            2,
                        )
                        .unwrap(),
                    ),
                ],
            )
            .unwrap()]
            .into_iter()
            .map(Ok),
            schema.clone(),
        );

        let table = conn
            .create_table("my_table", record_batch_iter)
            .execute()
            .await
            .unwrap();

        // check it can do update for each type
        let updates: Vec<(&str, &str)> = vec![
            ("string", "'foo'"),
            ("large_string", "'large_foo'"),
            ("int32", "1"),
            ("int64", "1"),
            ("uint32", "1"),
            ("float32", "1.0"),
            ("float64", "1.0"),
            ("bool", "true"),
            ("date32", "1"),
            ("timestamp_ns", "1"),
            ("timestamp_ms", "1"),
            ("vec_f32", "[1.0, 1.0]"),
            ("vec_f64", "[1.0, 1.0]"),
        ];

        let mut update_op = table.update();
        for (column, value) in updates {
            update_op = update_op.column(column, value);
        }
        update_op.execute().await.unwrap();

        let mut batches = table
            .query()
            .select(Select::columns(&[
                "string",
                "large_string",
                "int32",
                "int64",
                "uint32",
                "float32",
                "float64",
                "bool",
                "date32",
                "timestamp_ns",
                "timestamp_ms",
                "vec_f32",
                "vec_f64",
            ]))
            .execute()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let batch = batches.pop().unwrap();

        macro_rules! assert_column {
            ($column:expr, $array_type:ty, $expected:expr) => {
                let array = $column
                    .as_any()
                    .downcast_ref::<$array_type>()
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>();
                for v in array {
                    assert_eq!(v, Some($expected));
                }
            };
        }

        assert_column!(batch.column(0), StringArray, "foo");
        assert_column!(batch.column(1), LargeStringArray, "large_foo");
        assert_column!(batch.column(2), Int32Array, 1);
        assert_column!(batch.column(3), Int64Array, 1);
        assert_column!(batch.column(4), UInt32Array, 1);
        assert_column!(batch.column(5), Float32Array, 1.0);
        assert_column!(batch.column(6), Float64Array, 1.0);
        assert_column!(batch.column(7), BooleanArray, true);
        assert_column!(batch.column(8), Date32Array, 1);
        assert_column!(batch.column(9), TimestampNanosecondArray, 1);
        assert_column!(batch.column(10), TimestampMillisecondArray, 1);

        let array = batch
            .column(11)
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>();
        for v in array {
            let v = v.unwrap();
            let f32array = v.as_any().downcast_ref::<Float32Array>().unwrap();
            for v in f32array {
                assert_eq!(v, Some(1.0));
            }
        }

        let array = batch
            .column(12)
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>();
        for v in array {
            let v = v.unwrap();
            let f64array = v.as_any().downcast_ref::<Float64Array>().unwrap();
            for v in f64array {
                assert_eq!(v, Some(1.0));
            }
        }
    }

    #[tokio::test]
    async fn test_update_via_expr() {
        let tmp_dir = tempdir().unwrap();
        let dataset_path = tmp_dir.path().join("test.lance");
        let uri = dataset_path.to_str().unwrap();
        let conn = connect(uri)
            .read_consistency_interval(Duration::from_secs(0))
            .execute()
            .await
            .unwrap();
        let tbl = conn
            .create_table("my_table", make_test_batches())
            .execute()
            .await
            .unwrap();
        assert_eq!(1, tbl.count_rows(Some("i == 0".to_string())).await.unwrap());
        tbl.update().column("i", "i+1").execute().await.unwrap();
        assert_eq!(0, tbl.count_rows(Some("i == 0".to_string())).await.unwrap());
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
            original: Arc<dyn object_store::ObjectStore>,
            _storage_options: Option<&std::collections::HashMap<String, String>>,
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

    fn merge_insert_test_batches(
        offset: i32,
        age: i32,
    ) -> impl RecordBatchReader + Send + Sync + 'static {
        let schema = Arc::new(Schema::new(vec![
            Field::new("i", DataType::Int32, false),
            Field::new("age", DataType::Int32, false),
        ]));
        RecordBatchIterator::new(
            vec![RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from_iter_values(offset..(offset + 10))),
                    Arc::new(Int32Array::from_iter_values(iter::repeat(age).take(10))),
                ],
            )],
            schema,
        )
    }

    fn make_test_batches() -> impl RecordBatchReader + Send + Sync + 'static {
        let schema = Arc::new(Schema::new(vec![Field::new("i", DataType::Int32, false)]));
        RecordBatchIterator::new(
            vec![RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int32Array::from_iter_values(0..10))],
            )],
            schema,
        )
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

        let mut rng = rand::thread_rng();
        let float_arr = Float32Array::from(
            repeat_with(|| rng.gen::<f32>())
                .take(512 * dimension as usize)
                .collect::<Vec<f32>>(),
        );

        let vectors = Arc::new(create_fixed_size_list(float_arr, dimension).unwrap());
        let batches = RecordBatchIterator::new(
            vec![RecordBatch::try_new(schema.clone(), vec![vectors.clone()]).unwrap()]
                .into_iter()
                .map(Ok),
            schema,
        );

        let table = conn.create_table("test", batches).execute().await.unwrap();

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
        assert!(stats.loss.is_some());

        table.drop_index(index_name).await.unwrap();
        assert_eq!(table.list_indices().await.unwrap().len(), 0);
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

        let mut rng = rand::thread_rng();
        let float_arr = Float32Array::from(
            repeat_with(|| rng.gen::<f32>())
                .take(512 * dimension as usize)
                .collect::<Vec<f32>>(),
        );

        let vectors = Arc::new(create_fixed_size_list(float_arr, dimension).unwrap());
        let batches = RecordBatchIterator::new(
            vec![RecordBatch::try_new(schema.clone(), vec![vectors.clone()]).unwrap()]
                .into_iter()
                .map(Ok),
            schema,
        );

        let table = conn.create_table("test", batches).execute().await.unwrap();

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

        let mut rng = rand::thread_rng();
        let float_arr = Float32Array::from(
            repeat_with(|| rng.gen::<f32>())
                .take(512 * dimension as usize)
                .collect::<Vec<f32>>(),
        );

        let vectors = Arc::new(create_fixed_size_list(float_arr, dimension).unwrap());
        let batches = RecordBatchIterator::new(
            vec![RecordBatch::try_new(schema.clone(), vec![vectors.clone()]).unwrap()]
                .into_iter()
                .map(Ok),
            schema,
        );

        let table = conn.create_table("test", batches).execute().await.unwrap();
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

    fn some_sample_data() -> Box<dyn RecordBatchReader + Send> {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("i", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![1]))],
        )
        .unwrap();
        let schema = batch.schema().clone();
        let batch = Ok(batch);

        Box::new(RecordBatchIterator::new(vec![batch], schema))
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
            .create_table(
                "my_table",
                RecordBatchIterator::new(vec![Ok(batch.clone())], batch.schema()),
            )
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
    async fn test_create_bitmap_index() {
        let tmp_dir = tempdir().unwrap();
        let uri = tmp_dir.path().to_str().unwrap();

        let conn = ConnectBuilder::new(uri).execute().await.unwrap();

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("category", DataType::Utf8, true),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from_iter_values(0..100)),
                Arc::new(StringArray::from_iter_values(
                    (0..100).map(|i| format!("category_{}", i % 5)),
                )),
            ],
        )
        .unwrap();

        let table = conn
            .create_table(
                "test_bitmap",
                RecordBatchIterator::new(vec![Ok(batch.clone())], batch.schema()),
            )
            .execute()
            .await
            .unwrap();

        // Create bitmap index on the "category" column
        table
            .create_index(&["category"], Index::Bitmap(Default::default()))
            .execute()
            .await
            .unwrap();

        // Verify the index was created
        let index_configs = table.list_indices().await.unwrap();
        assert_eq!(index_configs.len(), 1);
        let index = index_configs.into_iter().next().unwrap();
        assert_eq!(index.index_type, crate::index::IndexType::Bitmap);
        assert_eq!(index.columns, vec!["category".to_string()]);
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
            .create_table(
                "test_bitmap",
                RecordBatchIterator::new(vec![Ok(batch.clone())], batch.schema()),
            )
            .execute()
            .await
            .unwrap();

        // Can not create btree or bitmap index on list column
        assert!(table
            .create_index(&["tags"], Index::BTree(Default::default()))
            .execute()
            .await
            .is_err());
        assert!(table
            .create_index(&["tags"], Index::Bitmap(Default::default()))
            .execute()
            .await
            .is_err());

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
            .create_table(
                "test_bitmap",
                RecordBatchIterator::new(vec![Ok(batch.clone())], batch.schema()),
            )
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
                .create_empty_table("my_table", data.schema())
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

        let mut new_field_metadata = HashMap::<String, String>::new();
        new_field_metadata.insert("test_field_key1".into(), "test_field_val1".into());
        native_tbl
            .replace_field_metadata(vec![(field.id as u32, new_field_metadata)])
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
            .create_table(
                "test_stats",
                RecordBatchIterator::new(vec![Ok(batch.clone())], batch.schema()),
            )
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
            table
                .add(RecordBatchIterator::new(
                    vec![Ok(batch.clone())],
                    batch.schema(),
                ))
                .execute()
                .await
                .unwrap();
        }

        let empty_table = conn
            .create_table(
                "test_stats_empty",
                RecordBatchIterator::new(vec![], batch.schema()),
            )
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
                total_bytes: 2000,
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
            .create_table(
                "test_list_indices_skip_frag_reuse",
                RecordBatchIterator::new(vec![Ok(batch.clone())], batch.schema()),
            )
            .execute()
            .await
            .unwrap();

        table
            .add(RecordBatchIterator::new(
                vec![Ok(batch.clone())],
                batch.schema(),
            ))
            .execute()
            .await
            .unwrap();

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
