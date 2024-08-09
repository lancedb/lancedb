// Copyright 2024 LanceDB Developers.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! LanceDB Table APIs

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use arrow::array::AsArray;
use arrow::datatypes::Float32Type;
use arrow_array::{RecordBatchIterator, RecordBatchReader};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion_physical_plan::ExecutionPlan;
use lance::dataset::builder::DatasetBuilder;
use lance::dataset::cleanup::RemovalStats;
use lance::dataset::optimize::{compact_files, CompactionMetrics, IndexRemapperOptions};
use lance::dataset::scanner::{DatasetRecordBatchStream, Scanner};
pub use lance::dataset::ColumnAlteration;
pub use lance::dataset::NewColumnTransform;
pub use lance::dataset::ReadParams;
use lance::dataset::{
    Dataset, UpdateBuilder as LanceUpdateBuilder, WhenMatched, WriteMode, WriteParams,
};
use lance::dataset::{MergeInsertBuilder as LanceMergeInsertBuilder, WhenNotMatchedBySource};
use lance::io::WrappingObjectStore;
use lance_datafusion::exec::execute_plan;
use lance_index::vector::hnsw::builder::HnswBuildParams;
use lance_index::vector::ivf::IvfBuildParams;
use lance_index::vector::pq::PQBuildParams;
use lance_index::vector::sq::builder::SQBuildParams;
use lance_index::DatasetIndexExt;
use lance_index::IndexType;
use log::info;
use serde::{Deserialize, Serialize};
use snafu::whatever;

use crate::arrow::IntoArrow;
use crate::connection::NoData;
use crate::embeddings::{EmbeddingDefinition, EmbeddingRegistry, MaybeEmbedded, MemoryRegistry};
use crate::error::{Error, Result};
use crate::index::vector::{
    IvfHnswPqIndexBuilder, IvfHnswSqIndexBuilder, IvfPqIndexBuilder, VectorIndex,
};
use crate::index::IndexConfig;
use crate::index::IndexStatistics;
use crate::index::{
    vector::{suggested_num_partitions, suggested_num_sub_vectors},
    Index, IndexBuilder,
};
use crate::query::{
    IntoQueryVector, Query, QueryExecutionOptions, Select, VectorQuery, DEFAULT_TOP_K,
};
use crate::utils::{default_vector_column, PatchReadParam, PatchWriteParam};

use self::dataset::{DatasetConsistencyWrapper, DatasetReadGuard};
use self::merge::MergeInsertBuilder;

pub(crate) mod dataset;
pub mod merge;

pub use chrono::Duration;
pub use lance::dataset::optimize::CompactionOptions;
pub use lance_index::optimize::OptimizeOptions;

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
    parent: Arc<dyn TableInternal>,
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

    pub async fn execute(self) -> Result<()> {
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
    parent: Arc<dyn TableInternal>,
    pub(crate) filter: Option<String>,
    pub(crate) columns: Vec<(String, String)>,
}

impl UpdateBuilder {
    fn new(parent: Arc<dyn TableInternal>) -> Self {
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

    /// Executes the update operation
    pub async fn execute(self) -> Result<()> {
        if self.columns.is_empty() {
            Err(Error::InvalidInput {
                message: "at least one column must be specified in an update operation".to_string(),
            })
        } else {
            self.parent.clone().update(self).await
        }
    }
}

#[async_trait]
pub(crate) trait TableInternal: std::fmt::Display + std::fmt::Debug + Send + Sync {
    #[allow(dead_code)]
    fn as_any(&self) -> &dyn std::any::Any;
    /// Cast as [`NativeTable`], or return None it if is not a [`NativeTable`].
    fn as_native(&self) -> Option<&NativeTable>;
    /// Get the name of the table.
    fn name(&self) -> &str;
    /// Get the arrow [Schema] of the table.
    async fn schema(&self) -> Result<SchemaRef>;
    /// Count the number of rows in this table.
    async fn count_rows(&self, filter: Option<String>) -> Result<usize>;
    async fn build_plan(
        &self,
        ds_ref: &DatasetReadGuard,
        query: &VectorQuery,
        options: Option<QueryExecutionOptions>,
    ) -> Result<Scanner>;
    async fn create_plan(
        &self,
        query: &VectorQuery,
        options: QueryExecutionOptions,
    ) -> Result<Arc<dyn ExecutionPlan>>;
    async fn plain_query(
        &self,
        query: &Query,
        options: QueryExecutionOptions,
    ) -> Result<DatasetRecordBatchStream>;
    async fn explain_plan(&self, query: &VectorQuery, verbose: bool) -> Result<String>;
    async fn add(
        &self,
        add: AddDataBuilder<NoData>,
        data: Box<dyn arrow_array::RecordBatchReader + Send>,
    ) -> Result<()>;
    async fn delete(&self, predicate: &str) -> Result<()>;
    async fn update(&self, update: UpdateBuilder) -> Result<()>;
    async fn create_index(&self, index: IndexBuilder) -> Result<()>;
    async fn list_indices(&self) -> Result<Vec<IndexConfig>>;
    async fn merge_insert(
        &self,
        params: MergeInsertBuilder,
        new_data: Box<dyn RecordBatchReader + Send>,
    ) -> Result<()>;
    async fn optimize(&self, action: OptimizeAction) -> Result<OptimizeStats>;
    async fn add_columns(
        &self,
        transforms: NewColumnTransform,
        read_columns: Option<Vec<String>>,
    ) -> Result<()>;
    async fn alter_columns(&self, alterations: &[ColumnAlteration]) -> Result<()>;
    async fn drop_columns(&self, columns: &[&str]) -> Result<()>;
    async fn version(&self) -> Result<u64>;
    async fn checkout(&self, version: u64) -> Result<()>;
    async fn checkout_latest(&self) -> Result<()>;
    async fn restore(&self) -> Result<()>;
    async fn table_definition(&self) -> Result<TableDefinition>;
}

/// A Table is a collection of strong typed Rows.
///
/// The type of the each row is defined in Apache Arrow [Schema].
#[derive(Clone)]
pub struct Table {
    inner: Arc<dyn TableInternal>,
    embedding_registry: Arc<dyn EmbeddingRegistry>,
}

impl std::fmt::Display for Table {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.inner)
    }
}

impl Table {
    pub(crate) fn new(inner: Arc<dyn TableInternal>) -> Self {
        Self {
            inner,
            embedding_registry: Arc::new(MemoryRegistry::new()),
        }
    }

    pub(crate) fn new_with_embedding_registry(
        inner: Arc<dyn TableInternal>,
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
        self.inner.count_rows(filter).await
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
    pub async fn delete(&self, predicate: &str) -> Result<()> {
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
    ///    source table and target table are matched.  Typically this is some
    ///    kind of key or id column.
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
    ) -> Result<()> {
        self.inner.add_columns(transforms, read_columns).await
    }

    /// Change a column's name or nullability.
    pub async fn alter_columns(&self, alterations: &[ColumnAlteration]) -> Result<()> {
        self.inner.alter_columns(alterations).await
    }

    /// Remove columns from the table.
    pub async fn drop_columns(&self, columns: &[&str]) -> Result<()> {
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

    /// List all indices that have been created with [`Self::create_index`]
    pub async fn list_indices(&self) -> Result<Vec<IndexConfig>> {
        self.inner.list_indices().await
    }
}

impl From<NativeTable> for Table {
    fn from(table: NativeTable) -> Self {
        Self::new(Arc::new(table))
    }
}

/// A table in a LanceDB database.
#[derive(Debug, Clone)]
pub struct NativeTable {
    name: String,
    uri: String,
    pub(crate) dataset: dataset::DatasetConsistencyWrapper,

    // the object store wrapper to use on write path
    store_wrapper: Option<Arc<dyn WrappingObjectStore>>,

    storage_options: HashMap<String, String>,

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

        let storage_options = params
            .store_options
            .clone()
            .unwrap_or_default()
            .storage_options
            .unwrap_or_default();

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
            store_wrapper: write_store_wrapper,
            storage_options,
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

    fn supported_btree_data_type(dtype: &DataType) -> bool {
        dtype.is_integer()
            || dtype.is_floating()
            || matches!(
                dtype,
                DataType::Boolean
                    | DataType::Utf8
                    | DataType::Time32(_)
                    | DataType::Time64(_)
                    | DataType::Date32
                    | DataType::Date64
                    | DataType::Timestamp(_, _)
            )
    }

    fn supported_bitmap_data_type(dtype: &DataType) -> bool {
        dtype.is_integer() || matches!(dtype, DataType::Utf8)
    }

    fn supported_label_list_data_type(dtype: &DataType) -> bool {
        match dtype {
            DataType::List(field) => Self::supported_bitmap_data_type(field.data_type()),
            DataType::FixedSizeList(field, _) => {
                Self::supported_bitmap_data_type(field.data_type())
            }
            _ => false,
        }
    }

    fn supported_fts_data_type(dtype: &DataType) -> bool {
        matches!(dtype, DataType::Utf8 | DataType::LargeUtf8)
    }

    fn supported_vector_data_type(dtype: &DataType) -> bool {
        match dtype {
            DataType::FixedSizeList(inner, _) => DataType::is_floating(inner.data_type()),
            _ => false,
        }
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
    pub(crate) async fn create(
        uri: &str,
        name: &str,
        batches: impl RecordBatchReader + Send + 'static,
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
        let storage_options = params
            .store_params
            .clone()
            .unwrap_or_default()
            .storage_options
            .unwrap_or_default();

        let dataset = Dataset::write(batches, uri, Some(params))
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
            store_wrapper: write_store_wrapper,
            storage_options,
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

    #[deprecated(since = "0.5.2", note = "Please use `index_stats` instead")]
    pub async fn count_indexed_rows(&self, index_uuid: &str) -> Result<Option<usize>> {
        #[allow(deprecated)]
        match self.load_index_stats(index_uuid).await? {
            Some(stats) => Ok(Some(stats.num_indexed_rows)),
            None => Ok(None),
        }
    }

    #[deprecated(since = "0.5.2", note = "Please use `index_stats` instead")]
    pub async fn count_unindexed_rows(&self, index_uuid: &str) -> Result<Option<usize>> {
        #[allow(deprecated)]
        match self.load_index_stats(index_uuid).await? {
            Some(stats) => Ok(Some(stats.num_unindexed_rows)),
            None => Ok(None),
        }
    }

    #[deprecated(since = "0.5.2", note = "Please use `index_stats` instead")]
    pub async fn get_index_type(&self, index_uuid: &str) -> Result<Option<String>> {
        #[allow(deprecated)]
        match self.load_index_stats(index_uuid).await? {
            Some(stats) => Ok(Some(stats.index_type.unwrap_or_default())),
            None => Ok(None),
        }
    }

    #[deprecated(since = "0.5.2", note = "Please use `index_stats` instead")]
    pub async fn get_distance_type(&self, index_uuid: &str) -> Result<Option<String>> {
        #[allow(deprecated)]
        match self.load_index_stats(index_uuid).await? {
            Some(stats) => Ok(Some(
                stats
                    .indices
                    .iter()
                    .filter_map(|i| i.metric_type.clone())
                    .collect(),
            )),
            None => Ok(None),
        }
    }

    #[deprecated(since = "0.5.2", note = "Please use `index_stats` instead")]
    pub async fn load_index_stats(&self, index_uuid: &str) -> Result<Option<IndexStatistics>> {
        let index = self
            .load_indices()
            .await?
            .into_iter()
            .find(|i| i.index_uuid == index_uuid);
        if index.is_none() {
            return Ok(None);
        }
        let dataset = self.dataset.get().await?;
        let index_stats = dataset.index_statistics(&index.unwrap().index_name).await?;
        let index_stats: IndexStatistics = whatever!(
            serde_json::from_str(&index_stats),
            "error deserializing index statistics {index_stats}",
        );

        Ok(Some(index_stats))
    }

    /// Get statistics about an index.
    /// Returns an error if the index does not exist.
    pub async fn index_stats(
        &self,
        index_name: impl AsRef<str>,
    ) -> Result<Option<IndexStatistics>> {
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

        serde_json::from_str(&stats).map_err(|e| Error::InvalidInput {
            message: format!("error deserializing index statistics: {}", e),
        })
    }

    pub async fn load_indices(&self) -> Result<Vec<VectorIndex>> {
        let dataset = self.dataset.get().await?;
        let (indices, mf) = futures::try_join!(dataset.load_indices(), dataset.latest_manifest())?;
        Ok(indices
            .iter()
            .map(|i| VectorIndex::new_from_format(&mf, i))
            .collect())
    }

    async fn create_ivf_pq_index(
        &self,
        index: IvfPqIndexBuilder,
        field: &Field,
        replace: bool,
    ) -> Result<()> {
        if !Self::supported_vector_data_type(field.data_type()) {
            return Err(Error::InvalidInput {
                message: format!(
                    "An IVF PQ index cannot be created on the column `{}` which has data type {}",
                    field.name(),
                    field.data_type()
                ),
            });
        }

        let num_partitions = if let Some(n) = index.num_partitions {
            n
        } else {
            suggested_num_partitions(self.count_rows(None).await?)
        };
        let num_sub_vectors: u32 = if let Some(n) = index.num_sub_vectors {
            n
        } else {
            match field.data_type() {
                arrow_schema::DataType::FixedSizeList(_, n) => {
                    Ok::<u32, Error>(suggested_num_sub_vectors(*n as u32))
                }
                _ => Err(Error::Schema {
                    message: format!("Column '{}' is not a FixedSizeList", field.name()),
                }),
            }?
        };
        let mut dataset = self.dataset.get_mut().await?;
        let lance_idx_params = lance::index::vector::VectorIndexParams::ivf_pq(
            num_partitions as usize,
            /*num_bits=*/ 8,
            num_sub_vectors as usize,
            index.distance_type.into(),
            index.max_iterations as usize,
        );
        dataset
            .create_index(
                &[field.name()],
                IndexType::Vector,
                None,
                &lance_idx_params,
                replace,
            )
            .await?;
        Ok(())
    }

    async fn create_ivf_hnsw_pq_index(
        &self,
        index: IvfHnswPqIndexBuilder,
        field: &Field,
        replace: bool,
    ) -> Result<()> {
        if !Self::supported_vector_data_type(field.data_type()) {
            return Err(Error::InvalidInput {
                message: format!(
                    "An IVF HNSW PQ index cannot be created on the column `{}` which has data type {}",
                    field.name(),
                    field.data_type()
                ),
            });
        }

        let num_partitions = if let Some(n) = index.num_partitions {
            n
        } else {
            suggested_num_partitions(self.count_rows(None).await?)
        };
        let num_sub_vectors: u32 = if let Some(n) = index.num_sub_vectors {
            n
        } else {
            match field.data_type() {
                arrow_schema::DataType::FixedSizeList(_, n) => {
                    Ok::<u32, Error>(suggested_num_sub_vectors(*n as u32))
                }
                _ => Err(Error::Schema {
                    message: format!("Column '{}' is not a FixedSizeList", field.name()),
                }),
            }?
        };

        let mut dataset = self.dataset.get_mut().await?;
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
        let lance_idx_params = lance::index::vector::VectorIndexParams::with_ivf_hnsw_pq_params(
            index.distance_type.into(),
            ivf_params,
            hnsw_params,
            pq_params,
        );
        dataset
            .create_index(
                &[field.name()],
                IndexType::Vector,
                None,
                &lance_idx_params,
                replace,
            )
            .await?;
        Ok(())
    }

    async fn create_ivf_hnsw_sq_index(
        &self,
        index: IvfHnswSqIndexBuilder,
        field: &Field,
        replace: bool,
    ) -> Result<()> {
        if !Self::supported_vector_data_type(field.data_type()) {
            return Err(Error::InvalidInput {
                message: format!(
                    "An IVF HNSW SQ index cannot be created on the column `{}` which has data type {}",
                    field.name(),
                    field.data_type()
                ),
            });
        }

        let num_partitions = if let Some(n) = index.num_partitions {
            n
        } else {
            suggested_num_partitions(self.count_rows(None).await?)
        };

        let mut dataset = self.dataset.get_mut().await?;
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
        let lance_idx_params = lance::index::vector::VectorIndexParams::with_ivf_hnsw_sq_params(
            index.distance_type.into(),
            ivf_params,
            hnsw_params,
            sq_params,
        );
        dataset
            .create_index(
                &[field.name()],
                IndexType::Vector,
                None,
                &lance_idx_params,
                replace,
            )
            .await?;
        Ok(())
    }

    async fn create_auto_index(&self, field: &Field, opts: IndexBuilder) -> Result<()> {
        if Self::supported_vector_data_type(field.data_type()) {
            self.create_ivf_pq_index(IvfPqIndexBuilder::default(), field, opts.replace)
                .await
        } else if Self::supported_btree_data_type(field.data_type()) {
            self.create_btree_index(field, opts).await
        } else {
            Err(Error::InvalidInput {
                message: format!(
                    "there are no indices supported for the field `{}` with the data type {}",
                    field.name(),
                    field.data_type()
                ),
            })
        }
    }

    async fn create_btree_index(&self, field: &Field, opts: IndexBuilder) -> Result<()> {
        if !Self::supported_btree_data_type(field.data_type()) {
            return Err(Error::Schema {
                message: format!(
                    "A BTree index cannot be created on the field `{}` which has data type {}",
                    field.name(),
                    field.data_type()
                ),
            });
        }

        let mut dataset = self.dataset.get_mut().await?;
        let lance_idx_params = lance_index::scalar::ScalarIndexParams {
            force_index_type: Some(lance_index::scalar::ScalarIndexType::BTree),
        };
        dataset
            .create_index(
                &[field.name()],
                IndexType::BTree,
                None,
                &lance_idx_params,
                opts.replace,
            )
            .await?;
        Ok(())
    }

    async fn create_bitmap_index(&self, field: &Field, opts: IndexBuilder) -> Result<()> {
        if !Self::supported_bitmap_data_type(field.data_type()) {
            return Err(Error::Schema {
                message: format!(
                    "A Bitmap index cannot be created on the field `{}` which has data type {}",
                    field.name(),
                    field.data_type()
                ),
            });
        }

        let mut dataset = self.dataset.get_mut().await?;
        let lance_idx_params = lance_index::scalar::ScalarIndexParams {
            force_index_type: Some(lance_index::scalar::ScalarIndexType::Bitmap),
        };
        dataset
            .create_index(
                &[field.name()],
                IndexType::Bitmap,
                None,
                &lance_idx_params,
                opts.replace,
            )
            .await?;
        Ok(())
    }

    async fn create_label_list_index(&self, field: &Field, opts: IndexBuilder) -> Result<()> {
        if !Self::supported_label_list_data_type(field.data_type()) {
            return Err(Error::Schema {
                message: format!(
                    "A LabelList index cannot be created on the field `{}` which has data type {}",
                    field.name(),
                    field.data_type()
                ),
            });
        }

        let mut dataset = self.dataset.get_mut().await?;
        let lance_idx_params = lance_index::scalar::ScalarIndexParams {
            force_index_type: Some(lance_index::scalar::ScalarIndexType::LabelList),
        };
        dataset
            .create_index(
                &[field.name()],
                IndexType::LabelList,
                None,
                &lance_idx_params,
                opts.replace,
            )
            .await?;
        Ok(())
    }

    async fn create_fts_index(&self, field: &Field, opts: IndexBuilder) -> Result<()> {
        if !Self::supported_fts_data_type(field.data_type()) {
            return Err(Error::Schema {
                message: format!(
                    "A FTS index cannot be created on the field `{}` which has data type {}",
                    field.name(),
                    field.data_type()
                ),
            });
        }

        let mut dataset = self.dataset.get_mut().await?;
        let lance_idx_params = lance_index::scalar::ScalarIndexParams {
            force_index_type: Some(lance_index::scalar::ScalarIndexType::Inverted),
        };
        dataset
            .create_index(
                &[field.name()],
                IndexType::Scalar,
                None,
                &lance_idx_params,
                opts.replace,
            )
            .await?;
        Ok(())
    }

    async fn generic_query(
        &self,
        query: &VectorQuery,
        options: QueryExecutionOptions,
    ) -> Result<DatasetRecordBatchStream> {
        let plan = self.create_plan(query, options).await?;
        Ok(DatasetRecordBatchStream::new(execute_plan(
            plan,
            Default::default(),
        )?))
    }
}

#[async_trait::async_trait]
impl TableInternal for NativeTable {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_native(&self) -> Option<&NativeTable> {
        Some(self)
    }

    fn name(&self) -> &str {
        self.name.as_str()
    }

    async fn version(&self) -> Result<u64> {
        Ok(self.dataset.get().await?.version().version)
    }

    async fn checkout(&self, version: u64) -> Result<()> {
        self.dataset.as_time_travel(version).await
    }

    async fn checkout_latest(&self) -> Result<()> {
        self.dataset
            .as_latest(self.read_consistency_interval)
            .await?;
        self.dataset.reload().await
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

    async fn count_rows(&self, filter: Option<String>) -> Result<usize> {
        Ok(self.dataset.get().await?.count_rows(filter).await?)
    }

    async fn add(
        &self,
        add: AddDataBuilder<NoData>,
        data: Box<dyn RecordBatchReader + Send>,
    ) -> Result<()> {
        let data =
            MaybeEmbedded::try_new(data, self.table_definition().await?, add.embedding_registry)?;

        // Still use the legacy lance format (v1) by default.
        // We don't want to accidentally switch to v2 format during an add operation.
        // If the table is already v2 this won't have any effect.
        let mut lance_params = add.write_options.lance_write_params.unwrap_or(WriteParams {
            mode: match add.mode {
                AddDataMode::Append => WriteMode::Append,
                AddDataMode::Overwrite => WriteMode::Overwrite,
            },
            ..Default::default()
        });

        // Bring storage options from table
        let storage_options = lance_params
            .store_params
            .get_or_insert(Default::default())
            .storage_options
            .get_or_insert(Default::default());
        for (key, value) in self.storage_options.iter() {
            if !storage_options.contains_key(key) {
                storage_options.insert(key.clone(), value.clone());
            }
        }

        // patch the params if we have a write store wrapper
        let lance_params = match self.store_wrapper.clone() {
            Some(wrapper) => lance_params.patch_with_store_wrapper(wrapper)?,
            None => lance_params,
        };

        self.dataset.ensure_mutable().await?;
        let dataset = Dataset::write(data, &self.uri, Some(lance_params)).await?;

        self.dataset.set_latest(dataset).await;
        Ok(())
    }

    async fn create_index(&self, opts: IndexBuilder) -> Result<()> {
        if opts.columns.len() != 1 {
            return Err(Error::Schema {
                message: "Multi-column (composite) indices are not yet supported".to_string(),
            });
        }
        let schema = self.schema().await?;

        let field = schema.field_with_name(&opts.columns[0])?;

        match opts.index {
            Index::Auto => self.create_auto_index(field, opts).await,
            Index::BTree(_) => self.create_btree_index(field, opts).await,
            Index::Bitmap(_) => self.create_bitmap_index(field, opts).await,
            Index::LabelList(_) => self.create_label_list_index(field, opts).await,
            Index::FTS(_) => self.create_fts_index(field, opts).await,
            Index::IvfPq(ivf_pq) => self.create_ivf_pq_index(ivf_pq, field, opts.replace).await,
            Index::IvfHnswPq(ivf_hnsw_pq) => {
                self.create_ivf_hnsw_pq_index(ivf_hnsw_pq, field, opts.replace)
                    .await
            }
            Index::IvfHnswSq(ivf_hnsw_sq) => {
                self.create_ivf_hnsw_sq_index(ivf_hnsw_sq, field, opts.replace)
                    .await
            }
        }
    }

    async fn update(&self, update: UpdateBuilder) -> Result<()> {
        let dataset = self.dataset.get().await?.clone();
        let mut builder = LanceUpdateBuilder::new(Arc::new(dataset));
        if let Some(predicate) = update.filter {
            builder = builder.update_where(&predicate)?;
        }

        for (column, value) in update.columns {
            builder = builder.set(column, &value)?;
        }

        let operation = builder.build()?;
        let ds = operation.execute().await?;
        self.dataset.set_latest(ds.as_ref().clone()).await;
        Ok(())
    }

    async fn build_plan(
        &self,
        ds_ref: &DatasetReadGuard,
        query: &VectorQuery,
        options: Option<QueryExecutionOptions>,
    ) -> Result<Scanner> {
        let mut scanner: Scanner = ds_ref.scan();

        if let Some(query_vector) = query.query_vector.as_ref() {
            // If there is a vector query, default to limit=10 if unspecified
            let column = if let Some(col) = query.column.as_ref() {
                col.clone()
            } else {
                // Infer a vector column with the same dimension of the query vector.
                let arrow_schema = Schema::from(ds_ref.schema());
                default_vector_column(&arrow_schema, Some(query_vector.len() as i32))?
            };

            let field = ds_ref.schema().field(&column).ok_or(Error::Schema {
                message: format!("Column {} not found in dataset schema", column),
            })?;

            if let arrow_schema::DataType::FixedSizeList(f, dim) = field.data_type() {
                if !f.data_type().is_floating() {
                    return Err(Error::InvalidInput {
                        message: format!(
                            "The data type of the vector column '{}' is not a floating point type",
                            column
                        ),
                    });
                }
                if dim != query_vector.len() as i32 {
                    return Err(Error::InvalidInput {
                    message: format!(
                        "The dimension of the query vector does not match with the dimension of the vector column '{}': \
                            query dim={}, expected vector dim={}",
                        column,
                        query_vector.len(),
                        dim,
                    ),
                });
                }
            }

            let query_vector = query_vector.as_primitive::<Float32Type>();
            scanner.nearest(
                &column,
                query_vector,
                query.base.limit.unwrap_or(DEFAULT_TOP_K),
            )?;
        } else {
            // If there is no vector query, it's ok to not have a limit
            scanner.limit(query.base.limit.map(|limit| limit as i64), None)?;
        }

        scanner.nprobs(query.nprobes);
        scanner.use_index(query.use_index);
        scanner.prefilter(query.prefilter);
        match query.base.select {
            Select::Columns(ref columns) => {
                scanner.project(columns.as_slice())?;
            }
            Select::Dynamic(ref select_with_transform) => {
                scanner.project_with_transform(select_with_transform.as_slice())?;
            }
            Select::All => {}
        }

        if let Some(opts) = options {
            scanner.batch_size(opts.max_batch_length as usize);
        }
        if query.base.fast_search {
            scanner.fast_search();
        }

        Ok(scanner)
    }

    async fn create_plan(
        &self,
        query: &VectorQuery,
        options: QueryExecutionOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let ds_ref = self.dataset.get().await?;

        let mut scanner = self.build_plan(&ds_ref, query, Some(options)).await?;

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
            scanner.filter(filter)?;
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

    async fn plain_query(
        &self,
        query: &Query,
        options: QueryExecutionOptions,
    ) -> Result<DatasetRecordBatchStream> {
        self.generic_query(&query.clone().into_vector(), options)
            .await
    }

    async fn explain_plan(&self, query: &VectorQuery, verbose: bool) -> Result<String> {
        let ds_ref = self.dataset.get().await?;

        let scanner = self.build_plan(&ds_ref, query, None).await?;

        let plan = scanner.explain_plan(verbose).await?;

        Ok(plan)
    }

    async fn merge_insert(
        &self,
        params: MergeInsertBuilder,
        new_data: Box<dyn RecordBatchReader + Send>,
    ) -> Result<()> {
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
        let job = builder.try_build()?;
        let (new_dataset, _stats) = job.execute_reader(new_data).await?;
        self.dataset.set_latest(new_dataset.as_ref().clone()).await;
        Ok(())
    }

    /// Delete rows from the table
    async fn delete(&self, predicate: &str) -> Result<()> {
        self.dataset.get_mut().await?.delete(predicate).await?;
        Ok(())
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
    ) -> Result<()> {
        self.dataset
            .get_mut()
            .await?
            .add_columns(transforms, read_columns)
            .await?;
        Ok(())
    }

    async fn alter_columns(&self, alterations: &[ColumnAlteration]) -> Result<()> {
        self.dataset
            .get_mut()
            .await?
            .alter_columns(alterations)
            .await?;
        Ok(())
    }

    async fn drop_columns(&self, columns: &[&str]) -> Result<()> {
        self.dataset.get_mut().await?.drop_columns(columns).await?;
        Ok(())
    }

    async fn list_indices(&self) -> Result<Vec<IndexConfig>> {
        let dataset = self.dataset.get().await?;
        let indices = dataset.load_indices().await?;
        indices.iter().map(|idx| {
            let mut is_vector = false;
            let mut columns = Vec::with_capacity(idx.fields.len());
            for field_id in &idx.fields {
                let field = dataset.schema().field_by_id(*field_id).ok_or_else(|| Error::Runtime { message: format!("The index with name {} and uuid {} referenced a field with id {} which does not exist in the schema", idx.name, idx.uuid, field_id) })?;
                if field.data_type().is_nested() {
                    // Temporary hack to determine if an index is scalar or vector
                    // Should be removed in https://github.com/lancedb/lance/issues/2039
                    is_vector = true;
                }
                columns.push(field.name.clone());
            }

            let index_type = if is_vector {
                crate::index::IndexType::IvfPq
            } else {
                crate::index::IndexType::BTree
            };

            let name = idx.name.clone();
            Ok(IndexConfig { index_type, columns, name })
        }).collect::<Result<Vec<_>>>()
    }
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
    use lance::dataset::{Dataset, WriteMode};
    use lance::index::DatasetIndexInternalExt;
    use lance::io::{ObjectStoreParams, WrappingObjectStore};
    use rand::Rng;
    use tempfile::tempdir;

    use super::*;
    use crate::connect;
    use crate::connection::ConnectBuilder;
    use crate::index::scalar::BTreeIndexBuilder;
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
        let table = NativeTable::create(uri, "test", batches, None, None, None)
            .await
            .unwrap();

        assert_eq!(table.count_rows(None).await.unwrap(), 10);
        assert_eq!(
            table.count_rows(Some("i >= 5".to_string())).await.unwrap(),
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

        assert_eq!(
            table
                .as_native()
                .unwrap()
                .count_indexed_rows("my_index")
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            table
                .as_native()
                .unwrap()
                .count_unindexed_rows("my_index")
                .await
                .unwrap(),
            None
        );

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
        let index_uuid = &indices[0].index_uuid;
        assert_eq!(
            table
                .as_native()
                .unwrap()
                .count_indexed_rows(index_uuid)
                .await
                .unwrap(),
            Some(512)
        );
        assert_eq!(
            table
                .as_native()
                .unwrap()
                .count_unindexed_rows(index_uuid)
                .await
                .unwrap(),
            Some(0)
        );
        assert_eq!(
            table
                .as_native()
                .unwrap()
                .get_index_type(index_uuid)
                .await
                .unwrap(),
            Some("IVF".to_string())
        );
        assert_eq!(
            table
                .as_native()
                .unwrap()
                .get_distance_type(index_uuid)
                .await
                .unwrap(),
            Some(crate::DistanceType::L2.to_string())
        );
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

        assert_eq!(
            table
                .as_native()
                .unwrap()
                .count_indexed_rows("my_index")
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            table
                .as_native()
                .unwrap()
                .count_unindexed_rows("my_index")
                .await
                .unwrap(),
            None
        );

        let index = IvfHnswSqIndexBuilder::default();
        table
            .create_index(&["embeddings"], Index::IvfHnswSq(index))
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
        let index_uuid = &indices[0].index_uuid;
        assert_eq!(
            table
                .as_native()
                .unwrap()
                .count_indexed_rows(index_uuid)
                .await
                .unwrap(),
            Some(512)
        );
        assert_eq!(
            table
                .as_native()
                .unwrap()
                .count_unindexed_rows(index_uuid)
                .await
                .unwrap(),
            Some(0)
        );
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

        assert_eq!(
            table
                .as_native()
                .unwrap()
                .count_indexed_rows("my_index")
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            table
                .as_native()
                .unwrap()
                .count_unindexed_rows("my_index")
                .await
                .unwrap(),
            None
        );

        let index = IvfHnswPqIndexBuilder::default();
        table
            .create_index(&["embeddings"], Index::IvfHnswPq(index))
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
        let index_uuid = &indices[0].index_uuid;
        assert_eq!(
            table
                .as_native()
                .unwrap()
                .count_indexed_rows(index_uuid)
                .await
                .unwrap(),
            Some(512)
        );
        assert_eq!(
            table
                .as_native()
                .unwrap()
                .count_unindexed_rows(index_uuid)
                .await
                .unwrap(),
            Some(0)
        );
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
        let index_uuid = &indices[0].index_uuid;
        assert_eq!(
            table
                .as_native()
                .unwrap()
                .count_indexed_rows(index_uuid)
                .await
                .unwrap(),
            Some(1)
        );
        assert_eq!(
            table
                .as_native()
                .unwrap()
                .count_unindexed_rows(index_uuid)
                .await
                .unwrap(),
            Some(0)
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
        // TODO: Fix via https://github.com/lancedb/lance/issues/2039
        // assert_eq!(index.index_type, crate::index::IndexType::Bitmap);
        assert_eq!(index.columns, vec!["category".to_string()]);

        // For now, just open the index to verify its type
        let lance_dataset = table.as_native().unwrap().dataset.get().await.unwrap();
        let indices = lance_dataset
            .load_indices_by_name(&index.name)
            .await
            .unwrap();
        let index_meta = &indices[0];
        let idx = lance_dataset
            .open_scalar_index("category", &index_meta.uuid.to_string())
            .await
            .unwrap();
        assert_eq!(idx.index_type(), IndexType::Bitmap);
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
            builder.values().append_value(TAGS[i % 3].to_string());
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
        // TODO: Fix via https://github.com/lancedb/lance/issues/2039
        // assert_eq!(index.index_type, crate::index::IndexType::LabelList);
        assert_eq!(index.columns, vec!["tags".to_string()]);

        // For now, just open the index to verify its type
        let lance_dataset = table.as_native().unwrap().dataset.get().await.unwrap();
        let indices = lance_dataset
            .load_indices_by_name(&index.name)
            .await
            .unwrap();
        let index_meta = &indices[0];
        let idx = lance_dataset
            .open_scalar_index("tags", &index_meta.uuid.to_string())
            .await
            .unwrap();
        assert_eq!(idx.index_type(), IndexType::LabelList);
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
}
