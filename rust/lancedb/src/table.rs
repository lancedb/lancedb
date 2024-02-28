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

use std::path::Path;
use std::sync::Arc;

use arrow_array::{RecordBatchIterator, RecordBatchReader};
use arrow_schema::{Schema, SchemaRef};
use async_trait::async_trait;
use chrono::Duration;
use lance::dataset::builder::DatasetBuilder;
use lance::dataset::cleanup::RemovalStats;
use lance::dataset::optimize::{
    compact_files, CompactionMetrics, CompactionOptions, IndexRemapperOptions,
};
pub use lance::dataset::ReadParams;
use lance::dataset::{
    ColumnAlteration, Dataset, NewColumnTransform, UpdateBuilder, WhenMatched, WriteMode,
    WriteParams,
};
use lance::dataset::{MergeInsertBuilder as LanceMergeInsertBuilder, WhenNotMatchedBySource};
use lance::io::WrappingObjectStore;
use lance_index::{optimize::OptimizeOptions, DatasetIndexExt};
use log::info;

use crate::error::{Error, Result};
use crate::index::vector::{VectorIndex, VectorIndexStatistics};
use crate::index::IndexBuilder;
use crate::query::Query;
use crate::utils::{PatchReadParam, PatchWriteParam};

use self::dataset::DatasetConsistencyWrapper;
use self::merge::{MergeInsert, MergeInsertBuilder};

pub(crate) mod dataset;
pub mod merge;

/// Optimize the dataset.
///
/// Similar to `VACUUM` in PostgreSQL, it offers different options to
/// optimize different parts of the table on disk.
///
/// By default, it optimizes everything, as [`OptimizeAction::All`].
pub enum OptimizeAction {
    /// Run optimization on every, with default options.
    All,
    /// Compact files in the dataset
    Compact {
        options: CompactionOptions,
        remap_options: Option<Arc<dyn IndexRemapperOptions>>,
    },
    /// Prune old version of datasets.
    Prune {
        /// The duration of time to keep versions of the dataset.
        older_than: Duration,
        /// Because they may be part of an in-progress transaction, files newer than 7 days old are not deleted by default.
        /// If you are sure that there are no in-progress transactions, then you can set this to True to delete all files older than `older_than`.
        delete_unverified: Option<bool>,
    },
    /// Optimize index.
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
    /// If set, these will take precedence over any overlapping `OpenTableOptions` options
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

#[derive(Debug, Default, Clone)]
pub struct AddDataOptions {
    /// Whether to add new rows (the default) or replace the existing data
    pub mode: AddDataMode,
    /// Options to use when writing the data
    pub write_options: WriteOptions,
}

/// A Table is a collection of strong typed Rows.
///
/// The type of the each row is defined in Apache Arrow [Schema].
#[async_trait::async_trait]
pub trait Table: std::fmt::Display + Send + Sync {
    fn as_any(&self) -> &dyn std::any::Any;

    /// Cast as [`NativeTable`], or return None it if is not a [`NativeTable`].
    fn as_native(&self) -> Option<&NativeTable>;

    /// Get the name of the table.
    fn name(&self) -> &str;

    /// Get the arrow [Schema] of the table.
    async fn schema(&self) -> Result<SchemaRef>;

    /// Count the number of rows in this dataset.
    ///
    /// # Arguments
    ///
    /// * `filter` if present, only count rows matching the filter
    async fn count_rows(&self, filter: Option<String>) -> Result<usize>;

    /// Insert new records into this Table
    ///
    /// # Arguments
    ///
    /// * `batches` data to be added to the Table
    /// * `options` options to control how data is added
    async fn add(
        &self,
        batches: Box<dyn RecordBatchReader + Send>,
        options: AddDataOptions,
    ) -> Result<()>;

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
    async fn delete(&self, predicate: &str) -> Result<()>;

    /// Create an index on the column name.
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
    /// tbl.create_index(&["vector"])
    ///     .ivf_pq()
    ///     .num_partitions(256)
    ///     .build()
    ///     .await
    ///     .unwrap();
    /// # });
    /// ```
    fn create_index(&self, column: &[&str]) -> IndexBuilder;

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
    fn merge_insert(&self, on: &[&str]) -> MergeInsertBuilder;

    /// Search the table with a given query vector.
    ///
    /// This is a convenience method for preparing an ANN query.
    fn search(&self, query: &[f32]) -> Query {
        self.query().nearest_to(query)
    }

    /// Create a generic [`Query`] Builder.
    ///
    /// When appropriate, various indices and statistics based pruning will be used to
    /// accelerate the query.
    ///
    /// # Examples
    ///
    /// ## Run a vector search (ANN) query.
    ///
    /// ```no_run
    /// # use arrow_array::RecordBatch;
    /// # use futures::TryStreamExt;
    /// # tokio::runtime::Runtime::new().unwrap().block_on(async {
    /// # let tbl = lancedb::table::NativeTable::open("/tmp/tbl").await.unwrap();
    /// use crate::lancedb::Table;
    /// let stream = tbl
    ///     .query()
    ///     .nearest_to(&[1.0, 2.0, 3.0])
    ///     .refine_factor(5)
    ///     .nprobes(10)
    ///     .execute_stream()
    ///     .await
    ///     .unwrap();
    /// let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
    /// # });
    /// ```
    ///
    /// ## Run a SQL-style filter
    /// ```no_run
    /// # use arrow_array::RecordBatch;
    /// # use futures::TryStreamExt;
    /// # tokio::runtime::Runtime::new().unwrap().block_on(async {
    /// # let tbl = lancedb::table::NativeTable::open("/tmp/tbl").await.unwrap();
    /// use crate::lancedb::Table;
    /// let stream = tbl
    ///     .query()
    ///     .filter("id > 5")
    ///     .limit(1000)
    ///     .execute_stream()
    ///     .await
    ///     .unwrap();
    /// let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
    /// # });
    /// ```
    ///
    /// ## Run a full scan query.
    /// ```no_run
    /// # use arrow_array::RecordBatch;
    /// # use futures::TryStreamExt;
    /// # tokio::runtime::Runtime::new().unwrap().block_on(async {
    /// # let tbl = lancedb::table::NativeTable::open("/tmp/tbl").await.unwrap();
    /// use crate::lancedb::Table;
    /// let stream = tbl.query().execute_stream().await.unwrap();
    /// let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
    /// # });
    /// ```
    fn query(&self) -> Query;

    /// Optimize the on-disk data and indices for better performance.
    ///
    /// <section class="warning">Experimental API</section>
    ///
    /// Modeled after ``VACUUM`` in PostgreSQL.
    /// Not all implementations support explicit optimization.
    async fn optimize(&self, action: OptimizeAction) -> Result<OptimizeStats>;

    /// Add new columns to the table, providing values to fill in.
    async fn add_columns(
        &self,
        transforms: NewColumnTransform,
        read_columns: Option<Vec<String>>,
    ) -> Result<()>;

    /// Change a column's name or nullability.
    async fn alter_columns(&self, alterations: &[ColumnAlteration]) -> Result<()>;

    /// Remove columns from the table.
    async fn drop_columns(&self, columns: &[&str]) -> Result<()>;
}

/// Reference to a Table pointer.
pub type TableRef = Arc<dyn Table>;

/// A table in a LanceDB database.
#[derive(Debug, Clone)]
pub struct NativeTable {
    name: String,
    uri: String,
    pub(crate) dataset: dataset::DatasetConsistencyWrapper,

    // the object store wrapper to use on write path
    store_wrapper: Option<Arc<dyn WrappingObjectStore>>,

    // This comes from the connection options. We store here so we can pass down
    // to the dataset when we recreate it (for example, in checkout_latest).
    read_consistency_interval: Option<std::time::Duration>,
}

impl std::fmt::Display for NativeTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Table({})", self.name)
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
                e => Error::Lance {
                    message: e.to_string(),
                },
            })?;

        let dataset = DatasetConsistencyWrapper::new_latest(dataset, read_consistency_interval);

        Ok(Self {
            name: name.to_string(),
            uri: uri.to_string(),
            dataset,
            store_wrapper: write_store_wrapper,
            read_consistency_interval,
        })
    }

    /// Checkout a specific version of this [NativeTable]
    pub async fn checkout(uri: &str, version: u64) -> Result<Self> {
        let name = Self::get_table_name(uri)?;
        Self::checkout_with_params(uri, &name, version, None, ReadParams::default(), None).await
    }

    pub async fn checkout_with_params(
        uri: &str,
        name: &str,
        version: u64,
        write_store_wrapper: Option<Arc<dyn WrappingObjectStore>>,
        params: ReadParams,
        read_consistency_interval: Option<std::time::Duration>,
    ) -> Result<Self> {
        // patch the params if we have a write store wrapper
        let params = match write_store_wrapper.clone() {
            Some(wrapper) => params.patch_with_store_wrapper(wrapper)?,
            None => params,
        };
        let dataset = DatasetBuilder::from_uri(uri)
            .with_version(version)
            .with_read_params(params)
            .load()
            .await?;
        let dataset = DatasetConsistencyWrapper::new_time_travel(dataset, version);

        Ok(Self {
            name: name.to_string(),
            uri: uri.to_string(),
            dataset,
            store_wrapper: write_store_wrapper,
            read_consistency_interval,
        })
    }

    pub async fn checkout_latest(&self) -> Result<Self> {
        let mut dataset = self.dataset.duplicate().await;
        dataset.as_latest(self.read_consistency_interval).await?;
        Ok(Self {
            dataset,
            ..self.clone()
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
    pub(crate) async fn create(
        uri: &str,
        name: &str,
        batches: impl RecordBatchReader + Send + 'static,
        write_store_wrapper: Option<Arc<dyn WrappingObjectStore>>,
        params: Option<WriteParams>,
        read_consistency_interval: Option<std::time::Duration>,
    ) -> Result<Self> {
        let params = params.unwrap_or_default();
        // patch the params if we have a write store wrapper
        let params = match write_store_wrapper.clone() {
            Some(wrapper) => params.patch_with_store_wrapper(wrapper)?,
            None => params,
        };

        let dataset = Dataset::write(batches, uri, Some(params))
            .await
            .map_err(|e| match e {
                lance::Error::DatasetAlreadyExists { .. } => Error::TableAlreadyExists {
                    name: name.to_string(),
                },
                e => Error::Lance {
                    message: e.to_string(),
                },
            })?;
        Ok(Self {
            name: name.to_string(),
            uri: uri.to_string(),
            dataset: DatasetConsistencyWrapper::new_latest(dataset, read_consistency_interval),
            store_wrapper: write_store_wrapper,
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

    /// Version of this Table
    pub async fn version(&self) -> Result<u64> {
        Ok(self.dataset.get().await?.version().version)
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

    pub async fn update(&self, predicate: Option<&str>, updates: Vec<(&str, &str)>) -> Result<()> {
        let dataset = self.dataset.get().await?.clone();
        let mut builder = UpdateBuilder::new(Arc::new(dataset));
        if let Some(predicate) = predicate {
            builder = builder.update_where(predicate)?;
        }

        for (column, value) in updates {
            builder = builder.set(column, value)?;
        }

        let operation = builder.build()?;
        let ds = operation.execute().await?;
        self.dataset.set_latest(ds.as_ref().clone()).await;
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
    ) -> Result<RemovalStats> {
        Ok(self
            .dataset
            .get_mut()
            .await?
            .cleanup_old_versions(older_than, delete_unverified)
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

    pub async fn count_indexed_rows(&self, index_uuid: &str) -> Result<Option<usize>> {
        match self.load_index_stats(index_uuid).await? {
            Some(stats) => Ok(Some(stats.num_indexed_rows)),
            None => Ok(None),
        }
    }

    pub async fn count_unindexed_rows(&self, index_uuid: &str) -> Result<Option<usize>> {
        match self.load_index_stats(index_uuid).await? {
            Some(stats) => Ok(Some(stats.num_unindexed_rows)),
            None => Ok(None),
        }
    }

    pub async fn load_indices(&self) -> Result<Vec<VectorIndex>> {
        let dataset = self.dataset.get().await?;
        let (indices, mf) = futures::try_join!(dataset.load_indices(), dataset.latest_manifest())?;
        Ok(indices
            .iter()
            .map(|i| VectorIndex::new_from_format(&mf, i))
            .collect())
    }

    async fn load_index_stats(&self, index_uuid: &str) -> Result<Option<VectorIndexStatistics>> {
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
        let index_stats: VectorIndexStatistics =
            serde_json::from_str(&index_stats).map_err(|e| Error::Lance {
                message: format!(
                    "error deserializing index statistics {}: {}",
                    e, index_stats
                ),
            })?;

        Ok(Some(index_stats))
    }
}

#[async_trait]
impl MergeInsert for NativeTable {
    async fn do_merge_insert(
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
        let new_dataset = job.execute_reader(new_data).await?;
        self.dataset.set_latest(new_dataset.as_ref().clone()).await;
        Ok(())
    }
}

#[async_trait::async_trait]
impl Table for NativeTable {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_native(&self) -> Option<&NativeTable> {
        Some(self)
    }

    fn name(&self) -> &str {
        self.name.as_str()
    }

    async fn schema(&self) -> Result<SchemaRef> {
        let lance_schema = self.dataset.get().await?.schema().clone();
        Ok(Arc::new(Schema::from(&lance_schema)))
    }

    async fn count_rows(&self, filter: Option<String>) -> Result<usize> {
        let dataset = self.dataset.get().await?;
        if let Some(filter) = filter {
            let mut scanner = dataset.scan();
            scanner.filter(&filter)?;
            Ok(scanner.count_rows().await? as usize)
        } else {
            Ok(dataset.count_rows().await?)
        }
    }

    async fn add(
        &self,
        batches: Box<dyn RecordBatchReader + Send>,
        params: AddDataOptions,
    ) -> Result<()> {
        let lance_params = params
            .write_options
            .lance_write_params
            .unwrap_or(WriteParams {
                mode: match params.mode {
                    AddDataMode::Append => WriteMode::Append,
                    AddDataMode::Overwrite => WriteMode::Overwrite,
                },
                ..Default::default()
            });

        // patch the params if we have a write store wrapper
        let lance_params = match self.store_wrapper.clone() {
            Some(wrapper) => lance_params.patch_with_store_wrapper(wrapper)?,
            None => lance_params,
        };

        let dataset = Dataset::write(batches, &self.uri, Some(lance_params)).await?;
        self.dataset.set_latest(dataset).await;
        Ok(())
    }

    fn merge_insert(&self, on: &[&str]) -> MergeInsertBuilder {
        let on = Vec::from_iter(on.iter().map(|key| key.to_string()));
        MergeInsertBuilder::new(Arc::new(self.clone()), on)
    }

    fn create_index(&self, columns: &[&str]) -> IndexBuilder {
        IndexBuilder::new(Arc::new(self.clone()), columns)
    }

    fn query(&self) -> Query {
        Query::new(self.dataset.clone())
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
                        older_than: Duration::days(7),
                        delete_unverified: None,
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
            } => {
                stats.prune = Some(
                    self.cleanup_old_versions(older_than, delete_unverified)
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
}

#[cfg(test)]
mod tests {
    use std::iter;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    use arrow_array::{
        Array, BooleanArray, Date32Array, FixedSizeListArray, Float32Array, Float64Array,
        Int32Array, Int64Array, LargeStringArray, RecordBatch, RecordBatchIterator,
        RecordBatchReader, StringArray, TimestampMillisecondArray, TimestampNanosecondArray,
        UInt32Array,
    };
    use arrow_data::ArrayDataBuilder;
    use arrow_schema::{DataType, Field, Schema, TimeUnit};
    use futures::TryStreamExt;
    use lance::dataset::{Dataset, WriteMode};
    use lance::io::{ObjectStoreParams, WrappingObjectStore};
    use rand::Rng;
    use tempfile::tempdir;

    use crate::connection::ConnectBuilder;

    use super::*;

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

        let batches = make_test_batches();
        let schema = batches.schema().clone();
        let table = NativeTable::create(uri, "test", batches, None, None, None)
            .await
            .unwrap();
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

        table
            .add(Box::new(new_batches), AddDataOptions::default())
            .await
            .unwrap();
        assert_eq!(table.count_rows(None).await.unwrap(), 20);
        assert_eq!(table.name, "test");
    }

    #[tokio::test]
    async fn test_merge_insert() {
        let tmp_dir = tempdir().unwrap();
        let uri = tmp_dir.path().to_str().unwrap();

        // Create a dataset with i=0..10
        let batches = merge_insert_test_batches(0, 0);
        let table = NativeTable::create(uri, "test", batches, None, None, None)
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

        let batches = make_test_batches();
        let schema = batches.schema().clone();
        let table = NativeTable::create(uri, "test", batches, None, None, None)
            .await
            .unwrap();
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
            .add(
                Box::new(new_batches),
                AddDataOptions {
                    mode: AddDataMode::Overwrite,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(table.count_rows(None).await.unwrap(), 10);
        assert_eq!(table.name, "test");

        // Can overwrite using underlying WriteParams (which
        // take precedence over AddDataOptions::mode)

        let param: WriteParams = WriteParams {
            mode: WriteMode::Overwrite,
            ..Default::default()
        };

        let opts = AddDataOptions {
            write_options: WriteOptions {
                lance_write_params: Some(param),
            },
            mode: AddDataMode::Append,
        };

        let new_batches = RecordBatchIterator::new(batches.clone(), schema.clone());
        table.add(Box::new(new_batches), opts).await.unwrap();
        assert_eq!(table.count_rows(None).await.unwrap(), 10);
        assert_eq!(table.name, "test");
    }

    #[tokio::test]
    async fn test_update_with_predicate() {
        let tmp_dir = tempdir().unwrap();
        let dataset_path = tmp_dir.path().join("test.lance");
        let uri = dataset_path.to_str().unwrap();

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

        Dataset::write(record_batch_iter, uri, None).await.unwrap();
        let table = NativeTable::open(uri).await.unwrap();

        table
            .update(Some("id > 5"), vec![("name", "'foo'")])
            .await
            .unwrap();

        let ds_after = Dataset::open(uri).await.unwrap();
        let mut batches = ds_after
            .scan()
            .project(&["id", "name"])
            .unwrap()
            .try_into_stream()
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

        Dataset::write(record_batch_iter, uri, None).await.unwrap();
        let table = NativeTable::open(uri).await.unwrap();

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

        // for (column, value) in test_cases {
        table.update(None, updates).await.unwrap();

        let ds_after = Dataset::open(uri).await.unwrap();
        let mut batches = ds_after
            .scan()
            .project(&[
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
            ])
            .unwrap()
            .try_into_stream()
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

        let batches = make_test_batches();
        Dataset::write(batches, dataset_path.to_str().unwrap(), None)
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
        let _ = NativeTable::open_with_params(uri, "test", None, Some(param), None)
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

        let table = NativeTable::create(uri, "test", batches, None, None, None)
            .await
            .unwrap();

        assert_eq!(table.count_indexed_rows("my_index").await.unwrap(), None);
        assert_eq!(table.count_unindexed_rows("my_index").await.unwrap(), None);

        table
            .create_index(&["embeddings"])
            .ivf_pq()
            .name("my_index")
            .num_partitions(256)
            .build()
            .await
            .unwrap();

        assert_eq!(table.load_indices().await.unwrap().len(), 1);
        assert_eq!(table.count_rows(None).await.unwrap(), 512);
        assert_eq!(table.name, "test");

        let indices = table.load_indices().await.unwrap();
        let index_uuid = &indices[0].index_uuid;
        assert_eq!(
            table.count_indexed_rows(index_uuid).await.unwrap(),
            Some(512)
        );
        assert_eq!(
            table.count_unindexed_rows(index_uuid).await.unwrap(),
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

    #[tokio::test]
    async fn test_read_consistency_interval() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("i", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![1]))],
        )
        .unwrap();

        let intervals = vec![
            None,
            Some(0),
            Some(100), // 100 ms
        ];

        for interval in intervals {
            let tmp_dir = tempdir().unwrap();
            let uri = tmp_dir.path().to_str().unwrap();

            let conn1 = ConnectBuilder::new(uri).execute().await.unwrap();
            let table1 = conn1
                .create_empty_table("my_table", batch.schema())
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

            table1
                .add(
                    Box::new(RecordBatchIterator::new(
                        vec![Ok(batch.clone())],
                        batch.schema(),
                    )),
                    AddDataOptions::default(),
                )
                .await
                .unwrap();
            assert_eq!(table1.count_rows(None).await.unwrap(), 1);

            match interval {
                None => {
                    assert_eq!(table2.count_rows(None).await.unwrap(), 0);
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
}
