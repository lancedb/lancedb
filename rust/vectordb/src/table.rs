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
use std::sync::{Arc, Mutex};

use arrow_array::RecordBatchReader;
use arrow_schema::{Schema, SchemaRef};
use chrono::Duration;
use lance::dataset::builder::DatasetBuilder;
use lance::dataset::cleanup::RemovalStats;
use lance::dataset::optimize::{
    compact_files, CompactionMetrics, CompactionOptions, IndexRemapperOptions,
};
pub use lance::dataset::ReadParams;
use lance::dataset::{Dataset, UpdateBuilder, WriteParams};
use lance::io::WrappingObjectStore;
use lance_index::{optimize::OptimizeOptions, DatasetIndexExt};
use log::info;

use crate::error::{Error, Result};
use crate::index::vector::{VectorIndex, VectorIndexStatistics};
use crate::index::IndexBuilder;
use crate::merge_insert::MergeInsertBuilder;
use crate::query::Query;
use crate::utils::{PatchReadParam, PatchWriteParam};
use crate::WriteMode;

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
    fn schema(&self) -> SchemaRef;

    /// Count the number of rows in this dataset.
    async fn count_rows(&self) -> Result<usize>;

    /// Insert new records into this Table
    ///
    /// # Arguments
    ///
    /// * `batches` RecordBatch to be saved in the Table
    /// * `params` Append / Overwrite existing records. Default: Append
    async fn add(
        &self,
        batches: Box<dyn RecordBatchReader + Send>,
        params: Option<WriteParams>,
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
    /// # use vectordb::connection::{Database, Connection};
    /// # use arrow_array::{FixedSizeListArray, types::Float32Type, RecordBatch,
    /// #   RecordBatchIterator, Int32Array};
    /// # use arrow_schema::{Schema, Field, DataType};
    /// # tokio::runtime::Runtime::new().unwrap().block_on(async {
    /// let tmpdir = tempfile::tempdir().unwrap();
    /// let db = Database::connect(tmpdir.path().to_str().unwrap()).await.unwrap();
    /// # let schema = Arc::new(Schema::new(vec![
    /// #  Field::new("id", DataType::Int32, false),
    /// #  Field::new("vector", DataType::FixedSizeList(
    /// #    Arc::new(Field::new("item", DataType::Float32, true)), 128), true),
    /// # ]));
    /// let batches = RecordBatchIterator::new(vec![
    ///     RecordBatch::try_new(schema.clone(),
    ///        vec![
    ///            Arc::new(Int32Array::from_iter_values(0..10)),
    ///            Arc::new(FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
    ///                (0..10).map(|_| Some(vec![Some(1.0); 128])), 128)),
    ///        ]).unwrap()
    ///    ].into_iter().map(Ok),
    ///   schema.clone());
    /// let tbl = db.create_table("delete_test", Box::new(batches), None).await.unwrap();
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
    /// # use vectordb::connection::{Database, Connection};
    /// # use arrow_array::{FixedSizeListArray, types::Float32Type, RecordBatch,
    /// #   RecordBatchIterator, Int32Array};
    /// # use arrow_schema::{Schema, Field, DataType};
    /// # tokio::runtime::Runtime::new().unwrap().block_on(async {
    /// let tmpdir = tempfile::tempdir().unwrap();
    /// let db = Database::connect(tmpdir.path().to_str().unwrap()).await.unwrap();
    /// # let tbl = db.open_table("idx_test").await.unwrap();
    /// tbl.create_index(&["vector"])
    ///     .ivf_pq()
    ///     .num_partitions(256)
    ///     .build()
    ///     .await
    ///     .unwrap();
    /// # });
    /// ```
    fn create_index(&self, column: &[&str]) -> IndexBuilder;

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
    /// # let tbl = vectordb::table::NativeTable::open("/tmp/tbl").await.unwrap();
    /// let stream = tbl.query().nearest_to(&[1.0, 2.0, 3.0])
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
    /// # let tbl = vectordb::table::NativeTable::open("/tmp/tbl").await.unwrap();
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
    /// # let tbl = vectordb::table::NativeTable::open("/tmp/tbl").await.unwrap();
    /// let stream = tbl
    ///     .query()
    ///     .execute_stream()
    ///     .await
    ///     .unwrap();
    /// let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
    /// # });
    /// ```
    fn query(&self) -> Query;

    /// Optimize the on-disk data and indices for better performance.
    ///
    /// <section class="warning">Experimental API</section>
    ///
    /// Modeled after ``VACCUM`` in PostgreSQL.
    /// Not all implementations support explicit optimization.
    async fn optimize(&self, action: OptimizeAction) -> Result<OptimizeStats>;

    fn merge_insert(&self) -> MergeInsertBuilder;
}

/// Reference to a Table pointer.
pub type TableRef = Arc<dyn Table>;

/// A table in a LanceDB database.
#[derive(Debug, Clone)]
pub struct NativeTable {
    name: String,
    uri: String,
    dataset: Arc<Mutex<Dataset>>,

    // the object store wrapper to use on write path
    store_wrapper: Option<Arc<dyn WrappingObjectStore>>,
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
        Self::open_with_params(uri, &name, None, ReadParams::default()).await
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
        params: ReadParams,
    ) -> Result<Self> {
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
        Ok(NativeTable {
            name: name.to_string(),
            uri: uri.to_string(),
            dataset: Arc::new(Mutex::new(dataset)),
            store_wrapper: write_store_wrapper,
        })
    }

    /// Make a new clone of the internal lance dataset.
    pub(crate) fn clone_inner_dataset(&self) -> Dataset {
        self.dataset.lock().expect("Lock poison").clone()
    }

    /// Checkout a specific version of this [NativeTable]
    ///
    pub async fn checkout(uri: &str, version: u64) -> Result<Self> {
        let name = Self::get_table_name(uri)?;
        Self::checkout_with_params(uri, &name, version, None, ReadParams::default()).await
    }

    pub async fn checkout_with_params(
        uri: &str,
        name: &str,
        version: u64,
        write_store_wrapper: Option<Arc<dyn WrappingObjectStore>>,
        params: ReadParams,
    ) -> Result<Self> {
        // patch the params if we have a write store wrapper
        let params = match write_store_wrapper.clone() {
            Some(wrapper) => params.patch_with_store_wrapper(wrapper)?,
            None => params,
        };
        let dataset = Dataset::checkout_with_params(uri, version, &params)
            .await
            .map_err(|e| match e {
                lance::Error::DatasetNotFound { .. } => Error::TableNotFound {
                    name: name.to_string(),
                },
                e => Error::Lance {
                    message: e.to_string(),
                },
            })?;
        Ok(NativeTable {
            name: name.to_string(),
            uri: uri.to_string(),
            dataset: Arc::new(Mutex::new(dataset)),
            store_wrapper: write_store_wrapper,
        })
    }

    pub async fn checkout_latest(&self) -> Result<Self> {
        let dataset = self.clone_inner_dataset();
        let latest_version_id = dataset.latest_version_id().await?;
        let dataset = if latest_version_id == dataset.version().version {
            dataset
        } else {
            dataset.checkout_version(latest_version_id).await?
        };

        Ok(Self {
            name: self.name.clone(),
            uri: self.uri.clone(),
            dataset: Arc::new(Mutex::new(dataset)),
            store_wrapper: self.store_wrapper.clone(),
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
    ) -> Result<Self> {
        // patch the params if we have a write store wrapper
        let params = match write_store_wrapper.clone() {
            Some(wrapper) => params.patch_with_store_wrapper(wrapper)?,
            None => params,
        };

        let dataset = Dataset::write(batches, uri, params)
            .await
            .map_err(|e| match e {
                lance::Error::DatasetAlreadyExists { .. } => Error::TableAlreadyExists {
                    name: name.to_string(),
                },
                e => Error::Lance {
                    message: e.to_string(),
                },
            })?;
        Ok(NativeTable {
            name: name.to_string(),
            uri: uri.to_string(),
            dataset: Arc::new(Mutex::new(dataset)),
            store_wrapper: write_store_wrapper,
        })
    }

    /// Version of this Table
    pub fn version(&self) -> u64 {
        self.dataset.lock().expect("lock poison").version().version
    }

    async fn optimize_indices(&self, options: &OptimizeOptions) -> Result<()> {
        info!("LanceDB: optimizing indices: {:?}", options);
        let mut dataset = self.clone_inner_dataset();
        dataset.optimize_indices(options).await?;

        Ok(())
    }

    pub fn query(&self) -> Query {
        Query::new(self.clone_inner_dataset().into())
    }

    pub fn filter(&self, expr: String) -> Query {
        Query::new(self.clone_inner_dataset().into()).filter(expr)
    }

    /// Returns the number of rows in this Table

    /// Merge new data into this table.
    pub async fn merge(
        &mut self,
        batches: impl RecordBatchReader + Send + 'static,
        left_on: &str,
        right_on: &str,
    ) -> Result<()> {
        let mut dataset = self.clone_inner_dataset();
        dataset.merge(batches, left_on, right_on).await?;
        self.dataset = Arc::new(Mutex::new(dataset));
        Ok(())
    }

    pub async fn update(&self, predicate: Option<&str>, updates: Vec<(&str, &str)>) -> Result<()> {
        let mut builder = UpdateBuilder::new(self.clone_inner_dataset().into());
        if let Some(predicate) = predicate {
            builder = builder.update_where(predicate)?;
        }

        for (column, value) in updates {
            builder = builder.set(column, value)?;
        }

        let operation = builder.build()?;
        let ds = operation.execute().await?;
        self.reset_dataset(ds.as_ref().clone());
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
        let dataset = self.clone_inner_dataset();
        Ok(dataset
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
        let mut dataset = self.clone_inner_dataset();
        let metrics = compact_files(&mut dataset, options, remap_options).await?;
        self.reset_dataset(dataset);
        Ok(metrics)
    }

    pub fn count_fragments(&self) -> usize {
        self.dataset.lock().expect("lock poison").count_fragments()
    }

    pub async fn count_deleted_rows(&self) -> Result<usize> {
        let dataset = self.clone_inner_dataset();
        Ok(dataset.count_deleted_rows().await?)
    }

    pub async fn num_small_files(&self, max_rows_per_group: usize) -> usize {
        let dataset = self.clone_inner_dataset();
        dataset.num_small_files(max_rows_per_group).await
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
        let dataset = self.clone_inner_dataset();
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
        let dataset = self.clone_inner_dataset();
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

    pub(crate) fn reset_dataset(&self, dataset: Dataset) {
        *self.dataset.lock().expect("lock poison") = dataset;
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

    fn schema(&self) -> SchemaRef {
        let lance_schema = { self.dataset.lock().expect("lock poison").schema().clone() };
        Arc::new(Schema::from(&lance_schema))
    }

    async fn count_rows(&self) -> Result<usize> {
        let dataset = { self.dataset.lock().expect("lock poison").clone() };
        Ok(dataset.count_rows().await?)
    }

    async fn add(
        &self,
        batches: Box<dyn RecordBatchReader + Send>,
        params: Option<WriteParams>,
    ) -> Result<()> {
        let params = Some(params.unwrap_or(WriteParams {
            mode: WriteMode::Append,
            ..WriteParams::default()
        }));

        // patch the params if we have a write store wrapper
        let params = match self.store_wrapper.clone() {
            Some(wrapper) => params.patch_with_store_wrapper(wrapper)?,
            None => params,
        };

        self.reset_dataset(Dataset::write(batches, &self.uri, params).await?);
        Ok(())
    }

    fn create_index(&self, columns: &[&str]) -> IndexBuilder {
        IndexBuilder::new(Arc::new(self.clone()), columns)
    }

    fn query(&self) -> Query {
        Query::new(Arc::new(self.dataset.lock().expect("lock poison").clone()))
    }

    /// Delete rows from the table
    async fn delete(&self, predicate: &str) -> Result<()> {
        let mut dataset = self.clone_inner_dataset();
        dataset.delete(predicate).await?;
        self.reset_dataset(dataset);
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

    fn merge_insert(&self) -> MergeInsertBuilder {
        MergeInsertBuilder::new(Arc::new(self.clone()))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

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
    async fn test_create_already_exists() {
        let tmp_dir = tempdir().unwrap();
        let uri = tmp_dir.path().to_str().unwrap();

        let batches = make_test_batches();
        let _ = batches.schema().clone();
        NativeTable::create(&uri, "test", batches, None, None)
            .await
            .unwrap();

        let batches = make_test_batches();
        let result = NativeTable::create(&uri, "test", batches, None, None).await;
        assert!(matches!(
            result.unwrap_err(),
            Error::TableAlreadyExists { .. }
        ));
    }

    #[tokio::test]
    async fn test_add() {
        let tmp_dir = tempdir().unwrap();
        let uri = tmp_dir.path().to_str().unwrap();

        let batches = make_test_batches();
        let schema = batches.schema().clone();
        let table = NativeTable::create(&uri, "test", batches, None, None)
            .await
            .unwrap();
        assert_eq!(table.count_rows().await.unwrap(), 10);

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

        table.add(Box::new(new_batches), None).await.unwrap();
        assert_eq!(table.count_rows().await.unwrap(), 20);
        assert_eq!(table.name, "test");
    }

    #[tokio::test]
    async fn test_add_overwrite() {
        let tmp_dir = tempdir().unwrap();
        let uri = tmp_dir.path().to_str().unwrap();

        let batches = make_test_batches();
        let schema = batches.schema().clone();
        let table = NativeTable::create(uri, "test", batches, None, None)
            .await
            .unwrap();
        assert_eq!(table.count_rows().await.unwrap(), 10);

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

        let param: WriteParams = WriteParams {
            mode: WriteMode::Overwrite,
            ..Default::default()
        };

        table.add(Box::new(new_batches), Some(param)).await.unwrap();
        assert_eq!(table.count_rows().await.unwrap(), 10);
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
                    Arc::new(Float32Array::from_iter_values(
                        (0..10).into_iter().map(|i| i as f32),
                    )),
                    Arc::new(Float64Array::from_iter_values(
                        (0..10).into_iter().map(|i| i as f64),
                    )),
                    Arc::new(Into::<BooleanArray>::into(vec![
                        true, false, true, false, true, false, true, false, true, false,
                    ])),
                    Arc::new(Date32Array::from_iter_values(0..10)),
                    Arc::new(TimestampNanosecondArray::from_iter_values(0..10)),
                    Arc::new(TimestampMillisecondArray::from_iter_values(0..10)),
                    Arc::new(
                        create_fixed_size_list(
                            Float32Array::from_iter_values((0..20).into_iter().map(|i| i as f32)),
                            2,
                        )
                        .unwrap(),
                    ),
                    Arc::new(
                        create_fixed_size_list(
                            Float64Array::from_iter_values((0..20).into_iter().map(|i| i as f64)),
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
            return original;
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

        let mut object_store_params = ObjectStoreParams::default();
        object_store_params.object_store_wrapper = Some(wrapper.clone());
        let param = ReadParams {
            store_options: Some(object_store_params),
            ..Default::default()
        };
        assert!(!wrapper.called());
        let _ = NativeTable::open_with_params(uri, "test", None, param)
            .await
            .unwrap();
        assert!(wrapper.called());
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

        let table = NativeTable::create(uri, "test", batches, None, None)
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
        assert_eq!(table.count_rows().await.unwrap(), 512);
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
}
