// Copyright 2023 LanceDB Developers.
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

use chrono::Duration;
use lance_index::IndexType;
use std::sync::Arc;

use arrow_array::{Float32Array, RecordBatchReader};
use arrow_schema::SchemaRef;
use lance::dataset::cleanup::RemovalStats;
use lance::dataset::optimize::{
    compact_files, CompactionMetrics, CompactionOptions, IndexRemapperOptions,
};
use lance::dataset::{Dataset, WriteParams};
use lance::index::DatasetIndexExt;
use lance::io::object_store::WrappingObjectStore;
use std::path::Path;

use crate::error::{Error, Result};
use crate::index::vector::{VectorIndex, VectorIndexBuilder};
use crate::query::Query;
use crate::utils::{PatchReadParam, PatchWriteParam};
use crate::WriteMode;

pub use lance::dataset::ReadParams;

pub const VECTOR_COLUMN_NAME: &str = "vector";

/// A table in a LanceDB database.
#[derive(Debug, Clone)]
pub struct Table {
    name: String,
    uri: String,
    dataset: Arc<Dataset>,

    // the object store wrapper to use on write path
    store_wrapper: Option<Arc<dyn WrappingObjectStore>>,
}

impl std::fmt::Display for Table {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Table({})", self.name)
    }
}

impl Table {
    /// Opens an existing Table
    ///
    /// # Arguments
    ///
    /// * `uri` - The uri to a [Table]
    /// * `name` - The table name
    ///
    /// # Returns
    ///
    /// * A [Table] object.
    pub async fn open(uri: &str) -> Result<Self> {
        let name = Self::get_table_name(uri)?;
        Self::open_with_params(uri, &name, None, ReadParams::default()).await
    }

    /// Open an Table with a given name.
    pub async fn open_with_name(uri: &str, name: &str) -> Result<Self> {
        Self::open_with_params(uri, name, None, ReadParams::default()).await
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
    /// * A [Table] object.
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
        let dataset = Dataset::open_with_params(uri, &params)
            .await
            .map_err(|e| match e {
                lance::Error::DatasetNotFound { .. } => Error::TableNotFound {
                    name: name.to_string(),
                },
                e => Error::Lance {
                    message: e.to_string(),
                },
            })?;
        Ok(Table {
            name: name.to_string(),
            uri: uri.to_string(),
            dataset: Arc::new(dataset),
            store_wrapper: write_store_wrapper,
        })
    }

    /// Checkout a specific version of this [`Table`]
    ///
    pub async fn checkout(uri: &str, version: u64) -> Result<Self> {
        let name = Self::get_table_name(uri)?;
        Self::checkout_with_params(uri, &name, version, None, ReadParams::default()).await
    }

    pub async fn checkout_with_name(uri: &str, name: &str, version: u64) -> Result<Self> {
        Self::checkout_with_params(uri, name, version, None, ReadParams::default()).await
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
        Ok(Table {
            name: name.to_string(),
            uri: uri.to_string(),
            dataset: Arc::new(dataset),
            store_wrapper: write_store_wrapper,
        })
    }

    pub async fn checkout_latest(&self) -> Result<Self> {
        let latest_version_id = self.dataset.latest_version_id().await?;
        let dataset = if latest_version_id == self.dataset.version().version {
            self.dataset.clone()
        } else {
            Arc::new(self.dataset.checkout_version(latest_version_id).await?)
        };

        Ok(Table {
            name: self.name.clone(),
            uri: self.uri.clone(),
            dataset,
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
    /// * A [Table] object.
    pub async fn create(
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
        Ok(Table {
            name: name.to_string(),
            uri: uri.to_string(),
            dataset: Arc::new(dataset),
            store_wrapper: write_store_wrapper,
        })
    }

    /// Schema of this Table.
    pub fn schema(&self) -> SchemaRef {
        Arc::new(self.dataset.schema().into())
    }

    /// Version of this Table
    pub fn version(&self) -> u64 {
        self.dataset.version().version
    }

    /// Create index on the table.
    pub async fn create_index(&mut self, index_builder: &impl VectorIndexBuilder) -> Result<()> {
        let mut dataset = self.dataset.as_ref().clone();
        dataset
            .create_index(
                &[index_builder
                    .get_column()
                    .unwrap_or(VECTOR_COLUMN_NAME.to_string())
                    .as_str()],
                IndexType::Vector,
                index_builder.get_index_name(),
                &index_builder.build(),
                index_builder.get_replace(),
            )
            .await?;
        self.dataset = Arc::new(dataset);
        Ok(())
    }

    pub async fn optimize_indices(&mut self) -> Result<()> {
        let mut dataset = self.dataset.as_ref().clone();

        dataset.optimize_indices().await?;

        Ok(())
    }

    /// Insert records into this Table
    ///
    /// # Arguments
    ///
    /// * `batches` RecordBatch to be saved in the Table
    /// * `write_mode` Append / Overwrite existing records. Default: Append
    /// # Returns
    ///
    /// * The number of rows added
    pub async fn add(
        &mut self,
        batches: impl RecordBatchReader + Send + 'static,
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

        self.dataset = Arc::new(Dataset::write(batches, &self.uri, params).await?);
        Ok(())
    }

    /// Creates a new Query object that can be executed.
    ///
    /// # Arguments
    ///
    /// * `vector` The vector used for this query.
    ///
    /// # Returns
    ///
    /// * A [Query] object.
    pub fn search(&self, query_vector: Float32Array) -> Query {
        Query::new(self.dataset.clone(), query_vector)
    }

    /// Returns the number of rows in this Table
    pub async fn count_rows(&self) -> Result<usize> {
        Ok(self.dataset.count_rows().await?)
    }

    /// Merge new data into this table.
    pub async fn merge(
        &mut self,
        batches: impl RecordBatchReader + Send + 'static,
        left_on: &str,
        right_on: &str,
    ) -> Result<()> {
        let mut dataset = self.dataset.as_ref().clone();
        dataset.merge(batches, left_on, right_on).await?;
        self.dataset = Arc::new(dataset);
        Ok(())
    }

    /// Delete rows from the table
    pub async fn delete(&mut self, predicate: &str) -> Result<()> {
        let mut dataset = self.dataset.as_ref().clone();
        dataset.delete(predicate).await?;
        self.dataset = Arc::new(dataset);
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
    pub async fn cleanup_old_versions(
        &self,
        older_than: Duration,
        delete_unverified: Option<bool>,
    ) -> Result<RemovalStats> {
        Ok(self
            .dataset
            .cleanup_old_versions(older_than, delete_unverified)
            .await?)
    }

    /// Compact files in the dataset.
    ///
    /// This can be run after making several small appends to optimize the table
    /// for faster reads.
    ///
    /// This calls into [lance::dataset::optimize::compact_files].
    pub async fn compact_files(
        &mut self,
        options: CompactionOptions,
        remap_options: Option<Arc<dyn IndexRemapperOptions>>,
    ) -> Result<CompactionMetrics> {
        let mut dataset = self.dataset.as_ref().clone();
        let metrics = compact_files(&mut dataset, options, remap_options).await?;
        self.dataset = Arc::new(dataset);
        Ok(metrics)
    }

    pub fn count_fragments(&self) -> usize {
        self.dataset.count_fragments()
    }

    pub async fn count_deleted_rows(&self) -> Result<usize> {
        Ok(self.dataset.count_deleted_rows().await?)
    }

    pub async fn num_small_files(&self, max_rows_per_group: usize) -> usize {
        self.dataset.num_small_files(max_rows_per_group).await
    }

    pub async fn count_indexed_rows(&self, index_uuid: &str) -> Result<Option<usize>> {
        Ok(self.dataset.count_indexed_rows(index_uuid).await?)
    }

    pub async fn count_unindexed_rows(&self, index_uuid: &str) -> Result<Option<usize>> {
        Ok(self.dataset.count_unindexed_rows(index_uuid).await?)
    }

    pub async fn load_indices(&self) -> Result<Vec<VectorIndex>> {
        let (indices, mf) =
            futures::try_join!(self.dataset.load_indices(), self.dataset.latest_manifest())?;
        Ok(indices
            .iter()
            .map(|i| VectorIndex::new_from_format(&mf, i))
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    use arrow_array::{
        Array, FixedSizeListArray, Float32Array, Int32Array, RecordBatch, RecordBatchIterator,
        RecordBatchReader,
    };
    use arrow_data::ArrayDataBuilder;
    use arrow_schema::{DataType, Field, Schema};
    use lance::dataset::{Dataset, WriteMode};
    use lance::index::vector::ivf::IvfBuildParams;
    use lance::index::vector::pq::PQBuildParams;
    use lance::io::object_store::{ObjectStoreParams, WrappingObjectStore};
    use rand::Rng;
    use tempfile::tempdir;

    use super::*;
    use crate::index::vector::IvfPQIndexBuilder;

    #[tokio::test]
    async fn test_open() {
        let tmp_dir = tempdir().unwrap();
        let dataset_path = tmp_dir.path().join("test.lance");

        let batches = make_test_batches();
        Dataset::write(batches, dataset_path.to_str().unwrap(), None)
            .await
            .unwrap();

        let table = Table::open(dataset_path.to_str().unwrap()).await.unwrap();

        assert_eq!(table.name, "test")
    }

    #[tokio::test]
    async fn test_open_not_found() {
        let tmp_dir = tempdir().unwrap();
        let uri = tmp_dir.path().to_str().unwrap();
        let table = Table::open(uri).await;
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
        Table::create(&uri, "test", batches, None, None)
            .await
            .unwrap();

        let batches = make_test_batches();
        let result = Table::create(&uri, "test", batches, None, None).await;
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
        let mut table = Table::create(&uri, "test", batches, None, None)
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

        table.add(new_batches, None).await.unwrap();
        assert_eq!(table.count_rows().await.unwrap(), 20);
        assert_eq!(table.name, "test");
    }

    #[tokio::test]
    async fn test_add_overwrite() {
        let tmp_dir = tempdir().unwrap();
        let uri = tmp_dir.path().to_str().unwrap();

        let batches = make_test_batches();
        let schema = batches.schema().clone();
        let mut table = Table::create(uri, "test", batches, None, None)
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

        table.add(new_batches, Some(param)).await.unwrap();
        assert_eq!(table.count_rows().await.unwrap(), 10);
        assert_eq!(table.name, "test");
    }

    #[tokio::test]
    async fn test_search() {
        let tmp_dir = tempdir().unwrap();
        let dataset_path = tmp_dir.path().join("test.lance");
        let uri = dataset_path.to_str().unwrap();

        let batches = make_test_batches();
        Dataset::write(batches, dataset_path.to_str().unwrap(), None)
            .await
            .unwrap();

        let table = Table::open(uri).await.unwrap();

        let vector = Float32Array::from_iter_values([0.1, 0.2]);
        let query = table.search(vector.clone());
        assert_eq!(vector, query.query_vector);
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
        let _ = Table::open_with_params(uri, "test", None, param)
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

        let mut table = Table::create(uri, "test", batches, None, None)
            .await
            .unwrap();
        let mut i = IvfPQIndexBuilder::new();

        let index_builder = i
            .column("embeddings".to_string())
            .index_name("my_index".to_string())
            .ivf_params(IvfBuildParams::new(256))
            .pq_params(PQBuildParams::default());

        table.create_index(index_builder).await.unwrap();

        assert_eq!(table.dataset.load_indices().await.unwrap().len(), 1);
        assert_eq!(table.count_rows().await.unwrap(), 512);
        assert_eq!(table.name, "test");
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
