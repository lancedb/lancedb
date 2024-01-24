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
use lance::dataset::builder::DatasetBuilder;
use lance::index::scalar::ScalarIndexParams;
use lance_index::optimize::OptimizeOptions;
use lance_index::IndexType;
use std::sync::Arc;

use arrow_array::{Float32Array, RecordBatchReader};
use arrow_schema::SchemaRef;
use lance::dataset::cleanup::RemovalStats;
use lance::dataset::optimize::{
    compact_files, CompactionMetrics, CompactionOptions, IndexRemapperOptions,
};
use lance::dataset::{Dataset, UpdateBuilder, WriteParams};
use lance::io::WrappingObjectStore;
use lance_index::DatasetIndexExt;
use std::path::Path;

use crate::error::{Error, Result};
use crate::index::vector::{VectorIndex, VectorIndexBuilder, VectorIndexStatistics};
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

    /// Create a scalar index on the table
    pub async fn create_scalar_index(&mut self, column: &str, replace: bool) -> Result<()> {
        let mut dataset = self.dataset.as_ref().clone();
        let params = ScalarIndexParams::default();
        dataset
            .create_index(&[column], IndexType::Scalar, None, &params, replace)
            .await?;
        Ok(())
    }

    pub async fn optimize_indices(&mut self, options: &OptimizeOptions) -> Result<()> {
        let mut dataset = self.dataset.as_ref().clone();
        dataset.optimize_indices(options).await?;

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

    pub fn query(&self) -> Query {
        Query::new(self.dataset.clone(), None)
    }

    /// Creates a new Query object that can be executed.
    ///
    /// # Arguments
    ///
    /// * `query_vector` The vector used for this query.
    ///
    /// # Returns
    /// * A [Query] object.
    pub fn search<T: Into<Float32Array>>(&self, query_vector: Option<T>) -> Query {
        Query::new(self.dataset.clone(), query_vector.map(|q| q.into()))
    }

    pub fn filter(&self, expr: String) -> Query {
        Query::new(self.dataset.clone(), None).filter(Some(expr))
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

    pub async fn update(
        &mut self,
        predicate: Option<&str>,
        updates: Vec<(&str, &str)>,
    ) -> Result<()> {
        let mut builder = UpdateBuilder::new(self.dataset.clone());
        if let Some(predicate) = predicate {
            builder = builder.update_where(predicate)?;
        }

        for (column, value) in updates {
            builder = builder.set(column, value)?;
        }

        let operation = builder.build()?;
        let new_ds = operation.execute().await?;
        self.dataset = new_ds;

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
        let (indices, mf) =
            futures::try_join!(self.dataset.load_indices(), self.dataset.latest_manifest())?;
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
        let index_stats = self
            .dataset
            .index_statistics(&index.unwrap().index_name)
            .await?;
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
    use lance::index::vector::pq::PQBuildParams;
    use lance::io::{ObjectStoreParams, WrappingObjectStore};
    use lance_index::vector::ivf::IvfBuildParams;
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
        let mut table = Table::open(uri).await.unwrap();

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
        let mut table = Table::open(uri).await.unwrap();

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
        let query = table.search(Some(vector.clone()));
        assert_eq!(vector, query.query_vector.unwrap());
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

        assert_eq!(table.count_indexed_rows("my_index").await.unwrap(), None);
        assert_eq!(table.count_unindexed_rows("my_index").await.unwrap(), None);

        let index_builder = i
            .column("embeddings".to_string())
            .index_name("my_index".to_string())
            .ivf_params(IvfBuildParams::new(256))
            .pq_params(PQBuildParams::default());

        table.create_index(index_builder).await.unwrap();

        assert_eq!(table.dataset.load_indices().await.unwrap().len(), 1);
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
