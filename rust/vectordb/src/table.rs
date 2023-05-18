// Copyright 2023 Lance Developers.
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

use std::path::PathBuf;
use std::sync::Arc;

use arrow_array::{Float32Array, RecordBatchReader};
use lance::dataset::{Dataset, WriteMode, WriteParams};
use lance::index::{DatasetIndexExt, IndexParams, IndexType};
use lance::index::vector::{MetricType, VectorIndexParams};

use crate::error::{Error, Result};
use crate::query::Query;

pub const VECTOR_COLUMN_NAME: &str = "vector";

pub const LANCE_FILE_EXTENSION: &str = "lance";

/// A table in a LanceDB database.
pub struct Table {
    name: String,
    path: String,
    dataset: Arc<Dataset>,
}

impl Table {
    /// Opens an existing Table
    ///
    /// # Arguments
    ///
    /// * `base_path` - The base path where the table is located
    /// * `name` The Table name
    ///
    /// # Returns
    ///
    /// * A [Table] object.
    pub async fn open(base_path: Arc<PathBuf>, name: String) -> Result<Self> {
        let ds_path = base_path.join(format!("{}.{}", name, LANCE_FILE_EXTENSION));
        let ds_uri = ds_path
            .to_str()
            .ok_or(Error::IO(format!("Unable to find table {}", name)))?;
        let dataset = Dataset::open(ds_uri).await?;
        let table = Table {
            name,
            path: ds_uri.to_string(),
            dataset: Arc::new(dataset),
        };
        Ok(table)
    }

    /// Creates a new Table
    ///
    /// # Arguments
    ///
    /// * `base_path` - The base path where the table is located
    /// * `name` The Table name
    /// * `batches` RecordBatch to be saved in the database
    ///
    /// # Returns
    ///
    /// * A [Table] object.
    pub async fn create(
        base_path: Arc<PathBuf>,
        name: String,
        mut batches: Box<dyn RecordBatchReader>,
    ) -> Result<Self> {
        let ds_path = base_path.join(format!("{}.{}", name, LANCE_FILE_EXTENSION));
        let path = ds_path
            .to_str()
            .ok_or(Error::IO(format!("Unable to find table {}", name)))?;

        let dataset =
            Arc::new(Dataset::write(&mut batches, path, Some(WriteParams::default())).await?);
        Ok(Table { name, path: path.to_string(), dataset })
    }

    //
    //     What is the best option to expose create index api?
    //

    // Option 1 - One api per index type, parameters match what the index type expects.
    pub async fn create_index_ivf(&self, metric_type: MetricType, num_bits: u8, num_partitions: usize, num_sub_vectors: usize, use_opq: bool, metric_type, max_iterations: usize) {
        let params: &dyn IndexParams = &VectorIndexParams::ivf_pq(num_partitions, num_bits, num_sub_vectors, use_opq, metric_type, max_iterations);
        self.dataset.create_index(&*[VECTOR_COLUMN_NAME],IndexType::Vector, None, params)
    }
    pub async fn create_index_diskann(&self, /* params here */) {}

    // Option 2 - Single API, accepts IndexParams defined in the lance crate
    pub async fn create_index(&self, params: &dyn IndexParams) {
        self.dataset.create_index(&*[VECTOR_COLUMN_NAME],IndexType::Vector, None, params)
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
        mut batches: Box<dyn RecordBatchReader>,
        write_mode: Option<WriteMode>
    ) -> Result<usize> {
        let mut params = WriteParams::default();
        params.mode = write_mode.unwrap_or(WriteMode::Append);

        self.dataset = Arc::new(Dataset::write(&mut batches, self.path.as_str(), Some(params)).await?);
        Ok(batches.count())
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
}

#[cfg(test)]
mod tests {
    use arrow_array::{Float32Array, Int32Array, RecordBatch, RecordBatchReader};
    use arrow_schema::{DataType, Field, Schema};
    use lance::arrow::RecordBatchBuffer;
    use lance::dataset::{Dataset, WriteMode};
    use std::sync::Arc;
    use tempfile::tempdir;

    use crate::table::Table;

    #[tokio::test]
    async fn test_new_table_not_exists() {
        let tmp_dir = tempdir().unwrap();
        let path_buf = tmp_dir.into_path();

        let table = Table::open(Arc::new(path_buf), "test".to_string()).await;
        assert!(table.is_err());
    }

    #[tokio::test]
    async fn test_open() {
        let tmp_dir = tempdir().unwrap();
        let path_buf = tmp_dir.into_path();

        let mut batches: Box<dyn RecordBatchReader> = Box::new(make_test_batches());
        Dataset::write(
            &mut batches,
            path_buf.join("test.lance").to_str().unwrap(),
            None,
        )
        .await
        .unwrap();

        let table = Table::open(Arc::new(path_buf), "test".to_string())
            .await
            .unwrap();

        assert_eq!(table.name, "test")
    }

    #[tokio::test]
    async fn test_add() {
        let tmp_dir = tempdir().unwrap();
        let path_buf = tmp_dir.into_path();

        let batches: Box<dyn RecordBatchReader> = Box::new(make_test_batches());
        let schema = batches.schema().clone();
        let mut table = Table::create(Arc::new(path_buf), "test".to_string(), batches).await.unwrap();
        assert_eq!(table.count_rows().await.unwrap(), 10);

        let new_batches: Box<dyn RecordBatchReader> = Box::new(RecordBatchBuffer::new(vec![RecordBatch::try_new(
            schema,
            vec![Arc::new(Int32Array::from_iter_values(100..110))],
        )
       .unwrap()]));

        table.add(new_batches, None).await.unwrap();
        assert_eq!(table.count_rows().await.unwrap(), 20);
        assert_eq!(table.name, "test");
    }

    #[tokio::test]
    async fn test_add_overwrite() {
        let tmp_dir = tempdir().unwrap();
        let path_buf = tmp_dir.into_path();

        let batches: Box<dyn RecordBatchReader> = Box::new(make_test_batches());
        let schema = batches.schema().clone();
        let mut table = Table::create(Arc::new(path_buf), "test".to_string(), batches).await.unwrap();
        assert_eq!(table.count_rows().await.unwrap(), 10);

        let new_batches: Box<dyn RecordBatchReader> = Box::new(RecordBatchBuffer::new(vec![RecordBatch::try_new(
            schema,
            vec![Arc::new(Int32Array::from_iter_values(100..110))],
        ).unwrap()]));

        table.add(new_batches, Some(WriteMode::Overwrite)).await.unwrap();
        assert_eq!(table.count_rows().await.unwrap(), 10);
        assert_eq!(table.name, "test");
    }

    #[tokio::test]
    async fn test_search() {
        let tmp_dir = tempdir().unwrap();
        let path_buf = tmp_dir.into_path();

        let mut batches: Box<dyn RecordBatchReader> = Box::new(make_test_batches());
        Dataset::write(
            &mut batches,
            path_buf.join("test.lance").to_str().unwrap(),
            None,
        )
        .await
        .unwrap();

        let table = Table::open(Arc::new(path_buf), "test".to_string())
            .await
            .unwrap();

        let vector = Float32Array::from_iter_values([0.1, 0.2]);
        let query = table.search(vector.clone());
        assert_eq!(vector, query.query_vector);
    }

    fn make_test_batches() -> RecordBatchBuffer {
        let schema = Arc::new(Schema::new(vec![Field::new("i", DataType::Int32, false)]));
        RecordBatchBuffer::new(vec![RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from_iter_values(0..10))],
        )
        .unwrap()])
    }
}
