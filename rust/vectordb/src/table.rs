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
use lance::dataset::{Dataset, WriteParams};

use crate::error::{Error, Result};
use crate::query::Query;

pub const VECTOR_COLUMN_NAME: &str = "vector";

pub const LANCE_FILE_EXTENSION: &str = "lance";

/// A table in a LanceDB database.
pub struct Table {
    name: String,
    dataset: Arc<Dataset>,
}

impl Table {
    /// Creates a new Table object
    ///
    /// # Arguments
    ///
    /// * `base_path` - The base path where the table is located
    /// * `name` The Table name
    ///
    /// # Returns
    ///
    /// * A [Table] object.
    pub async fn new(base_path: Arc<PathBuf>, name: String) -> Result<Self> {
        let ds_path = base_path.join(format!("{}.{}", name, LANCE_FILE_EXTENSION));
        let ds_uri = ds_path
            .to_str()
            .ok_or(Error::IO(format!("Unable to find table {}", name)))?;
        let dataset = Dataset::open(ds_uri).await?;
        let table = Table {
            name,
            dataset: Arc::new(dataset),
        };
        Ok(table)
    }

    pub async fn create(
        base_path: Arc<PathBuf>,
        name: String,
        batches: &mut Box<dyn RecordBatchReader + Send>,
    ) -> Result<Self> {
        let ds_path = base_path.join(format!("{}.{}", name, LANCE_FILE_EXTENSION));
        let ds_uri = ds_path
            .to_str()
            .ok_or(Error::IO(format!("Unable to find table {}", name)))?;

        // I had to make changes to Lance's ds write signature - add + Send to the batches parameters
        //
        // pub async fn write(
        //     batches: &mut Box<dyn RecordBatchReader + Send>,
        //     uri: &str,
        //     params: Option<WriteParams>,
        // ) -> lance::Result<Self> {

        let dataset = Arc::new(Dataset::write(batches, ds_uri, Some(WriteParams::default())).await?);
        Ok(Table { name, dataset })
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
}

#[cfg(test)]
mod tests {
    use arrow_array::{Float32Array, Int32Array, RecordBatch, RecordBatchReader};
    use arrow_schema::{DataType, Field, Schema};
    use lance::arrow::RecordBatchBuffer;
    use lance::dataset::Dataset;
    use std::sync::Arc;
    use tempfile::tempdir;

    use crate::table::Table;

    #[tokio::test]
    async fn test_new_table_not_exists() {
        let tmp_dir = tempdir().unwrap();
        let path_buf = tmp_dir.into_path();

        let table = Table::new(Arc::new(path_buf), "test".to_string()).await;
        assert!(table.is_err());
    }

    #[tokio::test]
    async fn test_new() {
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

        let table = Table::new(Arc::new(path_buf), "test".to_string())
            .await
            .unwrap();

        assert_eq!(table.name, "test")
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

        let table = Table::new(Arc::new(path_buf), "test".to_string())
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
            vec![Arc::new(Int32Array::from_iter_values(0..20))],
        )
        .unwrap()])
    }
}
