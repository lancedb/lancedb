// Copyright 2024 Lance Developers.
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

use std::sync::Arc;

use arrow_array::Float32Array;
use arrow_schema::Schema;
use lance::dataset::scanner::{DatasetRecordBatchStream, Scanner};
use lance::dataset::Dataset;
use lance_linalg::distance::MetricType;

use crate::error::Result;
use crate::utils::default_vector_column;
use crate::Error;

const DEFAULT_TOP_K: usize = 10;

/// A builder for nearest neighbor queries for LanceDB.
#[derive(Clone)]
pub struct Query {
    dataset: Arc<Dataset>,

    // The column to run the query on. If not specified, we will attempt to guess
    // the column based on the dataset's schema.
    column: Option<String>,

    // IVF PQ - ANN search.
    query_vector: Option<Float32Array>,
    nprobes: usize,
    refine_factor: Option<u32>,
    metric_type: Option<MetricType>,

    /// limit the number of rows to return.
    limit: Option<usize>,
    /// Apply filter to the returned rows.
    filter: Option<String>,
    /// Select column projection.
    select: Option<Vec<String>>,

    /// Default is true. Set to false to enforce a brute force search.
    use_index: bool,
    /// Apply filter before ANN search/
    prefilter: bool,
}

impl Query {
    /// Creates a new Query object
    ///
    /// # Arguments
    ///
    /// * `dataset` - Lance dataset.
    ///
    pub(crate) fn new(dataset: Arc<Dataset>) -> Self {
        Self {
            dataset,
            query_vector: None,
            column: None,
            limit: None,
            nprobes: 20,
            refine_factor: None,
            metric_type: None,
            use_index: true,
            filter: None,
            select: None,
            prefilter: false,
        }
    }

    /// Convert the query plan to a [`DatasetRecordBatchStream`]
    ///
    /// # Returns
    ///
    /// * A [DatasetRecordBatchStream] with the query's results.
    pub async fn execute_stream(&self) -> Result<DatasetRecordBatchStream> {
        let mut scanner: Scanner = self.dataset.scan();

        if let Some(query) = self.query_vector.as_ref() {
            // If there is a vector query, default to limit=10 if unspecified
            let column = if let Some(col) = self.column.as_ref() {
                col.clone()
            } else {
                // Infer a vector column with the same dimension of the query vector.
                let arrow_schema = Schema::from(self.dataset.schema());
                default_vector_column(&arrow_schema, Some(query.len() as i32))?
            };
            let field = self.dataset.schema().field(&column).ok_or(Error::Store {
                message: format!("Column {} not found in dataset schema", column),
            })?;
            if !matches!(field.data_type(), arrow_schema::DataType::FixedSizeList(f, dim) if f.data_type().is_floating() && dim == query.len() as i32)
            {
                return Err(Error::Store {
                    message: format!(
                        "Vector column '{}' does not match the dimension of the query vector: dim={}",
                        column,
                        query.len(),
                    ),
                });
            }
            scanner.nearest(&column, query, self.limit.unwrap_or(DEFAULT_TOP_K))?;
        } else {
            // If there is no vector query, it's ok to not have a limit
            scanner.limit(self.limit.map(|limit| limit as i64), None)?;
        }
        scanner.nprobs(self.nprobes);
        scanner.use_index(self.use_index);
        scanner.prefilter(self.prefilter);

        self.select.as_ref().map(|p| scanner.project(p.as_slice()));
        self.filter.as_ref().map(|f| scanner.filter(f));
        self.refine_factor.map(|rf| scanner.refine(rf));
        self.metric_type.map(|mt| scanner.distance_metric(mt));
        Ok(scanner.try_into_stream().await?)
    }

    /// Set the column to query
    ///
    /// # Arguments
    ///
    /// * `column` - The column name
    pub fn column(mut self, column: &str) -> Self {
        self.column = Some(column.to_string());
        self
    }

    /// Set the maximum number of results to return.
    ///
    /// # Arguments
    ///
    /// * `limit` - The maximum number of results to return.
    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Find the nearest vectors to the given query vector.
    ///
    /// # Arguments
    ///
    /// * `vector` - The vector that will be used for search.
    pub fn nearest_to(mut self, vector: &[f32]) -> Self {
        self.query_vector = Some(Float32Array::from(vector.to_vec()));
        self
    }

    /// Set the number of probes to use.
    ///
    /// # Arguments
    ///
    /// * `nprobes` - The number of probes to use.
    pub fn nprobes(mut self, nprobes: usize) -> Self {
        self.nprobes = nprobes;
        self
    }

    /// Set the refine factor to use.
    ///
    /// # Arguments
    ///
    /// * `refine_factor` - The refine factor to use.
    pub fn refine_factor(mut self, refine_factor: u32) -> Self {
        self.refine_factor = Some(refine_factor);
        self
    }

    /// Set the distance metric to use.
    ///
    /// # Arguments
    ///
    /// * `metric_type` - The distance metric to use. By default [MetricType::L2] is used.
    pub fn metric_type(mut self, metric_type: MetricType) -> Self {
        self.metric_type = Some(metric_type);
        self
    }

    /// Whether to use an ANN index if available
    ///
    /// # Arguments
    ///
    /// * `use_index` - Sets Whether to use an ANN index if available
    pub fn use_index(mut self, use_index: bool) -> Self {
        self.use_index = use_index;
        self
    }

    ///  A filter statement to be applied to this query.
    ///
    /// # Arguments
    ///
    /// * `filter` - SQL filter
    pub fn filter(mut self, filter: impl AsRef<str>) -> Self {
        self.filter = Some(filter.as_ref().to_string());
        self
    }

    /// Return only the specified columns.
    ///
    /// Only select the specified columns. If not specified, all columns will be returned.
    pub fn select(mut self, columns: &[impl AsRef<str>]) -> Self {
        self.select = Some(columns.iter().map(|c| c.as_ref().to_string()).collect());
        self
    }

    pub fn prefilter(mut self, prefilter: bool) -> Self {
        self.prefilter = prefilter;
        self
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use arrow_array::{
        cast::AsArray, Float32Array, Int32Array, RecordBatch, RecordBatchIterator,
        RecordBatchReader,
    };
    use arrow_schema::{DataType, Field as ArrowField, Schema as ArrowSchema};
    use futures::StreamExt;
    use lance::dataset::Dataset;
    use lance_testing::datagen::{BatchGenerator, IncrementingInt32, RandomVector};
    use tempfile::tempdir;

    use crate::query::Query;
    use crate::table::{NativeTable, Table};

    #[tokio::test]
    async fn test_setters_getters() {
        let batches = make_test_batches();
        let ds = Dataset::write(batches, "memory://foo", None).await.unwrap();

        let vector = Some(Float32Array::from_iter_values([0.1, 0.2]));
        let query = Query::new(Arc::new(ds)).nearest_to(&[0.1, 0.2]);
        assert_eq!(query.query_vector, vector);

        let new_vector = Float32Array::from_iter_values([9.8, 8.7]);

        let query = query
            .nearest_to(&[9.8, 8.7])
            .limit(100)
            .nprobes(1000)
            .use_index(true)
            .metric_type(MetricType::Cosine)
            .refine_factor(999);

        assert_eq!(query.query_vector.unwrap(), new_vector);
        assert_eq!(query.limit.unwrap(), 100);
        assert_eq!(query.nprobes, 1000);
        assert!(query.use_index);
        assert_eq!(query.metric_type, Some(MetricType::Cosine));
        assert_eq!(query.refine_factor, Some(999));
    }

    #[tokio::test]
    async fn test_execute() {
        let batches = make_non_empty_batches();
        let ds = Arc::new(Dataset::write(batches, "memory://foo", None).await.unwrap());

        let query = Query::new(ds.clone()).nearest_to(&[0.1; 4]);
        let result = query.limit(10).filter("id % 2 == 0").execute_stream().await;
        let mut stream = result.expect("should have result");
        // should only have one batch
        while let Some(batch) = stream.next().await {
            // post filter should have removed some rows
            assert!(batch.expect("should be Ok").num_rows() < 10);
        }

        let query = Query::new(ds).nearest_to(&[0.1; 4]);
        let result = query
            .limit(10)
            .filter(String::from("id % 2 == 0")) // Work with String too
            .prefilter(true)
            .execute_stream()
            .await;
        let mut stream = result.expect("should have result");
        // should only have one batch
        while let Some(batch) = stream.next().await {
            // pre filter should return 10 rows
            assert!(batch.expect("should be Ok").num_rows() == 10);
        }
    }

    #[tokio::test]
    async fn test_execute_no_vector() {
        // test that it's ok to not specify a query vector (just filter / limit)
        let batches = make_non_empty_batches();
        let ds = Arc::new(Dataset::write(batches, "memory://foo", None).await.unwrap());

        let query = Query::new(ds.clone());
        let result = query.filter("id % 2 == 0").execute_stream().await;
        let mut stream = result.expect("should have result");
        // should only have one batch
        while let Some(batch) = stream.next().await {
            let b = batch.expect("should be Ok");
            // cast arr into Int32Array
            let arr: &Int32Array = b["id"].as_primitive();
            assert!(arr.iter().all(|x| x.unwrap() % 2 == 0));
        }
    }

    fn make_non_empty_batches() -> impl RecordBatchReader + Send + 'static {
        let vec = Box::new(RandomVector::new().named("vector".to_string()));
        let id = Box::new(IncrementingInt32::new().named("id".to_string()));
        BatchGenerator::new().col(vec).col(id).batch(512)
    }

    fn make_test_batches() -> impl RecordBatchReader + Send + 'static {
        let dim: usize = 128;
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("key", DataType::Int32, false),
            ArrowField::new(
                "vector",
                DataType::FixedSizeList(
                    Arc::new(ArrowField::new("item", DataType::Float32, true)),
                    dim as i32,
                ),
                true,
            ),
            ArrowField::new("uri", DataType::Utf8, true),
        ]));
        RecordBatchIterator::new(
            vec![RecordBatch::new_empty(schema.clone())]
                .into_iter()
                .map(Ok),
            schema,
        )
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

        let table = NativeTable::open(uri).await.unwrap();

        let query = table.search(&[0.1, 0.2]);
        assert_eq!(&[0.1, 0.2], query.query_vector.unwrap().values());
    }
}
