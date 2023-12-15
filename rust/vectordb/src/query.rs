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

use std::sync::Arc;

use arrow_array::Float32Array;
use lance::dataset::scanner::{DatasetRecordBatchStream, Scanner};
use lance::dataset::Dataset;
use lance_linalg::distance::MetricType;

use crate::error::Result;

/// A builder for nearest neighbor queries for LanceDB.
pub struct Query {
    pub dataset: Arc<Dataset>,
    pub query_vector: Option<Float32Array>,
    pub column: String,
    pub limit: Option<usize>,
    pub filter: Option<String>,
    pub select: Option<Vec<String>>,
    pub nprobes: usize,
    pub refine_factor: Option<u32>,
    pub metric_type: Option<MetricType>,
    pub use_index: bool,
    pub prefilter: bool,
}

impl Query {
    /// Creates a new Query object
    ///
    /// # Arguments
    ///
    /// * `dataset` - The table / dataset the query will be run against.
    /// * `vector` The vector used for this query.
    ///
    /// # Returns
    ///
    /// * A [Query] object.
    pub(crate) fn new(dataset: Arc<Dataset>, vector: Option<Float32Array>) -> Self {
        Query {
            dataset,
            query_vector: vector,
            column: crate::table::VECTOR_COLUMN_NAME.to_string(),
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

    /// Execute the queries and return its results.
    ///
    /// # Returns
    ///
    /// * A [DatasetRecordBatchStream] with the query's results.
    pub async fn execute(&self) -> Result<DatasetRecordBatchStream> {
        let mut scanner: Scanner = self.dataset.scan();

        if let Some(query) = self.query_vector.as_ref() {
            // If there is a vector query, default to limit=10 if unspecified
            scanner.nearest(&self.column, query, self.limit.unwrap_or(10))?;
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
    pub fn column(mut self, column: &str) -> Query {
        self.column = column.into();
        self
    }

    /// Set the maximum number of results to return.
    ///
    /// # Arguments
    ///
    /// * `limit` - The maximum number of results to return.
    pub fn limit(mut self, limit: usize) -> Query {
        self.limit = Some(limit);
        self
    }

    /// Set the vector used for this query.
    ///
    /// # Arguments
    ///
    /// * `vector` - The vector that will be used for search.
    pub fn query_vector(mut self, query_vector: Float32Array) -> Query {
        self.query_vector = Some(query_vector);
        self
    }

    /// Set the number of probes to use.
    ///
    /// # Arguments
    ///
    /// * `nprobes` - The number of probes to use.
    pub fn nprobes(mut self, nprobes: usize) -> Query {
        self.nprobes = nprobes;
        self
    }

    /// Set the refine factor to use.
    ///
    /// # Arguments
    ///
    /// * `refine_factor` - The refine factor to use.
    pub fn refine_factor(mut self, refine_factor: Option<u32>) -> Query {
        self.refine_factor = refine_factor;
        self
    }

    /// Set the distance metric to use.
    ///
    /// # Arguments
    ///
    /// * `metric_type` - The distance metric to use. By default [MetricType::L2] is used.
    pub fn metric_type(mut self, metric_type: Option<MetricType>) -> Query {
        self.metric_type = metric_type;
        self
    }

    /// Whether to use an ANN index if available
    ///
    /// # Arguments
    ///
    /// * `use_index` - Sets Whether to use an ANN index if available
    pub fn use_index(mut self, use_index: bool) -> Query {
        self.use_index = use_index;
        self
    }

    ///  A filter statement to be applied to this query.
    ///
    /// # Arguments
    ///
    /// * `filter` -  value A filter in the same format used by a sql WHERE clause.
    pub fn filter(mut self, filter: Option<String>) -> Query {
        self.filter = filter;
        self
    }

    /// Return only the specified columns.
    ///
    /// Only select the specified columns. If not specified, all columns will be returned.
    pub fn select(mut self, columns: Option<Vec<String>>) -> Query {
        self.select = columns;
        self
    }

    pub fn prefilter(mut self, prefilter: bool) -> Query {
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

    use crate::query::Query;

    #[tokio::test]
    async fn test_setters_getters() {
        let batches = make_test_batches();
        let ds = Dataset::write(batches, "memory://foo", None).await.unwrap();

        let vector = Some(Float32Array::from_iter_values([0.1, 0.2]));
        let query = Query::new(Arc::new(ds), vector.clone());
        assert_eq!(query.query_vector, vector);

        let new_vector = Float32Array::from_iter_values([9.8, 8.7]);

        let query = query
            .query_vector(new_vector.clone())
            .limit(100)
            .nprobes(1000)
            .use_index(true)
            .metric_type(Some(MetricType::Cosine))
            .refine_factor(Some(999));

        assert_eq!(query.query_vector.unwrap(), new_vector);
        assert_eq!(query.limit.unwrap(), 100);
        assert_eq!(query.nprobes, 1000);
        assert_eq!(query.use_index, true);
        assert_eq!(query.metric_type, Some(MetricType::Cosine));
        assert_eq!(query.refine_factor, Some(999));
    }

    #[tokio::test]
    async fn test_execute() {
        let batches = make_non_empty_batches();
        let ds = Arc::new(Dataset::write(batches, "memory://foo", None).await.unwrap());

        let vector = Some(Float32Array::from_iter_values([0.1; 4]));

        let query = Query::new(ds.clone(), vector.clone());
        let result = query
            .limit(10)
            .filter(Some("id % 2 == 0".to_string()))
            .execute()
            .await;
        let mut stream = result.expect("should have result");
        // should only have one batch
        while let Some(batch) = stream.next().await {
            // post filter should have removed some rows
            assert!(batch.expect("should be Ok").num_rows() < 10);
        }

        let query = Query::new(ds, vector.clone());
        let result = query
            .limit(10)
            .filter(Some("id % 2 == 0".to_string()))
            .prefilter(true)
            .execute()
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

        let query = Query::new(ds.clone(), None);
        let result = query
            .filter(Some("id % 2 == 0".to_string()))
            .execute()
            .await;
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
}
