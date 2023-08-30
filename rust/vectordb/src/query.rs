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

use arrow_array::{Array, Float32Array};
use arrow_schema::DataType;
use lance::dataset::{
    scanner::{DatasetRecordBatchStream, Scanner},
    Dataset,
};
use lance::datatypes::Schema;
use lance::index::vector::MetricType;

use crate::error::{Error, Result};

struct VectorQuery<T: Array> {
    query: T,
    column: String,
    nprobs: usize,
    refine_factor: Option<u32>,
    metric_type: Option<MetricType>,
    use_index: bool,
}

/// Best effort to find potential vector columns in a [Schema], which is a fixed size column with
/// float number, where the list size is equal to the vector dimension.
///
fn find_vector_columns(schema: &Schema, dim: i32) -> Vec<String> {
    schema
        .fields
        .iter()
        .filter(|f| match &f.data_type() {
            DataType::FixedSizeList(field, list_size) => {
                *list_size == dim && field.data_type().is_floating()
            }
            _ => false,
        })
        .map(|f| f.name.to_string())
        .collect()
}

impl<T: Array> VectorQuery<T> {
    fn try_new(dataset: &Dataset, query: T) -> Result<Self> {
        let schema = dataset.schema();
        let dim: i32 = query.len() as i32;
        let vector_columns = find_vector_columns(schema, dim);

        if vector_columns.is_empty() {
            return Err(Error::InvalidQuery {
                message: format!("Unable to find a vector column with dimension {}", dim),
            });
        };
        if vector_columns.len() != 1 {
            return Err(Error::invalid_query(
                "Vector query can be applied to more than one vector columns, please specify the column to use"));
        }

        Ok(Self::with_column(query, &vector_columns[0]))
    }

    fn with_column(query: T, column: &str) -> Self {
        VectorQuery {
            query,
            column: column.to_string(),
            nprobs: 20,
            refine_factor: None,
            metric_type: None,
            use_index: true,
        }
    }
}

/// A builder for nearest neighbor queries for LanceDB.
pub struct Query {
    pub dataset: Arc<Dataset>,
    pub query_vector: Float32Array,
    pub limit: usize,
    pub filter: Option<String>,
    pub select: Option<Vec<String>>,
    pub nprobes: usize,
    pub refine_factor: Option<u32>,
    pub metric_type: Option<MetricType>,
    pub use_index: bool,
    vector_query: Option<VectorQuery<Float32Array>>,
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
    pub(crate) fn new(dataset: Arc<Dataset>, vector: Float32Array) -> Self {
        Query {
            dataset,
            vector_query: None,
            query_vector: vector,
            limit: 10,
            nprobes: 20,
            refine_factor: None,
            metric_type: None,
            use_index: true,
            filter: None,
            select: None,
        }
    }

    /// Execute the queries and return its results.
    ///
    /// # Returns
    ///
    /// * A [DatasetRecordBatchStream] with the query's results.
    pub async fn execute(&self) -> Result<DatasetRecordBatchStream> {
        let mut scanner: Scanner = self.dataset.scan();

        scanner.nearest(
            crate::table::VECTOR_COLUMN_NAME,
            &self.query_vector,
            self.limit,
        )?;
        scanner.nprobs(self.nprobes);
        scanner.use_index(self.use_index);
        self.select.as_ref().map(|p| scanner.project(p.as_slice()));
        self.filter.as_ref().map(|f| scanner.filter(f));
        self.refine_factor.map(|rf| scanner.refine(rf));
        self.metric_type.map(|mt| scanner.distance_metric(mt));
        Ok(scanner.try_into_stream().await?)
    }

    /// Set the maximum number of results to return.
    ///
    /// # Arguments
    ///
    /// * `limit` - The maximum number of results to return.
    pub fn limit(mut self, limit: usize) -> Query {
        self.limit = limit;
        self
    }

    /// Set the vector used for this query.
    ///
    /// # Arguments
    ///
    /// * `vector` - The vector that will be used for search.
    pub fn query_vector(mut self, query_vector: Float32Array) -> Query {
        self.query_vector = query_vector;
        self
    }

    pub fn vector_search(mut self, query: Float32Array) -> Result<Query> {
        let dim = query.len();

        self.query_vector = query;
        Ok(self)
    }

    /// Vector search on a given column.
    pub fn vector_search_on(mut self, query: Float32Array, column: &str) -> Result<Query> {
        if self.vector_query.is_some() {
            return Err(Error::invalid_query("Vector search is already set"));
        };

        let dim = query.len();

        self.query_vector = query;
        Ok(self)
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
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use arrow_array::{RecordBatch, RecordBatchIterator, RecordBatchReader};
    use arrow_schema::{DataType, Field as ArrowField, Schema as ArrowSchema};
    use lance::dataset::Dataset;
    use lance::index::vector::MetricType;

    use crate::query::Query;

    #[tokio::test]
    async fn test_setters_getters() {
        let batches = make_test_batches();
        let ds = Dataset::write(batches, "memory://foo", None).await.unwrap();

        let vector = Float32Array::from_iter_values([0.1, 0.2]);
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

        assert_eq!(query.query_vector, new_vector);
        assert_eq!(query.limit, 100);
        assert_eq!(query.nprobes, 1000);
        assert_eq!(query.use_index, true);
        assert_eq!(query.metric_type, Some(MetricType::Cosine));
        assert_eq!(query.refine_factor, Some(999));
    }

    #[tokio::test]
    async fn test_execute() {
        let batches = make_test_batches();
        let ds = Dataset::write(batches, "memory://foo", None).await.unwrap();

        let vector = Float32Array::from_iter_values([0.1; 128]);
        let query = Query::new(Arc::new(ds), vector.clone());
        let result = query.execute().await;
        assert_eq!(result.is_ok(), true);
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
