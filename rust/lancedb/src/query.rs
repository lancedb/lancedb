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

use std::future::Future;
use std::sync::Arc;

use arrow_array::{make_array, Array, Float16Array, Float32Array, Float64Array};
use arrow_schema::DataType;
use datafusion_physical_plan::ExecutionPlan;
use half::f16;
use lance::dataset::scanner::DatasetRecordBatchStream;
use lance_datafusion::exec::execute_plan;
use lance_index::scalar::FullTextSearchQuery;

use crate::arrow::SendableRecordBatchStream;
use crate::error::{Error, Result};
use crate::table::TableInternal;
use crate::DistanceType;

pub(crate) const DEFAULT_TOP_K: usize = 10;

/// Which columns should be retrieved from the database
#[derive(Debug, Clone)]
pub enum Select {
    /// Select all columns
    ///
    /// Warning: This will always be slower than selecting only the columns you need.
    All,
    /// Select the provided columns
    Columns(Vec<String>),
    /// Advanced selection which allows for dynamic column calculations
    ///
    /// The first item in each tuple is a name to assign to the output column.
    /// The second item in each tuple is an SQL expression to evaluate the result.
    ///
    /// See [`Query::select`] for more details and examples
    Dynamic(Vec<(String, String)>),
}

impl Select {
    /// Create a simple selection that only selects the given columns
    ///
    /// This method is a convenience method for creating a [`Select::Columns`] variant
    /// from either Vec<&str> or Vec<String>
    pub fn columns(columns: &[impl AsRef<str>]) -> Self {
        Self::Columns(columns.iter().map(|c| c.as_ref().to_string()).collect())
    }
    /// Create a dynamic selection that allows for advanced column selection
    ///
    /// This method is a convenience method for creating a [`Select::Dynamic`] variant
    /// from either &str or String tuples
    pub fn dynamic(columns: &[(impl AsRef<str>, impl AsRef<str>)]) -> Self {
        Self::Dynamic(
            columns
                .iter()
                .map(|(name, value)| (name.as_ref().to_string(), value.as_ref().to_string()))
                .collect(),
        )
    }
}

/// A trait for converting a type to a query vector
///
/// This is primarily intended to allow rust users that are unfamiliar with Arrow
/// a chance to use native types such as Vec<f32> instead of arrow arrays.  It also
/// serves as an integration point for other rust libraries such as polars.
///
/// By accepting the query vector as an array we are potentially allowing any data
/// type to be used as the query vector.  In the future, custom embedding models
/// may be installed.  These models may accept something other than f32.  For example,
/// sentence transformers typically expect the query to be a string.  This means that
/// any kind of conversion library should expect to convert more than just f32.
pub trait IntoQueryVector {
    /// Convert the user's query vector input to a query vector
    ///
    /// This trait exists to allow users to provide many different types as
    /// input to the [`crate::query::QueryBuilder::nearest_to`] method.
    ///
    /// By default, there is no embedding model registered, and the input should
    /// be the vector that the user wants to search with.  LanceDb expects a
    /// fixed-size-list of floats.  This means the input will need to be something
    /// that can be converted to a fixed-size-list of floats (e.g. a Vec<f32>)
    ///
    /// This crate provides a variety of default impls for common types.
    ///
    /// On the other hand, if an embedding model is registered, then the embedding
    /// model will determine the input type.  For example, sentence transformers expect
    /// the input to be strings.  The input should be converted to an array with
    /// a single string value.
    ///
    /// Trait impls should try and convert the source data to the requested data
    /// type if they can and fail with a meaningful error if they cannot.  An
    /// embedding model label is provided to help provide useful error messages.  For
    /// example, "failed to create query vector, the sentence transformer model
    /// expects strings but the input was a list of integers".
    ///
    /// Note that the output is an array but, in most cases, this will be an array of
    /// length one.  The query vector is considered a single "item" and arrays of
    /// length one are how arrow represents scalars.
    fn to_query_vector(
        self,
        data_type: &DataType,
        embedding_model_label: &str,
    ) -> Result<Arc<dyn Array>>;
}

// TODO: perhaps support some casts like f32->f64 and maybe even f64->f32?
impl IntoQueryVector for Arc<dyn Array> {
    fn to_query_vector(
        self,
        data_type: &DataType,
        _embedding_model_label: &str,
    ) -> Result<Arc<dyn Array>> {
        if data_type != self.data_type() {
            match data_type {
                // If the embedding wants floating point data we can try and cast
                DataType::Float16 | DataType::Float32 | DataType::Float64 => {
                    arrow_cast::cast(&self, data_type).map_err(|e| {
                        Error::InvalidInput {
                            message: format!(
                                "failed to create query vector, the input data type was {:?} but the expected data type was {:?}.  Attempt to cast yielded: {}",
                                self.data_type(),
                                data_type,
                                e
                            ),
                        }
                    })
                },
                // TODO: Should we try and cast even if the embedding wants non-numeric data?
                _ => Err(Error::InvalidInput {
                    message: format!(
                    "failed to create query vector, the input data type was {:?} but the expected data type was {:?}",
                    self.data_type(),
                    data_type
                )})
            }
        } else {
            Ok(self.clone())
        }
    }
}

impl IntoQueryVector for &dyn Array {
    fn to_query_vector(
        self,
        data_type: &DataType,
        _embedding_model_label: &str,
    ) -> Result<Arc<dyn Array>> {
        if data_type != self.data_type() {
            Err(Error::InvalidInput {
                message: format!(
                "failed to create query vector, the input data type was {:?} but the expected data type was {:?}",
                self.data_type(),
                data_type
            )})
        } else {
            let data = self.to_data();
            Ok(make_array(data))
        }
    }
}

impl IntoQueryVector for &[f16] {
    fn to_query_vector(
        self,
        data_type: &DataType,
        embedding_model_label: &str,
    ) -> Result<Arc<dyn Array>> {
        match data_type {
            DataType::Float16 => {
                let arr: Vec<f16> = self.to_vec();
                Ok(Arc::new(Float16Array::from(arr)))
            }
            DataType::Float32 => {
                let arr: Vec<f32> = self.iter().map(|x| f32::from(*x)).collect();
                Ok(Arc::new(Float32Array::from(arr)))
            },
            DataType::Float64 => {
                let arr: Vec<f64> = self.iter().map(|x| f64::from(*x)).collect();
                Ok(Arc::new(Float64Array::from(arr)))
            }
            _ => Err(Error::InvalidInput {
                message: format!(
                    "failed to create query vector, the input data type was &[f16] but the embedding model \"{}\" expected data type {:?}",
                    embedding_model_label,
                    data_type
                ),
            }),
        }
    }
}

impl IntoQueryVector for &[f32] {
    fn to_query_vector(
        self,
        data_type: &DataType,
        embedding_model_label: &str,
    ) -> Result<Arc<dyn Array>> {
        match data_type {
            DataType::Float16 => {
                let arr: Vec<f16> = self.iter().map(|x| f16::from_f32(*x)).collect();
                Ok(Arc::new(Float16Array::from(arr)))
            }
            DataType::Float32 => {
                let arr: Vec<f32> = self.to_vec();
                Ok(Arc::new(Float32Array::from(arr)))
            },
            DataType::Float64 => {
                let arr: Vec<f64> = self.iter().map(|x| *x as f64).collect();
                Ok(Arc::new(Float64Array::from(arr)))
            }
            _ => Err(Error::InvalidInput {
                message: format!(
                    "failed to create query vector, the input data type was &[f32] but the embedding model \"{}\" expected data type {:?}",
                    embedding_model_label,
                    data_type
                ),
            }),
        }
    }
}

impl IntoQueryVector for &[f64] {
    fn to_query_vector(
        self,
        data_type: &DataType,
        embedding_model_label: &str,
    ) -> Result<Arc<dyn Array>> {
        match data_type {
                DataType::Float16 => {
                    let arr: Vec<f16> = self.iter().map(|x| f16::from_f64(*x)).collect();
                    Ok(Arc::new(Float16Array::from(arr)))
                }
                DataType::Float32 => {
                    let arr: Vec<f32> = self.iter().map(|x| *x as f32).collect();
                    Ok(Arc::new(Float32Array::from(arr)))
                },
                DataType::Float64 => {
                    let arr: Vec<f64> = self.to_vec();
                    Ok(Arc::new(Float64Array::from(arr)))
                }
                _ => Err(Error::InvalidInput {
                    message: format!(
                        "failed to create query vector, the input data type was &[f64] but the embedding model \"{}\" expected data type {:?}",
                        embedding_model_label,
                        data_type
                    ),
                }),
            }
    }
}

impl<const N: usize> IntoQueryVector for &[f16; N] {
    fn to_query_vector(
        self,
        data_type: &DataType,
        embedding_model_label: &str,
    ) -> Result<Arc<dyn Array>> {
        self.as_slice()
            .to_query_vector(data_type, embedding_model_label)
    }
}

impl<const N: usize> IntoQueryVector for &[f32; N] {
    fn to_query_vector(
        self,
        data_type: &DataType,
        embedding_model_label: &str,
    ) -> Result<Arc<dyn Array>> {
        self.as_slice()
            .to_query_vector(data_type, embedding_model_label)
    }
}

impl<const N: usize> IntoQueryVector for &[f64; N] {
    fn to_query_vector(
        self,
        data_type: &DataType,
        embedding_model_label: &str,
    ) -> Result<Arc<dyn Array>> {
        self.as_slice()
            .to_query_vector(data_type, embedding_model_label)
    }
}

impl IntoQueryVector for Vec<f16> {
    fn to_query_vector(
        self,
        data_type: &DataType,
        embedding_model_label: &str,
    ) -> Result<Arc<dyn Array>> {
        self.as_slice()
            .to_query_vector(data_type, embedding_model_label)
    }
}

impl IntoQueryVector for Vec<f32> {
    fn to_query_vector(
        self,
        data_type: &DataType,
        embedding_model_label: &str,
    ) -> Result<Arc<dyn Array>> {
        self.as_slice()
            .to_query_vector(data_type, embedding_model_label)
    }
}

impl IntoQueryVector for Vec<f64> {
    fn to_query_vector(
        self,
        data_type: &DataType,
        embedding_model_label: &str,
    ) -> Result<Arc<dyn Array>> {
        self.as_slice()
            .to_query_vector(data_type, embedding_model_label)
    }
}

/// Common parameters that can be applied to scans and vector queries
pub trait QueryBase {
    /// Set the maximum number of results to return.
    ///
    /// By default, a plain search has no limit.  If this method is not
    /// called then every valid row from the table will be returned.
    ///
    /// A vector search always has a limit.  If this is not called then
    /// it will default to 10.
    fn limit(self, limit: usize) -> Self;

    /// Only return rows which match the filter.
    ///
    /// The filter should be supplied as an SQL query string.  For example:
    ///
    /// ```ignore
    /// x > 10
    /// y > 0 AND y < 100
    /// x > 5 OR y = 'test'
    /// ```
    ///
    /// Filtering performance can often be improved by creating a scalar index
    /// on the filter column(s).
    fn only_if(self, filter: impl AsRef<str>) -> Self;

    /// Perform a full text search on the table.
    ///
    /// The results will be returned in order of BM25 scores.
    ///
    /// This method is only valid on tables that have a full text search index.
    ///
    /// ```ignore
    /// query.full_text_search(FullTextSearchQuery::new("hello world"))
    /// ```
    fn full_text_search(self, query: FullTextSearchQuery) -> Self;

    /// Return only the specified columns.
    ///
    /// By default a query will return all columns from the table.  However, this can have
    /// a very significant impact on latency.  LanceDb stores data in a columnar fashion.  This
    /// means we can finely tune our I/O to select exactly the columns we need.
    ///
    /// As a best practice you should always limit queries to the columns that you need.
    ///
    /// You can also use this method to create new "dynamic" columns based on your existing columns.
    /// For example, you may not care about "a" or "b" but instead simply want "a + b".  This is often
    /// seen in the SELECT clause of an SQL query (e.g. `SELECT a+b FROM my_table`).
    ///
    /// To create dynamic columns use [`Select::Dynamic`] (it might be easier to create this with the
    /// helper method [`Select::dynamic`]).  A column will be returned for each tuple provided.  The
    /// first value in that tuple provides the name of the column.  The second value in the tuple is
    /// an SQL string used to specify how the column is calculated.
    ///
    /// For example, an SQL query might state `SELECT a + b AS combined, c`.  The equivalent
    /// input to [`Select::dynamic`] would be `&[("combined", "a + b"), ("c", "c")]`.
    ///
    /// Columns will always be returned in the order given, even if that order is different than
    /// the order used when adding the data.
    fn select(self, selection: Select) -> Self;

    /// Only execute the query over indexed data.
    ///
    /// This allows weak-consistent fast path for queries that only need to access the indexed data.
    ///
    /// Users can use [`crate::Table::optimize`] to merge new data into the index, and make the
    /// new data available for fast search.
    ///
    /// By default, it is false.
    fn fast_search(self) -> Self;
}

pub trait HasQuery {
    fn mut_query(&mut self) -> &mut Query;
}

impl<T: HasQuery> QueryBase for T {
    fn limit(mut self, limit: usize) -> Self {
        self.mut_query().limit = Some(limit);
        self
    }

    fn only_if(mut self, filter: impl AsRef<str>) -> Self {
        self.mut_query().filter = Some(filter.as_ref().to_string());
        self
    }

    fn full_text_search(mut self, query: FullTextSearchQuery) -> Self {
        self.mut_query().full_text_search = Some(query);
        self
    }

    fn select(mut self, select: Select) -> Self {
        self.mut_query().select = select;
        self
    }

    fn fast_search(mut self) -> Self {
        self.mut_query().fast_search = true;
        self
    }
}

/// Options for controlling the execution of a query
#[non_exhaustive]
pub struct QueryExecutionOptions {
    /// The maximum number of rows that will be contained in a single
    /// `RecordBatch` delivered by the query.
    ///
    /// Note: This is a maximum only.  The query may return smaller
    /// batches, even in the middle of a query, to avoid forcing
    /// memory copies due to concatenation.
    ///
    /// Note: Slicing an Arrow RecordBatch is a zero-copy operation
    /// and so the performance penalty of reading smaller batches
    /// is typically very small.
    ///
    /// By default, this is 1024
    pub max_batch_length: u32,
}

impl Default for QueryExecutionOptions {
    fn default() -> Self {
        Self {
            max_batch_length: 1024,
        }
    }
}

/// A trait for a query object that can be executed to get results
///
/// There are various kinds of queries but they all return results
/// in the same way.
pub trait ExecutableQuery {
    /// Return the Datafusion [ExecutionPlan].
    ///
    /// The caller can further optimize the plan or execute it.
    ///
    fn create_plan(
        &self,
        options: QueryExecutionOptions,
    ) -> impl Future<Output = Result<Arc<dyn ExecutionPlan>>> + Send;

    /// Execute the query with default options and return results
    ///
    /// See [`ExecutableQuery::execute_with_options`] for more details.
    fn execute(&self) -> impl Future<Output = Result<SendableRecordBatchStream>> + Send {
        self.execute_with_options(QueryExecutionOptions::default())
    }

    /// Execute the query and return results
    ///
    /// The query results are returned as a [`SendableRecordBatchStream`].  This is
    /// an Stream of Arrow [`arrow_array::RecordBatch`] (and you can also independently
    /// access the [`arrow_schema::Schema`] without polling the stream).
    ///
    /// Note: The size of the returned batches and the order of individual rows is
    /// not deterministic.
    ///
    /// LanceDb will use many threads to calculate results and, when
    /// the result set is large, multiple batches will be processed at one time.
    /// This readahead is limited however and backpressure will be applied if this
    /// stream is consumed slowly (this constrains the maximum memory used by a
    /// single query.
    ///
    /// For simpler access or row-based access we recommend creating extension traits
    /// to convert Arrow data into your internal data model.
    fn execute_with_options(
        &self,
        options: QueryExecutionOptions,
    ) -> impl Future<Output = Result<SendableRecordBatchStream>> + Send;

    fn explain_plan(&self, verbose: bool) -> impl Future<Output = Result<String>> + Send;
}

/// A builder for LanceDB queries.
///
/// See [`crate::Table::query`] for more details on queries
///
/// See [`QueryBase`] for methods that can be used to parameterize
/// the query.
///
/// See [`ExecutableQuery`] for methods that can be used to execute
/// the query and retrieve results.
///
/// This query object can be reused to issue the same query multiple
/// times.
#[derive(Debug, Clone)]
pub struct Query {
    parent: Arc<dyn TableInternal>,

    /// limit the number of rows to return.
    pub(crate) limit: Option<usize>,

    /// Apply filter to the returned rows.
    pub(crate) filter: Option<String>,

    /// Perform a full text search on the table.
    pub(crate) full_text_search: Option<FullTextSearchQuery>,

    /// Select column projection.
    pub(crate) select: Select,

    /// If set to true, the query is executed only on the indexed data,
    /// and yields faster results.
    ///
    /// By default, this is false.
    pub(crate) fast_search: bool,
}

impl Query {
    pub(crate) fn new(parent: Arc<dyn TableInternal>) -> Self {
        Self {
            parent,
            limit: None,
            filter: None,
            full_text_search: None,
            select: Select::All,
            fast_search: false,
        }
    }

    /// Helper method to convert the query to a VectorQuery with a `query_vector`
    /// of None.  This retrofits to some existing inner paths that work with a
    /// single query object for both vector and plain queries.
    pub(crate) fn into_vector(self) -> VectorQuery {
        VectorQuery::new(self)
    }

    /// Find the nearest vectors to the given query vector.
    ///
    /// This converts the query from a plain query to a vector query.
    ///
    /// This method will attempt to convert the input to the query vector
    /// expected by the embedding model.  If the input cannot be converted
    /// then an error will be returned.
    ///
    /// By default, there is no embedding model, and the input should be
    /// vector/slice of floats.
    ///
    /// If there is only one vector column (a column whose data type is a
    /// fixed size list of floats) then the column does not need to be specified.
    /// If there is more than one vector column you must use [`Query::column`]
    /// to specify which column you would like to compare with.
    ///
    /// If no index has been created on the vector column then a vector query
    /// will perform a distance comparison between the query vector and every
    /// vector in the database and then sort the results.  This is sometimes
    /// called a "flat search"
    ///
    /// For small databases, with a few hundred thousand vectors or less, this can
    /// be reasonably fast.  In larger databases you should create a vector index
    /// on the column.  If there is a vector index then an "approximate" nearest
    /// neighbor search (frequently called an ANN search) will be performed.  This
    /// search is much faster, but the results will be approximate.
    ///
    /// The query can be further parameterized using the returned builder.  There
    /// are various search parameters that will let you fine tune your recall
    /// accuracy vs search latency.
    ///
    /// # Arguments
    ///
    /// * `vector` - The vector that will be used for search.
    pub fn nearest_to(self, vector: impl IntoQueryVector) -> Result<VectorQuery> {
        let mut vector_query = self.into_vector();
        let query_vector = vector.to_query_vector(&DataType::Float32, "default")?;
        vector_query.query_vector = Some(query_vector);
        Ok(vector_query)
    }
}

impl HasQuery for Query {
    fn mut_query(&mut self) -> &mut Query {
        self
    }
}

impl ExecutableQuery for Query {
    async fn create_plan(&self, options: QueryExecutionOptions) -> Result<Arc<dyn ExecutionPlan>> {
        self.parent
            .clone()
            .create_plan(&self.clone().into_vector(), options)
            .await
    }

    async fn execute_with_options(
        &self,
        options: QueryExecutionOptions,
    ) -> Result<SendableRecordBatchStream> {
        Ok(SendableRecordBatchStream::from(
            self.parent.clone().plain_query(self, options).await?,
        ))
    }

    async fn explain_plan(&self, verbose: bool) -> Result<String> {
        self.parent
            .explain_plan(&self.clone().into_vector(), verbose)
            .await
    }
}

/// A builder for vector searches
///
/// This builder contains methods specific to vector searches.
///
/// /// See [`QueryBase`] for additional methods that can be used to
/// parameterize the query.
///
/// See [`ExecutableQuery`] for methods that can be used to execute
/// the query and retrieve results.
#[derive(Debug, Clone)]
pub struct VectorQuery {
    pub(crate) base: Query,
    // The column to run the query on. If not specified, we will attempt to guess
    // the column based on the dataset's schema.
    pub(crate) column: Option<String>,
    // IVF PQ - ANN search.
    pub(crate) query_vector: Option<Arc<dyn Array>>,
    pub(crate) nprobes: usize,
    pub(crate) refine_factor: Option<u32>,
    pub(crate) distance_type: Option<DistanceType>,
    /// Default is true. Set to false to enforce a brute force search.
    pub(crate) use_index: bool,
    /// Apply filter before ANN search/
    pub(crate) prefilter: bool,
}

impl VectorQuery {
    fn new(base: Query) -> Self {
        Self {
            base,
            column: None,
            query_vector: None,
            nprobes: 20,
            refine_factor: None,
            distance_type: None,
            use_index: true,
            prefilter: true,
        }
    }

    /// Set the vector column to query
    ///
    /// This controls which column is compared to the query vector supplied in
    /// the call to [`Query::nearest_to`]
    ///
    /// This parameter must be specified if the table has more than one column
    /// whose data type is a fixed-size-list of floats.
    pub fn column(mut self, column: &str) -> Self {
        self.column = Some(column.to_string());
        self
    }

    /// Set the number of partitions to search (probe)
    ///
    /// This argument is only used when the vector column has an IVF PQ index.
    /// If there is no index then this value is ignored.
    ///
    /// The IVF stage of IVF PQ divides the input into partitions (clusters) of
    /// related values.
    ///
    /// The partition whose centroids are closest to the query vector will be
    /// exhaustiely searched to find matches.  This parameter controls how many
    /// partitions should be searched.
    ///
    /// Increasing this value will increase the recall of your query but will
    /// also increase the latency of your query.  The default value is 20.  This
    /// default is good for many cases but the best value to use will depend on
    /// your data and the recall that you need to achieve.
    ///
    /// For best results we recommend tuning this parameter with a benchmark against
    /// your actual data to find the smallest possible value that will still give
    /// you the desired recall.
    pub fn nprobes(mut self, nprobes: usize) -> Self {
        self.nprobes = nprobes;
        self
    }

    /// A multiplier to control how many additional rows are taken during the refine step
    ///
    /// This argument is only used when the vector column has an IVF PQ index.
    /// If there is no index then this value is ignored.
    ///
    /// An IVF PQ index stores compressed (quantized) values.  They query vector is compared
    /// against these values and, since they are compressed, the comparison is inaccurate.
    ///
    /// This parameter can be used to refine the results.  It can improve both improve recall
    /// and correct the ordering of the nearest results.
    ///
    /// To refine results LanceDb will first perform an ANN search to find the nearest
    /// `limit` * `refine_factor` results.  In other words, if `refine_factor` is 3 and
    /// `limit` is the default (10) then the first 30 results will be selected.  LanceDb
    /// then fetches the full, uncompressed, values for these 30 results.  The results are
    /// then reordered by the true distance and only the nearest 10 are kept.
    ///
    /// Note: there is a difference between calling this method with a value of 1 and never
    /// calling this method at all.  Calling this method with any value will have an impact
    /// on your search latency.  When you call this method with a `refine_factor` of 1 then
    /// LanceDb still needs to fetch the full, uncompressed, values so that it can potentially
    /// reorder the results.
    ///
    /// Note: if this method is NOT called then the distances returned in the _distance column
    /// will be approximate distances based on the comparison of the quantized query vector
    /// and the quantized result vectors.  This can be considerably different than the true
    /// distance between the query vector and the actual uncompressed vector.
    pub fn refine_factor(mut self, refine_factor: u32) -> Self {
        self.refine_factor = Some(refine_factor);
        self
    }

    /// Set the distance metric to use
    ///
    /// When performing a vector search we try and find the "nearest" vectors according
    /// to some kind of distance metric.  This parameter controls which distance metric to
    /// use.  See [`DistanceType`] for more details on the different distance metrics
    /// available.
    ///
    /// Note: if there is a vector index then the distance type used MUST match the distance
    /// type used to train the vector index.  If this is not done then the results will be
    /// invalid.
    ///
    /// By default [`DistanceType::L2`] is used.
    pub fn distance_type(mut self, distance_type: DistanceType) -> Self {
        self.distance_type = Some(distance_type);
        self
    }

    /// If this is called then filtering will happen after the vector search instead of
    /// before.
    ///
    /// By default filtering will be performed before the vector search.  This is how
    /// filtering is typically understood to work.  This prefilter step does add some
    /// additional latency.  Creating a scalar index on the filter column(s) can
    /// often improve this latency.  However, sometimes a filter is too complex or scalar
    /// indices cannot be applied to the column.  In these cases postfiltering can be
    /// used instead of prefiltering to improve latency.
    ///
    /// Post filtering applies the filter to the results of the vector search.  This means
    /// we only run the filter on a much smaller set of data.  However, it can cause the
    /// query to return fewer than `limit` results (or even no results) if none of the nearest
    /// results match the filter.
    ///
    /// Post filtering happens during the "refine stage" (described in more detail in
    /// [`Self::refine_factor`]).  This means that setting a higher refine factor can often
    /// help restore some of the results lost by post filtering.
    pub fn postfilter(mut self) -> Self {
        self.prefilter = false;
        self
    }

    /// If this is called then any vector index is skipped
    ///
    /// An exhaustive (flat) search will be performed.  The query vector will
    /// be compared to every vector in the table.  At high scales this can be
    /// expensive.  However, this is often still useful.  For example, skipping
    /// the vector index can give you ground truth results which you can use to
    /// calculate your recall to select an appropriate value for nprobes.
    pub fn bypass_vector_index(mut self) -> Self {
        self.use_index = false;
        self
    }
}

impl ExecutableQuery for VectorQuery {
    async fn create_plan(&self, options: QueryExecutionOptions) -> Result<Arc<dyn ExecutionPlan>> {
        self.base.parent.clone().create_plan(self, options).await
    }

    async fn execute_with_options(
        &self,
        options: QueryExecutionOptions,
    ) -> Result<SendableRecordBatchStream> {
        Ok(SendableRecordBatchStream::from(
            DatasetRecordBatchStream::new(execute_plan(
                self.create_plan(options).await?,
                Default::default(),
            )?),
        ))
    }

    async fn explain_plan(&self, verbose: bool) -> Result<String> {
        self.base.parent.explain_plan(self, verbose).await
    }
}

impl HasQuery for VectorQuery {
    fn mut_query(&mut self) -> &mut Query {
        &mut self.base
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
    use futures::{StreamExt, TryStreamExt};
    use lance_testing::datagen::{BatchGenerator, IncrementingInt32, RandomVector};
    use tempfile::tempdir;

    use crate::{connect, Table};

    #[tokio::test]
    async fn test_setters_getters() {
        // TODO: Switch back to memory://foo after https://github.com/lancedb/lancedb/issues/1051
        // is fixed
        let tmp_dir = tempdir().unwrap();
        let dataset_path = tmp_dir.path().join("test.lance");
        let uri = dataset_path.to_str().unwrap();

        let batches = make_test_batches();
        let conn = connect(uri).execute().await.unwrap();
        let table = conn
            .create_table("my_table", Box::new(batches))
            .execute()
            .await
            .unwrap();

        let vector = Float32Array::from_iter_values([0.1, 0.2]);
        let query = table.query().nearest_to(&[0.1, 0.2]).unwrap();
        assert_eq!(*query.query_vector.unwrap().as_ref().as_primitive(), vector);

        let new_vector = Float32Array::from_iter_values([9.8, 8.7]);

        let query = table
            .query()
            .limit(100)
            .nearest_to(&[9.8, 8.7])
            .unwrap()
            .nprobes(1000)
            .postfilter()
            .distance_type(DistanceType::Cosine)
            .refine_factor(999);

        assert_eq!(
            *query.query_vector.unwrap().as_ref().as_primitive(),
            new_vector
        );
        assert_eq!(query.base.limit.unwrap(), 100);
        assert_eq!(query.nprobes, 1000);
        assert!(query.use_index);
        assert_eq!(query.distance_type, Some(DistanceType::Cosine));
        assert_eq!(query.refine_factor, Some(999));
    }

    #[tokio::test]
    async fn test_execute() {
        // TODO: Switch back to memory://foo after https://github.com/lancedb/lancedb/issues/1051
        // is fixed
        let tmp_dir = tempdir().unwrap();
        let dataset_path = tmp_dir.path().join("test.lance");
        let uri = dataset_path.to_str().unwrap();

        let batches = make_non_empty_batches();
        let conn = connect(uri).execute().await.unwrap();
        let table = conn
            .create_table("my_table", Box::new(batches))
            .execute()
            .await
            .unwrap();

        let query = table
            .query()
            .limit(10)
            .only_if("id % 2 == 0")
            .nearest_to(&[0.1; 4])
            .unwrap()
            .postfilter();
        let result = query.execute().await;
        let mut stream = result.expect("should have result");
        // should only have one batch
        while let Some(batch) = stream.next().await {
            // post filter should have removed some rows
            assert!(batch.expect("should be Ok").num_rows() < 10);
        }

        let query = table
            .query()
            .limit(10)
            .only_if(String::from("id % 2 == 0"))
            .nearest_to(&[0.1; 4])
            .unwrap();
        let result = query.execute().await;
        let mut stream = result.expect("should have result");
        // should only have one batch
        while let Some(batch) = stream.next().await {
            // pre filter should return 10 rows
            assert!(batch.expect("should be Ok").num_rows() == 10);
        }
    }

    #[tokio::test]
    async fn test_select_with_transform() {
        // TODO: Switch back to memory://foo after https://github.com/lancedb/lancedb/issues/1051
        // is fixed
        let tmp_dir = tempdir().unwrap();
        let dataset_path = tmp_dir.path().join("test.lance");
        let uri = dataset_path.to_str().unwrap();

        let batches = make_non_empty_batches();
        let conn = connect(uri).execute().await.unwrap();
        let table = conn
            .create_table("my_table", Box::new(batches))
            .execute()
            .await
            .unwrap();

        let query = table
            .query()
            .limit(10)
            .select(Select::dynamic(&[("id2", "id * 2"), ("id", "id")]));
        let result = query.execute().await;
        let mut batches = result
            .expect("should have result")
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(batches.len(), 1);
        let batch = batches.pop().unwrap();

        // id, and id2
        assert_eq!(batch.num_columns(), 2);

        let id: &Int32Array = batch.column_by_name("id").unwrap().as_primitive();
        let id2: &Int32Array = batch.column_by_name("id2").unwrap().as_primitive();

        id.iter().zip(id2.iter()).for_each(|(id, id2)| {
            let id = id.unwrap();
            let id2 = id2.unwrap();
            assert_eq!(id * 2, id2);
        });
    }

    #[tokio::test]
    async fn test_execute_no_vector() {
        // TODO: Switch back to memory://foo after https://github.com/lancedb/lancedb/issues/1051
        // is fixed
        let tmp_dir = tempdir().unwrap();
        let dataset_path = tmp_dir.path().join("test.lance");
        let uri = dataset_path.to_str().unwrap();

        // test that it's ok to not specify a query vector (just filter / limit)
        let batches = make_non_empty_batches();
        let conn = connect(uri).execute().await.unwrap();
        let table = conn
            .create_table("my_table", Box::new(batches))
            .execute()
            .await
            .unwrap();

        let query = table.query();
        let result = query.only_if("id % 2 == 0").execute().await;
        let mut stream = result.expect("should have result");
        // should only have one batch
        while let Some(batch) = stream.next().await {
            let b = batch.expect("should be Ok");
            // cast arr into Int32Array
            let arr: &Int32Array = b["id"].as_primitive();
            assert!(arr.iter().all(|x| x.unwrap() % 2 == 0));
        }

        // Reject bad filter
        let result = table.query().only_if("id = 0 AND").execute().await;
        assert!(result.is_err());
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

    async fn make_test_table(tmp_dir: &tempfile::TempDir) -> Table {
        let dataset_path = tmp_dir.path().join("test.lance");
        let uri = dataset_path.to_str().unwrap();

        let batches = make_non_empty_batches();
        let conn = connect(uri).execute().await.unwrap();
        conn.create_table("my_table", Box::new(batches))
            .execute()
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn test_execute_with_options() {
        let tmp_dir = tempdir().unwrap();
        let table = make_test_table(&tmp_dir).await;

        let mut results = table
            .query()
            .execute_with_options(QueryExecutionOptions {
                max_batch_length: 10,
                ..Default::default()
            })
            .await
            .unwrap();

        while let Some(batch) = results.next().await {
            assert!(batch.unwrap().num_rows() <= 10);
        }
    }

    fn assert_plan_exists(plan: &Arc<dyn ExecutionPlan>, name: &str) -> bool {
        if plan.name() == name {
            return true;
        }
        plan.children()
            .iter()
            .any(|child| assert_plan_exists(child, name))
    }

    #[tokio::test]
    async fn test_create_execute_plan() {
        let tmp_dir = tempdir().unwrap();
        let table = make_test_table(&tmp_dir).await;
        let plan = table
            .query()
            .nearest_to(vec![0.1, 0.2, 0.3, 0.4])
            .unwrap()
            .create_plan(QueryExecutionOptions::default())
            .await
            .unwrap();
        assert_plan_exists(&plan, "KNNFlatSearch");
        assert_plan_exists(&plan, "ProjectionExec");
    }

    #[tokio::test]
    async fn query_base_methods_on_vector_query() {
        // Make sure VectorQuery can be used as a QueryBase
        let tmp_dir = tempdir().unwrap();
        let table = make_test_table(&tmp_dir).await;

        let mut results = table
            .vector_search(&[1.0, 2.0, 3.0, 4.0])
            .unwrap()
            .limit(1)
            .execute()
            .await
            .unwrap();

        let first_batch = results.next().await.unwrap().unwrap();
        assert_eq!(first_batch.num_rows(), 1);
        assert!(results.next().await.is_none());

        // query with wrong vector dimension
        let error_result = table
            .vector_search(&[1.0, 2.0, 3.0])
            .unwrap()
            .limit(1)
            .execute()
            .await;
        assert!(error_result
            .err()
            .unwrap()
            .to_string()
            .contains("No vector column found to match with the query vector dimension: 3"));
    }

    #[tokio::test]
    async fn test_fast_search_plan() {
        let tmp_dir = tempdir().unwrap();
        let table = make_test_table(&tmp_dir).await;
        let plan = table
            .query()
            .select(Select::columns(&["_distance"]))
            .nearest_to(vec![0.1, 0.2, 0.3, 0.4])
            .unwrap()
            .fast_search()
            .explain_plan(true)
            .await
            .unwrap();
        assert!(!plan.contains("Take"));
    }
}
