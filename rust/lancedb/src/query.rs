// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::sync::Arc;
use std::{future::Future, time::Duration};

use arrow::compute::concat_batches;
use arrow_array::{make_array, Array, Float16Array, Float32Array, Float64Array};
use arrow_schema::DataType;
use datafusion_expr::Expr;
use datafusion_physical_plan::ExecutionPlan;
use futures::{stream, try_join, FutureExt, TryStreamExt};
use half::f16;
use lance::{
    arrow::RecordBatchExt,
    dataset::{scanner::DatasetRecordBatchStream, ROW_ID},
};
use lance_datafusion::exec::execute_plan;
use lance_index::scalar::inverted::SCORE_COL;
use lance_index::scalar::FullTextSearchQuery;
use lance_index::vector::DIST_COL;
use lance_io::stream::RecordBatchStreamAdapter;

use crate::error::{Error, Result};
use crate::rerankers::rrf::RRFReranker;
use crate::rerankers::{check_reranker_result, NormalizeMethod, Reranker};
use crate::table::BaseTable;
use crate::utils::TimeoutStream;
use crate::DistanceType;
use crate::{arrow::SendableRecordBatchStream, table::AnyQuery};

mod hybrid;

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

    /// Set the offset of the query.
    ///
    /// By default, it fetches starting with the first row.
    /// This method can be used to skip the first `offset` rows.
    fn offset(self, offset: usize) -> Self;

    /// Only return rows which match the filter.
    ///
    /// The filter should be supplied as an SQL query string.  For example:
    ///
    /// ```sql
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
    /// ```
    /// use lance_index::scalar::FullTextSearchQuery;
    /// use lancedb::query::{QueryBase, ExecutableQuery};
    ///
    /// # use lancedb::Table;
    /// # async fn query(table: &Table) -> Result<(), Box<dyn std::error::Error>> {
    /// let results = table.query()
    ///     .full_text_search(FullTextSearchQuery::new("hello world".into()))
    ///     .execute()
    ///     .await?;
    /// # Ok(())
    /// # }
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
    fn postfilter(self) -> Self;

    /// Return the `_rowid` meta column from the Table.
    fn with_row_id(self) -> Self;

    /// Rerank the results using the specified reranker.
    ///
    /// This is currently only supported for Hybrid Search.
    fn rerank(self, reranker: Arc<dyn Reranker>) -> Self;

    /// The method to normalize the scores. Can be "rank" or "Score". If "Rank",
    /// the scores are converted to ranks and then normalized. If "Score", the
    /// scores are normalized directly.
    fn norm(self, norm: NormalizeMethod) -> Self;
}

pub trait HasQuery {
    fn mut_query(&mut self) -> &mut QueryRequest;
}

impl<T: HasQuery> QueryBase for T {
    fn limit(mut self, limit: usize) -> Self {
        self.mut_query().limit = Some(limit);
        self
    }

    fn offset(mut self, offset: usize) -> Self {
        self.mut_query().offset = Some(offset);
        self
    }

    fn only_if(mut self, filter: impl AsRef<str>) -> Self {
        self.mut_query().filter = Some(QueryFilter::Sql(filter.as_ref().to_string()));
        self
    }

    fn full_text_search(mut self, query: FullTextSearchQuery) -> Self {
        if self.mut_query().limit.is_none() {
            self.mut_query().limit = Some(DEFAULT_TOP_K);
        }
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

    fn postfilter(mut self) -> Self {
        self.mut_query().prefilter = false;
        self
    }

    fn with_row_id(mut self) -> Self {
        self.mut_query().with_row_id = true;
        self
    }

    fn rerank(mut self, reranker: Arc<dyn Reranker>) -> Self {
        self.mut_query().reranker = Some(reranker);
        self
    }

    fn norm(mut self, norm: NormalizeMethod) -> Self {
        self.mut_query().norm = Some(norm);
        self
    }
}

/// Options for controlling the execution of a query
#[non_exhaustive]
#[derive(Debug, Clone)]
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
    /// Max duration to wait for the query to execute before timing out.
    pub timeout: Option<Duration>,
}

impl Default for QueryExecutionOptions {
    fn default() -> Self {
        Self {
            max_batch_length: 1024,
            timeout: None,
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

    fn analyze_plan(&self) -> impl Future<Output = Result<String>> + Send {
        self.analyze_plan_with_options(QueryExecutionOptions::default())
    }

    fn analyze_plan_with_options(
        &self,
        options: QueryExecutionOptions,
    ) -> impl Future<Output = Result<String>> + Send;
}

/// A query filter that can be applied to a query
#[derive(Clone, Debug)]
pub enum QueryFilter {
    /// The filter is an SQL string
    Sql(String),
    /// The filter is a Substrait ExtendedExpression message with a single expression
    Substrait(Arc<[u8]>),
    /// The filter is a Datafusion expression
    Datafusion(Expr),
}

/// A basic query into a table without any kind of search
///
/// This will result in a (potentially filtered) scan if executed
#[derive(Debug, Clone)]
pub struct QueryRequest {
    /// limit the number of rows to return.
    pub limit: Option<usize>,

    /// Offset of the query.
    pub offset: Option<usize>,

    /// Apply filter to the returned rows.
    pub filter: Option<QueryFilter>,

    /// Perform a full text search on the table.
    pub full_text_search: Option<FullTextSearchQuery>,

    /// Select column projection.
    pub select: Select,

    /// If set to true, the query is executed only on the indexed data,
    /// and yields faster results.
    ///
    /// By default, this is false.
    pub fast_search: bool,

    /// If set to true, the query will return the `_rowid` meta column.
    ///
    /// By default, this is false.
    pub with_row_id: bool,

    /// If set to false, the filter will be applied after the vector search.
    pub prefilter: bool,

    /// Implementation of reranker that can be used to reorder or combine query
    /// results, especially if using hybrid search
    pub reranker: Option<Arc<dyn Reranker>>,

    /// Configure how query results are normalized when doing hybrid search
    pub norm: Option<NormalizeMethod>,
}

impl Default for QueryRequest {
    fn default() -> Self {
        Self {
            limit: None,
            offset: None,
            filter: None,
            full_text_search: None,
            select: Select::All,
            fast_search: false,
            with_row_id: false,
            prefilter: true,
            reranker: None,
            norm: None,
        }
    }
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
    parent: Arc<dyn BaseTable>,
    request: QueryRequest,
}

impl Query {
    pub(crate) fn new(parent: Arc<dyn BaseTable>) -> Self {
        Self {
            parent,
            request: QueryRequest::default(),
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
        vector_query.request.query_vector.push(query_vector);

        if vector_query.request.base.limit.is_none() {
            vector_query.request.base.limit = Some(DEFAULT_TOP_K);
        }

        Ok(vector_query)
    }

    pub fn into_request(self) -> QueryRequest {
        self.request
    }

    pub fn current_request(&self) -> &QueryRequest {
        &self.request
    }
}

impl HasQuery for Query {
    fn mut_query(&mut self) -> &mut QueryRequest {
        &mut self.request
    }
}

impl ExecutableQuery for Query {
    async fn create_plan(&self, options: QueryExecutionOptions) -> Result<Arc<dyn ExecutionPlan>> {
        let req = AnyQuery::Query(self.request.clone());
        self.parent.clone().create_plan(&req, options).await
    }

    async fn execute_with_options(
        &self,
        options: QueryExecutionOptions,
    ) -> Result<SendableRecordBatchStream> {
        let query = AnyQuery::Query(self.request.clone());
        Ok(SendableRecordBatchStream::from(
            self.parent.clone().query(&query, options).await?,
        ))
    }

    async fn explain_plan(&self, verbose: bool) -> Result<String> {
        let query = AnyQuery::Query(self.request.clone());
        self.parent.explain_plan(&query, verbose).await
    }

    async fn analyze_plan_with_options(&self, options: QueryExecutionOptions) -> Result<String> {
        let query = AnyQuery::Query(self.request.clone());
        self.parent.analyze_plan(&query, options).await
    }
}

/// A request for a nearest-neighbors search into a table
#[derive(Debug, Clone)]
pub struct VectorQueryRequest {
    /// The base query
    pub base: QueryRequest,
    /// The column to run the search on
    ///
    /// If None, then the table will need to auto-detect which column to use
    pub column: Option<String>,
    /// The vector(s) to search for
    pub query_vector: Vec<Arc<dyn Array>>,
    /// The minimum number of partitions to search
    pub minimum_nprobes: usize,
    /// The maximum number of partitions to search
    pub maximum_nprobes: Option<usize>,
    /// The lower bound (inclusive) of the distance to search for.
    pub lower_bound: Option<f32>,
    /// The upper bound (exclusive) of the distance to search for.
    pub upper_bound: Option<f32>,
    /// The number of candidates to return during the refine step for HNSW,
    /// defaults to 1.5 * limit.
    pub ef: Option<usize>,
    /// A multiplier to control how many additional rows are taken during the refine step
    pub refine_factor: Option<u32>,
    /// The distance type to use for the search
    pub distance_type: Option<DistanceType>,
    /// Default is true. Set to false to enforce a brute force search.
    pub use_index: bool,
}

impl Default for VectorQueryRequest {
    fn default() -> Self {
        Self {
            base: QueryRequest::default(),
            column: None,
            query_vector: Vec::new(),
            minimum_nprobes: 20,
            maximum_nprobes: Some(20),
            lower_bound: None,
            upper_bound: None,
            ef: None,
            refine_factor: None,
            distance_type: None,
            use_index: true,
        }
    }
}

impl VectorQueryRequest {
    pub fn from_plain_query(query: QueryRequest) -> Self {
        Self {
            base: query,
            ..Default::default()
        }
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
    parent: Arc<dyn BaseTable>,
    request: VectorQueryRequest,
}

impl VectorQuery {
    fn new(base: Query) -> Self {
        Self {
            parent: base.parent,
            request: VectorQueryRequest::from_plain_query(base.request),
        }
    }

    pub fn into_request(self) -> VectorQueryRequest {
        self.request
    }

    pub fn current_request(&self) -> &VectorQueryRequest {
        &self.request
    }

    pub fn into_plain(self) -> Query {
        Query {
            parent: self.parent,
            request: self.request.base,
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
        self.request.column = Some(column.to_string());
        self
    }

    /// Add another query vector to the search.
    ///
    /// Multiple searches will be dispatched as part of the query.
    /// This is a convenience method for adding multiple query vectors
    /// to the search. It is not expected to be faster than issuing
    /// multiple queries concurrently.
    ///
    /// The output data will contain an additional columns `query_index` which
    /// will contain the index of the query vector that was used to generate the
    /// result.
    pub fn add_query_vector(mut self, vector: impl IntoQueryVector) -> Result<Self> {
        let query_vector = vector.to_query_vector(&DataType::Float32, "default")?;
        self.request.query_vector.push(query_vector);
        Ok(self)
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
    ///
    /// This method sets both the minimum and maximum number of partitions to search.
    /// For more fine-grained control see [`VectorQuery::minimum_nprobes`] and
    /// [`VectorQuery::maximum_nprobes`].
    pub fn nprobes(mut self, nprobes: usize) -> Self {
        self.request.minimum_nprobes = nprobes;
        self.request.maximum_nprobes = Some(nprobes);
        self
    }

    /// Set the minimum number of partitions to search
    ///
    /// This argument is only used when the vector column has an IVF PQ index.
    /// If there is no index then this value is ignored.
    ///
    /// See [`VectorQuery::nprobes`] for more details.
    ///
    /// These partitions will be searched on every indexed vector query.
    ///
    /// Will return an error if the value is not greater than 0 or if maximum_nprobes
    /// has been set and is less than the minimum_nprobes.
    pub fn minimum_nprobes(mut self, minimum_nprobes: usize) -> Result<Self> {
        if minimum_nprobes == 0 {
            return Err(Error::InvalidInput {
                message: "minimum_nprobes must be greater than 0".to_string(),
            });
        }
        if let Some(maximum_nprobes) = self.request.maximum_nprobes {
            if minimum_nprobes > maximum_nprobes {
                return Err(Error::InvalidInput {
                    message: "minimum_nprobes must be less than or equal to maximum_nprobes"
                        .to_string(),
                });
            }
        }
        self.request.minimum_nprobes = minimum_nprobes;
        Ok(self)
    }

    /// Set the maximum number of partitions to search
    ///
    /// This argument is only used when the vector column has an IVF PQ index.
    /// If there is no index then this value is ignored.
    ///
    /// See [`VectorQuery::nprobes`] for more details.
    ///
    /// If this value is greater than minimum_nprobes then the excess partitions will
    /// only be searched if the initial search does not return enough results.
    ///
    /// This can be useful when there is a narrow filter to allow these queries to
    /// spend more time searching and avoid potential false negatives.
    ///
    /// Set to None to search all partitions, if needed, to satsify the limit
    pub fn maximum_nprobes(mut self, maximum_nprobes: Option<usize>) -> Result<Self> {
        if let Some(maximum_nprobes) = maximum_nprobes {
            if maximum_nprobes == 0 {
                return Err(Error::InvalidInput {
                    message: "maximum_nprobes must be greater than 0".to_string(),
                });
            }
            if maximum_nprobes < self.request.minimum_nprobes {
                return Err(Error::InvalidInput {
                    message: "maximum_nprobes must be greater than or equal to minimum_nprobes"
                        .to_string(),
                });
            }
        }
        self.request.maximum_nprobes = maximum_nprobes;
        Ok(self)
    }

    /// Set the distance range for vector search,
    /// only rows with distances in the range [lower_bound, upper_bound) will be returned
    pub fn distance_range(mut self, lower_bound: Option<f32>, upper_bound: Option<f32>) -> Self {
        self.request.lower_bound = lower_bound;
        self.request.upper_bound = upper_bound;
        self
    }

    /// Set the number of candidates to return during the refine step for HNSW
    ///
    /// This argument is only used when the vector column has an HNSW index.
    /// If there is no index then this value is ignored.
    ///
    /// Increasing this value will increase the recall of your query but will
    /// also increase the latency of your query.  The default value is 1.5*limit.
    pub fn ef(mut self, ef: usize) -> Self {
        self.request.ef = Some(ef);
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
        self.request.refine_factor = Some(refine_factor);
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
        self.request.distance_type = Some(distance_type);
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
        self.request.use_index = false;
        self
    }

    pub async fn execute_hybrid(
        &self,
        options: QueryExecutionOptions,
    ) -> Result<SendableRecordBatchStream> {
        // clone query and specify we want to include row IDs, which can be needed for reranking
        let mut fts_query = Query::new(self.parent.clone());
        fts_query.request = self.request.base.clone();
        fts_query = fts_query.with_row_id();

        let mut vector_query = self.clone().with_row_id();

        vector_query.request.base.full_text_search = None;
        let (fts_results, vec_results) = try_join!(
            fts_query.execute_with_options(options.clone()),
            vector_query.inner_execute_with_options(options)
        )?;

        let (fts_results, vec_results) = try_join!(
            fts_results.try_collect::<Vec<_>>(),
            vec_results.try_collect::<Vec<_>>()
        )?;

        // try to get the schema to use when combining batches.
        // if either
        let (fts_schema, vec_schema) = hybrid::query_schemas(&fts_results, &vec_results);

        // concatenate all the batches together
        let mut fts_results = concat_batches(&fts_schema, fts_results.iter())?;
        let mut vec_results = concat_batches(&vec_schema, vec_results.iter())?;

        if matches!(self.request.base.norm, Some(NormalizeMethod::Rank)) {
            vec_results = hybrid::rank(vec_results, DIST_COL, None)?;
            fts_results = hybrid::rank(fts_results, SCORE_COL, None)?;
        }

        vec_results = hybrid::normalize_scores(vec_results, DIST_COL, None)?;
        fts_results = hybrid::normalize_scores(fts_results, SCORE_COL, None)?;

        let reranker = self
            .request
            .base
            .reranker
            .clone()
            .unwrap_or(Arc::new(RRFReranker::default()));

        let fts_query = self
            .request
            .base
            .full_text_search
            .as_ref()
            .ok_or(Error::Runtime {
                message: "there should be an FTS search".to_string(),
            })?;

        let mut results = reranker
            .rerank_hybrid(&fts_query.query.query(), vec_results, fts_results)
            .await?;

        check_reranker_result(&results)?;

        let limit = self.request.base.limit.unwrap_or(DEFAULT_TOP_K);
        if results.num_rows() > limit {
            results = results.slice(0, limit);
        }

        if !self.request.base.with_row_id {
            results = results.drop_column(ROW_ID)?;
        }

        Ok(SendableRecordBatchStream::from(
            RecordBatchStreamAdapter::new(results.schema(), stream::iter([Ok(results)])),
        ))
    }

    async fn inner_execute_with_options(
        &self,
        options: QueryExecutionOptions,
    ) -> Result<SendableRecordBatchStream> {
        let plan = self.create_plan(options.clone()).await?;
        let inner = execute_plan(plan, Default::default())?;
        let inner = if let Some(timeout) = options.timeout {
            TimeoutStream::new_boxed(inner, timeout)
        } else {
            inner
        };
        Ok(DatasetRecordBatchStream::new(inner).into())
    }
}

impl ExecutableQuery for VectorQuery {
    async fn create_plan(&self, options: QueryExecutionOptions) -> Result<Arc<dyn ExecutionPlan>> {
        let query = AnyQuery::VectorQuery(self.request.clone());
        self.parent.clone().create_plan(&query, options).await
    }

    async fn execute_with_options(
        &self,
        options: QueryExecutionOptions,
    ) -> Result<SendableRecordBatchStream> {
        if self.request.base.full_text_search.is_some() {
            let hybrid_result = async move { self.execute_hybrid(options).await }
                .boxed()
                .await?;
            return Ok(hybrid_result);
        }

        self.inner_execute_with_options(options).await
    }

    async fn explain_plan(&self, verbose: bool) -> Result<String> {
        let query = AnyQuery::VectorQuery(self.request.clone());
        self.parent.explain_plan(&query, verbose).await
    }

    async fn analyze_plan_with_options(&self, options: QueryExecutionOptions) -> Result<String> {
        let query = AnyQuery::VectorQuery(self.request.clone());
        self.parent.analyze_plan(&query, options).await
    }
}

impl HasQuery for VectorQuery {
    fn mut_query(&mut self) -> &mut QueryRequest {
        &mut self.request.base
    }
}

/// A builder for LanceDB take queries.
///
/// See [`crate::Table::query`] for more details on queries
///
/// A `TakeQuery` is a query that is used to select a subset of rows
/// from a table using dataset offsets or row ids.
///
/// See [`ExecutableQuery`] for methods that can be used to execute
/// the query and retrieve results.
///
/// This query object can be reused to issue the same query multiple
/// times.
#[derive(Debug, Clone)]
pub struct TakeQuery {
    parent: Arc<dyn BaseTable>,
    request: QueryRequest,
}

impl TakeQuery {
    /// Create a new `TakeQuery` that will return rows at the given offsets.
    ///
    /// See [`crate::Table::take_offsets`] for more details.
    pub fn from_offsets(parent: Arc<dyn BaseTable>, offsets: Vec<u64>) -> Self {
        let filter = format!(
            "_rowoffset in ({})",
            offsets
                .iter()
                .map(|o| o.to_string())
                .collect::<Vec<_>>()
                .join(",")
        );
        Self {
            parent,
            request: QueryRequest {
                filter: Some(QueryFilter::Sql(filter)),
                ..Default::default()
            },
        }
    }

    /// Create a new `TakeQuery` that will return rows with the given row ids.
    ///
    /// See [`crate::Table::take_row_ids`] for more details.
    pub fn from_row_ids(parent: Arc<dyn BaseTable>, row_ids: Vec<u64>) -> Self {
        let filter = format!(
            "_rowid in ({})",
            row_ids
                .iter()
                .map(|o| o.to_string())
                .collect::<Vec<_>>()
                .join(",")
        );
        Self {
            parent,
            request: QueryRequest {
                filter: Some(QueryFilter::Sql(filter)),
                ..Default::default()
            },
        }
    }

    /// Convert the `TakeQuery` into a `QueryRequest`.
    pub fn into_request(self) -> QueryRequest {
        self.request
    }

    /// Return the current `QueryRequest` for the `TakeQuery`.
    pub fn current_request(&self) -> &QueryRequest {
        &self.request
    }

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
    pub fn select(mut self, selection: Select) -> Self {
        self.request.select = selection;
        self
    }

    /// Return the `_rowid` meta column from the Table.
    pub fn with_row_id(mut self) -> Self {
        self.request.with_row_id = true;
        self
    }
}

impl HasQuery for TakeQuery {
    fn mut_query(&mut self) -> &mut QueryRequest {
        &mut self.request
    }
}

impl ExecutableQuery for TakeQuery {
    async fn create_plan(&self, options: QueryExecutionOptions) -> Result<Arc<dyn ExecutionPlan>> {
        let req = AnyQuery::Query(self.request.clone());
        self.parent.clone().create_plan(&req, options).await
    }

    async fn execute_with_options(
        &self,
        options: QueryExecutionOptions,
    ) -> Result<SendableRecordBatchStream> {
        let query = AnyQuery::Query(self.request.clone());
        Ok(SendableRecordBatchStream::from(
            self.parent.clone().query(&query, options).await?,
        ))
    }

    async fn explain_plan(&self, verbose: bool) -> Result<String> {
        let query = AnyQuery::Query(self.request.clone());
        self.parent.explain_plan(&query, verbose).await
    }

    async fn analyze_plan_with_options(&self, options: QueryExecutionOptions) -> Result<String> {
        let query = AnyQuery::Query(self.request.clone());
        self.parent.analyze_plan(&query, options).await
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, sync::Arc};

    use super::*;
    use arrow::{array::downcast_array, compute::concat_batches, datatypes::Int32Type};
    use arrow_array::{
        cast::AsArray, types::Float32Type, FixedSizeListArray, Float32Array, Int32Array,
        RecordBatch, RecordBatchIterator, RecordBatchReader, StringArray,
    };
    use arrow_schema::{DataType, Field as ArrowField, Schema as ArrowSchema};
    use futures::{StreamExt, TryStreamExt};
    use lance_testing::datagen::{BatchGenerator, IncrementingInt32, RandomVector};
    use rand::seq::IndexedRandom;
    use tempfile::tempdir;

    use crate::{connect, database::CreateTableMode, index::Index, Table};

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
        assert_eq!(
            *query
                .request
                .query_vector
                .first()
                .unwrap()
                .as_ref()
                .as_primitive(),
            vector
        );

        let new_vector = Float32Array::from_iter_values([9.8, 8.7]);

        let query = table
            .query()
            .limit(100)
            .offset(1)
            .nearest_to(&[9.8, 8.7])
            .unwrap()
            .nprobes(1000)
            .postfilter()
            .distance_type(DistanceType::Cosine)
            .refine_factor(999);

        assert_eq!(
            *query
                .request
                .query_vector
                .first()
                .unwrap()
                .as_ref()
                .as_primitive(),
            new_vector
        );
        assert_eq!(query.request.base.limit.unwrap(), 100);
        assert_eq!(query.request.base.offset.unwrap(), 1);
        assert_eq!(query.request.minimum_nprobes, 1000);
        assert_eq!(query.request.maximum_nprobes, Some(1000));
        assert!(query.request.use_index);
        assert_eq!(query.request.distance_type, Some(DistanceType::Cosine));
        assert_eq!(query.request.refine_factor, Some(999));
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
            assert_eq!(batch.expect("should be Ok").num_rows(), 10);
        }

        let query = table
            .query()
            .limit(10)
            .offset(1)
            .only_if(String::from("id % 2 == 0"))
            .nearest_to(&[0.1; 4])
            .unwrap();
        let result = query.execute().await;
        let mut stream = result.expect("should have result");
        // should only have one batch
        while let Some(batch) = stream.next().await {
            // pre filter should return 10 rows
            assert_eq!(batch.expect("should be Ok").num_rows(), 10);
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

    #[tokio::test]
    async fn test_analyze_plan() {
        let tmp_dir = tempdir().unwrap();
        let table = make_test_table(&tmp_dir).await;

        let result = table.query().analyze_plan().await.unwrap();
        assert!(result.contains("metrics="));
    }

    #[tokio::test]
    async fn test_analyze_plan_with_options() {
        let tmp_dir = tempdir().unwrap();
        let table = make_test_table(&tmp_dir).await;

        let result = table
            .query()
            .analyze_plan_with_options(QueryExecutionOptions {
                max_batch_length: 10,
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(result.contains("metrics="));
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

    #[tokio::test]
    async fn test_with_row_id() {
        let tmp_dir = tempdir().unwrap();
        let table = make_test_table(&tmp_dir).await;
        let results = table
            .vector_search(&[0.1, 0.2, 0.3, 0.4])
            .unwrap()
            .with_row_id()
            .limit(10)
            .execute()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        for batch in results {
            assert!(batch.column_by_name("_rowid").is_some());
        }
    }

    #[tokio::test]
    async fn test_distance_range() {
        let tmp_dir = tempdir().unwrap();
        let table = make_test_table(&tmp_dir).await;
        let results = table
            .vector_search(&[0.1, 0.2, 0.3, 0.4])
            .unwrap()
            .distance_range(Some(0.0), Some(1.0))
            .limit(10)
            .execute()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        for batch in results {
            let distances = batch["_distance"].as_primitive::<Float32Type>();
            assert!(distances.iter().all(|d| {
                let d = d.unwrap();
                (0.0..1.0).contains(&d)
            }));
        }
    }

    #[tokio::test]
    async fn test_multiple_query_vectors() {
        let tmp_dir = tempdir().unwrap();
        let table = make_test_table(&tmp_dir).await;
        let query = table
            .query()
            .nearest_to(&[0.1, 0.2, 0.3, 0.4])
            .unwrap()
            .add_query_vector(&[0.5, 0.6, 0.7, 0.8])
            .unwrap()
            .limit(1);

        let plan = query.explain_plan(true).await.unwrap();
        assert!(plan.contains("UnionExec"));

        let results = query
            .execute()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let results = concat_batches(&results[0].schema(), &results).unwrap();
        assert_eq!(results.num_rows(), 2); // One result for each query vector.
        let query_index = results["query_index"].as_primitive::<Int32Type>();
        // We don't guarantee order.
        assert!(query_index.values().contains(&0));
        assert!(query_index.values().contains(&1));
    }

    #[tokio::test]
    async fn test_hybrid_search() {
        let tmp_dir = tempdir().unwrap();
        let dataset_path = tmp_dir.path();
        let conn = connect(dataset_path.to_str().unwrap())
            .execute()
            .await
            .unwrap();

        let dims = 2;
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("text", DataType::Utf8, false),
            ArrowField::new(
                "vector",
                DataType::FixedSizeList(
                    Arc::new(ArrowField::new("item", DataType::Float32, true)),
                    dims,
                ),
                false,
            ),
        ]));

        let text = StringArray::from(vec!["dog", "cat", "a", "b"]);
        let vectors = vec![
            Some(vec![Some(0.0), Some(0.0)]),
            Some(vec![Some(-2.0), Some(-2.0)]),
            Some(vec![Some(50.0), Some(50.0)]),
            Some(vec![Some(-30.0), Some(-30.0)]),
        ];
        let vector = FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(vectors, dims);

        let record_batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(text), Arc::new(vector)]).unwrap();
        let record_batch_iter =
            RecordBatchIterator::new(vec![record_batch].into_iter().map(Ok), schema.clone());
        let table = conn
            .create_table("my_table", record_batch_iter)
            .execute()
            .await
            .unwrap();

        table
            .create_index(&["text"], crate::index::Index::FTS(Default::default()))
            .replace(true)
            .execute()
            .await
            .unwrap();

        let fts_query = FullTextSearchQuery::new("b".to_string());
        let results = table
            .query()
            .full_text_search(fts_query)
            .limit(2)
            .nearest_to(&[-10.0, -10.0])
            .unwrap()
            .execute()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        let batch = &results[0];

        let texts: StringArray = downcast_array(batch.column_by_name("text").unwrap());
        let texts = texts.iter().map(|e| e.unwrap()).collect::<HashSet<_>>();
        assert!(texts.contains("cat")); // should be close by vector search
        assert!(texts.contains("b")); // should be close by fts search

        // ensure that this works correctly if there are no matching FTS results
        let fts_query = FullTextSearchQuery::new("z".to_string());
        table
            .query()
            .full_text_search(fts_query)
            .limit(2)
            .nearest_to(&[-10.0, -10.0])
            .unwrap()
            .execute()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_hybrid_search_empty_table() {
        let tmp_dir = tempdir().unwrap();
        let dataset_path = tmp_dir.path();
        let conn = connect(dataset_path.to_str().unwrap())
            .execute()
            .await
            .unwrap();

        let dims = 2;

        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("text", DataType::Utf8, false),
            ArrowField::new(
                "vector",
                DataType::FixedSizeList(
                    Arc::new(ArrowField::new("item", DataType::Float32, true)),
                    dims,
                ),
                false,
            ),
        ]));

        // ensure hybrid search is also supported on a fully empty table
        let vectors: Vec<Option<Vec<Option<f32>>>> = Vec::new();
        let record_batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(Vec::<&str>::new())),
                Arc::new(
                    FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(vectors, dims),
                ),
            ],
        )
        .unwrap();
        let record_batch_iter =
            RecordBatchIterator::new(vec![record_batch].into_iter().map(Ok), schema.clone());
        let table = conn
            .create_table("my_table", record_batch_iter)
            .mode(CreateTableMode::Overwrite)
            .execute()
            .await
            .unwrap();
        table
            .create_index(&["text"], crate::index::Index::FTS(Default::default()))
            .replace(true)
            .execute()
            .await
            .unwrap();
        let fts_query = FullTextSearchQuery::new("b".to_string());
        let results = table
            .query()
            .full_text_search(fts_query)
            .limit(2)
            .nearest_to(&[-10.0, -10.0])
            .unwrap()
            .execute()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let batch = &results[0];
        assert_eq!(0, batch.num_rows());
        assert_eq!(2, batch.num_columns());
    }

    // TODO: Implement a good FTS test data generator in lance_datagen.
    fn fts_test_data(nrows: usize) -> RecordBatch {
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("text", DataType::Utf8, false),
            ArrowField::new("id", DataType::Int32, false),
        ]));

        let ids: Int32Array = (1..=nrows as i32).collect();

        // Sample 1 - 3 tokens for each string value
        let tokens = ["a", "b", "c", "d", "e"];
        use rand::{rng, Rng};

        let mut rng = rng();
        let text: StringArray = (0..nrows)
            .map(|_| {
                let num_tokens = rng.random_range(1..=3); // 1 to 3 tokens
                let selected_tokens: Vec<&str> = tokens
                    .choose_multiple(&mut rng, num_tokens)
                    .cloned()
                    .collect();
                Some(selected_tokens.join(" "))
            })
            .collect();

        RecordBatch::try_new(schema, vec![Arc::new(text), Arc::new(ids)]).unwrap()
    }

    async fn run_query_request(table: &dyn BaseTable, query: AnyQuery) -> RecordBatch {
        use lance::io::RecordBatchStream;
        let stream = table.query(&query, Default::default()).await.unwrap();
        let schema = stream.schema();
        let batches = stream.try_collect::<Vec<_>>().await.unwrap();
        arrow::compute::concat_batches(&schema, &batches).unwrap()
    }

    async fn test_pagination(table: &dyn BaseTable, full_query: AnyQuery, page_size: usize) {
        // Get full results
        let full_results = run_query_request(table, full_query.clone()).await;

        // Then use limit & offset to do paginated queries, assert each
        // is the same as a slice of the full results
        let mut offset = 0;
        while offset < full_results.num_rows() {
            let mut paginated_query = full_query.clone();
            let limit = page_size.min(full_results.num_rows() - offset);
            match &mut paginated_query {
                AnyQuery::Query(query)
                | AnyQuery::VectorQuery(VectorQueryRequest { base: query, .. }) => {
                    query.limit = Some(limit);
                    query.offset = Some(offset);
                }
            }
            let paginated_results = run_query_request(table, paginated_query).await;
            let expected_slice = full_results.slice(offset, limit);
            assert_eq!(
                paginated_results, expected_slice,
                "Paginated results do not match expected slice at offset {}, for page size {}",
                offset, page_size
            );
            offset += page_size;
        }
    }

    #[tokio::test]
    async fn test_pagination_with_scan() {
        let db = connect("memory://test").execute().await.unwrap();
        let table = db
            .create_table("test_table", make_non_empty_batches())
            .execute()
            .await
            .unwrap();
        let query = AnyQuery::Query(table.query().into_request());
        test_pagination(table.base_table().as_ref(), query.clone(), 3).await;
        test_pagination(table.base_table().as_ref(), query, 10).await;
    }

    #[tokio::test]
    async fn test_pagination_with_fts() {
        let db = connect("memory://test").execute().await.unwrap();
        let data = fts_test_data(400);
        let schema = data.schema();
        let data = RecordBatchIterator::new(vec![Ok(data)], schema);
        let table = db.create_table("test_table", data).execute().await.unwrap();

        table
            .create_index(&["text"], Index::FTS(Default::default()))
            .execute()
            .await
            .unwrap();
        let query = table
            .query()
            .full_text_search(FullTextSearchQuery::new("test".into()))
            .into_request();
        let query = AnyQuery::Query(query);
        test_pagination(table.base_table().as_ref(), query.clone(), 3).await;
        test_pagination(table.base_table().as_ref(), query, 10).await;
    }

    #[tokio::test]
    async fn test_pagination_with_vector_query() {
        let db = connect("memory://test").execute().await.unwrap();
        let table = db
            .create_table("test_table", make_non_empty_batches())
            .execute()
            .await
            .unwrap();
        let query_vector = vec![0.1_f32, 0.2, 0.3, 0.4];
        let query = table
            .query()
            .nearest_to(query_vector.as_slice())
            .unwrap()
            .limit(50)
            .into_request();
        let query = AnyQuery::VectorQuery(query);
        test_pagination(table.base_table().as_ref(), query.clone(), 3).await;
        test_pagination(table.base_table().as_ref(), query, 10).await;
    }

    #[tokio::test]
    async fn test_take_offsets() {
        let tmp_dir = tempdir().unwrap();
        let table = make_test_table(&tmp_dir).await;

        let results = table
            .take_offsets(vec![5, 1, 17])
            .execute()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].num_rows(), 3);
        assert_eq!(results[0].num_columns(), 2);

        let mut ids = results[0]
            .column_by_name("id")
            .unwrap()
            .as_primitive::<Int32Type>()
            .values()
            .to_vec();
        ids.sort();

        assert_eq!(ids, vec![1, 5, 17]);

        // Select specific columns
        let results = table
            .take_offsets(vec![5, 1, 17])
            .select(Select::Columns(vec!["vector".to_string()]))
            .execute()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].num_rows(), 3);
        assert_eq!(results[0].num_columns(), 1);
    }

    #[tokio::test]
    async fn test_take_row_ids() {
        let tmp_dir = tempdir().unwrap();
        let table = make_test_table(&tmp_dir).await;

        let results = table
            .take_row_ids(vec![5, 1, 17])
            .execute()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].num_rows(), 3);
        assert_eq!(results[0].num_columns(), 2);

        let mut ids = results[0]
            .column_by_name("id")
            .unwrap()
            .as_primitive::<Int32Type>()
            .values()
            .to_vec();

        ids.sort();

        assert_eq!(ids, vec![1, 5, 17]);
    }
}
