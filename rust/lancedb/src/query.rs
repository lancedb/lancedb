// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::sync::Arc;
use std::{future::Future, time::Duration};

use arrow::compute::concat_batches;
use arrow_array::{Array, Float16Array, Float32Array, Float64Array, RecordBatch, make_array};
use arrow_schema::{DataType, Schema, SchemaRef};
use datafusion_expr::{Expr, col, lit};
use datafusion_physical_plan::ExecutionPlan;
use futures::{FutureExt, TryFutureExt, TryStreamExt, stream, try_join};
use half::f16;
/// Re-export Lance ColumnOrdering type for use in query ordering
pub use lance::dataset::scanner::ColumnOrdering;
use lance::dataset::{ROW_ID, scanner::DatasetRecordBatchStream};
use lance_arrow::RecordBatchExt;
use lance_core::datatypes::parse_field_path;
use lance_datafusion::exec::execute_plan;
use lance_datafusion::planner::Planner;
use lance_index::scalar::FullTextSearchQuery;
use lance_index::scalar::inverted::SCORE_COL;
use lance_index::vector::DIST_COL;

use crate::error::{Error, FunctionErrorCode, Result};
use crate::function::{
    GENERATED_COLUMN_METADATA_KEY, GeneratedColumnBindingSnapshot, GeneratedColumnStatus,
};
use crate::rerankers::rrf::RRFReranker;
use crate::rerankers::{NormalizeMethod, Reranker, check_reranker_result};
use crate::table::BaseTable;
use crate::utils::{MaxBatchLengthStream, TimeoutStream, default_vector_column};
use crate::{ApproxMode, DistanceType};
use crate::{
    arrow::{SendableRecordBatchStream, SimpleRecordBatchStream},
    table::AnyQuery,
};

mod hybrid;

pub(crate) const DEFAULT_TOP_K: usize = 10;

/// Which columns should be retrieved from the database
#[derive(Debug, Clone)]
pub enum Select {
    /// Select all non-system columns
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
    /// Advanced selection using type-safe DataFusion expressions
    ///
    /// Similar to [`Select::Dynamic`] but uses [`datafusion_expr::Expr`] instead of
    /// raw SQL strings. Use [`crate::expr`] helpers to build expressions:
    ///
    /// ```
    /// use lancedb::expr::{col, lit};
    /// use lancedb::query::Select;
    ///
    /// // SELECT id, id * 2 AS id2 FROM ...
    /// let selection = Select::expr_projection(&[
    ///     ("id", col("id")),
    ///     ("id2", col("id") * lit(2)),
    /// ]);
    /// ```
    ///
    /// Note: For remote/server-side queries the expressions are serialized to SQL strings
    /// automatically (same as [`Select::Dynamic`]).
    Expr(Vec<(String, datafusion_expr::Expr)>),
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
    /// Create a typed-expression projection.
    ///
    /// This is a convenience method for creating a [`Select::Expr`] variant from
    /// a slice of `(name, expr)` pairs where each `expr` is a [`datafusion_expr::Expr`].
    ///
    /// # Example
    /// ```
    /// use lancedb::expr::{col, lit};
    /// use lancedb::query::Select;
    ///
    /// let selection = Select::expr_projection(&[
    ///     ("id", col("id")),
    ///     ("id2", col("id") * lit(2)),
    /// ]);
    /// ```
    pub fn expr_projection(columns: &[(impl AsRef<str>, datafusion_expr::Expr)]) -> Self {
        Self::Expr(
            columns
                .iter()
                .map(|(name, expr)| (name.as_ref().to_string(), expr.clone()))
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
                ),
            })
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
            }
            DataType::Float64 => {
                let arr: Vec<f64> = self.iter().map(|x| f64::from(*x)).collect();
                Ok(Arc::new(Float64Array::from(arr)))
            }
            _ => Err(Error::InvalidInput {
                message: format!(
                    "failed to create query vector, the input data type was &[f16] but the embedding model \"{}\" expected data type {:?}",
                    embedding_model_label, data_type
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
            }
            DataType::Float64 => {
                let arr: Vec<f64> = self.iter().map(|x| *x as f64).collect();
                Ok(Arc::new(Float64Array::from(arr)))
            }
            _ => Err(Error::InvalidInput {
                message: format!(
                    "failed to create query vector, the input data type was &[f32] but the embedding model \"{}\" expected data type {:?}",
                    embedding_model_label, data_type
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
            }
            DataType::Float64 => {
                let arr: Vec<f64> = self.to_vec();
                Ok(Arc::new(Float64Array::from(arr)))
            }
            _ => Err(Error::InvalidInput {
                message: format!(
                    "failed to create query vector, the input data type was &[f64] but the embedding model \"{}\" expected data type {:?}",
                    embedding_model_label, data_type
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
    ///
    /// Calling this multiple times combines the filters with a logical AND
    /// (i.e. `(previous) AND (new)`) rather than replacing the previous filter.
    fn only_if(self, filter: impl AsRef<str>) -> Self;

    /// Only return rows which match the filter, using an expression builder.
    ///
    /// Use [`crate::expr`] for building type-safe expressions:
    ///
    /// ```
    /// use lancedb::expr::{col, lit};
    /// use lancedb::query::{QueryBase, ExecutableQuery};
    ///
    /// # use lancedb::Table;
    /// # async fn query(table: &Table) -> Result<(), Box<dyn std::error::Error>> {
    /// let results = table.query()
    ///     .only_if_expr(col("age").gt(lit(18)).and(col("status").eq(lit("active"))))
    ///     .execute()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// Note: Expression filters are not supported for remote/server-side queries.
    /// Use [`QueryBase::only_if`] with SQL strings for remote tables.
    ///
    /// Calling this multiple times combines the expressions with a logical AND
    /// rather than replacing the previous filter.
    fn only_if_expr(self, filter: datafusion_expr::Expr) -> Self;

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

    /// Sort the results by the specified column(s).
    ///
    /// This allows ordering query results by one or more columns in either ascending or descending order.
    fn order_by(self, ordering: Option<Vec<ColumnOrdering>>) -> Self;

    /// Control MemWAL read routing for this query.
    ///
    /// By default (unset), when the table carries a MemWAL write spec (see
    /// [`crate::Table::set_lsm_write_spec`]), reads are routed through the LSM
    /// scanner so they also return data written via the `merge_insert` LSM path
    /// that has not yet been compacted into the base table (active/frozen
    /// memtables and flushed generations); a table without a spec reads the base
    /// table.
    ///
    /// - `use_lsm(true)` forces LSM routing and errors if the table has no
    ///   MemWAL write spec.
    /// - `use_lsm(false)` bypasses the MemWAL and reads the base table only,
    ///   even when a spec is present.
    ///
    /// Note: the LSM scanner does not support every query shape (e.g. reranking,
    /// hybrid search, `order_by`). On a MemWAL table those shapes error unless
    /// `use_lsm(false)` is set, because a base-only read would silently
    /// exclude un-compacted MemWAL data.
    fn use_lsm(self, enable: bool) -> Self;
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
        self.mut_query()
            .add_filter(QueryFilter::Sql(filter.as_ref().to_string()));
        self
    }

    fn only_if_expr(mut self, filter: datafusion_expr::Expr) -> Self {
        self.mut_query().add_filter(QueryFilter::Datafusion(filter));
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

    fn order_by(mut self, ordering: Option<Vec<ColumnOrdering>>) -> Self {
        self.mut_query().order_by = ordering;
        self
    }

    fn use_lsm(mut self, enable: bool) -> Self {
        self.mut_query().use_lsm = Some(enable);
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
    /// How distributed worker metrics should be displayed by
    /// [`ExecutableQuery::analyze_plan`].
    ///
    /// This only affects remote distributed query plans. Local query execution
    /// ignores this option.
    pub analyze_plan_distributed_metrics: AnalyzePlanDistributedMetrics,
}

impl Default for QueryExecutionOptions {
    fn default() -> Self {
        Self {
            max_batch_length: 1024,
            timeout: None,
            analyze_plan_distributed_metrics: AnalyzePlanDistributedMetrics::Aggregate,
        }
    }
}

impl QueryExecutionOptions {
    fn without_output_batch_length_limit(&self) -> Self {
        let mut options = self.clone();
        options.max_batch_length = 0;
        options
    }
}

/// How distributed worker metrics are displayed in analyzed query plans.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum AnalyzePlanDistributedMetrics {
    /// Preserve the legacy output: aggregate worker metrics into one synthetic tree.
    #[default]
    Aggregate,
    /// Render one raw worker-side tree per distributed worker.
    PerWorker,
    /// Render the aggregate tree followed by the raw per-worker trees.
    Full,
}

impl AnalyzePlanDistributedMetrics {
    pub(crate) fn as_query_param(self) -> &'static str {
        match self {
            Self::Aggregate => "aggregate",
            Self::PerWorker => "per_worker",
            Self::Full => "full",
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

    /// Explain the plan for a query
    ///
    /// This will create a string representation of the plan that will be used to
    /// execute the query.  This will not execute the query.
    ///
    /// This function can be used to get an understanding of what work will be done by the query
    /// and is useful for debugging query performance.
    fn explain_plan(&self, verbose: bool) -> impl Future<Output = Result<String>> + Send;

    /// Execute the query and display the runtime metrics
    ///
    /// This shows the same plan as [`ExecutableQuery::explain_plan`] but includes runtime metrics.
    ///
    /// This function will actually execute the query in order to get the runtime metrics.
    fn analyze_plan(&self) -> impl Future<Output = Result<String>> + Send {
        self.analyze_plan_with_options(QueryExecutionOptions::default())
    }

    /// Execute the query and display the runtime metrics
    ///
    /// This is the same as [`ExecutableQuery::analyze_plan`] but allows for specifying the execution options.
    fn analyze_plan_with_options(
        &self,
        options: QueryExecutionOptions,
    ) -> impl Future<Output = Result<String>> + Send;

    /// Return the output schema for data returned by the query without actually executing the query
    ///
    /// This can be useful when the selection for a query is built dynamically as it is not always
    /// obvious what the output schema will be.
    fn output_schema(&self) -> impl Future<Output = Result<SchemaRef>> + Send {
        self.create_plan(QueryExecutionOptions::default())
            .and_then(|plan| std::future::ready(Ok(plan.schema())))
    }
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

/// Combine two filters with a logical AND.
///
/// This is used when a query receives more than one filter (for example when
/// `where`/`only_if` is called multiple times) so the filters are composed
/// with AND rather than the later filter silently replacing the earlier one.
///
/// SQL string and expression filters are combined within their own
/// representation. When the two representations are mixed, the expression is
/// lowered to SQL (via [`crate::expr::expr_to_sql_string`]) and the filters are
/// combined as SQL strings. Substrait filters cannot be combined and return an
/// error.
fn and_filters(existing: QueryFilter, new: QueryFilter) -> Result<QueryFilter> {
    match (existing, new) {
        (QueryFilter::Sql(lhs), QueryFilter::Sql(rhs)) => {
            Ok(QueryFilter::Sql(format!("({lhs}) AND ({rhs})")))
        }
        (QueryFilter::Datafusion(lhs), QueryFilter::Datafusion(rhs)) => {
            Ok(QueryFilter::Datafusion(lhs.and(rhs)))
        }
        (QueryFilter::Sql(lhs), QueryFilter::Datafusion(rhs)) => {
            let rhs = crate::expr::expr_to_sql_string(&rhs)?;
            Ok(QueryFilter::Sql(format!("({lhs}) AND ({rhs})")))
        }
        (QueryFilter::Datafusion(lhs), QueryFilter::Sql(rhs)) => {
            let lhs = crate::expr::expr_to_sql_string(&lhs)?;
            Ok(QueryFilter::Sql(format!("({lhs}) AND ({rhs})")))
        }
        _ => Err(Error::InvalidInput {
            message: "cannot combine a Substrait filter with another filter".to_string(),
        }),
    }
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

    /// An error recorded while combining repeated filters that could not be
    /// composed (see [`QueryRequest::add_filter`]). It is surfaced when the
    /// query is executed via [`QueryRequest::check_filter`]. We defer the error
    /// because the builder methods that set filters return `Self` rather than a
    /// `Result`.
    pub(crate) filter_error: Option<String>,

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

    /// If set to true, disables automatic projection of scoring columns (_score, _distance).
    /// When disabled, these columns are only included if explicitly requested in the projection.
    ///
    /// By default, this is false (scoring columns are auto-projected for backward compatibility).
    pub disable_scoring_autoprojection: bool,

    /// Sort the results by the specified column(s).
    ///
    /// This allows ordering query results by one or more columns in either ascending or descending order.
    pub order_by: Option<Vec<ColumnOrdering>>,

    /// Controls MemWAL read routing. When unset (the default), a query against a
    /// table that carries a MemWAL write spec (see
    /// [`crate::Table::set_lsm_write_spec`]) is routed through the LSM scanner so
    /// it also sees data written via the `merge_insert` LSM path that has not yet
    /// been compacted into the base table — the active and frozen in-memory
    /// memtables and the flushed (L0) generations, deduplicated by primary key
    /// against the base table (newest generation wins); a table without a spec
    /// reads the base table.
    ///
    /// - `Some(true)` forces LSM routing and errors if the table has no MemWAL
    ///   write spec.
    /// - `Some(false)` reads only the base table, bypassing the MemWAL.
    pub use_lsm: Option<bool>,
}

impl Default for QueryRequest {
    fn default() -> Self {
        Self {
            limit: None,
            offset: None,
            filter: None,
            filter_error: None,
            full_text_search: None,
            select: Select::All,
            fast_search: false,
            with_row_id: false,
            prefilter: true,
            reranker: None,
            norm: None,
            disable_scoring_autoprojection: false,
            order_by: None,
            use_lsm: None,
        }
    }
}

impl QueryRequest {
    /// Add a filter, combining it with any existing filter using a logical AND.
    ///
    /// If the new filter cannot be combined with the existing one (because they
    /// use different representations) the error is recorded and surfaced later
    /// by [`Self::check_filter`].
    pub(crate) fn add_filter(&mut self, new: QueryFilter) {
        self.filter = Some(match self.filter.take() {
            None => new,
            Some(existing) => match and_filters(existing, new) {
                Ok(combined) => combined,
                Err(err) => {
                    // The filters were consumed while attempting to combine
                    // them; the recorded error is surfaced by `check_filter`
                    // before the query executes.
                    self.filter_error = Some(err.to_string());
                    return;
                }
            },
        });
    }

    /// Return an error if combining filters failed (see [`Self::add_filter`]).
    ///
    /// This must be called by every backend before executing a query.
    pub(crate) fn check_filter(&self) -> Result<()> {
        if let Some(message) = &self.filter_error {
            return Err(Error::InvalidInput {
                message: message.clone(),
            });
        }
        Ok(())
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
    /// The speed / accuracy tradeoff to use for approximate vector search
    pub approx_mode: Option<ApproxMode>,
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
            approx_mode: None,
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
        if let Some(maximum_nprobes) = self.request.maximum_nprobes
            && minimum_nprobes > maximum_nprobes
        {
            return Err(Error::InvalidInput {
                message: "minimum_nprobes must be less than or equal to maximum_nprobes"
                    .to_string(),
            });
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

    /// Set the speed / accuracy tradeoff for approximate vector search.
    ///
    /// This setting is currently only used by RQ-quantized indexes, such as
    /// IVF_RQ. Other index types ignore this setting.
    pub fn approx_mode(mut self, approx_mode: ApproxMode) -> Self {
        self.request.approx_mode = Some(approx_mode);
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
        let max_batch_length = options.max_batch_length as usize;
        let internal_options = options.without_output_batch_length_limit();
        // clone query and specify we want to include row IDs, which can be needed for reranking
        let mut fts_query = Query::new(self.parent.clone());
        fts_query.request = self.request.base.clone();
        fts_query = fts_query.with_row_id();

        let mut vector_query = self.clone().with_row_id();

        vector_query.request.base.full_text_search = None;
        let (fts_results, vec_results) = try_join!(
            fts_query.execute_with_options(internal_options.clone()),
            vector_query.inner_execute_with_options(internal_options)
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

        Ok(single_batch_stream(results, max_batch_length))
    }

    async fn inner_execute_with_options(
        &self,
        options: QueryExecutionOptions,
    ) -> Result<SendableRecordBatchStream> {
        let plan = self.create_plan(options.clone()).await?;
        let inner = execute_plan(plan, Default::default())?;
        let inner = MaxBatchLengthStream::new_boxed(inner, options.max_batch_length as usize);
        let inner = if let Some(timeout) = options.timeout {
            TimeoutStream::new_boxed(inner, timeout)
        } else {
            inner
        };
        Ok(DatasetRecordBatchStream::new(inner).into())
    }
}

fn single_batch_stream(batch: RecordBatch, max_batch_length: usize) -> SendableRecordBatchStream {
    let schema = batch.schema();
    if max_batch_length == 0 || batch.num_rows() <= max_batch_length {
        return Box::pin(SimpleRecordBatchStream::new(
            stream::iter([Ok(batch)]),
            schema,
        ));
    }

    let mut batches = Vec::with_capacity(batch.num_rows().div_ceil(max_batch_length));
    let mut offset = 0;
    while offset < batch.num_rows() {
        let length = (batch.num_rows() - offset).min(max_batch_length);
        batches.push(Ok(batch.slice(offset, length)));
        offset += length;
    }
    Box::pin(SimpleRecordBatchStream::new(stream::iter(batches), schema))
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
        let in_list: Vec<Expr> = offsets.iter().map(|o| lit(*o)).collect();
        Self {
            parent,
            request: QueryRequest {
                filter: Some(QueryFilter::Datafusion(
                    col("_rowoffset").in_list(in_list, false),
                )),
                ..Default::default()
            },
        }
    }

    /// Create a new `TakeQuery` that will return rows with the given row ids.
    ///
    /// See [`crate::Table::take_row_ids`] for more details.
    pub fn from_row_ids(parent: Arc<dyn BaseTable>, row_ids: Vec<u64>) -> Self {
        let in_list: Vec<Expr> = row_ids.iter().map(|id| lit(*id)).collect();
        Self {
            parent,
            request: QueryRequest {
                filter: Some(QueryFilter::Datafusion(col(ROW_ID).in_list(in_list, false))),
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

/// Pure generated-column query reference guard.
///
/// Consumes one [`GeneratedColumnBindingSnapshot`] and structurally inspects
/// supported query positions. Returns
/// [`Error::Function`]`(`[`FunctionErrorCode::GeneratedColumnIncomplete`]`)`
/// when a referenced generated output is incomplete. Does not wait, execute
/// UDFs, substitute NULL, consult a catalog/index, mutate state, or take a
/// second snapshot.
///
/// Native query runtime wires this into the exact-snapshot planner and
/// namespace QueryTable pushdown path: callers build the snapshot from the same
/// Lance [`lance::Dataset`] used to plan or dispatch, then invoke this helper
/// before reading bytes or sending `query_table`. Remote error projection is
/// out of scope for this helper.
pub(crate) fn validate_generated_column_query(
    snapshot: &GeneratedColumnBindingSnapshot,
    query: &AnyQuery,
) -> Result<()> {
    let base = query.base();
    base.check_filter()?;

    if matches!(base.filter, Some(QueryFilter::Substrait(_)))
        && snapshot_has_generated_metadata(snapshot)
    {
        return Err(Error::NotSupported {
            message: "Substrait filters are not supported with generated columns".into(),
        });
    }

    // Exact snapshot Arrow view for inference utilities (e.g. default vector
    // column). Top-level names may contain `.` (literal field names).
    let schema = snapshot_arrow_schema(snapshot);
    // Lance Planner SQL parsing converts the Arrow schema to a Lance schema for
    // literal coercion. Lance rejects top-level names containing `.`, so the
    // SQL planner view omits only those illegal top-level names. Identifier
    // case/quoting still use Planner semantics; quoted dotted names are still
    // collected structurally via Expr::column_refs without schema membership.
    let planner = Planner::new(snapshot_planner_schema(snapshot));
    // First-seen reference order across select → filter → order_by → FTS → vector.
    let mut refs = Vec::new();

    collect_select_refs(snapshot, &base.select, &planner, &mut refs)?;
    collect_filter_refs(&base.filter, &planner, &mut refs)?;
    collect_order_by_refs(&base.order_by, &mut refs)?;
    collect_fts_refs(snapshot, &base.full_text_search, &mut refs)?;
    collect_vector_refs(query, schema.as_ref(), &mut refs)?;

    validate_referenced_entries(snapshot, &refs)
}

fn snapshot_arrow_schema(snapshot: &GeneratedColumnBindingSnapshot) -> SchemaRef {
    Arc::new(Schema::new(
        snapshot
            .entries()
            .iter()
            .map(|entry| entry.field().as_ref().clone())
            .collect::<Vec<_>>(),
    ))
}

fn snapshot_planner_schema(snapshot: &GeneratedColumnBindingSnapshot) -> SchemaRef {
    Arc::new(Schema::new(
        snapshot
            .entries()
            .iter()
            .filter(|entry| !entry.field().name().contains('.'))
            .map(|entry| entry.field().as_ref().clone())
            .collect::<Vec<_>>(),
    ))
}

fn snapshot_has_generated_metadata(snapshot: &GeneratedColumnBindingSnapshot) -> bool {
    snapshot.entries().iter().any(|entry| {
        entry
            .field()
            .metadata()
            .contains_key(GENERATED_COLUMN_METADATA_KEY)
    })
}

/// Insert `name` if absent, preserving first-seen order.
fn insert_ref_if_absent(refs: &mut Vec<String>, name: String) {
    if !refs.iter().any(|existing| existing == &name) {
        refs.push(name);
    }
}

fn collect_select_refs(
    snapshot: &GeneratedColumnBindingSnapshot,
    select: &Select,
    planner: &Planner,
    refs: &mut Vec<String>,
) -> Result<()> {
    match select {
        Select::All => {
            for entry in snapshot.entries() {
                insert_ref_if_absent(refs, entry.field().name().clone());
            }
        }
        Select::Columns(columns) => {
            for column in columns {
                push_field_path_root(refs, column)?;
            }
        }
        Select::Dynamic(columns) => {
            for (_alias, sql) in columns {
                collect_sql_expr_refs(planner, sql, refs)?;
            }
        }
        Select::Expr(columns) => {
            for (_alias, expr) in columns {
                collect_expr_column_refs(expr, refs);
            }
        }
    }
    Ok(())
}

fn collect_filter_refs(
    filter: &Option<QueryFilter>,
    planner: &Planner,
    refs: &mut Vec<String>,
) -> Result<()> {
    match filter {
        Some(QueryFilter::Sql(sql)) => collect_sql_filter_refs(planner, sql, refs),
        Some(QueryFilter::Datafusion(expr)) => {
            collect_expr_column_refs(expr, refs);
            Ok(())
        }
        // Substrait either rejected above or has no generated metadata.
        Some(QueryFilter::Substrait(_)) | None => Ok(()),
    }
}

fn collect_order_by_refs(
    order_by: &Option<Vec<ColumnOrdering>>,
    refs: &mut Vec<String>,
) -> Result<()> {
    if let Some(orderings) = order_by {
        for ordering in orderings {
            push_field_path_root(refs, &ordering.column_name)?;
        }
    }
    Ok(())
}

fn collect_fts_refs(
    snapshot: &GeneratedColumnBindingSnapshot,
    fts: &Option<FullTextSearchQuery>,
    refs: &mut Vec<String>,
) -> Result<()> {
    let Some(fts) = fts else {
        return Ok(());
    };
    let columns = fts.columns();
    if columns.is_empty() {
        // Index membership is not in the snapshot; fail closed on top-level
        // generated fields whose Arrow type matches Lance implicit-FTS shapes.
        for entry in snapshot.entries() {
            if entry
                .field()
                .metadata()
                .contains_key(GENERATED_COLUMN_METADATA_KEY)
                && is_implicit_fts_candidate_type(entry.field().data_type())
            {
                insert_ref_if_absent(refs, entry.field().name().clone());
            }
        }
    } else {
        for column in columns {
            push_field_path_root(refs, &column)?;
        }
    }
    Ok(())
}

fn collect_vector_refs(query: &AnyQuery, schema: &Schema, refs: &mut Vec<String>) -> Result<()> {
    let AnyQuery::VectorQuery(vector) = query else {
        return Ok(());
    };
    if let Some(column) = &vector.column {
        return push_field_path_root(refs, column);
    }
    let Some(query_vector) = vector.query_vector.first() else {
        return Ok(());
    };
    // Reuse the same inference as the query path; propagate ambiguity / no-candidate.
    let inferred = default_vector_column(schema, Some(query_vector.len() as i32))?;
    push_field_path_root(refs, &inferred)
}

fn collect_sql_expr_refs(planner: &Planner, sql: &str, refs: &mut Vec<String>) -> Result<()> {
    let expr = planner.parse_expr(sql)?;
    collect_expr_column_refs(&expr, refs);
    Ok(())
}

fn collect_sql_filter_refs(planner: &Planner, sql: &str, refs: &mut Vec<String>) -> Result<()> {
    let expr = planner.parse_filter(sql)?;
    collect_expr_column_refs(&expr, refs);
    Ok(())
}

fn collect_expr_column_refs(expr: &Expr, refs: &mut Vec<String>) {
    // Expr::column_refs returns a HashSet; normalize to lexical order before
    // first-seen insertion so diagnostic winners stay deterministic.
    let mut names: Vec<String> = expr
        .column_refs()
        .into_iter()
        .map(|column| column.name.clone())
        .collect();
    names.sort();
    for name in names {
        insert_ref_if_absent(refs, name);
    }
}

fn push_field_path_root(refs: &mut Vec<String>, path: &str) -> Result<()> {
    let parts = parse_field_path(path).map_err(|e| Error::InvalidInput {
        message: format!("Invalid field path `{}`: {}", path, e),
    })?;
    if let Some(root) = parts.first() {
        insert_ref_if_absent(refs, root.clone());
    }
    Ok(())
}

fn is_implicit_fts_candidate_type(data_type: &DataType) -> bool {
    match data_type {
        DataType::Utf8 | DataType::LargeUtf8 => true,
        DataType::List(inner) | DataType::LargeList(inner) => {
            matches!(inner.data_type(), DataType::Utf8 | DataType::LargeUtf8)
        }
        _ => false,
    }
}

fn validate_referenced_entries(
    snapshot: &GeneratedColumnBindingSnapshot,
    refs: &[String],
) -> Result<()> {
    for name in refs {
        let Some(entry) = snapshot.field(name) else {
            continue;
        };
        match entry.generated_column_definition()? {
            None => {}
            Some(definition) => match definition.status() {
                GeneratedColumnStatus::Complete => {}
                GeneratedColumnStatus::Incomplete => {
                    return Err(Error::Function {
                        code: FunctionErrorCode::GeneratedColumnIncomplete,
                        message: format!("generated column `{name}` is incomplete"),
                    });
                }
            },
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, sync::Arc};

    use super::*;
    use arrow::{array::downcast_array, compute::concat_batches, datatypes::Int32Type};
    use arrow_array::{
        FixedSizeListArray, Float32Array, Int32Array, RecordBatch, StringArray, cast::AsArray,
        types::Float32Type,
    };
    use arrow_schema::{DataType, Field as ArrowField, Schema as ArrowSchema};
    use futures::{StreamExt, TryStreamExt};
    use lance_testing::datagen::{BatchGenerator, IncrementingInt32, RandomVector};
    use rand::seq::IndexedRandom;
    use tempfile::tempdir;

    use crate::{Table, connect, database::CreateTableMode, index::Index};

    #[tokio::test]
    async fn test_setters_getters() {
        let batches = make_test_batches();
        let conn = connect("memory://foo").execute().await.unwrap();
        let table = conn
            .create_table("my_table", batches)
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
            .approx_mode(ApproxMode::Accurate)
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
        assert_eq!(query.request.approx_mode, Some(ApproxMode::Accurate));
        assert_eq!(query.request.refine_factor, Some(999));
    }

    #[test]
    fn test_approx_mode_serde_parse_default_and_display() {
        assert_eq!(ApproxMode::default(), ApproxMode::Normal);
        assert_eq!(
            serde_json::to_string(&ApproxMode::Fast).unwrap(),
            "\"fast\""
        );
        assert_eq!(
            serde_json::from_str::<ApproxMode>("\"accurate\"").unwrap(),
            ApproxMode::Accurate
        );
        assert_eq!("normal".parse::<ApproxMode>().unwrap(), ApproxMode::Normal);
        assert_eq!(ApproxMode::try_from("FAST").unwrap(), ApproxMode::Fast);
        assert_eq!(ApproxMode::Accurate.to_string(), "accurate");
        assert!(ApproxMode::try_from("invalid").is_err());
    }

    #[tokio::test]
    async fn test_vector_query_approx_mode_builder() {
        let tmp_dir = tempdir().unwrap();
        let dataset_path = tmp_dir.path().join("test.lance");
        let uri = dataset_path.to_str().unwrap();

        let conn = connect(uri).execute().await.unwrap();
        let table = conn
            .create_table("my_table", make_test_batches())
            .execute()
            .await
            .unwrap();

        let query = table
            .query()
            .nearest_to(&[0.1, 0.2])
            .unwrap()
            .approx_mode(ApproxMode::Fast);

        assert_eq!(query.request.approx_mode, Some(ApproxMode::Fast));
    }

    #[tokio::test]
    async fn test_execute() {
        let batches = make_non_empty_batches();
        let conn = connect("memory://foo").execute().await.unwrap();
        let table = conn
            .create_table("my_table", batches)
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
    async fn test_repeated_only_if_combines_with_and() {
        use crate::expr::{col, lit};

        let tmp_dir = tempdir().unwrap();
        let dataset_path = tmp_dir.path().join("test.lance");
        let uri = dataset_path.to_str().unwrap();

        let conn = connect(uri).execute().await.unwrap();
        let table = conn
            .create_table("my_table", make_non_empty_batches())
            .execute()
            .await
            .unwrap();

        let query = table.query().only_if("id > 0").only_if("id < 100");
        match &query.request.filter {
            Some(QueryFilter::Sql(sql)) => assert_eq!(sql, "(id > 0) AND (id < 100)"),
            other => panic!("expected combined SQL filter, got {other:?}"),
        }

        // A single filter is left untouched.
        let query = table.query().only_if("id > 0");
        match &query.request.filter {
            Some(QueryFilter::Sql(sql)) => assert_eq!(sql, "id > 0"),
            other => panic!("expected single SQL filter, got {other:?}"),
        }

        // Expression filters are combined with a logical AND as well.
        let query = table
            .query()
            .only_if_expr(col("id").gt(lit(0i32)))
            .only_if_expr(col("id").lt(lit(100i32)));
        match &query.request.filter {
            Some(QueryFilter::Datafusion(expr)) => {
                assert_eq!(
                    expr,
                    &col("id").gt(lit(0i32)).and(col("id").lt(lit(100i32)))
                );
            }
            other => panic!("expected combined Datafusion filter, got {other:?}"),
        }

        // Mixing an SQL string filter with an expression filter lowers the
        // expression to SQL and combines them as SQL strings.
        let query = table
            .query()
            .only_if("id > 0")
            .only_if_expr(col("id").lt(lit(100i32)));
        match &query.request.filter {
            Some(QueryFilter::Sql(sql)) => {
                let expected = format!(
                    "(id > 0) AND ({})",
                    crate::expr::expr_to_sql_string(&col("id").lt(lit(100i32))).unwrap()
                );
                assert_eq!(sql, &expected);
            }
            other => panic!("expected combined SQL filter, got {other:?}"),
        }
        assert!(query.request.check_filter().is_ok());
        // The combined filter executes without error.
        query.execute().await.unwrap();
    }

    #[tokio::test]
    async fn test_select_with_transform() {
        let batches = make_non_empty_batches();
        let conn = connect("memory://foo").execute().await.unwrap();
        let table = conn
            .create_table("my_table", batches)
            .execute()
            .await
            .unwrap();

        let query = table
            .query()
            .limit(10)
            .select(Select::dynamic(&[("id2", "id * 2"), ("id", "id")]));

        let schema = query.output_schema().await.unwrap();
        assert_eq!(
            schema,
            Arc::new(ArrowSchema::new(vec![
                ArrowField::new("id2", DataType::Int32, true),
                ArrowField::new("id", DataType::Int32, true),
            ]))
        );

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
    async fn test_select_with_expr_projection() {
        // Mirrors test_select_with_transform but uses Select::Expr instead of Select::Dynamic
        let tmp_dir = tempdir().unwrap();
        let dataset_path = tmp_dir.path().join("test_expr.lance");
        let uri = dataset_path.to_str().unwrap();

        let batches = make_non_empty_batches();
        let conn = connect(uri).execute().await.unwrap();
        let table = conn
            .create_table("my_table", batches)
            .execute()
            .await
            .unwrap();

        use crate::expr::{col, lit};
        let query = table.query().limit(10).select(Select::expr_projection(&[
            ("id2", col("id") * lit(2i32)),
            ("id", col("id")),
        ]));

        let schema = query.output_schema().await.unwrap();
        assert_eq!(
            schema,
            Arc::new(ArrowSchema::new(vec![
                ArrowField::new("id2", DataType::Int32, true),
                ArrowField::new("id", DataType::Int32, true),
            ]))
        );

        let result = query.execute().await;
        let mut batches = result
            .expect("should have result")
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(batches.len(), 1);
        let batch = batches.pop().unwrap();

        // id and id2
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
        // test that it's ok to not specify a query vector (just filter / limit)
        let batches = make_non_empty_batches();
        let conn = connect("memory://foo").execute().await.unwrap();
        let table = conn
            .create_table("my_table", batches)
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

    fn make_non_empty_batches() -> Box<dyn arrow_array::RecordBatchReader + Send> {
        let vec = Box::new(RandomVector::new().named("vector".to_string()));
        let id = Box::new(IncrementingInt32::new().named("id".to_string()));
        Box::new(BatchGenerator::new().col(vec).col(id).batch(512))
    }

    fn make_test_batches() -> RecordBatch {
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
        RecordBatch::new_empty(schema)
    }

    async fn make_test_table(tmp_dir: &tempfile::TempDir) -> Table {
        let dataset_path = tmp_dir.path().join("test.lance");
        let uri = dataset_path.to_str().unwrap();

        let batches = make_non_empty_batches();
        let conn = connect(uri).execute().await.unwrap();
        conn.create_table("my_table", batches)
            .execute()
            .await
            .unwrap()
    }

    async fn make_large_vector_table(tmp_dir: &tempfile::TempDir, rows: usize) -> Table {
        let dataset_path = tmp_dir.path().join("large_test.lance");
        let uri = dataset_path.to_str().unwrap();

        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("id", DataType::Utf8, false),
            ArrowField::new(
                "vector",
                DataType::FixedSizeList(
                    Arc::new(ArrowField::new("item", DataType::Float32, true)),
                    4,
                ),
                false,
            ),
        ]));

        let ids = StringArray::from_iter_values((0..rows).map(|i| format!("row-{i}")));
        let vectors = FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
            (0..rows).map(|i| Some(vec![Some(i as f32), Some(1.0), Some(2.0), Some(3.0)])),
            4,
        );
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(ids), Arc::new(vectors)]).unwrap();

        let conn = connect(uri).execute().await.unwrap();
        conn.create_table("my_table", vec![batch])
            .execute()
            .await
            .unwrap()
    }

    async fn assert_stream_batches_at_most(
        mut results: SendableRecordBatchStream,
        max_batch_length: usize,
    ) {
        let mut saw_batch = false;
        while let Some(batch) = results.next().await {
            let batch = batch.unwrap();
            saw_batch = true;
            assert!(batch.num_rows() <= max_batch_length);
        }
        assert!(saw_batch);
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
    async fn test_vector_query_execute_with_options_respects_max_batch_length() {
        let tmp_dir = tempdir().unwrap();
        let table = make_large_vector_table(&tmp_dir, 10_000).await;

        let results = table
            .query()
            .nearest_to(vec![0.0, 1.0, 2.0, 3.0])
            .unwrap()
            .limit(10_000)
            .execute_with_options(QueryExecutionOptions {
                max_batch_length: 100,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_stream_batches_at_most(results, 100).await;
    }

    #[tokio::test]
    async fn test_hybrid_query_execute_with_options_respects_max_batch_length() {
        let tmp_dir = tempdir().unwrap();
        let dataset_path = tmp_dir.path();
        let conn = connect(dataset_path.to_str().unwrap())
            .execute()
            .await
            .unwrap();

        let dims = 2;
        let rows = 512;
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

        let text = StringArray::from_iter_values((0..rows).map(|_| "match"));
        let vectors = FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
            (0..rows).map(|i| Some(vec![Some(i as f32), Some(0.0)])),
            dims,
        );
        let record_batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(text), Arc::new(vectors)]).unwrap();
        let table = conn
            .create_table("my_table", record_batch)
            .execute()
            .await
            .unwrap();

        table
            .create_index(&["text"], crate::index::Index::FTS(Default::default()))
            .replace(true)
            .execute()
            .await
            .unwrap();

        let results = table
            .query()
            .full_text_search(FullTextSearchQuery::new("match".to_string()))
            .limit(rows)
            .nearest_to(&[0.0, 0.0])
            .unwrap()
            .execute_with_options(QueryExecutionOptions {
                max_batch_length: 100,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_stream_batches_at_most(results, 100).await;
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
        assert!(
            error_result
                .err()
                .unwrap()
                .to_string()
                .contains("No vector column found to match with the query vector dimension: 3")
        );
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
        let table = conn
            .create_table("my_table", record_batch)
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
        let table = conn
            .create_table("my_table", record_batch)
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
        use rand::{Rng, rng};

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

    /// RED contract for the missing pure generated-column query reference guard.
    ///
    /// Production seam frozen here:
    /// `super::validate_generated_column_query(snapshot, query) -> Result<()>`
    mod generated_column_query {
        use std::sync::Arc;

        use arrow_array::{ArrayRef, Float32Array, StringArray};
        use arrow_schema::{DataType, Field, Fields};
        use datafusion_expr::{col, lit};
        use datafusion_functions::core::expr_ext::FieldAccessor;

        use super::*;
        use crate::error::FunctionErrorCode;
        use crate::function::{
            Function, FunctionArgument, FunctionCall, FunctionId, FunctionOutput,
            FunctionParameter, FunctionSignature, GENERATED_COLUMN_METADATA_KEY,
            GeneratedColumnBindingSnapshot, GeneratedColumnDefinition, GeneratedColumnStatus,
        };
        use crate::table::AnyQuery;

        const LITERAL_MARKER: &str = "SENSITIVE_QUERY_GUARD_LITERAL_b3e1_7c4a";
        const FUNCTION_ID: &str = "fn.query.guard.b3e1";
        const VECTOR_DIM_ORDINARY: i32 = 2;
        const VECTOR_DIM_GENERATED: i32 = 4;

        fn sample_function() -> Function {
            Function::new(
                FunctionId::try_new(FUNCTION_ID).unwrap(),
                FunctionSignature::try_new(
                    vec![FunctionParameter::new("label", DataType::Utf8)],
                    FunctionOutput::new(DataType::Int32, true),
                )
                .unwrap(),
            )
        }

        fn sample_call() -> FunctionCall {
            FunctionCall::try_new(
                &sample_function(),
                vec![(
                    "label".to_string(),
                    FunctionArgument::try_literal(Arc::new(StringArray::from(vec![Some(
                        LITERAL_MARKER,
                    )])) as ArrayRef)
                    .unwrap(),
                )],
            )
            .unwrap()
        }

        fn definition(
            output_field_id: i32,
            dependency_epoch: u64,
            materialized_epoch: u64,
        ) -> GeneratedColumnDefinition {
            GeneratedColumnDefinition::try_new(
                output_field_id,
                sample_call(),
                dependency_epoch,
                materialized_epoch,
            )
            .unwrap()
        }

        fn call_carries_literal_marker(call: &FunctionCall) -> bool {
            call.arguments().iter().any(|(_, arg)| {
                arg.literal_array()
                    .map(|arr| {
                        arr.as_any()
                            .downcast_ref::<StringArray>()
                            .map(|s| s.value(0) == LITERAL_MARKER)
                            .unwrap_or(false)
                    })
                    .unwrap_or(false)
            })
        }

        fn vector_dtype(dim: i32) -> DataType {
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), dim)
        }

        fn struct_dtype() -> DataType {
            DataType::Struct(Fields::from(vec![Field::new(
                "child",
                DataType::Int32,
                true,
            )]))
        }

        fn ordinary(name: &str, data_type: DataType) -> Field {
            Field::new(name, data_type, true)
        }

        fn generated(
            name: &str,
            data_type: DataType,
            field_id: i32,
            dependency_epoch: u64,
            materialized_epoch: u64,
        ) -> Field {
            // Canonical FunctionArgument::Literal is Arrow IPC/base64 in the
            // strict metadata JSON, so raw JSON is not expected to contain the
            // plaintext LITERAL_MARKER. Prove the typed literal via round-trip.
            let json = definition(field_id, dependency_epoch, materialized_epoch)
                .to_metadata_json()
                .unwrap();
            let decoded = GeneratedColumnDefinition::from_metadata_json(&json, field_id)
                .expect("canonical fixture metadata must strict-decode");
            assert!(
                call_carries_literal_marker(decoded.function_call()),
                "fixture metadata must round-trip typed literal marker via strict decode"
            );
            Field::new(name, data_type, true)
                .with_metadata([(GENERATED_COLUMN_METADATA_KEY.to_string(), json)].into())
        }

        fn malformed(name: &str, field_id: i32) -> Field {
            // Deliberately invalid payload may embed the marker in plaintext.
            let raw = format!(
                r#"{{"format_version":1,"output_field_id":{field_id},"function_call":{LITERAL_MARKER},"dependency_epoch":1,"materialized_epoch":1}}"#
            );
            assert!(raw.contains(LITERAL_MARKER));
            Field::new(name, DataType::Int32, true)
                .with_metadata([(GENERATED_COLUMN_METADATA_KEY.to_string(), raw)].into())
        }

        fn snapshot_from(fields_and_ids: Vec<(Field, i32)>) -> GeneratedColumnBindingSnapshot {
            let (fields, ids): (Vec<_>, Vec<_>) = fields_and_ids
                .into_iter()
                .map(|(field, id)| (Arc::new(field) as _, id))
                .unzip();
            GeneratedColumnBindingSnapshot::try_new(7, fields, ids).unwrap()
        }

        /// Ordinary fields plus complete and incomplete generated outputs of
        /// several shapes used by the compact reference matrix.
        fn mixed_snapshot() -> GeneratedColumnBindingSnapshot {
            snapshot_from(vec![
                (ordinary("id", DataType::Int32), 1),
                (ordinary("text", DataType::Utf8), 2),
                (ordinary("vector", vector_dtype(VECTOR_DIM_ORDINARY)), 3),
                (generated("gen_complete", DataType::Int32, 4, 3, 3), 4),
                (generated("gen_incomplete", DataType::Int32, 5, 4, 2), 5),
                (generated("gen_struct", struct_dtype(), 6, 5, 1), 6),
                (generated("gen_text", DataType::Utf8, 7, 6, 2), 7),
                (
                    generated("gen_vector", vector_dtype(VECTOR_DIM_GENERATED), 8, 7, 3),
                    8,
                ),
                (ordinary("Score", DataType::Int32), 9),
                (generated("a.b", DataType::Int32, 10, 8, 1), 10),
                (generated("Weird Name", DataType::Int32, 11, 9, 4), 11),
            ])
        }

        fn all_complete_snapshot() -> GeneratedColumnBindingSnapshot {
            snapshot_from(vec![
                (ordinary("id", DataType::Int32), 1),
                (ordinary("text", DataType::Utf8), 2),
                (generated("gen_complete", DataType::Int32, 4, 3, 3), 4),
                (generated("gen_other", DataType::Utf8, 5, 2, 2), 5),
            ])
        }

        fn malformed_snapshot() -> GeneratedColumnBindingSnapshot {
            snapshot_from(vec![
                (ordinary("id", DataType::Int32), 1),
                (ordinary("text", DataType::Utf8), 2),
                (malformed("gen_bad", 12), 12),
                (generated("gen_incomplete", DataType::Int32, 5, 4, 2), 5),
            ])
        }

        fn list_of(inner: DataType) -> DataType {
            DataType::List(Arc::new(Field::new("item", inner, true)))
        }

        fn large_list_of(inner: DataType) -> DataType {
            DataType::LargeList(Arc::new(Field::new("item", inner, true)))
        }

        /// Dedicated snapshot with ordinary columns plus exactly one generated
        /// field. Used so implicit-FTS assertions cannot pass by diagnosing a
        /// different incomplete field.
        fn dedicated_generated_snapshot(
            name: &str,
            data_type: DataType,
            field_id: i32,
            complete: bool,
        ) -> GeneratedColumnBindingSnapshot {
            let (dependency_epoch, materialized_epoch) = if complete { (3, 3) } else { (4, 2) };
            snapshot_from(vec![
                (ordinary("id", DataType::Int32), 1),
                (ordinary("text", DataType::Utf8), 2),
                (
                    generated(
                        name,
                        data_type,
                        field_id,
                        dependency_epoch,
                        materialized_epoch,
                    ),
                    field_id,
                ),
            ])
        }

        fn implicit_fts_query() -> AnyQuery {
            plain(QueryRequest {
                select: Select::columns(&["id"]),
                full_text_search: Some(FullTextSearchQuery::new("hello".into())),
                ..QueryRequest::default()
            })
        }

        fn plain(request: QueryRequest) -> AnyQuery {
            AnyQuery::Query(request)
        }

        fn query_with(select: Select) -> AnyQuery {
            AnyQuery::Query(QueryRequest {
                select,
                ..QueryRequest::default()
            })
        }

        fn vector_query(column: Option<&str>, dim: i32) -> AnyQuery {
            let mut request = VectorQueryRequest::from_plain_query(QueryRequest {
                select: Select::columns(&["id"]),
                ..QueryRequest::default()
            });
            request.column = column.map(|c| c.to_string());
            request.query_vector = vec![Arc::new(Float32Array::from(vec![0.1; dim as usize]))];
            AnyQuery::VectorQuery(request)
        }

        /// Diagnostics must not leak definition payload: decoded literal marker,
        /// plain Function ID, metadata wire key, or any snapshot field's full
        /// raw metadata JSON (canonical IPC/base64 or deliberately malformed).
        fn assert_diagnostic_redacted(
            rendered: &str,
            snapshot: &GeneratedColumnBindingSnapshot,
            label: &str,
        ) {
            assert!(
                !rendered.contains(LITERAL_MARKER),
                "{label}: diagnostic leaked typed literal marker: {rendered}"
            );
            assert!(
                !rendered.contains(FUNCTION_ID),
                "{label}: diagnostic leaked function id: {rendered}"
            );
            assert!(
                !rendered.contains(GENERATED_COLUMN_METADATA_KEY),
                "{label}: diagnostic leaked metadata key: {rendered}"
            );
            assert!(
                !rendered.contains("function_call"),
                "{label}: diagnostic leaked function_call wire key / payload: {rendered}"
            );
            for entry in snapshot.entries() {
                if let Some(raw) = entry.field().metadata().get(GENERATED_COLUMN_METADATA_KEY) {
                    assert!(
                        !rendered.contains(raw.as_str()),
                        "{label}: diagnostic leaked raw metadata json: {rendered}"
                    );
                }
            }
        }

        fn assert_incomplete(
            snapshot: &GeneratedColumnBindingSnapshot,
            query: &AnyQuery,
            label: &str,
        ) {
            let err = super::super::validate_generated_column_query(snapshot, query)
                .expect_err(&format!("{label}: expected generated_column_incomplete"));
            match &err {
                Error::Function {
                    code: FunctionErrorCode::GeneratedColumnIncomplete,
                    message,
                } => {
                    let rendered = format!("{err}\n{err:?}\n{message}");
                    assert_diagnostic_redacted(&rendered, snapshot, label);
                }
                other => panic!(
                    "{label}: expected Error::Function(GeneratedColumnIncomplete), got {other:?}"
                ),
            }
        }

        fn assert_ok(snapshot: &GeneratedColumnBindingSnapshot, query: &AnyQuery, label: &str) {
            super::super::validate_generated_column_query(snapshot, query)
                .unwrap_or_else(|err| panic!("{label}: expected Ok, got {err:?}"));
        }

        fn assert_invalid_input_redacted(
            snapshot: &GeneratedColumnBindingSnapshot,
            query: &AnyQuery,
            label: &str,
        ) {
            let err = super::super::validate_generated_column_query(snapshot, query)
                .expect_err(&format!("{label}: expected InvalidInput"));
            assert!(
                matches!(err, Error::InvalidInput { .. }),
                "{label}: expected InvalidInput, got {err:?}"
            );
            let rendered = format!("{err}\n{err:?}");
            assert_diagnostic_redacted(&rendered, snapshot, label);
        }

        fn assert_incomplete_named(
            snapshot: &GeneratedColumnBindingSnapshot,
            query: &AnyQuery,
            expected_name: &str,
            label: &str,
        ) {
            let err = super::super::validate_generated_column_query(snapshot, query)
                .expect_err(&format!("{label}: expected generated_column_incomplete"));
            match &err {
                Error::Function {
                    code: FunctionErrorCode::GeneratedColumnIncomplete,
                    message,
                } => {
                    assert!(
                        message.contains(&format!("`{expected_name}`")),
                        "{label}: expected incomplete name `{expected_name}`, got {message}"
                    );
                    let rendered = format!("{err}\n{err:?}\n{message}");
                    assert_diagnostic_redacted(&rendered, snapshot, label);
                }
                other => panic!(
                    "{label}: expected Error::Function(GeneratedColumnIncomplete), got {other:?}"
                ),
            }
        }

        fn assert_same_error(
            snapshot: &GeneratedColumnBindingSnapshot,
            query: &AnyQuery,
            expected: &Error,
            label: &str,
        ) {
            let err = super::super::validate_generated_column_query(snapshot, query)
                .expect_err(&format!("{label}: expected Err"));
            assert_eq!(
                err.to_string(),
                expected.to_string(),
                "{label}: diagnostic must match the expected precedence winner"
            );
            let rendered = format!("{err}\n{err:?}");
            assert_diagnostic_redacted(&rendered, snapshot, label);
        }

        /// Repeat enough times that a per-call randomized set cannot look stable.
        const DETERMINISM_REPEATS: usize = 64;

        #[test]
        fn fixture_snapshot_exposes_complete_and_incomplete_definitions() {
            let snapshot = mixed_snapshot();
            let complete = snapshot
                .field("gen_complete")
                .unwrap()
                .generated_column_definition()
                .unwrap()
                .expect("complete metadata");
            assert_eq!(complete.output_field_id(), 4);
            assert_eq!(complete.status(), GeneratedColumnStatus::Complete);
            assert!(
                call_carries_literal_marker(complete.function_call()),
                "complete definition must carry typed literal marker"
            );

            let incomplete = snapshot
                .field("gen_incomplete")
                .unwrap()
                .generated_column_definition()
                .unwrap()
                .expect("incomplete metadata");
            assert_eq!(incomplete.output_field_id(), 5);
            assert_eq!(incomplete.status(), GeneratedColumnStatus::Incomplete);
            assert!(
                snapshot
                    .field("id")
                    .unwrap()
                    .generated_column_definition()
                    .unwrap()
                    .is_none()
            );
        }

        #[test]
        fn select_all_rejects_incomplete_and_allows_complete() {
            let mixed = mixed_snapshot();
            assert_incomplete(&mixed, &query_with(Select::All), "select_all_incomplete");

            let complete = all_complete_snapshot();
            assert_ok(&complete, &query_with(Select::All), "select_all_complete");
        }

        #[test]
        fn incomplete_rejected_in_supported_reference_positions() {
            let snapshot = mixed_snapshot();
            let cases: Vec<(&str, AnyQuery)> = vec![
                (
                    "select_columns",
                    query_with(Select::columns(&["id", "gen_incomplete"])),
                ),
                (
                    "select_nested_child_of_generated_struct",
                    query_with(Select::columns(&["gen_struct.child"])),
                ),
                (
                    "select_dynamic_sql",
                    query_with(Select::dynamic(&[("out", "gen_incomplete + 1")])),
                ),
                (
                    "select_expr_datafusion",
                    query_with(Select::expr_projection(&[(
                        "out",
                        col("gen_incomplete") + lit(1),
                    )])),
                ),
                (
                    "filter_sql",
                    plain(QueryRequest {
                        select: Select::columns(&["id"]),
                        filter: Some(QueryFilter::Sql("gen_incomplete > 0".into())),
                        ..QueryRequest::default()
                    }),
                ),
                (
                    "filter_datafusion",
                    plain(QueryRequest {
                        select: Select::columns(&["id"]),
                        filter: Some(QueryFilter::Datafusion(col("gen_incomplete").gt(lit(0)))),
                        ..QueryRequest::default()
                    }),
                ),
                (
                    "order_by",
                    plain(QueryRequest {
                        select: Select::columns(&["id"]),
                        order_by: Some(vec![ColumnOrdering::asc_nulls_last(
                            "gen_incomplete".to_string(),
                        )]),
                        ..QueryRequest::default()
                    }),
                ),
                (
                    "fts_explicit_generated_text",
                    plain(QueryRequest {
                        select: Select::columns(&["id"]),
                        full_text_search: Some(
                            FullTextSearchQuery::new("hello".into())
                                .with_column("gen_text".into())
                                .unwrap(),
                        ),
                        ..QueryRequest::default()
                    }),
                ),
                (
                    "vector_explicit_generated",
                    vector_query(Some("gen_vector"), VECTOR_DIM_GENERATED),
                ),
                (
                    "vector_inferred_generated",
                    // ordinary vector is dim=2; query dim=4 selects only gen_vector.
                    vector_query(None, VECTOR_DIM_GENERATED),
                ),
            ];

            for (label, query) in cases {
                assert_incomplete(&snapshot, &query, label);
            }
        }

        #[test]
        fn unreferenced_incomplete_does_not_block_ordinary_query() {
            let snapshot = mixed_snapshot();

            assert_ok(
                &snapshot,
                &plain(QueryRequest {
                    select: Select::columns(&["id", "text", "Score"]),
                    filter: Some(QueryFilter::Sql("id > 0".into())),
                    order_by: Some(vec![ColumnOrdering::asc_nulls_last("id".to_string())]),
                    ..QueryRequest::default()
                }),
                "ordinary_projection_filter_order",
            );

            assert_ok(
                &snapshot,
                &plain(QueryRequest {
                    select: Select::columns(&["id"]),
                    full_text_search: Some(
                        FullTextSearchQuery::new("hello".into())
                            .with_column("text".into())
                            .unwrap(),
                    ),
                    ..QueryRequest::default()
                }),
                "ordinary_fts_column",
            );

            assert_ok(
                &snapshot,
                &vector_query(Some("vector"), VECTOR_DIM_ORDINARY),
                "ordinary_vector_column",
            );

            // Output alias and string literal equal to the generated name are not
            // field references.
            assert_ok(
                &snapshot,
                &query_with(Select::dynamic(&[("gen_incomplete", "'gen_incomplete'")])),
                "alias_and_string_literal_equal_generated_name",
            );
            assert_ok(
                &snapshot,
                &plain(QueryRequest {
                    select: Select::columns(&["id"]),
                    filter: Some(QueryFilter::Sql("text = 'gen_incomplete'".into())),
                    ..QueryRequest::default()
                }),
                "predicate_string_literal_equal_generated_name",
            );
        }

        #[test]
        fn reference_resolution_follows_query_semantics() {
            let snapshot = mixed_snapshot();

            // Lance Planner SQL identifiers resolve case-insensitively, so this
            // is a real reference to gen_incomplete — not an invented guess.
            assert_incomplete(
                &snapshot,
                &plain(QueryRequest {
                    select: Select::columns(&["id"]),
                    filter: Some(QueryFilter::Sql("GEN_INCOMPLETE > 0".into())),
                    ..QueryRequest::default()
                }),
                "sql_case_insensitive_planner_reference",
            );

            // Nested SQL/DataFusion paths collect the top-level generated struct.
            assert_incomplete(
                &snapshot,
                &plain(QueryRequest {
                    select: Select::columns(&["id"]),
                    filter: Some(QueryFilter::Sql("gen_struct.child > 0".into())),
                    ..QueryRequest::default()
                }),
                "sql_nested_path",
            );
            assert_incomplete(
                &snapshot,
                &plain(QueryRequest {
                    select: Select::columns(&["id"]),
                    filter: Some(QueryFilter::Datafusion(
                        col("gen_struct").field("child").gt(lit(0)),
                    )),
                    ..QueryRequest::default()
                }),
                "datafusion_nested_path",
            );

            // Quoted special name is a structural identifier reference.
            assert_incomplete(
                &snapshot,
                &query_with(Select::columns(&["`Weird Name`"])),
                "quoted_special_name_columns",
            );
            assert_incomplete(
                &snapshot,
                &plain(QueryRequest {
                    select: Select::columns(&["id"]),
                    filter: Some(QueryFilter::Sql("`Weird Name` > 0".into())),
                    ..QueryRequest::default()
                }),
                "quoted_special_name_sql_filter",
            );

            // Top-level field literally named `a.b` is referenced only through a
            // quoted path. Bare `a.b` is nested path [a, b] and must not be treated
            // as snapshot.field("a.b") / substring matching.
            assert_incomplete(
                &snapshot,
                &query_with(Select::columns(&["`a.b`"])),
                "quoted_top_level_dotted_name",
            );
            assert_ok(
                &snapshot,
                &query_with(Select::columns(&["a.b"])),
                "bare_dotted_path_is_not_top_level_a_dot_b",
            );

            // order_by uses exact Lance schema.field lookup (case-sensitive).
            assert_ok(
                &snapshot,
                &plain(QueryRequest {
                    select: Select::columns(&["id"]),
                    order_by: Some(vec![ColumnOrdering::asc_nulls_last(
                        "GEN_INCOMPLETE".to_string(),
                    )]),
                    ..QueryRequest::default()
                }),
                "order_by_wrong_case_is_not_a_reference",
            );

            // Substring / containment must not invent references.
            assert_ok(
                &snapshot,
                &plain(QueryRequest {
                    select: Select::columns(&["id"]),
                    filter: Some(QueryFilter::Sql("text LIKE '%gen_incomplete%'".into())),
                    ..QueryRequest::default()
                }),
                "substring_in_string_predicate",
            );
        }

        #[test]
        fn malformed_metadata_fail_closed_when_visible_or_referenced() {
            let snapshot = malformed_snapshot();

            assert_invalid_input_redacted(
                &snapshot,
                &query_with(Select::All),
                "select_all_malformed",
            );
            assert_invalid_input_redacted(
                &snapshot,
                &query_with(Select::columns(&["gen_bad"])),
                "select_columns_malformed",
            );

            // Unreferenced malformed metadata must not be diagnosed merely because
            // it exists on the snapshot.
            assert_ok(
                &snapshot,
                &query_with(Select::columns(&["id", "text"])),
                "unreferenced_malformed_ignored",
            );
            assert_incomplete(
                &snapshot,
                &query_with(Select::columns(&["gen_incomplete"])),
                "referenced_incomplete_still_checked",
            );
        }

        #[test]
        fn incomplete_error_is_stable_and_redacted() {
            let snapshot = mixed_snapshot();
            let query = query_with(Select::columns(&["gen_incomplete"]));
            let err = super::super::validate_generated_column_query(&snapshot, &query)
                .expect_err("incomplete reference");
            match &err {
                Error::Function {
                    code: FunctionErrorCode::GeneratedColumnIncomplete,
                    message,
                } => {
                    // May identify the output column; must not leak definition payload.
                    let rendered = format!("{message}\n{err}\n{err:?}");
                    assert_diagnostic_redacted(&rendered, &snapshot, "incomplete_stable");
                    assert!(
                        !rendered.contains("SENSITIVE"),
                        "diagnostic leaked sensitive payload fragment: {rendered}"
                    );
                    let metadata_json = definition(5, 4, 2).to_metadata_json().unwrap();
                    assert!(
                        !rendered.contains(&metadata_json),
                        "diagnostic leaked reconstructed raw metadata json: {rendered}"
                    );
                }
                other => panic!("expected GeneratedColumnIncomplete, got {other:?}"),
            }
        }

        #[test]
        fn substrait_filter_is_not_supported_with_generated_metadata() {
            // Pure guard must not decode Substrait. Freeze NotSupported (never Ok)
            // whenever generated-column metadata is present on the snapshot.
            let snapshot = mixed_snapshot();
            let query = plain(QueryRequest {
                select: Select::columns(&["id"]),
                filter: Some(QueryFilter::Substrait(Arc::from([0u8, 1, 2, 3].as_slice()))),
                ..QueryRequest::default()
            });
            let err = super::super::validate_generated_column_query(&snapshot, &query)
                .expect_err("substrait with generated metadata");
            assert!(
                matches!(err, Error::NotSupported { .. }),
                "expected NotSupported for Substrait filter, got {err:?}"
            );
        }

        #[test]
        fn implicit_fts_without_columns_uses_text_shaped_generated_candidates() {
            // Lance resolves empty FTS columns from index metadata later. The
            // pure helper only has the snapshot, so empty columns must fail
            // closed on every top-level generated field whose Arrow type is an
            // implicit-FTS candidate shape (see lance scanner is_fts_indexable_field).
            let query = implicit_fts_query();
            assert!(
                query
                    .base()
                    .full_text_search
                    .as_ref()
                    .unwrap()
                    .columns()
                    .is_empty(),
                "fixture must use FTS with no explicit columns"
            );

            let incomplete_shapes: Vec<(&str, &str, DataType, i32)> = vec![
                ("incomplete_utf8", "gen_utf8", DataType::Utf8, 21),
                (
                    "incomplete_large_utf8",
                    "gen_large_utf8",
                    DataType::LargeUtf8,
                    22,
                ),
                (
                    "incomplete_list_utf8",
                    "gen_list_utf8",
                    list_of(DataType::Utf8),
                    23,
                ),
                (
                    "incomplete_list_large_utf8",
                    "gen_list_large_utf8",
                    list_of(DataType::LargeUtf8),
                    24,
                ),
                (
                    "incomplete_large_list_utf8",
                    "gen_large_list_utf8",
                    large_list_of(DataType::Utf8),
                    25,
                ),
                (
                    "incomplete_large_list_large_utf8",
                    "gen_large_list_large_utf8",
                    large_list_of(DataType::LargeUtf8),
                    26,
                ),
            ];
            for (label, name, data_type, field_id) in incomplete_shapes {
                let snapshot = dedicated_generated_snapshot(name, data_type, field_id, false);
                assert_eq!(
                    snapshot
                        .field(name)
                        .unwrap()
                        .generated_column_definition()
                        .unwrap()
                        .expect("generated")
                        .status(),
                    GeneratedColumnStatus::Incomplete
                );
                assert_incomplete(&snapshot, &query, label);
            }

            let complete_utf8 = dedicated_generated_snapshot("gen_utf8", DataType::Utf8, 31, true);
            assert_ok(&complete_utf8, &query, "complete_utf8_candidate_allowed");

            // Incomplete non-FTS generated type alone must not invent an FTS ref.
            let incomplete_int =
                dedicated_generated_snapshot("gen_incomplete_int", DataType::Int32, 32, false);
            assert_ok(
                &incomplete_int,
                &query,
                "incomplete_non_fts_type_does_not_block_implicit_fts",
            );

            // Explicit FTS columns keep exact structural behavior: ordinary
            // text is fine even when a dedicated incomplete text candidate exists.
            let incomplete_utf8 =
                dedicated_generated_snapshot("gen_utf8", DataType::Utf8, 33, false);
            assert_ok(
                &incomplete_utf8,
                &plain(QueryRequest {
                    select: Select::columns(&["id"]),
                    full_text_search: Some(
                        FullTextSearchQuery::new("hello".into())
                            .with_column("text".into())
                            .unwrap(),
                    ),
                    ..QueryRequest::default()
                }),
                "explicit_ordinary_fts_column_ignores_unreferenced_candidate",
            );
        }

        #[test]
        fn deferred_filter_composition_error_precedes_generated_incomplete() {
            // QueryRequest::check_filter is required before every backend query.
            // An incompatible Substrait+SQL composition records filter_error; the
            // guard must surface that existing InvalidInput before diagnosing an
            // incomplete generated selection.
            let snapshot =
                dedicated_generated_snapshot("gen_incomplete", DataType::Int32, 41, false);
            assert_incomplete(
                &snapshot,
                &query_with(Select::columns(&["gen_incomplete"])),
                "control_incomplete_selection",
            );

            let mut request = QueryRequest {
                select: Select::columns(&["gen_incomplete"]),
                filter: Some(QueryFilter::Substrait(Arc::from([0u8, 1, 2, 3].as_slice()))),
                ..QueryRequest::default()
            };
            request.add_filter(QueryFilter::Sql("id > 0".into()));
            assert!(
                request.filter_error.is_some(),
                "composition must record filter_error"
            );
            assert!(
                request.filter.is_none(),
                "failed composition leaves no executable filter"
            );
            let deferred = request
                .check_filter()
                .expect_err("check_filter must surface recorded composition error");
            assert!(
                matches!(deferred, Error::InvalidInput { .. }),
                "expected InvalidInput from check_filter, got {deferred:?}"
            );
            let deferred_message = deferred.to_string();
            assert!(
                deferred_message.contains("cannot combine a Substrait filter with another filter"),
                "unexpected deferred message: {deferred_message}"
            );

            let query = plain(request);
            let err = super::super::validate_generated_column_query(&snapshot, &query)
                .expect_err("expected deferred filter composition error");
            assert!(
                matches!(err, Error::InvalidInput { .. }),
                "filter_error must precede GeneratedColumnIncomplete, got {err:?}"
            );
            assert_eq!(
                err.to_string(),
                deferred_message,
                "guard must preserve the existing check_filter diagnostic"
            );
            let rendered = format!("{err}\n{err:?}");
            assert_diagnostic_redacted(&rendered, &snapshot, "filter_precedence");
            assert!(
                !matches!(
                    err,
                    Error::Function {
                        code: FunctionErrorCode::GeneratedColumnIncomplete,
                        ..
                    }
                ),
                "must not return GeneratedColumnIncomplete when filter_error is set"
            );
        }

        #[test]
        fn referenced_error_precedence_follows_deterministic_query_order() {
            // Freeze query traversal order for diagnostic selection. Reverse-
            // alphabetic / reverse-schema request order plus both directions
            // reject HashSet iteration (stable per process, arbitrary across
            // processes). Repeats reject any per-call randomized set.

            // Malformed: first referenced field's strict short InvalidInput wins.
            // Short diagnostics identify field id only (no raw metadata payload).
            let malformed = snapshot_from(vec![
                (ordinary("id", DataType::Int32), 1),
                (malformed("zz_bad", 61), 61),
                (malformed("aa_bad", 62), 62),
            ]);
            let zz_malformed_only = super::super::validate_generated_column_query(
                &malformed,
                &query_with(Select::columns(&["zz_bad"])),
            )
            .expect_err("solo zz_bad");
            let aa_malformed_only = super::super::validate_generated_column_query(
                &malformed,
                &query_with(Select::columns(&["aa_bad"])),
            )
            .expect_err("solo aa_bad");
            assert!(matches!(zz_malformed_only, Error::InvalidInput { .. }));
            assert!(matches!(aa_malformed_only, Error::InvalidInput { .. }));
            assert_ne!(
                zz_malformed_only.to_string(),
                aa_malformed_only.to_string(),
                "distinct field ids must yield distinguishable InvalidInput"
            );
            assert!(
                zz_malformed_only.to_string().contains("61")
                    && aa_malformed_only.to_string().contains("62"),
                "solo diagnostics must identify field ids; zz={} aa={}",
                zz_malformed_only,
                aa_malformed_only
            );

            for _ in 0..DETERMINISM_REPEATS {
                assert_same_error(
                    &malformed,
                    &query_with(Select::columns(&["zz_bad", "aa_bad"])),
                    &zz_malformed_only,
                    "select_malformed_zz_then_aa",
                );
                assert_same_error(
                    &malformed,
                    &query_with(Select::columns(&["aa_bad", "zz_bad"])),
                    &aa_malformed_only,
                    "select_malformed_aa_then_zz",
                );
                // Duplicates keep the first position.
                assert_same_error(
                    &malformed,
                    &query_with(Select::columns(&["zz_bad", "aa_bad", "zz_bad"])),
                    &zz_malformed_only,
                    "select_malformed_duplicate_keeps_first",
                );
            }

            // Incomplete: first referenced output name wins in the diagnostic.
            let incomplete = snapshot_from(vec![
                (ordinary("id", DataType::Int32), 1),
                (ordinary("text", DataType::Utf8), 2),
                (generated("zz_inc", DataType::Int32, 71, 4, 2), 71),
                (generated("aa_inc", DataType::Int32, 72, 4, 2), 72),
            ]);
            for _ in 0..DETERMINISM_REPEATS {
                assert_incomplete_named(
                    &incomplete,
                    &query_with(Select::columns(&["zz_inc", "aa_inc"])),
                    "zz_inc",
                    "select_incomplete_zz_then_aa",
                );
                assert_incomplete_named(
                    &incomplete,
                    &query_with(Select::columns(&["aa_inc", "zz_inc"])),
                    "aa_inc",
                    "select_incomplete_aa_then_zz",
                );
                assert_incomplete_named(
                    &incomplete,
                    &query_with(Select::columns(&["zz_inc", "aa_inc", "zz_inc"])),
                    "zz_inc",
                    "select_incomplete_duplicate_keeps_first",
                );

                // Select precedes filter / order / FTS / vector in traversal.
                assert_incomplete_named(
                    &incomplete,
                    &plain(QueryRequest {
                        select: Select::columns(&["aa_inc"]),
                        filter: Some(QueryFilter::Sql("zz_inc > 0".into())),
                        ..QueryRequest::default()
                    }),
                    "aa_inc",
                    "select_before_filter_incomplete",
                );
                assert_incomplete_named(
                    &incomplete,
                    &plain(QueryRequest {
                        select: Select::columns(&["id"]),
                        filter: Some(QueryFilter::Sql("zz_inc > 0".into())),
                        order_by: Some(vec![ColumnOrdering::asc_nulls_last("aa_inc".to_string())]),
                        ..QueryRequest::default()
                    }),
                    "zz_inc",
                    "filter_before_order_by_incomplete",
                );
            }

            // Select::All uses snapshot schema order (deliberately reverse-alpha).
            let all_zz_first = snapshot_from(vec![
                (ordinary("id", DataType::Int32), 1),
                (generated("zz_inc", DataType::Int32, 81, 4, 2), 81),
                (generated("aa_inc", DataType::Int32, 82, 4, 2), 82),
            ]);
            let all_aa_first = snapshot_from(vec![
                (ordinary("id", DataType::Int32), 1),
                (generated("aa_inc", DataType::Int32, 83, 4, 2), 83),
                (generated("zz_inc", DataType::Int32, 84, 4, 2), 84),
            ]);
            for _ in 0..DETERMINISM_REPEATS {
                assert_incomplete_named(
                    &all_zz_first,
                    &query_with(Select::All),
                    "zz_inc",
                    "select_all_schema_order_zz_first",
                );
                assert_incomplete_named(
                    &all_aa_first,
                    &query_with(Select::All),
                    "aa_inc",
                    "select_all_schema_order_aa_first",
                );
            }
        }

        #[test]
        fn invalid_select_field_path_is_invalid_input() {
            // Invalid exact field path must surface the same fail-closed
            // InvalidInput the query path uses for parse_field_path failures.
            let snapshot = all_complete_snapshot();
            assert!(
                snapshot.entries().iter().any(|entry| {
                    entry
                        .field()
                        .metadata()
                        .contains_key(GENERATED_COLUMN_METADATA_KEY)
                }),
                "fixture must carry generated metadata"
            );
            for _ in 0..DETERMINISM_REPEATS {
                assert_invalid_input_redacted(
                    &snapshot,
                    &query_with(Select::columns(&["parent..child"])),
                    "invalid_select_field_path",
                );
            }
        }

        #[test]
        fn ambiguous_default_vector_column_error_is_preserved() {
            // Ambiguous implicit vector column: propagate the existing
            // default_vector_column diagnostic. Snapshot has generated metadata
            // but no incomplete referenced output, so Ok would mean the guard
            // swallowed inference failure rather than diagnosing incomplete.
            let snapshot = snapshot_from(vec![
                (ordinary("id", DataType::Int32), 1),
                (ordinary("vector_a", vector_dtype(VECTOR_DIM_ORDINARY)), 2),
                (ordinary("vector_b", vector_dtype(VECTOR_DIM_ORDINARY)), 3),
                (generated("gen_complete", DataType::Int32, 4, 3, 3), 4),
            ]);
            let arrow_schema = arrow_schema::Schema::new(
                snapshot
                    .entries()
                    .iter()
                    .map(|entry| entry.field().as_ref().clone())
                    .collect::<Vec<_>>(),
            );
            let expected =
                crate::utils::default_vector_column(&arrow_schema, Some(VECTOR_DIM_ORDINARY))
                    .expect_err("fixture must be ambiguous for default_vector_column");
            assert!(
                expected.to_string().contains("More than one"),
                "unexpected default_vector_column diagnostic: {expected}"
            );
            let query = vector_query(None, VECTOR_DIM_ORDINARY);
            for _ in 0..DETERMINISM_REPEATS {
                let err = super::super::validate_generated_column_query(&snapshot, &query)
                    .expect_err("ambiguous vector inference must not become Ok");
                assert_eq!(
                    err.to_string(),
                    expected.to_string(),
                    "guard must preserve default_vector_column ambiguity diagnostic"
                );
                assert!(
                    !matches!(
                        err,
                        Error::Function {
                            code: FunctionErrorCode::GeneratedColumnIncomplete,
                            ..
                        }
                    ),
                    "must not disguise inference failure as GeneratedColumnIncomplete"
                );
                let rendered = format!("{err}\n{err:?}");
                assert_diagnostic_redacted(&rendered, &snapshot, "ambiguous_vector");
            }
        }
    }
}
