// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Lazy, immutable DataFrames for composing local or remote queries.
//!
//! Create a DataFrame from an opened [`crate::Table`], apply DataFusion-style
//! transformations, and call [`DataFrame::execute`] to submit the query. Query
//! planning and transport serialization are handled by LanceDB.
//!
//! ```
//! use lancedb::expr::{col, lit};
//! use lancedb::Table;
//!
//! # async fn query(table: &Table) -> lancedb::Result<()> {
//! let query = table
//!     .to_df()
//!     .await?
//!     .filter(col("id").gt(lit(10_i64)))?
//!     .limit(10, 0)?
//!     .execute()
//!     .await?;
//! let _results = query.reader().await?;
//! # Ok(())
//! # }
//! ```

use std::{
    sync::{
        Arc, LazyLock, Mutex,
        atomic::{AtomicU8, Ordering},
    },
    task::Poll,
};

use arrow_schema::Schema;
use async_trait::async_trait;
use datafusion::{
    dataframe::DataFrame as DfDataFrame,
    datasource::provider_as_source,
    execution::context::SessionContext,
    logical_expr::{JoinType as DfJoinType, LogicalPlanBuilder},
};
use datafusion_common::{Column, TableReference};
use datafusion_expr::{Expr, SortExpr};
use datafusion_functions_aggregate::expr_fn::{avg, count, max, min, sum};
use datafusion_substrait::logical_plan::producer::to_substrait_plan;
use futures::{
    Stream, TryStreamExt,
    stream::{AbortHandle, abortable, poll_fn},
};
use prost::Message;
use uuid::Uuid;

#[cfg(test)]
use datafusion_catalog::empty::EmptyTable;

use crate::{
    Error, Result,
    arrow::{SendableRecordBatchStream, SimpleRecordBatchStream},
    database::Database,
    sql::{Query, QueryDescription, QueryHandle, QueryStatus},
    table::{BaseTable, datafusion::BaseTableAdapter},
};

static DATAFRAME_SESSION: LazyLock<SessionContext> = LazyLock::new(SessionContext::new);

fn planning_error(error: impl std::fmt::Display) -> Error {
    Error::InvalidInput {
        message: error.to_string(),
    }
}

fn execution_error(error: impl std::fmt::Display) -> Error {
    Error::Runtime {
        message: error.to_string(),
    }
}

/// Join behavior supported by the language-neutral DataFrame planner.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JoinType {
    /// Keep rows whose keys match on both sides.
    Inner,
    /// Keep all left rows and matching right rows.
    Left,
    /// Keep matching left rows and all right rows.
    Right,
    /// Keep all rows from both sides.
    Full,
    /// Keep left rows that have a match, without right columns.
    LeftSemi,
    /// Keep right rows that have a match, without left columns.
    RightSemi,
    /// Keep left rows that have no match.
    LeftAnti,
    /// Keep right rows that have no match.
    RightAnti,
}

impl From<JoinType> for DfJoinType {
    fn from(value: JoinType) -> Self {
        match value {
            JoinType::Inner => Self::Inner,
            JoinType::Left => Self::Left,
            JoinType::Right => Self::Right,
            JoinType::Full => Self::Full,
            JoinType::LeftSemi => Self::LeftSemi,
            JoinType::RightSemi => Self::RightSemi,
            JoinType::LeftAnti => Self::LeftAnti,
            JoinType::RightAnti => Self::RightAnti,
        }
    }
}

struct EncodedPlan {
    bytes: Vec<u8>,
    version: String,
}

#[derive(Clone)]
struct ExecutionContext {
    database: Arc<dyn Database>,
    default_namespace_path: Vec<String>,
    execute_remotely: bool,
}

struct LocalQueryHandle {
    id: Uuid,
    stream: Mutex<Option<SendableRecordBatchStream>>,
    abort_handle: AbortHandle,
    status: Arc<AtomicU8>,
}

struct LocalStreamState {
    status: Arc<AtomicU8>,
    terminal: bool,
}

impl LocalStreamState {
    fn new(status: Arc<AtomicU8>) -> Self {
        Self {
            status,
            terminal: false,
        }
    }

    fn finish(&mut self) {
        let _ = self
            .status
            .compare_exchange(0, 1, Ordering::SeqCst, Ordering::SeqCst);
        self.terminal = true;
    }
}

impl Drop for LocalStreamState {
    fn drop(&mut self) {
        if !self.terminal {
            let _ = self
                .status
                .compare_exchange(0, 2, Ordering::SeqCst, Ordering::SeqCst);
        }
    }
}

fn prepare_local_stream(
    stream: datafusion_physical_plan::SendableRecordBatchStream,
) -> (SendableRecordBatchStream, AbortHandle, Arc<AtomicU8>) {
    let schema = stream.schema();
    let stream = stream.map_err(execution_error);
    let (stream, abort_handle) = abortable(stream);
    let status = Arc::new(AtomicU8::new(0));
    let mut stream_state = LocalStreamState::new(status.clone());
    let mut stream = Box::pin(stream);
    let stream = poll_fn(move |context| {
        if stream_state.terminal {
            return Poll::Ready(None);
        }

        match stream.as_mut().poll_next(context) {
            Poll::Ready(Some(Err(error))) => {
                stream_state.finish();
                Poll::Ready(Some(Err(error)))
            }
            Poll::Ready(None) if stream_state.status.load(Ordering::SeqCst) == 2 => {
                stream_state.terminal = true;
                Poll::Ready(Some(Err(Error::Runtime {
                    message: "DataFrame query was cancelled".to_string(),
                })))
            }
            Poll::Ready(None) => {
                stream_state.finish();
                Poll::Ready(None)
            }
            other => other,
        }
    });
    (
        Box::pin(SimpleRecordBatchStream::new(stream, schema)),
        abort_handle,
        status,
    )
}

impl LocalQueryHandle {
    fn new(
        stream: SendableRecordBatchStream,
        abort_handle: AbortHandle,
        status: Arc<AtomicU8>,
    ) -> Self {
        Self {
            id: Uuid::now_v7(),
            stream: Mutex::new(Some(stream)),
            abort_handle,
            status,
        }
    }

    fn status(&self) -> QueryStatus {
        match self.status.load(Ordering::SeqCst) {
            1 => QueryStatus::Finished,
            2 => QueryStatus::Cancelled,
            _ => QueryStatus::Running,
        }
    }
}

#[async_trait]
impl QueryHandle for LocalQueryHandle {
    fn id(&self) -> Uuid {
        self.id
    }

    async fn describe(&self) -> Result<QueryDescription> {
        let status = self.status();
        Ok(QueryDescription {
            id: self.id,
            status,
            progress: (status == QueryStatus::Finished).then_some(1.0),
            expires_at: None,
        })
    }

    async fn reader(&self) -> Result<SendableRecordBatchStream> {
        if self.abort_handle.is_aborted() {
            return Err(Error::Runtime {
                message: "DataFrame query was cancelled".to_string(),
            });
        }
        self.stream
            .lock()
            .unwrap()
            .take()
            .ok_or_else(|| Error::Runtime {
                message: "DataFrame query results can only be consumed once".to_string(),
            })
    }

    async fn cancel(&self) -> Result<()> {
        if self
            .status
            .compare_exchange(0, 2, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            self.abort_handle.abort();
            self.stream.lock().unwrap().take();
        }
        Ok(())
    }
}

/// An immutable, DataFusion-style logical query plan.
#[derive(Clone)]
pub struct DataFrame {
    inner: DfDataFrame,
    execution: Option<ExecutionContext>,
}

impl DataFrame {
    fn wrap(&self, result: datafusion_common::Result<DfDataFrame>) -> Result<Self> {
        result
            .map(|inner| Self {
                inner,
                execution: self.execution.clone(),
            })
            .map_err(planning_error)
    }

    #[cfg(test)]
    fn from_table(name: impl Into<String>, schema: Arc<Schema>) -> Result<Self> {
        Self::from_table_with_execution(name, schema, None)
    }

    pub(crate) async fn from_open_table(
        name: impl Into<String>,
        table: Arc<dyn BaseTable>,
        database: Arc<dyn Database>,
        namespace: &[String],
    ) -> Result<Self> {
        let default_namespace_path = if namespace.is_empty() {
            vec!["public".to_string()]
        } else {
            namespace.to_vec()
        };
        let source = provider_as_source(Arc::new(BaseTableAdapter::try_new(table).await?));
        let execute_remotely = database.executes_dataframe_remotely();
        let plan = LogicalPlanBuilder::scan(TableReference::bare(name.into()), source, None)
            .and_then(LogicalPlanBuilder::build)
            .map_err(planning_error)?;
        Ok(Self {
            inner: DfDataFrame::new(DATAFRAME_SESSION.state(), plan),
            execution: Some(ExecutionContext {
                database,
                default_namespace_path,
                execute_remotely,
            }),
        })
    }

    #[cfg(test)]
    fn from_table_with_execution(
        name: impl Into<String>,
        schema: Arc<Schema>,
        execution: Option<ExecutionContext>,
    ) -> Result<Self> {
        let source = provider_as_source(Arc::new(EmptyTable::new(schema)));
        let plan = LogicalPlanBuilder::scan(TableReference::bare(name.into()), source, None)
            .and_then(LogicalPlanBuilder::build)
            .map_err(planning_error)?;
        Ok(Self {
            inner: DfDataFrame::new(DATAFRAME_SESSION.state(), plan),
            execution,
        })
    }

    /// Project expressions into a new DataFrame.
    pub fn select(&self, expressions: Vec<Expr>) -> Result<Self> {
        self.wrap(self.inner.clone().select(expressions))
    }

    /// Keep rows matching a predicate.
    pub fn filter(&self, predicate: Expr) -> Result<Self> {
        self.wrap(self.inner.clone().filter(predicate))
    }

    /// Group rows and calculate aggregate expressions.
    pub fn aggregate(&self, groups: Vec<Expr>, aggregates: Vec<Expr>) -> Result<Self> {
        self.wrap(self.inner.clone().aggregate(groups, aggregates))
    }

    /// Sort by expressions expressed as `(expression, ascending, nulls_first)`.
    pub fn sort(&self, expressions: Vec<(Expr, bool, bool)>) -> Result<Self> {
        let expressions: Vec<SortExpr> = expressions
            .into_iter()
            .map(|(expr, ascending, nulls_first)| expr.sort(ascending, nulls_first))
            .collect();
        self.wrap(self.inner.clone().sort(expressions))
    }

    /// Limit the result to `count` rows after `offset` rows.
    pub fn limit(&self, count: usize, offset: usize) -> Result<Self> {
        self.wrap(self.inner.clone().limit(offset, Some(count)))
    }

    /// Remove duplicate rows.
    pub fn distinct(&self) -> Result<Self> {
        self.wrap(self.inner.clone().distinct())
    }

    /// Assign a relation alias, typically before a self join.
    pub fn alias(&self, name: impl Into<String>) -> Result<Self> {
        let name = name.into();
        self.wrap(self.inner.clone().alias(&name))
    }

    /// Resolve a literal field name to a relation-qualified expression.
    pub fn column(&self, name: &str) -> Result<Expr> {
        let field = self
            .inner
            .schema()
            .qualified_field_with_unqualified_name(name)
            .map_err(planning_error)?;
        Ok(Expr::Column(Column::from(field)))
    }

    /// Add or replace a column.
    pub fn with_column(&self, name: &str, expression: Expr) -> Result<Self> {
        self.wrap(self.inner.clone().with_column(name, expression))
    }

    /// Drop literal field names, rejecting missing or ambiguous fields.
    pub fn drop_columns(&self, columns: &[String]) -> Result<Self> {
        let columns = columns
            .iter()
            .map(|name| {
                self.inner
                    .schema()
                    .qualified_field_with_unqualified_name(name)
                    .map(Column::from)
                    .map_err(planning_error)
            })
            .collect::<Result<Vec<_>>>()?;
        self.wrap(self.inner.clone().drop_columns(&columns))
    }

    /// Rename a literal field while retaining its relation qualifier.
    pub fn with_column_renamed(&self, old_name: &str, new_name: &str) -> Result<Self> {
        let old_column = self
            .inner
            .schema()
            .qualified_field_with_unqualified_name(old_name)
            .map(Column::from)
            .map_err(planning_error)?;
        let projection = self
            .inner
            .schema()
            .iter()
            .map(|(qualifier, field)| {
                let column = Column::new(qualifier.cloned(), field.name());
                let expression = Expr::Column(column.clone());
                if column == old_column {
                    expression.alias_qualified(qualifier.cloned(), new_name)
                } else {
                    expression
                }
            })
            .collect::<Vec<_>>();
        self.wrap(self.inner.clone().select(projection))
    }

    /// Join two plans using corresponding equality keys.
    pub fn join(
        &self,
        other: &Self,
        left_on: &[String],
        right_on: &[String],
        how: JoinType,
    ) -> Result<Self> {
        if left_on.len() != right_on.len() {
            return Err(Error::InvalidInput {
                message: "left and right join key counts must match".to_string(),
            });
        }
        if left_on.is_empty() {
            return Err(Error::InvalidInput {
                message: "join requires at least one key".to_string(),
            });
        }
        self.validate_execution_context(other)?;
        let predicates = left_on
            .iter()
            .zip(right_on)
            .map(|(left, right)| Ok(self.column(left)?.eq(other.column(right)?)))
            .collect::<Result<Vec<_>>>()?;
        self.wrap(
            self.inner
                .clone()
                .join_on(other.inner.clone(), how.into(), predicates),
        )
    }

    /// Union two compatible plans, preserving duplicates when `all` is true.
    pub fn union(&self, other: &Self, all: bool) -> Result<Self> {
        self.validate_execution_context(other)?;
        if all {
            self.wrap(self.inner.clone().union(other.inner.clone()))
        } else {
            self.wrap(self.inner.clone().union_distinct(other.inner.clone()))
        }
    }

    /// Intersect two compatible plans, preserving duplicates when `all` is true.
    pub fn intersect(&self, other: &Self, all: bool) -> Result<Self> {
        self.validate_execution_context(other)?;
        if all {
            self.wrap(self.inner.clone().intersect(other.inner.clone()))
        } else {
            self.wrap(self.inner.clone().intersect_distinct(other.inner.clone()))
        }
    }

    /// Subtract a compatible plan, preserving duplicates when `all` is true.
    pub fn except(&self, other: &Self, all: bool) -> Result<Self> {
        self.validate_execution_context(other)?;
        if all {
            self.wrap(self.inner.clone().except(other.inner.clone()))
        } else {
            self.wrap(self.inner.clone().except_distinct(other.inner.clone()))
        }
    }

    /// Return the current output Arrow schema.
    pub fn schema(&self) -> Schema {
        self.inner.schema().as_arrow().clone()
    }

    fn encode_plan(&self) -> Result<EncodedPlan> {
        let (state, logical_plan) = self.inner.clone().into_parts();
        let plan = to_substrait_plan(&logical_plan, &state).map_err(planning_error)?;
        let version = plan
            .version
            .as_ref()
            .map(|version| {
                format!(
                    "{}.{}.{}",
                    version.major_number, version.minor_number, version.patch_number
                )
            })
            .ok_or_else(|| Error::Runtime {
                message: "generated Substrait plan has no version".to_string(),
            })?;
        Ok(EncodedPlan {
            bytes: plan.encode_to_vec(),
            version,
        })
    }

    /// Submit this plan and return its query lifecycle handle.
    ///
    /// Local tables execute in-process. Remote tables serialize the logical
    /// plan and submit it to the remote query service.
    pub async fn execute(&self) -> Result<crate::sql::Query> {
        let execution = self.execution.as_ref().ok_or_else(|| Error::InvalidInput {
            message: "this DataFrame is not bound to an opened table".to_string(),
        })?;
        if execution.execute_remotely {
            let plan = self.encode_plan()?;
            execution
                .database
                .execute_dataframe(
                    &plan.bytes,
                    &plan.version,
                    &execution.default_namespace_path,
                )
                .await
        } else {
            let stream = self
                .inner
                .clone()
                .execute_stream()
                .await
                .map_err(execution_error)?;
            let (stream, abort_handle, status) = prepare_local_stream(stream);
            Ok(Query::new(Arc::new(LocalQueryHandle::new(
                stream,
                abort_handle,
                status,
            ))))
        }
    }

    /// Render the logical plan for diagnostics.
    pub fn display(&self) -> String {
        self.inner.logical_plan().display_indent().to_string()
    }

    fn validate_execution_context(&self, other: &Self) -> Result<()> {
        match (&self.execution, &other.execution) {
            (None, None) => Ok(()),
            (Some(left), Some(right))
                if Arc::ptr_eq(&left.database, &right.database)
                    && left.default_namespace_path == right.default_namespace_path =>
            {
                Ok(())
            }
            _ => Err(Error::InvalidInput {
                message: "DataFrames must come from the same connection and namespace".to_string(),
            }),
        }
    }
}

/// Build a `SUM` aggregate expression.
pub fn aggregate_sum(expr: Expr) -> Expr {
    sum(expr)
}

/// Build an `AVG` aggregate expression.
pub fn aggregate_avg(expr: Expr) -> Expr {
    avg(expr)
}

/// Build a `MIN` aggregate expression.
pub fn aggregate_min(expr: Expr) -> Expr {
    min(expr)
}

/// Build a `MAX` aggregate expression.
pub fn aggregate_max(expr: Expr) -> Expr {
    max(expr)
}

/// Build a `COUNT` aggregate expression.
pub fn aggregate_count(expr: Expr) -> Expr {
    count(expr)
}

#[cfg(test)]
mod tests {
    use arrow_array::{Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field};
    use futures::TryStreamExt;

    use super::*;
    use crate::{
        connect,
        expr::{col, lit},
    };

    fn events() -> DataFrame {
        DataFrame::from_table(
            "events",
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("value", DataType::Int64, false),
            ])),
        )
        .unwrap()
    }

    #[test]
    fn builds_and_serializes_an_immutable_plan() {
        let frame = events()
            .filter(col("value").gt(lit(5_i64)))
            .unwrap()
            .select(vec![col("id"), col("value")])
            .unwrap()
            .sort(vec![(col("value"), false, true)])
            .unwrap()
            .limit(10, 2)
            .unwrap();

        assert_eq!(frame.schema().fields().len(), 2);
        let encoded = frame.encode_plan().unwrap();
        assert!(!encoded.bytes.is_empty());
        assert!(!encoded.version.is_empty());
    }

    #[test]
    fn qualified_renames_survive_aliased_self_joins() {
        let source = events();
        let left = source
            .alias("left")
            .unwrap()
            .with_column_renamed("value", "renamed")
            .unwrap();
        let right = source
            .alias("right")
            .unwrap()
            .with_column_renamed("value", "renamed")
            .unwrap();
        let joined = left
            .join(
                &right,
                &["id".to_string()],
                &["id".to_string()],
                JoinType::Inner,
            )
            .unwrap()
            .select(vec![
                left.column("renamed").unwrap().alias("left_value"),
                right.column("renamed").unwrap().alias("right_value"),
            ])
            .unwrap();

        assert_eq!(joined.schema().fields().len(), 2);
        assert!(!joined.encode_plan().unwrap().bytes.is_empty());
    }

    #[test]
    fn dotted_names_are_literal_and_missing_names_error() {
        let frame = DataFrame::from_table(
            "dotted",
            Arc::new(Schema::new(vec![Field::new(
                "left.value",
                DataType::Int64,
                true,
            )])),
        )
        .unwrap();

        assert!(frame.column("left.value").is_ok());
        assert_eq!(
            frame
                .with_column_renamed("left.value", "value")
                .unwrap()
                .schema()
                .field(0)
                .name(),
            "value"
        );
        assert!(frame.drop_columns(&["left.value".to_string()]).is_ok());
        assert!(frame.drop_columns(&["missing".to_string()]).is_err());

        let right = DataFrame::from_table(
            "right",
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)])),
        )
        .unwrap();
        let joined = frame
            .join(
                &right,
                &["left.value".to_string()],
                &["id".to_string()],
                JoinType::Inner,
            )
            .unwrap();
        assert!(!joined.encode_plan().unwrap().bytes.is_empty());
    }

    #[tokio::test]
    async fn executes_local_plan_in_process() {
        let directory = tempfile::tempdir().unwrap();
        let database = connect(directory.path().to_str().unwrap())
            .execute()
            .await
            .unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1_i64, 2, 3]))])
                .unwrap();
        let table = database
            .create_table("events", batch)
            .execute()
            .await
            .unwrap();

        let query = table
            .to_df()
            .await
            .unwrap()
            .filter(col("id").gt(lit(1_i64)))
            .unwrap()
            .execute()
            .await
            .unwrap();
        assert_eq!(query.describe().await.unwrap().status, QueryStatus::Running);
        let batches: Vec<RecordBatch> = query.reader().await.unwrap().try_collect().await.unwrap();
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
        assert_eq!(
            query.describe().await.unwrap().status,
            QueryStatus::Finished
        );
        assert!(query.reader().await.is_err());

        let cancelled = table.to_df().await.unwrap().execute().await.unwrap();
        cancelled.cancel().await.unwrap();
        assert_eq!(
            cancelled.describe().await.unwrap().status,
            QueryStatus::Cancelled
        );
        assert!(cancelled.reader().await.is_err());

        let cancelled = table.to_df().await.unwrap().execute().await.unwrap();
        let mut reader = cancelled.reader().await.unwrap();
        cancelled.cancel().await.unwrap();
        assert!(reader.try_next().await.is_err());
        assert_eq!(
            cancelled.describe().await.unwrap().status,
            QueryStatus::Cancelled
        );

        let abandoned = table.to_df().await.unwrap().execute().await.unwrap();
        let reader = abandoned.reader().await.unwrap();
        drop(reader);
        assert_eq!(
            abandoned.describe().await.unwrap().status,
            QueryStatus::Cancelled
        );
    }

    #[tokio::test]
    async fn local_stream_error_is_terminal() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let stream = futures::stream::iter([Err(datafusion_common::DataFusionError::Execution(
            "broken stream".to_string(),
        ))]);
        let stream = Box::pin(
            datafusion_physical_plan::stream::RecordBatchStreamAdapter::new(schema, stream),
        );
        let (mut stream, _abort_handle, status) = prepare_local_stream(stream);

        assert!(stream.try_next().await.is_err());
        assert_eq!(status.load(Ordering::SeqCst), 1);
    }
}
