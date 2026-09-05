// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Lazy, immutable DataFrames for composing local or remote queries.
//!
//! Create a DataFrame from an opened [`crate::Table`], apply DataFusion-style
//! transformations, and call [`DataFrame::execute`] to submit the query. Query
//! planning and execution are handled by LanceDB.
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
use futures::{
    Stream, TryStreamExt,
    stream::{AbortHandle, abortable, poll_fn},
};
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

mod sql;

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

#[derive(Clone)]
struct ExecutionContext {
    database: Arc<dyn Database>,
    default_namespace_path: Vec<String>,
    execute_as_sql: bool,
    source_tables: Vec<Arc<dyn BaseTable>>,
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

    fn fail(&mut self) {
        let _ = self
            .status
            .compare_exchange(0, 3, Ordering::SeqCst, Ordering::SeqCst);
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
                stream_state.fail();
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
            3 => QueryStatus::Failed,
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
///
/// Remote execution resolves source tables by name through the SQL query
/// service. It does not inherit per-table read-consistency intervals or
/// read-your-write freshness fences from the table handles used to build the
/// plan.
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
        let source = provider_as_source(Arc::new(BaseTableAdapter::try_new(table.clone()).await?));
        let execute_as_sql = database.execute_dataframe_as_sql();
        let plan = LogicalPlanBuilder::scan(TableReference::bare(name.into()), source, None)
            .and_then(LogicalPlanBuilder::build)
            .map_err(planning_error)?;
        Ok(Self {
            inner: DfDataFrame::new(DATAFRAME_SESSION.state(), plan),
            execution: Some(ExecutionContext {
                database,
                default_namespace_path,
                execute_as_sql,
                source_tables: vec![table],
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
    ///
    /// For remote SQL execution, apply sorting after filters and aliases. A
    /// single final projection directly after sorting is supported, but
    /// ordering through an intervening limit or multiple projections cannot
    /// yet be preserved.
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
    ///
    /// Remote SQL execution currently rejects a transformed plan containing a
    /// join when it is used as another join input. It also rejects operations
    /// that must isolate a join result whose columns retain multiple relation
    /// qualifiers; select uniquely aliased output columns first.
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
        let joined = self
            .inner
            .clone()
            .join_on(other.inner.clone(), how.into(), predicates)
            .map_err(planning_error)?;
        let projection: Vec<_> = joined
            .schema()
            .iter()
            .map(|(qualifier, field)| Expr::Column(Column::new(qualifier.cloned(), field.name())))
            .collect();
        self.wrap_binary(other, joined.select(projection))
    }

    /// Union two compatible plans, preserving duplicates when `all` is true.
    pub fn union(&self, other: &Self, all: bool) -> Result<Self> {
        self.validate_execution_context(other)?;
        if all {
            self.wrap_binary(other, self.inner.clone().union(other.inner.clone()))
        } else {
            self.wrap_binary(
                other,
                self.inner.clone().union_distinct(other.inner.clone()),
            )
        }
    }

    /// Intersect two compatible plans using DataFusion set semantics.
    ///
    /// When `all` is true, duplicate rows from the left plan are retained when
    /// a matching row exists in the right plan. When false, results are distinct.
    pub fn intersect(&self, other: &Self, all: bool) -> Result<Self> {
        self.validate_execution_context(other)?;
        if all {
            self.wrap_binary(other, self.inner.clone().intersect(other.inner.clone()))
        } else {
            self.wrap_binary(
                other,
                self.inner.clone().intersect_distinct(other.inner.clone()),
            )
        }
    }

    /// Subtract a compatible plan using DataFusion set semantics.
    ///
    /// When `all` is true, duplicate rows from the left plan are retained when
    /// no matching row exists in the right plan. When false, results are distinct.
    pub fn except(&self, other: &Self, all: bool) -> Result<Self> {
        self.validate_execution_context(other)?;
        if all {
            self.wrap_binary(other, self.inner.clone().except(other.inner.clone()))
        } else {
            self.wrap_binary(
                other,
                self.inner.clone().except_distinct(other.inner.clone()),
            )
        }
    }

    /// Return the current output Arrow schema.
    pub fn schema(&self) -> Schema {
        self.inner.schema().as_arrow().clone()
    }

    fn to_sql(&self) -> Result<String> {
        sql::plan_to_sql(self.inner.logical_plan()).map_err(|error| match error {
            datafusion_common::DataFusionError::NotImplemented(message) => {
                Error::NotSupported { message }
            }
            error => planning_error(error),
        })
    }

    /// Submit this plan and return its query lifecycle handle.
    ///
    /// Local tables execute in-process. Remote tables convert the logical plan
    /// to SQL and submit it through the remote query service. Remote execution
    /// uses the latest revisions visible to that service at submission time;
    /// per-table read-consistency intervals and freshness fences do not carry
    /// over to the SQL request.
    pub async fn execute(&self) -> Result<crate::sql::Query> {
        let execution = self.execution.as_ref().ok_or_else(|| Error::InvalidInput {
            message: "this DataFrame is not bound to an opened table".to_string(),
        })?;
        if execution
            .source_tables
            .iter()
            .any(|table| table.current_branch().is_some() || table.is_time_travel())
        {
            return Err(Error::NotSupported {
                message: "DataFrames do not yet support checked-out versions or branches"
                    .to_string(),
            });
        }
        if execution.execute_as_sql {
            let query = self.to_sql()?;
            execution
                .database
                .execute_query_async(&query, &execution.default_namespace_path)
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
                    && left.execute_as_sql == right.execute_as_sql
                    && (!left.execute_as_sql
                        || left.default_namespace_path == right.default_namespace_path) =>
            {
                Ok(())
            }
            _ => Err(Error::InvalidInput {
                message: "DataFrames must come from the same connection and namespace".to_string(),
            }),
        }
    }

    fn wrap_binary(
        &self,
        other: &Self,
        result: datafusion_common::Result<DfDataFrame>,
    ) -> Result<Self> {
        result
            .map(|inner| {
                let mut execution = self.execution.clone();
                if let (Some(execution), Some(other_execution)) = (&mut execution, &other.execution)
                {
                    for table in &other_execution.source_tables {
                        if !execution
                            .source_tables
                            .iter()
                            .any(|source| Arc::ptr_eq(source, table))
                        {
                            execution.source_tables.push(table.clone());
                        }
                    }
                }
                Self { inner, execution }
            })
            .map_err(planning_error)
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
    use std::collections::HashMap;

    use arrow_array::{Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field};
    use futures::TryStreamExt;
    use lance_namespace::{
        LanceNamespace,
        models::{
            CreateNamespaceRequest, CreateNamespaceResponse, DescribeNamespaceRequest,
            DescribeNamespaceResponse, DropNamespaceRequest, DropNamespaceResponse,
            ListNamespacesRequest, ListNamespacesResponse, ListTablesRequest, ListTablesResponse,
        },
    };

    use super::*;
    use crate::{
        connect,
        database::{
            CloneTableRequest, CreateTableRequest, OpenTableRequest, ReadConsistency,
            TableNamesRequest,
        },
        expr::{col, expr_cast, lit},
    };

    #[derive(Debug)]
    struct RecordingSqlDatabase {
        inner: Arc<dyn Database>,
        submitted: Mutex<Option<(String, Vec<String>)>>,
        sql_available: bool,
    }

    impl std::fmt::Display for RecordingSqlDatabase {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("RecordingSqlDatabase")
        }
    }

    #[async_trait]
    impl Database for RecordingSqlDatabase {
        fn uri(&self) -> &str {
            self.inner.uri()
        }

        async fn read_consistency(&self) -> Result<ReadConsistency> {
            self.inner.read_consistency().await
        }

        async fn list_namespaces(
            &self,
            request: ListNamespacesRequest,
        ) -> Result<ListNamespacesResponse> {
            self.inner.list_namespaces(request).await
        }

        async fn create_namespace(
            &self,
            request: CreateNamespaceRequest,
        ) -> Result<CreateNamespaceResponse> {
            self.inner.create_namespace(request).await
        }

        async fn drop_namespace(
            &self,
            request: DropNamespaceRequest,
        ) -> Result<DropNamespaceResponse> {
            self.inner.drop_namespace(request).await
        }

        async fn describe_namespace(
            &self,
            request: DescribeNamespaceRequest,
        ) -> Result<DescribeNamespaceResponse> {
            self.inner.describe_namespace(request).await
        }

        #[allow(deprecated)]
        async fn table_names(&self, request: TableNamesRequest) -> Result<Vec<String>> {
            self.inner.table_names(request).await
        }

        async fn list_tables(&self, request: ListTablesRequest) -> Result<ListTablesResponse> {
            self.inner.list_tables(request).await
        }

        async fn create_table(&self, request: CreateTableRequest) -> Result<Arc<dyn BaseTable>> {
            self.inner.create_table(request).await
        }

        async fn clone_table(&self, request: CloneTableRequest) -> Result<Arc<dyn BaseTable>> {
            self.inner.clone_table(request).await
        }

        async fn execute_query_async(
            &self,
            query: &str,
            default_namespace_path: &[String],
        ) -> Result<Query> {
            *self.submitted.lock().unwrap() =
                Some((query.to_string(), default_namespace_path.to_vec()));
            if self.sql_available {
                Err(Error::Runtime {
                    message: "query recorded".to_string(),
                })
            } else {
                Err(Error::NotSupported {
                    message: "SQL is unavailable".to_string(),
                })
            }
        }

        fn execute_dataframe_as_sql(&self) -> bool {
            true
        }

        async fn open_table(&self, request: OpenTableRequest) -> Result<Arc<dyn BaseTable>> {
            self.inner.open_table(request).await
        }

        async fn rename_table(
            &self,
            current_name: &str,
            new_name: &str,
            current_namespace_path: &[String],
            new_namespace_path: &[String],
        ) -> Result<()> {
            self.inner
                .rename_table(
                    current_name,
                    new_name,
                    current_namespace_path,
                    new_namespace_path,
                )
                .await
        }

        async fn drop_table(&self, name: &str, namespace_path: &[String]) -> Result<()> {
            self.inner.drop_table(name, namespace_path).await
        }

        async fn drop_all_tables(&self, namespace_path: &[String]) -> Result<()> {
            self.inner.drop_all_tables(namespace_path).await
        }

        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        async fn namespace_client(&self) -> Result<Arc<dyn LanceNamespace>> {
            self.inner.namespace_client().await
        }

        async fn namespace_client_config(&self) -> Result<(String, HashMap<String, String>)> {
            self.inner.namespace_client_config().await
        }
    }

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

    #[tokio::test]
    async fn namespace_restriction_only_applies_to_remote_sql() {
        let directory = tempfile::tempdir().unwrap();
        let database = connect(directory.path().to_str().unwrap())
            .execute()
            .await
            .unwrap()
            .database()
            .clone();
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let execution = |namespace: &str, execute_as_sql| ExecutionContext {
            database: database.clone(),
            default_namespace_path: vec![namespace.to_string()],
            execute_as_sql,
            source_tables: vec![],
        };

        let local_left = DataFrame::from_table_with_execution(
            "left",
            schema.clone(),
            Some(execution("first", false)),
        )
        .unwrap();
        let local_right = DataFrame::from_table_with_execution(
            "right",
            schema.clone(),
            Some(execution("second", false)),
        )
        .unwrap();
        assert!(local_left.union(&local_right, true).is_ok());

        let remote_left = DataFrame::from_table_with_execution(
            "left",
            schema.clone(),
            Some(execution("first", true)),
        )
        .unwrap();
        let remote_right =
            DataFrame::from_table_with_execution("right", schema, Some(execution("second", true)))
                .unwrap();
        assert!(matches!(
            remote_left.union(&remote_right, true),
            Err(Error::InvalidInput { .. })
        ));
    }

    #[test]
    fn builds_and_renders_an_immutable_plan() {
        let frame = DataFrame::from_table(
            "users",
            Arc::new(Schema::new(vec![
                Field::new("name", DataType::Utf8, false),
                Field::new("age", DataType::Int64, false),
            ])),
        )
        .unwrap()
        .filter(col("age").gt(lit(18_i64)))
        .unwrap()
        .select(vec![col("name"), col("age")])
        .unwrap();

        assert_eq!(frame.schema().fields().len(), 2);
        assert_eq!(
            frame.to_sql().unwrap(),
            "SELECT users.\"name\", users.age FROM users WHERE (users.age > 18)"
        );
    }

    #[test]
    fn renders_relational_operations_as_sql() {
        let aggregate = events()
            .filter(col("value").gt(lit(5_i64)))
            .unwrap()
            .aggregate(
                vec![col("id")],
                vec![aggregate_sum(col("value")).alias("total")],
            )
            .unwrap()
            .sort(vec![(col("total"), false, true)])
            .unwrap()
            .limit(10, 2)
            .unwrap();
        let aggregate_sql = aggregate.to_sql().unwrap();
        assert!(
            aggregate_sql.to_ascii_uppercase().contains("SUM"),
            "unexpected aggregate SQL: {aggregate_sql}"
        );
        assert!(aggregate_sql.contains("GROUP BY"));
        assert!(aggregate_sql.contains("ORDER BY"));
        assert!(aggregate_sql.contains("LIMIT 10"));
        assert!(aggregate_sql.contains("OFFSET 2"));
        let first_from = aggregate_sql.find(" FROM ").unwrap();
        assert!(
            !aggregate_sql[..first_from]
                .to_ascii_lowercase()
                .contains("sum("),
            "aggregate SQL output must match DataFrame schema order: {aggregate_sql}"
        );

        let distinct = events()
            .select(vec![col("value")])
            .unwrap()
            .distinct()
            .unwrap()
            .to_sql()
            .unwrap();
        assert!(distinct.starts_with("SELECT DISTINCT"));

        let expressions = events()
            .select(vec![
                expr_cast(col("id"), DataType::Float64).alias("id_float"),
                col("value").is_null().alias("value_is_null"),
                (col("value") + lit(1_i64)).alias("next_value"),
            ])
            .unwrap()
            .to_sql()
            .unwrap();
        assert!(expressions.contains("CAST"));
        assert!(expressions.contains("IS NULL"));
        assert!(expressions.contains("+ 1"));
    }

    #[tokio::test]
    async fn generated_sql_preserves_projection_and_aggregate_semantics() {
        let batch = RecordBatch::try_new(
            Arc::new(events().schema()),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
                Arc::new(Int64Array::from(vec![40, 10, 20, 30])),
            ],
        )
        .unwrap();
        let ctx = SessionContext::new();
        ctx.register_batch("events", batch).unwrap();

        let projected_sort_sql = events()
            .sort(vec![(col("value"), true, false)])
            .unwrap()
            .select(vec![col("id")])
            .unwrap()
            .limit(2, 0)
            .unwrap()
            .to_sql()
            .unwrap();
        let projected = ctx
            .sql(&projected_sort_sql)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let ids = projected[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ids.values(), &[2, 3]);

        let unsupported_orderings = [
            events()
                .sort(vec![(col("id"), true, false)])
                .unwrap()
                .with_column("value", col("value") * lit(2_i64))
                .unwrap()
                .filter(col("value").gt(lit(50_i64)))
                .unwrap()
                .limit(2, 0)
                .unwrap(),
            events()
                .sort(vec![(col("value"), true, false)])
                .unwrap()
                .select(vec![col("id"), col("value")])
                .unwrap()
                .select(vec![col("id")])
                .unwrap()
                .limit(2, 0)
                .unwrap(),
            events()
                .sort(vec![(col("value"), true, false)])
                .unwrap()
                .select(vec![col("id")])
                .unwrap()
                .alias("ordered")
                .unwrap()
                .limit(2, 0)
                .unwrap(),
            events()
                .sort(vec![(col("value"), true, false)])
                .unwrap()
                .limit(3, 0)
                .unwrap()
                .limit(2, 1)
                .unwrap(),
            events()
                .sort(vec![(col("value"), true, false)])
                .unwrap()
                .limit(3, 0)
                .unwrap()
                .select(vec![col("id")])
                .unwrap(),
        ];
        for frame in unsupported_orderings {
            assert!(matches!(
                frame.to_sql(),
                Err(Error::NotSupported { message })
                    if message.contains("apply sort after those transformations")
            ));
        }

        let distinct_sql = events()
            .sort(vec![(col("value"), true, false)])
            .unwrap()
            .select(vec![col("id")])
            .unwrap()
            .distinct()
            .unwrap()
            .to_sql()
            .unwrap();
        let distinct = ctx
            .sql(&distinct_sql)
            .await
            .unwrap_or_else(|error| panic!("invalid distinct SQL {distinct_sql}: {error}"))
            .collect()
            .await
            .unwrap();
        assert_eq!(distinct.iter().map(RecordBatch::num_rows).sum::<usize>(), 4);

        let aggregate_sql = events()
            .aggregate(
                vec![col("id")],
                vec![aggregate_sum(col("value")).alias("total")],
            )
            .unwrap()
            .sort(vec![(col("total"), false, true)])
            .unwrap()
            .limit(2, 0)
            .unwrap()
            .to_sql()
            .unwrap();
        let aggregate = ctx
            .sql(&aggregate_sql)
            .await
            .unwrap_or_else(|error| panic!("invalid aggregate SQL {aggregate_sql}: {error}"))
            .collect()
            .await
            .unwrap();
        let totals = aggregate[0]
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(totals.values(), &[40, 30]);
    }

    #[test]
    fn preserves_limit_and_set_operation_scopes_in_sql() {
        let limited = events().limit(2, 0).unwrap();

        let filtered_sql = limited
            .filter(col("value").gt(lit(1_i64)))
            .unwrap()
            .to_sql()
            .unwrap();
        assert!(
            filtered_sql.find("LIMIT 2").unwrap() < filtered_sql.find("WHERE").unwrap(),
            "limit must remain inside the filtered input: {filtered_sql}"
        );

        let sorted_sql = limited
            .sort(vec![(col("value"), true, false)])
            .unwrap()
            .to_sql()
            .unwrap();
        assert!(
            sorted_sql.find("LIMIT 2").unwrap() < sorted_sql.find("ORDER BY").unwrap(),
            "limit must remain inside the sorted input: {sorted_sql}"
        );

        let distinct_sql = limited.distinct().unwrap().to_sql().unwrap();
        assert!(
            distinct_sql.contains("FROM (SELECT") && distinct_sql.contains("LIMIT 2"),
            "limit must remain inside the distinct input: {distinct_sql}"
        );

        let projected_sql = limited.select(vec![col("id")]).unwrap().to_sql().unwrap();
        assert!(
            projected_sql.contains("LIMIT 2") && projected_sql.contains(") AS events"),
            "a projected limited input must have a usable relation alias: {projected_sql}"
        );

        let projected_sort_sql = events()
            .sort(vec![(col("value"), true, false)])
            .unwrap()
            .select(vec![col("id")])
            .unwrap()
            .limit(2, 0)
            .unwrap()
            .to_sql()
            .unwrap();
        assert!(
            projected_sort_sql.find("ORDER BY").unwrap()
                < projected_sort_sql.find("LIMIT 2").unwrap()
                && !projected_sort_sql.contains("FROM (SELECT"),
            "projection must preserve observable sort order: {projected_sort_sql}"
        );

        for input in [
            events().distinct().unwrap(),
            events()
                .with_column("next_value", col("value") + lit(1_i64))
                .unwrap(),
        ] {
            let sql = input.select(vec![col("id")]).unwrap().to_sql().unwrap();
            assert!(
                sql.contains("FROM (SELECT") && sql.contains(") AS events"),
                "projected compound input must retain a relation alias: {sql}"
            );
        }

        let unqualified_sql = events()
            .select(vec![col("id").alias("renamed")])
            .unwrap()
            .limit(2, 0)
            .unwrap()
            .filter(col("renamed").gt(lit(0_i64)))
            .unwrap()
            .to_sql()
            .unwrap();
        assert!(
            unqualified_sql.find("LIMIT 2").unwrap() < unqualified_sql.find("WHERE").unwrap(),
            "unqualified derived inputs must retain their scope: {unqualified_sql}"
        );

        let renamed_filter_sql = events()
            .select(vec![col("id").alias("renamed")])
            .unwrap()
            .filter(col("renamed").gt(lit(0_i64)))
            .unwrap()
            .to_sql()
            .unwrap();
        assert!(
            renamed_filter_sql.contains("FROM (SELECT")
                && renamed_filter_sql.contains(") AS __lancedb_filter_input WHERE"),
            "filter must not reference an alias in the same select block: {renamed_filter_sql}"
        );

        let nested_aggregate_sql = events()
            .aggregate(
                vec![col("id")],
                vec![aggregate_sum(col("value")).alias("total")],
            )
            .unwrap()
            .aggregate(
                vec![],
                vec![aggregate_sum(col("total")).alias("grand_total")],
            )
            .unwrap()
            .to_sql()
            .unwrap();
        assert!(
            nested_aggregate_sql
                .to_ascii_uppercase()
                .matches("SUM(")
                .count()
                == 2
                && nested_aggregate_sql.contains("GROUP BY"),
            "nested aggregate must retain both aggregation scopes: {nested_aggregate_sql}"
        );

        let chained_limit_sql = events()
            .limit(10, 0)
            .unwrap()
            .limit(5, 2)
            .unwrap()
            .to_sql()
            .unwrap();
        assert_eq!(
            chained_limit_sql.matches("LIMIT").count(),
            2,
            "both chained limits must be retained: {chained_limit_sql}"
        );

        let chained_sort_sql = events()
            .sort(vec![(col("id"), true, false)])
            .unwrap()
            .sort(vec![(col("value"), false, true)])
            .unwrap()
            .to_sql()
            .unwrap();
        assert_eq!(
            chained_sort_sql.matches("ORDER BY").count(),
            2,
            "both chained sorts must retain their scopes: {chained_sort_sql}"
        );

        let right = DataFrame::from_table(
            "other_events",
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("value", DataType::Int64, false),
            ])),
        )
        .unwrap();
        let union_sql = limited.union(&right, true).unwrap().to_sql().unwrap();
        assert!(
            union_sql.find("LIMIT 2").unwrap() < union_sql.find("UNION ALL").unwrap(),
            "limit must apply only to the left union input: {union_sql}"
        );

        for all in [true, false] {
            let filtered_union_sql = events()
                .union(&right, all)
                .unwrap()
                .filter(col("id").gt(lit(1_i64)))
                .unwrap()
                .to_sql()
                .unwrap();
            assert!(
                filtered_union_sql.find("UNION").unwrap()
                    < filtered_union_sql.find("WHERE").unwrap(),
                "filter must apply outside the union: {filtered_union_sql}"
            );
        }

        let mixed_union_sql = events()
            .union(&right, false)
            .unwrap()
            .union(&right, true)
            .unwrap()
            .to_sql()
            .unwrap();
        assert_eq!(
            mixed_union_sql.matches("UNION ALL").count(),
            1,
            "outer union-all must not inherit inner distinctness: {mixed_union_sql}"
        );

        let intersect_sql = limited.intersect(&right, true).unwrap().to_sql().unwrap();
        assert!(
            intersect_sql.find("LIMIT 2").unwrap() < intersect_sql.find("EXISTS").unwrap(),
            "limit must apply before the intersection: {intersect_sql}"
        );

        let nested_set_sql = events()
            .union(&right, true)
            .unwrap()
            .intersect(&right, true)
            .unwrap()
            .to_sql()
            .unwrap();
        assert!(
            nested_set_sql.find("UNION ALL").unwrap() < nested_set_sql.find("EXISTS").unwrap(),
            "nested union must remain inside the intersection input: {nested_set_sql}"
        );
    }

    #[test]
    fn renders_joins_and_set_operations_as_sql() {
        let left = events().alias("left").unwrap();
        let right = DataFrame::from_table(
            "other_events",
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("value", DataType::Int64, false),
            ])),
        )
        .unwrap()
        .alias("right")
        .unwrap();

        for (join_type, expected) in [
            (JoinType::Inner, " JOIN "),
            (JoinType::Left, " LEFT OUTER JOIN "),
            (JoinType::Right, " RIGHT OUTER JOIN "),
            (JoinType::Full, " FULL JOIN "),
            (JoinType::LeftSemi, " WHERE EXISTS "),
            (JoinType::RightSemi, " WHERE EXISTS "),
            (JoinType::LeftAnti, " WHERE NOT EXISTS "),
            (JoinType::RightAnti, " WHERE NOT EXISTS "),
        ] {
            let sql = left
                .join(&right, &["id".to_string()], &["id".to_string()], join_type)
                .unwrap()
                .to_sql()
                .unwrap();
            assert!(
                sql.to_ascii_uppercase().contains(expected),
                "expected {expected} in SQL: {sql}"
            );
            assert!(
                !sql.starts_with("SELECT *, *"),
                "join columns must be projected explicitly: {sql}"
            );
        }

        let filtered_left = left
            .filter(left.column("value").unwrap().gt(lit(1_i64)))
            .unwrap();
        let filtered_right = right
            .filter(right.column("value").unwrap().lt(lit(100_i64)))
            .unwrap();
        for (join_type, expected_join) in [
            (JoinType::Left, " LEFT OUTER JOIN "),
            (JoinType::Right, " RIGHT OUTER JOIN "),
            (JoinType::Full, " FULL JOIN "),
            (JoinType::LeftSemi, " WHERE EXISTS "),
            (JoinType::RightSemi, " WHERE EXISTS "),
            (JoinType::LeftAnti, " WHERE NOT EXISTS "),
            (JoinType::RightAnti, " WHERE NOT EXISTS "),
        ] {
            let sql = filtered_left
                .join(
                    &filtered_right,
                    &["id".to_string()],
                    &["id".to_string()],
                    join_type,
                )
                .unwrap()
                .to_sql()
                .unwrap();
            let uppercase = sql.to_ascii_uppercase();
            let join_position = uppercase
                .find(expected_join)
                .unwrap_or_else(|| panic!("expected {expected_join} in filtered join SQL: {sql}"));
            let left_filter_position = sql
                .find("> 1")
                .unwrap_or_else(|| panic!("left input filter was lost: {sql}"));
            let right_filter_position = sql
                .find("< 100")
                .unwrap_or_else(|| panic!("right input filter was lost: {sql}"));
            if matches!(join_type, JoinType::Left | JoinType::Right | JoinType::Full) {
                let on_position = uppercase.rfind(" ON ").unwrap();
                assert!(
                    left_filter_position < join_position,
                    "left input filter must remain inside its join input: {sql}"
                );
                assert!(
                    right_filter_position < on_position,
                    "right input filter must remain inside its join input: {sql}"
                );
            }
        }

        let union_all = left.union(&right, true).unwrap().to_sql().unwrap();
        assert!(
            union_all.contains("UNION ALL"),
            "unexpected SQL: {union_all}"
        );

        let union_distinct = left.union(&right, false).unwrap().to_sql().unwrap();
        assert!(
            union_distinct.contains("UNION"),
            "unexpected SQL: {union_distinct}"
        );
        assert!(
            !union_distinct.contains("UNION ALL"),
            "unexpected SQL: {union_distinct}"
        );

        let bare_left = events();
        let bare_right = DataFrame::from_table(
            "other_events",
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("value", DataType::Int64, false),
            ])),
        )
        .unwrap();
        for frame in [
            bare_left.intersect(&bare_right, true).unwrap(),
            bare_left.except(&bare_right, true).unwrap(),
        ] {
            let sql = frame.to_sql().unwrap();
            assert!(
                !sql.contains("SELECT FROM") && sql.starts_with("SELECT "),
                "set operation must include its output columns: {sql}"
            );
        }

        for (frame, distinct, expected_predicate) in [
            (left.intersect(&right, true).unwrap(), false, " EXISTS "),
            (left.intersect(&right, false).unwrap(), true, " EXISTS "),
            (left.except(&right, true).unwrap(), false, " NOT EXISTS "),
            (left.except(&right, false).unwrap(), true, " NOT EXISTS "),
        ] {
            let sql = frame.to_sql().unwrap();
            let uppercase = sql.to_ascii_uppercase();
            assert_eq!(
                uppercase.contains("SELECT DISTINCT "),
                distinct,
                "unexpected distinct lowering: {sql}"
            );
            assert!(
                uppercase.contains(expected_predicate),
                "expected {expected_predicate} in SQL: {sql}"
            );
            assert!(
                uppercase.contains(" IS NOT DISTINCT FROM "),
                "set operations must compare nulls safely: {sql}"
            );
        }

        for frame in [
            filtered_left.intersect(&filtered_right, true).unwrap(),
            filtered_left.intersect(&filtered_right, false).unwrap(),
            filtered_left.except(&filtered_right, true).unwrap(),
            filtered_left.except(&filtered_right, false).unwrap(),
        ] {
            let sql = frame.to_sql().unwrap();
            assert!(
                sql.contains("> 1"),
                "left set-operation input filter was lost: {sql}"
            );
            assert!(
                sql.contains("< 100"),
                "right set-operation input filter was lost: {sql}"
            );
        }
    }

    #[test]
    fn isolates_compound_inputs_when_rendering_joins() {
        let left = events().alias("left").unwrap();
        let right = DataFrame::from_table(
            "other_events",
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("value", DataType::Int64, false),
            ])),
        )
        .unwrap()
        .alias("right")
        .unwrap();

        let renamed_left = left.with_column_renamed("value", "left_value").unwrap();
        let renamed_right = right.with_column_renamed("value", "right_value").unwrap();
        let renamed_join_sql = renamed_left
            .join(
                &renamed_right,
                &["id".to_string()],
                &["id".to_string()],
                JoinType::Inner,
            )
            .unwrap()
            .to_sql()
            .unwrap();
        assert!(
            renamed_join_sql.contains(") AS \"left\"")
                && renamed_join_sql.contains(") AS \"right\""),
            "projected join inputs must retain their aliases: {renamed_join_sql}"
        );

        let joined = left
            .join(
                &right,
                &["id".to_string()],
                &["id".to_string()],
                JoinType::Inner,
            )
            .unwrap();
        let filtered_join_sql = joined
            .filter(left.column("value").unwrap().gt(lit(0_i64)))
            .unwrap()
            .to_sql()
            .unwrap();
        assert!(
            filtered_join_sql.contains(" JOIN ") && filtered_join_sql.contains(" WHERE "),
            "filter after join must remain lowerable: {filtered_join_sql}"
        );
        let projected_sort_sql = joined
            .sort(vec![(left.column("value").unwrap(), true, false)])
            .unwrap()
            .select(vec![
                left.column("id").unwrap(),
                right.column("value").unwrap(),
            ])
            .unwrap()
            .to_sql()
            .unwrap();
        assert!(
            projected_sort_sql.contains(" JOIN ")
                && projected_sort_sql.contains("ORDER BY")
                && !projected_sort_sql.contains("FROM (SELECT"),
            "a projected sorted join must remain lowerable: {projected_sort_sql}"
        );
        let scoped_join_error = joined
            .limit(2, 0)
            .unwrap()
            .select(vec![left.column("id").unwrap()])
            .unwrap()
            .to_sql()
            .unwrap_err();
        assert!(
            scoped_join_error
                .to_string()
                .contains("select uniquely aliased output columns"),
            "unexpected scoped join error: {scoped_join_error}"
        );
        let joined_aggregate_sql = joined
            .aggregate(
                vec![left.column("id").unwrap()],
                vec![aggregate_sum(right.column("value").unwrap()).alias("total")],
            )
            .unwrap()
            .to_sql()
            .unwrap();
        let group_position = joined_aggregate_sql.find("\"left\".id").unwrap();
        let aggregate_position = joined_aggregate_sql
            .to_ascii_lowercase()
            .find("sum(")
            .unwrap();
        assert!(
            group_position < aggregate_position,
            "aggregate output must follow DataFrame schema order: {joined_aggregate_sql}"
        );

        let aggregate = left
            .aggregate(
                vec![left.column("id").unwrap()],
                vec![aggregate_sum(left.column("value").unwrap()).alias("total")],
            )
            .unwrap();
        let aggregate_join_sql = aggregate
            .join(
                &right,
                &["id".to_string()],
                &["id".to_string()],
                JoinType::Inner,
            )
            .unwrap()
            .to_sql()
            .unwrap();
        let uppercase = aggregate_join_sql.to_ascii_uppercase();
        let join_position = uppercase.find(" JOIN ").unwrap();
        assert!(
            uppercase[..join_position].contains("SUM(")
                && uppercase[..join_position].contains("GROUP BY"),
            "aggregate input must remain a derived relation: {aggregate_join_sql}"
        );

        let nested = left
            .join(
                &right,
                &["id".to_string()],
                &["id".to_string()],
                JoinType::Inner,
            )
            .unwrap()
            .select(vec![
                left.column("id").unwrap().alias("left_id"),
                left.column("value").unwrap(),
            ])
            .unwrap()
            .filter(col("value").gt(lit(1_i64)))
            .unwrap();
        let third = DataFrame::from_table(
            "third_events",
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
        )
        .unwrap()
        .alias("third")
        .unwrap();
        for join_type in [JoinType::Right, JoinType::Full] {
            let error = nested
                .join(
                    &third,
                    &["left_id".to_string()],
                    &["id".to_string()],
                    join_type,
                )
                .unwrap()
                .to_sql()
                .unwrap_err();
            assert!(matches!(error, Error::NotSupported { .. }));
            assert!(
                error.to_string().contains("compound left join input"),
                "unexpected error: {error}"
            );
        }
    }

    #[tokio::test]
    async fn submits_rendered_sql_through_database_query_api() {
        let directory = tempfile::tempdir().unwrap();
        let database = connect(directory.path().to_str().unwrap())
            .execute()
            .await
            .unwrap();
        let table = database
            .create_table(
                "users",
                RecordBatch::try_new(
                    Arc::new(Schema::new(vec![
                        Field::new("name", DataType::Utf8, false),
                        Field::new("age", DataType::Int64, false),
                    ])),
                    vec![
                        Arc::new(arrow_array::StringArray::from(vec!["Ada"])),
                        Arc::new(Int64Array::from(vec![37_i64])),
                    ],
                )
                .unwrap(),
            )
            .execute()
            .await
            .unwrap();
        let recording_database = Arc::new(RecordingSqlDatabase {
            inner: database.database().clone(),
            submitted: Mutex::new(None),
            sql_available: true,
        });
        let table = crate::Table::new(table.base_table().clone(), recording_database.clone());

        let error = table
            .to_df()
            .await
            .unwrap()
            .filter(col("age").gt(lit(18_i64)))
            .unwrap()
            .select(vec![col("name"), col("age")])
            .unwrap()
            .execute()
            .await
            .unwrap_err();

        assert!(matches!(error, Error::Runtime { .. }));
        assert_eq!(
            recording_database.submitted.lock().unwrap().as_ref(),
            Some(&(
                "SELECT users.\"name\", users.age FROM users WHERE (users.age > 18)".to_string(),
                vec!["public".to_string()],
            ))
        );

        let unavailable_database = Arc::new(RecordingSqlDatabase {
            inner: database.database().clone(),
            submitted: Mutex::new(None),
            sql_available: false,
        });
        let table = crate::Table::new(table.base_table().clone(), unavailable_database);
        let error = table.to_df().await.unwrap().execute().await.unwrap_err();
        assert!(matches!(error, Error::NotSupported { .. }));
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
        let sql = joined.to_sql().unwrap();
        assert!(sql.contains("JOIN"));
        assert!(sql.contains("left_value"));
        assert!(sql.contains("right_value"));

        let chained_sql = joined
            .select(vec![col("left_value")])
            .unwrap()
            .to_sql()
            .unwrap();
        assert!(
            chained_sql.contains(" AS left_value"),
            "chained join projection must retain its output alias: {chained_sql}"
        );
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
        assert!(joined.to_sql().unwrap().contains("left.value"));
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
    async fn resolves_latest_data_and_rejects_later_checkout() {
        let directory = tempfile::tempdir().unwrap();
        let database = connect(directory.path().to_str().unwrap())
            .execute()
            .await
            .unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let first = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1_i64]))],
        )
        .unwrap();
        let table = database
            .create_table("events", first)
            .execute()
            .await
            .unwrap();
        let version = table.version().await.unwrap();
        let frame = table.to_df().await.unwrap();
        let second =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![2_i64]))]).unwrap();

        table.add(second).execute().await.unwrap();

        let batches: Vec<RecordBatch> = frame
            .execute()
            .await
            .unwrap()
            .reader()
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);

        table.checkout(version).await.unwrap();
        assert!(matches!(
            frame.execute().await,
            Err(Error::NotSupported { .. })
        ));
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
        let (stream, abort_handle, status) = prepare_local_stream(stream);
        let query = Query::new(Arc::new(LocalQueryHandle::new(
            stream,
            abort_handle,
            status,
        )));
        let mut stream = query.reader().await.unwrap();

        assert!(stream.try_next().await.is_err());
        let description = query.describe().await.unwrap();
        assert_eq!(description.status, QueryStatus::Failed);
        assert_eq!(description.progress, None);
    }
}
