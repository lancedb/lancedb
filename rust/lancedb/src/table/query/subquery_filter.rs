// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::{collections::HashMap, fmt, ops::ControlFlow, sync::Arc};

use arrow_array::{cast::AsArray, types::UInt64Type};
use arrow_schema::{DataType, Field, Schema};
use datafusion::{
    dataframe::DataFrame,
    execution::SessionStateBuilder,
    prelude::SessionContext,
    sql::{
        parser::Statement as DfStatement,
        sqlparser::{
            ast::{
                Expr, Ident, SelectItem, SetExpr, Statement, Value, ValueWithSpan, VisitMut,
                VisitorMut, visit_expressions,
            },
            parser::Parser,
            tokenizer::Token,
        },
    },
};
use datafusion_catalog::TableProvider;
use datafusion_common::{
    DataFusionError, Result as DfResult, TableReference,
    tree_node::{Transformed, TreeNode, TreeNodeRecursion},
};
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_expr::{Expr as DfExpr, LogicalPlan, LogicalPlanBuilder, col};
use datafusion_physical_expr::{EquivalenceProperties, Partitioning};
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, execute_stream,
    execution_plan::{Boundedness, EmissionType},
    stream::RecordBatchStreamAdapter,
};
use futures::{TryStreamExt, stream};
use lance::{
    Dataset,
    datafusion::LanceTableProvider,
    dataset::scanner::{RowAddrMask, RowAddrTreeMap, Scanner},
};
use lance_core::ROW_ID;
use lance_datafusion::{planner::Planner, udf::register_functions};
use tokio::sync::OnceCell;

use crate::{
    Error, Result,
    database::OpenTableRequest,
    expr::PredicateDialect,
    query::{QueryDatabase, QueryFilter, QueryRequest},
    table::{NativeTable, datafusion::BaseTableAdapter},
};

/// Leave ordinary SQL filters to Lance, including literals containing SQL keywords.
pub(super) fn predicate(filter: Option<&QueryFilter>) -> Option<Expr> {
    let Some(QueryFilter::Sql(sql)) = filter else {
        return None;
    };
    parse_predicate(sql)
}

pub(super) fn parse_predicate(sql: &str) -> Option<Expr> {
    if !sql
        .as_bytes()
        .windows(6)
        .any(|word| word.eq_ignore_ascii_case(b"select"))
    {
        return None;
    }

    let dialect = PredicateDialect::default();
    let mut parser = Parser::new(&dialect).try_with_sql(sql).ok()?;
    let expr = parser.parse_expr().ok()?;
    if parser.peek_token().token != Token::EOF {
        return None;
    }
    visit_expressions(&expr, |expr| match expr {
        Expr::InSubquery { .. } => ControlFlow::Break(()),
        _ => ControlFlow::Continue(()),
    })
    .is_break()
    .then_some(expr)
}

// DataFusion can lose UNKNOWN when it lowers membership embedded in a
// Boolean expression to a mark join. A positive top-level conjunct only
// retains matches, so FALSE and UNKNOWN are equivalent there.
fn validate_membership_predicate(predicate: &Expr) -> Result<()> {
    use datafusion::sql::sqlparser::ast::BinaryOperator;

    fn positive_conjuncts(expr: &Expr) -> usize {
        match expr {
            Expr::Nested(inner) => positive_conjuncts(inner),
            Expr::BinaryOp {
                left,
                op: BinaryOperator::And,
                right,
            } => positive_conjuncts(left) + positive_conjuncts(right),
            Expr::InSubquery { negated: false, .. } => 1,
            _ => 0,
        }
    }

    let mut total = 0;
    let _ = visit_expressions(predicate, |expr| {
        if matches!(expr, Expr::InSubquery { .. }) {
            total += 1;
        }
        ControlFlow::<()>::Continue(())
    });
    if total != positive_conjuncts(predicate) {
        return Err(Error::NotSupported {
            message: "SQL subquery filters require positive IN (SELECT ...) as a standalone predicate or directly joined by AND. NOT IN, Boolean expressions wrapping membership, and nested IN subqueries are not supported.".into(),
        });
    }
    Ok(())
}

/// Replace membership tests with Boolean columns for Lance's scalar planner.
/// Keep the subqueries intact for DataFusion, which resolves their own scopes.
struct MembershipSlots<'a> {
    sql: &'a str,
    schema: &'a Schema,
    names: Vec<String>,
    memberships: Vec<Expr>,
    operands: Vec<Expr>,
    candidate: usize,
}

impl VisitorMut for MembershipSlots<'_> {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<()> {
        if let Expr::InSubquery {
            expr: operand,
            subquery,
            negated,
        } = expr
        {
            let name = loop {
                let name = format!("__lancedb_subquery_{}", self.candidate);
                self.candidate += 1;
                if !self.sql.contains(&name)
                    && !self
                        .schema
                        .fields()
                        .iter()
                        .any(|f| f.name().eq_ignore_ascii_case(&name))
                {
                    break name;
                }
            };
            self.operands.push(*std::mem::replace(
                operand,
                Box::new(Expr::Value(Value::Null.into())),
            ));
            self.memberships.push(Expr::InSubquery {
                expr: operand.clone(),
                subquery: subquery.clone(),
                negated: *negated,
            });
            self.names.push(name.clone());
            *expr = Expr::Identifier(Ident::new(name));
        }
        ControlFlow::Continue(())
    }
}

// SQLParser's formatter also treats a backslash as an escape, which Lance's
// tokenizer does not. Double quotes in decoded literals before rendering them
// so a value such as a backslash followed by an apostrophe round-trips.
fn render_scalar_sql(mut expr: Expr) -> String {
    struct EscapeQuotes;
    impl VisitorMut for EscapeQuotes {
        type Break = ();

        fn post_visit_value(&mut self, value: &mut ValueWithSpan) -> ControlFlow<()> {
            if let Value::SingleQuotedString(text) = &mut value.value {
                *text = text.replace('\'', "''");
            }
            ControlFlow::Continue(())
        }
    }

    let _ = expr.visit(&mut EscapeQuotes);
    expr.to_string()
}

pub(crate) struct PreparedSubquery {
    pub(super) dataset: Arc<Dataset>,
    plan: Arc<dyn ExecutionPlan>,
    context: Arc<TaskContext>,
    // Only hybrid legs share a cache. A normal plan can be executed repeatedly.
    shared_mask: Option<OnceCell<Arc<RowAddrMask>>>,
}

impl fmt::Debug for PreparedSubquery {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PreparedSubquery").finish_non_exhaustive()
    }
}

impl PreparedSubquery {
    pub(super) async fn try_new(
        table: &NativeTable,
        dataset: Arc<Dataset>,
        query: &QueryRequest,
        mut predicate: Expr,
        shared: bool,
    ) -> Result<Self> {
        if !query.prefilter {
            return Err(Error::NotSupported {
                message: "SQL subquery filters require prefiltering.".into(),
            });
        }
        let Some(QueryDatabase(database)) = &query.database else {
            return Err(Error::NotSupported {
                message:
                    "SQL subquery filters require a table opened through a database connection."
                        .into(),
            });
        };

        validate_membership_predicate(&predicate)?;

        // Use the same snapshot for the outer scan and the ranked scan.
        let provider = LanceTableProvider::new_with_ordering(dataset.clone(), true, false, false);
        let schema = provider.schema();
        let sql = predicate.to_string().to_ascii_lowercase();
        let mut slots = MembershipSlots {
            sql: &sql,
            schema: &schema,
            names: Vec::new(),
            memberships: Vec::new(),
            operands: Vec::new(),
            candidate: 0,
        };
        let _ = predicate.visit(&mut slots);
        let slot_fields = slots
            .names
            .iter()
            .map(|name| Arc::new(Field::new(name, DataType::Boolean, true)));
        let outer_schema = Schema::new_with_metadata(
            schema
                .fields()
                .iter()
                .cloned()
                .chain(slot_fields)
                .collect::<Vec<_>>(),
            schema.metadata().clone(),
        );

        // Parse the outer scalar predicate, including the left operand of each
        // IN, exactly as a Lance filter. DataFusion must not rebind these
        // columns, or reinterpret Lance-specific functions and coercions.
        let filter =
            Planner::new(Arc::new(outer_schema)).parse_filter(&render_scalar_sql(predicate))?;
        let operand_planner = Planner::new(schema.clone());
        let operands = slots
            .operands
            .into_iter()
            .map(|expr| operand_planner.parse_expr(&render_scalar_sql(expr)))
            .collect::<lance_core::Result<Vec<_>>>()?;

        // Only expose the originating connection's tables. In particular, do not
        // enable DataFusion table functions such as readers of arbitrary paths.
        let ctx = SessionContext::new_with_state(
            SessionStateBuilder::new_with_default_features()
                .with_table_functions(HashMap::new())
                .build(),
        );
        register_functions(&ctx);
        let name = Ident::with_quote('`', &table.name);
        let state = ctx.state();
        let DfStatement::Statement(mut statement) =
            state.sql_to_statement(&format!("SELECT 1 FROM {name}"), &Default::default())?
        else {
            unreachable!("generated statement is a SELECT");
        };
        let Statement::Query(query) = statement.as_mut() else {
            unreachable!("generated statement is a SELECT");
        };
        let SetExpr::Select(select) = query.body.as_mut() else {
            unreachable!("generated statement is a SELECT");
        };
        select.projection = slots
            .memberships
            .into_iter()
            .zip(&slots.names)
            .map(|(expr, name)| SelectItem::ExprWithAlias {
                expr,
                alias: Ident::new(name),
            })
            .collect();
        let statement = DfStatement::Statement(statement);

        ctx.register_table(TableReference::bare(table.name.clone()), Arc::new(provider))?;
        for reference in state.resolve_table_references(&statement)? {
            let TableReference::Bare { table: name } = reference else {
                return Err(Error::NotSupported {
                    message: "Use unqualified table names in SQL subquery filters.".into(),
                });
            };
            if name.as_ref() == table.name {
                continue;
            }
            let source = database
                .open_table(OpenTableRequest {
                    name: name.to_string(),
                    namespace_path: table.namespace.clone(),
                    index_cache_size: None,
                    lance_read_params: None,
                    location: None,
                    namespace_client: None,
                    managed_versioning: None,
                })
                .await?;
            let provider = BaseTableAdapter::try_new(source).await?;
            ctx.register_table(TableReference::bare(name), Arc::new(provider))?;
        }
        let state = ctx.state();
        let LogicalPlan::Projection(memberships) = state.statement_to_plan(statement).await? else {
            return Err(Error::Runtime {
                message: "Expected a SQL subquery projection.".into(),
            });
        };
        let mut bindings = HashMap::new();
        for ((name, membership), operand) in
            slots.names.into_iter().zip(memberships.expr).zip(operands)
        {
            let DfExpr::InSubquery(mut membership) = membership.unalias() else {
                return Err(Error::Runtime {
                    message: "Expected an IN subquery.".into(),
                });
            };
            membership.expr = Box::new(operand);
            bindings.insert(name, DfExpr::InSubquery(membership));
        }
        let filter = filter
            .transform_up(|expr| {
                if let DfExpr::Column(column) = &expr
                    && let Some(membership) = bindings.get(&column.name).cloned()
                {
                    return Ok(Transformed::yes(membership));
                }
                Ok(Transformed::no(expr))
            })?
            .data;
        let plan = LogicalPlanBuilder::from(Arc::unwrap_or_clone(memberships.input))
            .filter(filter)?
            .project([col(ROW_ID)])?
            .build()?;
        let frame = DataFrame::new(state, plan);
        let context = Arc::new(frame.task_ctx());
        let plan = frame.create_physical_plan().await?;
        Ok(Self {
            dataset,
            plan,
            context,
            shared_mask: shared.then(OnceCell::new),
        })
    }

    async fn materialize(&self) -> DfResult<Arc<RowAddrMask>> {
        let mut batches = execute_stream(self.plan.clone(), self.context.clone())?;
        let mut rows = RowAddrTreeMap::default();
        while let Some(batch) = batches.try_next().await? {
            rows.extend(
                batch
                    .column(0)
                    .as_primitive::<UInt64Type>()
                    .iter()
                    .flatten(),
            );
        }
        Ok(Arc::new(RowAddrMask::from_allowed(rows)))
    }

    async fn row_mask(&self) -> DfResult<Arc<RowAddrMask>> {
        match &self.shared_mask {
            Some(cell) => cell.get_or_try_init(|| self.materialize()).await.cloned(),
            None => self.materialize().await,
        }
    }
}

/// The Lance scanner needs a concrete mask to build the ranked plan. Build a
/// schema-only preview during planning and defer the actual masked plan until
/// the returned stream is polled, inside the query execution timeout.
pub(super) struct SubqueryPrefilterExec {
    eligibility: Arc<PreparedSubquery>,
    scanner: Scanner,
    query: crate::query::VectorQueryRequest,
    preview: Arc<dyn ExecutionPlan>,
    properties: Arc<PlanProperties>,
}

impl SubqueryPrefilterExec {
    pub(super) fn new(
        eligibility: Arc<PreparedSubquery>,
        scanner: Scanner,
        query: crate::query::VectorQueryRequest,
        preview: Arc<dyn ExecutionPlan>,
    ) -> Self {
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(preview.schema()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        ));
        Self {
            eligibility,
            scanner,
            query,
            preview,
            properties,
        }
    }
}

impl fmt::Debug for SubqueryPrefilterExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SubqueryPrefilterExec")
            .finish_non_exhaustive()
    }
}

impl DisplayAs for SubqueryPrefilterExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "SubqueryPrefilterExec: eligibility before {}",
            self.preview.name()
        )
    }
}

impl ExecutionPlan for SubqueryPrefilterExec {
    fn name(&self) -> &str {
        "SubqueryPrefilterExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        // The scanner's preview is not executed and cannot be rewritten.
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return Err(DataFusionError::Internal(
                "SubqueryPrefilterExec does not have children".into(),
            ));
        }
        Ok(self)
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(
            &Arc<dyn datafusion_physical_plan::PhysicalExpr>,
        ) -> DfResult<TreeNodeRecursion>,
    ) -> DfResult<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "Invalid SubqueryPrefilterExec partition {partition}"
            )));
        }
        let eligibility = self.eligibility.clone();
        let mut scanner = self.scanner.clone();
        let query = self.query.clone();
        let stream = stream::once(async move {
            let mask = eligibility.row_mask().await?;
            scanner.with_row_id_prefilter((*mask).clone());
            let plan =
                super::create_scanner_plan(&mut scanner, eligibility.dataset.schema(), &query)
                    .await
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
            execute_stream(plan, context)
        })
        .try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        sync::atomic::{AtomicUsize, Ordering},
        time::Duration,
    };

    use arrow_array::{
        ArrayRef, FixedSizeListArray, Int32Array, RecordBatch, StringArray, UInt64Array,
        types::Float32Type,
    };
    use lance_core::ROW_ID_FIELD;
    use lance_datafusion::exec::OneShotExec;
    use lance_index::scalar::FullTextSearchQuery;
    use lance_namespace::models::CreateNamespaceRequest;

    use crate::{
        Table,
        connection::{NamespaceClientPushdownOperation, connect_namespace},
        query::{ExecutableQuery, HasQuery, QueryBase, QueryExecutionOptions},
    };

    const ELIGIBLE: &str = "id IN (SELECT id FROM acl)";

    async fn fixture() -> (crate::Connection, Table, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let db = connect_namespace(
            "dir",
            HashMap::from([("root".to_string(), dir.path().display().to_string())]),
        )
        .execute()
        .await
        .unwrap();
        let team = vec!["team".into()];
        db.create_namespace(CreateNamespaceRequest {
            id: Some(team.clone()),
            ..Default::default()
        })
        .await
        .unwrap();

        let table = RecordBatch::try_from_iter([
            (
                "id",
                Arc::new(Int32Array::from(vec![1, 2, 3, 4])) as ArrayRef,
            ),
            (
                "tenantId",
                Arc::new(Int32Array::from(vec![0, 1, 0, 1])) as ArrayRef,
            ),
            (
                "tenantid",
                Arc::new(Int32Array::from(vec![1, 0, 1, 0])) as ArrayRef,
            ),
            (
                "text",
                Arc::new(StringArray::from(vec!["alpha", "alpha", "alpha", "beta"])) as ArrayRef,
            ),
            (
                "vector",
                Arc::new(
                    FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
                        [0., 1., 2., 3.].map(|x| Some(vec![Some(x), Some(x)])),
                        2,
                    ),
                ) as ArrayRef,
            ),
        ])
        .unwrap();
        let table = db
            .create_table("chunks", vec![table])
            .namespace(team.clone())
            .execute()
            .await
            .unwrap();
        let keys = |ids| {
            RecordBatch::try_from_iter([("id", Arc::new(Int32Array::from(ids)) as ArrayRef)])
                .unwrap()
        };
        db.create_table("acl", vec![keys(vec![2, 3, 4])])
            .namespace(team)
            .execute()
            .await
            .unwrap();
        db.create_table("acl", vec![keys(vec![1])])
            .execute()
            .await
            .unwrap();
        (db, table, dir)
    }

    async fn top_id(table: &Table, filter: &str) -> Vec<i32> {
        let batches: Vec<_> = table
            .query()
            .nearest_to(&[0.0_f32, 0.0])
            .unwrap()
            .only_if(filter)
            .limit(1)
            .execute()
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column_by_name("id")
                    .unwrap()
                    .as_primitive::<arrow_array::types::Int32Type>()
                    .values()
                    .to_vec()
            })
            .collect()
    }

    #[tokio::test]
    async fn preserves_outer_case_and_lance_functions() {
        let (_, table, _dir) = fixture().await;
        // Opposing values distinguish exact bindings from lowercasing; id 4 is
        // eligible on membership and tenant alone but fails contains_tokens.
        let ordinary = "tenantId = 1 AND contains_tokens(text, 'alpha')";
        assert_eq!(top_id(&table, ordinary).await, vec![2]);
        assert_eq!(
            top_id(&table, &format!("{ordinary} AND {ELIGIBLE}")).await,
            vec![2]
        );
        assert_eq!(
            top_id(&table, "tenantid = 1 AND id IN (SELECT id FROM acl)").await,
            vec![3],
        );
        assert_eq!(
            top_id(
                &table,
                &format!("contains_tokens(text, 'beta') AND {ELIGIBLE}")
            )
            .await,
            vec![4],
        );
        assert_eq!(
            top_id(
                &table,
                "(tenantID = 1 OR id = 999) AND (id IN (SELECT id FROM acl))"
            )
            .await,
            vec![2],
        );
        assert_eq!(
            top_id(
                &table,
                "tenantId = 1 AND id NOT IN (2, 3) AND id IN (SELECT id FROM acl)"
            )
            .await,
            vec![4],
        );
    }

    #[tokio::test]
    async fn subquery_filter_positive_conjuncts_with_nullable_keys() {
        let db = crate::connect("memory://").execute().await.unwrap();
        let batch = RecordBatch::try_from_iter([
            ("id", Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef),
            (
                "key",
                Arc::new(Int32Array::from(vec![Some(10), Some(20), None])) as ArrayRef,
            ),
            (
                "vector",
                Arc::new(
                    FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
                        [0., 1., 2.].map(|x| Some(vec![Some(x), Some(x)])),
                        2,
                    ),
                ) as ArrayRef,
            ),
        ])
        .unwrap();
        let table = db.create_table("chunks", batch).execute().await.unwrap();
        let keys = RecordBatch::try_from_iter([(
            "key",
            Arc::new(Int32Array::from(vec![Some(20), None])) as ArrayRef,
        )])
        .unwrap();
        db.create_table("acl", keys).execute().await.unwrap();

        let membership = "key IN (SELECT key FROM acl)";
        assert_eq!(top_id(&table, membership).await, vec![2]);
        assert_eq!(
            top_id(
                &table,
                &format!("({membership}) AND (id = 2 OR id = 3) AND {membership}")
            )
            .await,
            vec![2]
        );
        for additional_condition in ["id = 1", "id = 3"] {
            assert!(
                top_id(&table, &format!("{membership} AND {additional_condition}"))
                    .await
                    .is_empty()
            );
        }
        assert!(
            top_id(&table, "key IN (SELECT key FROM acl WHERE key = 999)")
                .await
                .is_empty()
        );
        assert_eq!(
            top_id(&table, "id IN (2, 3) AND id NOT IN (2)").await,
            vec![3]
        );

        for filter in [
            "key NOT IN (SELECT key FROM acl)",
            "key NOT IN (SELECT key FROM acl) OR id = 3",
            "(key IN (SELECT key FROM acl)) IS NULL",
            "NOT (key IN (SELECT key FROM acl))",
            "(key IN (SELECT key FROM acl) OR id = 3)",
            "key IN (SELECT key FROM acl WHERE key NOT IN (SELECT key FROM acl))",
        ] {
            let result = table
                .query()
                .nearest_to(&[0.0_f32, 0.0])
                .unwrap()
                .only_if(filter)
                .execute()
                .await;
            assert!(
                matches!(&result, Err(Error::NotSupported { message })
                    if message.contains("positive IN")),
                "expected an unsupported subquery filter: {filter}"
            );
        }
    }

    #[tokio::test]
    async fn subquery_filter_preserves_cosine_bounds_for_appended_rows() {
        use crate::index::{Index, vector::IvfPqIndexBuilder};

        let db = crate::connect("memory://").execute().await.unwrap();
        let batch = |ids: Vec<i32>, vectors: Vec<Vec<Option<f32>>>| {
            RecordBatch::try_from_iter([
                ("id", Arc::new(Int32Array::from(ids)) as ArrayRef),
                (
                    "vector",
                    Arc::new(
                        FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
                            vectors.into_iter().map(Some),
                            8,
                        ),
                    ) as ArrayRef,
                ),
            ])
            .unwrap()
        };
        let mut state = 42_u64;
        let vectors = (0..256)
            .map(|_| {
                let vector = (0..8)
                    .map(|_| {
                        state = state
                            .wrapping_mul(6_364_136_223_846_793_005)
                            .wrapping_add(1);
                        (state >> 32) as u32 as f32 / u32::MAX as f32 - 0.5
                    })
                    .collect::<Vec<_>>();
                let norm = vector.iter().map(|v| v * v).sum::<f32>().sqrt();
                vector.into_iter().map(|v| Some(v / norm)).collect()
            })
            .collect();
        let table = db
            .create_table("chunks", batch((0..256).collect(), vectors))
            .execute()
            .await
            .unwrap();
        table
            .create_index(
                &["vector"],
                Index::IvfPq(
                    IvfPqIndexBuilder::default()
                        .distance_type(crate::DistanceType::Cosine)
                        .num_partitions(1)
                        .num_sub_vectors(1),
                ),
            )
            .execute()
            .await
            .unwrap();
        // The appended row lives outside the index. Its exact cosine
        // distance from [1, 0, ...] is 0.75 on the public scale.
        let mut appended = vec![Some(0.); 8];
        appended[0] = Some(0.25);
        appended[1] = Some((1.0_f32 - 0.25_f32.powi(2)).sqrt());
        table
            .add(batch(vec![256], vec![appended]))
            .execute()
            .await
            .unwrap();
        let keys =
            RecordBatch::try_from_iter([("id", Arc::new(Int32Array::from(vec![256])) as ArrayRef)])
                .unwrap();
        db.create_table("acl", keys).execute().await.unwrap();

        let mut mismatches = Vec::new();
        for (lower, upper, expected) in [(None, Some(0.5), vec![]), (Some(0.6), None, vec![256])] {
            for filter in ["id = 256", ELIGIBLE] {
                let batches = table
                    .query()
                    .nearest_to(&[1.0_f32, 0., 0., 0., 0., 0., 0., 0.])
                    .unwrap()
                    .distance_type(crate::DistanceType::Cosine)
                    .distance_range(lower, upper)
                    .only_if(filter)
                    .limit(1)
                    .execute()
                    .await
                    .unwrap()
                    .try_collect::<Vec<_>>()
                    .await
                    .unwrap();
                let ids = batches
                    .iter()
                    .flat_map(|b| {
                        b["id"]
                            .as_primitive::<arrow_array::types::Int32Type>()
                            .values()
                            .iter()
                            .copied()
                    })
                    .collect::<Vec<_>>();
                if ids != expected {
                    mismatches.push(format!(
                        "{filter}, range ({lower:?}, {upper:?}): expected {expected:?}, got {ids:?}"
                    ));
                }
            }
        }
        assert!(mismatches.is_empty(), "{mismatches:#?}");
    }

    #[tokio::test]
    async fn subquery_filter_bypasses_namespace_pushdown() {
        let (_, table, dir) = fixture().await;
        table
            .create_index(&["text"], crate::index::Index::FTS(Default::default()))
            .execute()
            .await
            .unwrap();
        let pushed = connect_namespace(
            "dir",
            HashMap::from([("root".to_string(), dir.path().display().to_string())]),
        )
        .pushdown_operations([NamespaceClientPushdownOperation::QueryTable])
        .execute()
        .await
        .unwrap()
        .open_table("chunks")
        .namespace(vec!["team".into()])
        .execute()
        .await
        .unwrap();
        let query = table
            .query()
            .nearest_to(&[0.0_f32, 0.0])
            .unwrap()
            .only_if(ELIGIBLE)
            .limit(1);
        let request = super::super::AnyQuery::VectorQuery(query.current_request().clone());
        for table in [&table, &pushed] {
            let batches = super::super::execute_query(
                table.as_native().unwrap(),
                &request,
                QueryExecutionOptions::default(),
            )
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
            assert_eq!(batches.len(), 1);
            let ids = batches[0]
                .column_by_name("id")
                .unwrap()
                .as_primitive::<arrow_array::types::Int32Type>();
            assert_eq!(ids.values(), &[2]);
            let batches = table
                .query()
                .full_text_search(FullTextSearchQuery::new("alpha".into()))
                .only_if("id IN (SELECT id FROM acl WHERE id = 2)")
                .limit(1)
                .execute()
                .await
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
                .unwrap();
            assert_eq!(
                batches[0]
                    .column_by_name("id")
                    .unwrap()
                    .as_primitive::<arrow_array::types::Int32Type>()
                    .values(),
                &[2]
            );
        }
    }

    #[tokio::test]
    async fn planning_never_polls_eligibility_and_timeout_covers_execution() {
        let (_, table, _dir) = fixture().await;
        let mut query = table
            .query()
            .nearest_to(&[0.0_f32, 0.0])
            .unwrap()
            .only_if(ELIGIBLE)
            .limit(1);
        let polled = Arc::new(AtomicUsize::new(0));
        let count = polled.clone();
        let schema = Arc::new(Schema::new(vec![ROW_ID_FIELD.clone()]));
        let eligibility = stream::once(async move {
            count.fetch_add(1, Ordering::SeqCst);
            std::future::pending::<DfResult<RecordBatch>>().await
        });
        let eligibility =
            OneShotExec::new(Box::pin(RecordBatchStreamAdapter::new(schema, eligibility)));
        let dataset = table.as_native().unwrap().dataset.get().await.unwrap();
        query.mut_query().prepared_subquery = Some(Arc::new(PreparedSubquery {
            dataset,
            plan: Arc::new(eligibility),
            context: SessionContext::new().task_ctx(),
            shared_mask: None,
        }));

        assert!(
            query
                .explain_plan(false)
                .await
                .unwrap()
                .contains("SubqueryPrefilterExec")
        );
        assert!(
            query
                .output_schema()
                .await
                .unwrap()
                .field_with_name("id")
                .is_ok()
        );
        assert_eq!(polled.load(Ordering::SeqCst), 0);
        let stream = query
            .execute_with_options(QueryExecutionOptions {
                timeout: Some(Duration::from_millis(50)),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(polled.load(Ordering::SeqCst), 0);
        let err = stream.try_collect::<Vec<_>>().await.unwrap_err();
        assert!(err.to_string().contains("Query timeout"));
        assert_eq!(polled.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn hybrid_legs_share_mask_materialization() {
        let (_, table, _dir) = fixture().await;
        let schema = Arc::new(Schema::new(vec![ROW_ID_FIELD.clone()]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(UInt64Array::from(vec![1]))]).unwrap();
        let eligibility = PreparedSubquery {
            dataset: table.as_native().unwrap().dataset.get().await.unwrap(),
            // This source errors if executed a second time.
            plan: Arc::new(OneShotExec::from_batch(batch)),
            context: SessionContext::new().task_ctx(),
            shared_mask: Some(OnceCell::new()),
        };
        let (left, right) =
            tokio::try_join!(eligibility.row_mask(), eligibility.row_mask()).unwrap();
        assert!(Arc::ptr_eq(&left, &right));
    }
}
