// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Views with `GROUP BY`: one row per group, planned and executed by
//! DataFusion, and recomputed in full whenever the source changes, since a
//! group spans fragments.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow_schema::{DataType, Field as ArrowField, Schema as ArrowSchema, SchemaRef};
use datafusion::catalog::default_table_source::provider_as_source;
use datafusion::datasource::empty::EmptyTable;
use datafusion::execution::SessionStateBuilder;
use datafusion::execution::context::SessionState;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::prelude::SessionContext;
use datafusion_common::TableReference;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion_expr::planner::{ContextProvider, ExprPlanner};
use datafusion_expr::{
    AggregateUDF, HigherOrderUDF, LogicalPlan, ScalarUDF, TableSource, WindowUDF,
};
use datafusion_sql::planner::SqlToRel;
use datafusion_sql::sqlparser::dialect::GenericDialect;
use datafusion_sql::sqlparser::parser::Parser;
use futures::StreamExt;
use lance::Dataset;
use lance_core::ROW_ID;
use lance_datafusion::exec::SessionContextExt;

use super::refresh::to_view_batch;
use super::{
    MaterializedViewDefinition, Planned, SOURCE_ROW_ID_COLUMN, ViewProjection, ensure_immutable,
    plan_filter, query, without_declarations,
};
use crate::{Error, Result};

/// The name the source is planned under; never visible to the user.
const SOURCE: &str = "__source";

fn session() -> SessionState {
    SessionStateBuilder::new().with_default_features().build()
}

/// The query DataFusion runs: the view's projections plus the group's
/// smallest source row id, which stands as the row's provenance. The filter
/// is not here; the lance scan applies it, as for any other view.
fn sql(definition: &MaterializedViewDefinition) -> String {
    let items: Vec<String> = definition
        .projections
        .iter()
        .map(|p| format!("{} AS {}", p.expression, query::ident_sql(&p.output)))
        .collect();
    format!(
        "SELECT {}, min({ROW_ID}) AS {ROW_ID} FROM {SOURCE} GROUP BY {}",
        items.join(", "),
        definition.group_by.join(", ")
    )
}

struct Provider<'a> {
    state: &'a SessionState,
    source: Arc<dyn TableSource>,
}

impl ContextProvider for Provider<'_> {
    fn get_table_source(
        &self,
        name: TableReference,
    ) -> datafusion_common::Result<Arc<dyn TableSource>> {
        if name.schema().is_none() && name.table() == SOURCE {
            Ok(self.source.clone())
        } else {
            datafusion_common::plan_err!("a grouped view reads only its source, not '{name}'")
        }
    }
    fn get_expr_planners(&self) -> &[Arc<dyn ExprPlanner>] {
        self.state.expr_planners()
    }
    fn get_function_meta(&self, name: &str) -> Option<Arc<ScalarUDF>> {
        self.state.scalar_functions().get(name).cloned()
    }
    fn get_higher_order_meta(&self, name: &str) -> Option<Arc<HigherOrderUDF>> {
        self.state.higher_order_functions().get(name).cloned()
    }
    fn get_aggregate_meta(&self, name: &str) -> Option<Arc<AggregateUDF>> {
        self.state.aggregate_functions().get(name).cloned()
    }
    fn get_window_meta(&self, name: &str) -> Option<Arc<WindowUDF>> {
        self.state.window_functions().get(name).cloned()
    }
    fn get_variable_type(&self, _: &[String]) -> Option<DataType> {
        None
    }
    fn options(&self) -> &ConfigOptions {
        self.state.config_options()
    }
    fn udf_names(&self) -> Vec<String> {
        self.state.scalar_functions().keys().cloned().collect()
    }
    fn higher_order_function_names(&self) -> Vec<String> {
        self.state
            .higher_order_functions()
            .keys()
            .cloned()
            .collect()
    }
    fn udaf_names(&self) -> Vec<String> {
        self.state.aggregate_functions().keys().cloned().collect()
    }
    fn udwf_names(&self) -> Vec<String> {
        self.state.window_functions().keys().cloned().collect()
    }
}

fn logical_plan(
    state: &SessionState,
    source: Arc<dyn TableSource>,
    definition: &MaterializedViewDefinition,
) -> Result<LogicalPlan> {
    let invalid = |e: &dyn std::fmt::Display| Error::InvalidInput {
        message: format!("invalid grouped view: {e}"),
    };
    let statement = Parser::parse_sql(&GenericDialect {}, &sql(definition))
        .map_err(|e| invalid(&e))?
        .pop()
        .ok_or_else(|| invalid(&"empty query"))?;
    SqlToRel::new(&Provider { state, source })
        .sql_statement_to_plan(statement)
        .map_err(|e| invalid(&e))
}

/// Plan a grouped definition against the source schema. `filter` is the
/// canonical filter, already spelled the way [`super::plan`] stores it.
pub(super) fn plan(
    source_schema: SchemaRef,
    definition: &MaterializedViewDefinition,
    filter: Option<String>,
) -> Result<Planned> {
    query::check_grouping(definition)?;
    let mut inputs = match &filter {
        Some(filter) => plan_filter(&source_schema, filter)?,
        None => Vec::new(),
    };
    let mut projections: Vec<ViewProjection> = Vec::with_capacity(definition.projections.len());
    for p in &definition.projections {
        if p.output == SOURCE_ROW_ID_COLUMN || p.output == ROW_ID {
            return Err(Error::InvalidInput {
                message: format!("view column name '{}' is reserved", p.output),
            });
        }
        if projections.iter().any(|seen| seen.output == p.output) {
            return Err(Error::ColumnAlreadyExists {
                name: p.output.clone(),
            });
        }
        let expression =
            query::canonical_expr(&p.expression).map_err(|e| Error::InvalidExpression {
                column: p.output.clone(),
                message: e.to_string(),
            })?;
        projections.push(ViewProjection {
            output: p.output.clone(),
            expression,
        });
    }
    let group_by = definition
        .group_by
        .iter()
        .map(|key| query::canonical_expr(key))
        .collect::<Result<Vec<_>>>()?;
    let definition = MaterializedViewDefinition {
        projections,
        filter,
        group_by,
        ..definition.clone()
    };

    let mut fields = source_schema.fields().to_vec();
    fields.push(Arc::new(ArrowField::new(ROW_ID, DataType::UInt64, false)));
    let empty = provider_as_source(Arc::new(EmptyTable::new(Arc::new(ArrowSchema::new(
        fields,
    )))));
    let planned = logical_plan(&session(), empty, &definition)?;

    let mut exprs = Vec::new();
    planned
        .apply(|node| {
            exprs.extend(node.expressions());
            Ok(TreeNodeRecursion::Continue)
        })
        .map_err(|e| Error::InvalidInput {
            message: format!("invalid grouped view: {e}"),
        })?;
    for expr in &exprs {
        ensure_immutable(expr, |message| Error::InvalidInput {
            message: format!("invalid grouped view: {message}"),
        })?;
        inputs.extend(
            expr.column_refs()
                .into_iter()
                .map(|c| c.name.clone())
                .filter(|name| name != ROW_ID && source_schema.field_with_name(name).is_ok()),
        );
    }
    inputs.sort();
    inputs.dedup();

    let fields = planned
        .schema()
        .fields()
        .iter()
        .filter(|f| f.name() != ROW_ID)
        .map(|f| without_declarations(f))
        .collect();
    Ok(Planned {
        definition,
        fields,
        lineage: HashMap::new(),
        inputs,
    })
}

/// Every group of `source`, as view rows in `schema`.
pub(super) async fn stream(
    source: &Dataset,
    definition: &MaterializedViewDefinition,
    inputs: &[String],
    schema: SchemaRef,
    rows_written: Arc<AtomicU64>,
) -> Result<SendableRecordBatchStream> {
    let mut scanner = source.scan();
    scanner.with_row_id();
    if let Some(filter) = &definition.filter {
        scanner.filter(filter)?;
    }
    scanner.project(inputs)?;
    let scan: SendableRecordBatchStream = scanner.try_into_stream().await?.into();

    let state = session();
    let ctx = SessionContext::new_with_state(state.clone());
    let source = ctx.read_one_shot(scan)?.into_view();
    let planned = logical_plan(&state, provider_as_source(source), definition)?;
    let groups = ctx
        .execute_logical_plan(planned)
        .await?
        .execute_stream()
        .await?;

    let out_schema = schema.clone();
    let mapped = groups.map(move |batch| {
        let batch = to_view_batch(&batch?, &out_schema)?;
        rows_written.fetch_add(batch.num_rows() as u64, Ordering::Relaxed);
        Ok(batch)
    });
    Ok(Box::pin(RecordBatchStreamAdapter::new(schema, mapped)))
}
