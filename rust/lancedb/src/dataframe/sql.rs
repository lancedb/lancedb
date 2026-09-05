// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! SQL lowering for LanceDB DataFrame plans.

use std::sync::Arc;

use datafusion_common::{
    Column, NullEquality,
    tree_node::{Transformed, TransformedResult, TreeNode},
};
use datafusion_expr::{Expr, LogicalPlan, LogicalPlanBuilder, Operator, expr_fn::binary_expr};
use datafusion_sql::unparser::Unparser;

pub(super) fn plan_to_sql(plan: &LogicalPlan) -> datafusion_common::Result<String> {
    let plan = preserve_join_semantics(plan)?;
    Unparser::default()
        .plan_to_sql(&plan)
        .map(|statement| statement.to_string())
}

fn preserve_join_semantics(plan: &LogicalPlan) -> datafusion_common::Result<LogicalPlan> {
    plan.clone()
        .transform_up(|plan| match plan {
            LogicalPlan::Join(mut join) => {
                let mut transformed = false;
                if simple_scan_has_filters(&join.left) {
                    join.left = isolate_join_input(join.left)?;
                    transformed = true;
                }
                if simple_scan_has_filters(&join.right) {
                    join.right = isolate_join_input(join.right)?;
                    transformed = true;
                }

                if join.null_equality != NullEquality::NullEqualsNull || join.on.is_empty() {
                    return Ok(if transformed {
                        Transformed::yes(LogicalPlan::Join(join))
                    } else {
                        Transformed::no(LogicalPlan::Join(join))
                    });
                }

                let null_safe_condition = std::mem::take(&mut join.on)
                    .into_iter()
                    .map(|(left, right)| binary_expr(left, Operator::IsNotDistinctFrom, right))
                    .reduce(Expr::and);
                join.filter = match (join.filter.take(), null_safe_condition) {
                    (Some(filter), Some(condition)) => Some(filter.and(condition)),
                    (filter, condition) => filter.or(condition),
                };
                join.null_equality = NullEquality::NullEqualsNothing;
                Ok(Transformed::yes(LogicalPlan::Join(join)))
            }
            _ => Ok(Transformed::no(plan)),
        })
        .data()
}

fn simple_scan_has_filters(plan: &LogicalPlan) -> bool {
    let mut plan = plan;
    let mut has_filters = false;
    loop {
        match plan {
            LogicalPlan::SubqueryAlias(alias) => plan = &alias.input,
            LogicalPlan::Filter(filter) => {
                has_filters = true;
                plan = &filter.input;
            }
            LogicalPlan::TableScan(scan) => return has_filters || !scan.filters.is_empty(),
            _ => return false,
        }
    }
}

fn isolate_join_input(input: Arc<LogicalPlan>) -> datafusion_common::Result<Arc<LogicalPlan>> {
    let Some(qualifier) = input
        .schema()
        .iter()
        .next()
        .and_then(|(qualifier, _)| qualifier.cloned())
    else {
        return Ok(input);
    };
    if input
        .schema()
        .iter()
        .any(|(candidate, _)| candidate != Some(&qualifier))
    {
        return Ok(input);
    }

    let projection = input
        .schema()
        .fields()
        .iter()
        .map(|field| Expr::Column(Column::new(Some(qualifier.clone()), field.name())))
        .collect::<Vec<_>>();
    let plan = LogicalPlanBuilder::from(input)
        .project(projection)?
        .alias(qualifier)?
        .build()?;
    Ok(Arc::new(plan))
}
