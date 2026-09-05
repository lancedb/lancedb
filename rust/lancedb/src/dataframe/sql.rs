// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! SQL lowering for LanceDB DataFrame plans.

use datafusion_common::{
    NullEquality,
    tree_node::{Transformed, TransformedResult, TreeNode},
};
use datafusion_expr::{Expr, LogicalPlan, Operator, expr_fn::binary_expr};
use datafusion_sql::unparser::Unparser;

pub(super) fn plan_to_sql(plan: &LogicalPlan) -> datafusion_common::Result<String> {
    let plan = preserve_null_equality(plan)?;
    Unparser::default()
        .plan_to_sql(&plan)
        .map(|statement| statement.to_string())
}

fn preserve_null_equality(plan: &LogicalPlan) -> datafusion_common::Result<LogicalPlan> {
    plan.clone()
        .transform_up(|plan| match plan {
            LogicalPlan::Join(mut join)
                if join.null_equality == NullEquality::NullEqualsNull && !join.on.is_empty() =>
            {
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
