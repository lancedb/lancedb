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
            LogicalPlan::Filter(mut filter)
                if matches!(filter.input.as_ref(), LogicalPlan::Limit(_)) =>
            {
                filter.input = isolate_join_input(filter.input)?;
                Ok(Transformed::yes(LogicalPlan::Filter(filter)))
            }
            LogicalPlan::Sort(mut sort) if matches!(sort.input.as_ref(), LogicalPlan::Limit(_)) => {
                sort.input = isolate_join_input(sort.input)?;
                Ok(Transformed::yes(LogicalPlan::Sort(sort)))
            }
            LogicalPlan::Distinct(mut distinct)
                if matches!(distinct.input().as_ref(), LogicalPlan::Limit(_)) =>
            {
                match &mut distinct {
                    datafusion_expr::logical_plan::Distinct::All(input) => {
                        *input = isolate_join_input(input.clone())?;
                    }
                    datafusion_expr::logical_plan::Distinct::On(on) => {
                        on.input = isolate_join_input(on.input.clone())?;
                    }
                }
                Ok(Transformed::yes(LogicalPlan::Distinct(distinct)))
            }
            LogicalPlan::Union(mut union) => {
                let mut transformed = false;
                for input in &mut union.inputs {
                    if set_input_needs_isolation(input) {
                        *input = isolate_join_input(input.clone())?;
                        transformed = true;
                    }
                }
                Ok(if transformed {
                    Transformed::yes(LogicalPlan::Union(union))
                } else {
                    Transformed::no(LogicalPlan::Union(union))
                })
            }
            LogicalPlan::Join(mut join) => {
                let mut transformed = false;
                let left_has_filter = contains_filter(&join.left);
                let left_has_aggregate = contains_aggregate(&join.left);
                let left_needs_isolation =
                    left_has_filter || left_has_aggregate || contains_join_modifier(&join.left);
                if contains_join(&join.left) && left_needs_isolation {
                    return Err(datafusion_common::DataFusionError::NotImplemented(
                        "SQL lowering does not yet support this compound left join input"
                            .to_string(),
                    ));
                }
                if left_needs_isolation {
                    join.left = isolate_join_input(join.left)?;
                    transformed = true;
                }
                let right_has_filter = contains_filter(&join.right);
                let right_has_aggregate = contains_aggregate(&join.right);
                let right_needs_isolation =
                    right_has_filter || right_has_aggregate || contains_join_modifier(&join.right);
                if contains_join(&join.right) && right_needs_isolation {
                    return Err(datafusion_common::DataFusionError::NotImplemented(
                        "SQL lowering does not yet support this compound right join input"
                            .to_string(),
                    ));
                }
                if right_needs_isolation {
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

fn contains_aggregate(plan: &LogicalPlan) -> bool {
    matches!(plan, LogicalPlan::Aggregate(_)) || plan.inputs().into_iter().any(contains_aggregate)
}

fn contains_filter(plan: &LogicalPlan) -> bool {
    matches!(plan, LogicalPlan::Filter(_))
        || matches!(plan, LogicalPlan::TableScan(scan) if !scan.filters.is_empty())
        || plan.inputs().into_iter().any(contains_filter)
}

fn contains_join(plan: &LogicalPlan) -> bool {
    matches!(plan, LogicalPlan::Join(_)) || plan.inputs().into_iter().any(contains_join)
}

fn contains_join_modifier(plan: &LogicalPlan) -> bool {
    matches!(
        plan,
        LogicalPlan::Limit(_)
            | LogicalPlan::Sort(_)
            | LogicalPlan::Distinct(_)
            | LogicalPlan::Union(_)
            | LogicalPlan::Window(_)
    ) || plan.inputs().into_iter().any(contains_join_modifier)
}

fn set_input_needs_isolation(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Limit(_) | LogicalPlan::Sort(_) => true,
        LogicalPlan::Filter(filter) => set_input_needs_isolation(&filter.input),
        LogicalPlan::Distinct(distinct) => set_input_needs_isolation(distinct.input()),
        _ => false,
    }
}

fn isolate_join_input(input: Arc<LogicalPlan>) -> datafusion_common::Result<Arc<LogicalPlan>> {
    let Some(qualifier) = input
        .schema()
        .iter()
        .find_map(|(qualifier, _)| qualifier.cloned())
    else {
        return Err(datafusion_common::DataFusionError::NotImplemented(
            "SQL lowering cannot isolate an unqualified join input".to_string(),
        ));
    };
    if input
        .schema()
        .iter()
        .any(|(candidate, _)| candidate.is_some_and(|candidate| candidate != &qualifier))
    {
        return Err(datafusion_common::DataFusionError::NotImplemented(
            "SQL lowering cannot isolate a join input with multiple relation qualifiers"
                .to_string(),
        ));
    }

    let projection = input
        .schema()
        .iter()
        .map(|(field_qualifier, field)| {
            Expr::Column(Column::new(field_qualifier.cloned(), field.name()))
        })
        .collect::<Vec<_>>();
    let plan = LogicalPlanBuilder::from(input)
        .project(projection)?
        .alias(qualifier)?
        .build()?;
    Ok(Arc::new(plan))
}
