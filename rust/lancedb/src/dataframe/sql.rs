// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! SQL lowering for LanceDB DataFrame plans.

use std::sync::Arc;

use datafusion_common::{
    Column, NullEquality, TableReference,
    tree_node::{Transformed, TransformedResult, TreeNode},
};
use datafusion_expr::{
    Expr, JoinType, LogicalPlan, LogicalPlanBuilder, Operator, expr_fn::binary_expr,
    expr_rewriter::NamePreserver, logical_plan::Projection,
};
use datafusion_sql::unparser::Unparser;

pub(super) fn plan_to_sql(plan: &LogicalPlan) -> datafusion_common::Result<String> {
    let plan = collapse_join_output_projections(plan)?;
    let plan = preserve_join_semantics(&plan)?;
    let plan = ensure_output_projection(plan)?;
    Unparser::default()
        .plan_to_sql(&plan)
        .map(|statement| statement.to_string())
}

fn collapse_join_output_projections(plan: &LogicalPlan) -> datafusion_common::Result<LogicalPlan> {
    plan.clone()
        .transform_up(|plan| {
            let LogicalPlan::Projection(projection) = plan else {
                return Ok(Transformed::no(plan));
            };
            let LogicalPlan::Projection(input) = projection.input.as_ref() else {
                return Ok(Transformed::no(LogicalPlan::Projection(projection)));
            };
            if !matches!(input.input.as_ref(), LogicalPlan::Join(_)) {
                return Ok(Transformed::no(LogicalPlan::Projection(projection)));
            }

            let name_preserver = NamePreserver::new_for_projection();
            let expr = projection
                .expr
                .into_iter()
                .map(|expr| {
                    let original_name = name_preserver.save(&expr);
                    expr.transform_up(|expr| match expr {
                        Expr::Column(column) => {
                            let index = input.schema.index_of_column(&column)?;
                            Ok(Transformed::yes(
                                input.expr[index].clone().unalias_nested().data,
                            ))
                        }
                        expr => Ok(Transformed::no(expr)),
                    })
                    .map(|result| original_name.restore(result.data))
                })
                .collect::<datafusion_common::Result<Vec<_>>>()?;
            let projection =
                Projection::try_new_with_schema(expr, Arc::clone(&input.input), projection.schema)?;
            Ok(Transformed::yes(LogicalPlan::Projection(projection)))
        })
        .data()
}

fn ensure_output_projection(plan: LogicalPlan) -> datafusion_common::Result<LogicalPlan> {
    if !output_needs_projection(&plan) {
        return Ok(plan);
    }
    let projection = plan
        .schema()
        .iter()
        .map(|(qualifier, field)| Expr::Column(Column::new(qualifier.cloned(), field.name())))
        .collect::<Vec<_>>();
    LogicalPlanBuilder::from(Arc::new(plan))
        .project(projection)?
        .build()
}

fn output_needs_projection(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Aggregate(_) => true,
        LogicalPlan::Join(join) => matches!(
            join.join_type,
            JoinType::LeftSemi | JoinType::RightSemi | JoinType::LeftAnti | JoinType::RightAnti
        ),
        LogicalPlan::Filter(filter) => output_needs_projection(&filter.input),
        LogicalPlan::Sort(sort) => output_needs_projection(&sort.input),
        LogicalPlan::Limit(limit) => output_needs_projection(&limit.input),
        LogicalPlan::Distinct(distinct) => output_needs_projection(distinct.input()),
        LogicalPlan::SubqueryAlias(alias) => output_needs_projection(&alias.input),
        _ => false,
    }
}

fn preserve_join_semantics(plan: &LogicalPlan) -> datafusion_common::Result<LogicalPlan> {
    plan.clone()
        .transform_up(|plan| match plan {
            LogicalPlan::Projection(mut projection)
                if projection_input_needs_isolation(&projection.input) =>
            {
                projection.input = isolate_plan(projection.input, "__lancedb_projection_input")?;
                Ok(Transformed::yes(LogicalPlan::Projection(projection)))
            }
            LogicalPlan::Filter(mut filter) if filter_input_needs_isolation(&filter.input) => {
                filter.input = isolate_plan(filter.input, "__lancedb_filter_input")?;
                Ok(Transformed::yes(LogicalPlan::Filter(filter)))
            }
            LogicalPlan::Aggregate(mut aggregate)
                if aggregate_input_needs_isolation(&aggregate.input) =>
            {
                aggregate.input = isolate_plan(aggregate.input, "__lancedb_aggregate_input")?;
                Ok(Transformed::yes(LogicalPlan::Aggregate(aggregate)))
            }
            LogicalPlan::Sort(mut sort)
                if matches!(
                    sort.input.as_ref(),
                    LogicalPlan::Limit(_) | LogicalPlan::Sort(_)
                ) =>
            {
                sort.input = isolate_plan(sort.input, "__lancedb_sort_input")?;
                Ok(Transformed::yes(LogicalPlan::Sort(sort)))
            }
            LogicalPlan::Limit(mut limit)
                if matches!(limit.input.as_ref(), LogicalPlan::Limit(_)) =>
            {
                limit.input = isolate_plan(limit.input, "__lancedb_limit_input")?;
                Ok(Transformed::yes(LogicalPlan::Limit(limit)))
            }
            LogicalPlan::Distinct(mut distinct)
                if matches!(distinct.input().as_ref(), LogicalPlan::Limit(_)) =>
            {
                match &mut distinct {
                    datafusion_expr::logical_plan::Distinct::All(input) => {
                        *input = isolate_plan(input.clone(), "__lancedb_distinct_input")?;
                    }
                    datafusion_expr::logical_plan::Distinct::On(on) => {
                        on.input = isolate_plan(on.input.clone(), "__lancedb_distinct_input")?;
                    }
                }
                Ok(Transformed::yes(LogicalPlan::Distinct(distinct)))
            }
            LogicalPlan::Union(mut union) => {
                let mut transformed = false;
                for (index, input) in union.inputs.iter_mut().enumerate() {
                    if set_input_needs_isolation(input) {
                        *input =
                            isolate_plan(input.clone(), &format!("__lancedb_set_input_{index}"))?;
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
                    join.left = isolate_plan(join.left, "__lancedb_left_input")?;
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
                    join.right = isolate_plan(join.right, "__lancedb_right_input")?;
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
            | LogicalPlan::Projection(_)
    ) || plan.inputs().into_iter().any(contains_join_modifier)
}

fn projection_input_needs_isolation(plan: &LogicalPlan) -> bool {
    matches!(
        plan,
        LogicalPlan::Projection(_)
            | LogicalPlan::Aggregate(_)
            | LogicalPlan::Limit(_)
            | LogicalPlan::Sort(_)
            | LogicalPlan::Distinct(_)
            | LogicalPlan::Union(_)
            | LogicalPlan::Window(_)
    )
}

fn aggregate_input_needs_isolation(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Projection(projection) => projection_establishes_scope(projection),
        LogicalPlan::Aggregate(_)
        | LogicalPlan::Limit(_)
        | LogicalPlan::Sort(_)
        | LogicalPlan::Distinct(_)
        | LogicalPlan::Union(_)
        | LogicalPlan::Window(_) => true,
        _ => false,
    }
}

fn filter_input_needs_isolation(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Projection(projection) => projection_establishes_scope(projection),
        LogicalPlan::Limit(_) | LogicalPlan::Union(_) => true,
        LogicalPlan::Distinct(distinct) => filter_input_needs_isolation(distinct.input()),
        _ => false,
    }
}

fn projection_establishes_scope(projection: &Projection) -> bool {
    projection
        .expr
        .iter()
        .any(|expression| !matches!(expression, Expr::Column(_)))
}

fn set_input_needs_isolation(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Limit(_) | LogicalPlan::Sort(_) => true,
        LogicalPlan::Filter(filter) => set_input_needs_isolation(&filter.input),
        LogicalPlan::Distinct(distinct) => {
            matches!(distinct.input().as_ref(), LogicalPlan::Union(_))
                || set_input_needs_isolation(distinct.input())
        }
        _ => false,
    }
}

fn isolate_plan(
    input: Arc<LogicalPlan>,
    fallback_alias: &str,
) -> datafusion_common::Result<Arc<LogicalPlan>> {
    let qualifier = input
        .schema()
        .iter()
        .find_map(|(qualifier, _)| qualifier.cloned())
        .unwrap_or_else(|| TableReference::bare(fallback_alias));
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
