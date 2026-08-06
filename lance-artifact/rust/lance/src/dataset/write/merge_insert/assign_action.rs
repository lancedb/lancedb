// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use super::{MERGE_SOURCE_SENTINEL, MergeInsertParams, WhenNotMatchedBySource};
use crate::{Result, dataset::WhenMatched};
use datafusion::common::{
    Column, TableReference,
    tree_node::{Transformed, TransformedResult, TreeNode},
};
use datafusion::scalar::ScalarValue;
use datafusion_expr::{Case, Expr, col};

// Note: right now, this is a fixed enum. In the future, this will need to be
// dynamic to support multiple merge insert update clauses like:
// ```sql
// MERGE my_table USING input ON table.id = input.id
// WHEN MATCHED AND input.event = "new_date" THEN UPDATE SET my_table.date = input.date
// WHEN MATCHED AND input.event = "new_name" THEN UPDATE SET my_table.name = input.new_name
// ```
// At that point we will have a variable number of actions.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum Action {
    Nothing = 0,
    /// Update all columns with source values
    UpdateAll = 1,
    Insert = 2,
    Delete = 3,
    /// Fail the operation if a match is found
    Fail = 4,
}

impl TryFrom<u8> for Action {
    type Error = crate::Error;

    fn try_from(value: u8) -> std::result::Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Nothing),
            1 => Ok(Self::UpdateAll),
            2 => Ok(Self::Insert),
            3 => Ok(Self::Delete),
            4 => Ok(Self::Fail),
            _ => Err(crate::Error::invalid_input(format!(
                "Invalid action code: {}",
                value
            ))),
        }
    }
}

impl Action {
    fn as_literal_expr(&self) -> Expr {
        Expr::Literal(ScalarValue::UInt8(Some(*self as u8)), None)
    }
}

fn qualify_unqualified_columns(expr: Expr, relation: &'static str) -> Result<Expr> {
    expr.transform(|expr| {
        Ok(if let Expr::Column(column) = expr {
            if column.relation.is_none() {
                let qualified = Column::new_unqualified(column.name)
                    .with_relation(TableReference::bare(relation));
                Transformed::yes(Expr::Column(qualified))
            } else {
                Transformed::no(Expr::Column(column))
            }
        } else {
            Transformed::no(expr)
        })
    })
    .data()
    .map_err(crate::Error::from)
}

/// Transforms merge insert parameters into a logical expression. The output
/// is a single "action" column, that describes what to do with each row.
pub fn merge_insert_action(
    params: &MergeInsertParams,
    schema: Option<&arrow_schema::Schema>,
) -> Result<Expr> {
    // Use a sentinel column to detect whether the source side contributed a row to the
    // join output.  This is NULL-safe: the sentinel is `true` for every source row and
    // is NULL-filled by the outer join for target-only rows, regardless of whether any
    // ON column contains NULL.  Using ON key columns for this purpose is incorrect
    // because a key column that is legitimately NULL is indistinguishable from a NULL
    // introduced by the outer join on the target side.
    let source_has_row = col(format!("source.\"{}\"", MERGE_SOURCE_SENTINEL)).is_not_null();

    let target_has_row = col("target._rowaddr").is_not_null();
    let matched = source_has_row.clone().and(target_has_row.clone());

    let source_only = source_has_row.and(col("target._rowaddr").is_null());

    let target_only =
        target_has_row.and(col(format!("source.\"{}\"", MERGE_SOURCE_SENTINEL)).is_null());

    let mut cases = vec![];

    if params.insert_not_matched {
        cases.push((source_only, Action::Insert.as_literal_expr()));
    }

    match &params.when_matched {
        WhenMatched::UpdateAll => {
            cases.push((matched, Action::UpdateAll.as_literal_expr()));
        }
        WhenMatched::UpdateIf(condition_str) => {
            // Parse the condition with qualified column references enabled for fast path
            if let Some(dataset_schema) = schema {
                let planner = lance_datafusion::planner::Planner::new(std::sync::Arc::new(
                    dataset_schema.clone(),
                ))
                .with_enable_relations(true);
                let condition = planner.parse_filter(condition_str).map_err(|e| {
                    crate::Error::invalid_input(format!(
                        "Failed to parse UpdateIf condition: {}",
                        e
                    ))
                })?;
                cases.push((matched.and(condition), Action::UpdateAll.as_literal_expr()));
            } else {
                // Fallback - this shouldn't happen in the fast path
                return Err(crate::Error::internal(
                    "Schema required for UpdateIf parsing",
                ));
            }
        }
        WhenMatched::UpdateIfExpr(condition) => {
            cases.push((
                matched.and(condition.clone()),
                Action::UpdateAll.as_literal_expr(),
            ));
        }
        WhenMatched::DoNothing => {}
        WhenMatched::Fail => {
            cases.push((matched, Action::Fail.as_literal_expr()));
        }
        WhenMatched::Delete => {
            cases.push((matched, Action::Delete.as_literal_expr()));
        }
    }

    match &params.delete_not_matched_by_source {
        WhenNotMatchedBySource::Delete => {
            cases.push((target_only, Action::Delete.as_literal_expr()));
        }
        WhenNotMatchedBySource::DeleteIf(condition) => {
            let target_condition = qualify_unqualified_columns(condition.clone(), "target")?;
            cases.push((
                target_only.and(target_condition),
                Action::Delete.as_literal_expr(),
            ));
        }
        WhenNotMatchedBySource::Keep => {}
    }

    Ok(Expr::Case(Case {
        expr: None,
        when_then_expr: cases
            .into_iter()
            .map(|(when, then)| (Box::new(when), Box::new(then)))
            .collect(),
        else_expr: Some(Box::new(Action::Nothing.as_literal_expr())),
    }))
}
