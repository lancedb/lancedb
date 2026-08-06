// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Aggregate specification for DataFusion aggregates.

use datafusion::logical_expr::Expr;

use crate::planner::Planner;

/// Aggregate specification with group by and aggregate expressions.
#[derive(Debug, Clone)]
pub struct Aggregate {
    /// Expressions to group by (e.g., column references).
    pub group_by: Vec<Expr>,
    /// Aggregate function expressions (e.g., SUM, COUNT, AVG).
    /// Use `.alias()` on the expression to set output column names.
    pub aggregates: Vec<Expr>,
}

impl Aggregate {
    /// Create a new Aggregate.
    pub fn new(group_by: Vec<Expr>, aggregates: Vec<Expr>) -> Self {
        Self {
            group_by,
            aggregates,
        }
    }

    /// Compute column names required by this aggregate.
    ///
    /// For COUNT(*), this returns empty. For SUM(x), GROUP BY y, this returns [x, y].
    pub fn required_columns(&self) -> Vec<String> {
        let mut required_columns = Vec::new();
        for expr in self.group_by.iter().chain(self.aggregates.iter()) {
            required_columns.extend(Planner::column_names_in_expr(expr));
        }
        required_columns.sort();
        required_columns.dedup();
        required_columns
    }
}
