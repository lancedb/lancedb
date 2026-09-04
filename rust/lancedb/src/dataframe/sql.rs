// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! SQL lowering for LanceDB DataFrame plans.

use datafusion_expr::LogicalPlan;
use datafusion_sql::unparser::Unparser;

pub(super) fn plan_to_sql(plan: &LogicalPlan) -> datafusion_common::Result<String> {
    Unparser::default()
        .with_pretty(true)
        .plan_to_sql(plan)
        .map(|statement| statement.to_string())
}
