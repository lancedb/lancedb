// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use datafusion_expr::Expr;
use datafusion_sql::unparser;

pub fn expr_to_sql_string(expr: &Expr) -> crate::Result<String> {
    let ast = unparser::expr_to_sql(expr).map_err(|e| crate::Error::InvalidInput {
        message: format!("failed to serialize expression to SQL: {}", e),
    })?;
    Ok(ast.to_string())
}
