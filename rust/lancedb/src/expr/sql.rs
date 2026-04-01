// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use datafusion_expr::Expr;
use datafusion_sql::unparser::{self, dialect::Dialect};

/// Unparser dialect that matches the quoting style expected by the Lance SQL
/// parser.  Lance uses backtick (`` ` ``) as the only delimited-identifier
/// quote character, so we must produce `` `firstName` `` rather than
/// `"firstName"` for identifiers that require quoting.
///
/// We quote an identifier when it:
/// * is a SQL reserved word, OR
/// * contains characters outside `[a-zA-Z0-9_]`, OR
/// * starts with a digit, OR
/// * contains upper-case letters (unquoted identifiers are normalised to
///   lower-case by the SQL parser, which would break case-sensitive schemas).
struct LanceSqlDialect;

impl Dialect for LanceSqlDialect {
    fn identifier_quote_style(&self, identifier: &str) -> Option<char> {
        let needs_quote = identifier.chars().any(|c| c.is_ascii_uppercase())
            || !identifier
                .chars()
                .enumerate()
                .all(|(i, c)| c == '_' || c.is_ascii_alphabetic() || (i > 0 && c.is_ascii_digit()));
        if needs_quote { Some('`') } else { None }
    }
}

pub fn expr_to_sql_string(expr: &Expr) -> crate::Result<String> {
    let ast = unparser::Unparser::new(&LanceSqlDialect)
        .expr_to_sql(expr)
        .map_err(|e| crate::Error::InvalidInput {
            message: format!("failed to serialize expression to SQL: {}", e),
        })?;
    Ok(ast.to_string())
}
