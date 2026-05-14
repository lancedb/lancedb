// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use datafusion_common::ScalarValue;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
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

/// Prefix for placeholder strings inserted in place of binary literals.  Chosen
/// to be extremely unlikely to occur in user data.
const BINARY_PLACEHOLDER_PREFIX: &str = "__lancedb_binary_placeholder_";

fn bytes_to_hex_sql(bytes: &[u8]) -> String {
    let hex: String = bytes.iter().map(|b| format!("{b:02X}")).collect();
    format!("X'{hex}'")
}

/// Returns true if *expr* contains a `Binary` or `LargeBinary` scalar literal
/// anywhere in its subtree.  DataFusion's SQL unparser cannot serialize those
/// variants, so we route such expressions through a placeholder-substitution
/// path that emits SQL `X'...'` byte-string literals.
fn has_binary_literal(expr: &Expr) -> bool {
    let mut found = false;
    let _ = expr.apply(&mut |e: &Expr| {
        if matches!(
            e,
            Expr::Literal(ScalarValue::Binary(_) | ScalarValue::LargeBinary(_), _)
        ) {
            found = true;
            Ok(TreeNodeRecursion::Stop)
        } else {
            Ok(TreeNodeRecursion::Continue)
        }
    });
    found
}

fn run_unparser(expr: &Expr) -> crate::Result<String> {
    let ast = unparser::Unparser::new(&LanceSqlDialect)
        .expr_to_sql(expr)
        .map_err(|e| crate::Error::InvalidInput {
            message: format!("failed to serialize expression to SQL: {}", e),
        })?;
    Ok(ast.to_string())
}

pub fn expr_to_sql_string(expr: &Expr) -> crate::Result<String> {
    // Fast path: no binary literals — DataFusion's unparser handles everything.
    if !has_binary_literal(expr) {
        return run_unparser(expr);
    }

    // Slow path: DataFusion's unparser cannot serialize `Binary`/`LargeBinary`
    // scalars, so we rewrite each one to a unique string-literal placeholder,
    // let the unparser do the rest of the work, then substitute the SQL
    // `X'...'` byte-string literal back in.  This keeps the operator/function
    // serialization logic centralized in DataFusion and works for every
    // expression node type the unparser supports.
    let mut bindings: Vec<Vec<u8>> = Vec::new();
    let rewritten = expr
        .clone()
        .transform(|e: Expr| match e {
            Expr::Literal(ScalarValue::Binary(Some(bytes)), m)
            | Expr::Literal(ScalarValue::LargeBinary(Some(bytes)), m) => {
                let placeholder = format!("{}{}__", BINARY_PLACEHOLDER_PREFIX, bindings.len());
                bindings.push(bytes);
                Ok(Transformed::yes(Expr::Literal(
                    ScalarValue::Utf8(Some(placeholder)),
                    m,
                )))
            }
            Expr::Literal(ScalarValue::Binary(None), m)
            | Expr::Literal(ScalarValue::LargeBinary(None), m) => {
                Ok(Transformed::yes(Expr::Literal(ScalarValue::Null, m)))
            }
            other => Ok(Transformed::no(other)),
        })
        .map_err(|e| crate::Error::InvalidInput {
            message: format!("failed to rewrite expression: {}", e),
        })?
        .data;

    let mut sql = run_unparser(&rewritten)?;
    for (i, bytes) in bindings.iter().enumerate() {
        // The unparser quotes string literals with single quotes, so the
        // placeholder appears as `'__lancedb_binary_placeholder_<i>__'`.
        let quoted = format!("'{}{}__'", BINARY_PLACEHOLDER_PREFIX, i);
        sql = sql.replace(&quoted, &bytes_to_hex_sql(bytes));
    }
    Ok(sql)
}
