// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::collections::HashSet;

use arrow_array::types::{
    Decimal32Type, Decimal64Type, Decimal128Type, Decimal256Type, DecimalType,
};
use arrow_schema::DataType;
use datafusion_common::ScalarValue;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_expr::{Expr, expr::Cast};
use datafusion_sql::sqlparser::keywords::ALL_KEYWORDS;
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
        let identifier_upper = identifier.to_ascii_uppercase();
        let needs_quote =
            (identifier_upper != "ID" && ALL_KEYWORDS.contains(&identifier_upper.as_str()))
                || identifier.chars().any(|c| c.is_ascii_uppercase())
                || !identifier.chars().enumerate().all(|(i, c)| {
                    c == '_' || c.is_ascii_alphabetic() || (i > 0 && c.is_ascii_digit())
                });
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

fn string_literals(expr: &Expr) -> HashSet<String> {
    let mut literals = HashSet::new();
    let _ = expr.apply(&mut |e: &Expr| {
        if let Expr::Literal(
            ScalarValue::Utf8(Some(value))
            | ScalarValue::LargeUtf8(Some(value))
            | ScalarValue::Utf8View(Some(value)),
            _,
        ) = e
        {
            literals.insert(value.clone());
        }
        Ok(TreeNodeRecursion::Continue)
    });
    literals
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
    // DataFusion's unparser needs a few adaptations before its SQL can be
    // reparsed by Lance without changing the typed expression's semantics:
    //
    // * decimal literals need an explicit cast to preserve precision and scale;
    // * an empty IN list is valid in DataFusion but invalid SQL;
    // * binary literals are unsupported by the unparser and need placeholders.
    let user_strings = string_literals(expr);
    let mut binary_bindings: Vec<(String, Vec<u8>)> = Vec::new();
    let rewritten = expr
        .clone()
        .transform(|e: Expr| match e {
            Expr::Literal(ScalarValue::Binary(Some(bytes)), m)
            | Expr::Literal(ScalarValue::LargeBinary(Some(bytes)), m) => {
                let mut placeholder =
                    format!("{}{}__", BINARY_PLACEHOLDER_PREFIX, binary_bindings.len());
                while user_strings
                    .iter()
                    .any(|value| value.contains(&placeholder))
                {
                    placeholder.push('_');
                }
                binary_bindings.push((placeholder.clone(), bytes));
                Ok(Transformed::yes(Expr::Literal(
                    ScalarValue::Utf8(Some(placeholder)),
                    m,
                )))
            }
            Expr::Literal(ScalarValue::Binary(None), m)
            | Expr::Literal(ScalarValue::LargeBinary(None), m) => {
                Ok(Transformed::yes(Expr::Literal(ScalarValue::Null, m)))
            }
            Expr::Literal(ScalarValue::Decimal32(Some(value), precision, scale), m) => {
                let value = Decimal32Type::format_decimal(value, precision, scale);
                Ok(Transformed::yes(Expr::Cast(Cast::new(
                    Box::new(Expr::Literal(ScalarValue::Utf8(Some(value)), m)),
                    DataType::Decimal32(precision, scale),
                ))))
            }
            Expr::Literal(ScalarValue::Decimal64(Some(value), precision, scale), m) => {
                let value = Decimal64Type::format_decimal(value, precision, scale);
                Ok(Transformed::yes(Expr::Cast(Cast::new(
                    Box::new(Expr::Literal(ScalarValue::Utf8(Some(value)), m)),
                    DataType::Decimal64(precision, scale),
                ))))
            }
            Expr::Literal(ScalarValue::Decimal128(Some(value), precision, scale), m) => {
                let value = Decimal128Type::format_decimal(value, precision, scale);
                Ok(Transformed::yes(Expr::Cast(Cast::new(
                    Box::new(Expr::Literal(ScalarValue::Utf8(Some(value)), m)),
                    DataType::Decimal128(precision, scale),
                ))))
            }
            Expr::Literal(ScalarValue::Decimal256(Some(value), precision, scale), m) => {
                let value = Decimal256Type::format_decimal(value, precision, scale);
                Ok(Transformed::yes(Expr::Cast(Cast::new(
                    Box::new(Expr::Literal(ScalarValue::Utf8(Some(value)), m)),
                    DataType::Decimal256(precision, scale),
                ))))
            }
            Expr::InList(in_list) if in_list.list.is_empty() => Ok(Transformed::yes(
                Expr::Literal(ScalarValue::Boolean(Some(in_list.negated)), None),
            )),
            other => Ok(Transformed::no(other)),
        })
        .map_err(|e| crate::Error::InvalidInput {
            message: format!("failed to rewrite expression: {}", e),
        })?
        .data;

    let mut sql = run_unparser(&rewritten)?;
    for (placeholder, bytes) in binary_bindings {
        // Each placeholder is unique and absent from every user string, so a
        // single replacement cannot rewrite an unrelated literal.
        let quoted = format!("'{placeholder}'");
        if !sql.contains(&quoted) {
            return Err(crate::Error::InvalidInput {
                message: "failed to bind binary literal while serializing expression".to_string(),
            });
        }
        sql = sql.replacen(&quoted, &bytes_to_hex_sql(&bytes), 1);
    }
    Ok(sql)
}
