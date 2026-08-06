// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::collections::{HashMap, HashSet};

use arrow_array::types::{
    Decimal32Type, Decimal64Type, Decimal128Type, Decimal256Type, DecimalType,
};
use arrow_schema::DataType;
use datafusion_common::ScalarValue;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_expr::Expr;
use datafusion_functions::core::expr_fn::{
    arrow_cast as datafusion_arrow_cast, arrow_try_cast as datafusion_arrow_try_cast,
};
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

fn typed_string_literal(value: String, data_type: DataType) -> Expr {
    datafusion_arrow_cast(
        Expr::Literal(ScalarValue::Utf8(Some(value)), None),
        Expr::Literal(ScalarValue::Utf8(Some(data_type.to_string())), None),
    )
}

fn cast_requires_arrow_type_name(data_type: &DataType) -> bool {
    !matches!(
        data_type,
        DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64
            | DataType::Timestamp(_, _)
            | DataType::Date32
            | DataType::Date64
            | DataType::Interval(_)
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Utf8View
            | DataType::Decimal32(_, _)
            | DataType::Decimal64(_, _)
            | DataType::Decimal128(_, _)
    )
}

fn next_binary_placeholder(user_strings: &HashSet<String>, next_id: &mut usize) -> String {
    loop {
        let placeholder = format!("{BINARY_PLACEHOLDER_PREFIX}{}__", *next_id);
        *next_id += 1;
        if !user_strings.contains(&placeholder) {
            return placeholder;
        }
    }
}

fn bind_binary_literals(
    sql: &str,
    mut bindings: HashMap<String, Vec<u8>>,
) -> crate::Result<String> {
    let bytes = sql.as_bytes();
    let mut output = Vec::with_capacity(bytes.len());
    let mut index = 0;

    // Walk SQL string tokens once. Placeholders are plain, unescaped string
    // literals, so this remains linear even when user strings are large or
    // deliberately resemble the placeholder prefix.
    while index < bytes.len() {
        if bytes[index] != b'\'' {
            output.push(bytes[index]);
            index += 1;
            continue;
        }

        let literal_start = index;
        index += 1;
        let content_start = index;
        let mut escaped = false;
        let mut content_end = None;
        while index < bytes.len() {
            if bytes[index] == b'\'' {
                if index + 1 < bytes.len() && bytes[index + 1] == b'\'' {
                    escaped = true;
                    index += 2;
                } else {
                    content_end = Some(index);
                    index += 1;
                    break;
                }
            } else {
                index += 1;
            }
        }

        let Some(content_end) = content_end else {
            return Err(crate::Error::InvalidInput {
                message: "unterminated string while binding binary literal".to_string(),
            });
        };

        let placeholder = &sql[content_start..content_end];
        if !escaped && let Some(value) = bindings.remove(placeholder) {
            output.extend_from_slice(bytes_to_hex_sql(&value).as_bytes());
        } else {
            output.extend_from_slice(&bytes[literal_start..index]);
        }
    }

    if !bindings.is_empty() {
        return Err(crate::Error::InvalidInput {
            message: "failed to bind binary literal while serializing expression".to_string(),
        });
    }

    String::from_utf8(output).map_err(|e| crate::Error::InvalidInput {
        message: format!("failed to bind binary literal: {e}"),
    })
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
    // Eliminate empty membership expressions before visiting their children.
    // Otherwise a discarded binary child could leave behind a stale binding.
    let rewritten = expr
        .clone()
        .transform(|e: Expr| match e {
            Expr::InList(in_list) if in_list.list.is_empty() => Ok(Transformed::yes(
                Expr::Literal(ScalarValue::Boolean(Some(in_list.negated)), None),
            )),
            other => Ok(Transformed::no(other)),
        })
        .map_err(|e| crate::Error::InvalidInput {
            message: format!("failed to rewrite expression: {e}"),
        })?
        .data;

    let user_strings = string_literals(&rewritten);
    let mut next_placeholder_id = 0;
    let mut binary_bindings = HashMap::new();
    let rewritten = rewritten
        .transform(|e: Expr| match e {
            Expr::Literal(ScalarValue::Binary(Some(bytes)), m)
            | Expr::Literal(ScalarValue::LargeBinary(Some(bytes)), m) => {
                let placeholder = next_binary_placeholder(&user_strings, &mut next_placeholder_id);
                binary_bindings.insert(placeholder.clone(), bytes);
                Ok(Transformed::yes(Expr::Literal(
                    ScalarValue::Utf8(Some(placeholder)),
                    m,
                )))
            }
            Expr::Literal(ScalarValue::Binary(None), m)
            | Expr::Literal(ScalarValue::LargeBinary(None), m) => {
                Ok(Transformed::yes(Expr::Literal(ScalarValue::Null, m)))
            }
            Expr::Literal(ScalarValue::Decimal32(Some(value), precision, scale), _m) => {
                let value = Decimal32Type::format_decimal(value, precision, scale);
                Ok(Transformed::yes(typed_string_literal(
                    value,
                    DataType::Decimal32(precision, scale),
                )))
            }
            Expr::Literal(ScalarValue::Decimal64(Some(value), precision, scale), _m) => {
                let value = Decimal64Type::format_decimal(value, precision, scale);
                Ok(Transformed::yes(typed_string_literal(
                    value,
                    DataType::Decimal64(precision, scale),
                )))
            }
            Expr::Literal(ScalarValue::Decimal128(Some(value), precision, scale), _m) => {
                let value = Decimal128Type::format_decimal(value, precision, scale);
                Ok(Transformed::yes(typed_string_literal(
                    value,
                    DataType::Decimal128(precision, scale),
                )))
            }
            Expr::Literal(ScalarValue::Decimal256(Some(value), precision, scale), _m) => {
                let value = Decimal256Type::format_decimal(value, precision, scale);
                Ok(Transformed::yes(typed_string_literal(
                    value,
                    DataType::Decimal256(precision, scale),
                )))
            }
            Expr::Literal(ScalarValue::Float16(Some(value)), _m) if !value.is_finite() => Ok(
                Transformed::yes(typed_string_literal(value.to_string(), DataType::Float16)),
            ),
            Expr::Literal(ScalarValue::Float32(Some(value)), _m) if !value.is_finite() => Ok(
                Transformed::yes(typed_string_literal(value.to_string(), DataType::Float32)),
            ),
            Expr::Literal(ScalarValue::Float64(Some(value)), _m) if !value.is_finite() => Ok(
                Transformed::yes(typed_string_literal(value.to_string(), DataType::Float64)),
            ),
            Expr::Cast(cast) if cast_requires_arrow_type_name(cast.field.data_type()) => {
                Ok(Transformed::yes(datafusion_arrow_cast(
                    *cast.expr,
                    Expr::Literal(
                        ScalarValue::Utf8(Some(cast.field.data_type().to_string())),
                        None,
                    ),
                )))
            }
            Expr::TryCast(cast) if cast_requires_arrow_type_name(cast.field.data_type()) => {
                Ok(Transformed::yes(datafusion_arrow_try_cast(
                    *cast.expr,
                    Expr::Literal(
                        ScalarValue::Utf8(Some(cast.field.data_type().to_string())),
                        None,
                    ),
                )))
            }
            other => Ok(Transformed::no(other)),
        })
        .map_err(|e| crate::Error::InvalidInput {
            message: format!("failed to rewrite expression: {}", e),
        })?
        .data;

    let sql = run_unparser(&rewritten)?;
    if binary_bindings.is_empty() {
        Ok(sql)
    } else {
        bind_binary_literals(&sql, binary_bindings)
    }
}
