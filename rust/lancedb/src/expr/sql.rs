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

/// Translate SQL-standard double-quoted identifiers into the backtick-quoted
/// identifiers expected by Lance's SQL parser.
///
/// Lance historically interpreted double-quoted values as string literals.
/// Rewriting them at the query boundary avoids silently evaluating a predicate
/// such as `"mixedCase" = 'value'` as a comparison between two literals. String
/// contents and existing backtick-quoted identifiers are left unchanged.
pub fn normalize_sql_filter(filter: &str) -> crate::Result<String> {
    #[derive(Clone, Copy, PartialEq, Eq)]
    enum Quote {
        None,
        Single,
        Backtick,
        Double,
    }

    let mut normalized = String::with_capacity(filter.len());
    let mut chars = filter.chars().peekable();
    let mut quote = Quote::None;

    while let Some(ch) = chars.next() {
        match quote {
            Quote::None => match ch {
                '\'' => {
                    normalized.push(ch);
                    quote = Quote::Single;
                }
                '`' => {
                    normalized.push(ch);
                    quote = Quote::Backtick;
                }
                '"' => {
                    normalized.push('`');
                    quote = Quote::Double;
                }
                _ => normalized.push(ch),
            },
            Quote::Single => {
                normalized.push(ch);
                if ch == '\\' {
                    if let Some(escaped) = chars.next() {
                        normalized.push(escaped);
                    }
                } else if ch == '\'' {
                    if chars.peek() == Some(&'\'') {
                        normalized.push(chars.next().expect("peeked character must exist"));
                    } else {
                        quote = Quote::None;
                    }
                }
            }
            Quote::Backtick => {
                normalized.push(ch);
                if ch == '`' {
                    if chars.peek() == Some(&'`') {
                        normalized.push(chars.next().expect("peeked character must exist"));
                    } else {
                        quote = Quote::None;
                    }
                }
            }
            Quote::Double => {
                if ch == '"' {
                    if chars.peek() == Some(&'"') {
                        // SQL escapes a double quote within an identifier by
                        // doubling it. A quote needs no escaping inside Lance's
                        // backtick-delimited form.
                        normalized.push('"');
                        chars.next();
                    } else {
                        normalized.push('`');
                        quote = Quote::None;
                    }
                } else if ch == '`' {
                    // Lance escapes a backtick within an identifier by doubling it.
                    normalized.push_str("``");
                } else {
                    normalized.push(ch);
                }
            }
        }
    }

    if quote == Quote::Double {
        return Err(crate::Error::InvalidInput {
            message: "unterminated double-quoted identifier in SQL filter".to_string(),
        });
    }

    Ok(normalized)
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

#[cfg(test)]
mod tests {
    use super::normalize_sql_filter;

    #[test]
    fn normalizes_double_quoted_identifiers() {
        assert_eq!(
            normalize_sql_filter(r#""PartyAbbrev" = 'D'"#).unwrap(),
            "`PartyAbbrev` = 'D'"
        );
        assert_eq!(
            normalize_sql_filter(r#""MetaData"."userId" = 5"#).unwrap(),
            "`MetaData`.`userId` = 5"
        );
        assert_eq!(normalize_sql_filter(r#""a""b" = 1"#).unwrap(), "`a\"b` = 1");
    }

    #[test]
    fn preserves_quotes_inside_literals_and_backticks() {
        let filter = r#"name = 'Alice "Ace"' AND `quoted"field` = 1"#;
        assert_eq!(normalize_sql_filter(filter).unwrap(), filter);
    }

    #[test]
    fn rejects_unterminated_double_quoted_identifier() {
        let error = normalize_sql_filter(r#""PartyAbbrev = 'D'"#).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("unterminated double-quoted identifier")
        );
    }
}
