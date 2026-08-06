// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Extends logical expression.

use std::sync::Arc;

use arrow_schema::DataType;

use crate::expr::safe_coerce_scalar;
use datafusion::logical_expr::{Between, ScalarUDF, ScalarUDFImpl};
use datafusion::logical_expr::{BinaryExpr, Operator, expr::ScalarFunction};
use datafusion::prelude::*;
use datafusion::scalar::ScalarValue;
use datafusion_functions::core::getfield::GetFieldFunc;
use lance_arrow::DataTypeExt;

use lance_core::datatypes::Schema;
use lance_core::{Error, Result};
/// Resolve a Value
fn resolve_value(expr: &Expr, data_type: &DataType) -> Result<Expr> {
    match expr {
        Expr::Literal(scalar_value, metadata) => {
            Ok(Expr::Literal(safe_coerce_scalar(scalar_value, data_type).ok_or_else(|| Error::invalid_input(format!("Received literal {expr} and could not convert to literal of type '{data_type:?}'")))?, metadata.clone()))
        }
        _ => Err(Error::invalid_input(format!("Expected a literal of type '{data_type:?}' but received: {expr}"))),
    }
}

/// A simple helper function that interprets an Expr as a string scalar
/// or returns None if it is not.
pub fn get_as_string_scalar_opt(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Literal(ScalarValue::Utf8(Some(s)), _) => Some(s),
        _ => None,
    }
}

/// Given a Expr::Column or Expr::GetIndexedField, get the data type of referenced
/// field in the schema.
///
/// If the column is not found in the schema, return None. If the expression is
/// not a field reference, also returns None.
pub fn resolve_column_type(expr: &Expr, schema: &Schema) -> Option<DataType> {
    let mut field_path = Vec::new();
    let mut current_expr = expr;
    // We are looping from outer-most reference to inner-most.
    loop {
        match current_expr {
            Expr::Column(c) => {
                field_path.push(c.name.as_str());
                break;
            }
            Expr::ScalarFunction(udf) if udf.name() == GetFieldFunc::default().name() => {
                let name = get_as_string_scalar_opt(&udf.args[1])?;
                field_path.push(name);
                current_expr = &udf.args[0];
            }
            _ => return None,
        }
    }

    let mut path_iter = field_path.iter().rev();
    let mut field = schema.field(path_iter.next()?)?;
    for name in path_iter {
        if field.data_type().is_struct() {
            field = field.children.iter().find(|f| &f.name == name)?;
        } else {
            return None;
        }
    }
    Some(field.data_type())
}

/// Resolve logical expression `expr`.
///
/// Parameters
///
/// - *expr*: a datafusion logical expression
/// - *schema*: lance schema.
pub fn resolve_expr(expr: &Expr, schema: &Schema) -> Result<Expr> {
    match expr {
        Expr::Between(Between {
            expr: inner_expr,
            low,
            high,
            negated,
        }) => {
            if let Some(inner_expr_type) = resolve_column_type(inner_expr.as_ref(), schema) {
                Ok(Expr::Between(Between {
                    expr: inner_expr.clone(),
                    low: Box::new(coerce_expr(low.as_ref(), &inner_expr_type)?),
                    high: Box::new(coerce_expr(high.as_ref(), &inner_expr_type)?),
                    negated: *negated,
                }))
            } else {
                Ok(expr.clone())
            }
        }
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            if matches!(op, Operator::And | Operator::Or) {
                Ok(Expr::BinaryExpr(BinaryExpr {
                    left: Box::new(resolve_expr(left.as_ref(), schema)?),
                    op: *op,
                    right: Box::new(resolve_expr(right.as_ref(), schema)?),
                }))
            } else if let Some(left_type) = resolve_column_type(left.as_ref(), schema) {
                match right.as_ref() {
                    Expr::Literal(..) => Ok(Expr::BinaryExpr(BinaryExpr {
                        left: left.clone(),
                        op: *op,
                        right: Box::new(resolve_value(right.as_ref(), &left_type)?),
                    })),
                    // For cases complex expressions (not just literals) on right hand side like x = 1 + 1 + -2*2
                    Expr::BinaryExpr(r) => Ok(Expr::BinaryExpr(BinaryExpr {
                        left: left.clone(),
                        op: *op,
                        right: Box::new(Expr::BinaryExpr(BinaryExpr {
                            left: coerce_expr(&r.left, &left_type).map(Box::new)?,
                            op: r.op,
                            right: coerce_expr(&r.right, &left_type).map(Box::new)?,
                        })),
                    })),
                    _ => Ok(expr.clone()),
                }
            } else if let Some(right_type) = resolve_column_type(right.as_ref(), schema) {
                match left.as_ref() {
                    Expr::Literal(..) => Ok(Expr::BinaryExpr(BinaryExpr {
                        left: Box::new(resolve_value(left.as_ref(), &right_type)?),
                        op: *op,
                        right: right.clone(),
                    })),
                    _ => Ok(expr.clone()),
                }
            } else {
                Ok(expr.clone())
            }
        }
        Expr::InList(in_list) => {
            if matches!(in_list.expr.as_ref(), Expr::Column(_)) {
                if let Some(resolved_type) = resolve_column_type(in_list.expr.as_ref(), schema) {
                    let resolved_values = in_list
                        .list
                        .iter()
                        .map(|val| coerce_expr(val, &resolved_type))
                        .collect::<Result<Vec<_>>>()?;
                    Ok(Expr::in_list(
                        in_list.expr.as_ref().clone(),
                        resolved_values,
                        in_list.negated,
                    ))
                } else {
                    Ok(expr.clone())
                }
            } else {
                Ok(expr.clone())
            }
        }
        _ => {
            // Passthrough
            Ok(expr.clone())
        }
    }
}

/// Coerce expression of literals to column type.
///
/// Parameters
///
/// - *expr*: a datafusion logical expression
/// - *dtype*: a lance data type
pub fn coerce_expr(expr: &Expr, dtype: &DataType) -> Result<Expr> {
    match expr {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => Ok(Expr::BinaryExpr(BinaryExpr {
            left: Box::new(coerce_expr(left, dtype)?),
            op: *op,
            right: Box::new(coerce_expr(right, dtype)?),
        })),
        literal_expr @ Expr::Literal(..) => Ok(resolve_value(literal_expr, dtype)?),
        _ => Ok(expr.clone()),
    }
}

/// Coerce logical expression for filters to boolean.
///
/// Parameters
///
/// - *expr*: a datafusion logical expression
pub fn coerce_filter_type_to_boolean(expr: Expr) -> Expr {
    match expr {
        // Coerce regexp_match to boolean by checking for non-null
        Expr::ScalarFunction(sf) if sf.func.name() == "regexp_match" => {
            log::warn!(
                "regexp_match now is coerced to boolean, this may be changed in the future, please use `regexp_like` instead"
            );
            Expr::IsNotNull(Box::new(Expr::ScalarFunction(sf)))
        }

        // Recurse into boolean contexts so nested regexp_match terms are also coerced
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => Expr::BinaryExpr(BinaryExpr {
            left: Box::new(coerce_filter_type_to_boolean(*left)),
            op,
            right: Box::new(coerce_filter_type_to_boolean(*right)),
        }),
        Expr::Not(inner) => Expr::Not(Box::new(coerce_filter_type_to_boolean(*inner))),
        Expr::IsNull(inner) => Expr::IsNull(Box::new(coerce_filter_type_to_boolean(*inner))),
        Expr::IsNotNull(inner) => Expr::IsNotNull(Box::new(coerce_filter_type_to_boolean(*inner))),

        // Pass-through for all other nodes
        other => other,
    }
}

// As part of the DF 37 release there are now two different ways to
// represent a nested field access in `Expr`.  The old way is to use
// `Expr::field` which returns a `GetStructField` and the new way is
// to use `Expr::ScalarFunction` with a `GetFieldFunc` UDF.
//
// Currently, the old path leads to bugs in DF.  This is probably a
// bug and will probably be fixed in a future version.  In the meantime
// we need to make sure we are always using the new way to avoid this
// bug.  This trait adds field_newstyle which lets us easily create
// logical `Expr` that use the new style.
pub trait ExprExt {
    // Helper function to replace Expr::field in DF 37 since DF
    // confuses itself with the GetStructField returned by Expr::field
    fn field_newstyle(&self, name: &str) -> Expr;
}

impl ExprExt for Expr {
    fn field_newstyle(&self, name: &str) -> Expr {
        Self::ScalarFunction(ScalarFunction {
            func: Arc::new(ScalarUDF::new_from_impl(GetFieldFunc::default())),
            args: vec![
                self.clone(),
                Self::Literal(ScalarValue::Utf8(Some(name.to_string())), None),
            ],
        })
    }
}

/// Convert a field path string into a DataFusion expression.
///
/// This function handles:
/// - Simple column names: "column"
/// - Nested paths: "parent.child" or "parent.child.grandchild"
/// - Backtick-escaped field names: "parent.`field.with.dots`"
///
/// # Arguments
///
/// * `field_path` - The field path to convert. Supports simple columns, nested paths,
///   and backtick-escaped field names.
///
/// # Returns
///
/// Returns `Result<Expr>` - Ok with the DataFusion expression, or Err if the path
/// could not be parsed.
///
/// # Example
///
/// ```
/// use lance_datafusion::logical_expr::field_path_to_expr;
///
/// // Simple column
/// let expr = field_path_to_expr("column_name").unwrap();
///
/// // Nested field
/// let expr = field_path_to_expr("parent.child").unwrap();
///
/// // Backtick-escaped field with dots
/// let expr = field_path_to_expr("parent.`field.with.dots`").unwrap();
/// ```
pub fn field_path_to_expr(field_path: &str) -> Result<Expr> {
    // Parse the field path to handle nested fields and backtick-escaped names
    let parts = lance_core::datatypes::parse_field_path(field_path)?;

    if parts.is_empty() {
        return Err(Error::invalid_input(format!(
            "Invalid empty field path: {}",
            field_path
        )));
    }

    // Build the column expression, handling nested fields.
    let mut expr = Expr::Column(datafusion::common::Column::new_unqualified(
        parts[0].clone(),
    ));
    for part in &parts[1..] {
        expr = expr.field_newstyle(part);
    }

    Ok(expr)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    use arrow_schema::{Field, Schema as ArrowSchema};
    use datafusion::common::Column;
    use datafusion_functions::core::expr_ext::FieldAccessor;

    #[test]
    fn test_field_path_to_expr_preserves_case_sensitive_root_column() {
        let expr = field_path_to_expr("VECTOR").unwrap();

        assert_eq!(expr, Expr::Column(Column::new_unqualified("VECTOR")));
    }

    #[test]
    fn test_field_path_to_expr_preserves_case_sensitive_escaped_nested_path() {
        let expr = field_path_to_expr("Parent.`Child.With.Dot`").unwrap();

        assert_eq!(
            expr,
            Expr::Column(Column::new_unqualified("Parent")).field_newstyle("Child.With.Dot")
        );
    }

    #[test]
    fn test_resolve_large_utf8() {
        let arrow_schema = ArrowSchema::new(vec![Field::new("a", DataType::LargeUtf8, false)]);
        let expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column("a".to_string().into())),
            op: Operator::Eq,
            right: Box::new(Expr::Literal(
                ScalarValue::Utf8(Some("a".to_string())),
                None,
            )),
        });

        let resolved = resolve_expr(&expr, &Schema::try_from(&arrow_schema).unwrap()).unwrap();
        match resolved {
            Expr::BinaryExpr(be) => {
                assert_eq!(
                    be.right.as_ref(),
                    &Expr::Literal(ScalarValue::LargeUtf8(Some("a".to_string())), None)
                )
            }
            _ => unreachable!("Expected BinaryExpr"),
        };
    }

    #[test]
    fn test_resolve_binary_expr_on_right() {
        let arrow_schema = ArrowSchema::new(vec![Field::new("a", DataType::Float64, false)]);
        let expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column("a".to_string().into())),
            op: Operator::Eq,
            right: Box::new(Expr::BinaryExpr(BinaryExpr {
                left: Box::new(Expr::Literal(ScalarValue::Int64(Some(2)), None)),
                op: Operator::Minus,
                right: Box::new(Expr::Literal(ScalarValue::Int64(Some(-1)), None)),
            })),
        });
        let resolved = resolve_expr(&expr, &Schema::try_from(&arrow_schema).unwrap()).unwrap();

        match resolved {
            Expr::BinaryExpr(be) => match be.right.as_ref() {
                Expr::BinaryExpr(r_be) => {
                    assert_eq!(
                        r_be.left.as_ref(),
                        &Expr::Literal(ScalarValue::Float64(Some(2.0)), None)
                    );
                    assert_eq!(
                        r_be.right.as_ref(),
                        &Expr::Literal(ScalarValue::Float64(Some(-1.0)), None)
                    );
                }
                _ => panic!("Expected BinaryExpr"),
            },
            _ => panic!("Expected BinaryExpr"),
        }
    }

    #[test]
    fn test_resolve_in_expr() {
        // Type coercion should apply for `A IN (0)` or `A NOT IN (0)`
        let arrow_schema = ArrowSchema::new(vec![Field::new("a", DataType::Float32, false)]);
        let expr = Expr::in_list(
            Expr::Column("a".to_string().into()),
            vec![Expr::Literal(ScalarValue::Float64(Some(0.0)), None)],
            false,
        );
        let resolved = resolve_expr(&expr, &Schema::try_from(&arrow_schema).unwrap()).unwrap();
        let expected = Expr::in_list(
            Expr::Column("a".to_string().into()),
            vec![Expr::Literal(ScalarValue::Float32(Some(0.0)), None)],
            false,
        );
        assert_eq!(resolved, expected);

        let expr = Expr::in_list(
            Expr::Column("a".to_string().into()),
            vec![Expr::Literal(ScalarValue::Float64(Some(0.0)), None)],
            true,
        );
        let resolved = resolve_expr(&expr, &Schema::try_from(&arrow_schema).unwrap()).unwrap();
        let expected = Expr::in_list(
            Expr::Column("a".to_string().into()),
            vec![Expr::Literal(ScalarValue::Float32(Some(0.0)), None)],
            true,
        );
        assert_eq!(resolved, expected);
    }

    #[test]
    fn test_resolve_column_type() {
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("int", DataType::Int32, true),
            Field::new(
                "st",
                DataType::Struct(
                    vec![
                        Field::new("str", DataType::Utf8, true),
                        Field::new(
                            "st",
                            DataType::Struct(
                                vec![Field::new("float", DataType::Float64, true)].into(),
                            ),
                            true,
                        ),
                    ]
                    .into(),
                ),
                true,
            ),
        ]));
        let schema = Schema::try_from(schema.as_ref()).unwrap();

        assert_eq!(
            resolve_column_type(&col("int"), &schema),
            Some(DataType::Int32)
        );
        assert_eq!(
            resolve_column_type(&col("st").field("str"), &schema),
            Some(DataType::Utf8)
        );
        assert_eq!(
            resolve_column_type(&col("st").field("st").field("float"), &schema),
            Some(DataType::Float64)
        );

        assert_eq!(resolve_column_type(&col("x"), &schema), None);
        assert_eq!(resolve_column_type(&col("str"), &schema), None);
        assert_eq!(resolve_column_type(&col("float"), &schema), None);
        assert_eq!(
            resolve_column_type(&col("st").field("str").eq(lit("x")), &schema),
            None
        );
    }

    #[test]
    fn test_resolve_utf8view_literal_against_utf8_column() {
        // Simulates DataFusion 43+ producing a Utf8View literal (e.g. from md5())
        // being compared against a Utf8 column stored in Lance.
        let arrow_schema = ArrowSchema::new(vec![Field::new("hash", DataType::Utf8, false)]);
        let schema = Schema::try_from(&arrow_schema).unwrap();

        let expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column("hash".to_string().into())),
            op: Operator::Eq,
            right: Box::new(Expr::Literal(
                ScalarValue::Utf8View(Some("abc".to_string())),
                None,
            )),
        });

        let resolved = resolve_expr(&expr, &schema).unwrap();
        match resolved {
            Expr::BinaryExpr(be) => {
                assert_eq!(
                    be.right.as_ref(),
                    &Expr::Literal(ScalarValue::Utf8(Some("abc".to_string())), None)
                )
            }
            _ => unreachable!("Expected BinaryExpr"),
        }
    }

    #[test]
    fn test_resolve_typed_null_against_dictionary_column() {
        // A dictionary-encoded string column, e.g. a categorical field.
        let dict_ty = DataType::Dictionary(Box::new(DataType::Int16), Box::new(DataType::Utf8));
        let arrow_schema = ArrowSchema::new(vec![Field::new("etld", dict_ty, true)]);
        let schema = Schema::try_from(&arrow_schema).unwrap();

        // A typed null must be wrapped in the dictionary type, not left as a bare
        // `Utf8(None)` literal sitting next to a `Dictionary(...)` column.
        let expected_null = Expr::Literal(
            ScalarValue::Dictionary(Box::new(DataType::Int16), Box::new(ScalarValue::Utf8(None))),
            None,
        );

        // `etld = <typed null>` built directly via the API, as opposed to coming
        // through SQL parsing.
        let expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column("etld".to_string().into())),
            op: Operator::Eq,
            right: Box::new(Expr::Literal(ScalarValue::Utf8(None), None)),
        });
        match resolve_expr(&expr, &schema).unwrap() {
            Expr::BinaryExpr(be) => assert_eq!(be.right.as_ref(), &expected_null),
            other => unreachable!("Expected BinaryExpr, got {other:?}"),
        }

        // `etld IN ('a', <typed null>)` — a typed value mixed with a typed null,
        // both already typed as Utf8. Every list element is wrapped in the
        // dictionary type.
        let expr = Expr::in_list(
            Expr::Column("etld".to_string().into()),
            vec![
                Expr::Literal(ScalarValue::Utf8(Some("a".to_string())), None),
                Expr::Literal(ScalarValue::Utf8(None), None),
            ],
            false,
        );
        let expected = Expr::in_list(
            Expr::Column("etld".to_string().into()),
            vec![
                Expr::Literal(
                    ScalarValue::Dictionary(
                        Box::new(DataType::Int16),
                        Box::new(ScalarValue::Utf8(Some("a".to_string()))),
                    ),
                    None,
                ),
                expected_null,
            ],
            false,
        );
        assert_eq!(resolve_expr(&expr, &schema).unwrap(), expected);
    }
}
