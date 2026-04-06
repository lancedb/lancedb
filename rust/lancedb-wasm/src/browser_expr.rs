// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::sync::Arc;

use arrow_array::{
    Array, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array, LargeStringArray,
    RecordBatch, StringArray, UInt64Array, new_null_array,
};
use arrow_schema::{DataType, Schema as ArrowSchema};
use sqlparser::ast::{
    BinaryOperator, Expr, Ident, UnaryOperator, Value as SqlValue, ValueWithSpan,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::local_error::{Error, Result};

pub(crate) const RESULT_DISTANCE_COLUMN: &str = "_distance";
pub(crate) const RESULT_SCORE_COLUMN: &str = "_score";
pub(crate) const RESULT_RELEVANCE_COLUMN: &str = "_relevance_score";

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct RowScores {
    pub distance: Option<f32>,
    pub text_score: Option<f32>,
    pub relevance_score: Option<f32>,
}

#[derive(Debug, Clone)]
pub(crate) struct RowContext<'a> {
    batch: &'a RecordBatch,
    row_index: usize,
    scores: RowScores,
}

impl<'a> RowContext<'a> {
    pub(crate) fn new(batch: &'a RecordBatch, row_index: usize, scores: RowScores) -> Self {
        Self {
            batch,
            row_index,
            scores,
        }
    }

    pub(crate) fn value_for(&self, name: &str) -> Result<ScalarValue> {
        match name {
            RESULT_DISTANCE_COLUMN => Ok(self
                .scores
                .distance
                .map(ScalarValue::Float32)
                .unwrap_or(ScalarValue::Null)),
            RESULT_SCORE_COLUMN => Ok(self
                .scores
                .text_score
                .map(ScalarValue::Float32)
                .unwrap_or(ScalarValue::Null)),
            RESULT_RELEVANCE_COLUMN => Ok(self
                .scores
                .relevance_score
                .map(ScalarValue::Float32)
                .unwrap_or(ScalarValue::Null)),
            _ => extract_batch_value(self.batch, self.row_index, name),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum ScalarValue {
    Null,
    Boolean(bool),
    Int32(i32),
    Int64(i64),
    UInt64(u64),
    Float32(f32),
    Float64(f64),
    Utf8(String),
}

impl ScalarValue {
    pub(crate) fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    fn as_bool(&self) -> Result<bool> {
        match self {
            Self::Boolean(value) => Ok(*value),
            Self::Null => Ok(false),
            _ => Err(Error::InvalidInput {
                message: format!(
                    "expected a boolean expression but found {:?}",
                    self.data_type()
                ),
            }),
        }
    }

    fn data_type(&self) -> DataType {
        match self {
            Self::Null => DataType::Null,
            Self::Boolean(_) => DataType::Boolean,
            Self::Int32(_) => DataType::Int32,
            Self::Int64(_) => DataType::Int64,
            Self::UInt64(_) => DataType::UInt64,
            Self::Float32(_) => DataType::Float32,
            Self::Float64(_) => DataType::Float64,
            Self::Utf8(_) => DataType::Utf8,
        }
    }

    fn cast_to(&self, data_type: &DataType) -> Result<Self> {
        if self.is_null() {
            return Ok(Self::Null);
        }

        match data_type {
            DataType::Null => Ok(Self::Null),
            DataType::Boolean => Ok(Self::Boolean(self.as_bool()?)),
            DataType::Int32 => Ok(Self::Int32(numeric_as_i64(self)? as i32)),
            DataType::Int64 => Ok(Self::Int64(numeric_as_i64(self)?)),
            DataType::UInt64 => Ok(Self::UInt64(numeric_as_u64(self)?)),
            DataType::Float32 => Ok(Self::Float32(numeric_as_f64(self)? as f32)),
            DataType::Float64 => Ok(Self::Float64(numeric_as_f64(self)?)),
            DataType::Utf8 | DataType::LargeUtf8 => Ok(Self::Utf8(stringify_scalar(self))),
            _ => Err(Error::NotSupported {
                message: format!(
                    "browser expression evaluation does not support casts to {data_type:?}"
                ),
            }),
        }
    }
}

pub(crate) fn parse_expression(source: &str) -> Result<Expr> {
    let dialect = GenericDialect {};
    Parser::new(&dialect)
        .try_with_sql(source)
        .and_then(|mut parser| parser.parse_expr())
        .map_err(|error| Error::InvalidInput {
            message: format!("invalid browser expression '{source}': {error}"),
        })
}

pub(crate) fn collect_referenced_columns(expr: &Expr, columns: &mut BTreeSet<String>) {
    match expr {
        Expr::Identifier(ident) => {
            columns.insert(normalize_ident(ident));
        }
        Expr::CompoundIdentifier(idents) => {
            if let Some(ident) = idents.last() {
                columns.insert(normalize_ident(ident));
            }
        }
        Expr::Nested(expr)
        | Expr::UnaryOp { expr, .. }
        | Expr::IsNull(expr)
        | Expr::IsNotNull(expr)
        | Expr::Cast { expr, .. } => collect_referenced_columns(expr, columns),
        Expr::BinaryOp { left, right, .. } => {
            collect_referenced_columns(left, columns);
            collect_referenced_columns(right, columns);
        }
        Expr::Like { expr, pattern, .. } | Expr::ILike { expr, pattern, .. } => {
            collect_referenced_columns(expr, columns);
            collect_referenced_columns(pattern, columns);
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            collect_referenced_columns(expr, columns);
            collect_referenced_columns(low, columns);
            collect_referenced_columns(high, columns);
        }
        Expr::InList { expr, list, .. } => {
            collect_referenced_columns(expr, columns);
            for item in list {
                collect_referenced_columns(item, columns);
            }
        }
        Expr::TypedString(value) => {
            let inner = Expr::Value(value.value.clone());
            collect_referenced_columns(&inner, columns);
        }
        Expr::Value(_) => {}
        _ => {}
    }
}

pub(crate) fn infer_expression_type(
    expr: &Expr,
    schema: &ArrowSchema,
    available_scores: &BTreeSet<String>,
) -> Result<DataType> {
    match expr {
        Expr::Identifier(ident) => lookup_type(schema, &normalize_ident(ident), available_scores),
        Expr::CompoundIdentifier(idents) => lookup_type(
            schema,
            &idents
                .last()
                .map(normalize_ident)
                .unwrap_or_else(|| "".to_string()),
            available_scores,
        ),
        Expr::Nested(expr) => infer_expression_type(expr, schema, available_scores),
        Expr::Value(value) => infer_literal_type(&value.value),
        Expr::TypedString(value) => infer_sql_cast_type(&value.data_type),
        Expr::UnaryOp { op, expr } => match op {
            UnaryOperator::Plus | UnaryOperator::Minus => {
                infer_expression_type(expr, schema, available_scores)
            }
            UnaryOperator::Not | UnaryOperator::BangNot => Ok(DataType::Boolean),
            _ => Err(Error::NotSupported {
                message: format!("browser expressions do not support unary operator {op}"),
            }),
        },
        Expr::BinaryOp { left, op, right } => {
            infer_binary_type(left, op, right, schema, available_scores)
        }
        Expr::IsNull(_)
        | Expr::IsNotNull(_)
        | Expr::Like { .. }
        | Expr::ILike { .. }
        | Expr::Between { .. }
        | Expr::InList { .. } => Ok(DataType::Boolean),
        Expr::Cast { data_type, .. } => infer_sql_cast_type(data_type),
        _ => Err(Error::NotSupported {
            message: format!("browser expressions do not support {expr:?}"),
        }),
    }
}

pub(crate) fn evaluate_filter(expr: &Expr, row: &RowContext<'_>) -> Result<bool> {
    evaluate_expression(expr, row, None)?.as_bool()
}

pub(crate) fn evaluate_expression(
    expr: &Expr,
    row: &RowContext<'_>,
    expected_type: Option<&DataType>,
) -> Result<ScalarValue> {
    match expr {
        Expr::Identifier(ident) => {
            let value = row.value_for(&normalize_ident(ident))?;
            if let Some(expected_type) = expected_type {
                value.cast_to(expected_type)
            } else {
                Ok(value)
            }
        }
        Expr::CompoundIdentifier(idents) => {
            let name = idents
                .last()
                .map(normalize_ident)
                .ok_or_else(|| Error::InvalidInput {
                    message: "compound identifier was empty".to_string(),
                })?;
            let value = row.value_for(&name)?;
            if let Some(expected_type) = expected_type {
                value.cast_to(expected_type)
            } else {
                Ok(value)
            }
        }
        Expr::Nested(expr) => evaluate_expression(expr, row, expected_type),
        Expr::Value(value) => literal_to_scalar(&value.value, expected_type),
        Expr::TypedString(value) => {
            let scalar = literal_to_scalar(
                &value.value.value,
                Some(&infer_sql_cast_type(&value.data_type)?),
            )?;
            if let Some(expected_type) = expected_type {
                scalar.cast_to(expected_type)
            } else {
                Ok(scalar)
            }
        }
        Expr::UnaryOp { op, expr } => {
            let value = evaluate_expression(expr, row, expected_type)?;
            match op {
                UnaryOperator::Plus => Ok(value),
                UnaryOperator::Minus => negate_scalar(value, expected_type),
                UnaryOperator::Not | UnaryOperator::BangNot => {
                    Ok(ScalarValue::Boolean(!value.as_bool()?))
                }
                _ => Err(Error::NotSupported {
                    message: format!("browser expressions do not support unary operator {op}"),
                }),
            }
        }
        Expr::BinaryOp { left, op, right } => match op {
            BinaryOperator::And => Ok(ScalarValue::Boolean(
                evaluate_filter(left, row)? && evaluate_filter(right, row)?,
            )),
            BinaryOperator::Or => Ok(ScalarValue::Boolean(
                evaluate_filter(left, row)? || evaluate_filter(right, row)?,
            )),
            BinaryOperator::Eq
            | BinaryOperator::NotEq
            | BinaryOperator::Gt
            | BinaryOperator::GtEq
            | BinaryOperator::Lt
            | BinaryOperator::LtEq => compare_scalars(
                evaluate_expression(left, row, None)?,
                evaluate_expression(right, row, None)?,
                op,
            ),
            BinaryOperator::Plus
            | BinaryOperator::Minus
            | BinaryOperator::Multiply
            | BinaryOperator::Divide
            | BinaryOperator::Modulo
            | BinaryOperator::DuckIntegerDivide
            | BinaryOperator::MyIntegerDivide
            | BinaryOperator::StringConcat => arithmetic_scalars(
                evaluate_expression(left, row, None)?,
                evaluate_expression(right, row, None)?,
                op,
                expected_type,
            ),
            _ => Err(Error::NotSupported {
                message: format!("browser expressions do not support binary operator {op}"),
            }),
        },
        Expr::IsNull(expr) => Ok(ScalarValue::Boolean(
            evaluate_expression(expr, row, None)?.is_null(),
        )),
        Expr::IsNotNull(expr) => Ok(ScalarValue::Boolean(
            !evaluate_expression(expr, row, None)?.is_null(),
        )),
        Expr::Like {
            negated,
            expr,
            pattern,
            escape_char,
            any,
        } => {
            if *any {
                return Err(Error::NotSupported {
                    message: "browser expressions do not support LIKE ANY".to_string(),
                });
            }
            let value = stringify_scalar(&evaluate_expression(expr, row, None)?);
            let pattern = stringify_scalar(&evaluate_expression(pattern, row, None)?);
            let escape = escape_char.as_ref().and_then(sql_escape_char);
            let matched = like_matches(&value, &pattern, escape);
            Ok(ScalarValue::Boolean(if *negated {
                !matched
            } else {
                matched
            }))
        }
        Expr::ILike {
            negated,
            expr,
            pattern,
            escape_char,
            any,
        } => {
            if *any {
                return Err(Error::NotSupported {
                    message: "browser expressions do not support ILIKE ANY".to_string(),
                });
            }
            let value = stringify_scalar(&evaluate_expression(expr, row, None)?);
            let pattern = stringify_scalar(&evaluate_expression(pattern, row, None)?);
            let escape = escape_char.as_ref().and_then(sql_escape_char);
            let matched = like_matches(
                &value.to_ascii_lowercase(),
                &pattern.to_ascii_lowercase(),
                escape,
            );
            Ok(ScalarValue::Boolean(if *negated {
                !matched
            } else {
                matched
            }))
        }
        Expr::Between {
            expr,
            negated,
            low,
            high,
        } => {
            let value = evaluate_expression(expr, row, None)?;
            let low = evaluate_expression(low, row, None)?;
            let high = evaluate_expression(high, row, None)?;
            let lower = compare_scalars(value.clone(), low, &BinaryOperator::GtEq)?.as_bool()?;
            let upper = compare_scalars(value, high, &BinaryOperator::LtEq)?.as_bool()?;
            let matched = lower && upper;
            Ok(ScalarValue::Boolean(if *negated {
                !matched
            } else {
                matched
            }))
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let value = evaluate_expression(expr, row, None)?;
            let matched = list.iter().try_fold(false, |matched, item| {
                if matched {
                    Ok(true)
                } else {
                    compare_scalars(
                        value.clone(),
                        evaluate_expression(item, row, None)?,
                        &BinaryOperator::Eq,
                    )?
                    .as_bool()
                }
            })?;
            Ok(ScalarValue::Boolean(if *negated {
                !matched
            } else {
                matched
            }))
        }
        Expr::Cast {
            expr, data_type, ..
        } => evaluate_expression(expr, row, Some(&infer_sql_cast_type(data_type)?)),
        _ => Err(Error::NotSupported {
            message: format!("browser expressions do not support {expr:?}"),
        }),
    }
}

pub(crate) fn scalar_to_array(value: ScalarValue, data_type: &DataType) -> Result<Arc<dyn Array>> {
    if value.is_null() {
        return Ok(new_null_array(data_type, 1));
    }

    match data_type {
        DataType::Boolean => Ok(Arc::new(BooleanArray::from(vec![Some(value.as_bool()?)]))),
        DataType::Int32 => Ok(Arc::new(Int32Array::from(vec![Some(
            numeric_as_i64(&value)? as i32,
        )]))),
        DataType::Int64 => Ok(Arc::new(Int64Array::from(vec![Some(numeric_as_i64(
            &value,
        )?)]))),
        DataType::UInt64 => Ok(Arc::new(UInt64Array::from(vec![Some(numeric_as_u64(
            &value,
        )?)]))),
        DataType::Float32 => Ok(Arc::new(Float32Array::from(vec![Some(
            numeric_as_f64(&value)? as f32,
        )]))),
        DataType::Float64 => Ok(Arc::new(Float64Array::from(vec![Some(numeric_as_f64(
            &value,
        )?)]))),
        DataType::Utf8 => Ok(Arc::new(StringArray::from(vec![Some(stringify_scalar(
            &value,
        ))]))),
        DataType::LargeUtf8 => Ok(Arc::new(LargeStringArray::from(vec![Some(
            stringify_scalar(&value),
        )]))),
        _ => Err(Error::NotSupported {
            message: format!("browser expressions cannot materialize {data_type:?}"),
        }),
    }
}

fn infer_binary_type(
    left: &Expr,
    op: &BinaryOperator,
    right: &Expr,
    schema: &ArrowSchema,
    available_scores: &BTreeSet<String>,
) -> Result<DataType> {
    let left_type = infer_expression_type(left, schema, available_scores)?;
    let right_type = infer_expression_type(right, schema, available_scores)?;

    match op {
        BinaryOperator::And
        | BinaryOperator::Or
        | BinaryOperator::Eq
        | BinaryOperator::NotEq
        | BinaryOperator::Gt
        | BinaryOperator::GtEq
        | BinaryOperator::Lt
        | BinaryOperator::LtEq => Ok(DataType::Boolean),
        BinaryOperator::StringConcat => Ok(DataType::Utf8),
        BinaryOperator::Divide => Ok(DataType::Float64),
        BinaryOperator::Plus
        | BinaryOperator::Minus
        | BinaryOperator::Multiply
        | BinaryOperator::Modulo
        | BinaryOperator::DuckIntegerDivide
        | BinaryOperator::MyIntegerDivide => {
            if is_string_type(&left_type) || is_string_type(&right_type) {
                if matches!(op, BinaryOperator::Plus) {
                    Ok(DataType::Utf8)
                } else {
                    Err(Error::NotSupported {
                        message: format!(
                            "browser expressions do not support {op} on string values"
                        ),
                    })
                }
            } else {
                Ok(preferred_numeric_type(
                    left,
                    &left_type,
                    right,
                    &right_type,
                    op,
                ))
            }
        }
        _ => Err(Error::NotSupported {
            message: format!("browser expressions do not support binary operator {op}"),
        }),
    }
}

fn preferred_numeric_type(
    left: &Expr,
    left_type: &DataType,
    right: &Expr,
    right_type: &DataType,
    op: &BinaryOperator,
) -> DataType {
    if matches!(
        op,
        BinaryOperator::DuckIntegerDivide | BinaryOperator::MyIntegerDivide
    ) {
        return DataType::Int64;
    }
    if matches!(left_type, DataType::Float64) || matches!(right_type, DataType::Float64) {
        return DataType::Float64;
    }
    if matches!(left_type, DataType::Float32) || matches!(right_type, DataType::Float32) {
        return DataType::Float32;
    }

    if is_numeric_literal_expr(left) && is_integer_type(right_type) {
        return right_type.clone();
    }
    if is_numeric_literal_expr(right) && is_integer_type(left_type) {
        return left_type.clone();
    }

    match (left_type, right_type) {
        (DataType::UInt64, DataType::UInt64) => DataType::UInt64,
        (DataType::Int64, _) | (_, DataType::Int64) => DataType::Int64,
        (DataType::UInt64, _) | (_, DataType::UInt64) => DataType::UInt64,
        _ => DataType::Int32,
    }
}

fn infer_literal_type(value: &SqlValue) -> Result<DataType> {
    match value {
        SqlValue::Number(raw, _) => {
            if raw.contains('.') || raw.contains('e') || raw.contains('E') {
                Ok(DataType::Float64)
            } else {
                Ok(DataType::Int64)
            }
        }
        SqlValue::SingleQuotedString(_)
        | SqlValue::TripleSingleQuotedString(_)
        | SqlValue::EscapedStringLiteral(_)
        | SqlValue::UnicodeStringLiteral(_)
        | SqlValue::DoubleQuotedString(_)
        | SqlValue::TripleDoubleQuotedString(_) => Ok(DataType::Utf8),
        SqlValue::Boolean(_) => Ok(DataType::Boolean),
        SqlValue::Null => Ok(DataType::Null),
        _ => Err(Error::NotSupported {
            message: format!("browser expressions do not support literal {value:?}"),
        }),
    }
}

fn infer_sql_cast_type(data_type: &sqlparser::ast::DataType) -> Result<DataType> {
    let rendered = data_type.to_string().to_ascii_uppercase();
    if rendered.starts_with("INT") || rendered.starts_with("INTEGER") {
        Ok(DataType::Int32)
    } else if rendered.starts_with("BIGINT") {
        Ok(DataType::Int64)
    } else if rendered.starts_with("FLOAT") || rendered.starts_with("REAL") {
        Ok(DataType::Float32)
    } else if rendered.starts_with("DOUBLE") || rendered.starts_with("NUMERIC") {
        Ok(DataType::Float64)
    } else if rendered.starts_with("BOOL") {
        Ok(DataType::Boolean)
    } else if rendered.starts_with("TEXT")
        || rendered.starts_with("VARCHAR")
        || rendered.starts_with("CHAR")
        || rendered.starts_with("STRING")
    {
        Ok(DataType::Utf8)
    } else {
        Err(Error::NotSupported {
            message: format!("browser expressions do not support CAST AS {rendered}"),
        })
    }
}

fn lookup_type(
    schema: &ArrowSchema,
    name: &str,
    available_scores: &BTreeSet<String>,
) -> Result<DataType> {
    match name {
        RESULT_DISTANCE_COLUMN | RESULT_SCORE_COLUMN | RESULT_RELEVANCE_COLUMN
            if available_scores.contains(name) =>
        {
            Ok(DataType::Float32)
        }
        _ => schema
            .field_with_name(name)
            .map(|field| field.data_type().clone())
            .map_err(|_| Error::InvalidInput {
                message: format!("unknown browser expression column '{name}'"),
            }),
    }
}

fn normalize_ident(ident: &Ident) -> String {
    ident.value.clone()
}

fn literal_to_scalar(value: &SqlValue, expected_type: Option<&DataType>) -> Result<ScalarValue> {
    let scalar = match value {
        SqlValue::Number(raw, _) => {
            if raw.contains('.') || raw.contains('e') || raw.contains('E') {
                ScalarValue::Float64(raw.parse::<f64>().map_err(|source| Error::InvalidInput {
                    message: format!("invalid numeric literal '{raw}': {source}"),
                })?)
            } else {
                ScalarValue::Int64(raw.parse::<i64>().map_err(|source| Error::InvalidInput {
                    message: format!("invalid integer literal '{raw}': {source}"),
                })?)
            }
        }
        SqlValue::SingleQuotedString(value)
        | SqlValue::TripleSingleQuotedString(value)
        | SqlValue::EscapedStringLiteral(value)
        | SqlValue::UnicodeStringLiteral(value)
        | SqlValue::DoubleQuotedString(value)
        | SqlValue::TripleDoubleQuotedString(value) => ScalarValue::Utf8(value.clone()),
        SqlValue::Boolean(value) => ScalarValue::Boolean(*value),
        SqlValue::Null => ScalarValue::Null,
        _ => {
            return Err(Error::NotSupported {
                message: format!("browser expressions do not support literal {value:?}"),
            });
        }
    };

    if let Some(expected_type) = expected_type {
        scalar.cast_to(expected_type)
    } else {
        Ok(scalar)
    }
}

fn negate_scalar(value: ScalarValue, expected_type: Option<&DataType>) -> Result<ScalarValue> {
    let inferred_type = value.data_type();
    let target = expected_type.unwrap_or(&inferred_type);
    if value.is_null() {
        return Ok(ScalarValue::Null);
    }
    match target {
        DataType::Float32 => Ok(ScalarValue::Float32(-(numeric_as_f64(&value)? as f32))),
        DataType::Float64 => Ok(ScalarValue::Float64(-numeric_as_f64(&value)?)),
        DataType::UInt64 => Err(Error::InvalidInput {
            message: "cannot negate an unsigned integer in a browser expression".to_string(),
        }),
        DataType::Int64 => Ok(ScalarValue::Int64(-numeric_as_i64(&value)?)),
        _ => Ok(ScalarValue::Int32(-(numeric_as_i64(&value)? as i32))),
    }
}

fn compare_scalars(
    left: ScalarValue,
    right: ScalarValue,
    op: &BinaryOperator,
) -> Result<ScalarValue> {
    if left.is_null() || right.is_null() {
        return Ok(ScalarValue::Boolean(false));
    }

    let ordering = if is_string_value(&left) || is_string_value(&right) {
        stringify_scalar(&left).cmp(&stringify_scalar(&right))
    } else if is_boolean_value(&left) || is_boolean_value(&right) {
        left.as_bool()?.cmp(&right.as_bool()?)
    } else if is_float_value(&left) || is_float_value(&right) {
        numeric_as_f64(&left)?
            .partial_cmp(&numeric_as_f64(&right)?)
            .unwrap_or(Ordering::Equal)
    } else {
        numeric_as_i64(&left)?.cmp(&numeric_as_i64(&right)?)
    };

    let value = match op {
        BinaryOperator::Eq => ordering == Ordering::Equal,
        BinaryOperator::NotEq => ordering != Ordering::Equal,
        BinaryOperator::Gt => ordering == Ordering::Greater,
        BinaryOperator::GtEq => ordering != Ordering::Less,
        BinaryOperator::Lt => ordering == Ordering::Less,
        BinaryOperator::LtEq => ordering != Ordering::Greater,
        _ => {
            return Err(Error::InvalidInput {
                message: format!("invalid comparison operator {op}"),
            });
        }
    };
    Ok(ScalarValue::Boolean(value))
}

fn arithmetic_scalars(
    left: ScalarValue,
    right: ScalarValue,
    op: &BinaryOperator,
    expected_type: Option<&DataType>,
) -> Result<ScalarValue> {
    if left.is_null() || right.is_null() {
        return Ok(ScalarValue::Null);
    }

    if matches!(op, BinaryOperator::StringConcat)
        || (matches!(op, BinaryOperator::Plus)
            && (is_string_value(&left) || is_string_value(&right)))
    {
        return Ok(ScalarValue::Utf8(format!(
            "{}{}",
            stringify_scalar(&left),
            stringify_scalar(&right)
        )));
    }

    let inferred_type = preferred_runtime_type(&left, &right, op);
    let target = expected_type.unwrap_or(&inferred_type);
    match target {
        DataType::Float32 => {
            let result = apply_float_op(
                numeric_as_f64(&left)? as f32,
                numeric_as_f64(&right)? as f32,
                op,
            )?;
            Ok(ScalarValue::Float32(result))
        }
        DataType::Float64 => {
            let result = apply_float_op(numeric_as_f64(&left)?, numeric_as_f64(&right)?, op)?;
            Ok(ScalarValue::Float64(result))
        }
        DataType::UInt64 => {
            let result = apply_uint_op(numeric_as_u64(&left)?, numeric_as_u64(&right)?, op)?;
            Ok(ScalarValue::UInt64(result))
        }
        DataType::Int64 => {
            let result = apply_int_op(numeric_as_i64(&left)?, numeric_as_i64(&right)?, op)?;
            Ok(ScalarValue::Int64(result))
        }
        _ => {
            let result = apply_int_op(numeric_as_i64(&left)?, numeric_as_i64(&right)?, op)?;
            Ok(ScalarValue::Int32(result as i32))
        }
    }
}

fn preferred_runtime_type(
    left: &ScalarValue,
    right: &ScalarValue,
    op: &BinaryOperator,
) -> DataType {
    if matches!(op, BinaryOperator::Divide) || is_float_value(left) || is_float_value(right) {
        return DataType::Float64;
    }
    if matches!(left, ScalarValue::UInt64(_)) && matches!(right, ScalarValue::UInt64(_)) {
        return DataType::UInt64;
    }
    if matches!(left, ScalarValue::Int64(_) | ScalarValue::UInt64(_))
        || matches!(right, ScalarValue::Int64(_) | ScalarValue::UInt64(_))
    {
        return DataType::Int64;
    }
    DataType::Int32
}

fn apply_float_op<T>(left: T, right: T, op: &BinaryOperator) -> Result<T>
where
    T: Copy
        + std::ops::Add<Output = T>
        + std::ops::Sub<Output = T>
        + std::ops::Mul<Output = T>
        + std::ops::Div<Output = T>
        + std::ops::Rem<Output = T>,
{
    Ok(match op {
        BinaryOperator::Plus => left + right,
        BinaryOperator::Minus => left - right,
        BinaryOperator::Multiply => left * right,
        BinaryOperator::Divide => left / right,
        BinaryOperator::Modulo => left % right,
        _ => {
            return Err(Error::NotSupported {
                message: format!("browser expressions do not support numeric operator {op}"),
            });
        }
    })
}

fn apply_int_op(left: i64, right: i64, op: &BinaryOperator) -> Result<i64> {
    match op {
        BinaryOperator::Plus => Ok(left + right),
        BinaryOperator::Minus => Ok(left - right),
        BinaryOperator::Multiply => Ok(left * right),
        BinaryOperator::Divide => Ok(left / right),
        BinaryOperator::Modulo => Ok(left % right),
        BinaryOperator::DuckIntegerDivide | BinaryOperator::MyIntegerDivide => Ok(left / right),
        _ => Err(Error::NotSupported {
            message: format!("browser expressions do not support integer operator {op}"),
        }),
    }
}

fn apply_uint_op(left: u64, right: u64, op: &BinaryOperator) -> Result<u64> {
    match op {
        BinaryOperator::Plus => Ok(left + right),
        BinaryOperator::Multiply => Ok(left * right),
        BinaryOperator::Divide => Ok(left / right),
        BinaryOperator::Modulo => Ok(left % right),
        _ => Err(Error::NotSupported {
            message: format!("browser expressions do not support unsigned operator {op}"),
        }),
    }
}

fn extract_batch_value(batch: &RecordBatch, row_index: usize, name: &str) -> Result<ScalarValue> {
    let index = batch.schema().index_of(name).map_err(Error::from)?;
    let column = batch.column(index);
    if column.is_null(row_index) {
        return Ok(ScalarValue::Null);
    }

    match column.data_type() {
        DataType::Boolean => {
            let array = column
                .as_any()
                .downcast_ref::<BooleanArray>()
                .expect("boolean array downcast");
            Ok(ScalarValue::Boolean(array.value(row_index)))
        }
        DataType::Int32 => {
            let array = column
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("int32 array downcast");
            Ok(ScalarValue::Int32(array.value(row_index)))
        }
        DataType::Int64 => {
            let array = column
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("int64 array downcast");
            Ok(ScalarValue::Int64(array.value(row_index)))
        }
        DataType::UInt64 => {
            let array = column
                .as_any()
                .downcast_ref::<UInt64Array>()
                .expect("uint64 array downcast");
            Ok(ScalarValue::UInt64(array.value(row_index)))
        }
        DataType::Float32 => {
            let array = column
                .as_any()
                .downcast_ref::<Float32Array>()
                .expect("float32 array downcast");
            Ok(ScalarValue::Float32(array.value(row_index)))
        }
        DataType::Float64 => {
            let array = column
                .as_any()
                .downcast_ref::<Float64Array>()
                .expect("float64 array downcast");
            Ok(ScalarValue::Float64(array.value(row_index)))
        }
        DataType::Utf8 => {
            let array = column
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("utf8 array downcast");
            Ok(ScalarValue::Utf8(array.value(row_index).to_string()))
        }
        DataType::LargeUtf8 => {
            let array = column
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .expect("large utf8 array downcast");
            Ok(ScalarValue::Utf8(array.value(row_index).to_string()))
        }
        other => Err(Error::NotSupported {
            message: format!("browser expressions do not support values from {other:?}"),
        }),
    }
}

fn numeric_as_f64(value: &ScalarValue) -> Result<f64> {
    match value {
        ScalarValue::Int32(value) => Ok(*value as f64),
        ScalarValue::Int64(value) => Ok(*value as f64),
        ScalarValue::UInt64(value) => Ok(*value as f64),
        ScalarValue::Float32(value) => Ok((*value).into()),
        ScalarValue::Float64(value) => Ok(*value),
        _ => Err(Error::InvalidInput {
            message: format!("expected a numeric value but found {:?}", value.data_type()),
        }),
    }
}

fn numeric_as_i64(value: &ScalarValue) -> Result<i64> {
    match value {
        ScalarValue::Int32(value) => Ok((*value).into()),
        ScalarValue::Int64(value) => Ok(*value),
        ScalarValue::UInt64(value) => (*value).try_into().map_err(|_| Error::InvalidInput {
            message: format!("value {value} does not fit in i64"),
        }),
        ScalarValue::Float32(value) => Ok(*value as i64),
        ScalarValue::Float64(value) => Ok(*value as i64),
        _ => Err(Error::InvalidInput {
            message: format!("expected a numeric value but found {:?}", value.data_type()),
        }),
    }
}

fn numeric_as_u64(value: &ScalarValue) -> Result<u64> {
    match value {
        ScalarValue::Int32(value) if *value >= 0 => Ok((*value).try_into().unwrap()),
        ScalarValue::Int64(value) if *value >= 0 => Ok((*value).try_into().unwrap()),
        ScalarValue::UInt64(value) => Ok(*value),
        ScalarValue::Float32(value) if *value >= 0.0 => Ok(*value as u64),
        ScalarValue::Float64(value) if *value >= 0.0 => Ok(*value as u64),
        _ => Err(Error::InvalidInput {
            message: format!(
                "expected a non-negative numeric value but found {:?}",
                value.data_type()
            ),
        }),
    }
}

fn stringify_scalar(value: &ScalarValue) -> String {
    match value {
        ScalarValue::Null => String::new(),
        ScalarValue::Boolean(value) => value.to_string(),
        ScalarValue::Int32(value) => value.to_string(),
        ScalarValue::Int64(value) => value.to_string(),
        ScalarValue::UInt64(value) => value.to_string(),
        ScalarValue::Float32(value) => value.to_string(),
        ScalarValue::Float64(value) => value.to_string(),
        ScalarValue::Utf8(value) => value.clone(),
    }
}

fn like_matches(value: &str, pattern: &str, escape: Option<char>) -> bool {
    like_matches_inner(
        &value.chars().collect::<Vec<_>>(),
        &pattern.chars().collect::<Vec<_>>(),
        escape,
        0,
        0,
    )
}

fn like_matches_inner(
    value: &[char],
    pattern: &[char],
    escape: Option<char>,
    value_index: usize,
    pattern_index: usize,
) -> bool {
    if pattern_index == pattern.len() {
        return value_index == value.len();
    }

    let current = pattern[pattern_index];
    if Some(current) == escape && pattern_index + 1 < pattern.len() {
        return value
            .get(value_index)
            .is_some_and(|candidate| *candidate == pattern[pattern_index + 1])
            && like_matches_inner(value, pattern, escape, value_index + 1, pattern_index + 2);
    }

    match current {
        '%' => {
            for next_value_index in value_index..=value.len() {
                if like_matches_inner(value, pattern, escape, next_value_index, pattern_index + 1) {
                    return true;
                }
            }
            false
        }
        '_' => value.get(value_index).is_some_and(|_| {
            like_matches_inner(value, pattern, escape, value_index + 1, pattern_index + 1)
        }),
        literal => {
            value
                .get(value_index)
                .is_some_and(|candidate| *candidate == literal)
                && like_matches_inner(value, pattern, escape, value_index + 1, pattern_index + 1)
        }
    }
}

fn sql_escape_char(value: &SqlValue) -> Option<char> {
    match value {
        SqlValue::SingleQuotedString(value)
        | SqlValue::DoubleQuotedString(value)
        | SqlValue::TripleSingleQuotedString(value)
        | SqlValue::TripleDoubleQuotedString(value)
        | SqlValue::EscapedStringLiteral(value)
        | SqlValue::UnicodeStringLiteral(value) => value.chars().next(),
        _ => None,
    }
}

fn is_numeric_literal_expr(expr: &Expr) -> bool {
    matches!(
        expr,
        Expr::Value(ValueWithSpan {
            value: SqlValue::Number(_, _),
            ..
        })
    )
}

fn is_integer_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int32 | DataType::Int64 | DataType::UInt64
    )
}

fn is_string_type(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Utf8 | DataType::LargeUtf8)
}

fn is_string_value(value: &ScalarValue) -> bool {
    matches!(value, ScalarValue::Utf8(_))
}

fn is_boolean_value(value: &ScalarValue) -> bool {
    matches!(value, ScalarValue::Boolean(_))
}

fn is_float_value(value: &ScalarValue) -> bool {
    matches!(value, ScalarValue::Float32(_) | ScalarValue::Float64(_))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Int32Array;
    use arrow_schema::{Field, Schema};

    fn make_context() -> RowContext<'static> {
        let batch = Box::new(
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![
                    Field::new("id", DataType::Int32, false),
                    Field::new("doc", DataType::Utf8, false),
                ])),
                vec![
                    Arc::new(Int32Array::from(vec![2])) as Arc<dyn Array>,
                    Arc::new(StringArray::from(vec!["apple pie"])) as Arc<dyn Array>,
                ],
            )
            .unwrap(),
        );
        let batch = Box::leak(batch);
        RowContext::new(
            batch,
            0,
            RowScores {
                distance: Some(0.25),
                text_score: Some(2.0),
                relevance_score: Some(1.5),
            },
        )
    }

    #[test]
    fn evaluates_simple_filter_expressions() {
        let expr = parse_expression("id = 2 AND doc LIKE '%apple%'").unwrap();
        assert!(evaluate_filter(&expr, &make_context()).unwrap());
    }

    #[test]
    fn evaluates_dynamic_expressions() {
        let expr = parse_expression("id * 2").unwrap();
        let data_type = infer_expression_type(
            &expr,
            make_context().batch.schema().as_ref(),
            &BTreeSet::new(),
        )
        .unwrap();
        assert_eq!(data_type, DataType::Int32);
        let value = evaluate_expression(&expr, &make_context(), Some(&data_type)).unwrap();
        assert_eq!(value, ScalarValue::Int32(4));
    }
}
