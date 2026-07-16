// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Expression builder API for type-safe query construction
//!
//! This module provides a fluent API for building expressions that can be used
//! in filters and projections. It wraps DataFusion's expression system.
//!
//! # Examples
//!
//! ```rust
//! use std::ops::Mul;
//! use lancedb::expr::{col, lit};
//!
//! let expr = col("age").gt(lit(18));
//! let expr = col("age").gt(lit(18)).and(col("status").eq(lit("active")));
//! let expr = col("price") * lit(1.1);
//! ```

mod sql;

pub use sql::expr_to_sql_string;

use std::sync::Arc;

use arrow_schema::DataType;
use datafusion_expr::{Expr, ScalarUDF, expr_fn::cast};
use datafusion_functions::string::expr_fn as string_expr_fn;

pub use datafusion_expr::lit;

/// Create a column reference expression, preserving the name exactly as given.
///
/// Unlike DataFusion's built-in [`col`][datafusion_expr::col], this function
/// does **not** normalise the identifier to lower-case, so
/// `col("firstName")` correctly references a field named `firstName`.
pub fn col(name: impl Into<String>) -> DfExpr {
    use datafusion_common::Column;
    DfExpr::Column(Column::new_unqualified(name))
}

pub use datafusion_expr::Expr as DfExpr;

pub fn lower(expr: Expr) -> Expr {
    string_expr_fn::lower(expr)
}

pub fn upper(expr: Expr) -> Expr {
    string_expr_fn::upper(expr)
}

pub fn contains(expr: Expr, search: Expr) -> Expr {
    string_expr_fn::contains(expr, search)
}

pub fn expr_cast(expr: Expr, data_type: DataType) -> Expr {
    cast(expr, data_type)
}

pub fn is_in(expr: Expr, list: Vec<Expr>) -> Expr {
    expr.in_list(list, false)
}

static FUNC_REGISTRY: std::sync::LazyLock<std::collections::HashMap<String, Arc<ScalarUDF>>> =
    std::sync::LazyLock::new(|| {
        let mut m = std::collections::HashMap::new();
        m.insert("lower".to_string(), datafusion_functions::string::lower());
        m.insert("upper".to_string(), datafusion_functions::string::upper());
        m.insert(
            "contains".to_string(),
            datafusion_functions::string::contains(),
        );
        m.insert("btrim".to_string(), datafusion_functions::string::btrim());
        m.insert("ltrim".to_string(), datafusion_functions::string::ltrim());
        m.insert("rtrim".to_string(), datafusion_functions::string::rtrim());
        m.insert("concat".to_string(), datafusion_functions::string::concat());
        m.insert(
            "octet_length".to_string(),
            datafusion_functions::string::octet_length(),
        );
        m
    });

pub fn func(name: impl AsRef<str>, args: Vec<Expr>) -> crate::Result<Expr> {
    let name = name.as_ref();
    let udf = FUNC_REGISTRY
        .get(name)
        .ok_or_else(|| crate::Error::InvalidInput {
            message: format!("unknown function: {}", name),
        })?;
    Ok(Expr::ScalarFunction(
        datafusion_expr::expr::ScalarFunction::new_udf(udf.clone(), args),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_col_lit_comparisons() {
        let expr = col("age").gt(lit(18));
        let sql = expr_to_sql_string(&expr).unwrap();
        assert!(sql.contains("age") && sql.contains("18"));

        let expr = col("name").eq(lit("Alice"));
        let sql = expr_to_sql_string(&expr).unwrap();
        assert!(sql.contains("name") && sql.contains("Alice"));
    }

    #[test]
    fn test_compound_expression() {
        let expr = col("age").gt(lit(18)).and(col("status").eq(lit("active")));
        let sql = expr_to_sql_string(&expr).unwrap();
        assert!(sql.contains("age") && sql.contains("status"));
    }

    #[test]
    fn test_string_functions() {
        let expr = lower(col("name"));
        let sql = expr_to_sql_string(&expr).unwrap();
        assert!(sql.to_lowercase().contains("lower"));

        let expr = contains(col("text"), lit("search"));
        let sql = expr_to_sql_string(&expr).unwrap();
        assert!(sql.to_lowercase().contains("contains"));
    }

    #[test]
    fn test_func() {
        let expr = func("lower", vec![col("x")]).unwrap();
        let sql = expr_to_sql_string(&expr).unwrap();
        assert!(sql.to_lowercase().contains("lower"));

        let result = func("unknown_func", vec![col("x")]);
        assert!(result.is_err());
    }

    #[test]
    fn test_arithmetic() {
        let expr = col("price") * lit(1.1);
        let sql = expr_to_sql_string(&expr).unwrap();
        assert!(sql.contains("price"));
    }

    #[test]
    fn test_binary_literal() {
        use datafusion_common::ScalarValue;
        let expr = lit(ScalarValue::Binary(Some(vec![0xde, 0xad, 0xbe, 0xef])));
        let sql = expr_to_sql_string(&expr).unwrap();
        assert_eq!(sql, "X'DEADBEEF'");
    }

    #[test]
    fn test_binary_literal_in_filter() {
        use datafusion_common::ScalarValue;
        let expr = col("data").eq(lit(ScalarValue::Binary(Some(vec![0xca, 0xfe]))));
        let sql = expr_to_sql_string(&expr).unwrap();
        assert_eq!(sql, "(data = X'CAFE')");
    }

    #[test]
    fn test_binary_literal_compound() {
        use datafusion_common::ScalarValue;
        let bin_expr = col("data").eq(lit(ScalarValue::Binary(Some(vec![0x01]))));
        let int_expr = col("id").gt(lit(5i64));
        let combined = bin_expr.and(int_expr);
        let sql = expr_to_sql_string(&combined).unwrap();
        assert_eq!(sql, "((data = X'01') AND (id > 5))");
    }

    #[test]
    fn test_null_binary_literal() {
        use datafusion_common::ScalarValue;
        let expr = lit(ScalarValue::Binary(None));
        let sql = expr_to_sql_string(&expr).unwrap();
        assert_eq!(sql, "NULL");
    }

    #[test]
    fn test_binary_literal_in_function_call() {
        use datafusion_common::ScalarValue;
        // Binary literals inside scalar function arguments must also be
        // serialized correctly (regression test for placeholder rewrite path).
        let expr = contains(col("data"), lit(ScalarValue::Binary(Some(vec![0xff]))));
        let sql = expr_to_sql_string(&expr).unwrap();
        assert_eq!(sql, "contains(data, X'FF')");
    }

    #[test]
    fn test_binary_literal_in_negation() {
        use datafusion_common::ScalarValue;
        use std::ops::Not;
        let expr = col("data")
            .eq(lit(ScalarValue::Binary(Some(vec![0xab, 0xcd]))))
            .not();
        let sql = expr_to_sql_string(&expr).unwrap();
        assert_eq!(sql, "NOT (data = X'ABCD')");
    }

    #[test]
    fn test_is_in() {
        let expr = is_in(col("id"), vec![lit(1i64), lit(2i64), lit(3i64)]);
        let sql = expr_to_sql_string(&expr).unwrap();
        assert!(sql.contains("IN"), "expected IN in: {}", sql);
    }

    #[test]
    fn test_multiple_binary_literals() {
        use datafusion_common::ScalarValue;
        let lhs = col("a").eq(lit(ScalarValue::Binary(Some(vec![0x01]))));
        let rhs = col("b").eq(lit(ScalarValue::Binary(Some(vec![0x02, 0x03]))));
        let expr = lhs.and(rhs);
        let sql = expr_to_sql_string(&expr).unwrap();
        assert_eq!(sql, "((a = X'01') AND (b = X'0203'))");
    }
}
