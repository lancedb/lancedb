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
use datafusion_expr::{expr_fn::cast, Expr, ScalarUDF};
use datafusion_functions::string::expr_fn as string_expr_fn;

pub use datafusion_expr::{col, lit};

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

lazy_static::lazy_static! {
    static ref FUNC_REGISTRY: std::sync::RwLock<std::collections::HashMap<String, Arc<ScalarUDF>>> = {
        let mut m = std::collections::HashMap::new();
        m.insert("lower".to_string(), datafusion_functions::string::lower());
        m.insert("upper".to_string(), datafusion_functions::string::upper());
        m.insert("contains".to_string(), datafusion_functions::string::contains());
        m.insert("btrim".to_string(), datafusion_functions::string::btrim());
        m.insert("ltrim".to_string(), datafusion_functions::string::ltrim());
        m.insert("rtrim".to_string(), datafusion_functions::string::rtrim());
        m.insert("concat".to_string(), datafusion_functions::string::concat());
        m.insert("octet_length".to_string(), datafusion_functions::string::octet_length());
        std::sync::RwLock::new(m)
    };
}

pub fn func(name: impl AsRef<str>, args: Vec<Expr>) -> crate::Result<Expr> {
    let name = name.as_ref();
    let registry = FUNC_REGISTRY
        .read()
        .map_err(|e| crate::Error::InvalidInput {
            message: format!("lock poisoned: {}", e),
        })?;
    let udf = registry
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
}
