// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! PyO3 bindings for the LanceDB expression builder API.
//!
//! This module exposes [`PyExpr`] and helper free functions so Python can
//! build type-safe filter / projection expressions that map directly to
//! DataFusion [`Expr`] nodes, bypassing SQL string parsing.

use arrow::{datatypes::DataType, pyarrow::PyArrowType};
use lancedb::expr::{DfExpr, col as ldb_col, contains, expr_cast, lit as df_lit, lower, upper};
use pyo3::{Bound, PyAny, PyResult, exceptions::PyValueError, prelude::*, pyfunction};

/// A type-safe DataFusion expression.
///
/// Instances are constructed via the free functions [`expr_col`] and
/// [`expr_lit`] and combined with the methods on this struct.  On the Python
/// side a thin wrapper class (`lancedb.expr.Expr`) delegates to these methods
/// and adds Python operator overloads.
#[pyclass(name = "PyExpr", from_py_object)]
#[derive(Clone)]
pub struct PyExpr(pub DfExpr);

#[pymethods]
impl PyExpr {
    // ── comparisons ──────────────────────────────────────────────────────────

    fn eq(&self, other: &Self) -> Self {
        Self(self.0.clone().eq(other.0.clone()))
    }

    fn ne(&self, other: &Self) -> Self {
        Self(self.0.clone().not_eq(other.0.clone()))
    }

    fn lt(&self, other: &Self) -> Self {
        Self(self.0.clone().lt(other.0.clone()))
    }

    fn lte(&self, other: &Self) -> Self {
        Self(self.0.clone().lt_eq(other.0.clone()))
    }

    fn gt(&self, other: &Self) -> Self {
        Self(self.0.clone().gt(other.0.clone()))
    }

    fn gte(&self, other: &Self) -> Self {
        Self(self.0.clone().gt_eq(other.0.clone()))
    }

    // ── logical ──────────────────────────────────────────────────────────────

    fn and_(&self, other: &Self) -> Self {
        Self(self.0.clone().and(other.0.clone()))
    }

    fn or_(&self, other: &Self) -> Self {
        Self(self.0.clone().or(other.0.clone()))
    }

    fn not_(&self) -> Self {
        use std::ops::Not;
        Self(self.0.clone().not())
    }

    // ── arithmetic ───────────────────────────────────────────────────────────

    fn add(&self, other: &Self) -> Self {
        use std::ops::Add;
        Self(self.0.clone().add(other.0.clone()))
    }

    fn sub(&self, other: &Self) -> Self {
        use std::ops::Sub;
        Self(self.0.clone().sub(other.0.clone()))
    }

    fn mul(&self, other: &Self) -> Self {
        use std::ops::Mul;
        Self(self.0.clone().mul(other.0.clone()))
    }

    fn div(&self, other: &Self) -> Self {
        use std::ops::Div;
        Self(self.0.clone().div(other.0.clone()))
    }

    // ── string functions ─────────────────────────────────────────────────────

    /// Convert string column to lowercase.
    fn lower(&self) -> Self {
        Self(lower(self.0.clone()))
    }

    /// Convert string column to uppercase.
    fn upper(&self) -> Self {
        Self(upper(self.0.clone()))
    }

    /// Test whether the string contains `substr`.
    fn contains(&self, substr: &Self) -> Self {
        Self(contains(self.0.clone(), substr.0.clone()))
    }

    // ── type cast ────────────────────────────────────────────────────────────

    /// Cast the expression to `data_type`.
    ///
    /// `data_type` must be a PyArrow `DataType` (e.g. `pa.int32()`).
    /// On the Python side, `lancedb.expr.Expr.cast` also accepts type name
    /// strings via `pa.lib.ensure_type` before forwarding here.
    fn cast(&self, data_type: PyArrowType<DataType>) -> Self {
        Self(expr_cast(self.0.clone(), data_type.0))
    }

    // ── utilities ────────────────────────────────────────────────────────────

    /// Render the expression as a SQL string (useful for debugging).
    fn to_sql(&self) -> PyResult<String> {
        lancedb::expr::expr_to_sql_string(&self.0).map_err(|e| PyValueError::new_err(e.to_string()))
    }

    fn __repr__(&self) -> PyResult<String> {
        let sql =
            lancedb::expr::expr_to_sql_string(&self.0).unwrap_or_else(|_| "<expr>".to_string());
        Ok(format!("PyExpr({})", sql))
    }
}

// ── free functions ────────────────────────────────────────────────────────────

/// Create a column reference expression.
///
/// The column name is preserved exactly as given (case-sensitive), so
/// `col("firstName")` correctly references a field named `firstName`.
#[pyfunction]
pub fn expr_col(name: &str) -> PyExpr {
    PyExpr(ldb_col(name))
}

/// Create a literal value expression.
///
/// Supported Python types: `bool`, `int`, `float`, `str`.
#[pyfunction]
pub fn expr_lit(value: Bound<'_, PyAny>) -> PyResult<PyExpr> {
    // bool must be checked before int because bool is a subclass of int in Python
    if let Ok(b) = value.extract::<bool>() {
        return Ok(PyExpr(df_lit(b)));
    }
    if let Ok(i) = value.extract::<i64>() {
        return Ok(PyExpr(df_lit(i)));
    }
    if let Ok(f) = value.extract::<f64>() {
        return Ok(PyExpr(df_lit(f)));
    }
    if let Ok(s) = value.extract::<String>() {
        return Ok(PyExpr(df_lit(s)));
    }
    Err(PyValueError::new_err(format!(
        "unsupported literal type: {}. Supported: bool, int, float, str",
        value.get_type().name()?
    )))
}

/// Call an arbitrary registered SQL function by name.
///
/// See `lancedb::expr::func` for the list of supported function names.
#[pyfunction]
pub fn expr_func(name: &str, args: Vec<PyExpr>) -> PyResult<PyExpr> {
    let df_args: Vec<DfExpr> = args.into_iter().map(|e| e.0).collect();
    lancedb::expr::func(name, df_args)
        .map(PyExpr)
        .map_err(|e| PyValueError::new_err(e.to_string()))
}
