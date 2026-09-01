// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::sync::Arc;

use arrow::{
    datatypes::Schema,
    pyarrow::{PyArrowType, ToPyArrow},
};
use lancedb::dataframe::{DataFrame, JoinType};
use pyo3::{
    Bound, Py, PyAny, PyResult, Python, exceptions::PyValueError, pyclass, pyfunction, pymethods,
    types::PyBytes,
};

use crate::expr::PyExpr;

fn dataframe_error(error: impl std::fmt::Display) -> pyo3::PyErr {
    PyValueError::new_err(error.to_string())
}

fn join_type(value: &str) -> PyResult<JoinType> {
    match value.to_ascii_lowercase().as_str() {
        "inner" => Ok(JoinType::Inner),
        "left" | "left_outer" => Ok(JoinType::Left),
        "right" | "right_outer" => Ok(JoinType::Right),
        "full" | "full_outer" => Ok(JoinType::Full),
        "semi" | "left_semi" => Ok(JoinType::LeftSemi),
        "right_semi" => Ok(JoinType::RightSemi),
        "anti" | "left_anti" => Ok(JoinType::LeftAnti),
        "right_anti" => Ok(JoinType::RightAnti),
        _ => Err(PyValueError::new_err(format!(
            "unsupported join type: {value}"
        ))),
    }
}

pub(crate) fn plan_version(plan: &[u8]) -> PyResult<String> {
    lancedb::dataframe::plan_version(plan).map_err(dataframe_error)
}

/// Python binding around the language-neutral LanceDB DataFrame planner.
#[pyclass(name = "NativeDataFrame", module = "lancedb._lancedb", from_py_object)]
#[derive(Clone)]
pub struct NativeDataFrame {
    inner: DataFrame,
}

impl NativeDataFrame {
    fn wrap(result: lancedb::Result<DataFrame>) -> PyResult<Self> {
        result.map(|inner| Self { inner }).map_err(dataframe_error)
    }
}

#[pymethods]
impl NativeDataFrame {
    #[staticmethod]
    fn from_table(name: String, schema: PyArrowType<Schema>) -> PyResult<Self> {
        Self::wrap(DataFrame::from_table(name, Arc::new(schema.0)))
    }

    fn select(&self, expressions: Vec<PyExpr>) -> PyResult<Self> {
        Self::wrap(
            self.inner
                .select(expressions.into_iter().map(|expr| expr.0).collect()),
        )
    }

    fn filter(&self, predicate: PyExpr) -> PyResult<Self> {
        Self::wrap(self.inner.filter(predicate.0))
    }

    fn aggregate(&self, groups: Vec<PyExpr>, aggregates: Vec<PyExpr>) -> PyResult<Self> {
        Self::wrap(self.inner.aggregate(
            groups.into_iter().map(|expr| expr.0).collect(),
            aggregates.into_iter().map(|expr| expr.0).collect(),
        ))
    }

    fn sort(&self, expressions: Vec<(PyExpr, bool, bool)>) -> PyResult<Self> {
        Self::wrap(
            self.inner.sort(
                expressions
                    .into_iter()
                    .map(|(expr, ascending, nulls_first)| (expr.0, ascending, nulls_first))
                    .collect(),
            ),
        )
    }

    #[pyo3(signature = (count, offset=0))]
    fn limit(&self, count: usize, offset: usize) -> PyResult<Self> {
        Self::wrap(self.inner.limit(count, offset))
    }

    fn distinct(&self) -> PyResult<Self> {
        Self::wrap(self.inner.distinct())
    }

    fn alias(&self, name: &str) -> PyResult<Self> {
        Self::wrap(self.inner.alias(name))
    }

    fn column(&self, name: &str) -> PyResult<PyExpr> {
        self.inner.column(name).map(PyExpr).map_err(dataframe_error)
    }

    fn with_column(&self, name: &str, expression: PyExpr) -> PyResult<Self> {
        Self::wrap(self.inner.with_column(name, expression.0))
    }

    #[pyo3(name = "drop")]
    fn drop_columns(&self, columns: Vec<String>) -> PyResult<Self> {
        Self::wrap(self.inner.drop_columns(&columns))
    }

    fn with_column_renamed(&self, old_name: String, new_name: &str) -> PyResult<Self> {
        Self::wrap(self.inner.with_column_renamed(&old_name, new_name))
    }

    #[pyo3(signature = (other, left_on, right_on, how="inner"))]
    fn join(
        &self,
        other: &Self,
        left_on: Vec<String>,
        right_on: Vec<String>,
        how: &str,
    ) -> PyResult<Self> {
        Self::wrap(
            self.inner
                .join(&other.inner, &left_on, &right_on, join_type(how)?),
        )
    }

    #[pyo3(signature = (other, all=true))]
    fn union(&self, other: &Self, all: bool) -> PyResult<Self> {
        Self::wrap(self.inner.union(&other.inner, all))
    }

    #[pyo3(signature = (other, all=false))]
    fn intersect(&self, other: &Self, all: bool) -> PyResult<Self> {
        Self::wrap(self.inner.intersect(&other.inner, all))
    }

    #[pyo3(signature = (other, all=false))]
    fn except_(&self, other: &Self, all: bool) -> PyResult<Self> {
        Self::wrap(self.inner.except(&other.inner, all))
    }

    fn schema(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.inner
            .schema()
            .to_pyarrow(py)
            .map(Bound::unbind)
            .map_err(dataframe_error)
    }

    fn to_substrait(&self, py: Python<'_>) -> PyResult<Py<PyBytes>> {
        let plan = self.inner.to_substrait().map_err(dataframe_error)?;
        Ok(PyBytes::new(py, plan.bytes()).unbind())
    }

    fn to_substrait_with_version(&self, py: Python<'_>) -> PyResult<(Py<PyBytes>, String)> {
        let plan = self.inner.to_substrait().map_err(dataframe_error)?;
        let (bytes, version) = plan.into_parts();
        Ok((PyBytes::new(py, &bytes).unbind(), version))
    }

    fn __repr__(&self) -> String {
        format!("NativeDataFrame({})", self.inner.display())
    }
}

#[pyfunction]
pub fn aggregate_sum(expr: PyExpr) -> PyExpr {
    PyExpr(lancedb::dataframe::aggregate_sum(expr.0))
}

#[pyfunction]
pub fn aggregate_avg(expr: PyExpr) -> PyExpr {
    PyExpr(lancedb::dataframe::aggregate_avg(expr.0))
}

#[pyfunction]
pub fn aggregate_min(expr: PyExpr) -> PyExpr {
    PyExpr(lancedb::dataframe::aggregate_min(expr.0))
}

#[pyfunction]
pub fn aggregate_max(expr: PyExpr) -> PyExpr {
    PyExpr(lancedb::dataframe::aggregate_max(expr.0))
}

#[pyfunction]
pub fn aggregate_count(expr: PyExpr) -> PyExpr {
    PyExpr(lancedb::dataframe::aggregate_count(expr.0))
}
