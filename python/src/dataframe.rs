// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use arrow::pyarrow::ToPyArrow;
use lancedb::dataframe::{DataFrame, JoinType};
use pyo3::{
    Bound, Py, PyAny, PyRef, PyResult, Python, exceptions::PyValueError, pyclass, pyfunction,
    pymethods,
};

use crate::error::PythonErrorExt;
use crate::expr::PyExpr;
use crate::runtime::future_into_py;

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

/// Python binding around LanceDB's native DataFrame planner.
#[pyclass(name = "NativeDataFrame", module = "lancedb._lancedb", from_py_object)]
#[derive(Clone)]
pub struct NativeDataFrame {
    inner: DataFrame,
}

impl NativeDataFrame {
    pub(crate) fn new(inner: DataFrame) -> Self {
        Self { inner }
    }

    fn wrap(result: lancedb::Result<DataFrame>) -> PyResult<Self> {
        result.map(Self::new).map_err(dataframe_error)
    }
}

#[pymethods]
impl NativeDataFrame {
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

    #[pyo3(signature = (other, all=true))]
    fn intersect(&self, other: &Self, all: bool) -> PyResult<Self> {
        Self::wrap(self.inner.intersect(&other.inner, all))
    }

    #[pyo3(signature = (other, all=true))]
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

    fn execute_async(self_: PyRef<'_, Self>) -> PyResult<Bound<'_, PyAny>> {
        let inner = self_.inner.clone();
        future_into_py(self_.py(), async move {
            inner
                .execute()
                .await
                .map(crate::sql::Query::new)
                .infer_error()
        })
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
