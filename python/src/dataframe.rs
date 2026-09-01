// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::sync::Arc;

use arrow::{
    datatypes::Schema,
    pyarrow::{PyArrowType, ToPyArrow},
};
use datafusion::{
    dataframe::DataFrame as DfDataFrame,
    datasource::provider_as_source,
    execution::context::SessionContext,
    logical_expr::{JoinType, LogicalPlanBuilder},
};
use datafusion_catalog::empty::EmptyTable;
use datafusion_common::TableReference;
use datafusion_expr::SortExpr;
use datafusion_functions_aggregate::expr_fn::{avg, count, max, min, sum};
use datafusion_substrait::{logical_plan::producer::to_substrait_plan, substrait::proto::Plan};
use prost::Message;
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
    let plan = Plan::decode(plan).map_err(dataframe_error)?;
    let version = plan
        .version
        .ok_or_else(|| PyValueError::new_err("Substrait plan does not contain a version"))?;
    Ok(format!(
        "{}.{}.{}",
        version.major_number, version.minor_number, version.patch_number
    ))
}

/// Native immutable DataFusion logical-plan wrapper used by the Python DataFrame API.
#[pyclass(name = "NativeDataFrame", module = "lancedb._lancedb", from_py_object)]
#[derive(Clone)]
pub struct NativeDataFrame {
    inner: DfDataFrame,
}

impl NativeDataFrame {
    fn wrap(result: datafusion_common::Result<DfDataFrame>) -> PyResult<Self> {
        result.map(|inner| Self { inner }).map_err(dataframe_error)
    }

    fn encoded_plan(&self) -> PyResult<(Vec<u8>, String)> {
        let (state, logical_plan) = self.inner.clone().into_parts();
        let plan = to_substrait_plan(&logical_plan, &state).map_err(dataframe_error)?;
        let version = plan
            .version
            .as_ref()
            .map(|version| {
                format!(
                    "{}.{}.{}",
                    version.major_number, version.minor_number, version.patch_number
                )
            })
            .ok_or_else(|| PyValueError::new_err("generated Substrait plan has no version"))?;
        Ok((plan.encode_to_vec(), version))
    }
}

#[pymethods]
impl NativeDataFrame {
    #[staticmethod]
    fn from_table(name: String, schema: PyArrowType<Schema>) -> PyResult<Self> {
        let context = SessionContext::new();
        let source = provider_as_source(Arc::new(EmptyTable::new(Arc::new(schema.0))));
        let plan = LogicalPlanBuilder::scan(TableReference::bare(name), source, None)
            .and_then(LogicalPlanBuilder::build)
            .map_err(dataframe_error)?;
        Ok(Self {
            inner: DfDataFrame::new(context.state(), plan),
        })
    }

    fn select(&self, expressions: Vec<PyExpr>) -> PyResult<Self> {
        Self::wrap(
            self.inner
                .clone()
                .select(expressions.into_iter().map(|expr| expr.0)),
        )
    }

    fn filter(&self, predicate: PyExpr) -> PyResult<Self> {
        Self::wrap(self.inner.clone().filter(predicate.0))
    }

    fn aggregate(&self, groups: Vec<PyExpr>, aggregates: Vec<PyExpr>) -> PyResult<Self> {
        Self::wrap(self.inner.clone().aggregate(
            groups.into_iter().map(|expr| expr.0).collect(),
            aggregates.into_iter().map(|expr| expr.0).collect(),
        ))
    }

    fn sort(&self, expressions: Vec<(PyExpr, bool, bool)>) -> PyResult<Self> {
        let expressions: Vec<SortExpr> = expressions
            .into_iter()
            .map(|(expr, ascending, nulls_first)| expr.0.sort(ascending, nulls_first))
            .collect();
        Self::wrap(self.inner.clone().sort(expressions))
    }

    #[pyo3(signature = (count, offset=0))]
    fn limit(&self, count: usize, offset: usize) -> PyResult<Self> {
        Self::wrap(self.inner.clone().limit(offset, Some(count)))
    }

    fn distinct(&self) -> PyResult<Self> {
        Self::wrap(self.inner.clone().distinct())
    }

    fn alias(&self, name: &str) -> PyResult<Self> {
        Self::wrap(self.inner.clone().alias(name))
    }

    fn with_column(&self, name: &str, expression: PyExpr) -> PyResult<Self> {
        Self::wrap(self.inner.clone().with_column(name, expression.0))
    }

    #[pyo3(name = "drop")]
    fn drop_columns(&self, columns: Vec<String>) -> PyResult<Self> {
        Self::wrap(self.inner.clone().drop_columns(&columns))
    }

    fn with_column_renamed(&self, old_name: String, new_name: &str) -> PyResult<Self> {
        Self::wrap(self.inner.clone().with_column_renamed(old_name, new_name))
    }

    #[pyo3(signature = (other, left_on, right_on, how="inner"))]
    fn join(
        &self,
        other: &Self,
        left_on: Vec<String>,
        right_on: Vec<String>,
        how: &str,
    ) -> PyResult<Self> {
        let left_on = left_on.iter().map(String::as_str).collect::<Vec<_>>();
        let right_on = right_on.iter().map(String::as_str).collect::<Vec<_>>();
        Self::wrap(self.inner.clone().join(
            other.inner.clone(),
            join_type(how)?,
            &left_on,
            &right_on,
            None,
        ))
    }

    #[pyo3(signature = (other, all=true))]
    fn union(&self, other: &Self, all: bool) -> PyResult<Self> {
        if all {
            Self::wrap(self.inner.clone().union(other.inner.clone()))
        } else {
            Self::wrap(self.inner.clone().union_distinct(other.inner.clone()))
        }
    }

    #[pyo3(signature = (other, all=false))]
    fn intersect(&self, other: &Self, all: bool) -> PyResult<Self> {
        if all {
            Self::wrap(self.inner.clone().intersect(other.inner.clone()))
        } else {
            Self::wrap(self.inner.clone().intersect_distinct(other.inner.clone()))
        }
    }

    #[pyo3(signature = (other, all=false))]
    fn except_(&self, other: &Self, all: bool) -> PyResult<Self> {
        if all {
            Self::wrap(self.inner.clone().except(other.inner.clone()))
        } else {
            Self::wrap(self.inner.clone().except_distinct(other.inner.clone()))
        }
    }

    fn schema(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.inner
            .schema()
            .as_arrow()
            .clone()
            .to_pyarrow(py)
            .map(Bound::unbind)
    }

    fn to_substrait(&self, py: Python<'_>) -> PyResult<Py<PyBytes>> {
        let (plan, _) = self.encoded_plan()?;
        Ok(PyBytes::new(py, &plan).unbind())
    }

    fn to_substrait_with_version(&self, py: Python<'_>) -> PyResult<(Py<PyBytes>, String)> {
        let (plan, version) = self.encoded_plan()?;
        Ok((PyBytes::new(py, &plan).unbind(), version))
    }

    fn __repr__(&self) -> String {
        format!(
            "NativeDataFrame({})",
            self.inner.logical_plan().display_indent()
        )
    }
}

#[pyfunction]
pub fn aggregate_sum(expr: PyExpr) -> PyExpr {
    PyExpr(sum(expr.0))
}

#[pyfunction]
pub fn aggregate_avg(expr: PyExpr) -> PyExpr {
    PyExpr(avg(expr.0))
}

#[pyfunction]
pub fn aggregate_min(expr: PyExpr) -> PyExpr {
    PyExpr(min(expr.0))
}

#[pyfunction]
pub fn aggregate_max(expr: PyExpr) -> PyExpr {
    PyExpr(max(expr.0))
}

#[pyfunction]
pub fn aggregate_count(expr: PyExpr) -> PyExpr {
    PyExpr(count(expr.0))
}
