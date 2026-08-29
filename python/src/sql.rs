// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::sync::Arc;

use arrow::pyarrow::ToPyArrow;
use chrono::{DateTime, Utc};
use pyo3::{
    Bound, Py, PyAny, PyRef, PyResult, Python, pyclass, pymethods,
    types::{PyDict, PyList, PyListMethods, PyModule, PyModuleMethods},
};

use crate::error::PythonErrorExt;
use crate::runtime::future_into_py;

#[pyclass(name = "SqlQuery")]
pub struct Query {
    inner: Arc<lancedb::sql::Query>,
}

impl Query {
    pub(crate) fn new(inner: lancedb::sql::Query) -> Self {
        Self {
            inner: Arc::new(inner),
        }
    }
}

#[pymethods]
impl Query {
    #[getter]
    pub fn id(&self) -> String {
        self.inner.id().to_string()
    }

    pub fn describe(self_: PyRef<'_, Self>) -> PyResult<Bound<'_, PyAny>> {
        let inner = self_.inner.clone();
        future_into_py(self_.py(), async move {
            inner
                .describe()
                .await
                .map(QueryDescription::from)
                .infer_error()
        })
    }

    pub fn result(self_: PyRef<'_, Self>) -> PyResult<Bound<'_, PyAny>> {
        let inner = self_.inner.clone();
        future_into_py(self_.py(), async move {
            let batches = inner.result().await.infer_error()?;
            Python::attach(|py| batches_to_pyarrow(py, batches))
        })
    }

    pub fn cancel(self_: PyRef<'_, Self>) -> PyResult<Bound<'_, PyAny>> {
        let inner = self_.inner.clone();
        future_into_py(self_.py(), async move {
            inner.cancel().await.infer_error()?;
            Ok(())
        })
    }
}

#[pyclass(get_all, skip_from_py_object)]
#[derive(Clone)]
pub struct QueryDescription {
    id: String,
    status: String,
    progress: Option<f64>,
    expires_at: Option<DateTime<Utc>>,
}

#[pymethods]
impl QueryDescription {
    fn __repr__(&self) -> String {
        format!(
            "QueryDescription(id={:?}, status={:?}, progress={:?}, expires_at={:?})",
            self.id, self.status, self.progress, self.expires_at
        )
    }
}

impl From<lancedb::sql::QueryDescription> for QueryDescription {
    fn from(description: lancedb::sql::QueryDescription) -> Self {
        Self {
            id: description.id,
            status: description.status,
            progress: description.progress,
            expires_at: description.expires_at,
        }
    }
}

fn batches_to_pyarrow(
    py: Python<'_>,
    batches: Vec<arrow::array::RecordBatch>,
) -> PyResult<Py<PyAny>> {
    let pyarrow = PyModule::import(py, "pyarrow")?;
    if batches.is_empty() {
        return Ok(pyarrow.call_method1("table", (PyDict::new(py),))?.unbind());
    }
    let py_batches = PyList::empty(py);
    for batch in batches {
        py_batches.append(batch.to_pyarrow(py)?)?;
    }
    Ok(pyarrow
        .getattr("Table")?
        .call_method1("from_batches", (py_batches,))?
        .unbind())
}
