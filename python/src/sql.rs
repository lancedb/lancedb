// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::sync::Arc;

use chrono::{DateTime, Utc};
use pyo3::{Bound, PyAny, PyRef, PyResult, pyclass, pymethods};

use crate::arrow::RecordBatchStream;
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

    pub fn reader(self_: PyRef<'_, Self>) -> PyResult<Bound<'_, PyAny>> {
        let inner = self_.inner.clone();
        future_into_py(self_.py(), async move {
            let stream = inner.reader().await.infer_error()?;
            Ok(RecordBatchStream::new(stream))
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
