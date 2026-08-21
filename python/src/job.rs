// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::sync::Arc;

use crate::runtime::future_into_py;
use pyo3::{Bound, PyAny, PyRef, PyResult, pyclass, pymethods};

use crate::error::PythonErrorExt;

#[pyclass]
pub struct Job {
    inner: Arc<lancedb::Job>,
}

/// Python bridge for a typed remote Function registration job.
///
/// The public Python layer decodes the canonical JSON returned by `wait`
/// into its immutable `FunctionVersion` model.
#[pyclass]
pub struct FunctionJob {
    inner: Arc<lancedb::Job<lancedb::function::FunctionVersion>>,
}

/// Python bridge for a typed remote Function-column refresh job.
#[pyclass]
pub struct RefreshJob {
    inner: Arc<lancedb::Job>,
}

impl FunctionJob {
    pub(crate) fn new(inner: lancedb::Job<lancedb::function::FunctionVersion>) -> Self {
        Self {
            inner: Arc::new(inner),
        }
    }
}

impl Job {
    pub(crate) fn new(inner: lancedb::Job) -> Self {
        Self {
            inner: Arc::new(inner),
        }
    }
}

impl RefreshJob {
    pub(crate) fn new(inner: lancedb::Job) -> Self {
        Self {
            inner: Arc::new(inner),
        }
    }
}

#[pymethods]
impl RefreshJob {
    #[getter]
    pub fn id(&self) -> Option<String> {
        self.inner.id().map(str::to_string)
    }

    pub fn status(self_: PyRef<'_, Self>) -> PyResult<Bound<'_, PyAny>> {
        let inner = self_.inner.clone();
        future_into_py(
            self_.py(),
            async move { inner.status().await.infer_error() },
        )
    }

    pub fn wait(self_: PyRef<'_, Self>) -> PyResult<Bound<'_, PyAny>> {
        let inner = self_.inner.clone();
        future_into_py(self_.py(), async move {
            let value = inner.wait_raw_json().await.infer_error()?.ok_or_else(|| {
                pyo3::exceptions::PyValueError::new_err(
                    "successful refresh job did not contain a result",
                )
            })?;
            let result: lancedb::function::RefreshColumnResult = serde_json::from_value(value)
                .map_err(|error| {
                    pyo3::exceptions::PyValueError::new_err(format!(
                        "invalid refresh job result: {error}"
                    ))
                })?;
            result.to_canonical_json().infer_error()
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

#[pymethods]
impl FunctionJob {
    #[getter]
    pub fn id(&self) -> Option<String> {
        self.inner.id().map(str::to_string)
    }

    pub fn status(self_: PyRef<'_, Self>) -> PyResult<Bound<'_, PyAny>> {
        let inner = self_.inner.clone();
        future_into_py(
            self_.py(),
            async move { inner.status().await.infer_error() },
        )
    }

    pub fn wait(self_: PyRef<'_, Self>) -> PyResult<Bound<'_, PyAny>> {
        let inner = self_.inner.clone();
        future_into_py(self_.py(), async move {
            inner
                .wait()
                .await
                .infer_error()?
                .to_canonical_json()
                .infer_error()
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

#[pymethods]
impl Job {
    #[getter]
    pub fn id(&self) -> Option<String> {
        self.inner.id().map(str::to_string)
    }

    pub fn status(self_: PyRef<'_, Self>) -> PyResult<Bound<'_, PyAny>> {
        let inner = self_.inner.clone();
        future_into_py(
            self_.py(),
            async move { inner.status().await.infer_error() },
        )
    }

    pub fn wait(self_: PyRef<'_, Self>) -> PyResult<Bound<'_, PyAny>> {
        let inner = self_.inner.clone();
        future_into_py(self_.py(), async move {
            inner.wait().await.infer_error()?;
            Ok(())
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

/// A row from `Connection.list_jobs`: one server-side job.
#[pyclass(get_all, skip_from_py_object)]
#[derive(Clone)]
pub struct JobInfo {
    job_id: String,
    table: String,
    job_type: String,
    state: String,
    created_at_millis: i64,
}

#[pymethods]
impl JobInfo {
    fn __repr__(&self) -> String {
        format!(
            "JobInfo(job_id={:?}, table={:?}, job_type={:?}, state={:?}, created_at_millis={})",
            self.job_id, self.table, self.job_type, self.state, self.created_at_millis
        )
    }
}

impl From<lancedb::database::JobInfo> for JobInfo {
    fn from(info: lancedb::database::JobInfo) -> Self {
        Self {
            job_id: info.job_id,
            table: info.table,
            job_type: info.job_type,
            state: info.state,
            created_at_millis: info.created_at_millis,
        }
    }
}

/// The server's account of why a job failed.
#[pyclass(get_all, skip_from_py_object)]
#[derive(Clone)]
pub struct JobFailureInfo {
    phase: Option<String>,
    message: Option<String>,
    retryable: Option<bool>,
}

#[pymethods]
impl JobFailureInfo {
    fn __repr__(&self) -> String {
        format!(
            "JobFailureInfo(phase={:?}, message={:?}, retryable={:?})",
            self.phase, self.message, self.retryable
        )
    }
}

/// A described job from `Connection.get_job`.
#[pyclass(get_all, skip_from_py_object)]
#[derive(Clone)]
pub struct JobDescription {
    job_id: String,
    job_type: String,
    state: String,
    creation_ms: i64,
    spec_json: Option<String>,
    failure: Option<JobFailureInfo>,
}

#[pymethods]
impl JobDescription {
    fn __repr__(&self) -> String {
        format!(
            "JobDescription(job_id={:?}, job_type={:?}, state={:?}, creation_ms={})",
            self.job_id, self.job_type, self.state, self.creation_ms
        )
    }
}

impl From<lancedb::database::JobDescription> for JobDescription {
    fn from(description: lancedb::database::JobDescription) -> Self {
        Self {
            job_id: description.job_id,
            job_type: description.job_type,
            state: description.state,
            creation_ms: description.creation_ms,
            spec_json: (!description.spec.is_null()).then(|| description.spec.to_string()),
            failure: description.failure.map(|failure| JobFailureInfo {
                phase: failure.phase,
                message: failure.message,
                retryable: failure.retryable,
            }),
        }
    }
}
