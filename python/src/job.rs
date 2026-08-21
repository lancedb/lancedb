// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::sync::Arc;

use crate::runtime::future_into_py;
use async_trait::async_trait;
use pyo3::{Bound, PyAny, PyRef, PyResult, pyclass, pymethods};
use serde::Serialize;

use crate::error::PythonErrorExt;

#[pyclass]
pub struct Job {
    inner: Arc<lancedb::Job>,
}

/// Type-erased Python bridge for any JSON-serializable Rust [`lancedb::Job`]
/// result.
#[pyclass]
pub struct TypedJob {
    inner: Arc<dyn TypedJobHandle>,
}

impl TypedJob {
    pub(crate) fn new<T>(inner: lancedb::Job<T>) -> Self
    where
        T: Clone + Serialize + Send + Sync + 'static,
    {
        Self {
            inner: Arc::new(TypedJobHandleImpl {
                inner: Arc::new(inner),
            }),
        }
    }
}

#[async_trait]
trait TypedJobHandle: Send + Sync {
    fn id(&self) -> Option<&str>;
    async fn status(&self) -> lancedb::Result<String>;
    async fn wait_json(&self) -> lancedb::Result<String>;
    async fn cancel(&self) -> lancedb::Result<()>;
}

struct TypedJobHandleImpl<T>
where
    T: Clone + Serialize + Send + Sync + 'static,
{
    inner: Arc<lancedb::Job<T>>,
}

#[async_trait]
impl<T> TypedJobHandle for TypedJobHandleImpl<T>
where
    T: Clone + Serialize + Send + Sync + 'static,
{
    fn id(&self) -> Option<&str> {
        self.inner.id()
    }

    async fn status(&self) -> lancedb::Result<String> {
        self.inner.status().await
    }

    async fn wait_json(&self) -> lancedb::Result<String> {
        let result = self.inner.wait().await?;
        serde_json::to_string(&result).map_err(|error| lancedb::Error::Runtime {
            message: format!("failed to serialize typed job result: {error}"),
        })
    }

    async fn cancel(&self) -> lancedb::Result<()> {
        self.inner.cancel().await
    }
}

impl Job {
    pub(crate) fn new(inner: lancedb::Job) -> Self {
        Self {
            inner: Arc::new(inner),
        }
    }
}

#[pymethods]
impl TypedJob {
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
        future_into_py(
            self_.py(),
            async move { inner.wait_json().await.infer_error() },
        )
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
            Ok(None::<()>)
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
