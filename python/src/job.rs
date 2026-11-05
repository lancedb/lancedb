// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::sync::Arc;

use crate::runtime::future_into_py;
use arrow::{
    datatypes::Schema,
    pyarrow::{IntoPyArrow, Table as PyArrowTable},
};
use lancedb::job::JobEventsRequest;
use pyo3::{
    Bound, Py, PyAny, PyRef, PyResult, Python,
    exceptions::PyValueError,
    pyclass, pymethods,
    types::{PyAnyMethods, PyDict, PyDictMethods},
};
use serde::Serialize;

use crate::error::PythonErrorExt;

const REPR_INDENT: &str = "    ";

/// Parse a stored JSON payload into Python data. The bindings carry these as
/// strings because that is what crosses the boundary cheaply; the public
/// Python surface is the parsed form.
fn parse_json_payload<'py>(
    py: Python<'py>,
    raw: Option<&str>,
) -> PyResult<Option<Bound<'py, PyAny>>> {
    match raw {
        None => Ok(None),
        Some(raw) => Ok(Some(py.import("json")?.call_method1("loads", (raw,))?)),
    }
}

/// A payload rendered as indented JSON, aligned under the field that holds it.
fn pretty_json_payload(py: Python<'_>, raw: Option<&str>) -> PyResult<Option<String>> {
    let Some(parsed) = parse_json_payload(py, raw)? else {
        return Ok(None);
    };
    let kwargs = PyDict::new(py);
    kwargs.set_item("indent", 4)?;
    let rendered: String = py
        .import("json")?
        .call_method("dumps", (parsed,), Some(&kwargs))?
        .extract()?;
    Ok(Some(rendered.replace('\n', &format!("\n{REPR_INDENT}"))))
}

#[pyclass]
pub struct Job {
    inner: Arc<lancedb::Job<std::result::Result<Option<String>, String>>>,
}

impl Job {
    pub(crate) fn new(inner: lancedb::Job) -> Self {
        Self {
            inner: Arc::new(inner.map(|()| Ok(None))),
        }
    }

    pub(crate) fn new_typed<T>(inner: lancedb::Job<T>) -> Self
    where
        T: Clone + Serialize + Send + Sync + 'static,
    {
        Self {
            inner: Arc::new(inner.map(|result| {
                serde_json::to_string(&result)
                    .map(Some)
                    .map_err(|error| format!("failed to serialize typed job result: {error}"))
            })),
        }
    }
}

#[pymethods]
impl Job {
    #[getter]
    pub fn id(&self) -> Option<String> {
        self.inner.id().map(str::to_string)
    }

    #[getter]
    pub fn job_type(&self) -> Option<String> {
        self.inner.job_type()
    }

    pub fn status(self_: PyRef<'_, Self>) -> PyResult<Bound<'_, PyAny>> {
        let inner = self_.inner.clone();
        future_into_py(
            self_.py(),
            async move { inner.status().await.infer_error() },
        )
    }

    #[pyo3(signature = (progress=None))]
    pub fn wait(self_: PyRef<'_, Self>, progress: Option<Py<PyAny>>) -> PyResult<Bound<'_, PyAny>> {
        let inner = self_.inner.clone();
        future_into_py(self_.py(), async move {
            let result = if let Some(progress) = progress {
                let observer = move |description: &lancedb::database::JobDescription| {
                    Python::attach(|py| {
                        let snapshot = description.progress_snapshot();
                        let callback_result: PyResult<()> = (|| {
                            let raw = snapshot.to_string();
                            let parsed = parse_json_payload(py, Some(&raw))?
                                .ok_or_else(|| PyValueError::new_err("missing progress"))?;
                            progress.bind(py).call1((parsed,))?;
                            Ok(())
                        })();
                        if let Err(error) = callback_result {
                            log::warn!("job progress callback error: {error}");
                        }
                    });
                };
                inner.wait_with_progress(observer).await.infer_error()?
            } else {
                inner.wait().await.infer_error()?
            };
            result
                .map_err(|message| lancedb::Error::Runtime { message })
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

    pub fn refresh(self_: PyRef<'_, Self>) -> PyResult<Bound<'_, PyAny>> {
        let inner = self_.inner.clone();
        future_into_py(self_.py(), async move {
            inner.refresh().await.infer_error()?;
            Ok(())
        })
    }

    /// The last observed lifecycle state, without contacting the backend.
    #[getter]
    pub fn _state(&self) -> Option<String> {
        self.inner.state()
    }

    /// The last observed server-side record. `None` for an in-process job.
    #[getter]
    pub fn _description(&self) -> Option<JobDescription> {
        self.inner.description().map(JobDescription::from)
    }

    #[pyo3(signature = (*, limit=None, filter=None))]
    pub fn events(
        self_: PyRef<'_, Self>,
        limit: Option<u32>,
        filter: Option<String>,
    ) -> PyResult<Bound<'_, PyAny>> {
        let inner = self_.inner.clone();
        let request = JobEventsRequest { limit, filter };
        future_into_py(self_.py(), async move {
            let batches = inner.events(request).await.infer_error()?;
            Python::attach(|py| {
                let schema = batches
                    .first()
                    .map(|batch| batch.schema())
                    .unwrap_or_else(|| Arc::new(Schema::empty()));
                let table = PyArrowTable::try_new(batches, schema)
                    .map_err(|err| PyValueError::new_err(err.to_string()))?;
                table.into_pyarrow(py).map(|table| table.unbind())
            })
        })
    }
}

/// A row from `Connection.list_jobs`: one server-side job.
#[pyclass(module = "lancedb._lancedb", get_all, skip_from_py_object)]
#[derive(Clone)]
pub struct JobInfo {
    job_id: String,
    table: String,
    job_type: String,
    state: String,
    created_at_millis: i64,
    _progress_json: Option<String>,
    paused: bool,
}

#[pymethods]
impl JobInfo {
    fn __repr__(&self) -> String {
        format!(
            "JobInfo(job_id={:?}, table={:?}, job_type={:?}, state={:?}, created_at_millis={})",
            self.job_id, self.table, self.job_type, self.state, self.created_at_millis
        )
    }

    /// Structured progress for job types that report it. Missing fields are
    /// unknown, not zero.
    #[getter]
    fn progress<'py>(&self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyAny>>> {
        parse_json_payload(py, self._progress_json.as_deref())
    }

    /// Whether an operator has parked this job.
    #[getter]
    fn paused(&self) -> bool {
        self.paused
    }
}

impl From<lancedb::database::JobInfo> for JobInfo {
    fn from(info: lancedb::database::JobInfo) -> Self {
        let progress = info.progress_snapshot().to_string();
        Self {
            job_id: info.job_id,
            table: info.table,
            job_type: info.job_type,
            state: info.state,
            created_at_millis: info.created_at_millis,
            _progress_json: Some(progress),
            paused: info.paused,
        }
    }
}

/// The server's account of why a job failed.
#[pyclass(module = "lancedb._lancedb", get_all, skip_from_py_object)]
#[derive(Clone)]
pub struct JobFailureInfo {
    phase: Option<String>,
    message: Option<String>,
    traceback: Option<String>,
    retryable: Option<bool>,
    sql_job_id: Option<String>,
}

#[pymethods]
impl JobFailureInfo {
    fn __repr__(&self) -> String {
        let phase = self
            .phase
            .as_ref()
            .map_or_else(|| "None".to_string(), |value| format!("{value:?}"));
        let message = self
            .message
            .as_ref()
            .map_or_else(|| "None".to_string(), |value| format!("{value:?}"));
        let retryable = match self.retryable {
            Some(true) => "True",
            Some(false) => "False",
            None => "None",
        };
        let sql_job_id = self
            .sql_job_id
            .as_ref()
            .map_or_else(|| "None".to_string(), |value| format!("{value:?}"));
        format!(
            "JobFailureInfo(phase={phase}, message={message}, retryable={retryable}, \
             sql_job_id={sql_job_id})"
        )
    }
}

/// The server-side record behind a `Job` handle.
#[pyclass(module = "lancedb._lancedb", get_all, skip_from_py_object)]
#[derive(Clone)]
pub struct JobDescription {
    job_id: String,
    job_type: String,
    state: String,
    creation_ms: i64,
    /// Internal: the wire form behind the `spec` property.
    _spec_json: Option<String>,
    /// Internal: the wire form behind the `result` property.
    _result_json: Option<String>,
    _progress_json: Option<String>,
    failure: Option<JobFailureInfo>,
}

#[pymethods]
impl JobDescription {
    /// The job-type-specific specification it was submitted with.
    #[getter]
    fn spec<'py>(&self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyAny>>> {
        parse_json_payload(py, self._spec_json.as_deref())
    }

    /// The job-type-specific terminal result. `None` until the job succeeds.
    #[getter]
    fn result<'py>(&self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyAny>>> {
        parse_json_payload(py, self._result_json.as_deref())
    }

    /// Structured progress snapshot. Lifecycle and queue fields are present
    /// even when a job type or older server supplies no counters.
    #[getter]
    fn progress<'py>(&self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyAny>>> {
        parse_json_payload(py, self._progress_json.as_deref())
    }

    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        let mut fields = vec![
            format!("job_id={:?}", self.job_id),
            format!("job_type={:?}", self.job_type),
            format!("state={:?}", self.state),
            format!("creation_ms={}", self.creation_ms),
        ];
        // Lay the payloads out as indented JSON, the same way the `Job` repr
        // does, so the two agree on how the same data looks.
        for (name, payload) in [("spec", &self._spec_json), ("result", &self._result_json)] {
            if let Some(rendered) = pretty_json_payload(py, payload.as_deref())? {
                fields.push(format!("{name}={rendered}"));
            }
        }
        if let Some(failure) = &self.failure {
            fields.push(format!("failure={}", failure.__repr__()));
        }
        let body = fields
            .iter()
            .map(|field| format!("\n{REPR_INDENT}{field},"))
            .collect::<String>();
        Ok(format!("JobDescription({body}\n)"))
    }
}

impl From<lancedb::database::JobDescription> for JobDescription {
    fn from(description: lancedb::database::JobDescription) -> Self {
        let progress_json = description.progress_snapshot().to_string();
        Self {
            job_id: description.job_id,
            job_type: description.job_type,
            state: description.state,
            creation_ms: description.creation_ms,
            _spec_json: (!description.spec.is_null()).then(|| description.spec.to_string()),
            _result_json: description
                .result
                .filter(|result| !result.is_null())
                .map(|result| result.to_string()),
            _progress_json: Some(progress_json),
            failure: description.failure.map(|failure| JobFailureInfo {
                phase: failure.phase,
                message: failure.message,
                traceback: failure.traceback,
                retryable: failure.retryable,
                sql_job_id: failure.sql_job_id,
            }),
        }
    }
}
