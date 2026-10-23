// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::sync::Arc;

use arrow_array::RecordBatch;
use lancedb::job::JobEventsRequest;
use napi::bindgen_prelude::Buffer;
use napi_derive::napi;

use crate::error::NapiErrorExt;

/// A handle to an operation that may still be running.
#[napi]
pub struct Job {
    inner: Arc<lancedb::Job>,
}

impl Job {
    pub(crate) fn new<T>(inner: lancedb::Job<T>) -> Self
    where
        T: Clone + Send + Sync + 'static,
    {
        Self {
            inner: Arc::new(inner.map(|_| ())),
        }
    }
}

#[napi]
impl Job {
    /// Identifies the operation on the server that is running it. Operations
    /// that run in this process have no server id. The value is opaque.
    #[napi(getter)]
    pub fn id(&self) -> Option<String> {
        self.inner.id().map(str::to_string)
    }

    /// The operation's current lifecycle state: "running", "finished",
    /// "failed", or "cancelled".
    ///
    /// A point snapshot; unlike {@link Job.wait} it does not block or reject
    /// on a terminal failure state. States a newer server reports that this
    /// client version does not know pass through as-is.
    #[napi(catch_unwind)]
    pub async fn status(&self) -> napi::Result<String> {
        self.inner.status().await.default_error()
    }

    /// Wait until the operation reaches a terminal state.
    #[napi(catch_unwind)]
    pub async fn wait(&self) -> napi::Result<()> {
        self.inner.wait().await.default_error()
    }

    /// Request cancellation. Cancelling a finished operation is a no-op.
    #[napi(catch_unwind)]
    pub async fn cancel(&self) -> napi::Result<()> {
        self.inner.cancel().await.default_error()
    }

    /// Ask the backend for this job's current state, and for a server-side job
    /// its full record, then cache it for the getters below.
    ///
    /// They are all null until this runs, because submitting an operation
    /// returns only a job id. {@link Job.status} fetches the whole record too;
    /// {@link Job.wait} records only the terminal state it establishes.
    #[napi(catch_unwind)]
    pub async fn refresh(&self) -> napi::Result<()> {
        self.inner.refresh().await.default_error()
    }

    /// The last observed lifecycle state, without contacting the backend.
    #[napi(getter)]
    pub fn state(&self) -> Option<String> {
        self.inner.state()
    }

    /// The job's type, as the server names it. Null for an in-process job,
    /// which has no server-side record.
    #[napi(getter)]
    pub fn job_type(&self) -> Option<String> {
        self.inner.job_type()
    }

    /// When the job was created, in milliseconds since the epoch.
    #[napi(getter)]
    pub fn creation_ms(&self) -> Option<i64> {
        self.inner.creation_ms()
    }

    /// The job-type-specific specification as a JSON string, when present.
    #[napi(getter)]
    pub fn spec_json(&self) -> Option<String> {
        self.inner.spec().map(|spec| spec.to_string())
    }

    /// The job-type-specific terminal result as a JSON string. Null until the
    /// job succeeds, so a job that never terminates reports its progress
    /// through {@link Job.events} instead.
    #[napi(getter)]
    pub fn result_json(&self) -> Option<String> {
        self.inner.result().map(|result| result.to_string())
    }

    /// Why the job failed, when it failed and the server reports a reason.
    #[napi(getter)]
    pub fn failure(&self) -> Option<JobFailureInfo> {
        self.inner.failure().map(|failure| JobFailureInfo {
            phase: failure.phase,
            message: failure.message,
            retryable: failure.retryable,
        })
    }

    /// This job's recorded lifecycle events, as an Arrow IPC stream buffer.
    /// The TypeScript wrapper turns it into an Arrow table.
    #[napi(catch_unwind)]
    pub async fn events(&self, limit: Option<u32>, filter: Option<String>) -> napi::Result<Buffer> {
        let batches = self
            .inner
            .events(JobEventsRequest { limit, filter })
            .await
            .default_error()?;
        batches_to_ipc_buffer(&batches)
    }
}

/// Serialise Arrow batches as a single IPC stream for the TypeScript layer.
fn batches_to_ipc_buffer(batches: &[RecordBatch]) -> napi::Result<Buffer> {
    let Some(first) = batches.first() else {
        return Ok(Buffer::from(Vec::<u8>::new()));
    };
    let mut out = Vec::new();
    let mut writer = arrow_ipc::writer::StreamWriter::try_new(&mut out, &first.schema())
        .map_err(|e| napi::Error::from_reason(e.to_string()))?;
    for batch in batches {
        writer
            .write(batch)
            .map_err(|e| napi::Error::from_reason(e.to_string()))?;
    }
    writer
        .finish()
        .map_err(|e| napi::Error::from_reason(e.to_string()))?;
    drop(writer);
    Ok(Buffer::from(out))
}

/// A row from `Connection.listJobs`: one server-side job.
#[napi(object)]
pub struct JobInfo {
    /// The job id -- what `Connection.openJob` and `Connection.cancelJob`
    /// accept.
    pub job_id: String,
    /// The table the job runs against, without URI or namespace.
    pub table: String,
    pub job_type: String,
    /// Lifecycle state: "running", "finished", "failed", or "cancelled".
    pub state: String,
    /// When the job was created, in milliseconds since the epoch.
    pub created_at_millis: i64,
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
#[napi(object)]
pub struct JobFailureInfo {
    pub phase: Option<String>,
    pub message: Option<String>,
    pub retryable: Option<bool>,
}
