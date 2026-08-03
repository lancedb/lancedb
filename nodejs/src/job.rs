// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::sync::Arc;

use napi_derive::napi;

use crate::error::NapiErrorExt;

/// A handle to an operation that may still be running.
#[napi]
pub struct Job {
    inner: Arc<lancedb::Job>,
}

impl Job {
    pub(crate) fn new(inner: lancedb::Job) -> Self {
        Self {
            inner: Arc::new(inner),
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
}

/// A row from `Connection.listJobs`: one server-side job.
#[napi(object)]
pub struct JobInfo {
    /// The job id -- what `Connection.getJob` and `Connection.cancelJob`
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

/// A described job from `Connection.getJob`.
#[napi(object)]
pub struct JobDescription {
    pub job_id: String,
    pub job_type: String,
    /// Lifecycle state: "running", "finished", "failed", or "cancelled".
    pub state: String,
    /// When the job was created, in milliseconds since the epoch.
    pub creation_ms: i64,
    /// The job-type-specific specification as a JSON string, when present.
    pub spec_json: Option<String>,
    /// Why the job failed, when the job is failed and the server reports a
    /// reason.
    pub failure: Option<JobFailureInfo>,
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
