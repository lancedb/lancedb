// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Handles to operations a server may run asynchronously.

use std::sync::Arc;

use async_trait::async_trait;
use serde::de::DeserializeOwned;
use serde_json::Value;
use tokio::sync::watch;
use tokio::task::{AbortHandle, JoinHandle};

use crate::error::{Error, JobFailure, Result};

/// Backend-specific tracking for an asynchronous operation.
#[async_trait]
pub(crate) trait JobHandle: Send + Sync {
    /// Server-assigned id, when the backend has one.
    fn id(&self) -> Option<&str> {
        None
    }
    async fn status(&self) -> Result<String>;
    async fn wait(&self) -> Result<TerminalResult>;
    async fn cancel(&self) -> Result<()>;
}

/// A successful remote terminal result.
pub(crate) struct TerminalResult {
    value: Option<Value>,
    request_id: Option<String>,
}

impl TerminalResult {
    pub(crate) fn remote(value: Option<Value>, request_id: String) -> Self {
        Self {
            value,
            request_id: Some(request_id),
        }
    }

    fn decode<T: DeserializeOwned>(self) -> Result<T> {
        let request_id = self.request_id.unwrap_or_default();
        let value = self.value.ok_or_else(|| Error::Http {
            source: "successful typed job response did not contain a result".into(),
            request_id: request_id.clone(),
            status_code: None,
        })?;
        serde_json::from_value(value).map_err(|error| Error::Http {
            source: format!("failed to parse typed job result: {error}").into(),
            request_id,
            status_code: None,
        })
    }
}

type ResultDecoder<T> = fn(TerminalResult) -> Result<T>;

enum JobInner<T> {
    Handle {
        handle: Box<dyn JobHandle>,
        decode: ResultDecoder<T>,
    },
    Spawned(SpawnedJob<T>),
    Completed(T),
}

/// A handle to an operation that may still be running.
///
/// The operation may already be complete when the handle is created. `T` is
/// the endpoint's successful terminal result; unit-result operations use the
/// default `Job<()>`.
pub struct Job<T = ()>
where
    T: Clone + Send + Sync + 'static,
{
    inner: JobInner<T>,
}

impl<T> std::fmt::Debug for Job<T>
where
    T: Clone + Send + Sync + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let done = matches!(&self.inner, JobInner::Completed(_))
            || matches!(&self.inner, JobInner::Spawned(job) if job.is_done());
        f.debug_struct("Job")
            .field("id", &self.id())
            .field("done", &done)
            .finish()
    }
}

impl Job<()> {
    /// A job whose operation finished before the handle was created.
    pub(crate) fn new_done() -> Self {
        Self {
            inner: JobInner::Completed(()),
        }
    }

    pub(crate) fn new(handle: Box<dyn JobHandle>) -> Self {
        Self {
            inner: JobInner::Handle {
                handle,
                decode: |_| Ok(()),
            },
        }
    }
}

impl<T> Job<T>
where
    T: Clone + DeserializeOwned + Send + Sync + 'static,
{
    /// Construct a typed remote Job for a result-specific submit API.
    pub(crate) fn new_typed(handle: Box<dyn JobHandle>) -> Self {
        Self {
            inner: JobInner::Handle {
                handle,
                decode: TerminalResult::decode::<T>,
            },
        }
    }
}

impl<T> Job<T>
where
    T: Clone + Send + Sync + 'static,
{
    /// A typed job running as a task in this process.
    pub(crate) fn spawned(task: JoinHandle<Result<T>>) -> Self {
        Self {
            inner: JobInner::Spawned(SpawnedJob::new(task)),
        }
    }

    /// Identifies the operation on the server that is running it.
    ///
    /// Returned for correlating with server logs or the jobs API. Operations
    /// that run in this process have no server id and return `None`. The
    /// value is opaque: parsing it or storing it to resume the job later is
    /// not supported.
    pub fn id(&self) -> Option<&str> {
        match &self.inner {
            JobInner::Handle { handle, .. } => handle.id(),
            JobInner::Spawned(_) => None,
            JobInner::Completed(_) => None,
        }
    }

    /// The operation's current lifecycle state: "running", "finished",
    /// "failed", or "cancelled".
    ///
    /// A point snapshot; unlike [`Job::wait`] it does not block, raise on a
    /// terminal failure state, or retry. States a newer server reports that
    /// this client version does not know pass through as-is.
    pub async fn status(&self) -> Result<String> {
        match &self.inner {
            JobInner::Handle { handle, .. } => handle.status().await,
            JobInner::Spawned(job) => Ok(job.status()),
            JobInner::Completed(_) => Ok("finished".to_string()),
        }
    }

    /// Waits until the operation reaches a terminal state.
    ///
    /// Returns the endpoint's typed result. Unit-result jobs return `()`.
    ///
    /// Returns [`crate::Error::JobFailed`] if the operation failed and
    /// [`crate::Error::JobCancelled`] if it was cancelled.
    pub async fn wait(&self) -> Result<T> {
        match &self.inner {
            JobInner::Handle { handle, decode } => decode(handle.wait().await?),
            JobInner::Spawned(job) => job.wait().await,
            JobInner::Completed(result) => Ok(result.clone()),
        }
    }

    /// Requests cancellation of the operation.
    ///
    /// Cancelling an operation that already finished is a no-op.
    pub async fn cancel(&self) -> Result<()> {
        match &self.inner {
            JobInner::Handle { handle, .. } => handle.cancel().await,
            JobInner::Spawned(job) => {
                job.cancel();
                Ok(())
            }
            JobInner::Completed(_) => Ok(()),
        }
    }
}

/// How an in-process operation ended. Cloneable so every waiter can be given
/// the outcome; [`Error`] is not, so failures share one behind an [`Arc`].
#[derive(Clone)]
enum Outcome<T> {
    Succeeded(T),
    Failed(Arc<Error>),
    Cancelled,
}

impl<T> Outcome<T> {
    fn into_result(self) -> Result<T> {
        match self {
            Self::Succeeded(result) => Ok(result),
            Self::Failed(source) => Err(Error::JobFailed {
                job_id: None,
                failure: JobFailure::from_source(source),
            }),
            Self::Cancelled => Err(Error::JobCancelled { job_id: None }),
        }
    }
}

/// Tracks an operation running as a task in this process. A second task
/// watches the first so that aborting it still produces an outcome, and so
/// that every caller of `wait` observes the same one.
struct SpawnedJob<T> {
    outcome: watch::Receiver<Option<Outcome<T>>>,
    abort: AbortHandle,
}

impl<T> SpawnedJob<T>
where
    T: Clone + Send + Sync + 'static,
{
    fn new(task: JoinHandle<Result<T>>) -> Self {
        let abort = task.abort_handle();
        let (tx, outcome) = watch::channel(None);
        tokio::spawn(async move {
            let outcome = match task.await {
                Ok(Ok(result)) => Outcome::Succeeded(result),
                Ok(Err(err)) => Outcome::Failed(Arc::new(err)),
                Err(err) if err.is_cancelled() => Outcome::Cancelled,
                Err(err) => Outcome::Failed(Arc::new(Error::Runtime {
                    message: format!("job task failed: {err}"),
                })),
            };
            let _ = tx.send(Some(outcome));
        });
        Self { outcome, abort }
    }

    fn is_done(&self) -> bool {
        self.outcome.borrow().is_some()
    }

    fn status(&self) -> String {
        let label = match &*self.outcome.borrow() {
            None => "running",
            Some(Outcome::Succeeded(_)) => "finished",
            Some(Outcome::Failed(_)) => "failed",
            Some(Outcome::Cancelled) => "cancelled",
        };
        label.to_string()
    }

    async fn wait(&self) -> Result<T> {
        let mut outcome = self.outcome.clone();
        let settled = outcome
            .wait_for(|outcome| outcome.is_some())
            .await
            .map_err(|_| Error::Runtime {
                message: "job outcome was dropped before it completed".to_string(),
            })?
            .clone()
            .expect("wait_for returns once an outcome is set");
        settled.into_result()
    }

    fn cancel(&self) {
        self.abort.abort();
    }
}
