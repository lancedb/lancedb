// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Handles to operations a server may run asynchronously.

use std::sync::Arc;

use async_trait::async_trait;
use serde::{Serialize, de::DeserializeOwned};
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

/// A backend-neutral successful terminal result.
#[derive(Clone)]
pub(crate) struct TerminalResult {
    value: Option<Value>,
    request_id: Option<String>,
}

impl TerminalResult {
    fn local(value: Value) -> Self {
        Self {
            value: Some(value),
            request_id: None,
        }
    }

    pub(crate) fn remote(value: Option<Value>, request_id: String) -> Self {
        Self {
            value,
            request_id: Some(request_id),
        }
    }

    pub(crate) fn value(&self) -> Option<&Value> {
        self.value.as_ref()
    }

    #[cfg(feature = "remote")]
    fn remote_decode_error(request_id: String, message: String) -> Error {
        Error::Http {
            source: message.into(),
            request_id,
            status_code: None,
        }
    }

    #[cfg(not(feature = "remote"))]
    fn remote_decode_error(_request_id: String, message: String) -> Error {
        Error::Runtime { message }
    }

    fn decode<T: DeserializeOwned>(self) -> Result<T> {
        let value = self.value.ok_or_else(|| match &self.request_id {
            Some(request_id) => Self::remote_decode_error(
                request_id.clone(),
                "successful typed job response did not contain a result".to_string(),
            ),
            None => Error::Runtime {
                message: "successful typed job did not contain a result".to_string(),
            },
        })?;
        serde_json::from_value(value).map_err(|error| match self.request_id {
            Some(request_id) => Self::remote_decode_error(
                request_id,
                format!("failed to parse typed job result: {error}"),
            ),
            None => Error::Runtime {
                message: format!("failed to parse typed job result: {error}"),
            },
        })
    }
}

type ResultDecoder<T> = Arc<dyn Fn(TerminalResult) -> Result<T> + Send + Sync>;

enum JobInner<T> {
    Handle {
        handle: Box<dyn JobHandle>,
        decode: ResultDecoder<T>,
    },
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
        f.debug_struct("Job")
            .field("id", &self.id())
            .field("done", &matches!(self.inner, JobInner::Completed(_)))
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
                decode: Arc::new(|_| Ok(())),
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
                decode: Arc::new(TerminalResult::decode::<T>),
            },
        }
    }
}

impl<T> Job<T>
where
    T: Clone + Serialize + DeserializeOwned + Send + Sync + 'static,
{
    /// A typed job running as a task in this process.
    pub(crate) fn spawned(task: JoinHandle<Result<T>>) -> Self {
        Self::new_typed(Box::new(SpawnedJob::new(task)))
    }
}

impl<T> Job<T>
where
    T: Clone + Send + Sync + 'static,
{
    /// Identifies the operation on the server that is running it.
    ///
    /// Returned for correlating with server logs or the jobs API. Operations
    /// that run in this process have no server id and return `None`. The
    /// value is opaque: parsing it or storing it to resume the job later is
    /// not supported.
    pub fn id(&self) -> Option<&str> {
        match &self.inner {
            JobInner::Handle { handle, .. } => handle.id(),
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
            JobInner::Handle { handle, decode } => (decode)(handle.wait().await?),
            JobInner::Completed(result) => Ok(result.clone()),
        }
    }

    /// Requests cancellation of the operation.
    ///
    /// Cancelling an operation that already finished is a no-op.
    pub async fn cancel(&self) -> Result<()> {
        match &self.inner {
            JobInner::Handle { handle, .. } => handle.cancel().await,
            JobInner::Completed(_) => Ok(()),
        }
    }

    /// Maps a successful terminal result without changing the job lifecycle.
    /// The mapping may run once for each call to [`Job::wait`], so it should
    /// be deterministic and free of externally visible side effects.
    ///
    /// ```
    /// use lancedb::{Job, function::RefreshColumnResult};
    ///
    /// # async fn rows_assigned(
    /// #     job: Job<RefreshColumnResult>,
    /// # ) -> lancedb::Result<u64> {
    /// let job = job.map(|result| result.rows_assigned);
    /// job.wait().await
    /// # }
    /// ```
    pub fn map<U, F>(self, map: F) -> Job<U>
    where
        U: Clone + Send + Sync + 'static,
        F: Fn(T) -> U + Send + Sync + 'static,
    {
        match self.inner {
            JobInner::Handle { handle, decode } => Job {
                inner: JobInner::Handle {
                    handle,
                    decode: Arc::new(move |result| Ok(map((decode)(result)?))),
                },
            },
            JobInner::Completed(result) => Job {
                inner: JobInner::Completed(map(result)),
            },
        }
    }
}

/// How an in-process operation ended. Cloneable so every waiter can be given
/// the outcome; [`Error`] is not, so failures share one behind an [`Arc`].
#[derive(Clone)]
enum Outcome {
    Succeeded(TerminalResult),
    Failed(Arc<Error>),
    Cancelled,
}

impl Outcome {
    fn into_result(self) -> Result<TerminalResult> {
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
struct SpawnedJob {
    outcome: watch::Receiver<Option<Outcome>>,
    abort: AbortHandle,
}

impl SpawnedJob {
    fn new<T>(task: JoinHandle<Result<T>>) -> Self
    where
        T: Serialize + Send + 'static,
    {
        let abort = task.abort_handle();
        let (tx, outcome) = watch::channel(None);
        tokio::spawn(async move {
            let outcome = match task.await {
                Ok(Ok(result)) => match serde_json::to_value(result) {
                    Ok(value) => Outcome::Succeeded(TerminalResult::local(value)),
                    Err(err) => Outcome::Failed(Arc::new(Error::Runtime {
                        message: format!("failed to serialize job result: {err}"),
                    })),
                },
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
}

#[async_trait]
impl JobHandle for SpawnedJob {
    async fn status(&self) -> Result<String> {
        let label = match &*self.outcome.borrow() {
            None => "running",
            Some(Outcome::Succeeded(_)) => "finished",
            Some(Outcome::Failed(_)) => "failed",
            Some(Outcome::Cancelled) => "cancelled",
        };
        Ok(label.to_string())
    }

    async fn wait(&self) -> Result<TerminalResult> {
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

    async fn cancel(&self) -> Result<()> {
        self.abort.abort();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::future::pending;

    use super::*;

    #[tokio::test]
    async fn mapped_spawned_job_reuses_outcome() {
        let job = Job::spawned(tokio::spawn(async { Ok(41_u64) })).map(|value| value + 1);

        assert_eq!(job.wait().await.unwrap(), 42);
        assert_eq!(job.wait().await.unwrap(), 42);
        assert_eq!(job.status().await.unwrap(), "finished");
    }

    #[tokio::test]
    async fn mapped_spawned_job_preserves_cancellation() {
        let job = Job::spawned(tokio::spawn(async { pending::<Result<u64>>().await }))
            .map(|value| value.to_string());

        job.cancel().await.unwrap();
        assert!(matches!(job.wait().await, Err(Error::JobCancelled { .. })));
        assert_eq!(job.status().await.unwrap(), "cancelled");
    }
}
