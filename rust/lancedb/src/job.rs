// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Handles to operations a server may run asynchronously.

use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

use arrow_array::RecordBatch;
use async_trait::async_trait;
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;
use tokio::sync::watch;
use tokio::task::{AbortHandle, JoinHandle};

use crate::database::JobDescription;
use crate::error::{Error, JobFailure, Result};

/// Which of a job's events [`Job::events`] returns.
///
/// This is [`crate::database::QueryJobEventsRequest`] without `job_id`, which
/// the handle already knows.
#[derive(Debug, Clone, Default)]
pub struct JobEventsRequest {
    /// Maximum event rows to return. The server applies its own default
    /// (1000 rows) and maximum (10,000 rows) when this is `None`, and
    /// truncates without saying so, which matters for a job with one event
    /// per fragment.
    pub limit: Option<u32>,
    /// SQL-like filter over the event columns `state`, `updated_by`,
    /// `emitted_from`, `emitted_by`, and `claim_entity`. For example
    /// `state = 'claim_complete'` selects only per-claim completions.
    pub filter: Option<String>,
}

impl JobEventsRequest {
    pub fn limit(mut self, limit: u32) -> Self {
        self.limit = Some(limit);
        self
    }

    pub fn filter(mut self, filter: impl Into<String>) -> Self {
        self.filter = Some(filter.into());
        self
    }
}

fn job_detail_not_supported<T>(what: &str) -> Result<T> {
    Err(Error::NotSupported {
        message: format!("{what} is only available for server-side jobs"),
    })
}

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
    /// The job's full server-side record. Backends that run the operation in
    /// this process have none and keep the default.
    async fn describe(&self) -> Result<JobDescription> {
        job_detail_not_supported("describing a job")
    }
    /// The job's recorded lifecycle events.
    async fn events(&self, _request: JobEventsRequest) -> Result<Vec<RecordBatch>> {
        job_detail_not_supported("job event history")
    }
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

    fn decode<T: DeserializeOwned>(self) -> Result<T> {
        let value = self.value.ok_or_else(|| match &self.request_id {
            Some(request_id) => Error::Http {
                source: "successful typed job response did not contain a result".into(),
                request_id: request_id.clone(),
                status_code: None,
            },
            None => Error::Runtime {
                message: "successful typed job did not contain a result".to_string(),
            },
        })?;
        serde_json::from_value(value).map_err(|error| match self.request_id {
            Some(request_id) => Error::Http {
                source: format!("failed to parse typed job result: {error}").into(),
                request_id,
                status_code: None,
            },
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

/// What a handle last learned about its job. `state` is separate because an
/// in-process job can report one but has no server-side record behind it.
#[derive(Default)]
struct JobCache {
    state: Option<String>,
    description: Option<JobDescription>,
}

/// A handle to an operation that may still be running.
///
/// The operation may already be complete when the handle is created. `T` is
/// the endpoint's successful terminal result; unit-result operations use the
/// default `Job<()>`.
///
/// The detail accessors ([`Job::state`], [`Job::job_type`], ...) read what the
/// handle last observed. Submitting an operation returns only a job id, so
/// populating them eagerly would cost an extra round trip on every call:
///
/// - [`Job::refresh`] and [`Job::status`] fetch the whole record.
/// - [`Job::wait`] records the terminal state it establishes, but not the rest
///   of the record; call [`Job::refresh`] for that.
/// - Everything is `None` until one of those runs.
pub struct Job<T = ()>
where
    T: Clone + Send + Sync + 'static,
{
    inner: JobInner<T>,
    cache: RwLock<JobCache>,
}

impl<T> std::fmt::Debug for Job<T>
where
    T: Clone + Send + Sync + 'static,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let cache = self.cache_read();
        let mut out = f.debug_struct("Job");
        out.field("id", &self.id())
            .field("done", &matches!(self.inner, JobInner::Completed(_)));
        if let Some(state) = &cache.state {
            out.field("state", state);
        }
        if let Some(description) = &cache.description {
            out.field("job_type", &description.job_type)
                .field("creation_ms", &description.creation_ms);
            if !description.spec.is_null() {
                out.field("spec", &description.spec);
            }
            if let Some(result) = &description.result {
                out.field("result", result);
            }
            if let Some(failure) = &description.failure {
                out.field("failure", failure);
            }
        }
        out.finish()
    }
}

impl Job<()> {
    /// A job whose operation finished before the handle was created.
    pub(crate) fn new_done() -> Self {
        Self {
            inner: JobInner::Completed(()),
            cache: RwLock::default(),
        }
    }

    pub(crate) fn new(handle: Box<dyn JobHandle>) -> Self {
        Self {
            inner: JobInner::Handle {
                handle,
                decode: Arc::new(|_| Ok(())),
            },
            cache: RwLock::default(),
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
            cache: RwLock::default(),
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

    fn cache_read(&self) -> RwLockReadGuard<'_, JobCache> {
        self.cache.read().unwrap_or_else(|err| err.into_inner())
    }

    fn cache_write(&self) -> RwLockWriteGuard<'_, JobCache> {
        self.cache.write().unwrap_or_else(|err| err.into_inner())
    }

    /// Asks the backend for this job's current state, and for a server-side
    /// job its full record, then caches the answer for the detail accessors.
    ///
    /// In-process operations have no server-side record, so only
    /// [`Job::state`] is populated for them.
    pub async fn refresh(&self) -> Result<()> {
        self.refresh_state().await.map(|_| ())
    }

    /// Refreshes and reports the state, which every backend can answer.
    async fn refresh_state(&self) -> Result<String> {
        let JobInner::Handle { handle, .. } = &self.inner else {
            let state = "finished".to_string();
            self.cache_write().state = Some(state.clone());
            return Ok(state);
        };
        match handle.describe().await {
            Ok(description) => {
                let state = description.state.clone();
                let mut cache = self.cache_write();
                cache.state = Some(state.clone());
                cache.description = Some(description);
                Ok(state)
            }
            // An in-process job knows its own state and nothing more.
            Err(Error::NotSupported { .. }) => {
                let state = handle.status().await?;
                self.cache_write().state = Some(state.clone());
                Ok(state)
            }
            Err(err) => Err(err),
        }
    }

    /// The operation's current lifecycle state: "running", "finished",
    /// "failed", or "cancelled".
    ///
    /// A point snapshot; unlike [`Job::wait`] it does not block, raise on a
    /// terminal failure state, or retry. States a newer server reports that
    /// this client version does not know pass through as-is. Also refreshes
    /// the detail accessors.
    pub async fn status(&self) -> Result<String> {
        self.refresh_state().await
    }

    /// The last lifecycle state this handle observed, without contacting the
    /// backend. `None` until the handle has.
    pub fn state(&self) -> Option<String> {
        self.cache_read().state.clone()
    }

    /// The whole server-side record this handle last observed. The accessors
    /// below read individual fields out of it. `None` for an in-process job,
    /// which has no such record.
    pub fn description(&self) -> Option<JobDescription> {
        self.cache_read().description.clone()
    }

    /// The job's type, as the server names it. `None` for an in-process job.
    pub fn job_type(&self) -> Option<String> {
        self.with_description(|description| description.job_type.clone())
    }

    /// When the job was created, in milliseconds since the epoch. `None` for
    /// an in-process job.
    pub fn creation_ms(&self) -> Option<i64> {
        self.with_description(|description| description.creation_ms)
    }

    /// The job-type-specific specification it was submitted with.
    pub fn spec(&self) -> Option<Value> {
        self.with_description(|description| description.spec.clone())
            .filter(|spec| !spec.is_null())
    }

    /// The job-type-specific terminal result, as reported data rather than the
    /// typed model [`Job::wait`] returns. `None` until the job succeeds.
    pub fn result(&self) -> Option<Value> {
        self.with_description(|description| description.result.clone())
            .flatten()
    }

    /// Why the job failed, when it failed and the server reports a reason.
    pub fn failure(&self) -> Option<JobFailure> {
        self.with_description(|description| description.failure.clone())
            .flatten()
    }

    fn with_description<R>(&self, read: impl FnOnce(&JobDescription) -> R) -> Option<R> {
        self.cache_read().description.as_ref().map(read)
    }

    /// This job's recorded lifecycle events.
    ///
    /// Unlike the detail accessors, which report a terminal result only once
    /// the job reaches one, events are written as the job runs and outlive the
    /// workers that produced them. A distributed job records a
    /// `claim`/`claim_complete` pair per unit of work, each carrying
    /// `rows_processed`, so a job that never finishes still accounts for what
    /// it did. In-process operations keep no event history.
    pub async fn events(&self, request: JobEventsRequest) -> Result<Vec<RecordBatch>> {
        match &self.inner {
            JobInner::Handle { handle, .. } => handle.events(request).await,
            // The operation finished before the handle existed, so there is no
            // id to query with even when a server ran it.
            JobInner::Completed(_) => Err(Error::NotSupported {
                message: "this operation completed before its handle was created, so it \
                          carries no job id to query events with"
                    .to_string(),
            }),
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
            JobInner::Handle { handle, decode } => {
                let settled = handle.wait().await;
                // Waiting already established a terminal state; record it so
                // the detail accessors do not need another round trip for it.
                if let Some(state) = terminal_state(&settled) {
                    self.cache_write().state = Some(state.to_string());
                }
                (decode)(settled?)
            }
            JobInner::Completed(result) => {
                self.cache_write().state = Some("finished".to_string());
                Ok(result.clone())
            }
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
        // The mapped handle tracks the same job, so it inherits what this one
        // has already learned about it.
        let Self { inner, cache } = self;
        match inner {
            JobInner::Handle { handle, decode } => Job {
                inner: JobInner::Handle {
                    handle,
                    decode: Arc::new(move |result| Ok(map((decode)(result)?))),
                },
                cache,
            },
            JobInner::Completed(result) => Job {
                inner: JobInner::Completed(map(result)),
                cache,
            },
        }
    }
}

/// The lifecycle state a settled [`JobHandle::wait`] implies.
fn terminal_state(settled: &Result<TerminalResult>) -> Option<&'static str> {
    match settled {
        Ok(_) => Some("finished"),
        Err(Error::JobFailed { .. }) => Some("failed"),
        Err(Error::JobCancelled { .. }) => Some("cancelled"),
        // Anything else is a transport failure, not a verdict on the job.
        Err(_) => None,
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
