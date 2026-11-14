// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Handles to operations a server may run asynchronously.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::Duration;

use arrow_array::RecordBatch;
use async_trait::async_trait;
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;
use tokio::sync::watch;
use tokio::task::{AbortHandle, JoinHandle};
use uuid::Uuid;

use crate::database::JobDescription;
use crate::docs::DocLink;
use crate::error::{Error, JobFailure, Result};

mod state;
pub use state::JobState;

/// Observer called with each description fetched by a job's existing wait
/// poller. Keeping this callback on the poller avoids a second lifecycle or
/// retry loop just to render progress.
pub type JobProgressCallback = Arc<Mutex<dyn FnMut(&JobDescription) + Send>>;

/// Which of a job's events [`Job::events`] returns.
///
/// The handle already knows which job to ask about, so this narrows the
/// query rather than naming one.
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
    /// Opaque continuation from the preceding page's Arrow schema metadata.
    pub page_token: Option<String>,
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

    /// Continue a previous page without deriving a cursor from its row count.
    pub fn page_token(mut self, token: impl Into<String>) -> Self {
        self.page_token = Some(token.into());
        self
    }
}

/// The continuation returned with one event page. Absence means the page is complete.
pub fn job_event_page_token(batches: &[RecordBatch]) -> Option<&str> {
    batches
        .first()?
        .schema_ref()
        .metadata()
        .get("lancedb.job_coordinator.page_token")
        .map(String::as_str)
}

fn job_detail_not_supported<T>(what: &str) -> Result<T> {
    Err(Error::NotSupported {
        message: DocLink::Jobs.append_to(format!("{what} is only available for server-side jobs")),
    })
}

/// Backend-specific tracking for an asynchronous operation.
#[async_trait]
pub(crate) trait JobHandle: Send + Sync {
    /// Backend-specific id, when the operation has one.
    fn id(&self) -> Option<&str> {
        None
    }
    fn job_type(&self) -> Option<&str> {
        None
    }
    async fn status(&self) -> Result<String>;
    async fn wait(&self) -> Result<TerminalResult>;
    /// Wait while observing descriptions from the same backend poller.
    /// Backends without a server-side record keep the default behavior.
    async fn wait_with_progress(
        &self,
        _progress: Option<JobProgressCallback>,
    ) -> Result<TerminalResult> {
        self.wait().await
    }
    async fn cancel(&self) -> Result<()>;
    /// The job's full server-side record. Backends that run the operation in
    /// this process have none and keep the default.
    async fn describe(&self) -> Result<JobDescription> {
        job_detail_not_supported("describing a job")
    }
    /// Chrome Trace Event JSON for a server-side Function job.
    async fn timeline(&self) -> Result<Value> {
        job_detail_not_supported("job timeline export")
    }

    /// A printable report of recorded Function job metrics.
    async fn stats(&self) -> Result<Option<String>> {
        job_detail_not_supported("job statistics")
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

    #[cfg(feature = "remote")]
    pub(crate) fn remote(value: Option<Value>, request_id: String) -> Self {
        Self {
            value,
            request_id: Some(request_id),
        }
    }

    pub(crate) fn value(&self) -> Option<&Value> {
        self.value.as_ref()
    }

    pub(crate) fn with_value(mut self, value: Value) -> Self {
        self.value = Some(value);
        self
    }

    fn decode<T: DeserializeOwned>(self) -> Result<T> {
        let value = self.value.ok_or_else(|| match &self.request_id {
            Some(request_id) => remote_job_error(
                request_id.clone(),
                "successful typed job response did not contain a result".to_string(),
            ),
            None => Error::Runtime {
                message: "successful typed job did not contain a result".to_string(),
            },
        })?;
        serde_json::from_value(value).map_err(|error| match self.request_id {
            Some(request_id) => remote_job_error(
                request_id,
                format!("failed to parse typed job result: {error}"),
            ),
            None => Error::Runtime {
                message: format!("failed to parse typed job result: {error}"),
            },
        })
    }
}

/// Build the error for a failed remote typed-job result.
///
/// The `Error::Http` variant is gated behind the `remote` feature, so without
/// it we fall back to `Error::Runtime`. A `request_id` is only ever `Some`
/// for remote jobs, making the fallback unreachable in practice without the
/// feature.
#[cfg(feature = "remote")]
fn remote_job_error(request_id: String, source: String) -> Error {
    Error::Http {
        source: source.into(),
        request_id,
        status_code: None,
    }
}

#[cfg(not(feature = "remote"))]
fn remote_job_error(request_id: String, source: String) -> Error {
    Error::Runtime {
        message: format!("{source} (request_id={request_id})"),
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
    lifecycle_state: Option<JobState>,
    description: Option<JobDescription>,
    local_operation_id: Option<String>,
}

/// A handle to an operation that may still be running.
///
/// The operation may already be complete when the handle is created. `T` is
/// the endpoint's successful terminal result; unit-result operations use the
/// default `Job<()>`.
///
/// Registry detail accessors ([`Job::state`], [`Job::job_type`], ...) read what the
/// handle last observed. Submitting an operation returns only a job id, so
/// populating them eagerly would cost an extra round trip on every call:
///
/// - [`Job::refresh`] and [`Job::status`] fetch the whole record.
/// - [`Job::wait`] records the terminal state it establishes, but not the rest
///   of the record; call [`Job::refresh`] for that.
/// - Registry details are `None` until one of those runs. Query-backed handles
///   report [`Job::job_type`] as `"query"` immediately.
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
    /// A job whose operation finished before the handle was created. Its
    /// state is known without asking anyone, so the cache starts populated.
    pub(crate) fn new_done() -> Self {
        Self {
            inner: JobInner::Completed(()),
            cache: RwLock::new(JobCache {
                state: Some("finished".to_string()),
                lifecycle_state: Some(JobState::Succeeded),
                description: None,
                local_operation_id: Some(Uuid::new_v4().to_string()),
            }),
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

    /// A handle whose record the caller has already fetched, so the detail
    /// accessors answer without a second round trip.
    #[cfg(feature = "remote")]
    pub(crate) fn opened(handle: Box<dyn JobHandle>, description: JobDescription) -> Self {
        let job = Self::new(handle);
        {
            let mut cache = job.cache_write();
            cache.state = Some(description.state.clone());
            cache.lifecycle_state = Some(
                description
                    .lifecycle_state
                    .clone()
                    .unwrap_or_else(|| JobState::from(description.state.as_str())),
            );
            cache.description = Some(description);
        }
        job
    }

    /// Decode this job's terminal payload as `T` when waiting.
    ///
    /// Use this after [`crate::Connection::open_job`] when the job's result
    /// type is known. The job identity and cached description are preserved.
    /// Missing or incompatible payloads are reported by [`Job::wait`].
    /// A job that completed before its handle was created has only a unit
    /// result; converting it fails immediately if `T` cannot decode JSON null.
    ///
    /// ```no_run
    /// # async fn reopen(
    /// #     db: &lancedb::Connection, id: &str,
    /// # ) -> lancedb::Result<lancedb::function::RefreshColumnResult> {
    /// let job = db.open_job(id).await?
    ///     .into_typed::<lancedb::function::RefreshColumnResult>()?;
    /// job.wait().await
    /// # }
    /// ```
    pub fn into_typed<T>(self) -> Result<Job<T>>
    where
        T: Clone + DeserializeOwned + Send + Sync + 'static,
    {
        let inner = match self.inner {
            JobInner::Handle { handle, .. } => JobInner::Handle {
                handle,
                decode: Arc::new(TerminalResult::decode::<T>),
            },
            JobInner::Completed(()) => {
                JobInner::Completed(TerminalResult::local(Value::Null).decode::<T>()?)
            }
        };
        Ok(Job {
            inner,
            cache: self.cache,
        })
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
        let job = Self::new_typed(Box::new(SpawnedJob::new(task)));
        job.cache_write().local_operation_id = Some(Uuid::new_v4().to_string());
        job
    }

    pub(crate) fn from_query_task<F, Fut>(
        query_id: Uuid,
        task: JoinHandle<Result<T>>,
        cancel: F,
    ) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        let cancellation: Cancellation = Arc::new(move || Box::pin(cancel()));
        let job = Self::new_typed(Box::new(SpawnedJob::new_with_cancellation(
            query_id,
            task,
            cancellation,
        )));
        job.cache_write().local_operation_id = Some(query_id.to_string());
        job
    }
}

impl<T> Job<T>
where
    T: Clone + Send + Sync + 'static,
{
    /// The operation's opaque identity, interpreted according to [`Job::job_type`].
    /// Registry handles expose a registry job ID; query handles expose the
    /// connection-scoped SQL query UUID as a string. Local handles return None.
    /// A query ID cannot be used with registry APIs or to resume after restart.
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
                cache.lifecycle_state = Some(
                    description
                        .lifecycle_state
                        .clone()
                        .unwrap_or_else(|| JobState::from(state.as_str())),
                );
                cache.description = Some(description);
                Ok(state)
            }
            // An in-process job knows its own state and nothing more.
            Err(Error::NotSupported { .. }) => {
                let state = handle.status().await?;
                let mut cache = self.cache_write();
                cache.state = Some(state.clone());
                cache.lifecycle_state = Some(JobState::from(state.as_str()));
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

    /// The current structured progress snapshot, or `None` for an in-process
    /// operation. This refreshes the server-side record once, matching
    /// [`Job::status`]; older servers return lifecycle and queue fields without
    /// job-type-specific counters.
    pub async fn progress(&self) -> Result<Option<Value>> {
        self.refresh_state().await?;
        Ok(self.with_description(JobDescription::progress_snapshot))
    }

    /// The last lifecycle state this handle observed, without contacting the
    /// backend. `None` until the handle has.
    pub fn state(&self) -> Option<String> {
        self.cache_read().state.clone()
    }

    /// Detailed state last observed, preserving unknown future server values.
    /// Local operations use the same typed states; legacy `state()` is unchanged.
    pub fn lifecycle_state(&self) -> Option<JobState> {
        let cache = self.cache_read();
        cache
            .lifecycle_state
            .clone()
            .or_else(|| cache.state.as_deref().map(JobState::from))
    }

    /// Refresh the state and return its forward-compatible typed form.
    pub async fn status_typed(&self) -> Result<JobState> {
        self.refresh_state().await?;
        Ok(self.lifecycle_state().expect("refresh records a state"))
    }

    /// Identity of this attempt. Local identities cannot be passed to
    /// `Connection::open_job` and last for the lifetime of this handle.
    pub fn job_id(&self) -> Option<String> {
        self.id()
            .map(str::to_string)
            .or_else(|| self.cache_read().local_operation_id.clone())
    }

    /// Root of this attempt's explicit retry lineage. Older servers may omit it.
    pub fn operation_id(&self) -> Option<String> {
        let cache = self.cache_read();
        cache
            .description
            .as_ref()
            .and_then(|description| description.operation_id.clone())
            .or_else(|| cache.local_operation_id.clone())
    }

    /// One-based retry attempt. Worker replacement does not increment it.
    pub fn attempt(&self) -> Option<u32> {
        let cache = self.cache_read();
        cache
            .description
            .as_ref()
            .and_then(|description| description.attempt)
            .or_else(|| cache.local_operation_id.as_ref().map(|_| 1))
    }

    /// Immediate predecessor attempt, when supplied by the backend.
    pub fn retry_of(&self) -> Option<String> {
        self.with_description(|description| description.retry_of.clone())
            .flatten()
    }

    /// Source snapshot pinned for this attempt, when supplied by the backend.
    pub fn source_table_version(&self) -> Option<u64> {
        self.with_description(|description| description.source_table_version)
            .flatten()
    }

    /// Most recent execution's fencing token, when supplied by the backend.
    pub fn execution_epoch(&self) -> Option<i64> {
        self.with_description(|description| description.execution_epoch)
            .flatten()
    }

    /// The whole server-side record this handle last observed. The accessors
    /// below read individual fields out of it. `None` for an in-process job,
    /// which has no such record.
    pub fn description(&self) -> Option<JobDescription> {
        self.cache_read().description.clone()
    }

    /// The job's type: `"query"` for SQL-backed handles, otherwise the cached
    /// server-reported kind of work. `None` for an in-process job.
    pub fn job_type(&self) -> Option<String> {
        if let JobInner::Handle { handle, .. } = &self.inner
            && let Some(job_type) = handle.job_type()
        {
            return Some(job_type.to_string());
        }
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

    /// Collect retained event pages, following the backend's opaque cursors.
    /// `limit` is the size of each page; it is not a total history limit.
    pub async fn history(&self, mut request: JobEventsRequest) -> Result<Vec<RecordBatch>> {
        let mut batches = Vec::new();
        loop {
            let page = self.events(request.clone()).await?;
            let next = job_event_page_token(&page).map(str::to_string);
            let done = next.is_none();
            if next.is_some() && next == request.page_token {
                return Err(Error::Runtime {
                    message: "job history repeated its page token".to_string(),
                });
            }
            batches.extend(page);
            if done {
                return Ok(batches);
            }
            request.page_token = next;
        }
    }

    /// Fetch a recorded Function job timeline as Chrome Trace Event JSON.
    ///
    /// The snapshot opens in Perfetto or Chrome tracing. It contains retained
    /// split and positioned phase spans; batch statistics remain arguments,
    /// because individual batch timestamps are not recorded. Running jobs return
    /// a partial recording without waiting for completion. Requires server support;
    /// in-process operations and SQL query handles do not support this operation.
    ///
    /// ```no_run
    /// # async fn export(job: &lancedb::job::Job) -> Result<(), Box<dyn std::error::Error>> {
    /// let trace = job.timeline().await?;
    /// std::fs::write("job.trace.json", trace.to_string())?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn timeline(&self) -> Result<Value> {
        match &self.inner {
            JobInner::Handle { handle, .. } => handle.timeline().await,
            JobInner::Completed(_) => job_detail_not_supported("job timeline export"),
        }
    }

    /// Fetch a printable run report for a server-side Function job.
    ///
    /// The server report includes recorded phase timings, finishing-supervisor
    /// slot occupancy, all-attempt split execution and batch latency, and
    /// accepted-row throughput. Returns `None` for running jobs or when no recorded
    /// profile is available, without waiting for completion. Requires server
    /// support; local operations and SQL query handles do not support reports.
    ///
    /// ```no_run
    /// # async fn report(job: &lancedb::job::Job) -> Result<(), Box<dyn std::error::Error>> {
    /// if let Some(report) = job.stats().await? {
    ///     println!("{report}");
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn stats(&self) -> Result<Option<String>> {
        match &self.inner {
            JobInner::Handle { handle, .. } => handle.stats().await,
            JobInner::Completed(_) => job_detail_not_supported("job statistics"),
        }
    }

    /// Waits until the operation reaches a terminal state.
    ///
    /// Returns the endpoint's typed result. Unit-result jobs return `()`.
    ///
    /// Returns [`crate::Error::JobFailed`] if the operation failed and
    /// [`crate::Error::JobCancelled`] if it was cancelled.
    pub async fn wait(&self) -> Result<T> {
        self.wait_with_progress_opt(None).await
    }

    /// Wait for at most `timeout` without cancelling the operation. The same
    /// handle can be waited on again after [`Error::Timeout`].
    pub async fn wait_timeout(&self, timeout: Duration) -> Result<T> {
        tokio::time::timeout(timeout, self.wait()).await.map_err(|_| Error::Timeout {
            message: format!("job {:?} is still running after {} seconds; waiting timed out without cancelling it", self.job_id(), timeout.as_secs_f64()),
        })?
    }

    /// Wait with a progress observer and a deadline. Timing out leaves the
    /// operation running and the handle reusable, just like [`Self::wait_timeout`].
    pub async fn wait_with_progress_timeout<F>(&self, timeout: Duration, progress: F) -> Result<T>
    where
        F: FnMut(&JobDescription) + Send + 'static,
    {
        tokio::time::timeout(timeout, self.wait_with_progress(progress)).await.map_err(|_| Error::Timeout {
            message: format!("job {:?} is still running after {} seconds; waiting timed out without cancelling it", self.job_id(), timeout.as_secs_f64()),
        })?
    }

    /// Wait for the operation while observing progress from the backend's
    /// existing wait poller. The callback is called once for each description
    /// the poller fetches, including the terminal observation when the server
    /// supplies one. It does not create another lifecycle or retry loop.
    pub async fn wait_with_progress<F>(&self, progress: F) -> Result<T>
    where
        F: FnMut(&JobDescription) + Send + 'static,
    {
        self.wait_with_progress_opt(Some(Arc::new(Mutex::new(progress))))
            .await
    }

    async fn wait_with_progress_opt(&self, progress: Option<JobProgressCallback>) -> Result<T> {
        match &self.inner {
            JobInner::Handle { handle, decode } => {
                let settled = handle.wait_with_progress(progress).await;
                // Waiting already established a terminal state; record it so
                // the detail accessors do not need another round trip for it.
                if let Some(state) = terminal_state(&settled) {
                    let mut cache = self.cache_write();
                    cache.state = Some(state.to_string());
                    cache.lifecycle_state = Some(JobState::from(state));
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
            Self::Failed(source) => {
                let (job_id, failure) = match source.as_ref() {
                    Error::JobFailed { job_id, failure } => (job_id.clone(), failure.clone()),
                    _ => (None, Box::new(JobFailure::from_source(source))),
                };
                Err(Error::JobFailed { job_id, failure })
            }
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
    cancellation: Option<Cancellation>,
    id: Option<String>,
}

type CancellationFuture = Pin<Box<dyn Future<Output = Result<()>> + Send>>;
type Cancellation = Arc<dyn Fn() -> CancellationFuture + Send + Sync>;

impl SpawnedJob {
    fn new<T>(task: JoinHandle<Result<T>>) -> Self
    where
        T: Serialize + Send + 'static,
    {
        Self::new_inner(task, None)
    }

    fn new_with_cancellation<T>(
        query_id: Uuid,
        task: JoinHandle<Result<T>>,
        cancellation: Cancellation,
    ) -> Self
    where
        T: Serialize + Send + 'static,
    {
        let mut handle = Self::new_inner(task, Some(cancellation));
        handle.id = Some(query_id.to_string());
        handle
    }

    fn new_inner<T>(task: JoinHandle<Result<T>>, cancellation: Option<Cancellation>) -> Self
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
        Self {
            outcome,
            abort,
            cancellation,
            id: None,
        }
    }
}

#[async_trait]
impl JobHandle for SpawnedJob {
    fn id(&self) -> Option<&str> {
        self.id.as_deref()
    }

    fn job_type(&self) -> Option<&str> {
        self.id.as_ref().map(|_| "query")
    }

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
        if let Some(cancellation) = &self.cancellation {
            let mut outcome = self.outcome.clone();
            tokio::select! {
                biased;
                settled = outcome.wait_for(Option::is_some) => {
                    settled.map_err(|_| Error::Runtime {
                        message: "job outcome was dropped before cancellation completed".to_string(),
                    })?;
                    return Ok(());
                }
                result = cancellation() => result?,
            }
        }
        self.abort.abort();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::future::pending;

    use super::*;

    #[test]
    fn local_typed_result_decode_errors_are_runtime_errors() {
        assert!(matches!(
            TerminalResult::local(Value::Null).decode::<u64>(),
            Err(Error::Runtime { .. })
        ));
    }

    #[tokio::test]
    async fn completed_unit_job_can_be_retyped_as_unit() {
        let job = Job::new_done().into_typed::<()>().unwrap();
        job.wait().await.unwrap();
        assert_eq!(job.state().as_deref(), Some("finished"));
    }

    #[test]
    fn completed_unit_job_rejects_an_incompatible_result_type() {
        assert!(matches!(
            Job::new_done().into_typed::<u64>(),
            Err(Error::Runtime { .. })
        ));
    }

    #[tokio::test]
    async fn mapped_spawned_job_reuses_outcome() {
        let job = Job::spawned(tokio::spawn(async { Ok(41_u64) })).map(|value| value + 1);

        assert_eq!(job.job_type(), None);
        assert_eq!(job.id(), None);
        assert_eq!(job.wait().await.unwrap(), 42);
        assert_eq!(job.wait().await.unwrap(), 42);
        assert_eq!(job.status().await.unwrap(), "finished");
    }

    #[tokio::test]
    async fn mapped_query_job_preserves_identity_and_type() {
        let query_id = Uuid::new_v4();
        let job = Job::from_query_task(query_id, tokio::spawn(async { Ok(41_u64) }), || async {
            Ok(())
        })
        .map(|value| value + 1);
        assert_eq!(job.job_type().as_deref(), Some("query"));
        assert_eq!(job.id(), Some(query_id.to_string().as_str()));
        assert_eq!(job.wait().await.unwrap(), 42);
        assert_eq!(job.wait().await.unwrap(), 42);
        assert_eq!(job.job_type().as_deref(), Some("query"));
        assert_eq!(job.id(), Some(query_id.to_string().as_str()));
        let completed = Job::new_done();
        assert_eq!(completed.job_type(), None);
        assert_eq!(completed.id(), None);
    }

    #[tokio::test]
    async fn mapped_spawned_job_preserves_cancellation() {
        let job = Job::spawned(tokio::spawn(async { pending::<Result<u64>>().await }))
            .map(|value| value.to_string());

        job.cancel().await.unwrap();
        assert!(matches!(job.wait().await, Err(Error::JobCancelled { .. })));
        assert_eq!(job.status().await.unwrap(), "cancelled");
    }

    #[test]
    fn decode_missing_result_uses_feature_appropriate_error() {
        // A `request_id` is only `Some` for remote jobs. The error for a
        // missing result must use `Error::Http` with the `remote` feature and
        // fall back to `Error::Runtime` without it (where `Error::Http`
        // does not exist). See https://github.com/lancedb/lancedb/issues/4142.
        let result = TerminalResult {
            value: None,
            request_id: Some("req-1".to_string()),
        };
        let err = result.decode::<Value>().unwrap_err();
        #[cfg(feature = "remote")]
        assert!(
            matches!(err, Error::Http { .. }),
            "expected Error::Http, got {err:?}"
        );
        #[cfg(not(feature = "remote"))]
        assert!(
            matches!(err, Error::Runtime { .. }),
            "expected Error::Runtime, got {err:?}"
        );
    }

    #[test]
    fn decode_unparsable_result_uses_feature_appropriate_error() {
        let result = TerminalResult {
            value: Some(Value::String("not a u64".to_string())),
            request_id: Some("req-1".to_string()),
        };
        let err = result.decode::<u64>().unwrap_err();
        #[cfg(feature = "remote")]
        assert!(
            matches!(err, Error::Http { .. }),
            "expected Error::Http, got {err:?}"
        );
        #[cfg(not(feature = "remote"))]
        assert!(
            matches!(err, Error::Runtime { .. }),
            "expected Error::Runtime, got {err:?}"
        );
    }
}
