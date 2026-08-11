// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Handles to operations a server may run asynchronously.

use std::sync::Arc;

use async_trait::async_trait;
use serde::de::Error as DeError;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use tokio::sync::watch;
use tokio::task::{AbortHandle, JoinHandle};

use crate::error::{Error, JobFailure, Result};
use crate::function::Function;

const JOB_RESULT_FORMAT_VERSION_V1: u32 = 1;

fn invalid_input(message: impl Into<String>) -> Error {
    Error::InvalidInput {
        message: message.into(),
    }
}

/// Result value produced by a completed Job (format version 1).
///
/// This is a non-resource transport value. It is not a Job handle, does not
/// observe lifecycle, and does not preserve unknown wire shapes.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum JobResult {
    /// The Job completed without a Function result.
    None,
    /// The Job completed with a [`Function`] value.
    Function(Function),
}

impl JobResult {
    /// Wire format version (always 1 for this type).
    pub fn format_version(&self) -> u32 {
        JOB_RESULT_FORMAT_VERSION_V1
    }

    /// Borrow the nested [`Function`] when this is [`JobResult::Function`].
    pub fn function(&self) -> Option<&Function> {
        match self {
            Self::None => None,
            Self::Function(function) => Some(function),
        }
    }

    /// Consume this value and return the nested [`Function`] when present.
    pub fn into_function(self) -> Option<Function> {
        match self {
            Self::None => None,
            Self::Function(function) => Some(function),
        }
    }

    fn to_wire(&self) -> JobResultWire {
        match self {
            Self::None => JobResultWire::None {
                format_version: JOB_RESULT_FORMAT_VERSION_V1,
            },
            Self::Function(function) => JobResultWire::Function {
                format_version: JOB_RESULT_FORMAT_VERSION_V1,
                function: function.clone(),
            },
        }
    }

    fn from_wire(wire: JobResultWire) -> Result<Self> {
        match wire {
            JobResultWire::None { format_version } => {
                if format_version != JOB_RESULT_FORMAT_VERSION_V1 {
                    return Err(invalid_input(format!(
                        "unsupported JobResult format_version {format_version}"
                    )));
                }
                Ok(Self::None)
            }
            JobResultWire::Function {
                format_version,
                function,
            } => {
                if format_version != JOB_RESULT_FORMAT_VERSION_V1 {
                    return Err(invalid_input(format!(
                        "unsupported JobResult format_version {format_version}"
                    )));
                }
                Ok(Self::Function(function))
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "kind", deny_unknown_fields)]
enum JobResultWire {
    #[serde(rename = "none")]
    None { format_version: u32 },
    #[serde(rename = "function")]
    Function {
        format_version: u32,
        function: Function,
    },
}

impl Serialize for JobResult {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.to_wire().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for JobResult {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = JobResultWire::deserialize(deserializer)?;
        Self::from_wire(wire).map_err(D::Error::custom)
    }
}

/// Backend-specific tracking for an asynchronous operation.
#[async_trait]
pub(crate) trait JobHandle: Send + Sync {
    /// Server-assigned id, when the backend has one.
    fn id(&self) -> Option<&str> {
        None
    }
    async fn status(&self) -> Result<String>;
    async fn wait(&self) -> Result<JobResult>;
    async fn cancel(&self) -> Result<()>;
}

/// A handle to an operation that may still be running.
///
/// The operation may already be complete when the handle is created.
pub struct Job {
    handle: Option<Box<dyn JobHandle>>,
}

impl std::fmt::Debug for Job {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Job")
            .field("id", &self.id())
            .field("done", &self.handle.is_none())
            .finish()
    }
}

impl Job {
    /// A job whose operation finished before the handle was created.
    pub(crate) fn new_done() -> Self {
        Self { handle: None }
    }

    pub(crate) fn new(handle: Box<dyn JobHandle>) -> Self {
        Self {
            handle: Some(handle),
        }
    }

    /// A job running as a task in this process.
    pub(crate) fn spawned(task: JoinHandle<Result<JobResult>>) -> Self {
        Self::new(Box::new(SpawnedJob::new(task)))
    }

    /// Identifies the operation on the server that is running it.
    ///
    /// Returned for correlating with server logs or the jobs API. Operations
    /// that run in this process have no server id and return `None`. The
    /// value is opaque: parsing it or storing it to resume the job later is
    /// not supported.
    pub fn id(&self) -> Option<&str> {
        self.handle.as_ref().and_then(|handle| handle.id())
    }

    /// The operation's current lifecycle state: "running", "finished",
    /// "failed", or "cancelled".
    ///
    /// A point snapshot; unlike [`Job::wait`] it does not block, raise on a
    /// terminal failure state, or retry. States a newer server reports that
    /// this client version does not know pass through as-is.
    pub async fn status(&self) -> Result<String> {
        match &self.handle {
            None => Ok("finished".to_string()),
            Some(handle) => handle.status().await,
        }
    }

    /// Waits until the operation reaches a terminal state.
    ///
    /// On success, returns the job's [`JobResult`]. Operations that produce no
    /// resource result yield [`JobResult::None`].
    ///
    /// Returns [`crate::Error::JobFailed`] if the operation failed and
    /// [`crate::Error::JobCancelled`] if it was cancelled.
    pub async fn wait(&self) -> Result<JobResult> {
        match &self.handle {
            None => Ok(JobResult::None),
            Some(handle) => handle.wait().await,
        }
    }

    /// Requests cancellation of the operation.
    ///
    /// Cancelling an operation that already finished is a no-op.
    pub async fn cancel(&self) -> Result<()> {
        match &self.handle {
            None => Ok(()),
            Some(handle) => handle.cancel().await,
        }
    }
}

/// How an in-process operation ended. Cloneable so every waiter can be given
/// the outcome; [`Error`] is not, so failures share one behind an [`Arc`].
#[derive(Clone)]
enum Outcome {
    Succeeded(JobResult),
    Failed(Arc<Error>),
    Cancelled,
}

impl Outcome {
    fn into_result(self) -> Result<JobResult> {
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
    fn new(task: JoinHandle<Result<JobResult>>) -> Self {
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

    async fn wait(&self) -> Result<JobResult> {
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
    use std::future::Future;
    use std::pin::pin;
    use std::task::{Context, Poll, Waker};

    use arrow_schema::DataType;
    use tokio::sync::oneshot;

    use super::*;
    use crate::error::FunctionErrorCode;
    use crate::function::{
        Function, FunctionId, FunctionOutput, FunctionParameter, FunctionSignature,
    };

    fn sample_success_function() -> Function {
        let id = FunctionId::try_new("fn.exact.local-job-result").expect("valid FunctionId");
        let signature = FunctionSignature::try_new(
            vec![FunctionParameter::new("x", DataType::Int32)],
            FunctionOutput::new(DataType::Int32, true),
        )
        .expect("valid FunctionSignature");
        Function::new(id, signature)
    }

    fn assert_exact_function(actual: &Function, expected: &Function) {
        assert_eq!(actual.id(), expected.id());
        assert_eq!(actual.signature(), expected.signature());
    }

    /// A completed-before-handle local job projects success as None.
    #[tokio::test]
    async fn local_job_result_new_done_wait_returns_none() {
        let job = Job::new_done();
        let result = job.wait().await.expect("new_done must succeed");
        assert_eq!(result, JobResult::None);
    }

    /// A local spawned unit / no-resource success projects as None.
    #[tokio::test]
    async fn local_job_result_spawned_unit_success_projects_none() {
        let job = Job::spawned(tokio::spawn(async { Ok(JobResult::None) }));
        let result = job
            .wait()
            .await
            .expect("unit success must finish without error");
        assert_eq!(result, JobResult::None);
    }

    /// Function success is cloneable and shared by concurrent + late waiters.
    ///
    /// Wait futures are pinned and polled once to Pending while success is still
    /// gated, proving they observed the running state before publication.
    #[tokio::test]
    async fn local_job_result_spawned_function_shared_by_waiters() {
        let expected = sample_success_function();
        let (release_tx, release_rx) = oneshot::channel();

        let job = Job::spawned(tokio::spawn({
            let function = expected.clone();
            async move {
                release_rx
                    .await
                    .expect("success task must be released by the test");
                Ok(JobResult::Function(function))
            }
        }));

        let mut wait_a = pin!(job.wait());
        let mut wait_b = pin!(job.wait());
        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);

        assert!(
            matches!(wait_a.as_mut().poll(&mut cx), Poll::Pending),
            "waiter A must poll Pending before success publication"
        );
        assert!(
            matches!(wait_b.as_mut().poll(&mut cx), Poll::Pending),
            "waiter B must poll Pending before success publication"
        );

        release_tx
            .send(())
            .expect("success task must still be waiting on the gate");

        let result_a = wait_a
            .await
            .expect("concurrent waiter A must observe success");
        let result_b = wait_b
            .await
            .expect("concurrent waiter B must observe success");
        let result_late = job
            .wait()
            .await
            .expect("late waiter must observe the same success");

        for result in [&result_a, &result_b, &result_late] {
            match result {
                JobResult::Function(function) => assert_exact_function(function, &expected),
                JobResult::None => panic!("Function success must not project as JobResult::None"),
            }
        }
        assert_eq!(result_a, result_b);
        assert_eq!(result_a, result_late);
    }

    #[tokio::test]
    async fn spawned_job_function_failure_returns_job_failed_with_same_code() {
        let job = Job::spawned(tokio::spawn(async {
            Err(Error::Function {
                code: FunctionErrorCode::UdfExecutionFailure,
                // Message names a different category on purpose; code is structural.
                message: "looks like name_conflict to a string parser".to_string(),
            })
        }));

        let err = job
            .wait()
            .await
            .expect_err("Function failure must fail the job");
        match err {
            Error::JobFailed { failure, .. } => match &failure.error_code {
                Some(code) => {
                    assert_eq!(code, &FunctionErrorCode::UdfExecutionFailure);
                    assert_ne!(code, &FunctionErrorCode::NameConflict);
                }
                None => panic!("local Function failure must project error_code onto JobFailure"),
            },
            other => panic!("expected Error::JobFailed, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn spawned_job_preserves_unrecognized_function_error_code() {
        let raw = "enterprise_future_category_xyz";
        let job = Job::spawned(tokio::spawn({
            let raw = raw.to_string();
            async move {
                Err(Error::Function {
                    code: FunctionErrorCode::Unrecognized(raw),
                    message: "future server category".to_string(),
                })
            }
        }));

        let err = job
            .wait()
            .await
            .expect_err("Function failure must fail the job");
        match err {
            Error::JobFailed { failure, .. } => match &failure.error_code {
                Some(FunctionErrorCode::Unrecognized(preserved)) => {
                    assert_eq!(preserved, raw);
                }
                Some(other) => panic!("unrecognized code must not become known: {other:?}"),
                None => panic!("unrecognized Function code must be preserved on JobFailure"),
            },
            other => panic!("expected Error::JobFailed, got {other:?}"),
        }
    }
}
