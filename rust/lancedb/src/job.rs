// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Handles to operations a server may run asynchronously.

use async_trait::async_trait;
use tokio::sync::Mutex;
use tokio::task::{AbortHandle, JoinHandle};

use crate::error::{Error, Result};

/// Backend-specific tracking for an asynchronous operation.
#[async_trait]
pub(crate) trait JobHandle: Send + Sync {
    async fn wait(&self) -> Result<()>;
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
    pub(crate) fn spawned(task: JoinHandle<Result<()>>) -> Self {
        Self::new(Box::new(SpawnedJob {
            abort: task.abort_handle(),
            task: Mutex::new(Some(task)),
        }))
    }

    /// Waits until the operation reaches a terminal state.
    ///
    /// Returns [`crate::Error::JobFailed`] if the operation failed and
    /// [`crate::Error::JobCancelled`] if it was cancelled.
    pub async fn wait(&self) -> Result<()> {
        match &self.handle {
            None => Ok(()),
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

/// Tracks an operation running as a task in this process. `wait` consumes the
/// task, so only the first call reports its outcome.
struct SpawnedJob {
    task: Mutex<Option<JoinHandle<Result<()>>>>,
    abort: AbortHandle,
}

#[async_trait]
impl JobHandle for SpawnedJob {
    async fn wait(&self) -> Result<()> {
        let Some(task) = self.task.lock().await.take() else {
            return Ok(());
        };
        match task.await {
            Ok(result) => result,
            Err(err) if err.is_cancelled() => Err(Error::JobCancelled { job_id: None }),
            Err(err) => Err(Error::Runtime {
                message: format!("index job task failed: {err}"),
            }),
        }
    }

    async fn cancel(&self) -> Result<()> {
        self.abort.abort();
        Ok(())
    }
}
