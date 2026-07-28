// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Handles to operations a server may run asynchronously.

use async_trait::async_trait;

use crate::error::Result;

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
