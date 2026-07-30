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
