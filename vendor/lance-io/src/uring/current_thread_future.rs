// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Future implementation for thread-local io_uring operations.
//!
//! This future actively processes completions during polling instead of
//! relying on background tasks.

use super::current_thread::{process_thread_local_completions, submit_and_wait_thread_local};
use super::requests::IoRequest;
use bytes::Bytes;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

/// Future that awaits completion of a thread-local io_uring read operation
pub struct UringCurrentThreadFuture {
    request: Arc<IoRequest>,
}

impl UringCurrentThreadFuture {
    pub(super) fn new(request: Arc<IoRequest>) -> Self {
        Self { request }
    }
}

impl Future for UringCurrentThreadFuture {
    type Output = object_store::Result<Bytes>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // Check thread safety
        if self.request.thread_id != std::thread::current().id() {
            panic!("Request thread ID does not match current thread ID");
        }

        // First, check if we've been completed by some other future polling for completions.
        let mut state = self.request.state.lock().unwrap();

        if state.completed {
            // Take result and return Ready
            match state.err.take() {
                Some(err) => {
                    return Poll::Ready(Err(object_store::Error::Generic {
                        store: "io_uring_ct",
                        source: Box::new(err),
                    }));
                }
                None => {
                    let br = state.bytes_read;
                    state.buffer.truncate(br);
                    let bytes = std::mem::take(&mut state.buffer).freeze();
                    return Poll::Ready(Ok(bytes));
                }
            }
        }

        drop(state);

        // If not, then we should do any available work and then process completions.
        if let Err(e) = submit_and_wait_thread_local() {
            log::debug!("Submit and wait error: {:?}", e);
        }

        if let Err(e) = process_thread_local_completions() {
            log::warn!("Error processing completions: {:?}", e);
        }

        // Check if our request completed
        let mut state = self.request.state.lock().unwrap();

        if state.completed {
            // Take result and return Ready
            match state.err.take() {
                Some(err) => {
                    return Poll::Ready(Err(object_store::Error::Generic {
                        store: "io_uring_ct",
                        source: Box::new(err),
                    }));
                }
                None => {
                    let br = state.bytes_read;
                    state.buffer.truncate(br);
                    let bytes = std::mem::take(&mut state.buffer).freeze();
                    return Poll::Ready(Ok(bytes));
                }
            }
        }

        // Not done yet - immediately wake and return Pending (don't store waker)
        // which will force the future to be polled again.  This is intentionally
        // a busy loop.  io_uring is intended for fast disks where read latency is
        // so small that the cost of a true context switch (parking and unparking)
        // would be too high.
        //
        // We are effectively doing a "yield" here while we wait for
        // the io_uring thread to complete the request.
        drop(state);
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}
