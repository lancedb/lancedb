// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Future implementation for io_uring read operations.

use super::requests::IoRequest;
use bytes::Bytes;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

/// Future that awaits completion of an io_uring read operation.
///
/// This future is woken by the io_uring thread when the operation completes.
pub(super) struct UringReadFuture {
    pub(super) request: Arc<IoRequest>,
}

impl Future for UringReadFuture {
    type Output = object_store::Result<Bytes>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut state = self.request.state.lock().unwrap();

        if state.completed {
            // Operation completed - take the result
            match state.err.take() {
                Some(err) => Poll::Ready(Err(object_store::Error::Generic {
                    store: "io_uring",
                    source: Box::new(err),
                })),
                None => {
                    let br = state.bytes_read;
                    state.buffer.truncate(br);
                    let bytes = std::mem::take(&mut state.buffer).freeze();
                    Poll::Ready(Ok(bytes))
                }
            }
        } else {
            // Operation not yet complete - store waker and return Pending
            state.waker = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}
