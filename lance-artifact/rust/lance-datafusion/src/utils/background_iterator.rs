// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use futures::Stream;
use futures::ready;
use std::{
    future::Future,
    panic,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::task::JoinHandle;

/// Wrap an iterator as a stream that executes the iterator in a background
/// blocking thread.
///
/// The size hint is preserved, but the stream is not fused.
#[pin_project::pin_project]
pub struct BackgroundIterator<I: Iterator + Send + 'static> {
    #[pin]
    state: BackgroundIterState<I>,
}

impl<I: Iterator + Send + 'static> BackgroundIterator<I> {
    pub fn new(iter: I) -> Self {
        Self {
            state: BackgroundIterState::Current { iter },
        }
    }
}

impl<I: Iterator + Send + 'static> Stream for BackgroundIterator<I>
where
    I::Item: Send + 'static,
{
    type Item = I::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        if let Some(mut iter) = this.state.as_mut().take_iter() {
            this.state.set(BackgroundIterState::Running {
                size_hint: iter.size_hint(),
                task: tokio::task::spawn_blocking(move || {
                    let next = iter.next();
                    next.map(|next| (iter, next))
                }),
            });
        }

        let step = match this.state.as_mut().project_future() {
            Some(task) => ready!(task.poll(cx)),
            None => panic!(
                "BackgroundIterator must not be polled after it returned `Poll::Ready(None)`"
            ),
        };

        match step {
            Ok(Some((iter, next))) => {
                this.state.set(BackgroundIterState::Current { iter });
                Poll::Ready(Some(next))
            }
            Ok(None) => {
                this.state.set(BackgroundIterState::Empty);
                Poll::Ready(None)
            }
            Err(err) => {
                if err.is_panic() {
                    // Resume the panic on the main task
                    panic::resume_unwind(err.into_panic());
                } else {
                    panic!("Background task failed: {:?}", err);
                }
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match &self.state {
            BackgroundIterState::Current { iter } => iter.size_hint(),
            BackgroundIterState::Running { size_hint, .. } => *size_hint,
            BackgroundIterState::Empty => (0, Some(0)),
        }
    }
}

// Inspired by Unfold implementation: https://github.com/rust-lang/futures-rs/blob/master/futures-util/src/unfold_state.rs#L22
#[pin_project::pin_project(project = StateProj, project_replace = StateReplace)]
enum BackgroundIterState<I: Iterator> {
    Current {
        iter: I,
    },
    Running {
        size_hint: (usize, Option<usize>),
        #[pin]
        task: NextHandle<I, I::Item>,
    },
    Empty,
}

type NextHandle<I, Item> = JoinHandle<Option<(I, Item)>>;

impl<I: Iterator + Send + 'static> BackgroundIterState<I> {
    fn project_future(self: Pin<&mut Self>) -> Option<Pin<&mut NextHandle<I, I::Item>>> {
        match self.project() {
            StateProj::Running { task, .. } => Some(task),
            _ => None,
        }
    }

    fn take_iter(self: Pin<&mut Self>) -> Option<I> {
        match &*self {
            Self::Current { .. } => match self.project_replace(Self::Empty) {
                StateReplace::Current { iter } => Some(iter),
                _ => None,
            },
            _ => None,
        }
    }
}
