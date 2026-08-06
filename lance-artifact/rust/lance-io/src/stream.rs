// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::pin::Pin;
use std::task::{Context, Poll};

use arrow_array::RecordBatch;
use arrow_schema::{ArrowError, SchemaRef};
use futures::stream::BoxStream;
use futures::{Stream, StreamExt};
use pin_project::pin_project;

use lance_core::Result;

pub type BatchStream = BoxStream<'static, Result<RecordBatch>>;

pub fn arrow_stream_to_lance_stream(
    arrow_stream: BoxStream<'static, std::result::Result<RecordBatch, ArrowError>>,
) -> BatchStream {
    arrow_stream.map(|r| r.map_err(Into::into)).boxed()
}

/// RecordBatch Stream trait.
pub trait RecordBatchStream: Stream<Item = Result<RecordBatch>> + Send {
    /// Returns the schema of the stream.
    fn schema(&self) -> SchemaRef;
}

/// Combines a [`Stream`] with a [`SchemaRef`] implementing
/// [`RecordBatchStream`] for the combination
#[pin_project]
pub struct RecordBatchStreamAdapter<S> {
    schema: SchemaRef,

    #[pin]
    stream: S,
}

impl<S> RecordBatchStreamAdapter<S> {
    /// Creates a new [`RecordBatchStreamAdapter`] from the provided schema and stream
    pub fn new(schema: SchemaRef, stream: S) -> Self {
        Self { schema, stream }
    }
}

impl<S> std::fmt::Debug for RecordBatchStreamAdapter<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecordBatchStreamAdapter")
            .field("schema", &self.schema)
            .finish()
    }
}

impl<S> RecordBatchStream for RecordBatchStreamAdapter<S>
where
    S: Stream<Item = Result<RecordBatch>> + Send + 'static,
{
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl<S> Stream for RecordBatchStreamAdapter<S>
where
    S: Stream<Item = Result<RecordBatch>>,
{
    type Item = Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.project().stream.poll_next(cx)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.stream.size_hint()
    }
}
