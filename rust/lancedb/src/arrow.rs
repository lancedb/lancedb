// Copyright 2023 Lance Developers.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{pin::Pin, sync::Arc};

pub use arrow_array;
pub use arrow_schema;
use futures::{Stream, StreamExt};

use crate::error::Result;

/// An iterator of batches that also has a schema
pub trait RecordBatchReader: Iterator<Item = Result<arrow_array::RecordBatch>> {
    /// Returns the schema of this `RecordBatchReader`.
    ///
    /// Implementation of this trait should guarantee that all `RecordBatch`'s returned by this
    /// reader should have the same schema as returned from this method.
    fn schema(&self) -> Arc<arrow_schema::Schema>;
}

/// A simple RecordBatchReader formed from the two parts (iterator + schema)
pub struct SimpleRecordBatchReader<I: Iterator<Item = Result<arrow_array::RecordBatch>>> {
    pub schema: Arc<arrow_schema::Schema>,
    pub batches: I,
}

impl<I: Iterator<Item = Result<arrow_array::RecordBatch>>> Iterator for SimpleRecordBatchReader<I> {
    type Item = Result<arrow_array::RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        self.batches.next()
    }
}

impl<I: Iterator<Item = Result<arrow_array::RecordBatch>>> RecordBatchReader
    for SimpleRecordBatchReader<I>
{
    fn schema(&self) -> Arc<arrow_schema::Schema> {
        self.schema.clone()
    }
}

/// A stream of batches that also has a schema
pub trait RecordBatchStream: Stream<Item = Result<arrow_array::RecordBatch>> {
    /// Returns the schema of this `RecordBatchStream`.
    ///
    /// Implementation of this trait should guarantee that all `RecordBatch`'s returned by this
    /// stream should have the same schema as returned from this method.
    fn schema(&self) -> Arc<arrow_schema::Schema>;
}

/// A boxed RecordBatchStream that is also Send
pub type SendableRecordBatchStream = Pin<Box<dyn RecordBatchStream + Send>>;

impl<I: lance::io::RecordBatchStream + 'static> From<I> for SendableRecordBatchStream {
    fn from(stream: I) -> Self {
        let schema = stream.schema();
        let mapped_stream = Box::pin(stream.map(|r| r.map_err(Into::into)));
        Box::pin(SimpleRecordBatchStream {
            schema,
            stream: mapped_stream,
        })
    }
}

/// A simple RecordBatchStream formed from the two parts (stream + schema)
#[pin_project::pin_project]
pub struct SimpleRecordBatchStream<S: Stream<Item = Result<arrow_array::RecordBatch>>> {
    pub schema: Arc<arrow_schema::Schema>,
    #[pin]
    pub stream: S,
}

impl<S: Stream<Item = Result<arrow_array::RecordBatch>>> Stream for SimpleRecordBatchStream<S> {
    type Item = Result<arrow_array::RecordBatch>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.project();
        this.stream.poll_next(cx)
    }
}

impl<S: Stream<Item = Result<arrow_array::RecordBatch>>> RecordBatchStream
    for SimpleRecordBatchStream<S>
{
    fn schema(&self) -> Arc<arrow_schema::Schema> {
        self.schema.clone()
    }
}
