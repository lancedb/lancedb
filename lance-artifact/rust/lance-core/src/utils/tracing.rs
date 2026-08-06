// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use futures::Stream;
use pin_project::pin_project;
use tracing::Span;

#[pin_project]
pub struct InstrumentedStream<I: Stream> {
    #[pin]
    stream: I,
    span: Span,
}

impl<I: Stream> Stream for InstrumentedStream<I> {
    type Item = I::Item;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.project();
        let _guard = this.span.enter();
        this.stream.poll_next(cx)
    }
}

// It would be nice to call the method in_current_span but sadly the Instrumented trait in
// the tracing crate already stole the name for all Sized types
pub trait StreamTracingExt {
    /// All calls to poll the stream will be done in the context of the current span (when this method is called)
    fn stream_in_current_span(self) -> InstrumentedStream<Self>
    where
        Self: Stream,
        Self: Sized;

    fn stream_in_span(self, span: Span) -> InstrumentedStream<Self>
    where
        Self: Stream,
        Self: Sized;
}

impl<S: Stream> StreamTracingExt for S {
    fn stream_in_current_span(self) -> InstrumentedStream<Self>
    where
        Self: Stream,
        Self: Sized,
    {
        self.stream_in_span(Span::current())
    }

    fn stream_in_span(self, span: Span) -> InstrumentedStream<Self>
    where
        Self: Stream,
        Self: Sized,
    {
        InstrumentedStream { stream: self, span }
    }
}

pub const TRACE_FILE_AUDIT: &str = "lance::file_audit";
pub const AUDIT_MODE_CREATE: &str = "create";
pub const AUDIT_MODE_DELETE: &str = "delete";
pub const AUDIT_MODE_DELETE_UNVERIFIED: &str = "delete_unverified";
pub const AUDIT_TYPE_DELETION: &str = "deletion";
pub const AUDIT_TYPE_MANIFEST: &str = "manifest";
pub const AUDIT_TYPE_INDEX: &str = "index";
pub const AUDIT_TYPE_DATA: &str = "data";
pub const AUDIT_TYPE_TRANSACTION: &str = "transaction";
pub const TRACE_FILE_CREATE: &str = "create";
pub const TRACE_IO_EVENTS: &str = "lance::io_events";
pub const IO_TYPE_OPEN_SCALAR: &str = "open_scalar_index";
pub const IO_TYPE_OPEN_VECTOR: &str = "open_vector_index";
pub const IO_TYPE_OPEN_FRAG_REUSE: &str = "open_frag_reuse_index";
pub const IO_TYPE_OPEN_MEM_WAL: &str = "open_mem_wal_index";
pub const IO_TYPE_LOAD_VECTOR_PART: &str = "load_vector_part";
pub const IO_TYPE_LOAD_SCALAR_PART: &str = "load_scalar_part";
pub const TRACE_EXECUTION: &str = "lance::execution";
pub const EXECUTION_PLAN_RUN: &str = "plan_run";
pub const TRACE_DATASET_EVENTS: &str = "lance::dataset_events";
pub const DATASET_WRITING_EVENT: &str = "writing";
pub const DATASET_COMMITTED_EVENT: &str = "committed";
pub const DATASET_DROPPING_COLUMN_EVENT: &str = "dropping_column";
pub const DATASET_DELETING_EVENT: &str = "deleting";
pub const DATASET_COMPACTING_EVENT: &str = "compacting";
pub const DATASET_CLEANING_EVENT: &str = "cleaning";
pub const DATASET_LOADING_EVENT: &str = "loading";
pub const TRACE_OBJECT_STORE_THROTTLE: &str = "lance::object_store::throttle";
