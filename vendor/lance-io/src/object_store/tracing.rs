// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Wrappers around object_store that apply tracing

use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use futures::StreamExt;
use futures::stream::BoxStream;
use lance_core::utils::tracing::StreamTracingExt;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions, Result as OSResult,
    UploadPart,
};
use tracing::{Instrument, Span, instrument};

#[derive(Debug)]
pub struct TracedMultipartUpload {
    write_span: Span,
    target: Box<dyn MultipartUpload>,
    write_size: usize,
}

#[async_trait::async_trait]
impl MultipartUpload for TracedMultipartUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        let write_span = self.write_span.clone();
        self.write_size += data.content_length();
        let fut = self.target.put_part(data);
        Box::pin(fut.instrument(write_span))
    }

    #[instrument(level = "debug", skip_all)]
    async fn complete(&mut self) -> OSResult<PutResult> {
        let res = self.target.complete().await?;
        self.write_span.record("size", self.write_size);
        Ok(res)
    }

    #[instrument(level = "debug", skip_all)]
    async fn abort(&mut self) -> OSResult<()> {
        self.target.abort().await
    }
}

#[derive(Debug)]
pub struct TracedObjectStore {
    target: Arc<dyn object_store::ObjectStore>,
}

impl std::fmt::Display for TracedObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("TracedObjectStore({})", self.target))
    }
}

#[async_trait::async_trait]
#[deny(clippy::missing_trait_methods)]
impl object_store::ObjectStore for TracedObjectStore {
    #[instrument(level = "debug", skip(self, bytes, location, opts), fields(path = location.as_ref(), size = bytes.content_length()))]
    async fn put_opts(
        &self,
        location: &Path,
        bytes: PutPayload,
        opts: PutOptions,
    ) -> OSResult<PutResult> {
        self.target.put_opts(location, bytes, opts).await
    }

    #[instrument(level = "debug", skip(self, location, opts), fields(path = location.as_ref(), size = tracing::field::Empty))]
    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> OSResult<Box<dyn object_store::MultipartUpload>> {
        let upload = self.target.put_multipart_opts(location, opts).await?;
        Ok(Box::new(TracedMultipartUpload {
            target: upload,
            write_span: tracing::Span::current(),
            write_size: 0,
        }))
    }

    #[instrument(level = "debug", skip(self, options, location), fields(path = location.as_ref(), size = tracing::field::Empty))]
    async fn get_opts(&self, location: &Path, options: GetOptions) -> OSResult<GetResult> {
        let res = self.target.get_opts(location, options).await?;

        let span = tracing::Span::current();
        span.record("size", res.range.end - res.range.start);

        Ok(res)
    }

    #[instrument(level = "debug", skip(self, location), fields(path = location.as_ref(), size = ranges.iter().map(|r| r.end - r.start).sum::<u64>()))]
    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> OSResult<Vec<Bytes>> {
        self.target.get_ranges(location, ranges).await
    }

    #[instrument(level = "debug", skip_all)]
    fn delete_stream(
        &self,
        locations: BoxStream<'static, OSResult<Path>>,
    ) -> BoxStream<'static, OSResult<Path>> {
        self.target
            .delete_stream(locations)
            .stream_in_current_span()
            .boxed()
    }

    #[instrument(level = "debug", skip(self, prefix), fields(prefix = prefix.map(|p| p.as_ref())))]
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, OSResult<ObjectMeta>> {
        self.target.list(prefix).stream_in_current_span().boxed()
    }

    #[instrument(level = "debug", skip(self, prefix, offset), fields(prefix = prefix.map(|p| p.as_ref()), offset = offset.as_ref()))]
    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, OSResult<ObjectMeta>> {
        self.target
            .list_with_offset(prefix, offset)
            .stream_in_current_span()
            .boxed()
    }

    #[instrument(level = "debug", skip(self, prefix), fields(prefix = prefix.map(|p| p.as_ref())))]
    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> OSResult<ListResult> {
        self.target.list_with_delimiter(prefix).await
    }

    #[instrument(level = "debug", skip(self, from, to, opts), fields(from = from.as_ref(), to = to.as_ref()))]
    async fn copy_opts(&self, from: &Path, to: &Path, opts: CopyOptions) -> OSResult<()> {
        self.target.copy_opts(from, to, opts).await
    }

    #[instrument(level = "debug", skip(self, from, to, opts), fields(from = from.as_ref(), to = to.as_ref()))]
    async fn rename_opts(&self, from: &Path, to: &Path, opts: RenameOptions) -> OSResult<()> {
        self.target.rename_opts(from, to, opts).await
    }
}

pub trait ObjectStoreTracingExt {
    fn traced(self) -> Arc<dyn object_store::ObjectStore>;
}

impl ObjectStoreTracingExt for Arc<dyn object_store::ObjectStore> {
    fn traced(self) -> Arc<dyn object_store::ObjectStore> {
        Arc::new(TracedObjectStore { target: self })
    }
}

impl<T: object_store::ObjectStore> ObjectStoreTracingExt for Arc<T> {
    fn traced(self) -> Arc<dyn object_store::ObjectStore> {
        Arc::new(TracedObjectStore { target: self })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use bytes::Bytes;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::{ObjectStoreExt, PutPayload};
    use tracing_mock::{expect, subscriber};

    fn payload(data: &[u8]) -> PutPayload {
        PutPayload::from_bytes(Bytes::copy_from_slice(data))
    }

    fn make_store() -> Arc<dyn object_store::ObjectStore> {
        Arc::new(InMemory::new()).traced()
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_put_records_path_and_size() {
        let path = Path::from("a/b.bin");
        let data = b"hello world";

        let span = expect::span().named("put_opts");
        let (sub, handle) = subscriber::mock()
            .new_span(
                span.clone().with_fields(
                    expect::field("path")
                        .with_value(&"a/b.bin")
                        .and(expect::field("size").with_value(&data.len()))
                        .only(),
                ),
            )
            .enter(span.clone())
            .exit(span.clone())
            .run_with_handle();

        let _guard = tracing::subscriber::set_default(sub);
        make_store().put(&path, payload(data)).await.unwrap();
        drop(_guard);

        handle.assert_finished();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_get_records_path_and_size() {
        let path = Path::from("a/b.bin");
        let data = b"hello world";
        let size = data.len() as u64; // meta.size is u64

        // Seed without an active mock subscriber.
        let store = make_store();
        store.put(&path, payload(data)).await.unwrap();

        let span = expect::span().named("get_opts");
        let (sub, handle) = subscriber::mock()
            .new_span(
                // size = Empty at span creation, so only path is visited.
                span.clone()
                    .with_fields(expect::field("path").with_value(&"a/b.bin").only()),
            )
            .enter(span.clone())
            .record(span.clone(), expect::field("size").with_value(&size))
            .exit(span.clone())
            .run_with_handle();

        let _guard = tracing::subscriber::set_default(sub);
        store.get(&path).await.unwrap();
        drop(_guard);

        handle.assert_finished();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_get_range_records_path_and_size() {
        let path = Path::from("a/b.bin");
        let data = b"hello world";

        let store = make_store();
        store.put(&path, payload(data)).await.unwrap();

        let range = 2u64..7u64;
        let size = range.end - range.start;

        let span = expect::span().named("get_opts");
        let (sub, handle) = subscriber::mock()
            .new_span(
                span.clone()
                    .with_fields(expect::field("path").with_value(&"a/b.bin").only()),
            )
            .enter(span.clone())
            .record(span.clone(), expect::field("size").with_value(&size))
            .exit(span.clone())
            .run_with_handle();

        let _guard = tracing::subscriber::set_default(sub);
        store.get_range(&path, range).await.unwrap();
        drop(_guard);

        handle.assert_finished();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_get_ranges_records_path_and_total_size() {
        let path = Path::from("a/b.bin");
        let data = b"hello world";

        let store = make_store();
        store.put(&path, payload(data)).await.unwrap();

        let ranges = [2u64..5u64, 6u64..9u64];
        let size: u64 = ranges.iter().map(|r| r.end - r.start).sum();

        let span = expect::span().named("get_ranges");
        let (sub, handle) = subscriber::mock()
            .new_span(
                // `ranges` is also captured automatically as a debug field since
                // it is not in the skip list, so we don't use `.only()` here.
                span.clone().with_fields(
                    expect::field("path")
                        .with_value(&"a/b.bin")
                        .and(expect::field("size").with_value(&size)),
                ),
            )
            .enter(span.clone())
            .exit(span.clone())
            .run_with_handle();

        let _guard = tracing::subscriber::set_default(sub);
        store.get_ranges(&path, &ranges).await.unwrap();
        drop(_guard);

        handle.assert_finished();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_head_records_path() {
        let path = Path::from("a/b.bin");
        let data = b"hello world";
        let size = data.len() as u64;

        let store = make_store();
        store.put(&path, payload(data)).await.unwrap();

        let span = expect::span().named("get_opts");
        let (sub, handle) = subscriber::mock()
            .new_span(
                span.clone()
                    .with_fields(expect::field("path").with_value(&"a/b.bin").only()),
            )
            .enter(span.clone())
            .record(span.clone(), expect::field("size").with_value(&size))
            .exit(span.clone())
            .run_with_handle();

        let _guard = tracing::subscriber::set_default(sub);
        store.head(&path).await.unwrap();
        drop(_guard);

        handle.assert_finished();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_delete_records_path() {
        let path = Path::from("a/b.bin");
        let data = b"hello world";

        let store = make_store();
        store.put(&path, payload(data)).await.unwrap();

        let span = expect::span().named("delete_stream");
        let (sub, handle) = subscriber::mock()
            .new_span(span.clone())
            .enter(span.clone())
            .exit(span.clone())
            .run_with_handle();

        let _guard = tracing::subscriber::set_default(sub);
        store.delete(&path).await.unwrap();
        drop(_guard);

        handle.assert_finished();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_copy_records_from_and_to() {
        let from = Path::from("a/src.bin");
        let to = Path::from("a/dst.bin");
        let data = b"hello world";

        let store = make_store();
        store.put(&from, payload(data)).await.unwrap();

        let span = expect::span().named("copy_opts");
        let (sub, handle) = subscriber::mock()
            .new_span(
                span.clone().with_fields(
                    expect::field("from")
                        .with_value(&"a/src.bin")
                        .and(expect::field("to").with_value(&"a/dst.bin"))
                        .only(),
                ),
            )
            .enter(span.clone())
            .exit(span.clone())
            .run_with_handle();

        let _guard = tracing::subscriber::set_default(sub);
        store.copy(&from, &to).await.unwrap();
        drop(_guard);

        handle.assert_finished();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_put_multipart_records_path() {
        let path = Path::from("a/b.bin");
        let data = b"hello world";

        let put_mp_span = expect::span().named("put_multipart_opts");
        // Expect only the span creation; any subsequent enter/exit/record
        // events are not in the queue so they are silently ignored.
        let (sub, handle) = subscriber::mock()
            .new_span(
                // size = Empty at span creation, so only path is visited.
                put_mp_span.with_fields(expect::field("path").with_value(&"a/b.bin").only()),
            )
            .run_with_handle();

        let _guard = tracing::subscriber::set_default(sub);
        let store = make_store();
        let mut upload = store.put_multipart(&path).await.unwrap();
        upload.put_part(payload(data)).await.unwrap();
        upload.complete().await.unwrap();
        drop(_guard);

        handle.assert_finished();
    }
}
