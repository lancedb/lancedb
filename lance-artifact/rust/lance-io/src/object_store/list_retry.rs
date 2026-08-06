// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::{future::Future, pin::Pin, sync::Arc, task::Poll, time::Duration};

use futures::stream::BoxStream;
use futures::{Stream, StreamExt};
use object_store::{ObjectMeta, ObjectStore, path::Path};
use rand::Rng;
use tokio::time::Sleep;

const DEFAULT_BASE_RETRY_DELAY: Duration = Duration::from_millis(100);
const DEFAULT_MAX_RETRY_DELAY: Duration = Duration::from_secs(5);

/// A stream that does outer retries on list operations.
///
/// This is to handle request responses that ObjectStore doesn't handle, such as
/// the error `error decoding response body` from queries to GCS.
pub struct ListRetryStream {
    object_store: Arc<dyn ObjectStore>,
    current_stream: BoxStream<'static, object_store::Result<ObjectMeta>>,
    prefix: Option<Path>,
    last_successful_key: Option<Path>,
    max_retries: usize,
    current_retries: usize,
    retry_sleep: Option<Pin<Box<Sleep>>>,
    base_retry_delay: Duration,
    max_retry_delay: Duration,
}

impl ListRetryStream {
    pub fn new(
        object_store: Arc<dyn ObjectStore>,
        prefix: Option<Path>,
        max_retries: usize,
    ) -> Self {
        let current_stream = object_store.list(prefix.as_ref());
        Self {
            object_store,
            current_stream,
            prefix,
            last_successful_key: None,
            max_retries,
            current_retries: 0,
            retry_sleep: None,
            base_retry_delay: DEFAULT_BASE_RETRY_DELAY,
            max_retry_delay: DEFAULT_MAX_RETRY_DELAY,
        }
    }

    #[cfg(test)]
    fn new_with_backoff(
        object_store: Arc<dyn ObjectStore>,
        prefix: Option<Path>,
        max_retries: usize,
        base_retry_delay: Duration,
        max_retry_delay: Duration,
    ) -> Self {
        let current_stream = object_store.list(prefix.as_ref());
        Self {
            object_store,
            current_stream,
            prefix,
            last_successful_key: None,
            max_retries,
            current_retries: 0,
            retry_sleep: None,
            base_retry_delay,
            max_retry_delay,
        }
    }

    fn is_retryable(error: &object_store::Error) -> bool {
        !matches!(
            error,
            object_store::Error::NotFound { .. }
                | object_store::Error::InvalidPath { .. }
                | object_store::Error::NotSupported { .. }
                | object_store::Error::NotImplemented { .. }
        )
    }

    fn retry_delay(&self) -> Duration {
        let exponent = self.current_retries.saturating_sub(1).min(16) as u32;
        let base_ms = self.base_retry_delay.as_millis().max(1);
        let max_ms = self.max_retry_delay.as_millis().max(base_ms);
        let cap_ms = base_ms.saturating_mul(1_u128 << exponent).min(max_ms);
        let min_ms = (cap_ms / 2).max(1);
        let delay_ms = if cap_ms > min_ms {
            rand::rng().random_range(min_ms..=cap_ms)
        } else {
            cap_ms
        };
        Duration::from_millis(delay_ms.min(u64::MAX as u128) as u64)
    }

    fn recreate_stream(&mut self) {
        self.current_stream = if let Some(offset) = self.last_successful_key.clone() {
            self.object_store
                .list_with_offset(self.prefix.as_ref(), &offset)
        } else {
            self.object_store.list(self.prefix.as_ref())
        };
    }
}

impl Stream for ListRetryStream {
    type Item = Result<ObjectMeta, object_store::Error>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            if let Some(sleep) = this.retry_sleep.as_mut() {
                match sleep.as_mut().poll(cx) {
                    Poll::Ready(()) => {
                        this.retry_sleep = None;
                        this.recreate_stream();
                    }
                    Poll::Pending => return Poll::Pending,
                }
            }

            match this.current_stream.poll_next_unpin(cx) {
                Poll::Ready(Some(Ok(meta))) => {
                    this.last_successful_key = Some(meta.location.clone());
                    return Poll::Ready(Some(Ok(meta)));
                }
                Poll::Ready(None) => {
                    // If the stream is done, return None
                    return Poll::Ready(None);
                }
                Poll::Ready(Some(Err(error))) if Self::is_retryable(&error) => {
                    if this.current_retries < this.max_retries {
                        this.current_retries += 1;
                        this.retry_sleep = Some(Box::pin(tokio::time::sleep(this.retry_delay())));

                        continue;
                    } else {
                        return Poll::Ready(Some(Err(error)));
                    }
                }
                Poll::Ready(Some(Err(error))) => {
                    return Poll::Ready(Some(Err(error)));
                }
                Poll::Pending => {
                    return Poll::Pending;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::fmt::{Debug, Display, Formatter};
    use std::ops::Range;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Instant;

    use async_trait::async_trait;
    use bytes::Bytes;
    use futures::stream;
    use object_store::memory::InMemory;
    use object_store::{
        CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, PutMultipartOptions,
        PutOptions, PutPayload, PutResult, Result as OSResult,
    };

    fn assert_send<T: Send>() {}

    #[test]
    fn test_list_retry_stream_send() {
        // Ensure that ListRetryStream is Send
        assert_send::<ListRetryStream>();
    }

    fn object_meta(path: &str) -> ObjectMeta {
        ObjectMeta {
            location: Path::from(path),
            last_modified: chrono::Utc::now(),
            size: 1,
            e_tag: None,
            version: None,
        }
    }

    fn retryable_error() -> object_store::Error {
        object_store::Error::Generic {
            store: "scripted",
            source: "retryable list error".into(),
        }
    }

    fn not_found_error() -> object_store::Error {
        object_store::Error::NotFound {
            path: "missing".to_string(),
            source: "missing".into(),
        }
    }

    struct ScriptedListStore {
        inner: InMemory,
        list_streams: Mutex<VecDeque<Vec<OSResult<ObjectMeta>>>>,
        offset_streams: Mutex<VecDeque<Vec<OSResult<ObjectMeta>>>>,
        list_calls: AtomicUsize,
        offset_calls: AtomicUsize,
        last_offset: Mutex<Option<Path>>,
    }

    impl ScriptedListStore {
        fn new(
            list_streams: Vec<Vec<OSResult<ObjectMeta>>>,
            offset_streams: Vec<Vec<OSResult<ObjectMeta>>>,
        ) -> Self {
            Self {
                inner: InMemory::new(),
                list_streams: Mutex::new(list_streams.into()),
                offset_streams: Mutex::new(offset_streams.into()),
                list_calls: AtomicUsize::new(0),
                offset_calls: AtomicUsize::new(0),
                last_offset: Mutex::new(None),
            }
        }

        fn list_calls(&self) -> usize {
            self.list_calls.load(Ordering::SeqCst)
        }

        fn offset_calls(&self) -> usize {
            self.offset_calls.load(Ordering::SeqCst)
        }

        fn last_offset(&self) -> Option<Path> {
            self.last_offset.lock().unwrap().clone()
        }
    }

    impl Display for ScriptedListStore {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            write!(f, "ScriptedListStore")
        }
    }

    impl Debug for ScriptedListStore {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("ScriptedListStore").finish()
        }
    }

    #[async_trait]
    impl ObjectStore for ScriptedListStore {
        async fn put_opts(
            &self,
            location: &Path,
            bytes: PutPayload,
            opts: PutOptions,
        ) -> OSResult<PutResult> {
            self.inner.put_opts(location, bytes, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: PutMultipartOptions,
        ) -> OSResult<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(&self, location: &Path, options: GetOptions) -> OSResult<GetResult> {
            self.inner.get_opts(location, options).await
        }

        async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> OSResult<Vec<Bytes>> {
            self.inner.get_ranges(location, ranges).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, OSResult<Path>>,
        ) -> BoxStream<'static, OSResult<Path>> {
            self.inner.delete_stream(locations)
        }

        fn list(&self, _prefix: Option<&Path>) -> BoxStream<'static, OSResult<ObjectMeta>> {
            self.list_calls.fetch_add(1, Ordering::SeqCst);
            let results = self
                .list_streams
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or_default();
            stream::iter(results).boxed()
        }

        fn list_with_offset(
            &self,
            _prefix: Option<&Path>,
            offset: &Path,
        ) -> BoxStream<'static, OSResult<ObjectMeta>> {
            self.offset_calls.fetch_add(1, Ordering::SeqCst);
            *self.last_offset.lock().unwrap() = Some(offset.clone());
            let results = self
                .offset_streams
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or_default();
            stream::iter(results).boxed()
        }

        async fn list_with_delimiter(&self, prefix: Option<&Path>) -> OSResult<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(&self, from: &Path, to: &Path, opts: CopyOptions) -> OSResult<()> {
            self.inner.copy_opts(from, to, opts).await
        }
    }

    #[tokio::test]
    async fn test_list_retry_stream_retries_after_backoff() {
        let store = Arc::new(ScriptedListStore::new(
            vec![
                vec![Err(retryable_error())],
                vec![Ok(object_meta("prefix/file"))],
            ],
            vec![],
        ));
        let stream = ListRetryStream::new_with_backoff(
            store.clone(),
            Some(Path::from("prefix")),
            1,
            Duration::from_millis(20),
            Duration::from_millis(20),
        );

        let start = Instant::now();
        let items = stream.collect::<Vec<_>>().await;

        assert_eq!(items.len(), 1);
        assert!(items[0].is_ok());
        assert_eq!(store.list_calls(), 2);
        assert!(
            start.elapsed() >= Duration::from_millis(10),
            "retry should wait before recreating the list stream"
        );
    }

    #[tokio::test]
    async fn test_list_retry_stream_resumes_after_last_successful_key() {
        let store = Arc::new(ScriptedListStore::new(
            vec![vec![Ok(object_meta("prefix/a")), Err(retryable_error())]],
            vec![vec![Ok(object_meta("prefix/b"))]],
        ));
        let stream = ListRetryStream::new_with_backoff(
            store.clone(),
            Some(Path::from("prefix")),
            1,
            Duration::from_millis(1),
            Duration::from_millis(1),
        );

        let items = stream.collect::<Vec<_>>().await;

        assert_eq!(items.len(), 2);
        assert_eq!(items[0].as_ref().unwrap().location, Path::from("prefix/a"));
        assert_eq!(items[1].as_ref().unwrap().location, Path::from("prefix/b"));
        assert_eq!(store.list_calls(), 1);
        assert_eq!(store.offset_calls(), 1);
        assert_eq!(store.last_offset(), Some(Path::from("prefix/a")));
    }

    #[tokio::test]
    async fn test_list_retry_stream_non_retryable_errors_return_immediately() {
        let store = Arc::new(ScriptedListStore::new(
            vec![vec![Err(not_found_error())]],
            vec![],
        ));
        let stream = ListRetryStream::new_with_backoff(
            store.clone(),
            Some(Path::from("prefix")),
            5,
            Duration::from_millis(1),
            Duration::from_millis(1),
        );

        let items = stream.collect::<Vec<_>>().await;

        assert_eq!(items.len(), 1);
        assert!(matches!(
            items.into_iter().next().unwrap(),
            Err(object_store::Error::NotFound { .. })
        ));
        assert_eq!(store.list_calls(), 1);
        assert_eq!(store.offset_calls(), 0);
    }
}
