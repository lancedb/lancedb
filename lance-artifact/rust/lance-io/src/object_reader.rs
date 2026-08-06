// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::fs::File;
use std::ops::Range;
use std::sync::Arc;

use crate::local::join_local_io;
#[cfg(windows)]
use crate::local::read_exact_at;
#[cfg(unix)]
use std::os::unix::fs::FileExt;

use bytes::Bytes;
use futures::{
    FutureExt,
    future::{BoxFuture, Shared},
    stream::{self, StreamExt},
};
use lance_core::deepsize::DeepSizeOf;
use lance_core::{Error, Result, error::CloneableError};
use object_store::ObjectStoreExt;
use object_store::{GetOptions, GetResult, ObjectStore, Result as OSResult, path::Path};
use tokio::sync::OnceCell;
use tracing::instrument;

use crate::{
    object_store::DEFAULT_CLOUD_IO_PARALLELISM,
    traits::{ByteStream, Reader},
};

trait StaticGetRange {
    fn path(&self) -> &Path;
    fn get_range(&self) -> BoxFuture<'static, OSResult<GetResult>>;
}

/// A wrapper around an object store and a path that implements a static
/// get_range method by assuming self is stored in an Arc.
struct GetRequest {
    object_store: Arc<dyn ObjectStore>,
    path: Path,
    options: GetOptions,
}

impl StaticGetRange for Arc<GetRequest> {
    fn path(&self) -> &Path {
        &self.path
    }

    fn get_range(&self) -> BoxFuture<'static, OSResult<GetResult>> {
        let store_and_path = self.clone();
        Box::pin(async move {
            store_and_path
                .object_store
                .get_opts(&store_and_path.path, store_and_path.options.clone())
                .await
        })
    }
}

/// Object Reader
///
/// Object Store + Base Path
#[derive(Debug)]
pub struct CloudObjectReader {
    // Object Store.
    pub object_store: Arc<dyn ObjectStore>,
    // File path
    pub path: Path,
    // File size, if known.
    size: OnceCell<usize>,

    block_size: usize,
    download_retry_count: usize,
}

impl DeepSizeOf for CloudObjectReader {
    fn deep_size_of_children(&self, context: &mut lance_core::deepsize::Context) -> usize {
        // Skipping object_store because there is no easy way to do that and it shouldn't be too big
        self.path.as_ref().deep_size_of_children(context)
    }
}

impl CloudObjectReader {
    /// Create an ObjectReader from URI
    pub fn new(
        object_store: Arc<dyn ObjectStore>,
        path: Path,
        block_size: usize,
        known_size: Option<usize>,
        download_retry_count: usize,
    ) -> Result<Self> {
        Ok(Self {
            object_store,
            path,
            size: OnceCell::new_with(known_size),
            block_size,
            download_retry_count,
        })
    }
}

// Retries for the initial request are handled by object store, but
// there are no retries for failures that occur during the streaming
// of the response body. Thus we add an outer retry loop here.
async fn do_with_retry<'a, O>(f: impl Fn() -> BoxFuture<'a, OSResult<O>> + Clone) -> OSResult<O> {
    let mut retries = 3;
    loop {
        let f = f.clone();
        match f().await {
            Ok(val) => return Ok(val),
            Err(err) => {
                if retries == 0 {
                    return Err(err);
                }
                retries -= 1;
            }
        }
    }
}

// We have a separate retry loop here.  This is because object_store does not
// attempt retries on downloads that fail during streaming of the response body.
//
// However, this failure is pretty common (e.g. timeout) and we want to retry in these
// situations.  In addition, we provide additional logging information in these
// failures cases.
async fn do_get_with_outer_retry(
    download_retry_count: usize,
    get_request: Arc<GetRequest>,
    desc: impl Fn() -> String,
) -> OSResult<Bytes> {
    let mut retries = download_retry_count;
    loop {
        let get_request_clone = get_request.clone();
        let get_result = do_with_retry(move || get_request_clone.get_range()).await?;
        match get_result.bytes().await {
            Ok(bytes) => return Ok(bytes),
            Err(err) => {
                if retries == 0 {
                    log::warn!(
                        "Failed to download {} from {} after {} attempts.  This may indicate that cloud storage is overloaded or your timeout settings are too restrictive.  Error details: {:?}",
                        desc(),
                        get_request.path(),
                        download_retry_count,
                        err
                    );
                    return Err(err);
                }
                log::debug!(
                    "Retrying {} from {} (remaining retries: {}).  Error details: {:?}",
                    desc(),
                    get_request.path(),
                    retries,
                    err
                );
                retries -= 1;
            }
        }
    }
}

impl Reader for CloudObjectReader {
    fn path(&self) -> &Path {
        &self.path
    }

    fn block_size(&self) -> usize {
        self.block_size
    }

    fn io_parallelism(&self) -> usize {
        DEFAULT_CLOUD_IO_PARALLELISM
    }

    /// Object/File Size.
    fn size(&self) -> BoxFuture<'_, object_store::Result<usize>> {
        Box::pin(async move {
            self.size
                .get_or_try_init(|| async move {
                    let meta =
                        do_with_retry(|| Box::pin(self.object_store.head(&self.path))).await?;
                    Ok(meta.size as usize)
                })
                .await
                .cloned()
        })
    }

    #[instrument(level = "debug", skip(self))]
    fn get_range(&self, range: Range<usize>) -> BoxFuture<'static, OSResult<Bytes>> {
        let object_store = self.object_store.clone();
        let path = self.path.clone();
        let get_range = Range {
            start: range.start as u64,
            end: range.end as u64,
        };
        Box::pin(async move {
            let bytes = do_with_retry(move || {
                let object_store = object_store.clone();
                let path = path.clone();
                let get_range = get_range.clone();
                Box::pin(async move { object_store.get_ranges(&path, &[get_range]).await })
            })
            .await?;

            bytes
                .into_iter()
                .next()
                .ok_or_else(|| object_store::Error::Generic {
                    store: "CloudObjectReader",
                    source: "get_ranges returned no bytes".into(),
                })
        })
    }

    #[instrument(level = "debug", skip_all)]
    fn get_all(&self) -> BoxFuture<'_, OSResult<Bytes>> {
        let get_request = Arc::new(GetRequest {
            object_store: self.object_store.clone(),
            path: self.path.clone(),
            options: GetOptions::default(),
        });
        Box::pin(async move {
            do_get_with_outer_retry(self.download_retry_count, get_request, || {
                "read_all".to_string()
            })
            .await
        })
    }

    fn get_stream(&self) -> BoxFuture<'_, OSResult<ByteStream>> {
        let get_request = Arc::new(GetRequest {
            object_store: self.object_store.clone(),
            path: self.path.clone(),
            options: GetOptions::default(),
        });
        Box::pin(async move {
            let get_request_clone = get_request.clone();
            let get_result = do_with_retry(move || get_request_clone.get_range()).await?;
            Ok(get_result.into_stream())
        })
    }

    fn get_range_stream(&self, range: Range<usize>) -> BoxFuture<'_, OSResult<ByteStream>> {
        let get_request = Arc::new(GetRequest {
            object_store: self.object_store.clone(),
            path: self.path.clone(),
            options: GetOptions {
                range: Some(
                    Range {
                        start: range.start as u64,
                        end: range.end as u64,
                    }
                    .into(),
                ),
                ..Default::default()
            },
        });
        Box::pin(async move {
            let get_request_clone = get_request.clone();
            let get_result = do_with_retry(move || get_request_clone.get_range()).await?;
            Ok(get_result.into_stream())
        })
    }
}

#[derive(Debug)]
pub struct SmallReaderInner {
    path: Path,
    size: usize,
    state: std::sync::Mutex<SmallReaderState>,
}

/// A reader for a file so small, we just eagerly read it all into memory.
///
/// When created, it represents a future that will read the whole file into memory.
///
/// On the first read call, it will start the read. Multiple threads can call read at the same time.
///
/// Once the read is complete, any thread can call read again to get the result.
#[derive(Clone, Debug)]
pub struct SmallReader {
    inner: Arc<SmallReaderInner>,
}

enum SmallReaderState {
    Loading(Shared<BoxFuture<'static, std::result::Result<Bytes, CloneableError>>>),
    Finished(std::result::Result<Bytes, CloneableError>),
}

impl std::fmt::Debug for SmallReaderState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Loading(_) => write!(f, "Loading"),
            Self::Finished(Ok(data)) => {
                write!(f, "Finished({} bytes)", data.len())
            }
            Self::Finished(Err(err)) => {
                write!(f, "Finished({})", err.0)
            }
        }
    }
}

impl SmallReader {
    pub fn new(
        store: Arc<dyn ObjectStore>,
        path: Path,
        download_retry_count: usize,
        size: usize,
    ) -> Self {
        let path_ref = path.clone();
        let state = SmallReaderState::Loading(
            Box::pin(async move {
                let object_reader =
                    CloudObjectReader::new(store, path_ref, 0, None, download_retry_count)
                        .map_err(CloneableError)?;
                object_reader
                    .get_all()
                    .await
                    .map_err(|err| CloneableError(Error::from(err)))
            })
            .boxed()
            .shared(),
        );
        Self {
            inner: Arc::new(SmallReaderInner {
                path,
                size,
                state: std::sync::Mutex::new(state),
            }),
        }
    }
}

impl SmallReaderInner {
    async fn wait(&self) -> OSResult<Bytes> {
        let future = {
            let state = self.state.lock().unwrap();
            match &*state {
                SmallReaderState::Loading(future) => future.clone(),
                SmallReaderState::Finished(result) => {
                    return result.clone().map_err(|err| err.0.into());
                }
            }
        };

        let result = future.await;
        let result_to_return = result.clone().map_err(|err| err.0.into());
        let mut state = self.state.lock().unwrap();
        if matches!(*state, SmallReaderState::Loading(_)) {
            *state = SmallReaderState::Finished(result);
        }
        result_to_return
    }
}

impl Reader for SmallReader {
    fn path(&self) -> &Path {
        &self.inner.path
    }

    fn block_size(&self) -> usize {
        64 * 1024
    }

    fn io_parallelism(&self) -> usize {
        1024
    }

    /// Object/File Size.
    fn size(&self) -> BoxFuture<'_, OSResult<usize>> {
        let size = self.inner.size;
        Box::pin(async move { Ok(size) })
    }

    fn get_range(&self, range: Range<usize>) -> BoxFuture<'static, OSResult<Bytes>> {
        let inner = self.inner.clone();
        Box::pin(async move {
            let bytes = inner.wait().await?;
            let start = range.start;
            let end = range.end;
            if start >= bytes.len() || end > bytes.len() {
                return Err(object_store::Error::Generic {
                    store: "memory",
                    source: format!(
                        "Invalid range {}..{} for object of size {} bytes",
                        start,
                        end,
                        bytes.len()
                    )
                    .into(),
                });
            }
            Ok(bytes.slice(range))
        })
    }

    fn get_all(&self) -> BoxFuture<'_, OSResult<Bytes>> {
        Box::pin(async move { self.inner.wait().await })
    }
}

pub(crate) fn stream_local_range(
    file: Arc<File>,
    path: Path,
    io_tracker: Arc<crate::utils::tracking_store::IOTracker>,
    range: Range<usize>,
    chunk_size: usize,
) -> ByteStream {
    stream::try_unfold(
        (file, path, io_tracker, range.start, range.end),
        move |state| async move {
            let (file, path, io_tracker, start, end) = state;
            if start >= end {
                return Ok(None);
            }

            let next = (start + chunk_size).min(end);
            let file_clone = file.clone();
            let path_clone = path.clone();
            let num_bytes = (next - start) as u64;
            let metrics = io_tracker.begin_io("get");
            let result = join_local_io(tokio::task::spawn_blocking(move || {
                let mut buf = bytes::BytesMut::with_capacity(next - start);
                // Safety: buffer capacity matches the exact number of bytes we read below.
                unsafe { buf.set_len(next - start) };
                #[cfg(unix)]
                file_clone.read_exact_at(buf.as_mut(), start as u64)?;
                #[cfg(windows)]
                read_exact_at(file_clone, buf.as_mut(), start as u64)?;
                Ok::<_, std::io::Error>(buf.freeze())
            }))
            .await;
            metrics.record(&result, num_bytes);
            let bytes = result?;

            io_tracker.record_read(
                "get_range_stream",
                path_clone,
                num_bytes,
                Some(start as u64..next as u64),
            );

            Ok(Some((bytes, (file, path, io_tracker, next, end))))
        },
    )
    .boxed()
}

impl DeepSizeOf for SmallReader {
    fn deep_size_of_children(&self, context: &mut lance_core::deepsize::Context) -> usize {
        let mut size = self.inner.path.as_ref().deep_size_of_children(context);

        if let Ok(guard) = self.inner.state.try_lock()
            && let SmallReaderState::Finished(Ok(data)) = &*guard
        {
            size += data.len();
        }

        size
    }
}
