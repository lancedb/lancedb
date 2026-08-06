// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! UringReader implementation.

use super::future::UringReadFuture;
use super::requests::IoRequest;
use super::thread::{SUBMITTED_COUNTER, THREAD_SELECTOR, URING_THREADS};
use super::{DEFAULT_URING_BLOCK_SIZE, DEFAULT_URING_IO_PARALLELISM, URING_BLOCK_SIZE};
use crate::local::to_local_path;
use crate::traits::Reader;
use crate::uring::requests::RequestState;
use crate::utils::tracking_store::IOTracker;
use bytes::{Bytes, BytesMut};
use futures::FutureExt;
use futures::future::BoxFuture;
use lance_core::deepsize::DeepSizeOf;
use lance_core::{Error, Result};
use object_store::path::Path;
use std::fs::File;
use std::future::Future;
use std::io::{self, ErrorKind};
use std::ops::Range;
use std::os::unix::io::{AsRawFd, RawFd};
use std::pin::Pin;
use std::sync::atomic::Ordering;
use std::sync::{Arc, LazyLock, Mutex};
use std::time::Duration;
use tracing::instrument;

/// Cache key for UringReader instances.
/// We cache by (path, block_size) because block_size affects reader behavior.
#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub(super) struct CacheKey {
    path: String,
    block_size: usize,
}

impl CacheKey {
    pub(super) fn new(path: &Path, block_size: usize) -> Self {
        Self {
            path: path.to_string(),
            block_size,
        }
    }
}

/// Data stored in the cache for each opened file.
#[derive(Clone)]
pub(super) struct CachedReaderData {
    pub(super) handle: Arc<UringFileHandle>,
    pub(super) size: usize,
}

/// Global cache of open file handles.
/// Entries expire after 60 seconds to ensure files are eventually closed.
pub(super) static HANDLE_CACHE: LazyLock<moka::future::Cache<CacheKey, CachedReaderData>> =
    LazyLock::new(|| {
        moka::future::Cache::builder()
            .time_to_live(Duration::from_secs(60))
            .max_capacity(10_000)
            .build()
    });

/// File handle for io_uring operations.
///
/// Keeps the file alive and provides the raw file descriptor.
#[derive(Debug)]
pub(super) struct UringFileHandle {
    /// The file (kept alive via Arc)
    #[allow(unused)]
    file: Arc<File>,

    /// Raw file descriptor for io_uring
    pub(super) fd: RawFd,

    /// Object store path
    pub(super) path: Path,
}

impl UringFileHandle {
    pub(super) fn new(file: File, path: Path) -> Self {
        let fd = file.as_raw_fd();
        Self {
            file: Arc::new(file),
            fd,
            path,
        }
    }
}

/// io_uring-based reader for local files.
///
/// This reader uses a dedicated process-wide thread running an io_uring event loop
/// for high-performance asynchronous I/O.
#[derive(Debug)]
pub struct UringReader {
    /// File handle
    handle: Arc<UringFileHandle>,

    /// Block size for I/O operations
    block_size: usize,

    /// File size (determined at open time)
    size: usize,

    /// I/O tracker for monitoring operations
    io_tracker: Arc<IOTracker>,
}

impl DeepSizeOf for UringReader {
    fn deep_size_of_children(&self, context: &mut lance_core::deepsize::Context) -> usize {
        // Skip file handle (just a system resource)
        // Only count the path's deep size
        self.handle.path.as_ref().deep_size_of_children(context)
    }
}

impl UringReader {
    /// Open a file with io_uring.
    ///
    /// This is the internal constructor used by ObjectStore.
    #[instrument(level = "debug")]
    pub(crate) async fn open(
        path: &Path,
        block_size: usize,
        known_size: Option<usize>,
        io_tracker: Arc<IOTracker>,
    ) -> Result<Box<dyn Reader>> {
        // Determine block size with environment variable override
        let block_size = URING_BLOCK_SIZE.unwrap_or(block_size.max(DEFAULT_URING_BLOCK_SIZE));

        let cache_key = CacheKey::new(path, block_size);

        // Try to get from cache first
        if let Some(data) = HANDLE_CACHE.get(&cache_key).await {
            // Use known_size if provided, otherwise use cached size
            let size = known_size.unwrap_or(data.size);
            return Ok(Box::new(Self {
                handle: data.handle,
                block_size,
                size,
                io_tracker,
            }) as Box<dyn Reader>);
        }

        // Cache miss - open file and get size
        let path_clone = path.clone();
        let local_path = to_local_path(path);

        let data = tokio::task::spawn_blocking(move || {
            let file = File::open(&local_path).map_err(|e| match e.kind() {
                ErrorKind::NotFound => Error::not_found(path_clone.to_string()),
                _ => e.into(),
            })?;

            // Get size from known_size or file metadata
            let size = match known_size {
                Some(s) => s,
                None => file.metadata()?.len() as usize,
            };

            Ok::<_, Error>(CachedReaderData {
                handle: Arc::new(UringFileHandle::new(file, path_clone)),
                size,
            })
        })
        .await??;

        // Insert into cache
        HANDLE_CACHE.insert(cache_key, data.clone()).await;

        // Return new reader instance
        Ok(Box::new(Self {
            handle: data.handle.clone(),
            block_size,
            size: data.size,
            io_tracker,
        }) as Box<dyn Reader>)
    }

    /// Submit a read request to the io_uring thread via channel and return a future.
    fn submit_read(
        &self,
        offset: u64,
        length: usize,
    ) -> Pin<Box<dyn Future<Output = object_store::Result<Bytes>> + Send>> {
        let mut buffer = BytesMut::with_capacity(length);
        unsafe {
            buffer.set_len(length);
        }

        // Create IoRequest with all data
        let request = Arc::new(IoRequest {
            fd: self.handle.fd,
            offset,
            length,
            thread_id: std::thread::current().id(),
            state: Mutex::new(RequestState {
                completed: false,
                waker: None,
                err: None,
                buffer,
                bytes_read: 0,
            }),
        });

        // Increment submitted counter before sending to channel
        SUBMITTED_COUNTER.fetch_add(1, Ordering::Relaxed);

        // Select thread in round-robin fashion
        let thread_idx =
            (THREAD_SELECTOR.fetch_add(1, Ordering::Relaxed) as usize) % URING_THREADS.len();

        // Send to selected thread via channel
        match URING_THREADS[thread_idx]
            .request_tx
            .send(Arc::clone(&request))
        {
            Ok(()) => {
                // Return future that will be woken when operation completes
                Box::pin(UringReadFuture { request })
            }
            Err(_) => {
                // Thread died - decrement counter and return error future
                SUBMITTED_COUNTER.fetch_sub(1, Ordering::Relaxed);
                Box::pin(async move {
                    Err(object_store::Error::Generic {
                        store: "UringReader",
                        source: Box::new(io::Error::new(
                            io::ErrorKind::BrokenPipe,
                            "io_uring thread died",
                        )),
                    })
                })
            }
        }
    }
}

impl Reader for UringReader {
    fn path(&self) -> &Path {
        &self.handle.path
    }

    fn block_size(&self) -> usize {
        self.block_size
    }

    fn io_parallelism(&self) -> usize {
        std::env::var("LANCE_URING_IO_PARALLELISM")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_URING_IO_PARALLELISM)
    }

    /// Returns the file size.
    fn size(&self) -> BoxFuture<'_, object_store::Result<usize>> {
        Box::pin(async move { Ok(self.size) })
    }

    /// Read a range of bytes using io_uring.
    #[instrument(level = "debug", skip(self))]
    fn get_range(&self, range: Range<usize>) -> BoxFuture<'static, object_store::Result<Bytes>> {
        let io_tracker = self.io_tracker.clone();
        let path = self.handle.path.clone();
        let num_bytes = range.len() as u64;
        let range_u64 = (range.start as u64)..(range.end as u64);

        let metrics = self.io_tracker.begin_io("get");
        self.submit_read(range.start as u64, range.len())
            .map(move |result| {
                metrics.record(&result, num_bytes);
                if result.is_ok() {
                    io_tracker.record_read("get_range", path, num_bytes, Some(range_u64));
                }
                result
            })
            .boxed()
    }

    /// Read the entire file using io_uring.
    #[instrument(level = "debug", skip(self))]
    fn get_all(&self) -> BoxFuture<'static, object_store::Result<Bytes>> {
        let size = self.size;
        let io_tracker = self.io_tracker.clone();
        let path = self.handle.path.clone();

        let metrics = self.io_tracker.begin_io("get");
        self.submit_read(0, size)
            .map(move |result| {
                let num_bytes = result.as_ref().map_or(0, |bytes| bytes.len() as u64);
                metrics.record(&result, num_bytes);
                if result.is_ok() {
                    io_tracker.record_read("get_all", path, num_bytes, None);
                }
                result
            })
            .boxed()
    }
}
