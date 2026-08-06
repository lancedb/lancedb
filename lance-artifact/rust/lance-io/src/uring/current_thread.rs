// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Thread-local io_uring implementation for current-thread runtimes.
//!
//! This implementation creates a thread-local IoUring instance per thread
//! and directly processes completions during future polling, eliminating
//! the need for background threads and MPSC channels.

use super::requests::{IoRequest, RequestState};
use super::{DEFAULT_URING_BLOCK_SIZE, DEFAULT_URING_IO_PARALLELISM, URING_BLOCK_SIZE};
use crate::local::to_local_path;
use crate::traits::Reader;
use crate::uring::DEFAULT_URING_QUEUE_DEPTH;
use crate::utils::tracking_store::IOTracker;
use bytes::{Bytes, BytesMut};
use futures::FutureExt;
use futures::future::BoxFuture;
use io_uring::{IoUring, opcode, types};
use lance_core::deepsize::DeepSizeOf;
use lance_core::{Error, Result};
use object_store::path::Path;

use std::cell::{LazyCell, RefCell};
use std::collections::HashMap;
use std::fs::File;
use std::future::Future;
use std::io::{self, ErrorKind};
use std::ops::Range;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tracing::instrument;

// Re-use file handle types from reader.rs
use super::reader::{CacheKey, CachedReaderData, HANDLE_CACHE, UringFileHandle};

/// Global counter for generating unique user_data values
static USER_DATA_COUNTER: AtomicU64 = AtomicU64::new(1);

/// Thread-local io_uring instance with pending requests
struct ThreadLocalUring {
    ring: IoUring,
    pending: HashMap<u64, Arc<IoRequest>>,
}

thread_local! {
    static URING: LazyCell<RefCell<ThreadLocalUring>> = LazyCell::new(|| {
        let queue_depth = std::env::var("LANCE_URING_QUEUE_DEPTH")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_URING_QUEUE_DEPTH);

        let ring = IoUring::builder()
            // Ensures work is only done in submit_and_wait
            .setup_defer_taskrun()
            // Enable perf. optimization when there is only one issuer thread
            .setup_single_issuer()
            .build(queue_depth as u32)
            .expect("Failed to create io_uring");

        log::debug!(
            "Created thread-local io_uring with queue depth {}",
            queue_depth
        );

        RefCell::new(ThreadLocalUring {
            ring,
            pending: HashMap::new(),
        })
    });
}

/// Push request to thread-local submission queue
pub(super) fn push_request(request: Arc<IoRequest>) -> io::Result<()> {
    URING.with(|cell| {
        let mut uring = cell.borrow_mut();

        // Generate unique user_data
        let user_data = USER_DATA_COUNTER.fetch_add(1, Ordering::Relaxed);

        // Get buffer pointer, adjusting for any bytes already read (short read retry)
        let (buffer_ptr, read_offset, read_length) = {
            let state = request.state.lock().unwrap();
            let br = state.bytes_read;
            (
                unsafe { state.buffer.as_ptr().add(br) as *mut u8 },
                request.offset + br as u64,
                (request.length - br) as u32,
            )
        };

        // Prepare read operation
        let read_op =
            opcode::Read::new(types::Fd(request.fd), buffer_ptr, read_length).offset(read_offset);

        // Get submission queue
        let mut sq = uring.ring.submission();

        // Check if SQ has space
        if sq.is_full() {
            drop(sq);
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "io_uring submission queue full",
            ));
        }

        // Push to SQ
        unsafe {
            sq.push(&read_op.build().user_data(user_data))
                .map_err(|_| io::Error::other("Failed to push to SQ"))?;
        }
        drop(sq);

        // Track request in pending map
        uring.pending.insert(user_data, request);

        // Don't submit here - let the future handle submission

        Ok(())
    })
}

/// Process completions from thread-local IoUring
pub(super) fn process_thread_local_completions() -> io::Result<usize> {
    URING.with(|cell| {
        let mut uring = cell.borrow_mut();
        let mut completed = 0;
        let mut retries: Vec<Arc<IoRequest>> = Vec::new();

        // Collect completions first to avoid borrowing ring and pending simultaneously
        let cqes: Vec<_> = uring
            .ring
            .completion()
            .map(|cqe| (cqe.user_data(), cqe.result()))
            .collect();

        for (user_data, result) in cqes {
            if let Some(request) = uring.pending.remove(&user_data) {
                let mut state = request.state.lock().unwrap();

                if result < 0 {
                    // Kernel error
                    state.err = Some(io::Error::from_raw_os_error(-result));
                    state.completed = true;
                } else if result == 0 {
                    // EOF before full read completed
                    let br = state.bytes_read;
                    state.err = Some(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        format!("unexpected EOF: read {} of {} bytes", br, request.length),
                    ));
                    state.buffer.truncate(br);
                    state.completed = true;
                } else {
                    // Positive result: n bytes read
                    let n = result as usize;
                    state.bytes_read += n;
                    let br = state.bytes_read;

                    if br >= request.length {
                        // Full read complete
                        state.buffer.truncate(br);
                        state.completed = true;
                    } else {
                        // Short read — need retry; don't mark completed or wake
                        drop(state);
                        retries.push(request);

                        continue;
                    }
                }

                // Wake waiting future
                if let Some(waker) = state.waker.take() {
                    drop(state);
                    waker.wake();
                }

                completed += 1;
            } else {
                log::warn!("Received completion for unknown user_data: {}", user_data);
            }
        }

        // Resubmit short-read retries
        for request in retries {
            // Generate unique user_data
            let user_data = USER_DATA_COUNTER.fetch_add(1, Ordering::Relaxed);

            let (buffer_ptr, read_offset, read_length) = {
                let state = request.state.lock().unwrap();
                let br = state.bytes_read;
                (
                    unsafe { state.buffer.as_ptr().add(br) as *mut u8 },
                    request.offset + br as u64,
                    (request.length - br) as u32,
                )
            };

            let read_op = opcode::Read::new(types::Fd(request.fd), buffer_ptr, read_length)
                .offset(read_offset);

            let mut sq = uring.ring.submission();
            if sq.is_full() {
                drop(sq);
                request.fail(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "io_uring submission queue full during retry",
                ));
                continue;
            }

            unsafe {
                if sq.push(&read_op.build().user_data(user_data)).is_err() {
                    request.fail(io::Error::other("Failed to push short-read retry to SQ"));
                    continue;
                }
            }
            drop(sq);

            uring.pending.insert(user_data, request);
        }

        if completed > 0 {
            log::trace!("Processed {} completions", completed);
        }

        Ok(completed)
    })
}

/// Submit all pending requests and wait with timeout 0 (non-blocking)
pub(super) fn submit_and_wait_thread_local() -> io::Result<()> {
    URING.with(|cell| {
        let uring = cell.borrow_mut();
        // Submit with wait=1 (do at least some work)
        uring.ring.submit_and_wait(1)?;
        Ok(())
    })
}

/// Thread-local io_uring-based reader for current-thread runtimes
#[derive(Debug)]
pub struct UringCurrentThreadReader {
    /// File handle
    handle: Arc<UringFileHandle>,

    /// Block size for I/O operations
    block_size: usize,

    /// File size (determined at open time)
    size: usize,

    /// I/O tracker for monitoring operations
    io_tracker: Arc<IOTracker>,
}

impl DeepSizeOf for UringCurrentThreadReader {
    fn deep_size_of_children(&self, context: &mut lance_core::deepsize::Context) -> usize {
        // Skip file handle (just a system resource)
        // Only count the path's deep size
        self.handle.path.as_ref().deep_size_of_children(context)
    }
}

impl UringCurrentThreadReader {
    /// Open a file with thread-local io_uring
    ///
    /// This reuses the file handle caching infrastructure from UringReader
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

    /// Submit a read request and return a future
    fn submit_read(
        &self,
        offset: u64,
        length: usize,
    ) -> Pin<Box<dyn Future<Output = object_store::Result<Bytes>> + Send>> {
        let mut buffer = BytesMut::with_capacity(length);
        unsafe {
            buffer.set_len(length);
        }

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

        match push_request(request.clone()) {
            Ok(()) => Box::pin(super::current_thread_future::UringCurrentThreadFuture::new(
                request,
            )),
            Err(e) => Box::pin(async move {
                Err(object_store::Error::Generic {
                    store: "io_uring_ct",
                    source: Box::new(e),
                })
            }),
        }
    }
}

impl Reader for UringCurrentThreadReader {
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

    /// Returns the file size
    fn size(&self) -> BoxFuture<'_, object_store::Result<usize>> {
        Box::pin(async move { Ok(self.size) })
    }

    /// Read a range of bytes using thread-local io_uring
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

    /// Read the entire file using thread-local io_uring
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
