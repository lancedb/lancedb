// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! io_uring-based I/O for disks with high IOPS capacity (e.g. NVMe)
//!
//! This module provides two implementations of the [`Reader`](crate::traits::Reader) trait
//! using Linux's io_uring interface for asynchronous I/O.
//!
//! One of these uses a pool of dedicated background threads which each own an io_uring instance.
//! Read requests are submitted to a background thread's pool.
//!
//! The other implementation uses a thread-local io_uring instance.  This only works if the future
//! is polled by the same thread that submitted the request.  This means that the runtime must be
//! a single-threaded runtime.
//!
//! # Configuration
//!
//! The io_uring reader is enabled by using the `file+uring://` URI scheme instead of `file://`.
//! Additional tuning parameters are controlled by environment variables:
//!
//! - `LANCE_URING_CURRENT_THREAD` - Use thread-local io_uring (default: false)
//! - `LANCE_URING_BLOCK_SIZE` - Block size in bytes (default: 4KB)
//! - `LANCE_URING_IO_PARALLELISM` - Max concurrent operations (default: 128)
//! - `LANCE_URING_QUEUE_DEPTH` - io_uring queue depth (default: 16K)
//! - `LANCE_URING_THREAD_COUNT` - Number of io_uring threads to use (default: 2)
//! - `LANCE_URING_SUBMIT_BATCH_SIZE` - Number of requests to batch before submitting (default: 128)
//! - `LANCE_URING_POLL_TIMEOUT_MS` - Thread poll timeout in milliseconds (default: 10)
//!
//! Note: the block size and io parallelism are not actually used by the io_uring implementation.  These
//! variables just control what the filesystem reports up to Lance.
//!
//! # Platform Support
//!
//! This module is only available on Linux and requires kernel 5.1 or newer.
//! On other platforms, the code falls back to [`LocalObjectReader`](crate::local::LocalObjectReader).
//!
//! # Example
//!
//! ```no_run
//! # use lance_io::object_store::ObjectStore;
//! # async fn example() -> lance_core::Result<()> {
//! // Enable io_uring by using the file+uring:// scheme
//! let uri = "file+uring:///path/to/file.dat";
//! let (store, path) = ObjectStore::from_uri(uri).await?;
//! let reader = store.open(&path).await?;
//!
//! // Reader will use io_uring
//! let data = reader.get_range(0..1024).await?;
//! # Ok(())
//! # }
//! ```

mod future;
mod reader;
mod requests;
mod thread;

// Thread-local io_uring implementation for current-thread runtimes
pub(crate) mod current_thread;
pub(crate) mod current_thread_future;

#[cfg(test)]
mod tests;

use std::sync::LazyLock;

pub(crate) use current_thread::UringCurrentThreadReader;
pub use reader::UringReader;

/// Default block size for io_uring reads (4KB)
pub const DEFAULT_URING_BLOCK_SIZE: usize = 4 * 1024;

/// Default I/O parallelism for io_uring (128 concurrent operations)
pub const DEFAULT_URING_IO_PARALLELISM: usize = 128;

/// Default io_uring queue depth (16K entries)
pub const DEFAULT_URING_QUEUE_DEPTH: usize = 16 * 1024;

/// Cached `LANCE_URING_BLOCK_SIZE` env var, read once at first access.
pub(crate) static URING_BLOCK_SIZE: LazyLock<Option<usize>> = LazyLock::new(|| {
    std::env::var("LANCE_URING_BLOCK_SIZE")
        .ok()
        .and_then(|s| s.parse().ok())
});
