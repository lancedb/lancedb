// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::{ops::Range, sync::Arc};

use bytes::Bytes;
use futures::{FutureExt, TryFutureExt, future::BoxFuture};

use lance_core::Result;

mod array_encoding;
pub mod buffer;
pub mod compression;
pub mod compression_config;
pub mod constants;
pub mod data;
pub mod decoder;
pub mod encoder;
pub mod encodings;
pub mod format;
pub mod repdef;
pub mod statistics;
#[cfg(test)]
pub mod testing;
pub mod utils;

// We can definitely add support for big-endian machines someday.  However, it's not a priority and
// would involve extensive testing (probably through emulation) to ensure that the encodings are
// correct.
#[cfg(not(target_endian = "little"))]
compile_error!("Lance encodings only support little-endian systems.");

/// A trait for an I/O service
///
/// This represents the I/O API that the encoders and decoders need in order to operate.
/// We specify this as a trait so that lance-encodings does not need to depend on lance-io
///
/// In general, it is assumed that this trait will be implemented by some kind of "file reader"
/// or "file scheduler".  The encodings here are all limited to accessing a single file.
pub trait EncodingsIo: std::fmt::Debug + Send + Sync {
    /// Submit an I/O request
    ///
    /// The response must contain a `Bytes` object for each range requested even if the underlying
    /// I/O was coalesced into fewer actual requests.
    ///
    /// # Arguments
    ///
    /// * `ranges` - the byte ranges to request
    /// * `priority` - the priority of the request
    ///
    /// Priority should be set to the lowest row number that this request is delivering data for.
    /// This is important in cases where indirect I/O causes high priority requests to be submitted
    /// after low priority requests.  We want to fulfill the indirect I/O more quickly so that we
    /// can decode as quickly as possible.
    ///
    /// The implementation should be able to handle empty ranges, and should return an empty
    /// byte buffer for each empty range.
    fn submit_request(
        &self,
        range: Vec<Range<u64>>,
        priority: u64,
    ) -> BoxFuture<'static, Result<Vec<Bytes>>>;

    /// Submit an I/O request with a single range
    ///
    /// This is just a utitliy function that wraps [`EncodingsIo::submit_request`] for the common
    /// case of a single range request.
    fn submit_single(
        &self,
        range: std::ops::Range<u64>,
        priority: u64,
    ) -> BoxFuture<'static, lance_core::Result<bytes::Bytes>> {
        self.submit_request(vec![range], priority)
            .map_ok(|mut v| v.pop().unwrap())
            .boxed()
    }

    /// Returns a version of this I/O service that bypasses backpressure for all requests.
    ///
    /// This is intended for indirect I/O (e.g. fetching items after decoding offsets) where
    /// blocking on backpressure could cause deadlocks or excessive latency.
    ///
    /// Returns `None` if this implementation does not support bypass (e.g. in-memory or test
    /// schedulers), in which case the caller should fall back to using self.
    fn with_bypass_backpressure(&self) -> Option<Arc<dyn EncodingsIo>> {
        None
    }

    /// Returns a version of this I/O service that additionally records the I/O it
    /// performs into `stats`, on top of any global accounting.  This is the seam
    /// used to measure exact per-scope (e.g. per-query) I/O without re-opening
    /// files: wrap a reader's I/O service, perform the reads, then inspect the
    /// recorder.
    ///
    /// Returns `None` if this implementation does not support per-scope I/O
    /// statistics (e.g. in-memory or test schedulers), in which case the caller
    /// should fall back to using self (and no statistics are recorded).
    fn with_io_stats(
        &self,
        _stats: Arc<dyn lance_core::utils::io_stats::IoStatsRecorder>,
    ) -> Option<Arc<dyn EncodingsIo>> {
        None
    }
}

/// An implementation of EncodingsIo that serves data from an in-memory buffer
#[derive(Debug)]
pub struct BufferScheduler {
    data: Bytes,
}

impl BufferScheduler {
    pub fn new(data: Bytes) -> Self {
        Self { data }
    }

    fn satisfy_request(&self, req: Range<u64>) -> Bytes {
        self.data.slice(req.start as usize..req.end as usize)
    }
}

impl EncodingsIo for BufferScheduler {
    fn submit_request(
        &self,
        ranges: Vec<Range<u64>>,
        _priority: u64,
    ) -> BoxFuture<'static, Result<Vec<Bytes>>> {
        std::future::ready(Ok(ranges
            .into_iter()
            .map(|range| self.satisfy_request(range))
            .collect::<Vec<_>>()))
        .boxed()
    }
}
