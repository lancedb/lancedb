// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::sync::Arc;

use futures::{FutureExt, future::BoxFuture};
use lance_encoding::EncodingsIo;
use lance_io::scheduler::FileScheduler;

use super::reader::DEFAULT_READ_CHUNK_SIZE;

#[derive(Debug)]
pub struct LanceEncodingsIo {
    scheduler: FileScheduler,
    /// Size of chunks when reading large pages
    read_chunk_size: u64,
}

impl LanceEncodingsIo {
    pub fn new(scheduler: FileScheduler) -> Self {
        Self {
            scheduler,
            read_chunk_size: DEFAULT_READ_CHUNK_SIZE,
        }
    }

    pub fn with_read_chunk_size(mut self, read_chunk_size: u64) -> Self {
        self.read_chunk_size = read_chunk_size;
        self
    }
}

impl EncodingsIo for LanceEncodingsIo {
    fn with_bypass_backpressure(&self) -> Option<Arc<dyn EncodingsIo>> {
        Some(Arc::new(Self {
            scheduler: self.scheduler.with_bypass_backpressure(),
            read_chunk_size: self.read_chunk_size,
        }))
    }

    fn with_io_stats(
        &self,
        stats: Arc<dyn lance_core::utils::io_stats::IoStatsRecorder>,
    ) -> Option<Arc<dyn EncodingsIo>> {
        Some(Arc::new(Self {
            scheduler: self.scheduler.with_io_stats(stats),
            read_chunk_size: self.read_chunk_size,
        }))
    }

    fn submit_request(
        &self,
        ranges: Vec<std::ops::Range<u64>>,
        priority: u64,
    ) -> BoxFuture<'static, lance_core::Result<Vec<bytes::Bytes>>> {
        let mut split_ranges = Vec::new();
        let mut split_indices = Vec::new(); // Track which original range each split came from
        // Large ranges (above read_chunk_size) will be split into
        // multiple reads.  Empty ranges will skip the I/O layer
        // entirely.  If we have either of these we will need to
        // reassemble our results, inserting empties and merging parts
        let mut needs_reassembly = false;

        // Split large ranges into smaller chunks
        //
        // TODO: consider read_chunk_size before submitting requests.
        for (idx, range) in ranges.iter().enumerate() {
            if range.start == range.end {
                // EncodingsIo requires one result per input range. Zero-length
                // ranges schedule no I/O, so their empty results are restored
                // after the non-empty requests complete.
                needs_reassembly = true;
                continue;
            }
            let range_size = range.end - range.start;

            if range_size > self.read_chunk_size {
                needs_reassembly = true;
                let num_chunks = range_size.div_ceil(self.read_chunk_size);
                let chunk_size = range_size / num_chunks;

                for i in 0..num_chunks {
                    let start = range.start + i * chunk_size;
                    let end = if i == num_chunks - 1 {
                        range.end // Last chunk gets any remaining bytes
                    } else {
                        start + chunk_size
                    };
                    split_ranges.push(start..end);
                    split_indices.push(idx);
                }
            } else {
                split_ranges.push(range.clone());
                split_indices.push(idx);
            }
        }

        let fut = self.scheduler.submit_request(split_ranges, priority);

        async move {
            let split_results = fut.await?;

            if split_results.len() != split_indices.len() {
                return Err(lance_core::Error::internal(format!(
                    "Encoding I/O returned {} results for {} requested range chunks",
                    split_results.len(),
                    split_indices.len()
                )));
            }
            if !needs_reassembly {
                return Ok(split_results);
            }

            let mut results = vec![Vec::new(); ranges.len()];

            for (split_result, orig_idx) in split_results.into_iter().zip(split_indices) {
                results[orig_idx].push(split_result);
            }

            let mut reassembled = Vec::with_capacity(ranges.len());
            for (range, chunks) in ranges.iter().zip(results) {
                if chunks.is_empty() {
                    if range.start == range.end {
                        reassembled.push(bytes::Bytes::new());
                        continue;
                    }
                    return Err(lance_core::Error::internal(format!(
                        "Encoding I/O returned no data for non-empty range {}..{}",
                        range.start, range.end
                    )));
                }
                if chunks.len() == 1 {
                    reassembled.push(chunks[0].clone());
                    continue;
                }

                let total_size: usize = chunks.iter().map(|c| c.len()).sum();
                let mut combined = Vec::with_capacity(total_size);
                for chunk in chunks {
                    combined.extend_from_slice(&chunk);
                }
                reassembled.push(bytes::Bytes::from(combined));
            }
            Ok(reassembled)
        }
        .boxed()
    }
}
