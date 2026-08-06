// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Routines for encoding and decoding miniblock data
//!
//! Miniblock encoding is one of the two structural encodings in Lance 2.1.
//! In this approach the data is compressed into a series of chunks put into
//! a single buffer.
//!
//! A chunk must be encoded or decoded as a unit.  There is a small amount of
//! chunk metadata such as the number and size of each buffer in the chunk.
//!
//! Any form of compression can be used since we are compressing and decompressing
//! entire chunks.
use crate::{buffer::LanceBuffer, data::DataBlock, format::pb21::CompressiveEncoding};

use lance_core::Result;

pub const MAX_MINIBLOCK_BYTES: u64 = 8 * 1024 - 6;

const DEFAULT_MAX_MINIBLOCK_VALUES: u64 = 4096;
/// Maximum number of values that any mini-block decoder accepts from page metadata.
pub(crate) const MAX_CONFIGURABLE_MINIBLOCK_VALUES: u64 = 32768;

fn parse_max_miniblock_values() -> u64 {
    let val = std::env::var("LANCE_MINIBLOCK_MAX_VALUES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_MAX_MINIBLOCK_VALUES);
    val.clamp(1, MAX_CONFIGURABLE_MINIBLOCK_VALUES)
}

pub static MAX_MINIBLOCK_VALUES: std::sync::LazyLock<u64> =
    std::sync::LazyLock::new(parse_max_miniblock_values);

/// Maximum number of rep/def levels the structural planner should place into
/// a single mini-block chunk.
pub fn max_repdef_levels_per_chunk(bits_per_level: u64) -> u64 {
    debug_assert!(bits_per_level > 0);
    const REPDEF_BUDGET_BITS: u64 = 16 * 1024 * 8;
    let budgeted_levels = REPDEF_BUDGET_BITS / bits_per_level;
    budgeted_levels.min(u16::MAX as u64)
}

/// Page data that has been compressed into a series of chunks put into
/// a single buffer.
#[derive(Debug)]
pub struct MiniBlockCompressed {
    /// The buffers of compressed data
    pub data: Vec<LanceBuffer>,
    /// Describes the size of each chunk
    pub chunks: Vec<MiniBlockChunk>,
    /// The number of values in the entire page
    pub num_values: u64,
}

/// Per-page framing details that can affect a mini-block compressor's choice.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MiniBlockCompressionContext {
    common_chunk_buffers: u64,
    support_large_chunk: bool,
    allow_generic_offsets: bool,
}

impl MiniBlockCompressionContext {
    /// Creates the framing context supplied by the owning mini-block page.
    pub fn new(
        common_chunk_buffers: u64,
        support_large_chunk: bool,
        allow_generic_offsets: bool,
    ) -> Self {
        Self {
            common_chunk_buffers,
            support_large_chunk,
            allow_generic_offsets,
        }
    }
}

/// Describes the size of a mini-block chunk of data
///
/// Mini-block chunks are designed to be small (just a few disk sectors)
/// and contain a power-of-two number of values (except for the last chunk)
///
/// By default we limit a chunk to 4Ki values and slightly less than
/// 8KiB of compressed value data.  The byte budget remains the primary
/// constraint, so only encodings that compress many values into that
/// budget can use larger value counts when explicitly configured.
///
/// The maximum number of values per chunk can be configured via the
/// `LANCE_MINIBLOCK_MAX_VALUES` environment variable.  This is only
/// useful in extremely bandwidth-limited environments; the default is
/// appropriate for local disks and same-region cloud object storage.
#[derive(Debug)]
pub struct MiniBlockChunk {
    // The size in bytes of each buffer in the chunk.
    //
    // In Lance 2.1, the chunk size is limited to 32KiB, so only 16-bits are used.
    // Since Lance 2.2, the chunk size uses u32 to support larger chunk size
    pub buffer_sizes: Vec<u32>,
    // The log (base 2) of the number of values in the chunk.  If this is the final chunk
    // then this should be 0 (the number of values will be calculated by subtracting the
    // size of all other chunks from the total size of the page)
    //
    // For example, 1 would mean there are 2 values in the chunk and 15 would mean there
    // are 32Ki values in the chunk.
    //
    // This must be <= log2(MAX_MINIBLOCK_VALUES) (i.e. <= 12 at the default of 4096)
    pub log_num_values: u8,
}

impl MiniBlockChunk {
    /// Gets the number of values in this block
    ///
    /// This requires `vals_in_prev_blocks` and `total_num_values` because the
    /// last block in a page is a special case which stores 0 for log_num_values
    /// and, in that case, the number of values is determined by subtracting
    /// `vals_in_prev_blocks` from `total_num_values`
    pub fn num_values(&self, vals_in_prev_blocks: u64, total_num_values: u64) -> u64 {
        if self.log_num_values == 0 {
            total_num_values - vals_in_prev_blocks
        } else {
            1 << self.log_num_values
        }
    }
}

/// Trait for compression algorithms that are suitable for use in the miniblock structural encoding
///
/// These compression algorithms should be capable of encoding the data into small chunks
/// where each chunk (except the last) has 2^N values (N can vary between chunks)
pub trait MiniBlockCompressor: std::fmt::Debug + Send + Sync {
    /// Compress a `page` of data into multiple chunks
    ///
    /// See [`MiniBlockCompressed`] for details on how chunks should be sized.
    ///
    /// This method also returns a description of the encoding applied that will be
    /// used at decode time to read the data.
    fn compress(
        &self,
        context: MiniBlockCompressionContext,
        page: DataBlock,
    ) -> Result<(MiniBlockCompressed, CompressiveEncoding)>;
}

#[cfg(test)]
mod tests {
    use serial_test::serial;

    use super::*;

    #[test]
    #[serial]
    fn test_parse_default() {
        unsafe { std::env::remove_var("LANCE_MINIBLOCK_MAX_VALUES") };
        assert_eq!(parse_max_miniblock_values(), 4096);
    }

    #[test]
    #[serial]
    fn test_parse_custom_value() {
        unsafe { std::env::set_var("LANCE_MINIBLOCK_MAX_VALUES", "256") };
        assert_eq!(parse_max_miniblock_values(), 256);
        unsafe { std::env::remove_var("LANCE_MINIBLOCK_MAX_VALUES") };
    }

    #[test]
    #[serial]
    fn test_parse_can_raise_to_32k() {
        unsafe { std::env::set_var("LANCE_MINIBLOCK_MAX_VALUES", "32768") };
        assert_eq!(parse_max_miniblock_values(), 32768);
        unsafe { std::env::remove_var("LANCE_MINIBLOCK_MAX_VALUES") };
    }

    #[test]
    #[serial]
    fn test_parse_clamps_zero_to_one() {
        unsafe { std::env::set_var("LANCE_MINIBLOCK_MAX_VALUES", "0") };
        assert_eq!(parse_max_miniblock_values(), 1);
        unsafe { std::env::remove_var("LANCE_MINIBLOCK_MAX_VALUES") };
    }

    #[test]
    #[serial]
    fn test_parse_clamps_above_max() {
        unsafe { std::env::set_var("LANCE_MINIBLOCK_MAX_VALUES", "99999") };
        assert_eq!(
            parse_max_miniblock_values(),
            MAX_CONFIGURABLE_MINIBLOCK_VALUES
        );
        unsafe { std::env::remove_var("LANCE_MINIBLOCK_MAX_VALUES") };
    }

    #[test]
    #[serial]
    fn test_parse_invalid_falls_back_to_default() {
        unsafe { std::env::set_var("LANCE_MINIBLOCK_MAX_VALUES", "not_a_number") };
        assert_eq!(parse_max_miniblock_values(), DEFAULT_MAX_MINIBLOCK_VALUES);
        unsafe { std::env::remove_var("LANCE_MINIBLOCK_MAX_VALUES") };
    }
}
