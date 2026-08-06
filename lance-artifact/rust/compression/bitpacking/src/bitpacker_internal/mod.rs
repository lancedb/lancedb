// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

// Lance-owned u32 SIMD bitpacking kernels.
//
// This is adapted from the MIT-licensed `bitpacking` crate so Lance can keep the
// hot FTS posting-list bitpacking implementation inside lance-bitpacking while
// preserving byte compatibility with the existing 4x format.

#![allow(dead_code)]
#![allow(unsafe_op_in_unsafe_fn)]
#![allow(clippy::redundant_pub_crate)]
#![allow(clippy::upper_case_acronyms)]
#![allow(clippy::use_self)]

#[macro_use]
mod macros;

mod bitpacker4x;
mod bitpacker8x;

pub use bitpacker4x::BitPacker4x;
pub use bitpacker8x::BitPacker8x;

pub(crate) trait Available {
    fn available() -> bool;
}

pub(crate) trait UnsafeBitPacker {
    const BLOCK_LEN: usize;

    unsafe fn compress(decompressed: &[u32], compressed: &mut [u8], num_bits: u8) -> usize;

    unsafe fn compress_sorted(
        initial: u32,
        decompressed: &[u32],
        compressed: &mut [u8],
        num_bits: u8,
    ) -> usize;

    unsafe fn compress_strictly_sorted(
        initial: Option<u32>,
        decompressed: &[u32],
        compressed: &mut [u8],
        num_bits: u8,
    ) -> usize;

    unsafe fn decompress(compressed: &[u8], decompressed: &mut [u32], num_bits: u8) -> usize;

    unsafe fn decompress_sorted(
        initial: u32,
        compressed: &[u8],
        decompressed: &mut [u32],
        num_bits: u8,
    ) -> usize;

    unsafe fn decompress_strictly_sorted(
        initial: Option<u32>,
        compressed: &[u8],
        decompressed: &mut [u32],
        num_bits: u8,
    ) -> usize;

    unsafe fn num_bits(decompressed: &[u32]) -> u8;

    unsafe fn num_bits_sorted(initial: u32, decompressed: &[u32]) -> u8;

    unsafe fn num_bits_strictly_sorted(initial: Option<u32>, decompressed: &[u32]) -> u8;
}

/// Block bitpacker for fixed-size `u32` blocks.
///
/// Implementations own runtime SIMD dispatch and use caller-provided buffers.
/// Packed bytes are stable for a given implementation and bit width.
pub trait BitPacker: Sized + Clone + Copy {
    /// Number of `u32` values in one physical block.
    const BLOCK_LEN: usize;

    /// Select the best supported implementation for the current CPU.
    ///
    /// Lance uses SIMD backends when available and falls back to a scalar
    /// backend otherwise, matching the existing allocation-free call shape used
    /// by the upstream `bitpacking` crate.
    fn new() -> Self;

    /// Compress one full block of raw values into `compressed`.
    fn compress(&self, decompressed: &[u32], compressed: &mut [u8], num_bits: u8) -> usize;

    /// Delta-compress one full non-decreasing block into `compressed`.
    fn compress_sorted(
        &self,
        initial: u32,
        decompressed: &[u32],
        compressed: &mut [u8],
        num_bits: u8,
    ) -> usize;

    /// Delta-compress one full strictly increasing block into `compressed`.
    fn compress_strictly_sorted(
        &self,
        initial: Option<u32>,
        decompressed: &[u32],
        compressed: &mut [u8],
        num_bits: u8,
    ) -> usize;

    /// Decompress one raw block into `decompressed`.
    fn decompress(&self, compressed: &[u8], decompressed: &mut [u32], num_bits: u8) -> usize;

    /// Decompress one delta-compressed non-decreasing block.
    fn decompress_sorted(
        &self,
        initial: u32,
        compressed: &[u8],
        decompressed: &mut [u32],
        num_bits: u8,
    ) -> usize;

    /// Decompress one delta-compressed strictly increasing block.
    fn decompress_strictly_sorted(
        &self,
        initial: Option<u32>,
        compressed: &[u8],
        decompressed: &mut [u32],
        num_bits: u8,
    ) -> usize;

    /// Return the minimum bit width needed to represent a full raw block.
    fn num_bits(&self, decompressed: &[u32]) -> u8;

    /// Return the minimum bit width needed to represent deltas in a full block.
    fn num_bits_sorted(&self, initial: u32, decompressed: &[u32]) -> u8;

    /// Return the minimum bit width needed to represent strict deltas in a full block.
    fn num_bits_strictly_sorted(&self, initial: Option<u32>, decompressed: &[u32]) -> u8;

    /// Return the byte size of one compressed block at `num_bits`.
    #[must_use]
    fn compressed_block_size(num_bits: u8) -> usize {
        Self::BLOCK_LEN * num_bits as usize / 8
    }
}

#[inline]
fn most_significant_bit(value: u32) -> u8 {
    (u32::BITS - value.leading_zeros()) as u8
}
