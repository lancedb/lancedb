// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Split Block Bloom Filter (SBBF) implementation for Lance
//!
//! Based on the Apache Arrow Parquet SBBF implementation but with public APIs
//! for use in Lance indexing. This implementation follows the Parquet spec
//! <https://github.com/apache/arrow-rs/blob/main/parquet/src/bloom_filter/mod.rs>
//! for SBBF as described in <https://github.com/apache/parquet-format/blob/master/BloomFilter.md>
//! FIXME: Make the upstream SBBF implementation public so that this file could be
//! removed from Lance.
//! <https://github.com/apache/arrow-rs/issues/8277>

use super::as_bytes::AsBytes;
use libm::lgamma;
use std::error::Error;
use std::fmt;
use std::io::Write;
use twox_hash::XxHash64;

#[derive(Debug)]
pub enum SbbfError {
    InvalidFpp { fpp: f64 },
    WriteError { source: std::io::Error },
    InvalidData { message: String },
}

impl fmt::Display for SbbfError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidFpp { fpp } => {
                write!(
                    f,
                    "False positive probability must be between 0.0 and 1.0, got {}",
                    fpp
                )
            }
            Self::WriteError { source } => {
                write!(f, "Failed to write bloom filter: {}", source)
            }
            Self::InvalidData { message } => {
                write!(f, "Invalid bloom filter data: {}", message)
            }
        }
    }
}

impl Error for SbbfError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::WriteError { source } => Some(source),
            _ => None,
        }
    }
}

pub type Result<T> = std::result::Result<T, SbbfError>;

/// Salt as defined in the Parquet spec
const SALT: [u32; 8] = [
    0x47b6137b_u32,
    0x44974d91_u32,
    0x8824ad5b_u32,
    0xa2b7289d_u32,
    0x705495c7_u32,
    0x2df1424b_u32,
    0x9efc4947_u32,
    0x5c6bfb31_u32,
];

/// Each block is 256 bits, broken up into eight contiguous "words", each consisting of 32 bits.
/// Each word is thought of as an array of bits; each bit is either "set" or "not set".
#[derive(Debug, Copy, Clone)]
struct Block([u32; 8]);

impl Block {
    const ZERO: Self = Self([0; 8]);

    /// Takes as its argument a single unsigned 32-bit integer and returns a block in which each
    /// word has exactly one bit set.
    fn mask(x: u32) -> Self {
        let mut result = [0_u32; 8];
        for i in 0..8 {
            // wrapping instead of checking for overflow
            let y = x.wrapping_mul(SALT[i]);
            let y = y >> 27;
            result[i] = 1 << y;
        }
        Self(result)
    }

    #[inline]
    #[cfg(target_endian = "little")]
    fn to_le_bytes(self) -> [u8; 32] {
        self.to_ne_bytes()
    }

    #[inline]
    #[cfg(not(target_endian = "little"))]
    fn to_le_bytes(self) -> [u8; 32] {
        self.swap_bytes().to_ne_bytes()
    }

    #[inline]
    fn to_ne_bytes(self) -> [u8; 32] {
        // SAFETY: [u32; 8] and [u8; 32] have the same size and neither has invalid bit patterns.
        unsafe { std::mem::transmute(self.0) }
    }

    #[inline]
    #[cfg(not(target_endian = "little"))]
    fn swap_bytes(mut self) -> Self {
        self.0.iter_mut().for_each(|x| *x = x.swap_bytes());
        self
    }

    /// Setting every bit in the block that was also set in the result from mask
    fn insert(&mut self, hash: u32) {
        let mask = Self::mask(hash);
        for i in 0..8 {
            self[i] |= mask[i];
        }
    }

    /// Returns true when every bit that is set in the result of mask is also set in the block.
    fn check(&self, hash: u32) -> bool {
        let mask = Self::mask(hash);
        for i in 0..8 {
            if self[i] & mask[i] == 0 {
                return false;
            }
        }
        true
    }
}

impl std::ops::Index<usize> for Block {
    type Output = u32;

    #[inline]
    fn index(&self, index: usize) -> &Self::Output {
        self.0.index(index)
    }
}

impl std::ops::IndexMut<usize> for Block {
    #[inline]
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        self.0.index_mut(index)
    }
}

// This implements the false positive probability in Putze et al.'s "Cache-, hash-and
// space-efficient bloom filters", equation 3.
#[inline]
fn false_positive_probability(ndv: u64, log_space_bytes: u8) -> f64 {
    const WORD_BITS: f64 = 32.0;
    const BUCKET_WORDS: f64 = 8.0;
    let bytes = (1u64 << log_space_bytes) as f64;
    let ndv = ndv as f64;
    if ndv == 0.0 {
        return 0.0;
    }
    // This short-cuts a slowly-converging sum for very dense filters
    if ndv / (bytes * u8::BITS as f64) > 2.0 {
        return 1.0;
    }
    let mut result: f64 = 0.0;
    // lam is the usual parameter to the Poisson's PMF. Following the notation in the paper,
    // lam is B/c, where B is the number of bits in a bucket and c is the number of bits per
    // distinct value
    let lam = BUCKET_WORDS * WORD_BITS / ((bytes * u8::BITS as f64) / ndv);
    // Some of the calculations are done in log-space to increase numerical stability
    let loglam = lam.ln();

    // 750 iterations are sufficient to cause the sum to converge in all of the tests. In
    // other words, setting the iterations higher than 750 will give the same result as
    // leaving it at 750.
    const ITERS: i32 = 750;
    // We start with the highest value of i, since the values we're adding to result are
    // mostly smaller at high i, and this increases accuracy to sum from the smallest
    // values up.
    for i in (0..ITERS).rev() {
        // The PMF of the Poisson distribution is lam^i * exp(-lam) / i!. In logspace, using
        // lgamma for the log of the factorial function:
        let logp = i as f64 * loglam - lam - lgamma((i + 1).into());
        // The f_inner part of the equation in the paper is the probability of a single
        // collision in the bucket. Since there are kBucketWords non-overlapping lanes in each
        // bucket, the log of this probability is:
        let logfinner = BUCKET_WORDS * (1.0 - (1.0 - 1.0 / WORD_BITS).powi(i)).ln();
        // Here we are forced out of log-space calculations
        result += (logp + logfinner).exp();
    }
    result.min(1.0)
}

/// Minimum and maximum filter sizes
const BITSET_LOG2_MIN_BYTES: u8 = 5; // 32B (1 Block)
const BITSET_LOG2_MAX_BYTES: u8 = 27; // 128MiB

#[inline]
fn min_log2_bytes(ndv: u64, fpp: f64) -> u8 {
    let mut low = 0;
    let mut high = 64;
    while high > low + 1 {
        let mid = (high + low) / 2;
        let candidate = false_positive_probability(ndv, mid);
        if candidate <= fpp {
            high = mid;
        } else {
            low = mid;
        }
    }
    high
}

/// A Split Block Bloom Filter (SBBF) implementation
///
/// This is a high-performance bloom filter optimized for SIMD operations,
/// compatible with the Parquet specification.
#[derive(Debug, Clone)]
pub struct Sbbf {
    blocks: Vec<Block>,
}

impl Sbbf {
    /// Create a new SBBF from raw bitset data
    pub fn new(bitset: &[u8]) -> Result<Self> {
        if !bitset.len().is_multiple_of(32) {
            return Err(SbbfError::InvalidData {
                message: format!(
                    "Bitset length must be a multiple of 32, got {}",
                    bitset.len()
                ),
            });
        }

        let data = bitset
            .chunks_exact(4 * 8)
            .map(|chunk| {
                let mut block = Block::ZERO;
                for (i, word) in chunk.chunks_exact(4).enumerate() {
                    block[i] = u32::from_le_bytes(word.try_into().unwrap());
                }
                block
            })
            .collect::<Vec<Block>>();

        Ok(Self { blocks: data })
    }

    /// Create a new empty SBBF with the given number of bytes
    /// The actual size will be adjusted to the next power of two within bounds
    pub fn with_log2_num_bytes(log2_num_bytes: u8) -> Self {
        let num_bytes =
            1_usize << log2_num_bytes.clamp(BITSET_LOG2_MIN_BYTES, BITSET_LOG2_MAX_BYTES);
        let bitset = vec![0_u8; num_bytes];
        // unwrap is safe because we know the size is valid
        Self::new(&bitset).unwrap()
    }

    /// Create a new SBBF with given number of distinct values and false positive probability
    pub fn with_ndv_fpp(ndv: u64, fpp: f64) -> Result<Self> {
        if !(0.0..1.0).contains(&fpp) {
            return Err(SbbfError::InvalidFpp { fpp });
        }
        let log2_num_bytes = min_log2_bytes(ndv, fpp);
        Ok(Self::with_log2_num_bytes(log2_num_bytes))
    }

    /// Get the hash-to-block-index for a given hash
    #[inline]
    fn hash_to_block_index(&self, hash: u64) -> usize {
        (((hash >> 32).saturating_mul(self.blocks.len() as u64)) >> 32) as usize
    }

    /// Insert an AsBytes value into the filter
    pub fn insert<T: AsBytes + ?Sized>(&mut self, value: &T) {
        self.insert_hash(hash_as_bytes(value));
    }

    /// Insert a hash into the filter
    pub fn insert_hash(&mut self, hash: u64) {
        let block_index = self.hash_to_block_index(hash);
        self.blocks[block_index].insert(hash as u32)
    }

    /// Check if an AsBytes value is probably present or definitely absent in the filter
    pub fn check<T: AsBytes + ?Sized>(&self, value: &T) -> bool {
        self.check_hash(hash_as_bytes(value))
    }

    /// Check if a hash is in the filter. May return
    /// true for values that were never inserted ("false positive")
    /// but will always return false if a hash has not been inserted.
    pub fn check_hash(&self, hash: u64) -> bool {
        let block_index = self.hash_to_block_index(hash);
        self.blocks[block_index].check(hash as u32)
    }

    /// Write the bitset in serialized form to the writer
    pub fn write_bitset<W: Write>(&self, mut writer: W) -> Result<()> {
        for block in &self.blocks {
            writer
                .write_all(block.to_le_bytes().as_slice())
                .map_err(|source| SbbfError::WriteError { source })?;
        }
        Ok(())
    }

    /// Get the raw bitset as bytes
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut result = Vec::with_capacity(self.blocks.len() * 32);
        for block in &self.blocks {
            result.extend_from_slice(&block.to_le_bytes());
        }
        result
    }

    /// Get the number of blocks in this filter
    pub fn num_blocks(&self) -> usize {
        self.blocks.len()
    }

    /// Get the size in bytes of this filter
    pub fn size_bytes(&self) -> usize {
        self.blocks.len() * 32
    }

    /// Return the total in memory size of this bloom filter in bytes
    pub fn estimated_memory_size(&self) -> usize {
        self.blocks.capacity() * std::mem::size_of::<Block>()
    }

    /// Check if this filter might intersect with another filter.
    /// Returns true if there's at least one bit position where both filters have a 1.
    /// This is a fast check that may return false positives but never false negatives.
    ///
    /// Returns an error if the filters have different sizes, as bloom filters with
    /// different configurations cannot be reliably compared.
    pub fn might_intersect(&self, other: &Self) -> Result<bool> {
        if self.blocks.len() != other.blocks.len() {
            return Err(SbbfError::InvalidData {
                message: format!(
                    "Cannot compare bloom filters with different sizes: {} blocks vs {} blocks. \
                     Both filters must use the same configuration.",
                    self.blocks.len(),
                    other.blocks.len()
                ),
            });
        }
        for i in 0..self.blocks.len() {
            for j in 0..8 {
                if (self.blocks[i][j] & other.blocks[i][j]) != 0 {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    /// Check if this filter might intersect with a raw bitmap.
    /// The bitmap should be in the same format as produced by to_bytes().
    ///
    /// Returns an error if the bitmaps have different sizes, as bloom filters with
    /// different configurations cannot be reliably compared.
    pub fn might_intersect_bytes(&self, other_bytes: &[u8]) -> Result<bool> {
        Self::bytes_might_intersect(&self.to_bytes(), other_bytes)
    }

    /// Check if two raw bloom filter bitmaps might intersect.
    /// Returns true if there's at least one bit position where both filters have a 1.
    ///
    /// This is a fast probabilistic check: if it returns false, the filters definitely
    /// have no common elements. If it returns true, they might have common elements
    /// (with possible false positives).
    ///
    /// Returns an error if the bitmaps have different sizes, as bloom filters with
    /// different configurations cannot be reliably compared.
    pub fn bytes_might_intersect(a: &[u8], b: &[u8]) -> Result<bool> {
        if a.len() != b.len() {
            return Err(SbbfError::InvalidData {
                message: format!(
                    "Cannot compare bloom filters with different sizes: {} bytes vs {} bytes. \
                     Both filters must use the same configuration.",
                    a.len(),
                    b.len()
                ),
            });
        }
        for i in 0..a.len() {
            if (a[i] & b[i]) != 0 {
                return Ok(true);
            }
        }
        Ok(false)
    }
}

// Per spec we use xxHash with seed=0
const SEED: u64 = 0;

#[inline]
fn hash_as_bytes<A: AsBytes + ?Sized>(value: &A) -> u64 {
    XxHash64::oneshot(SEED, value.as_bytes().as_ref())
}

/// Builder for creating SBBF instances with a fluent API
pub struct SbbfBuilder {
    ndv: Option<u64>,
    fpp: Option<f64>,
    log2_num_bytes: Option<u8>,
}

impl SbbfBuilder {
    /// Create a new SBBF builder
    pub fn new() -> Self {
        Self {
            ndv: None,
            fpp: None,
            log2_num_bytes: None,
        }
    }

    /// Set the expected number of distinct values
    pub fn expected_items(mut self, ndv: u64) -> Self {
        self.ndv = Some(ndv);
        self
    }

    /// Set the desired false positive probability
    pub fn false_positive_probability(mut self, fpp: f64) -> Self {
        self.fpp = Some(fpp);
        self
    }

    /// Set the number of bytes directly
    pub fn log2_num_bytes(mut self, log2_num_bytes: u8) -> Self {
        self.log2_num_bytes = Some(log2_num_bytes);
        self
    }

    /// Build the SBBF
    pub fn build(self) -> Result<Sbbf> {
        if let Some(log2_num_bytes) = self.log2_num_bytes {
            Ok(Sbbf::with_log2_num_bytes(log2_num_bytes))
        } else if let (Some(ndv), Some(fpp)) = (self.ndv, self.fpp) {
            Sbbf::with_ndv_fpp(ndv, fpp)
        } else {
            Err(SbbfError::InvalidData {
                message: "Must specify either log2_num_bytes or both ndv and fpp".to_string(),
            })
        }
    }
}

impl Default for SbbfBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash_bytes() {
        assert_eq!(hash_as_bytes(""), 17241709254077376921);
    }

    #[test]
    fn test_mask_set_quick_check() {
        for i in 0..1_000 {
            let result = Block::mask(i);
            assert!(result.0.iter().all(|&x| x.is_power_of_two()));
        }
    }

    #[test]
    fn test_block_insert_and_check() {
        for i in 0..1_000 {
            let mut block = Block::ZERO;
            block.insert(i);
            assert!(block.check(i));
        }
    }

    #[test]
    fn test_sbbf_insert_and_check() {
        let mut sbbf = Sbbf::with_log2_num_bytes(10);
        for i in 0..1_000 {
            sbbf.insert(&i);
            assert!(sbbf.check(&i));
        }
    }

    #[test]
    fn test_sbbf_builder() {
        let sbbf = SbbfBuilder::new()
            .expected_items(1000)
            .false_positive_probability(0.01)
            .build()
            .unwrap();

        assert!(sbbf.num_blocks() > 0);
    }

    #[test]
    fn test_sbbf_string_types() {
        let mut sbbf = SbbfBuilder::new()
            .expected_items(100)
            .false_positive_probability(0.01)
            .build()
            .unwrap();

        // Test different string types
        let string_val = "hello";
        let str_val = "world";
        let bytes_val = b"bytes";

        sbbf.insert(string_val);
        sbbf.insert(str_val);
        sbbf.insert(&bytes_val[..]);

        assert!(sbbf.check(string_val));
        assert!(sbbf.check(str_val));
        assert!(sbbf.check(&bytes_val[..]));
        assert!(!sbbf.check("not_inserted"));
    }

    #[test]
    fn test_sbbf_numeric_types() {
        let mut sbbf = SbbfBuilder::new()
            .expected_items(100)
            .false_positive_probability(0.01)
            .build()
            .unwrap();

        // Test different numeric types
        let i32_val = 42i32;
        let i64_val = 12345i64;
        let f64_val = std::f64::consts::PI;
        let bool_val = true;

        sbbf.insert(&i32_val);
        sbbf.insert(&i64_val);
        sbbf.insert(&f64_val);
        sbbf.insert(&bool_val);

        assert!(sbbf.check(&i32_val));
        assert!(sbbf.check(&i64_val));
        assert!(sbbf.check(&f64_val));
        assert!(sbbf.check(&bool_val));
        assert!(!sbbf.check(&999i32));
    }

    #[test]
    fn test_num_of_bits_from_ndv_fpp() {
        for (fpp, ndv, log2_num_bytes) in &[
            (0.1, 10, 3),
            (0.01, 10, 4),
            (0.001, 10, 5),
            (0.1, 100, 7),
            (0.01, 100, 8),
            (0.001, 100, 8),
            (0.1, 1000, 10),
            (0.01, 1000, 11),
            (0.001, 1000, 12),
        ] {
            assert_eq!(*log2_num_bytes, min_log2_bytes(*ndv, *fpp));
        }
    }

    #[test]
    fn test_serialization() {
        let mut sbbf = SbbfBuilder::new()
            .expected_items(100)
            .false_positive_probability(0.01)
            .build()
            .unwrap();

        // Insert some values
        for i in 0..50 {
            sbbf.insert(&i);
        }

        // Serialize to bytes
        let bytes = sbbf.to_bytes();
        assert!(!bytes.is_empty());
        assert_eq!(bytes.len(), sbbf.size_bytes());

        // Deserialize from bytes
        let sbbf2 = Sbbf::new(&bytes).unwrap();
        assert_eq!(sbbf.num_blocks(), sbbf2.num_blocks());

        // Check that deserialized filter works
        for i in 0..50 {
            assert!(sbbf2.check(&i));
        }
        assert!(!sbbf2.check(&999));
    }
}
