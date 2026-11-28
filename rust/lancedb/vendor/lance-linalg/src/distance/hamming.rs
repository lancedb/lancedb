// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Hamming distance.
//!
//! This module provides hamming distance computation for binary vectors,
//! including SIMD-accelerated pairwise hamming distance for binary hashes.

use std::collections::HashMap;
use std::sync::{Arc, LazyLock};

use arrow_array::builder::{ListBuilder, UInt64Builder};
use arrow_array::{
    Array, ArrayRef, FixedSizeListArray, Float32Array, RecordBatch, RecordBatchIterator,
    RecordBatchReader, UInt32Array, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use rayon::prelude::*;

use crate::distance::{BatchKernel, assert_batch_output_layout};
use crate::{Error, Result};

/// Schema of a Hamming distance-pair batch.
static DISTANCE_PAIR_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("row_id_a", DataType::UInt64, false),
        Field::new("row_id_b", DataType::UInt64, false),
        Field::new("distance", DataType::UInt32, false),
    ]))
});

/// Schema of a Hamming clustering-result batch.
static CLUSTER_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("representative", DataType::UInt64, false),
        Field::new(
            "duplicates",
            DataType::List(Arc::new(Field::new("item", DataType::UInt64, true))),
            false,
        ),
    ]))
});

pub trait Hamming {
    /// Hamming distance between two vectors.
    fn hamming(x: &[u8], y: &[u8]) -> f32;
}

/// Hamming distance from `x` to `y`.
type HammingKernel = unsafe fn(&[u8], &[u8]) -> f32;

lance_core::dispatch! {
    static HAMMING: Dispatch<HammingKernel> = {
        #[cfg(target_arch = "aarch64")]
        {
            &[aarch64::HAMMING_NEON, HAMMING_PORTABLE]
        }
        // No AVX-512 candidate. A `Dispatch` resolves at compile time only
        // when the build baseline covers its first candidate; Haswell builds
        // cover AVX2 but not AVX-512, so listing AVX-512 first would turn this
        // per-pair call into a runtime selection. The batch kernels below
        // select once per batch, so they do list it.
        #[cfg(target_arch = "x86_64")]
        {
            &[x86::HAMMING_AVX2, HAMMING_PORTABLE]
        }
        #[cfg(not(any(target_arch = "aarch64", target_arch = "x86_64")))]
        {
            &[HAMMING_PORTABLE]
        }
    };
}

lance_core::dispatch! {
    static HAMMING_BATCH: Dispatch<BatchKernel<u8>> = {
        #[cfg(target_arch = "aarch64")]
        {
            &[aarch64::HAMMING_BATCH_NEON, HAMMING_BATCH_PORTABLE]
        }
        #[cfg(target_arch = "x86_64")]
        {
            &[
                x86::HAMMING_BATCH_AVX512,
                x86::HAMMING_BATCH_AVX2,
                HAMMING_BATCH_PORTABLE,
            ]
        }
        #[cfg(not(any(target_arch = "aarch64", target_arch = "x86_64")))]
        {
            &[HAMMING_BATCH_PORTABLE]
        }
    };
}

/// Bytes the NEON kernel sums into its 16-bit lanes before widening them.
///
/// A 64-byte step adds four `vcntq_u8` vectors (at most `4 * 8 = 32` per byte
/// lane, which fits `u8`) and pairwise-widens them into the 8 `u16` lanes with
/// `vpadalq_u8`, adding at most `2 * 32 = 64` per lane; a 16-byte step adds at
/// most `2 * 8 = 16`. Either way a lane gains at most 1 per input byte, so a
/// block of at most `1023 * 64` bytes keeps every lane at most
/// `65472 <= u16::MAX`.
#[cfg_attr(not(any(target_arch = "aarch64", test)), allow(dead_code))]
const HAMMING_NEON_FLUSH_BYTES: usize = 1023 * 64;

/// Hamming distance between two vectors.
///
/// Like [`hamming_scalar`], compares only the first `min(x.len(), y.len())`
/// bytes when the lengths differ.
#[inline]
pub fn hamming(x: &[u8], y: &[u8]) -> f32 {
    HAMMING.get().call(x, y)
}

lance_core::target_kernel! {
    #[target_features()]
    #[candidate(HAMMING_PORTABLE: HammingKernel)]
    #[inline]
    fn hamming_portable(x: &[u8], y: &[u8]) -> f32 {
        hamming_words(x, y)
    }
}

lance_core::target_kernel! {
    #[target_features()]
    #[candidate(HAMMING_BATCH_PORTABLE: BatchKernel<u8>)]
    #[inline]
    fn hamming_into_portable(from: &[u8], to: &[u8], dimension: usize, output: &mut [f32]) {
        hamming_rows(from, to, dimension, output, hamming_words);
    }
}

/// Hamming distance over 8-byte words, which LLVM vectorizes to the widest
/// popcount the enclosing kernel's target features allow.
#[inline(always)]
fn hamming_words(x: &[u8], y: &[u8]) -> f32 {
    let n = x.len().min(y.len());
    let (x_words, x_tail) = x[..n].as_chunks::<8>();
    let (y_words, y_tail) = y[..n].as_chunks::<8>();
    // Summing in u64 lets LLVM accumulate `vpsadbw`/`vpopcntq` results
    // directly instead of narrowing each one to u32.
    let words = x_words
        .iter()
        .zip(y_words)
        .map(|(a, b)| (u64::from_ne_bytes(*a) ^ u64::from_ne_bytes(*b)).count_ones() as u64)
        .sum::<u64>();
    let tail = x_tail
        .iter()
        .zip(y_tail)
        .map(|(&a, &b)| (a ^ b).count_ones() as u64)
        .sum::<u64>();
    (words + tail) as f32
}

/// Writes `pair(from, vector)` for each `dimension`-sized vector of `to`.
#[inline(always)]
fn hamming_rows(
    from: &[u8],
    to: &[u8],
    dimension: usize,
    output: &mut [f32],
    pair: impl Fn(&[u8], &[u8]) -> f32,
) {
    for (distance, vector) in output.iter_mut().zip(to.chunks_exact(dimension)) {
        *distance = pair(from, vector);
    }
}

#[cfg(target_arch = "aarch64")]
mod aarch64 {
    use std::arch::aarch64::*;

    use super::{HAMMING_NEON_FLUSH_BYTES, HammingKernel, hamming_rows};
    use crate::distance::BatchKernel;

    lance_core::target_kernel! {
        #[target_features("neon")]
        #[candidate(pub(super) HAMMING_BATCH_NEON: BatchKernel<u8>)]
        #[inline]
        pub(super) fn hamming_into_neon(
            from: &[u8],
            to: &[u8],
            dimension: usize,
            output: &mut [f32],
        ) {
            hamming_rows(from, to, dimension, output, |x, y| hamming_neon(x, y));
        }
    }

    lance_core::target_kernel! {
        #[target_features("neon")]
        #[candidate(pub(super) HAMMING_NEON: HammingKernel)]
        /// Sums per-byte popcounts in 16-bit lanes and reduces them once per
        /// block.
        #[inline]
        pub(super) fn hamming_neon(x: &[u8], y: &[u8]) -> f32 {
            let n = x.len().min(y.len());
            let (mut x, mut y) = (&x[..n], &y[..n]);
            let mut total = 0;
            // A plain loop rather than `chunks().zip()`, whose fold LLVM
            // left as a call per vector.
            while x.len() > HAMMING_NEON_FLUSH_BYTES {
                let (x_block, x_rest) = x.split_at(HAMMING_NEON_FLUSH_BYTES);
                let (y_block, y_rest) = y.split_at(HAMMING_NEON_FLUSH_BYTES);
                total += hamming_block(x_block, y_block);
                (x, y) = (x_rest, y_rest);
            }
            (total + hamming_block(x, y)) as f32
        }
    }

    /// `x` and `y` must have equal lengths.
    #[inline(always)]
    fn hamming_block(x: &[u8], y: &[u8]) -> u32 {
        let (x_64, x_rest) = x.as_chunks::<64>();
        let (y_64, y_rest) = y.as_chunks::<64>();
        let (x_16, x_tail) = x_rest.as_chunks::<16>();
        let (y_16, y_tail) = y_rest.as_chunks::<16>();
        // SAFETY: every load reads 16 bytes at an offset of at most 48 into a
        // 64-byte array, or at the start of a 16-byte array.
        let sum = unsafe {
            let mut acc = vdupq_n_u16(0);
            for (a, b) in x_64.iter().zip(y_64) {
                let (a, b) = (a.as_ptr(), b.as_ptr());
                let c0 = vcntq_u8(veorq_u8(vld1q_u8(a), vld1q_u8(b)));
                let c1 = vcntq_u8(veorq_u8(vld1q_u8(a.add(16)), vld1q_u8(b.add(16))));
                let c2 = vcntq_u8(veorq_u8(vld1q_u8(a.add(32)), vld1q_u8(b.add(32))));
                let c3 = vcntq_u8(veorq_u8(vld1q_u8(a.add(48)), vld1q_u8(b.add(48))));
                acc = vpadalq_u8(acc, vaddq_u8(vaddq_u8(c0, c1), vaddq_u8(c2, c3)));
            }
            for (a, b) in x_16.iter().zip(y_16) {
                let c = vcntq_u8(veorq_u8(vld1q_u8(a.as_ptr()), vld1q_u8(b.as_ptr())));
                acc = vpadalq_u8(acc, c);
            }
            vaddlvq_u16(acc)
        };
        let tail = x_tail
            .iter()
            .zip(y_tail)
            .map(|(&a, &b)| (a ^ b).count_ones())
            .sum::<u32>();
        sum + tail
    }
}

/// Scalar version of hamming distance. Used for benchmarks.
#[inline]
pub fn hamming_scalar(x: &[u8], y: &[u8]) -> f32 {
    x.iter()
        .zip(y.iter())
        .map(|(xi, yi)| (xi ^ yi).count_ones())
        .sum::<u32>() as f32
}

/// Writes the Hamming distance from `from` to each `dimension`-sized vector of
/// `to` into the matching element of `output`.
///
/// # Examples
///
/// ```
/// use lance_linalg::distance::hamming::hamming_distance_batch_into;
///
/// let query = [0, u8::MAX];
/// let targets = [0, u8::MAX, 1, u8::MAX - 1];
/// let mut distances = [0.0; 2];
/// hamming_distance_batch_into(&query, &targets, query.len(), &mut distances);
/// assert_eq!(distances, [0.0, 2.0]);
/// ```
///
/// # Panics
///
/// Panics if `dimension` is zero, `from.len()` differs from `dimension`, `to`
/// does not contain a whole number of vectors with that dimension, or
/// `output` does not have one slot per vector.
pub fn hamming_distance_batch_into(from: &[u8], to: &[u8], dimension: usize, output: &mut [f32]) {
    assert_batch_output_layout(from.len(), to.len(), dimension, output.len());
    HAMMING_BATCH.get().call(from, to, dimension, output);
}

/// Compute Hamming distances with the validation of [`super::DistanceType::batch`].
#[deprecated(note = "Use DistanceType::Hamming.batch")]
pub fn hamming_distance_arrow_batch(
    from: &dyn Array,
    to: &FixedSizeListArray,
) -> Result<Arc<Float32Array>> {
    super::DistanceType::Hamming.batch(from, to).map(Arc::new)
}

/// Compute hamming distance between two 64-bit values using POPCNT.
#[inline(always)]
pub fn hamming_u64(a: u64, b: u64) -> u32 {
    (a ^ b).count_ones()
}

/// Binary hash values stored as 64-bit lanes in lane-major order.
///
/// For a hash width of `N` bytes, `N` must be a positive multiple of 8 and the
/// number of lanes is `N / 8`. Lane-major layout keeps each lane contiguous for
/// all rows, which allows the pairwise path to reuse the SIMD `u64` batch
/// implementation for wider hashes.
#[derive(Debug, Clone)]
pub struct BinaryHashValues {
    lane_values: Vec<u64>,
    num_rows: usize,
    byte_width: usize,
}

impl BinaryHashValues {
    /// Create hash values from lane-major `u64` values.
    pub fn try_new(lane_values: Vec<u64>, num_rows: usize, byte_width: usize) -> Result<Self> {
        let num_lanes = validate_hash_byte_width(byte_width)?;
        let expected_values = checked_lane_value_count(num_rows, num_lanes)?;
        if lane_values.len() != expected_values {
            return Err(Error::InvalidArgumentError(format!(
                "Expected {} lane values for {} rows and {} byte hashes, got {}",
                expected_values,
                num_rows,
                byte_width,
                lane_values.len()
            )));
        }
        Ok(Self {
            lane_values,
            num_rows,
            byte_width,
        })
    }

    /// Extract binary hash values from a `FixedSizeList<UInt8, N>` Arrow array.
    pub fn from_fixed_size_list(array: &FixedSizeListArray) -> Result<Self> {
        let byte_width = usize::try_from(array.value_length()).map_err(|_| {
            Error::InvalidArgumentError(format!(
                "Expected FixedSizeList with a positive size that is a multiple of 8 bytes, got size {}",
                array.value_length()
            ))
        })?;
        let num_lanes = validate_hash_byte_width(byte_width)?;

        let values = array
            .values()
            .as_any()
            .downcast_ref::<arrow_array::UInt8Array>()
            .ok_or_else(|| {
                Error::InvalidArgumentError(
                    "Expected UInt8Array values in FixedSizeList".to_string(),
                )
            })?;

        let num_rows = array.len();
        let value_count = checked_lane_value_count(num_rows, num_lanes)?;
        let expected_bytes = checked_hash_byte_count(num_rows, byte_width)?;
        let bytes = values.values();
        if bytes.len() != expected_bytes {
            return Err(Error::InvalidArgumentError(format!(
                "Expected {} bytes for {} rows and {} byte hashes, got {}",
                expected_bytes,
                num_rows,
                byte_width,
                bytes.len()
            )));
        }

        let mut lane_values = vec![0u64; value_count];
        for row in 0..num_rows {
            let row_start = row * byte_width;
            for lane in 0..num_lanes {
                let start = row_start + lane * 8;
                let mut arr = [0u8; 8];
                arr.copy_from_slice(&bytes[start..start + 8]);
                lane_values[lane * num_rows + row] = u64::from_le_bytes(arr);
            }
        }

        Ok(Self {
            lane_values,
            num_rows,
            byte_width,
        })
    }

    /// Concatenate chunks with the same hash width into a single lane-major set.
    pub fn concat(chunks: &[Self]) -> Result<Self> {
        let Some(first) = chunks.first() else {
            return Err(Error::InvalidArgumentError(
                "Cannot concatenate zero binary hash chunks".to_string(),
            ));
        };

        let byte_width = first.byte_width;
        let num_lanes = first.num_lanes();
        let mut num_rows = 0usize;
        for chunk in chunks {
            if chunk.byte_width != byte_width {
                return Err(Error::InvalidArgumentError(format!(
                    "Cannot concatenate binary hash chunks with different widths: {} and {} bytes",
                    byte_width, chunk.byte_width
                )));
            }
            num_rows = num_rows.checked_add(chunk.num_rows).ok_or_else(|| {
                Error::InvalidArgumentError(
                    "Binary hash row count overflowed while concatenating chunks".to_string(),
                )
            })?;
        }

        let value_count = checked_lane_value_count(num_rows, num_lanes)?;
        let mut lane_values = vec![0u64; value_count];
        let mut row_offset = 0;
        for chunk in chunks {
            for lane in 0..num_lanes {
                let dest_start = lane * num_rows + row_offset;
                let dest_end = dest_start + chunk.num_rows;
                lane_values[dest_start..dest_end].copy_from_slice(chunk.lane(lane));
            }
            row_offset += chunk.num_rows;
        }

        Ok(Self {
            lane_values,
            num_rows,
            byte_width,
        })
    }

    pub fn len(&self) -> usize {
        self.num_rows
    }

    pub fn is_empty(&self) -> bool {
        self.num_rows == 0
    }

    pub fn byte_width(&self) -> usize {
        self.byte_width
    }

    pub fn num_lanes(&self) -> usize {
        self.byte_width / 8
    }

    pub fn lane(&self, lane: usize) -> &[u64] {
        let start = lane * self.num_rows;
        &self.lane_values[start..start + self.num_rows]
    }

    pub fn into_u64_values(self) -> Result<Vec<u64>> {
        if self.num_lanes() != 1 {
            return Err(Error::InvalidArgumentError(format!(
                "Expected 8-byte binary hashes, got {} byte hashes",
                self.byte_width
            )));
        }
        Ok(self.lane_values)
    }
}

fn checked_lane_value_count(num_rows: usize, num_lanes: usize) -> Result<usize> {
    num_rows.checked_mul(num_lanes).ok_or_else(|| {
        Error::InvalidArgumentError(format!(
            "Binary hash lane value count overflowed for {} rows and {} lanes",
            num_rows, num_lanes
        ))
    })
}

fn checked_hash_byte_count(num_rows: usize, byte_width: usize) -> Result<usize> {
    num_rows.checked_mul(byte_width).ok_or_else(|| {
        Error::InvalidArgumentError(format!(
            "Binary hash byte count overflowed for {} rows and {} byte hashes",
            num_rows, byte_width
        ))
    })
}

fn validate_hash_byte_width(byte_width: usize) -> Result<usize> {
    if byte_width == 0 || !byte_width.is_multiple_of(8) {
        return Err(Error::InvalidArgumentError(format!(
            "Expected FixedSizeList with a positive size that is a multiple of 8 bytes, got size {}",
            byte_width
        )));
    }
    Ok(byte_width / 8)
}

/// Result of pairwise hamming distance computation.
#[derive(Debug, Clone)]
pub struct PairwiseResult {
    pub row_id_a: Vec<u64>,
    pub row_id_b: Vec<u64>,
    pub distances: Vec<u32>,
}

impl PairwiseResult {
    pub fn new() -> Self {
        Self {
            row_id_a: Vec::new(),
            row_id_b: Vec::new(),
            distances: Vec::new(),
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            row_id_a: Vec::with_capacity(capacity),
            row_id_b: Vec::with_capacity(capacity),
            distances: Vec::with_capacity(capacity),
        }
    }

    pub fn push(&mut self, a: u64, b: u64, dist: u32) {
        self.row_id_a.push(a);
        self.row_id_b.push(b);
        self.distances.push(dist);
    }

    pub fn len(&self) -> usize {
        self.row_id_a.len()
    }

    pub fn is_empty(&self) -> bool {
        self.row_id_a.is_empty()
    }

    pub fn extend(&mut self, other: Self) {
        self.row_id_a.extend(other.row_id_a);
        self.row_id_b.extend(other.row_id_b);
        self.distances.extend(other.distances);
    }

    /// Convert to Arrow RecordBatch, consuming self.
    pub fn into_record_batch(self) -> RecordBatch {
        let schema = DISTANCE_PAIR_SCHEMA.clone();

        let row_id_a = Arc::new(UInt64Array::from(self.row_id_a));
        let row_id_b = Arc::new(UInt64Array::from(self.row_id_b));
        let distances = Arc::new(UInt32Array::from(self.distances));

        RecordBatch::try_new(schema, vec![row_id_a, row_id_b, distances])
            .expect("Failed to create RecordBatch")
    }
}

impl Default for PairwiseResult {
    fn default() -> Self {
        Self::new()
    }
}

/// Writes the Hamming distance from `query` to each target into the matching
/// result slot. Kernels fill `min(targets.len(), results.len())` slots.
type HammingBatchU64Kernel = unsafe fn(u64, &[u64], &mut [u32]);

lance_core::dispatch! {
    static HAMMING_BATCH_U64: Dispatch<HammingBatchU64Kernel> = {
        #[cfg(target_arch = "x86_64")]
        {
            &[
                x86::HAMMING_BATCH_U64_AVX512,
                x86::HAMMING_BATCH_U64_AVX2,
                HAMMING_BATCH_U64_PORTABLE,
            ]
        }
        #[cfg(not(target_arch = "x86_64"))]
        {
            &[HAMMING_BATCH_U64_PORTABLE]
        }
    };
}

/// Compute hamming distances for a query against multiple targets.
/// Uses SIMD acceleration when available.
///
/// # Panics
///
/// Panics if `results` is not the same length as `targets`.
#[inline]
pub fn hamming_batch_u64(query: u64, targets: &[u64], results: &mut [u32]) {
    // The kernels would stay in bounds on a mismatch, but silently fill only
    // the shorter of the two, leaving an over-long `results` tail unwritten for
    // the caller to read as distances. That is wrong output rather than a
    // crash, which is why this is an `assert_eq!` and not a `debug_assert_eq!`.
    let num_targets = targets.len();
    let num_slots = results.len();
    assert_eq!(
        num_targets, num_slots,
        "hamming_batch_u64 needs one result slot per target, \
         got {num_targets} target(s) and {num_slots} slot(s)"
    );
    HAMMING_BATCH_U64.get().call(query, targets, results);
}

lance_core::target_kernel! {
    #[target_features()]
    #[candidate(HAMMING_BATCH_U64_PORTABLE: HammingBatchU64Kernel)]
    /// A plain zip lets LLVM vectorize the popcount (NEON `cnt` on aarch64)
    /// with no per-element bounds checks.
    #[inline]
    fn hamming_batch_u64_portable(query: u64, targets: &[u64], results: &mut [u32]) {
        for (result, &target) in results.iter_mut().zip(targets) {
            *result = (query ^ target).count_ones();
        }
    }
}

#[cfg(target_arch = "x86_64")]
mod x86 {
    use std::arch::x86_64::*;

    use super::{HammingBatchU64Kernel, HammingKernel, hamming_rows, hamming_words};
    use crate::distance::BatchKernel;

    lance_core::target_kernel! {
        #[target_features("avx2")]
        #[candidate(pub(super) HAMMING_AVX2: HammingKernel)]
        /// PSHUFB nibble-lookup popcount, generated by LLVM.
        #[inline]
        pub(super) fn hamming_avx2(x: &[u8], y: &[u8]) -> f32 {
            hamming_words(x, y)
        }
    }

    lance_core::target_kernel! {
        #[target_features("avx2")]
        #[candidate(pub(super) HAMMING_BATCH_AVX2: BatchKernel<u8>)]
        pub(super) fn hamming_into_avx2(
            from: &[u8],
            to: &[u8],
            dimension: usize,
            output: &mut [f32],
        ) {
            hamming_rows(from, to, dimension, output, hamming_words);
        }
    }

    lance_core::target_kernel! {
        #[target_features("avx512f", "avx512vpopcntdq")]
        #[candidate(pub(super) HAMMING_BATCH_AVX512: BatchKernel<u8>)]
        /// `vpopcntq` over 64-byte vectors, generated by LLVM.
        pub(super) fn hamming_into_avx512(
            from: &[u8],
            to: &[u8],
            dimension: usize,
            output: &mut [f32],
        ) {
            hamming_rows(from, to, dimension, output, hamming_words);
        }
    }

    lance_core::target_kernel! {
        #[target_features("avx512f", "avx512vpopcntdq")]
        #[candidate(pub(super) HAMMING_BATCH_U64_AVX512: HammingBatchU64Kernel)]
        /// AVX-512 VPOPCNTDQ: Process 8 x 64-bit values at once.
        pub(super) fn hamming_batch_u64_avx512(query: u64, targets: &[u64], results: &mut [u32]) {
            // The chunk loop loads and stores through raw pointers, so `n`
            // bounds both slices, as a safe function must.
            let n = targets.len().min(results.len());
            let query_vec = _mm512_set1_epi64(query as i64);

            let chunks = n / 8;
            let remainder = n % 8;

            for i in 0..chunks {
                let offset = i * 8;
                // SAFETY: `offset + 8 <= n`, which bounds both slices.
                unsafe {
                    let targets_ptr = targets.as_ptr().add(offset) as *const __m512i;
                    let target_vec = _mm512_loadu_si512(targets_ptr);

                    let xor_result = _mm512_xor_si512(query_vec, target_vec);
                    let popcount = _mm512_popcnt_epi64(xor_result);
                    let popcount_32 = _mm512_cvtepi64_epi32(popcount);

                    _mm256_storeu_si256(
                        results.as_mut_ptr().add(offset) as *mut __m256i,
                        popcount_32,
                    );
                }
            }

            if remainder > 0 {
                let offset = chunks * 8;
                for j in 0..remainder {
                    results[offset + j] = (query ^ targets[offset + j]).count_ones();
                }
            }
        }
    }

    lance_core::target_kernel! {
        #[target_features("avx2")]
        #[candidate(pub(super) HAMMING_BATCH_U64_AVX2: HammingBatchU64Kernel)]
        /// AVX2 popcount using a PSHUFB nibble lookup table.
        pub(super) fn hamming_batch_u64_avx2(query: u64, targets: &[u64], results: &mut [u32]) {
            // See `hamming_batch_u64_avx512`.
            let n = targets.len().min(results.len());

            let lookup = _mm256_setr_epi8(
                0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4, 0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3,
                2, 3, 3, 4,
            );
            let low_mask = _mm256_set1_epi8(0x0f);
            let query_vec = _mm256_set1_epi64x(query as i64);

            let chunks = n / 4;
            let remainder = n % 4;

            for i in 0..chunks {
                let offset = i * 4;
                // SAFETY: `offset + 4 <= n`, which bounds both slices.
                unsafe {
                    let targets_ptr = targets.as_ptr().add(offset) as *const __m256i;
                    let target_vec = _mm256_loadu_si256(targets_ptr);

                    let xor_result = _mm256_xor_si256(query_vec, target_vec);

                    // Popcount using nibble lookup
                    let lo = _mm256_and_si256(xor_result, low_mask);
                    let hi = _mm256_and_si256(_mm256_srli_epi16(xor_result, 4), low_mask);
                    let popcnt_lo = _mm256_shuffle_epi8(lookup, lo);
                    let popcnt_hi = _mm256_shuffle_epi8(lookup, hi);
                    let popcnt_bytes = _mm256_add_epi8(popcnt_lo, popcnt_hi);
                    let popcount = _mm256_sad_epu8(popcnt_bytes, _mm256_setzero_si256());

                    let results_ptr = results.as_mut_ptr().add(offset);
                    *results_ptr = _mm256_extract_epi32::<0>(popcount) as u32;
                    *results_ptr.add(1) = _mm256_extract_epi32::<2>(popcount) as u32;
                    *results_ptr.add(2) = _mm256_extract_epi32::<4>(popcount) as u32;
                    *results_ptr.add(3) = _mm256_extract_epi32::<6>(popcount) as u32;
                }
            }

            if remainder > 0 {
                let offset = chunks * 4;
                for j in 0..remainder {
                    results[offset + j] = (query ^ targets[offset + j]).count_ones();
                }
            }
        }
    }
}

/// Compute pairwise hamming distances for all pairs of hashes.
///
/// Returns pairs where distance <= threshold (if provided).
///
/// # Arguments
/// * `hashes` - Vector of 64-bit hash values
/// * `row_ids` - Optional row IDs (defaults to indices if None)
/// * `threshold` - Optional maximum distance to include in results
pub fn pairwise_hamming_distance(
    hashes: &[u64],
    row_ids: Option<&[u64]>,
    threshold: Option<u32>,
) -> PairwiseResult {
    let n = hashes.len();
    if n < 2 {
        return PairwiseResult::new();
    }

    let threshold = threshold.unwrap_or(u32::MAX);
    let num_pairs = n * (n - 1) / 2;
    let mut result = PairwiseResult::with_capacity(num_pairs.min(1_000_000));

    for i in 0..n {
        for j in (i + 1)..n {
            let dist = hamming_u64(hashes[i], hashes[j]);
            if dist <= threshold {
                let id_a = row_ids.map_or(i as u64, |ids| ids[i]);
                let id_b = row_ids.map_or(j as u64, |ids| ids[j]);
                result.push(id_a, id_b, dist);
            }
        }
    }

    result
}

/// Compute pairwise hamming distances in parallel using rayon + SIMD.
///
/// Uses chunked parallelization for balanced workload distribution.
pub fn pairwise_hamming_distance_parallel(
    hashes: &[u64],
    row_ids: Option<&[u64]>,
    threshold: Option<u32>,
) -> PairwiseResult {
    let n = hashes.len();
    if n < 2 {
        return PairwiseResult::new();
    }

    let threshold = threshold.unwrap_or(u32::MAX);
    let total_pairs = n * (n - 1) / 2;

    // For small datasets, use sequential to avoid thread overhead
    if total_pairs < 10_000 {
        return pairwise_hamming_distance(hashes, row_ids, Some(threshold));
    }

    let threads = rayon::current_num_threads();
    let pairs_per_chunk = total_pairs.div_ceil(threads);
    let chunks = compute_balanced_chunks(n, pairs_per_chunk);

    let results: Vec<PairwiseResult> = chunks
        .into_par_iter()
        .map(|(start_row, end_row)| {
            process_row_range(hashes, row_ids, threshold, start_row, end_row)
        })
        .collect();

    let mut combined = PairwiseResult::new();
    for r in results {
        combined.extend(r);
    }
    combined
}

/// Compute pairwise hamming distances for all pairs of fixed-width binary hashes.
///
/// This supports any hash width that is a positive multiple of 8 bytes. For
/// 8-byte hashes this delegates to the existing `u64` implementation.
pub fn pairwise_hamming_distance_binary(
    hashes: &BinaryHashValues,
    row_ids: Option<&[u64]>,
    threshold: Option<u32>,
) -> PairwiseResult {
    let n = hashes.len();
    if n < 2 {
        return PairwiseResult::new();
    }

    if hashes.num_lanes() == 1 {
        return pairwise_hamming_distance(hashes.lane(0), row_ids, threshold);
    }

    let threshold = threshold.unwrap_or(u32::MAX);
    let num_pairs = n * (n - 1) / 2;
    let mut result = PairwiseResult::with_capacity(num_pairs.min(1_000_000));

    for i in 0..n {
        for j in (i + 1)..n {
            let mut dist = 0;
            for lane in 0..hashes.num_lanes() {
                let lane_values = hashes.lane(lane);
                dist += hamming_u64(lane_values[i], lane_values[j]);
            }
            if dist <= threshold {
                let id_a = row_ids.map_or(i as u64, |ids| ids[i]);
                let id_b = row_ids.map_or(j as u64, |ids| ids[j]);
                result.push(id_a, id_b, dist);
            }
        }
    }

    result
}

/// Compute pairwise hamming distances in parallel for fixed-width binary hashes.
///
/// Wider hashes reuse the SIMD `u64` batch implementation lane-by-lane.
pub fn pairwise_hamming_distance_binary_parallel(
    hashes: &BinaryHashValues,
    row_ids: Option<&[u64]>,
    threshold: Option<u32>,
) -> PairwiseResult {
    let n = hashes.len();
    if n < 2 {
        return PairwiseResult::new();
    }

    if hashes.num_lanes() == 1 {
        return pairwise_hamming_distance_parallel(hashes.lane(0), row_ids, threshold);
    }

    let threshold = threshold.unwrap_or(u32::MAX);
    let total_pairs = n * (n - 1) / 2;

    if total_pairs < 10_000 {
        return pairwise_hamming_distance_binary(hashes, row_ids, Some(threshold));
    }

    let threads = rayon::current_num_threads();
    let pairs_per_chunk = total_pairs.div_ceil(threads);
    let chunks = compute_balanced_chunks(n, pairs_per_chunk);

    let results: Vec<PairwiseResult> = chunks
        .into_par_iter()
        .map(|(start_row, end_row)| {
            process_row_range_binary(hashes, row_ids, threshold, start_row, end_row)
        })
        .collect();

    let mut combined = PairwiseResult::new();
    for r in results {
        combined.extend(r);
    }
    combined
}

/// Compute balanced chunks for parallel processing.
fn compute_balanced_chunks(n: usize, target_pairs_per_chunk: usize) -> Vec<(usize, usize)> {
    let mut chunks = Vec::new();
    let mut current_start = 0;
    let mut current_pairs = 0;

    for i in 0..n {
        let pairs_for_row = n - i - 1;
        current_pairs += pairs_for_row;

        if current_pairs >= target_pairs_per_chunk || i == n - 1 {
            chunks.push((current_start, i + 1));
            current_start = i + 1;
            current_pairs = 0;
        }
    }

    chunks
}

/// Calls `on_match(offset, distance)` for each distance at most `threshold`.
#[inline]
fn for_each_within(distances: &[u32], threshold: u32, mut on_match: impl FnMut(usize, u32)) {
    // Matches are rare, so test 16 distances at a time without branching,
    // which LLVM vectorizes, and only walk the chunks that hold one.
    let (chunks, tail) = distances.as_chunks::<16>();
    for (chunk_index, chunk) in chunks.iter().enumerate() {
        if chunk
            .iter()
            .fold(false, |any, &dist| any | (dist <= threshold))
        {
            for (k, &dist) in chunk.iter().enumerate() {
                if dist <= threshold {
                    on_match(chunk_index * 16 + k, dist);
                }
            }
        }
    }
    let tail_start = chunks.len() * 16;
    for (k, &dist) in tail.iter().enumerate() {
        if dist <= threshold {
            on_match(tail_start + k, dist);
        }
    }
}

/// Process a range of rows for pairwise comparison using SIMD.
fn process_row_range(
    hashes: &[u64],
    row_ids: Option<&[u64]>,
    threshold: u32,
    start_row: usize,
    end_row: usize,
) -> PairwiseResult {
    let n = hashes.len();
    let mut result = PairwiseResult::new();
    // Selected once per range rather than per row through `hamming_batch_u64`,
    // whose length assert is redundant here: each row slices `distances` to
    // exactly the targets' length.
    let kernel = HAMMING_BATCH_U64.get();
    // Rows only get shorter, so the first row's buffer fits every later one,
    // and the kernel overwrites each slot it is handed.
    let mut distances = vec![0u32; n.saturating_sub(start_row + 1)];

    for i in start_row..end_row {
        let remaining = n - i - 1;
        if remaining == 0 {
            continue;
        }

        let row_distances = &mut distances[..remaining];
        kernel.call(hashes[i], &hashes[i + 1..], row_distances);

        let id_a = row_ids.map_or(i as u64, |ids| ids[i]);
        for_each_within(row_distances, threshold, |j_offset, dist| {
            let j = i + 1 + j_offset;
            let id_b = row_ids.map_or(j as u64, |ids| ids[j]);
            result.push(id_a, id_b, dist);
        });
    }

    result
}

/// Process a range of rows for pairwise comparison of wider binary hashes.
fn process_row_range_binary(
    hashes: &BinaryHashValues,
    row_ids: Option<&[u64]>,
    threshold: u32,
    start_row: usize,
    end_row: usize,
) -> PairwiseResult {
    let n = hashes.len();
    let mut result = PairwiseResult::new();
    let kernel = HAMMING_BATCH_U64.get();
    // See `process_row_range`.
    let max_remaining = n.saturating_sub(start_row + 1);
    let mut distances = vec![0u32; max_remaining];
    let mut lane_distances = vec![0u32; max_remaining];

    for i in start_row..end_row {
        let remaining = n - i - 1;
        if remaining == 0 {
            continue;
        }

        let row_distances = &mut distances[..remaining];
        let first_lane = hashes.lane(0);
        kernel.call(first_lane[i], &first_lane[i + 1..], row_distances);

        let row_lane_distances = &mut lane_distances[..remaining];
        for lane in 1..hashes.num_lanes() {
            let lane_values = hashes.lane(lane);
            kernel.call(lane_values[i], &lane_values[i + 1..], row_lane_distances);
            for (distance, lane_distance) in row_distances.iter_mut().zip(&*row_lane_distances) {
                *distance += *lane_distance;
            }
        }

        let id_a = row_ids.map_or(i as u64, |ids| ids[i]);
        for_each_within(row_distances, threshold, |j_offset, dist| {
            let j = i + 1 + j_offset;
            let id_b = row_ids.map_or(j as u64, |ids| ids[j]);
            result.push(id_a, id_b, dist);
        });
    }

    result
}

/// Extract u64 hashes from a FixedSizeList<UInt8, 8> Arrow array.
pub fn extract_hashes_from_fixed_list(array: &FixedSizeListArray) -> Result<Vec<u64>> {
    let hashes = extract_binary_hashes_from_fixed_list(array)?;
    if hashes.byte_width() != 8 {
        return Err(Error::InvalidArgumentError(format!(
            "Expected FixedSizeList with size 8, got size {}",
            hashes.byte_width()
        )));
    }
    hashes.into_u64_values()
}

/// Extract binary hashes from a `FixedSizeList<UInt8, N>` Arrow array where
/// `N` is a positive multiple of 8 bytes.
pub fn extract_binary_hashes_from_fixed_list(
    array: &FixedSizeListArray,
) -> Result<BinaryHashValues> {
    BinaryHashValues::from_fixed_size_list(array)
}

/// Union-Find data structure with path compression for clustering.
pub struct UnionFind {
    parent: HashMap<u64, u64>,
    rank: HashMap<u64, u32>,
}

impl UnionFind {
    pub fn new() -> Self {
        Self {
            parent: HashMap::new(),
            rank: HashMap::new(),
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            parent: HashMap::with_capacity(capacity),
            rank: HashMap::with_capacity(capacity),
        }
    }

    /// Find the root of a node with path compression.
    pub fn find(&mut self, x: u64) -> u64 {
        if let std::collections::hash_map::Entry::Vacant(e) = self.parent.entry(x) {
            e.insert(x);
            self.rank.insert(x, 0);
            return x;
        }

        let mut current = x;
        let mut path = Vec::new();

        while self.parent[&current] != current {
            path.push(current);
            current = self.parent[&current];
        }
        let root = current;

        for node in path {
            self.parent.insert(node, root);
        }

        root
    }

    /// Union two nodes, using union by rank.
    pub fn union(&mut self, a: u64, b: u64) -> bool {
        let root_a = self.find(a);
        let root_b = self.find(b);

        if root_a == root_b {
            return false;
        }

        let rank_a = self.rank[&root_a];
        let rank_b = self.rank[&root_b];

        if rank_a < rank_b {
            self.parent.insert(root_a, root_b);
        } else if rank_a > rank_b {
            self.parent.insert(root_b, root_a);
        } else if root_a < root_b {
            self.parent.insert(root_b, root_a);
            *self.rank.get_mut(&root_a).unwrap() += 1;
        } else {
            self.parent.insert(root_a, root_b);
            *self.rank.get_mut(&root_b).unwrap() += 1;
        }

        true
    }

    pub fn nodes(&self) -> impl Iterator<Item = &u64> {
        self.parent.keys()
    }

    pub fn len(&self) -> usize {
        self.parent.len()
    }

    pub fn is_empty(&self) -> bool {
        self.parent.is_empty()
    }
}

impl Default for UnionFind {
    fn default() -> Self {
        Self::new()
    }
}

/// A cluster with representative and duplicates.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cluster {
    /// The representative row ID (smallest in the cluster).
    pub representative: u64,
    /// List of duplicate row IDs (excludes the representative).
    pub duplicates: Vec<u64>,
}

impl Cluster {
    pub fn size(&self) -> usize {
        1 + self.duplicates.len()
    }
}

/// Result of the clustering operation.
#[derive(Debug, Clone)]
pub struct ClusteringResult {
    /// List of clusters, each with a representative and duplicates.
    pub clusters: Vec<Cluster>,
}

impl ClusteringResult {
    pub fn num_clusters(&self) -> usize {
        self.clusters.len()
    }

    pub fn num_duplicates(&self) -> usize {
        self.clusters.iter().map(|c| c.duplicates.len()).sum()
    }

    pub fn num_unique(&self) -> usize {
        self.clusters.len()
    }

    /// Get the schema for clustering result batches.
    pub fn schema() -> SchemaRef {
        CLUSTER_SCHEMA.clone()
    }

    /// Convert to Arrow RecordBatch with columns:
    /// - `representative`: `UInt64`
    /// - `duplicates`: `List<UInt64>`
    pub fn to_record_batch(&self) -> RecordBatch {
        let schema = Self::schema();

        let mut representatives = Vec::with_capacity(self.clusters.len());
        let mut duplicates_builder = ListBuilder::new(UInt64Builder::new());

        for cluster in &self.clusters {
            representatives.push(cluster.representative);
            for &dup in &cluster.duplicates {
                duplicates_builder.values().append_value(dup);
            }
            duplicates_builder.append(true);
        }

        let representative_array: ArrayRef = Arc::new(UInt64Array::from(representatives));
        let duplicates_array: ArrayRef = Arc::new(duplicates_builder.finish());

        RecordBatch::try_new(schema, vec![representative_array, duplicates_array])
            .expect("Failed to create RecordBatch")
    }

    /// Convert to a RecordBatchReader that yields batches of the specified size.
    ///
    /// # Arguments
    /// * `batch_size` - Number of clusters per batch (default: 10000)
    pub fn into_reader(self, batch_size: Option<usize>) -> Box<dyn RecordBatchReader + Send> {
        let batch_size = batch_size.unwrap_or(10_000);
        let schema = Self::schema();

        if self.clusters.is_empty() {
            // Return empty reader
            let batches: Vec<std::result::Result<RecordBatch, arrow_schema::ArrowError>> = vec![];
            return Box::new(RecordBatchIterator::new(batches, schema));
        }

        let batches: Vec<std::result::Result<RecordBatch, arrow_schema::ArrowError>> = self
            .clusters
            .chunks(batch_size)
            .map(|chunk| {
                let mut representatives = Vec::with_capacity(chunk.len());
                let mut duplicates_builder = ListBuilder::new(UInt64Builder::new());

                for cluster in chunk {
                    representatives.push(cluster.representative);
                    for &dup in &cluster.duplicates {
                        duplicates_builder.values().append_value(dup);
                    }
                    duplicates_builder.append(true);
                }

                let representative_array: ArrayRef = Arc::new(UInt64Array::from(representatives));
                let duplicates_array: ArrayRef = Arc::new(duplicates_builder.finish());

                RecordBatch::try_new(Self::schema(), vec![representative_array, duplicates_array])
            })
            .collect();

        Box::new(RecordBatchIterator::new(batches, schema))
    }
}

/// Cluster edges using union-find algorithm.
///
/// Takes a list of edges (row_id_a, row_id_b) and groups connected nodes
/// into clusters. Each cluster has a representative (smallest row ID)
/// and a list of duplicates.
pub fn cluster_edges<I>(edges: I) -> ClusteringResult
where
    I: IntoIterator<Item = (u64, u64)>,
{
    let mut uf = UnionFind::new();

    for (a, b) in edges {
        uf.union(a, b);
    }

    let mut clusters_map: HashMap<u64, Vec<u64>> = HashMap::new();
    let nodes: Vec<u64> = uf.nodes().copied().collect();

    for node in nodes {
        let root = uf.find(node);
        clusters_map.entry(root).or_default().push(node);
    }

    let mut clusters = Vec::new();
    for (_root, mut members) in clusters_map {
        members.sort_unstable();

        if members.len() > 1 {
            let representative = *members.iter().min().unwrap();
            let duplicates: Vec<u64> = members
                .into_iter()
                .filter(|&m| m != representative)
                .collect();

            clusters.push(Cluster {
                representative,
                duplicates,
            });
        }
    }

    clusters.sort_by_key(|c| c.representative);

    ClusteringResult { clusters }
}

/// Cluster edges from PairwiseResult.
pub fn cluster_pairwise_result(result: &PairwiseResult) -> ClusteringResult {
    let edges = result
        .row_id_a
        .iter()
        .zip(result.row_id_b.iter())
        .map(|(&a, &b)| (a, b));

    cluster_edges(edges)
}

#[cfg(test)]
mod tests {
    use super::*;
    use lance_arrow::FixedSizeListArrayExt;
    use rstest::rstest;

    #[test]
    fn test_result_schemas_are_initialized_once() {
        // These schemas are handed out per call and per batch, so they are
        // shared rather than rebuilt. Pointer equality is what distinguishes a
        // shared schema from an equal-but-freshly-allocated one.
        assert!(Arc::ptr_eq(
            &ClusteringResult::schema(),
            &ClusteringResult::schema()
        ));

        let batch = PairwiseResult::default().into_record_batch();
        assert!(Arc::ptr_eq(&batch.schema(), &DISTANCE_PAIR_SCHEMA));
    }

    #[test]
    fn test_hamming() {
        let x = vec![0b1101_1010, 0b1010_1010, 0b1010_1010];
        let y = vec![0b1101_1010, 0b1010_1010, 0b1010_1010];
        assert_eq!(hamming(&x, &y), 0.0);

        let y = vec![0b1101_1010, 0b1010_1010, 0b1010_1000];
        assert_eq!(hamming(&x, &y), 1.0);

        let y = vec![0b1101_1010, 0b1010_1010, 0b1010_1001];
        assert_eq!(hamming(&x, &y), 2.0);
    }

    /// Lengths straddle the 16- and 64-byte steps of the NEON kernel; the
    /// longest ones run past the block after which it flushes its 16-bit
    /// accumulators, with every bit differing so each lane is at its maximum.
    /// Unequal inputs compare their common prefix, as `hamming_scalar` does.
    #[rstest]
    #[case::empty(0)]
    #[case::one(1)]
    #[case::fifteen(15)]
    #[case::sixteen(16)]
    #[case::seventeen(17)]
    #[case::sixty_three(63)]
    #[case::sixty_four(64)]
    #[case::sixty_five(65)]
    #[case::one_hundred_twenty_eight(128)]
    #[case::two_hundred(200)]
    #[case::one_flush(HAMMING_NEON_FLUSH_BYTES + 81)]
    #[case::two_flushes(2 * HAMMING_NEON_FLUSH_BYTES + 17)]
    fn test_hamming_kernels_match_reference(#[case] len: usize) {
        const EXTRA: usize = 17;
        let random: Vec<u8> = (0..(2 * len + EXTRA) as u64)
            .map(|i| (i.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 56) as u8)
            .collect();
        let zeros = vec![0; len];
        let ones = vec![u8::MAX; len + EXTRA];
        let inputs = [
            ("random", &random[..len], &random[len..2 * len]),
            ("all bits differ", &zeros[..], &ones[..len]),
            ("y longer", &zeros[..], &ones[..]),
            ("x longer", &ones[..], &zeros[..]),
            ("random y longer", &random[..len], &random[len..]),
        ];
        for (pattern, x, y) in inputs {
            let want = hamming_scalar(x, y);
            for candidate in HAMMING.candidates() {
                let Some(kernel) = candidate.kernel() else {
                    continue;
                };
                let got = kernel.call(x, y);
                assert_eq!(got, want, "{} [{pattern}] len={len}", candidate.name());
            }
            assert_eq!(hamming(x, y), want, "hamming [{pattern}] len={len}");
        }
    }

    #[rstest]
    #[case::one(1)]
    #[case::eight(8)]
    #[case::sixteen(16)]
    #[case::thirty_one(31)]
    #[case::sixty_four(64)]
    #[case::two_hundred(200)]
    fn test_hamming_batch_kernels_match_reference(#[case] dimension: usize) {
        const NUM_VECTORS: usize = 5;
        let random: Vec<u8> = (0..(NUM_VECTORS + 1) * dimension)
            .map(|i| ((i as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 56) as u8)
            .collect();
        let zeros = vec![0; dimension];
        let ones = vec![u8::MAX; NUM_VECTORS * dimension];
        let inputs = [
            ("random", &random[..dimension], &random[dimension..]),
            ("all bits differ", &zeros[..], &ones[..]),
        ];
        for (pattern, query, vectors) in inputs {
            let want: Vec<f32> = vectors
                .chunks_exact(dimension)
                .map(|vector| hamming_scalar(query, vector))
                .collect();
            for candidate in HAMMING_BATCH.candidates() {
                let Some(kernel) = candidate.kernel() else {
                    continue;
                };
                let mut got = vec![f32::NAN; NUM_VECTORS];
                kernel.call(query, vectors, dimension, &mut got);
                assert_eq!(
                    got,
                    want,
                    "{} [{pattern}] dimension={dimension}",
                    candidate.name()
                );
            }
            let mut got = vec![f32::NAN; NUM_VECTORS];
            hamming_distance_batch_into(query, vectors, dimension, &mut got);
            assert_eq!(
                got, want,
                "hamming_distance_batch_into [{pattern}] dimension={dimension}"
            );
        }
    }

    #[test]
    fn test_hamming_distance_batch_into() {
        let query = [0, u8::MAX];
        let targets = [0, u8::MAX, 1, u8::MAX - 1, u8::MAX, 0];
        let mut distances = [f32::NAN; 3];
        hamming_distance_batch_into(&query, &targets, query.len(), &mut distances);
        assert_eq!(distances, [0.0, 2.0, 16.0]);
    }

    #[test]
    #[should_panic(
        expected = "distance batch length must be divisible by dimension: batch=5, dimension=2"
    )]
    fn test_hamming_distance_batch_into_rejects_partial_vector() {
        let query = [0, 0];
        let targets = [u8::MAX, u8::MAX, 0, 1, 0];
        let mut distances = [0.0; 2];
        hamming_distance_batch_into(&query, &targets, query.len(), &mut distances);
    }

    #[test]
    #[should_panic(expected = "distance output length must match")]
    fn test_hamming_distance_batch_into_rejects_short_output() {
        let query = [0, 0];
        let targets = [u8::MAX; 4];
        let mut distances = [0.0; 1];
        hamming_distance_batch_into(&query, &targets, query.len(), &mut distances);
    }

    #[test]
    fn test_hamming_u64() {
        assert_eq!(hamming_u64(0, 0), 0);
        assert_eq!(hamming_u64(0, 1), 1);
        assert_eq!(hamming_u64(0b1111, 0b0000), 4);
        assert_eq!(hamming_u64(u64::MAX, 0), 64);
        assert_eq!(hamming_u64(0xAAAAAAAAAAAAAAAA, 0x5555555555555555), 64);
    }

    /// Runs `kernel` with `num_slots` result slots and 8 guard slots past
    /// them, then checks that it filled the first `min(targets, slots)` slots
    /// and wrote nothing else.
    ///
    /// 8 is the widest unchecked chunk store in the file, so a chunk store that
    /// overruns by one chunk lands inside this allocation instead of past it.
    fn check_kernel(
        query: u64,
        targets: &[u64],
        num_slots: usize,
        label: &str,
        kernel: impl FnOnce(u64, &[u64], &mut [u32]),
    ) {
        const GUARD: u32 = u32::MAX;
        const GUARD_SLOTS: usize = 8;
        let mut results = vec![GUARD; num_slots + GUARD_SLOTS];

        kernel(query, targets, &mut results[..num_slots]);

        let n = targets.len().min(num_slots);
        for (slot, &target) in targets[..n].iter().enumerate() {
            assert_eq!(
                results[slot],
                (query ^ target).count_ones(),
                "{label}: n={n}, slot {slot}"
            );
        }
        for (slot, &guard) in results.iter().enumerate().skip(n) {
            assert_eq!(
                guard, GUARD,
                "{label}: n={n}: wrote past results, at {slot}"
            );
        }
    }

    /// The x86 candidates take a fixed number of targets per chunk and finish
    /// the rest in a remainder loop: 8 for AVX-512, 4 for AVX2. The portable
    /// kernel's chunking is whatever LLVM vectorizes it to. The cases cover
    /// every residue of both widths, with and without a chunk ahead of it.
    #[rstest]
    #[case::one(1)]
    #[case::two(2)]
    #[case::three(3)]
    #[case::four(4)]
    #[case::five(5)]
    #[case::six(6)]
    #[case::seven(7)]
    #[case::eight(8)]
    #[case::nine(9)]
    #[case::fifteen(15)]
    #[case::sixteen(16)]
    #[case::seventeen(17)]
    #[case::one_hundred_twenty_eight(128)]
    fn test_hamming_batch_u64_kernels(#[case] n: usize) {
        let random: Vec<u64> = (0..n as u64)
            .map(|i| i.wrapping_mul(0x9E37_79B9_7F4A_7C15))
            .collect();
        let extremes: Vec<u64> = (0..n)
            .map(|i| if i % 2 == 0 { u64::MAX } else { 0 })
            .collect();
        let inputs = [
            ("random", 0xDEAD_BEEF_CAFE_F00D_u64, random),
            ("extremes", 0, extremes),
        ];

        for (pattern, query, targets) in &inputs {
            for candidate in HAMMING_BATCH_U64.candidates() {
                let Some(kernel) = candidate.kernel() else {
                    continue;
                };
                let label = format!("{} [{pattern}]", candidate.name());
                check_kernel(*query, targets, n, &label, |query, targets, results| {
                    kernel.call(query, targets, results)
                });
            }
            let label = format!("hamming_batch_u64 [{pattern}]");
            check_kernel(*query, targets, n, &label, hamming_batch_u64);
        }
    }

    /// The kernels are safe functions, so a length mismatch that
    /// `hamming_batch_u64` would reject must still stay in bounds.
    #[rstest]
    #[case::fewer_slots(17, 9)]
    #[case::more_slots(9, 17)]
    fn test_hamming_batch_u64_kernels_stay_in_bounds(
        #[case] num_targets: usize,
        #[case] num_slots: usize,
    ) {
        let targets: Vec<u64> = (0..num_targets as u64).map(|i| !i).collect();
        for candidate in HAMMING_BATCH_U64.candidates() {
            let Some(kernel) = candidate.kernel() else {
                continue;
            };
            check_kernel(0, &targets, num_slots, candidate.name(), |q, t, r| {
                kernel.call(q, t, r)
            });
        }
    }

    #[rstest]
    // Fewer slots than targets is the case that used to write out of bounds: on a
    // host that takes the AVX2 path, `hamming_batch_u64_avx2` wrote 28 bytes past a
    // one-slot `results`.
    #[case::eight_targets_one_slot(8, 1)]
    // On the same path this one overran by 4 bytes instead of 28.
    #[case::eight_targets_seven_slots(8, 7)]
    // More slots than targets left the tail unwritten rather than overrunning, but
    // the contract is one slot per target either way. This and the zero-target case
    // below are the two that catch a guard weakened to a one-sided
    // `results.len() >= targets.len()`.
    #[case::eight_targets_nine_slots(8, 9)]
    // The target count varies too, so that the two shortcuts most likely to appear
    // here cannot slip past every case: an `if targets.is_empty() { return; }` above
    // the check, or a `targets.len() < 8` fast path, would otherwise leave every
    // eight-target case green. Four is the smallest target count that still reaches
    // the AVX2 chunk loop.
    #[case::four_targets_one_slot(4, 1)]
    #[case::no_targets_one_slot(0, 1)]
    // Zero slots is the case with no in-bounds slot at all, and the only one here
    // that a `results.is_empty()` early return above the check would not survive.
    // Its sentinel assertion is vacuous, so it pins the message and nothing else.
    #[case::eight_targets_no_slots(8, 0)]
    fn test_hamming_batch_u64_rejects_mismatched_lengths(
        #[case] num_targets: usize,
        #[case] num_slots: usize,
    ) {
        // No CPU feature gate: the length check precedes every kernel, so the panic
        // happens on each host and no case reaches a kernel at all. Every case here is
        // a mismatch; matching lengths are covered by `test_hamming_batch_u64_kernels`
        // above, which calls `hamming_batch_u64` directly, and by
        // `test_pairwise_correctness_1000_deterministic` below, which reaches it
        // through `pairwise_hamming_distance_parallel`'s chunked path and checks the
        // result against `reference_pairwise`.
        //
        // `catch_unwind` rather than `#[should_panic]`, because the property worth
        // protecting is that the check runs before the dispatch, and
        // `#[should_panic]` cannot observe order: a check moved below it can still
        // panic with this same message, after a kernel has already written through
        // `results`. The sentinel assertion at the end is what rules that out. It is
        // also why the nine-slot case is here: a kernel's writes all land in bounds
        // there, so nothing else would notice that it ran.
        //
        // A `debug_assert_eq!` carrying the same message still passes here when
        // debug assertions are on, which is the profile `--profile ci` gives this
        // crate; `cargo test --release -p lance-linalg --lib` (rust.yml) is what
        // catches that revert.
        const SENTINEL: u32 = 0xDEAD_BEEF;
        let targets = vec![u64::MAX; num_targets];
        let mut results = vec![SENTINEL; num_slots];

        let payload = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            hamming_batch_u64(0, &targets, &mut results);
        }))
        .expect_err("a length mismatch must panic");

        let message = payload
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| payload.downcast_ref::<&'static str>().copied())
            .expect("panic payload should be a string");
        // Matching the whole message, not the two numbers separately, is deliberate:
        // it pins the function name and the order the two lengths are reported in,
        // which is what a message copied from a sibling guard would get wrong.
        let expected = format!(
            "hamming_batch_u64 needs one result slot per target, \
             got {num_targets} target(s) and {num_slots} slot(s)"
        );
        assert!(
            message.contains(&expected),
            "expected {expected:?} in panic message, got {message:?}"
        );
        assert_eq!(
            results.iter().position(|&slot| slot != SENTINEL),
            None,
            "a kernel reached `results`, so the length check no longer precedes it"
        );
    }

    #[test]
    fn test_pairwise_basic() {
        let hashes = vec![0b0000u64, 0b0001, 0b0011, 0b0111];
        let result = pairwise_hamming_distance(&hashes, None, None);

        assert_eq!(result.len(), 6); // C(4,2) = 6 pairs
        assert!(result.distances.iter().all(|&d| d <= 3));
    }

    #[test]
    fn test_pairwise_with_threshold() {
        let hashes = vec![0b0000u64, 0b0001, 0b1111];
        let result = pairwise_hamming_distance(&hashes, None, Some(1));

        assert_eq!(result.len(), 1);
        assert_eq!(result.row_id_a[0], 0);
        assert_eq!(result.row_id_b[0], 1);
        assert_eq!(result.distances[0], 1);
    }

    #[test]
    fn test_pairwise_with_row_ids() {
        let hashes = vec![0b0000u64, 0b0001];
        let row_ids = vec![100u64, 200u64];
        let result = pairwise_hamming_distance(&hashes, Some(&row_ids), None);

        assert_eq!(result.len(), 1);
        assert_eq!(result.row_id_a[0], 100);
        assert_eq!(result.row_id_b[0], 200);
    }

    #[test]
    fn test_pairwise_parallel() {
        let hashes: Vec<u64> = (0..100).collect();
        let result_seq = pairwise_hamming_distance(&hashes, None, None);
        let result_par = pairwise_hamming_distance_parallel(&hashes, None, None);

        assert_eq!(result_seq.len(), result_par.len());
    }

    #[test]
    fn test_union_find_basic() {
        let mut uf = UnionFind::new();

        assert_eq!(uf.find(1), 1);
        assert_eq!(uf.find(2), 2);
        assert_eq!(uf.find(3), 3);

        assert!(uf.union(1, 2));
        assert_eq!(uf.find(1), uf.find(2));

        assert!(uf.union(2, 3));
        assert_eq!(uf.find(1), uf.find(3));

        assert!(!uf.union(1, 3));
    }

    #[test]
    fn test_cluster_edges_simple() {
        let edges = vec![(1, 2), (2, 3), (4, 5)];
        let result = cluster_edges(edges);

        assert_eq!(result.num_clusters(), 2);

        let c1 = result
            .clusters
            .iter()
            .find(|c| c.representative == 1)
            .unwrap();
        assert_eq!(c1.duplicates.len(), 2);
        assert!(c1.duplicates.contains(&2));
        assert!(c1.duplicates.contains(&3));

        let c2 = result
            .clusters
            .iter()
            .find(|c| c.representative == 4)
            .unwrap();
        assert_eq!(c2.duplicates.len(), 1);
        assert!(c2.duplicates.contains(&5));
    }

    #[test]
    fn test_cluster_pairwise_result() {
        let hashes = vec![0b0000u64, 0b0001, 0b0011]; // distances: (0,1)=1, (0,2)=2, (1,2)=1
        let pairwise = pairwise_hamming_distance(&hashes, None, Some(1)); // threshold 1

        // Only pairs with distance <= 1: (0,1) and (1,2)
        assert_eq!(pairwise.len(), 2);

        let clustering = cluster_pairwise_result(&pairwise);
        // All three should be in one cluster since 0-1-2 are connected
        assert_eq!(clustering.num_clusters(), 1);
        assert_eq!(clustering.clusters[0].representative, 0);
        assert_eq!(clustering.clusters[0].duplicates.len(), 2);
    }

    #[test]
    fn test_into_record_batch() {
        let hashes = vec![0b0000u64, 0b0001, 0b0011];
        let result = pairwise_hamming_distance(&hashes, None, None);
        let batch = result.into_record_batch();

        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.num_columns(), 3);
        assert_eq!(batch.schema().field(0).name(), "row_id_a");
        assert_eq!(batch.schema().field(1).name(), "row_id_b");
        assert_eq!(batch.schema().field(2).name(), "distance");
    }

    // =========================================================================
    // Additional tests from pairwise-hamming reference implementation
    // =========================================================================

    /// Reference implementation for validation - simple O(n²) nested loop
    fn reference_pairwise(hashes: &[u64], threshold: Option<u32>) -> Vec<(usize, usize, u32)> {
        let threshold = threshold.unwrap_or(u32::MAX);
        let mut results = Vec::new();
        for i in 0..hashes.len() {
            for j in (i + 1)..hashes.len() {
                let dist = (hashes[i] ^ hashes[j]).count_ones();
                if dist <= threshold {
                    results.push((i, j, dist));
                }
            }
        }
        results
    }

    /// Convert PairwiseResult to sorted vec for comparison
    fn result_to_sorted_vec(result: &PairwiseResult) -> Vec<(u64, u64, u32)> {
        let mut v: Vec<_> = result
            .row_id_a
            .iter()
            .zip(result.row_id_b.iter())
            .zip(result.distances.iter())
            .map(|((&a, &b), &d)| (a, b, d))
            .collect();
        v.sort();
        v
    }

    #[test]
    fn test_extract_binary_hashes_from_fixed_list_128() {
        use arrow_array::UInt8Array;

        let rows = [
            (0x0102_0304_0506_0708u64, 0x1112_1314_1516_1718u64),
            (0x2122_2324_2526_2728u64, 0x3132_3334_3536_3738u64),
        ];
        let bytes: Vec<u8> = rows
            .iter()
            .flat_map(|(lo, hi)| lo.to_le_bytes().into_iter().chain(hi.to_le_bytes()))
            .collect();
        let array = FixedSizeListArray::try_new_from_values(UInt8Array::from(bytes), 16).unwrap();

        let hashes = extract_binary_hashes_from_fixed_list(&array).unwrap();
        assert_eq!(hashes.len(), 2);
        assert_eq!(hashes.byte_width(), 16);
        assert_eq!(hashes.num_lanes(), 2);
        assert_eq!(hashes.lane(0), &[rows[0].0, rows[1].0]);
        assert_eq!(hashes.lane(1), &[rows[0].1, rows[1].1]);
    }

    #[test]
    fn test_extract_binary_hashes_rejects_non_u64_multiple() {
        use arrow_array::UInt8Array;

        let array =
            FixedSizeListArray::try_new_from_values(UInt8Array::from(vec![0u8; 24]), 12).unwrap();
        let err = extract_binary_hashes_from_fixed_list(&array).unwrap_err();
        assert!(err.to_string().contains("multiple of 8 bytes"), "{}", err);
    }

    #[test]
    fn test_binary_hash_values_rejects_width_not_divisible_by_8() {
        let err = BinaryHashValues::try_new(Vec::new(), 0, 12).unwrap_err();
        assert!(err.to_string().contains("multiple of 8 bytes"), "{}", err);
    }

    #[test]
    fn test_pairwise_binary_hashes_128() {
        let hashes = BinaryHashValues::try_new(
            vec![
                0,
                0,
                1,
                u64::MAX, // lane 0
                0,
                1,
                1,
                u64::MAX, // lane 1
            ],
            4,
            16,
        )
        .unwrap();

        let seq = pairwise_hamming_distance_binary(&hashes, None, Some(1));
        let par = pairwise_hamming_distance_binary_parallel(&hashes, None, Some(1));
        let expected = vec![(0, 1, 1), (1, 2, 1)];
        assert_eq!(result_to_sorted_vec(&seq), expected);
        assert_eq!(result_to_sorted_vec(&par), expected);
    }

    /// 160 rows is past the sequential cutoff, so this reaches
    /// `process_row_range_binary`; 4 lanes reuse its lane buffer several
    /// times per row.
    #[rstest]
    #[case::bits_128(2)]
    #[case::bits_256(4)]
    fn test_pairwise_binary_hashes_parallel_matches_sequential(#[case] num_lanes: usize) {
        let rows: Vec<Vec<u64>> = (0..80)
            .flat_map(|i| {
                let lanes: Vec<u64> = (0..num_lanes)
                    .map(|lane| {
                        (i as u64 + 1)
                            .wrapping_mul(0x9E37_79B9_7F4A_7C15)
                            .rotate_left(17 * lane as u32)
                    })
                    .collect();
                let mut near = lanes.clone();
                near[num_lanes - 1] ^= 1;
                [lanes, near]
            })
            .collect();
        let lane_values: Vec<u64> = (0..num_lanes)
            .flat_map(|lane| rows.iter().map(move |row| row[lane]))
            .collect();
        let hashes = BinaryHashValues::try_new(lane_values, rows.len(), num_lanes * 8).unwrap();
        let row_ids: Vec<u64> = (0..rows.len()).map(|i| 10_000 + i as u64).collect();

        let seq = pairwise_hamming_distance_binary(&hashes, Some(&row_ids), Some(1));
        let par = pairwise_hamming_distance_binary_parallel(&hashes, Some(&row_ids), Some(1));

        assert_eq!(result_to_sorted_vec(&par), result_to_sorted_vec(&seq));
        assert!(par.len() >= 80);
    }

    /// Parallel chunks start past row 0, and the last chunk can hold only the
    /// final row, which has no later row to pair with.
    #[rstest]
    #[case::whole(0, 40)]
    #[case::middle(5, 23)]
    #[case::tail(30, 40)]
    #[case::last_row_only(39, 40)]
    fn test_process_row_range_matches_sequential(#[case] start_row: usize, #[case] end_row: usize) {
        const NUM_ROWS: usize = 40;
        const THRESHOLD: u32 = 40;
        let lane_values: Vec<u64> = (0..2 * NUM_ROWS as u64)
            .map(|i| i.wrapping_mul(0x9E37_79B9_7F4A_7C15))
            .collect();
        let hashes = BinaryHashValues::try_new(lane_values, NUM_ROWS, 16).unwrap();
        let row_ids: Vec<u64> = (0..NUM_ROWS as u64).map(|i| 1000 + i).collect();
        let in_range = |result: &PairwiseResult| -> Vec<(u64, u64, u32)> {
            result_to_sorted_vec(result)
                .into_iter()
                .filter(|&(a, _, _)| (start_row..end_row).contains(&((a - 1000) as usize)))
                .collect()
        };

        let lane = hashes.lane(0);
        let want = in_range(&pairwise_hamming_distance(
            lane,
            Some(&row_ids),
            Some(THRESHOLD),
        ));
        let got = process_row_range(lane, Some(&row_ids), THRESHOLD, start_row, end_row);
        assert_eq!(result_to_sorted_vec(&got), want);

        let want = in_range(&pairwise_hamming_distance_binary(
            &hashes,
            Some(&row_ids),
            Some(THRESHOLD),
        ));
        let got = process_row_range_binary(&hashes, Some(&row_ids), THRESHOLD, start_row, end_row);
        assert_eq!(result_to_sorted_vec(&got), want);
    }

    #[test]
    fn test_pairwise_binary_hashes_32_with_row_ids() {
        let hashes = BinaryHashValues::try_new(
            vec![
                0,
                0,
                1, // lane 0
                0,
                1,
                1, // lane 1
                7,
                7,
                7, // lane 2
                u64::MAX,
                u64::MAX,
                u64::MAX - 1, // lane 3
            ],
            3,
            32,
        )
        .unwrap();
        let row_ids = [10, 20, 30];

        let result = pairwise_hamming_distance_binary_parallel(&hashes, Some(&row_ids), Some(2));
        assert_eq!(
            result_to_sorted_vec(&result),
            vec![(10, 20, 1), (20, 30, 2)]
        );
    }

    #[test]
    fn test_pairwise_correctness_small() {
        // Deterministic hashes with known distances
        let hashes = vec![
            0b0000_0000u64, // 0
            0b0000_0001u64, // 1 bit from 0
            0b0000_0011u64, // 2 bits from 0, 1 bit from 1
            0b0000_0111u64, // 3 bits from 0, 2 bits from 1, 1 bit from 2
            0b0000_1111u64, // 4 bits from 0, 3 bits from 1, 2 bits from 2, 1 bit from 3
        ];

        let result = pairwise_hamming_distance(&hashes, None, None);
        let reference = reference_pairwise(&hashes, None);

        assert_eq!(result.len(), reference.len());
        assert_eq!(result.len(), 10); // C(5,2) = 10 pairs

        // Verify specific distances
        let result_vec = result_to_sorted_vec(&result);
        for (i, j, expected_dist) in &reference {
            let found = result_vec
                .iter()
                .find(|(a, b, _)| *a == *i as u64 && *b == *j as u64);
            assert!(found.is_some(), "Missing pair ({}, {})", i, j);
            assert_eq!(
                found.unwrap().2,
                *expected_dist,
                "Wrong distance for pair ({}, {})",
                i,
                j
            );
        }
    }

    /// Groups of four hashes one or two bits apart, so matches cluster within
    /// rows and the threshold separates them from the ~32-bit background.
    /// 200 hashes is past the sequential cutoff.
    #[rstest]
    #[case::exact_only(0)]
    #[case::one_bit(1)]
    #[case::two_bits(2)]
    #[case::half(32)]
    #[case::all_pairs(u32::MAX)]
    fn test_pairwise_parallel_threshold_matches_reference(#[case] threshold: u32) {
        let hashes: Vec<u64> = (0u64..200)
            .map(|i| {
                let base = (i / 4 + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15);
                base ^ ((1 << (i % 4)) >> 1)
            })
            .collect();
        let want: Vec<(u64, u64, u32)> = reference_pairwise(&hashes, Some(threshold))
            .into_iter()
            .map(|(i, j, dist)| (i as u64, j as u64, dist))
            .collect();
        let got = pairwise_hamming_distance_parallel(&hashes, None, Some(threshold));
        assert_eq!(result_to_sorted_vec(&got), want);
    }

    #[test]
    fn test_pairwise_correctness_1000_deterministic() {
        // Generate deterministic hashes using simple linear pattern
        let hashes: Vec<u64> = (0u64..1000)
            .map(|i| i.wrapping_mul(0x123456789ABCDEF))
            .collect();

        let result_seq = pairwise_hamming_distance(&hashes, None, Some(10));
        let result_par = pairwise_hamming_distance_parallel(&hashes, None, Some(10));
        let reference = reference_pairwise(&hashes, Some(10));

        // Both implementations should match reference
        assert_eq!(
            result_seq.len(),
            reference.len(),
            "Sequential result count mismatch"
        );
        assert_eq!(
            result_par.len(),
            reference.len(),
            "Parallel result count mismatch"
        );

        // Verify all pairs match
        let seq_sorted = result_to_sorted_vec(&result_seq);
        let par_sorted = result_to_sorted_vec(&result_par);

        for (i, j, dist) in &reference {
            let seq_found = seq_sorted
                .iter()
                .find(|(a, b, _)| *a == *i as u64 && *b == *j as u64);
            let par_found = par_sorted
                .iter()
                .find(|(a, b, _)| *a == *i as u64 && *b == *j as u64);

            assert!(
                seq_found.is_some(),
                "Sequential missing pair ({}, {})",
                i,
                j
            );
            assert!(par_found.is_some(), "Parallel missing pair ({}, {})", i, j);
            assert_eq!(seq_found.unwrap().2, *dist);
            assert_eq!(par_found.unwrap().2, *dist);
        }
    }

    #[test]
    fn test_pairwise_correctness_10000_deterministic() {
        // Larger test with 10K hashes
        let hashes: Vec<u64> = (0u64..10_000)
            .map(|i| {
                // Mix bits using a simple hash-like transformation
                let x = i.wrapping_mul(0xDEADBEEFCAFEBABE);
                x ^ (x >> 17) ^ (x << 13)
            })
            .collect();

        let result_seq = pairwise_hamming_distance(&hashes, None, Some(5));
        let result_par = pairwise_hamming_distance_parallel(&hashes, None, Some(5));

        // Both should find the same number of pairs
        assert_eq!(
            result_seq.len(),
            result_par.len(),
            "10K test: sequential found {} pairs, parallel found {} pairs",
            result_seq.len(),
            result_par.len()
        );

        // Verify they contain the same pairs (sorted comparison)
        let seq_sorted = result_to_sorted_vec(&result_seq);
        let par_sorted = result_to_sorted_vec(&result_par);
        assert_eq!(seq_sorted, par_sorted, "10K test: pair contents differ");
    }

    #[test]
    fn test_pairwise_total_pairs_count() {
        // Without threshold, should return exactly n*(n-1)/2 pairs
        for n in [10, 50, 100, 500] {
            let hashes: Vec<u64> = (0..n).map(|i| i as u64).collect();
            let result = pairwise_hamming_distance_parallel(&hashes, None, None);
            let expected = n * (n - 1) / 2;
            assert_eq!(
                result.len(),
                expected,
                "n={}: expected {} pairs, got {}",
                n,
                expected,
                result.len()
            );
        }
    }

    #[test]
    fn test_pairwise_threshold_filtering() {
        // All identical hashes should have distance 0
        let hashes = vec![0xABCDEF0123456789u64; 100];
        let result = pairwise_hamming_distance_parallel(&hashes, None, Some(0));

        // All pairs should be included (distance 0)
        assert_eq!(result.len(), 100 * 99 / 2);
        assert!(result.distances.iter().all(|&d| d == 0));

        // With threshold 0 and all different hashes, should find fewer pairs
        let different_hashes: Vec<u64> = (0u64..100).collect();
        let result2 = pairwise_hamming_distance_parallel(&different_hashes, None, Some(0));
        // Only pairs with identical values should match (none in this case except 0^0)
        assert!(result2.len() < 100 * 99 / 2);
    }

    #[test]
    fn test_pairwise_row_ids_preserved() {
        let hashes: Vec<u64> = (0u64..100).collect();
        let row_ids: Vec<u64> = (1000u64..1100).collect(); // offset row IDs

        let result = pairwise_hamming_distance_parallel(&hashes, Some(&row_ids), Some(5));

        // All row IDs should be in range [1000, 1100)
        for &id in &result.row_id_a {
            assert!((1000..1100).contains(&id), "row_id_a {} out of range", id);
        }
        for &id in &result.row_id_b {
            assert!((1000..1100).contains(&id), "row_id_b {} out of range", id);
        }
        // row_id_a should always be less than row_id_b (upper triangular)
        for (&a, &b) in result.row_id_a.iter().zip(result.row_id_b.iter()) {
            assert!(a < b, "Expected row_id_a < row_id_b, got {} >= {}", a, b);
        }
    }

    #[test]
    fn test_pairwise_distance_bounds() {
        // All distances should be in [0, 64] for u64 hashes
        let hashes: Vec<u64> = (0u64..1000).map(|i| i.wrapping_mul(0x123456789)).collect();

        let result = pairwise_hamming_distance_parallel(&hashes, None, None);

        for &d in &result.distances {
            assert!(d <= 64, "Distance {} exceeds maximum 64", d);
        }
    }

    #[test]
    fn test_pairwise_symmetry() {
        // Hamming distance is symmetric: d(a,b) = d(b,a)
        let hashes: Vec<u64> = vec![
            0x0000000000000000,
            0xFFFFFFFFFFFFFFFF,
            0xAAAAAAAAAAAAAAAA,
            0x5555555555555555,
            0x123456789ABCDEF0,
        ];

        let result = pairwise_hamming_distance(&hashes, None, None);

        // For each pair (i,j), verify distance matches manual calculation
        for idx in 0..result.len() {
            let i = result.row_id_a[idx] as usize;
            let j = result.row_id_b[idx] as usize;
            let dist = result.distances[idx];

            let expected = (hashes[i] ^ hashes[j]).count_ones();
            assert_eq!(dist, expected, "Distance mismatch for pair ({}, {})", i, j);
        }
    }

    #[test]
    fn test_balanced_chunks() {
        // Verify chunks are reasonably balanced
        let n = 10000;
        let total_pairs = n * (n - 1) / 2;
        let target_per_chunk = total_pairs / 16;

        let chunks = compute_balanced_chunks(n, target_per_chunk);

        // Should have roughly 16 chunks
        assert!(
            chunks.len() >= 14 && chunks.len() <= 18,
            "Expected ~16 chunks, got {}",
            chunks.len()
        );

        // Each chunk should have roughly equal work
        for (start, end) in &chunks {
            let mut chunk_pairs = 0usize;
            for i in *start..*end {
                chunk_pairs += n - i - 1;
            }
            // Allow 20% deviation from target
            let lower = target_per_chunk * 80 / 100;
            // last chunk may be smaller
            assert!(
                chunk_pairs >= lower || *end == n,
                "Chunk [{}, {}) has {} pairs, expected ~{}",
                start,
                end,
                chunk_pairs,
                target_per_chunk
            );
        }

        // Chunks should cover all rows without gaps
        assert_eq!(chunks[0].0, 0);
        assert_eq!(chunks.last().unwrap().1, n);
        for i in 1..chunks.len() {
            assert_eq!(chunks[i].0, chunks[i - 1].1, "Gap between chunks");
        }
    }

    // =========================================================================
    // Additional clustering tests
    // =========================================================================

    #[test]
    fn test_union_find_path_compression() {
        let mut uf = UnionFind::new();

        // Create a chain: 1 -> 2 -> 3 -> 4 -> 5
        uf.union(4, 5);
        uf.union(3, 4);
        uf.union(2, 3);
        uf.union(1, 2);

        // All should have the same root
        let root = uf.find(1);
        assert_eq!(uf.find(2), root);
        assert_eq!(uf.find(3), root);
        assert_eq!(uf.find(4), root);
        assert_eq!(uf.find(5), root);
    }

    #[test]
    fn test_cluster_edges_single_cluster() {
        // All connected: 1-2-3-4-5
        let edges = vec![(1, 2), (2, 3), (3, 4), (4, 5)];
        let result = cluster_edges(edges);

        assert_eq!(result.num_clusters(), 1);
        let cluster = &result.clusters[0];
        assert_eq!(cluster.representative, 1);
        assert_eq!(cluster.duplicates.len(), 4);
        assert_eq!(cluster.size(), 5);
    }

    #[test]
    fn test_cluster_edges_no_duplicates() {
        // No edges means no clusters
        let edges: Vec<(u64, u64)> = vec![];
        let result = cluster_edges(edges);

        assert_eq!(result.num_clusters(), 0);
        assert_eq!(result.num_duplicates(), 0);
    }

    #[test]
    fn test_cluster_edges_self_loop() {
        // Self-loop shouldn't create a cluster (size 1)
        let edges = vec![(1, 1), (2, 3)];
        let result = cluster_edges(edges);

        // Only {2,3} should be a cluster
        assert_eq!(result.num_clusters(), 1);
        assert_eq!(result.clusters[0].representative, 2);
    }

    #[test]
    fn test_cluster_edges_duplicate_edges() {
        // Duplicate edges should be handled correctly
        let edges = vec![(1, 2), (1, 2), (2, 3), (2, 3), (3, 1)];
        let result = cluster_edges(edges);

        assert_eq!(result.num_clusters(), 1);
        assert_eq!(result.clusters[0].size(), 3);
    }

    #[test]
    fn test_cluster_edges_large() {
        // Create 100 clusters of size 10 each
        let mut edges = Vec::new();
        for cluster_id in 0..100u64 {
            let base = cluster_id * 10;
            for i in 0..9 {
                edges.push((base + i, base + i + 1));
            }
        }

        let result = cluster_edges(edges);

        assert_eq!(result.num_clusters(), 100);
        for cluster in &result.clusters {
            assert_eq!(cluster.size(), 10);
            assert_eq!(cluster.duplicates.len(), 9);
        }
    }

    #[test]
    fn test_cluster_edges_random_order() {
        // Same edges in different order should produce same result
        let edges1 = vec![(1, 2), (2, 3), (4, 5), (3, 4)];
        let edges2 = vec![(4, 5), (1, 2), (3, 4), (2, 3)];
        let edges3 = vec![(3, 4), (4, 5), (2, 3), (1, 2)];

        let r1 = cluster_edges(edges1);
        let r2 = cluster_edges(edges2);
        let r3 = cluster_edges(edges3);

        // All should produce the same single cluster
        assert_eq!(r1.num_clusters(), 1);
        assert_eq!(r2.num_clusters(), 1);
        assert_eq!(r3.num_clusters(), 1);

        assert_eq!(r1.clusters[0].representative, 1);
        assert_eq!(r2.clusters[0].representative, 1);
        assert_eq!(r3.clusters[0].representative, 1);

        assert_eq!(r1.clusters[0].size(), 5);
        assert_eq!(r2.clusters[0].size(), 5);
        assert_eq!(r3.clusters[0].size(), 5);
    }

    #[test]
    fn test_cluster_edges_non_contiguous_ids() {
        // Row IDs don't need to be contiguous
        let edges = vec![(100, 200), (200, 500), (1000, 2000)];
        let result = cluster_edges(edges);

        assert_eq!(result.num_clusters(), 2);

        let c1 = result
            .clusters
            .iter()
            .find(|c| c.representative == 100)
            .unwrap();
        assert_eq!(c1.duplicates, vec![200, 500]);

        let c2 = result
            .clusters
            .iter()
            .find(|c| c.representative == 1000)
            .unwrap();
        assert_eq!(c2.duplicates, vec![2000]);
    }

    #[test]
    fn test_cluster_representative_is_minimum() {
        // Representative should always be the minimum row ID in cluster
        let edges = vec![
            (5, 3),
            (3, 7),
            (7, 1), // 1 is minimum
            (100, 50),
            (50, 75), // 50 is minimum
        ];
        let result = cluster_edges(edges);

        assert_eq!(result.num_clusters(), 2);

        let c1 = result
            .clusters
            .iter()
            .find(|c| c.duplicates.contains(&7))
            .unwrap();
        assert_eq!(c1.representative, 1);

        let c2 = result
            .clusters
            .iter()
            .find(|c| c.duplicates.contains(&100))
            .unwrap();
        assert_eq!(c2.representative, 50);
    }

    #[test]
    fn test_cluster_duplicates_sorted() {
        // Duplicates should be sorted
        let edges = vec![(1, 5), (1, 3), (1, 7), (1, 2)];
        let result = cluster_edges(edges);

        assert_eq!(result.num_clusters(), 1);
        assert_eq!(result.clusters[0].representative, 1);
        assert_eq!(result.clusters[0].duplicates, vec![2, 3, 5, 7]);
    }

    #[test]
    fn test_clustering_result_stats() {
        let edges = vec![
            (1, 2),
            (2, 3), // cluster of 3
            (10, 20),
            (20, 30),
            (30, 40), // cluster of 4
        ];
        let result = cluster_edges(edges);

        assert_eq!(result.num_clusters(), 2);
        assert_eq!(result.num_duplicates(), 5); // 2 + 3
        assert_eq!(result.num_unique(), 2);
    }
}
