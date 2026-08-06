// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Hamming distance.
//!
//! This module provides hamming distance computation for binary vectors,
//! including SIMD-accelerated pairwise hamming distance for binary hashes.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::builder::{ListBuilder, UInt64Builder};
use arrow_array::cast::AsArray;
use arrow_array::types::UInt8Type;
use arrow_array::{
    Array, ArrayRef, FixedSizeListArray, Float32Array, RecordBatch, RecordBatchIterator,
    RecordBatchReader, UInt32Array, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use rayon::prelude::*;

use crate::{Error, Result};

pub trait Hamming {
    /// Hamming distance between two vectors.
    fn hamming(x: &[u8], y: &[u8]) -> f32;
}

/// Hamming distance between two vectors.
#[inline]
pub fn hamming(x: &[u8], y: &[u8]) -> f32 {
    hamming_autovec::<64>(x, y)
}

#[inline]
fn hamming_autovec<const L: usize>(x: &[u8], y: &[u8]) -> f32 {
    let x_chunk = x.chunks_exact(L);
    let y_chunk = y.chunks_exact(L);
    let sum = x_chunk
        .remainder()
        .iter()
        .zip(y_chunk.remainder())
        .map(|(&a, &b)| (a ^ b).count_ones())
        .sum::<u32>();
    (sum + x_chunk
        .zip(y_chunk)
        .map(|(x, y)| {
            x.iter()
                .zip(y.iter())
                .map(|(&a, &b)| (a ^ b).count_ones())
                .sum::<u32>()
        })
        .sum::<u32>()) as f32
}

/// Scalar version of hamming distance. Used for benchmarks.
#[inline]
pub fn hamming_scalar(x: &[u8], y: &[u8]) -> f32 {
    x.iter()
        .zip(y.iter())
        .map(|(xi, yi)| (xi ^ yi).count_ones())
        .sum::<u32>() as f32
}

pub fn hamming_distance_batch<'a>(
    from: &'a [u8],
    to: &'a [u8],
    dimension: usize,
) -> Box<dyn Iterator<Item = f32> + 'a> {
    debug_assert_eq!(from.len(), dimension);
    debug_assert_eq!(to.len() % dimension, 0);
    Box::new(to.chunks_exact(dimension).map(|v| hamming(from, v)))
}

pub fn hamming_distance_arrow_batch(
    from: &dyn Array,
    to: &FixedSizeListArray,
) -> Result<Arc<Float32Array>> {
    let dists = match *from.data_type() {
        DataType::UInt8 => hamming_distance_batch(
            from.as_primitive::<UInt8Type>().values(),
            to.values().as_primitive::<UInt8Type>().values(),
            from.len(),
        ),
        _ => {
            return Err(Error::InvalidArgumentError(format!(
                "Unsupported data type: {:?}",
                from.data_type()
            )));
        }
    };

    Ok(Arc::new(Float32Array::new(
        dists.collect(),
        to.nulls().cloned(),
    )))
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
        let schema = Arc::new(Schema::new(vec![
            Field::new("row_id_a", DataType::UInt64, false),
            Field::new("row_id_b", DataType::UInt64, false),
            Field::new("distance", DataType::UInt32, false),
        ]));

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

/// Compute hamming distances for a query against multiple targets.
/// Uses SIMD acceleration when available.
#[inline]
pub fn hamming_batch_u64(query: u64, targets: &[u64], results: &mut [u32]) {
    debug_assert_eq!(targets.len(), results.len());
    hamming_batch_simd(query, targets, results);
}

/// SIMD-accelerated batch hamming distance computation.
#[inline]
fn hamming_batch_simd(query: u64, targets: &[u64], results: &mut [u32]) {
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx512vpopcntdq") && is_x86_feature_detected!("avx512f") {
            unsafe {
                hamming_batch_avx512(query, targets, results);
            }
            return;
        }
        if is_x86_feature_detected!("avx2") {
            unsafe {
                hamming_batch_avx2(query, targets, results);
            }
            return;
        }
    }

    // Scalar fallback (LLVM auto-vectorizes well on Apple Silicon)
    hamming_batch_scalar(query, targets, results);
}

/// Scalar fallback using count_ones() which compiles to POPCNT.
#[inline]
fn hamming_batch_scalar(query: u64, targets: &[u64], results: &mut [u32]) {
    // Unroll for better auto-vectorization
    let n = targets.len();
    let chunks = n / 8;
    let mut i = 0;

    for _ in 0..chunks {
        results[i] = (query ^ targets[i]).count_ones();
        results[i + 1] = (query ^ targets[i + 1]).count_ones();
        results[i + 2] = (query ^ targets[i + 2]).count_ones();
        results[i + 3] = (query ^ targets[i + 3]).count_ones();
        results[i + 4] = (query ^ targets[i + 4]).count_ones();
        results[i + 5] = (query ^ targets[i + 5]).count_ones();
        results[i + 6] = (query ^ targets[i + 6]).count_ones();
        results[i + 7] = (query ^ targets[i + 7]).count_ones();
        i += 8;
    }

    // Handle remainder
    while i < n {
        results[i] = (query ^ targets[i]).count_ones();
        i += 1;
    }
}

/// AVX-512 VPOPCNTDQ: Process 8 x 64-bit values at once.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f", enable = "avx512vpopcntdq")]
unsafe fn hamming_batch_avx512(query: u64, targets: &[u64], results: &mut [u32]) {
    use std::arch::x86_64::*;

    let n = targets.len();
    let query_vec = _mm512_set1_epi64(query as i64);

    let chunks = n / 8;
    let remainder = n % 8;

    for i in 0..chunks {
        let offset = i * 8;
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

    if remainder > 0 {
        let offset = chunks * 8;
        for j in 0..remainder {
            results[offset + j] = (query ^ targets[offset + j]).count_ones();
        }
    }
}

/// AVX2 popcount using lookup table (Harley-Seal / PSHUFB method).
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn hamming_batch_avx2(query: u64, targets: &[u64], results: &mut [u32]) {
    use std::arch::x86_64::*;

    let n = targets.len();

    let lookup = _mm256_setr_epi8(
        0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4, 0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3,
        3, 4,
    );
    let low_mask = _mm256_set1_epi8(0x0f);
    let query_vec = _mm256_set1_epi64x(query as i64);

    let chunks = n / 4;
    let remainder = n % 4;

    for i in 0..chunks {
        let offset = i * 4;
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

    if remainder > 0 {
        let offset = chunks * 4;
        for j in 0..remainder {
            results[offset + j] = (query ^ targets[offset + j]).count_ones();
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

    for i in start_row..end_row {
        let remaining = n - i - 1;
        if remaining == 0 {
            continue;
        }

        let mut distances = vec![0u32; remaining];
        hamming_batch_u64(hashes[i], &hashes[i + 1..], &mut distances);

        let id_a = row_ids.map_or(i as u64, |ids| ids[i]);
        for (j_offset, &dist) in distances.iter().enumerate() {
            if dist <= threshold {
                let j = i + 1 + j_offset;
                let id_b = row_ids.map_or(j as u64, |ids| ids[j]);
                result.push(id_a, id_b, dist);
            }
        }
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
    let mut distances = Vec::new();
    let mut lane_distances = Vec::new();

    for i in start_row..end_row {
        let remaining = n - i - 1;
        if remaining == 0 {
            continue;
        }

        distances.clear();
        distances.resize(remaining, 0);

        let first_lane = hashes.lane(0);
        hamming_batch_u64(
            first_lane[i],
            &first_lane[i + 1..],
            distances.as_mut_slice(),
        );

        for lane in 1..hashes.num_lanes() {
            lane_distances.clear();
            lane_distances.resize(remaining, 0);
            let lane_values = hashes.lane(lane);
            hamming_batch_u64(
                lane_values[i],
                &lane_values[i + 1..],
                lane_distances.as_mut_slice(),
            );
            for (distance, lane_distance) in distances.iter_mut().zip(&lane_distances) {
                *distance += *lane_distance;
            }
        }

        let id_a = row_ids.map_or(i as u64, |ids| ids[i]);
        for (j_offset, &dist) in distances.iter().enumerate() {
            if dist <= threshold {
                let j = i + 1 + j_offset;
                let id_b = row_ids.map_or(j as u64, |ids| ids[j]);
                result.push(id_a, id_b, dist);
            }
        }
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
        Arc::new(Schema::new(vec![
            Field::new("representative", DataType::UInt64, false),
            Field::new(
                "duplicates",
                DataType::List(Arc::new(Field::new("item", DataType::UInt64, true))),
                false,
            ),
        ]))
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

    #[test]
    fn test_hamming_u64() {
        assert_eq!(hamming_u64(0, 0), 0);
        assert_eq!(hamming_u64(0, 1), 1);
        assert_eq!(hamming_u64(0b1111, 0b0000), 4);
        assert_eq!(hamming_u64(u64::MAX, 0), 64);
        assert_eq!(hamming_u64(0xAAAAAAAAAAAAAAAA, 0x5555555555555555), 64);
    }

    #[test]
    fn test_hamming_batch_u64() {
        let query = 0u64;
        let targets: Vec<u64> = (0..128).collect();
        let mut results = vec![0u32; 128];

        hamming_batch_u64(query, &targets, &mut results);

        assert_eq!(results[0], 0);
        assert_eq!(results[1], 1);
        assert_eq!(results[3], 2); // 0b11 has 2 bits set
        assert_eq!(results[7], 3); // 0b111 has 3 bits set
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

    #[test]
    fn test_pairwise_binary_hashes_parallel_128_matches_sequential() {
        let rows: Vec<(u64, u64)> = (0..80)
            .flat_map(|i| {
                let lo = (i as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
                let hi = !lo.rotate_left(17);
                [(lo, hi), (lo ^ 1, hi)]
            })
            .collect();
        let mut lane_values = Vec::with_capacity(rows.len() * 2);
        lane_values.extend(rows.iter().map(|(lo, _)| *lo));
        lane_values.extend(rows.iter().map(|(_, hi)| *hi));
        let hashes = BinaryHashValues::try_new(lane_values, rows.len(), 16).unwrap();
        let row_ids: Vec<u64> = (0..rows.len()).map(|i| 10_000 + i as u64).collect();

        let seq = pairwise_hamming_distance_binary(&hashes, Some(&row_ids), Some(1));
        let par = pairwise_hamming_distance_binary_parallel(&hashes, Some(&row_ids), Some(1));

        assert_eq!(result_to_sorted_vec(&par), result_to_sorted_vec(&seq));
        assert!(par.len() >= 80);
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
    // SIMD-specific tests
    // =========================================================================

    #[test]
    #[cfg(target_arch = "x86_64")]
    fn test_avx2_popcount() {
        if !is_x86_feature_detected!("avx2") {
            return;
        }

        let query = 0u64;
        let targets = vec![0u64, 1, 3, 7, 15, 31, 63, 127];
        let mut results = vec![0u32; 8];

        unsafe {
            hamming_batch_avx2(query, &targets, &mut results);
        }

        assert_eq!(results[0], 0); // 0 ^ 0 = 0 bits
        assert_eq!(results[1], 1); // 0 ^ 1 = 1 bit
        assert_eq!(results[2], 2); // 0 ^ 3 = 2 bits
        assert_eq!(results[3], 3); // 0 ^ 7 = 3 bits
        assert_eq!(results[4], 4); // 0 ^ 15 = 4 bits
        assert_eq!(results[5], 5); // 0 ^ 31 = 5 bits
        assert_eq!(results[6], 6); // 0 ^ 63 = 6 bits
        assert_eq!(results[7], 7); // 0 ^ 127 = 7 bits
    }

    #[test]
    #[cfg(target_arch = "x86_64")]
    fn test_avx2_max_distance() {
        if !is_x86_feature_detected!("avx2") {
            return;
        }

        let query = 0u64;
        let targets = vec![u64::MAX; 4];
        let mut results = vec![0u32; 4];

        unsafe {
            hamming_batch_avx2(query, &targets, &mut results);
        }

        for &r in &results {
            assert_eq!(r, 64);
        }
    }

    #[test]
    #[cfg(target_arch = "x86_64")]
    fn test_avx512_popcount() {
        if !is_x86_feature_detected!("avx512vpopcntdq") || !is_x86_feature_detected!("avx512f") {
            return;
        }

        let query = 0u64;
        let targets = vec![0u64, 1, 3, 7, 15, 31, 63, 127];
        let mut results = vec![0u32; 8];

        unsafe {
            hamming_batch_avx512(query, &targets, &mut results);
        }

        assert_eq!(results[0], 0);
        assert_eq!(results[1], 1);
        assert_eq!(results[2], 2);
        assert_eq!(results[3], 3);
        assert_eq!(results[4], 4);
        assert_eq!(results[5], 5);
        assert_eq!(results[6], 6);
        assert_eq!(results[7], 7);
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
