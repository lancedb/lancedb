// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

#[cfg(target_arch = "aarch64")]
use std::arch::aarch64::*;
#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::*;

#[allow(unused_imports)]
use lance_core::utils::cpu::{SIMD_SUPPORT, SimdSupport};

pub const PERM0: [usize; 16] = [0, 8, 1, 9, 2, 10, 3, 11, 4, 12, 5, 13, 6, 14, 7, 15];
pub const PERM0_INVERSE: [usize; 16] = [0, 2, 4, 6, 8, 10, 12, 14, 1, 3, 5, 7, 9, 11, 13, 15];
pub const BATCH_SIZE: usize = 32;

// This function is used to sum the distance table for 4-bit codes.
// the distance table is a 2D array, that dist_table[i][j] is the distance between the i-th subvector and the code j,
// the distance table is stored as a flat array for better cache locality and SIMD instruction usage.
//
// The codes are organized in the order of PERM0:
// +----------+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+
// | address  |  0 |  1 |  2 |  3 |  4 |  5 |  6 |  7 |  8 |  9 | 10 | 11 | 12 | 13 | 14 | 15 |
// | (bytes)  |    |    |    |    |    |    |    |    |    |    |    |    |    |    |    |    |
// +----------+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+
// | bits 0..3|  0 |  8 |  1 |  9 |  2 | 10 |  3 | 11 |  4 | 12 |  5 | 13 |  6 | 14 |  7 | 15 |
// | bits 4..7| 16 | 24 | 17 | 25 | 18 | 26 | 19 | 27 | 20 | 28 | 21 | 29 | 22 | 30 | 23 | 31 |
// +----------+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+----+
// so that we can use SIMD instruction (especially _mm256_shuffle_epi8) to do the summation.
#[inline]
pub fn sum_4bit_dist_table(
    n: usize,
    code_len: usize,
    codes: &[u8],
    dist_table: &[u8],
    dists: &mut [u16],
) {
    debug_assert!(n.is_multiple_of(BATCH_SIZE));

    match *SIMD_SUPPORT {
        #[cfg(all(kernel_support = "avx512_dist_table", target_arch = "x86_64"))]
        SimdSupport::Avx512 | SimdSupport::Avx512FP16
            if std::arch::is_x86_feature_detected!("avx512bw") =>
        {
            for i in (0..n).step_by(BATCH_SIZE) {
                let codes = &codes[i * code_len..(i + BATCH_SIZE) * code_len];
                unsafe {
                    sum_4bit_dist_table_32bytes_batch_avx512(
                        codes.as_ptr(),
                        codes.len(),
                        dist_table.as_ptr(),
                        dists[i..i + BATCH_SIZE].as_mut_ptr(),
                    )
                }
            }
        }
        #[cfg(target_arch = "x86_64")]
        SimdSupport::Avx2 => unsafe {
            for i in (0..n).step_by(BATCH_SIZE) {
                sum_dist_table_32bytes_batch_avx2(
                    &codes[i * code_len..(i + BATCH_SIZE) * code_len],
                    dist_table,
                    &mut dists[i..i + BATCH_SIZE],
                )
            }
        },
        #[cfg(target_arch = "aarch64")]
        SimdSupport::Neon => unsafe {
            for i in (0..n).step_by(BATCH_SIZE) {
                sum_dist_table_32bytes_batch_neon(
                    &codes[i * code_len..(i + BATCH_SIZE) * code_len],
                    dist_table,
                    &mut dists[i..i + BATCH_SIZE],
                )
            }
        },
        // SimdSupport::AvxFma and SimdSupport::Avx fall through here:
        // the AVX2 inner uses `_mm256_shuffle_epi8` / `_mm256_and_si256` /
        // `_mm256_srli_epi16` / `_mm256_add_epi16` integer ops which
        // neither AVX nor AVX+FMA provides. Scalar is the correct route.
        _ => sum_4bit_dist_table_scalar(code_len, codes, dist_table, dists),
    }
}

#[inline]
#[allow(unused)]
pub fn sum_4bit_dist_table_scalar(
    code_len: usize,
    codes: &[u8],
    dist_table: &[u8],
    dists: &mut [u16],
) {
    let num_full_vectors = codes.len() / (BATCH_SIZE * code_len) * BATCH_SIZE;
    dists[..num_full_vectors].fill(0);

    for (vec_block_idx, blocks) in codes.chunks_exact(BATCH_SIZE * code_len).enumerate() {
        for (sub_vec_idx, block) in blocks.chunks_exact(BATCH_SIZE).enumerate() {
            let current_dist_table = &dist_table[sub_vec_idx * 2 * 16..(sub_vec_idx * 2 + 1) * 16];
            let next_dist_table =
                &dist_table[(sub_vec_idx * 2 + 1) * 16..(sub_vec_idx * 2 + 2) * 16];

            for j in 0..16 {
                let low_current_code = (block[j] & 0x0F) as usize;
                let high_current_code = (block[j] >> 4) as usize;
                let low_next_code = (block[j + 16] & 0x0F) as usize;
                let high_next_code = (block[j + 16] >> 4) as usize;

                let lower_id = vec_block_idx * BATCH_SIZE + PERM0[j];
                let higher_id = vec_block_idx * BATCH_SIZE + PERM0[j] + 16;
                dists[lower_id] = dists[lower_id]
                    .saturating_add(current_dist_table[low_current_code] as u16)
                    .saturating_add(next_dist_table[low_next_code] as u16);
                dists[higher_id] = dists[higher_id]
                    .saturating_add(current_dist_table[high_current_code] as u16)
                    .saturating_add(next_dist_table[high_next_code] as u16);
            }
        }
    }
}

#[inline]
#[allow(unused)]
pub fn sum_4bit_dist_table_u16(
    n: usize,
    code_len: usize,
    codes: &[u8],
    dist_table: &[u16],
    dists: &mut [u32],
) {
    debug_assert!(n.is_multiple_of(BATCH_SIZE));
    debug_assert!(dists.len() >= n);
    debug_assert!(codes.len() >= n * code_len);
    sum_4bit_dist_table_u16_scalar(
        code_len,
        &codes[..n * code_len],
        dist_table,
        &mut dists[..n],
    );
}

#[inline]
pub fn transfer_4bit_dist_table_u16(dist_table: &[u16], hacc_dist_table: &mut Vec<u8>) {
    debug_assert!(dist_table.len().is_multiple_of(32));

    let num_tables = dist_table.len() / 16;
    hacc_dist_table.clear();
    hacc_dist_table.resize(dist_table.len() * 2, 0);

    for table_idx in 0..num_tables {
        let table = &dist_table[table_idx * 16..(table_idx + 1) * 16];
        let low_offset = (table_idx / 2) * 64 + (table_idx % 2) * 16;
        let high_offset = low_offset + 32;
        for (code, value) in table.iter().enumerate() {
            hacc_dist_table[low_offset + code] = *value as u8;
            hacc_dist_table[high_offset + code] = (value >> 8) as u8;
        }
    }
}

#[inline]
pub fn sum_4bit_hacc_dist_table(
    n: usize,
    code_len: usize,
    codes: &[u8],
    hacc_dist_table: &[u8],
    dists: &mut [u32],
) {
    debug_assert!(n.is_multiple_of(BATCH_SIZE));
    debug_assert!(dists.len() >= n);
    debug_assert!(codes.len() >= n * code_len);
    debug_assert!(hacc_dist_table.len() >= code_len * 64);

    match *SIMD_SUPPORT {
        #[cfg(target_arch = "x86_64")]
        SimdSupport::Avx512 | SimdSupport::Avx512FP16 | SimdSupport::Avx2
            if std::arch::is_x86_feature_detected!("avx2") =>
        {
            sum_4bit_hacc_dist_table_avx2(n, code_len, codes, hacc_dist_table, dists);
        }
        _ => sum_4bit_hacc_dist_table_scalar(code_len, codes, hacc_dist_table, dists),
    }
}

#[inline]
#[allow(unused)]
pub fn sum_4bit_hacc_dist_table_scalar(
    code_len: usize,
    codes: &[u8],
    hacc_dist_table: &[u8],
    dists: &mut [u32],
) {
    let num_full_vectors = codes.len() / (BATCH_SIZE * code_len) * BATCH_SIZE;
    dists[..num_full_vectors].fill(0);

    for (vec_block_idx, blocks) in codes.chunks_exact(BATCH_SIZE * code_len).enumerate() {
        for (sub_vec_idx, block) in blocks.chunks_exact(BATCH_SIZE).enumerate() {
            let table_offset = sub_vec_idx * 64;
            let current_low = &hacc_dist_table[table_offset..table_offset + 16];
            let next_low = &hacc_dist_table[table_offset + 16..table_offset + 32];
            let current_high = &hacc_dist_table[table_offset + 32..table_offset + 48];
            let next_high = &hacc_dist_table[table_offset + 48..table_offset + 64];

            for j in 0..16 {
                let low_current_code = (block[j] & 0x0F) as usize;
                let high_current_code = (block[j] >> 4) as usize;
                let low_next_code = (block[j + 16] & 0x0F) as usize;
                let high_next_code = (block[j + 16] >> 4) as usize;

                let lower_id = vec_block_idx * BATCH_SIZE + PERM0[j];
                let higher_id = lower_id + 16;
                dists[lower_id] += ((current_high[low_current_code] as u32) << 8)
                    + current_low[low_current_code] as u32
                    + ((next_high[low_next_code] as u32) << 8)
                    + next_low[low_next_code] as u32;
                dists[higher_id] += ((current_high[high_current_code] as u32) << 8)
                    + current_low[high_current_code] as u32
                    + ((next_high[high_next_code] as u32) << 8)
                    + next_low[high_next_code] as u32;
            }
        }
    }
}

#[inline]
#[allow(unused)]
pub fn sum_4bit_dist_table_u16_scalar(
    code_len: usize,
    codes: &[u8],
    dist_table: &[u16],
    dists: &mut [u32],
) {
    let num_full_vectors = codes.len() / (BATCH_SIZE * code_len) * BATCH_SIZE;
    dists[..num_full_vectors].fill(0);

    for (vec_block_idx, blocks) in codes.chunks_exact(BATCH_SIZE * code_len).enumerate() {
        for (sub_vec_idx, block) in blocks.chunks_exact(BATCH_SIZE).enumerate() {
            let current_dist_table = &dist_table[sub_vec_idx * 2 * 16..(sub_vec_idx * 2 + 1) * 16];
            let next_dist_table =
                &dist_table[(sub_vec_idx * 2 + 1) * 16..(sub_vec_idx * 2 + 2) * 16];

            for j in 0..16 {
                let low_current_code = (block[j] & 0x0F) as usize;
                let high_current_code = (block[j] >> 4) as usize;
                let low_next_code = (block[j + 16] & 0x0F) as usize;
                let high_next_code = (block[j + 16] >> 4) as usize;

                let lower_id = vec_block_idx * BATCH_SIZE + PERM0[j];
                let higher_id = lower_id + 16;
                dists[lower_id] += current_dist_table[low_current_code] as u32
                    + next_dist_table[low_next_code] as u32;
                dists[higher_id] += current_dist_table[high_current_code] as u32
                    + next_dist_table[high_next_code] as u32;
            }
        }
    }
}

#[cfg(target_arch = "x86_64")]
#[inline]
fn sum_4bit_hacc_dist_table_avx2(
    n: usize,
    code_len: usize,
    codes: &[u8],
    hacc_dist_table: &[u8],
    dists: &mut [u32],
) {
    const SAFE_CODE_LEN: usize = 128;

    for i in (0..n).step_by(BATCH_SIZE) {
        let batch_codes = &codes[i * code_len..(i + BATCH_SIZE) * code_len];
        let batch_dists = &mut dists[i..i + BATCH_SIZE];
        batch_dists.fill(0);

        for code_start in (0..code_len).step_by(SAFE_CODE_LEN) {
            let code_end = (code_start + SAFE_CODE_LEN).min(code_len);
            let code_range = code_start * BATCH_SIZE..code_end * BATCH_SIZE;
            let table_range = code_start * 64..code_end * 64;
            if code_start == 0 && code_end == code_len {
                unsafe {
                    sum_hacc_dist_table_32bytes_batch_avx2(
                        &batch_codes[code_range],
                        &hacc_dist_table[table_range],
                        batch_dists,
                    );
                }
            } else {
                let mut chunk_dists = [0u32; BATCH_SIZE];
                unsafe {
                    sum_hacc_dist_table_32bytes_batch_avx2(
                        &batch_codes[code_range],
                        &hacc_dist_table[table_range],
                        &mut chunk_dists,
                    );
                }
                batch_dists
                    .iter_mut()
                    .zip(chunk_dists.iter())
                    .for_each(|(dist, chunk_dist)| *dist += *chunk_dist);
            }
        }
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
#[inline]
#[allow(unused)]
unsafe fn sum_hacc_dist_table_32bytes_batch_avx2(
    codes: &[u8],
    hacc_dist_table: &[u8],
    dists: &mut [u32],
) {
    let low_mask = _mm256_set1_epi8(0x0f);
    let mut low_accu0 = _mm256_setzero_si256();
    let mut low_accu1 = _mm256_setzero_si256();
    let mut low_accu2 = _mm256_setzero_si256();
    let mut low_accu3 = _mm256_setzero_si256();
    let mut high_accu0 = _mm256_setzero_si256();
    let mut high_accu1 = _mm256_setzero_si256();
    let mut high_accu2 = _mm256_setzero_si256();
    let mut high_accu3 = _mm256_setzero_si256();

    for code_offset in (0..codes.len()).step_by(BATCH_SIZE) {
        let table_offset = code_offset * 2;
        let c = _mm256_loadu_si256(codes.as_ptr().add(code_offset) as *const __m256i);
        let lo = _mm256_and_si256(c, low_mask);
        let hi = _mm256_and_si256(_mm256_srli_epi16(c, 4), low_mask);

        let low_lut =
            _mm256_loadu_si256(hacc_dist_table.as_ptr().add(table_offset) as *const __m256i);
        let low_res_lo = _mm256_shuffle_epi8(low_lut, lo);
        let low_res_hi = _mm256_shuffle_epi8(low_lut, hi);
        low_accu0 = _mm256_add_epi16(low_accu0, low_res_lo);
        low_accu1 = _mm256_add_epi16(low_accu1, _mm256_srli_epi16(low_res_lo, 8));
        low_accu2 = _mm256_add_epi16(low_accu2, low_res_hi);
        low_accu3 = _mm256_add_epi16(low_accu3, _mm256_srli_epi16(low_res_hi, 8));

        let high_lut =
            _mm256_loadu_si256(hacc_dist_table.as_ptr().add(table_offset + 32) as *const __m256i);
        let high_res_lo = _mm256_shuffle_epi8(high_lut, lo);
        let high_res_hi = _mm256_shuffle_epi8(high_lut, hi);
        high_accu0 = _mm256_add_epi16(high_accu0, high_res_lo);
        high_accu1 = _mm256_add_epi16(high_accu1, _mm256_srli_epi16(high_res_lo, 8));
        high_accu2 = _mm256_add_epi16(high_accu2, high_res_hi);
        high_accu3 = _mm256_add_epi16(high_accu3, _mm256_srli_epi16(high_res_hi, 8));
    }

    low_accu0 = _mm256_sub_epi16(low_accu0, _mm256_slli_epi16(low_accu1, 8));
    let low_dis0 = _mm256_add_epi16(
        _mm256_permute2f128_si256(low_accu0, low_accu1, 0x21),
        _mm256_blend_epi32(low_accu0, low_accu1, 0xF0),
    );
    low_accu2 = _mm256_sub_epi16(low_accu2, _mm256_slli_epi16(low_accu3, 8));
    let low_dis1 = _mm256_add_epi16(
        _mm256_permute2f128_si256(low_accu2, low_accu3, 0x21),
        _mm256_blend_epi32(low_accu2, low_accu3, 0xF0),
    );

    high_accu0 = _mm256_sub_epi16(high_accu0, _mm256_slli_epi16(high_accu1, 8));
    let high_dis0 = _mm256_add_epi16(
        _mm256_permute2f128_si256(high_accu0, high_accu1, 0x21),
        _mm256_blend_epi32(high_accu0, high_accu1, 0xF0),
    );
    high_accu2 = _mm256_sub_epi16(high_accu2, _mm256_slli_epi16(high_accu3, 8));
    let high_dis1 = _mm256_add_epi16(
        _mm256_permute2f128_si256(high_accu2, high_accu3, 0x21),
        _mm256_blend_epi32(high_accu2, high_accu3, 0xF0),
    );

    let low0 = _mm256_cvtepu16_epi32(_mm256_castsi256_si128(low_dis0));
    let low1 = _mm256_cvtepu16_epi32(_mm256_extracti128_si256(low_dis0, 1));
    let high0 = _mm256_cvtepu16_epi32(_mm256_castsi256_si128(high_dis0));
    let high1 = _mm256_cvtepu16_epi32(_mm256_extracti128_si256(high_dis0, 1));
    let res0 = _mm256_add_epi32(low0, _mm256_slli_epi32(high0, 8));
    let res1 = _mm256_add_epi32(low1, _mm256_slli_epi32(high1, 8));
    _mm256_storeu_si256(dists.as_mut_ptr() as *mut __m256i, res0);
    _mm256_storeu_si256(dists.as_mut_ptr().add(8) as *mut __m256i, res1);

    let low2 = _mm256_cvtepu16_epi32(_mm256_castsi256_si128(low_dis1));
    let low3 = _mm256_cvtepu16_epi32(_mm256_extracti128_si256(low_dis1, 1));
    let high2 = _mm256_cvtepu16_epi32(_mm256_castsi256_si128(high_dis1));
    let high3 = _mm256_cvtepu16_epi32(_mm256_extracti128_si256(high_dis1, 1));
    let res2 = _mm256_add_epi32(low2, _mm256_slli_epi32(high2, 8));
    let res3 = _mm256_add_epi32(low3, _mm256_slli_epi32(high3, 8));
    _mm256_storeu_si256(dists.as_mut_ptr().add(16) as *mut __m256i, res2);
    _mm256_storeu_si256(dists.as_mut_ptr().add(24) as *mut __m256i, res3);
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
#[inline]
#[allow(unused)]
unsafe fn sum_dist_table_32bytes_batch_avx2(codes: &[u8], dist_table: &[u8], dists: &mut [u16]) {
    let mut c = _mm256_undefined_si256();
    let mut lo = _mm256_undefined_si256();
    let mut hi = _mm256_undefined_si256();
    let mut lut_vec = _mm256_undefined_si256();
    let mut res_lo = _mm256_undefined_si256();
    let mut res_hi = _mm256_undefined_si256();

    let mut accu0 = _mm256_setzero_si256();
    let mut accu1 = _mm256_setzero_si256();
    let mut accu2 = _mm256_setzero_si256();
    let mut accu3 = _mm256_setzero_si256();
    let low_mask = _mm256_set1_epi8(0x0f);

    for i in (0..codes.len()).step_by(64) {
        // load 32 * 2 codes (we pack 2 codes into 1 byte)
        c = _mm256_loadu_si256(codes.as_ptr().add(i) as *const __m256i);
        lut_vec = _mm256_loadu_si256(dist_table.as_ptr().add(i) as *const __m256i);

        // split the first 4 bits and the second 4 bits
        lo = _mm256_and_si256(c, low_mask);
        hi = _mm256_and_si256(_mm256_srli_epi16(c, 4), low_mask);

        // lookup the lut
        res_lo = _mm256_shuffle_epi8(lut_vec, lo);
        res_hi = _mm256_shuffle_epi8(lut_vec, hi);

        accu0 = _mm256_add_epi16(accu0, res_lo);
        accu1 = _mm256_add_epi16(accu1, _mm256_srli_epi16(res_lo, 8));
        accu2 = _mm256_add_epi16(accu2, res_hi);
        accu3 = _mm256_add_epi16(accu3, _mm256_srli_epi16(res_hi, 8));

        if i + 32 >= codes.len() {
            continue;
        }

        // load the left 32 bytes of codes and lut
        c = _mm256_loadu_si256(codes.as_ptr().add(i + 32) as *const __m256i);
        lut_vec = _mm256_loadu_si256(dist_table.as_ptr().add(i + 32) as *const __m256i);

        lo = _mm256_and_si256(c, low_mask);
        hi = _mm256_and_si256(_mm256_srli_epi16(c, 4), low_mask);

        res_lo = _mm256_shuffle_epi8(lut_vec, lo);
        res_hi = _mm256_shuffle_epi8(lut_vec, hi);

        accu0 = _mm256_add_epi16(accu0, res_lo);
        accu1 = _mm256_add_epi16(accu1, _mm256_srli_epi16(res_lo, 8));
        accu2 = _mm256_add_epi16(accu2, res_hi);
        accu3 = _mm256_add_epi16(accu3, _mm256_srli_epi16(res_hi, 8));
    }

    // merge the low 4 bits
    accu0 = _mm256_sub_epi16(accu0, _mm256_slli_epi16(accu1, 8));
    let dis0 = _mm256_add_epi16(
        _mm256_permute2f128_si256(accu0, accu1, 0x21),
        _mm256_blend_epi32(accu0, accu1, 0xF0),
    );
    _mm256_storeu_si256(dists.as_mut_ptr() as *mut __m256i, dis0);

    // merge the high 4 bits
    accu2 = _mm256_sub_epi16(accu2, _mm256_slli_epi16(accu3, 8));
    let dis1 = _mm256_add_epi16(
        _mm256_permute2f128_si256(accu2, accu3, 0x21),
        _mm256_blend_epi32(accu2, accu3, 0xF0),
    );

    _mm256_storeu_si256(dists.as_mut_ptr().add(16) as *mut __m256i, dis1);
}

#[cfg(target_arch = "aarch64")]
#[inline]
unsafe fn sum_dist_table_32bytes_batch_neon(codes: &[u8], dist_table: &[u8], dists: &mut [u16]) {
    let low_mask = vdupq_n_u8(0x0f);

    // 8 accumulators: 4 per 128-bit "lane" (lo = bytes 0..16, hi = bytes 16..32 of each block)
    let mut accu0_lo = vdupq_n_u16(0);
    let mut accu1_lo = vdupq_n_u16(0);
    let mut accu2_lo = vdupq_n_u16(0);
    let mut accu3_lo = vdupq_n_u16(0);
    let mut accu0_hi = vdupq_n_u16(0);
    let mut accu1_hi = vdupq_n_u16(0);
    let mut accu2_hi = vdupq_n_u16(0);
    let mut accu3_hi = vdupq_n_u16(0);

    let codes_ptr = codes.as_ptr();
    let dt_ptr = dist_table.as_ptr();

    for i in (0..codes.len()).step_by(32) {
        // Process lo lane: bytes [i..i+16]
        let c_lo = vld1q_u8(codes_ptr.add(i));
        let lut_lo = vld1q_u8(dt_ptr.add(i));

        let lo_lo = vandq_u8(c_lo, low_mask);
        let hi_lo = vshrq_n_u8::<4>(c_lo);

        let res_lo_lo = vqtbl1q_u8(lut_lo, lo_lo);
        let res_hi_lo = vqtbl1q_u8(lut_lo, hi_lo);

        accu0_lo = vaddq_u16(accu0_lo, vreinterpretq_u16_u8(res_lo_lo));
        accu1_lo = vaddq_u16(accu1_lo, vshrq_n_u16::<8>(vreinterpretq_u16_u8(res_lo_lo)));
        accu2_lo = vaddq_u16(accu2_lo, vreinterpretq_u16_u8(res_hi_lo));
        accu3_lo = vaddq_u16(accu3_lo, vshrq_n_u16::<8>(vreinterpretq_u16_u8(res_hi_lo)));

        // Process hi lane: bytes [i+16..i+32]
        let c_hi = vld1q_u8(codes_ptr.add(i + 16));
        let lut_hi = vld1q_u8(dt_ptr.add(i + 16));

        let lo_hi = vandq_u8(c_hi, low_mask);
        let hi_hi = vshrq_n_u8::<4>(c_hi);

        let res_lo_hi = vqtbl1q_u8(lut_hi, lo_hi);
        let res_hi_hi = vqtbl1q_u8(lut_hi, hi_hi);

        accu0_hi = vaddq_u16(accu0_hi, vreinterpretq_u16_u8(res_lo_hi));
        accu1_hi = vaddq_u16(accu1_hi, vshrq_n_u16::<8>(vreinterpretq_u16_u8(res_lo_hi)));
        accu2_hi = vaddq_u16(accu2_hi, vreinterpretq_u16_u8(res_hi_hi));
        accu3_hi = vaddq_u16(accu3_hi, vshrq_n_u16::<8>(vreinterpretq_u16_u8(res_hi_hi)));
    }

    // Merge: clean even bytes by subtracting the odd-byte bleed
    accu0_lo = vsubq_u16(accu0_lo, vshlq_n_u16::<8>(accu1_lo));
    accu0_hi = vsubq_u16(accu0_hi, vshlq_n_u16::<8>(accu1_hi));

    // Cross-lane merge: add lo and hi lane accumulators
    // This is the NEON equivalent of AVX2's permute2f128 + blend + add
    let dis0_even = vaddq_u16(accu0_lo, accu0_hi);
    let dis0_odd = vaddq_u16(accu1_lo, accu1_hi);
    vst1q_u16(dists.as_mut_ptr(), dis0_even);
    vst1q_u16(dists.as_mut_ptr().add(8), dis0_odd);

    // Same for hi-nibble accumulators (vectors 16..31)
    accu2_lo = vsubq_u16(accu2_lo, vshlq_n_u16::<8>(accu3_lo));
    accu2_hi = vsubq_u16(accu2_hi, vshlq_n_u16::<8>(accu3_hi));

    let dis1_even = vaddq_u16(accu2_lo, accu2_hi);
    let dis1_odd = vaddq_u16(accu3_lo, accu3_hi);
    vst1q_u16(dists.as_mut_ptr().add(16), dis1_even);
    vst1q_u16(dists.as_mut_ptr().add(24), dis1_odd);
}

// We implement the AVX512 version in C because AVX512 is not stable yet in Rust,
// implement it in Rust once we upgrade rust to 1.89.0.
unsafe extern "C" {
    #[cfg(all(kernel_support = "avx512_dist_table", target_arch = "x86_64"))]
    pub fn sum_4bit_dist_table_32bytes_batch_avx512(
        codes: *const u8,
        code_length: usize,
        dist_table: *const u8,
        dists: *mut u16,
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_perm0_inverse_matches_perm0() {
        for (idx, &value) in PERM0.iter().enumerate() {
            assert_eq!(PERM0_INVERSE[value], idx);
        }
    }

    #[test]
    fn test_sum_4bit_dist_table_basic() {
        // we have 32 vectors
        let n = 32;

        // each code is 2 bytes (16 dim), so code_len = 2
        let code_len = 2;

        let codes = [
            0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, // codes[0..8]
            0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, // codes[8..16]
            0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x00, // codes[16..24]
            0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, // codes[24..32]
        ];
        let codes = codes.repeat(n * code_len / codes.len());

        let mut dist_table = vec![0u8; 16 * 4];
        for (i, dist) in dist_table.iter_mut().enumerate() {
            *dist = (i % 16 + 1) as u8;
        }

        // Test the function
        let mut dists = vec![0u16; n];
        sum_4bit_dist_table(n, code_len, &codes, &dist_table, &mut dists);

        // Compare with reference implementation
        let mut expected_dists = vec![0u16; n];
        sum_4bit_dist_table_scalar(code_len, &codes, &dist_table, &mut expected_dists);

        assert_eq!(dists, expected_dists);
        // the vector 1's code is the low 4bits of codes[PERM0_INVERSE[1]] = codes[2],
        // the first 4 bits are the low 4 bits of codes[2], so it's 0x6,
        // the second 4 bits are the low 4 bits of codes[2 + 16], so it's 0xb,
        // the third 4 bits are the same as the first 4 bits, so it's 0x6,
        // the fourth 4 bits are the same as the second 4 bits, so it's 0xb,

        // so the distance is 2 * (dist_table[0x6] + dist_table[0xb + 16]) = 2*(7 + 12) = 38
        assert_eq!(dists[1], 38);
    }

    #[test]
    fn test_sum_4bit_dist_table_overwrites_output() {
        let n = BATCH_SIZE;
        let code_len = 16;
        let codes = vec![0x12; n * code_len];
        let dist_table = vec![1u8; BATCH_SIZE * code_len];

        let mut expected = vec![u16::MAX; n];
        sum_4bit_dist_table_scalar(code_len, &codes, &dist_table, &mut expected);

        let mut actual = vec![u16::MAX; n];
        sum_4bit_dist_table(n, code_len, &codes, &dist_table, &mut actual);

        assert_eq!(actual, expected);
        assert!(actual.iter().all(|dist| *dist != u16::MAX));
    }

    #[test]
    fn test_sum_4bit_dist_table_u16_basic() {
        let n = BATCH_SIZE;
        let code_len = 2;
        let codes = [
            0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66,
            0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x00, 0x12, 0x34, 0x56, 0x78,
            0x9a, 0xbc, 0xde, 0xf0,
        ];
        let codes = codes.repeat(n * code_len / codes.len());
        let dist_table: Vec<u16> = (0..16 * 4).map(|idx| (idx % 16 + 1) as u16).collect();

        let mut dists = vec![0u32; n];
        sum_4bit_dist_table_u16(n, code_len, &codes, &dist_table, &mut dists);

        assert_eq!(dists[1], 38);
    }

    #[test]
    fn test_transfer_4bit_dist_table_u16_layout() {
        let dist_table: Vec<u16> = (0..32).map(|idx| 0x1200 + idx as u16).collect();
        let mut hacc_dist_table = Vec::new();
        transfer_4bit_dist_table_u16(&dist_table, &mut hacc_dist_table);

        assert_eq!(hacc_dist_table.len(), 64);
        for code in 0..16 {
            assert_eq!(hacc_dist_table[code], dist_table[code] as u8);
            assert_eq!(hacc_dist_table[16 + code], dist_table[16 + code] as u8);
            assert_eq!(hacc_dist_table[32 + code], (dist_table[code] >> 8) as u8);
            assert_eq!(
                hacc_dist_table[48 + code],
                (dist_table[16 + code] >> 8) as u8
            );
        }
    }

    #[test]
    fn test_sum_4bit_dist_table_u16_matches_reference_multi_batch() {
        use rand::{Rng, SeedableRng};
        let mut rng = rand::rngs::StdRng::seed_from_u64(99);

        for code_len in [1, 3, 16, 191, 192, 1024] {
            let n = BATCH_SIZE * 4;
            let codes: Vec<u8> = (0..n * code_len).map(|_| rng.random::<u8>()).collect();
            let dist_table: Vec<u16> = (0..BATCH_SIZE * code_len)
                .map(|_| rng.random::<u16>())
                .collect();

            let mut expected = vec![0u32; n];
            sum_4bit_dist_table_u16_scalar(code_len, &codes, &dist_table, &mut expected);

            let mut actual = vec![u32::MAX; n];
            sum_4bit_dist_table_u16(n, code_len, &codes, &dist_table, &mut actual);

            assert_eq!(
                actual,
                expected,
                "u16 dist-table mismatch for code_len={} (DIM={})",
                code_len,
                code_len * 8,
            );
        }
    }

    #[test]
    fn test_sum_4bit_hacc_dist_table_matches_u16_reference_multi_batch() {
        use rand::{Rng, SeedableRng};
        let mut rng = rand::rngs::StdRng::seed_from_u64(101);

        for code_len in [1, 3, 16, 191, 192, 1024] {
            let n = BATCH_SIZE * 4;
            let codes: Vec<u8> = (0..n * code_len).map(|_| rng.random::<u8>()).collect();
            let dist_table: Vec<u16> = (0..BATCH_SIZE * code_len)
                .map(|_| rng.random::<u16>())
                .collect();

            let mut hacc_dist_table = Vec::new();
            transfer_4bit_dist_table_u16(&dist_table, &mut hacc_dist_table);

            let mut expected = vec![0u32; n];
            sum_4bit_dist_table_u16_scalar(code_len, &codes, &dist_table, &mut expected);

            let mut actual = vec![u32::MAX; n];
            sum_4bit_hacc_dist_table(n, code_len, &codes, &hacc_dist_table, &mut actual);

            assert_eq!(
                actual,
                expected,
                "hacc dist-table mismatch for code_len={} (DIM={})",
                code_len,
                code_len * 8,
            );
        }
    }

    /// Test that the SIMD path (NEON on ARM, AVX2 on x86) produces identical
    /// results to the scalar reference across a range of dimensions, including
    /// very large ones (up to DIM=65536).
    ///
    /// Note: dist_table values are capped to avoid u16 overflow, matching
    /// production behavior where values are quantized to a small range.
    /// (The scalar path uses saturating_add while SIMD uses wrapping add,
    /// so they diverge on overflow — but overflow never occurs with real
    /// quantized data.)
    #[test]
    fn test_simd_matches_scalar_varied_dimensions() {
        use rand::{Rng, SeedableRng};
        let mut rng = rand::rngs::StdRng::seed_from_u64(42);

        // code_len = dim / 8 for 1-bit quantization; we test various code_lens
        // directly since that's what the function sees.
        // code_len=16 → DIM=128, code_len=192 → DIM=1536,
        // code_len=512 → DIM=4096, code_len=8192 → DIM=65536
        for code_len in [1, 2, 3, 16, 95, 96, 192, 512, 1024, 8192] {
            let n = BATCH_SIZE; // 32 vectors per batch

            // Each code byte produces 2 lookups; cap values so
            // 2 * code_len * max_val < u16::MAX.
            let max_val = (u16::MAX as usize / (2 * code_len)).min(255) as u8;

            let codes: Vec<u8> = (0..n * code_len).map(|_| rng.random::<u8>()).collect();
            let dist_table: Vec<u8> = (0..BATCH_SIZE * code_len)
                .map(|_| rng.random_range(0..=max_val))
                .collect();

            let mut expected = vec![0u16; n];
            sum_4bit_dist_table_scalar(code_len, &codes, &dist_table, &mut expected);

            let mut actual = vec![0u16; n];
            sum_4bit_dist_table(n, code_len, &codes, &dist_table, &mut actual);

            assert_eq!(
                actual,
                expected,
                "SIMD and scalar mismatch for code_len={} (DIM={})",
                code_len,
                code_len * 8,
            );
        }
    }

    /// Test with multiple batches to verify accumulation across batch boundaries.
    #[test]
    fn test_simd_matches_scalar_multi_batch() {
        use rand::{Rng, SeedableRng};
        let mut rng = rand::rngs::StdRng::seed_from_u64(123);

        for code_len in [1, 3, 16, 191, 192, 1024] {
            let n = BATCH_SIZE * 10; // 320 vectors = 10 batches

            let max_val = (u16::MAX as usize / (2 * code_len)).min(255) as u8;

            let codes: Vec<u8> = (0..n * code_len).map(|_| rng.random::<u8>()).collect();
            let dist_table: Vec<u8> = (0..BATCH_SIZE * code_len)
                .map(|_| rng.random_range(0..=max_val))
                .collect();

            let mut expected = vec![0u16; n];
            sum_4bit_dist_table_scalar(code_len, &codes, &dist_table, &mut expected);

            let mut actual = vec![0u16; n];
            sum_4bit_dist_table(n, code_len, &codes, &dist_table, &mut actual);

            assert_eq!(
                actual,
                expected,
                "SIMD and scalar mismatch for multi-batch code_len={} (DIM={}, n={})",
                code_len,
                code_len * 8,
                n,
            );
        }
    }
}
