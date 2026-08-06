// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Inner-product kernels between an `f32` query and bit-packed RaBitQ ex-codes.
//!
//! Multi-bit RaBitQ reranking reduces to `sum_d query[d] * ex_code[d]`, where
//! `ex_code[d]` is an unsigned `ex_bits`-wide integer. Materializing a
//! `dim * 2^ex_bits` lookup table and gathering one entry per dimension is
//! cache-hostile (the table is 1MiB for `ex_bits=8`, `dim=1024`); these kernels
//! instead unpack the codes with shifts and masks and FMA them against the
//! query directly, following the kernel design of the RaBitQ reference library
//! (<https://github.com/VectorDB-NTU/RaBitQ-Library>, Apache-2.0).
//!
//! Codes are stored in the *blocked* layout: dims are grouped into 64-dim
//! blocks (the last block zero-padded) and bit-interleaved within each block
//! so that the SIMD unpack emits codes in natural dim order:
//!
//! ```text
//! per 64-dim block (T = ex_bits - 1, the top bit; "run k" = dims 16k..16k+16):
//! 1 bit:  [8B]  bit i of the LE word = dim i
//! 2 bits: [16B] byte b = dims {b, b+16, b+32, b+48} at bit pairs 0/2/4/6
//! 3 bits: [16B 2-bit plane as above][8B top-bit plane]
//! 4 bits: [32B] byte 8j+b = dim 16j+b (low nibble) | dim 16j+8+b (high nibble)
//! 5 bits: [32B 4-bit plane: byte b = dims b|b+16; byte 16+b = dims b+32|b+48]
//!         [8B top-bit plane]
//! 6 bits: [48B] byte 16k+b = dim 16k+b (6 low bits) | bits 2k..2k+2 of
//!         dim 48+b (2 high bits)
//! 7 bits: [48B as 6 bits][8B top-bit plane]
//! 8 bits: [64B] identity
//! top-bit plane: top bit of dim 16k+b at bit 8*(b%8) + 2k + b/8 of a LE u64
//! ```
//!
//! Because unpack order is natural, the kernels read the rotated query
//! directly; it only needs zero-padding ([`pad_query_into`]) when the rotated
//! dim is not a multiple of 64. Legacy indexes store ex codes sequentially
//! (LSB-first bit stream) and are repacked once at load time
//! ([`repack_sequential_row`]); for `ex_bits` ∈ {1, 8} the two layouts agree
//! (modulo trailing padding, which the kernels tolerate) and rows are used as
//! stored.

use std::sync::LazyLock;

/// Dims are packed in blocks of this size; the query is zero-padded to a
/// whole number of blocks when the rotated dim is not already a multiple.
pub const EX_DOT_BLOCK_DIMS: usize = 64;

/// `f32` length of the query consumed by the kernels.
pub fn padded_query_len(dim: usize) -> usize {
    dim.next_multiple_of(EX_DOT_BLOCK_DIMS)
}

/// Whether the legacy sequential layout of a row already matches the blocked
/// layout (modulo trailing zero padding, which the kernels tolerate), so
/// legacy rows can be consumed without repacking.
pub fn sequential_matches_blocked(ex_bits: u8) -> bool {
    matches!(ex_bits, 1 | 8)
}

/// Bytes per row of the blocked ex-code layout.
pub fn blocked_ex_code_bytes(dim: usize, ex_bits: u8) -> usize {
    debug_assert!((1..=8).contains(&ex_bits));
    padded_query_len(dim) * ex_bits as usize / 8
}

/// Dimensions per unpacking group for the given code width.
fn group_dims(ex_bits: u8) -> usize {
    match ex_bits {
        1 | 4 | 8 => 16,
        _ => EX_DOT_BLOCK_DIMS,
    }
}

fn group_bytes(ex_bits: u8) -> usize {
    group_dims(ex_bits) * ex_bits as usize / 8
}

/// Extract the `ex_bits`-wide code of `dim_idx` from a sequentially bit-packed
/// row (LSB-first, codes may straddle byte boundaries).
#[inline]
pub fn packed_ex_code_value(row_codes: &[u8], dim_idx: usize, ex_bits: u8) -> u8 {
    debug_assert!(ex_bits > 0);
    let bit_offset = dim_idx * ex_bits as usize;
    let byte_idx = bit_offset / u8::BITS as usize;
    let bit_shift = bit_offset % u8::BITS as usize;
    let bits = row_codes[byte_idx] as u16
        | row_codes
            .get(byte_idx + 1)
            .map(|byte| (*byte as u16) << u8::BITS)
            .unwrap_or_default();
    let mask = (1u16 << ex_bits) - 1;
    ((bits >> bit_shift) & mask) as u8
}

/// Zero-pad the rotated query to a whole number of 64-dim blocks. Only needed
/// when `dim` is not a multiple of [`EX_DOT_BLOCK_DIMS`]; aligned queries are
/// passed to the kernels as-is.
pub fn pad_query_into(rotated_query: &[f32], out: &mut [f32]) {
    debug_assert_eq!(out.len(), padded_query_len(rotated_query.len()));
    out[..rotated_query.len()].copy_from_slice(rotated_query);
    out[rotated_query.len()..].fill(0.0);
}

/// Pack the top bit of each of 64 codes into a `u64` so kernels can position
/// it with two shifts per 16-code run: the top bit of dim `16k + b` is stored
/// at bit `8 * (b % 8) + 2k + b / 8`.
fn pack_top_plane(block_values: &[u8; 64], top_bit: u8) -> u64 {
    let mut plane = 0u64;
    for k in 0..4 {
        for b in 0..16 {
            let bit = (block_values[16 * k + b] >> top_bit) & 1;
            plane |= (bit as u64) << (8 * (b % 8) + 2 * k + b / 8);
        }
    }
    plane
}

/// Shift `plane` so that its bit `8j + from_bit` lands at bit `8j + to_bit`.
#[inline(always)]
fn shift_plane(plane: u64, from_bit: usize, to_bit: usize) -> u64 {
    if from_bit >= to_bit {
        plane >> (from_bit - to_bit)
    } else {
        plane << (to_bit - from_bit)
    }
}

/// Pack one block of 64 code values (natural dim order) into the blocked
/// layout described in the module docs.
fn pack_block(ex_bits: u8, block_values: &[u8; 64], out: &mut [u8]) {
    let v = block_values;
    match ex_bits {
        1 => {
            for (b, byte) in out[..8].iter_mut().enumerate() {
                *byte = (0..8).fold(0, |acc, t| acc | ((v[8 * b + t] & 1) << t));
            }
        }
        2 | 3 => {
            for b in 0..16 {
                out[b] = (v[b] & 0b11)
                    | ((v[16 + b] & 0b11) << 2)
                    | ((v[32 + b] & 0b11) << 4)
                    | ((v[48 + b] & 0b11) << 6);
            }
            if ex_bits == 3 {
                out[16..24].copy_from_slice(&pack_top_plane(v, 2).to_le_bytes());
            }
        }
        4 => {
            for unit in 0..4 {
                for b in 0..8 {
                    out[8 * unit + b] =
                        (v[16 * unit + b] & 0x0f) | ((v[16 * unit + 8 + b] & 0x0f) << 4);
                }
            }
        }
        5 => {
            for b in 0..16 {
                out[b] = (v[b] & 0x0f) | ((v[16 + b] & 0x0f) << 4);
                out[16 + b] = (v[32 + b] & 0x0f) | ((v[48 + b] & 0x0f) << 4);
            }
            out[32..40].copy_from_slice(&pack_top_plane(v, 4).to_le_bytes());
        }
        6 | 7 => {
            // Runs 0..3 keep their 6 low bits in place; the fourth run's dims
            // are split into three 2-bit pieces stored in the runs' top bits.
            for k in 0..3 {
                for b in 0..16 {
                    out[16 * k + b] =
                        (v[16 * k + b] & 0x3f) | (((v[48 + b] >> (2 * k)) & 0b11) << 6);
                }
            }
            if ex_bits == 7 {
                out[48..56].copy_from_slice(&pack_top_plane(v, 6).to_le_bytes());
            }
        }
        8 => out[..64].copy_from_slice(v),
        _ => unreachable!("invalid RabitQ ex_bits={ex_bits}"),
    }
}

/// Pack one row of unpacked code values (one `u8` per dim) into the blocked
/// layout; the writer path. `out` must have [`blocked_ex_code_bytes`] bytes.
pub fn pack_blocked_row(values: &[u8], ex_bits: u8, out: &mut [u8]) {
    debug_assert_eq!(out.len(), blocked_ex_code_bytes(values.len(), ex_bits));
    let block_bytes = EX_DOT_BLOCK_DIMS * ex_bits as usize / 8;
    let mut block_values = [0u8; 64];
    for (block, out) in out.chunks_exact_mut(block_bytes).enumerate() {
        let base = block * EX_DOT_BLOCK_DIMS;
        let count = EX_DOT_BLOCK_DIMS.min(values.len() - base);
        block_values[..count].copy_from_slice(&values[base..base + count]);
        block_values[count..].fill(0);
        pack_block(ex_bits, &block_values, out);
    }
}

/// Repack one legacy sequentially bit-packed row into the blocked layout.
/// `out` must have [`blocked_ex_code_bytes`] bytes.
pub fn repack_sequential_row(seq_row: &[u8], dim: usize, ex_bits: u8, out: &mut [u8]) {
    debug_assert_eq!(out.len(), blocked_ex_code_bytes(dim, ex_bits));
    let block_bytes = EX_DOT_BLOCK_DIMS * ex_bits as usize / 8;
    let mut block_values = [0u8; 64];
    for (block, out) in out.chunks_exact_mut(block_bytes).enumerate() {
        block_values.fill(0);
        let base = block * EX_DOT_BLOCK_DIMS;
        let count = EX_DOT_BLOCK_DIMS.min(dim.saturating_sub(base));
        for (i, value) in block_values[..count].iter_mut().enumerate() {
            *value = packed_ex_code_value(seq_row, base + i, ex_bits);
        }
        pack_block(ex_bits, &block_values, out);
    }
}

/// Unpack one code group into per-dim values (natural dim order). Reference
/// implementation for the SIMD unpackers; also the scalar fallback.
fn unpack_group(ex_bits: u8, group_codes: &[u8], out: &mut [u8; 64]) {
    debug_assert_eq!(group_codes.len(), group_bytes(ex_bits));
    match ex_bits {
        1 => {
            for (i, value) in out[..16].iter_mut().enumerate() {
                *value = (group_codes[i / 8] >> (i % 8)) & 1;
            }
        }
        2 => {
            for k in 0..4 {
                for b in 0..16 {
                    out[16 * k + b] = (group_codes[b] >> (2 * k)) & 0b11;
                }
            }
        }
        3 => {
            let plane = u64::from_le_bytes(group_codes[16..24].try_into().unwrap());
            for k in 0..4 {
                for b in 0..16 {
                    let top = (plane >> (8 * (b % 8) + 2 * k + b / 8)) & 1;
                    out[16 * k + b] = ((group_codes[b] >> (2 * k)) & 0b11) | ((top as u8) << 2);
                }
            }
        }
        4 => {
            for b in 0..8 {
                out[b] = group_codes[b] & 0x0f;
                out[8 + b] = group_codes[b] >> 4;
            }
        }
        5 => {
            let plane = u64::from_le_bytes(group_codes[32..40].try_into().unwrap());
            for k in 0..4 {
                for b in 0..16 {
                    let nibble = (group_codes[16 * (k / 2) + b] >> (4 * (k % 2))) & 0x0f;
                    let top = (plane >> (8 * (b % 8) + 2 * k + b / 8)) & 1;
                    out[16 * k + b] = nibble | ((top as u8) << 4);
                }
            }
        }
        6 | 7 => {
            for k in 0..3 {
                for b in 0..16 {
                    out[16 * k + b] = group_codes[16 * k + b] & 0x3f;
                }
            }
            for b in 0..16 {
                out[48 + b] = (group_codes[b] >> 6)
                    | ((group_codes[16 + b] >> 6) << 2)
                    | ((group_codes[32 + b] >> 6) << 4);
            }
            if ex_bits == 7 {
                let plane = u64::from_le_bytes(group_codes[48..56].try_into().unwrap());
                for k in 0..4 {
                    for b in 0..16 {
                        let top = (plane >> (8 * (b % 8) + 2 * k + b / 8)) & 1;
                        out[16 * k + b] |= (top as u8) << 6;
                    }
                }
            }
        }
        8 => out[..16].copy_from_slice(group_codes),
        _ => unreachable!("invalid RabitQ ex_bits={ex_bits}"),
    }
}

/// `sum_d query[d] * code[d]` for one row of blocked-layout codes.
///
/// The query must cover a whole number of 64-dim blocks (the rotated query
/// as-is for aligned dims, otherwise zero-padded via [`pad_query_into`]);
/// `codes` is the blocked row slice. Rows shorter than the padded query
/// length are treated as zero-padded.
pub type ExDotFn = fn(&[f32], &[u8]) -> f32;

/// Resolve the dot kernel for `ex_bits` once; the result can be cached by the
/// caller for per-candidate use.
pub fn ex_dot_kernel(ex_bits: u8) -> ExDotFn {
    debug_assert!((1..=8).contains(&ex_bits));
    static KERNELS: LazyLock<[ExDotFn; 8]> =
        LazyLock::new(|| std::array::from_fn(|i| select_ex_dot_kernel(i as u8 + 1)));
    KERNELS[usize::from(ex_bits) - 1]
}

fn select_ex_dot_kernel(ex_bits: u8) -> ExDotFn {
    #[cfg(target_arch = "x86_64")]
    {
        if std::arch::is_x86_feature_detected!("avx512f") {
            return x86::avx512_kernel(ex_bits);
        }
        if std::arch::is_x86_feature_detected!("avx2") && std::arch::is_x86_feature_detected!("fma")
        {
            return x86::avx2_kernel(ex_bits);
        }
    }
    #[cfg(target_arch = "aarch64")]
    {
        // NEON is part of the aarch64 baseline.
        return neon::kernel(ex_bits);
    }
    #[allow(unreachable_code)]
    scalar_kernel(ex_bits)
}

fn scalar_kernel(ex_bits: u8) -> ExDotFn {
    match ex_bits {
        1 => ex_dot_scalar::<1>,
        2 => ex_dot_scalar::<2>,
        3 => ex_dot_scalar::<3>,
        4 => ex_dot_scalar::<4>,
        5 => ex_dot_scalar::<5>,
        6 => ex_dot_scalar::<6>,
        7 => ex_dot_scalar::<7>,
        8 => ex_dot_scalar::<8>,
        _ => unreachable!("invalid RabitQ ex_bits={ex_bits}"),
    }
}

fn ex_dot_scalar<const EX_BITS: u8>(ex_query: &[f32], codes: &[u8]) -> f32 {
    let group_dims = group_dims(EX_BITS);
    let bytes_per_group = group_bytes(EX_BITS);
    debug_assert_eq!(ex_query.len() % EX_DOT_BLOCK_DIMS, 0);
    debug_assert!(codes.len() * u8::BITS as usize <= ex_query.len() * EX_BITS as usize);

    let mut sum = 0.0f32;
    let mut unpacked = [0u8; 64];
    let mut padded = [0u8; 56];
    for (group, query) in ex_query.chunks_exact(group_dims).enumerate() {
        let start = group * bytes_per_group;
        if start >= codes.len() {
            // The remaining query lanes are zero padding.
            break;
        }
        let group_codes = if start + bytes_per_group <= codes.len() {
            &codes[start..start + bytes_per_group]
        } else {
            let avail = codes.len() - start;
            padded[..bytes_per_group].fill(0);
            padded[..avail].copy_from_slice(&codes[start..]);
            &padded[..bytes_per_group]
        };
        unpack_group(EX_BITS, group_codes, &mut unpacked);
        for (q, &code) in query.iter().zip(unpacked[..group_dims].iter()) {
            sum += q * code as f32;
        }
    }
    sum
}

#[cfg(target_arch = "x86_64")]
mod x86 {
    use super::ExDotFn;
    use std::arch::x86_64::*;

    pub(super) fn avx2_kernel(ex_bits: u8) -> ExDotFn {
        match ex_bits {
            1 => dot_u1_avx2_dispatch,
            2 => dot_u2_avx2_dispatch,
            3 => dot_u3_avx2_dispatch,
            4 => dot_u4_avx2_dispatch,
            5 => dot_u5_avx2_dispatch,
            6 => dot_u6_avx2_dispatch,
            7 => dot_u7_avx2_dispatch,
            8 => dot_u8_avx2_dispatch,
            _ => unreachable!("invalid RabitQ ex_bits={ex_bits}"),
        }
    }

    pub(super) fn avx512_kernel(ex_bits: u8) -> ExDotFn {
        match ex_bits {
            1 => dot_u1_avx512_dispatch,
            2 => dot_u2_avx512_dispatch,
            3 => dot_u3_avx512_dispatch,
            4 => dot_u4_avx512_dispatch,
            5 => dot_u5_avx512_dispatch,
            6 => dot_u6_avx512_dispatch,
            7 => dot_u7_avx512_dispatch,
            8 => dot_u8_avx512_dispatch,
            _ => unreachable!("invalid RabitQ ex_bits={ex_bits}"),
        }
    }

    /// Broadcast a byte to the 8 bytes of a `u64`.
    #[inline(always)]
    fn splat_byte(byte: u8) -> u64 {
        byte as u64 * 0x0101_0101_0101_0101
    }

    // Unpack helpers. They read exactly one group of code bytes and return
    // runs of 16 codes matching the kernel-order query. Only SSE2 (baseline on
    // x86_64) is required.

    /// 16 1-bit codes from 2 bytes: compare each replicated byte against
    /// per-lane bit masks to turn set bits into 0/1 bytes.
    #[inline]
    #[target_feature(enable = "sse2")]
    unsafe fn unpack_u1(ptr: *const u8) -> [__m128i; 1] {
        let (b0, b1) = unsafe { (ptr.read(), ptr.add(1).read()) };
        let bytes = _mm_set_epi64x(splat_byte(b1) as i64, splat_byte(b0) as i64);
        let bit_select = _mm_set1_epi64x(0x8040_2010_0804_0201u64 as i64);
        let selected = _mm_cmpeq_epi8(_mm_and_si128(bytes, bit_select), bit_select);
        [_mm_and_si128(selected, _mm_set1_epi8(1))]
    }

    /// 64 2-bit codes from 16 bytes: byte b holds dims 4b..4b+3 at bit pairs.
    /// The 16-bit shifts drag bits across byte boundaries, which the per-byte
    /// mask removes.
    #[inline]
    #[target_feature(enable = "sse2")]
    unsafe fn unpack_u2(ptr: *const u8) -> [__m128i; 4] {
        let raw = unsafe { _mm_loadu_si128(ptr as *const __m128i) };
        let mask = _mm_set1_epi8(0b11);
        [
            _mm_and_si128(raw, mask),
            _mm_and_si128(_mm_srli_epi16::<2>(raw), mask),
            _mm_and_si128(_mm_srli_epi16::<4>(raw), mask),
            _mm_and_si128(_mm_srli_epi16::<6>(raw), mask),
        ]
    }

    /// Position the top-bit plane (see [`super::pack_top_plane`]) of run `k`
    /// at `top_bit` within each byte.
    #[inline]
    #[target_feature(enable = "sse2")]
    fn top_plane_run(plane: u64, k: usize, top_bit: usize) -> __m128i {
        let lo = super::shift_plane(plane, 2 * k, top_bit);
        let hi = super::shift_plane(plane, 2 * k + 1, top_bit);
        _mm_and_si128(
            _mm_set_epi64x(hi as i64, lo as i64),
            _mm_set1_epi8(1 << top_bit),
        )
    }

    #[inline]
    #[target_feature(enable = "sse2")]
    unsafe fn unpack_u3(ptr: *const u8) -> [__m128i; 4] {
        let mut runs = unsafe { unpack_u2(ptr) };
        let plane = unsafe { (ptr.add(16) as *const u64).read_unaligned() };
        for (k, run) in runs.iter_mut().enumerate() {
            *run = _mm_or_si128(*run, top_plane_run(plane, k, 2));
        }
        runs
    }

    /// 16 4-bit codes from 8 bytes: low nibbles are the even dims, high
    /// nibbles the odd dims.
    #[inline]
    #[target_feature(enable = "sse2")]
    unsafe fn unpack_u4(ptr: *const u8) -> [__m128i; 1] {
        let word = unsafe { (ptr as *const u64).read_unaligned() };
        let mask = 0x0f0f_0f0f_0f0f_0f0fu64;
        [_mm_set_epi64x(
            ((word >> 4) & mask) as i64,
            (word & mask) as i64,
        )]
    }

    #[inline]
    #[target_feature(enable = "sse2")]
    unsafe fn unpack_u5(ptr: *const u8) -> [__m128i; 4] {
        let blk0 = unsafe { _mm_loadu_si128(ptr as *const __m128i) };
        let blk1 = unsafe { _mm_loadu_si128(ptr.add(16) as *const __m128i) };
        let plane = unsafe { (ptr.add(32) as *const u64).read_unaligned() };
        let mask = _mm_set1_epi8(0x0f);
        let mut runs = [
            _mm_and_si128(blk0, mask),
            _mm_and_si128(_mm_srli_epi16::<4>(blk0), mask),
            _mm_and_si128(blk1, mask),
            _mm_and_si128(_mm_srli_epi16::<4>(blk1), mask),
        ];
        for (k, run) in runs.iter_mut().enumerate() {
            *run = _mm_or_si128(*run, top_plane_run(plane, k, 4));
        }
        runs
    }

    #[inline]
    #[target_feature(enable = "sse2")]
    unsafe fn unpack_u6(ptr: *const u8) -> [__m128i; 4] {
        let blk0 = unsafe { _mm_loadu_si128(ptr as *const __m128i) };
        let blk1 = unsafe { _mm_loadu_si128(ptr.add(16) as *const __m128i) };
        let blk2 = unsafe { _mm_loadu_si128(ptr.add(32) as *const __m128i) };
        let mask6 = _mm_set1_epi8(0x3f);
        let mask2 = _mm_set1_epi8(0b1100_0000u8 as i8);
        let stolen = _mm_or_si128(
            _mm_or_si128(
                _mm_srli_epi16::<6>(_mm_and_si128(blk0, mask2)),
                _mm_srli_epi16::<4>(_mm_and_si128(blk1, mask2)),
            ),
            _mm_srli_epi16::<2>(_mm_and_si128(blk2, mask2)),
        );
        [
            _mm_and_si128(blk0, mask6),
            _mm_and_si128(blk1, mask6),
            _mm_and_si128(blk2, mask6),
            stolen,
        ]
    }

    #[inline]
    #[target_feature(enable = "sse2")]
    unsafe fn unpack_u7(ptr: *const u8) -> [__m128i; 4] {
        let mut runs = unsafe { unpack_u6(ptr) };
        let plane = unsafe { (ptr.add(48) as *const u64).read_unaligned() };
        for (k, run) in runs.iter_mut().enumerate() {
            *run = _mm_or_si128(*run, top_plane_run(plane, k, 6));
        }
        runs
    }

    #[inline]
    #[target_feature(enable = "sse2")]
    unsafe fn unpack_u8x16(ptr: *const u8) -> [__m128i; 1] {
        [unsafe { _mm_loadu_si128(ptr as *const __m128i) }]
    }

    /// FMA 16 code bytes against 16 query floats (AVX2: two 8-float halves).
    #[inline]
    #[target_feature(enable = "avx2", enable = "fma")]
    unsafe fn fma16_avx2(codes: __m128i, query: *const f32, acc: &mut [__m256; 2]) {
        let lo = _mm256_cvtepi32_ps(_mm256_cvtepu8_epi32(codes));
        acc[0] = _mm256_fmadd_ps(lo, unsafe { _mm256_loadu_ps(query) }, acc[0]);
        let hi = _mm256_cvtepi32_ps(_mm256_cvtepu8_epi32(_mm_srli_si128::<8>(codes)));
        acc[1] = _mm256_fmadd_ps(hi, unsafe { _mm256_loadu_ps(query.add(8)) }, acc[1]);
    }

    #[inline]
    #[target_feature(enable = "avx2")]
    unsafe fn reduce_add_avx2(acc: [__m256; 2]) -> f32 {
        let v = _mm256_add_ps(acc[0], acc[1]);
        let halves = _mm_add_ps(_mm256_castps256_ps128(v), _mm256_extractf128_ps::<1>(v));
        let pairs = _mm_add_ps(halves, _mm_movehl_ps(halves, halves));
        let total = _mm_add_ss(pairs, _mm_shuffle_ps::<0b01>(pairs, pairs));
        _mm_cvtss_f32(total)
    }

    /// FMA 16 code bytes against 16 query floats (AVX-512: one 16-float lane).
    #[inline]
    #[target_feature(enable = "avx512f")]
    unsafe fn fma16_avx512(codes: __m128i, query: *const f32, acc: &mut __m512) {
        let values = _mm512_cvtepi32_ps(_mm512_cvtepu8_epi32(codes));
        *acc = _mm512_fmadd_ps(values, unsafe { _mm512_loadu_ps(query) }, *acc);
    }

    macro_rules! x86_dot_kernel {
        ($name:ident, $dispatch:ident, $unpack:ident, $ex_bits:expr, $runs:expr) => {
            #[target_feature(enable = "avx2", enable = "fma")]
            unsafe fn $name(ex_query: &[f32], codes: &[u8]) -> f32 {
                const GROUP_DIMS: usize = if $runs == 1 { 16 } else { 64 };
                const GROUP_BYTES: usize = GROUP_DIMS * $ex_bits / 8;
                debug_assert_eq!(ex_query.len() % super::EX_DOT_BLOCK_DIMS, 0);
                debug_assert!(codes.len() * 8 <= ex_query.len() * $ex_bits);

                let groups = ex_query.len() / GROUP_DIMS;
                let full_groups = (codes.len() / GROUP_BYTES).min(groups);
                // Two accumulators per run position break the FMA latency
                // chain; they are summed once at the end.
                let mut acc = [_mm256_setzero_ps(); 2];
                for group in 0..full_groups {
                    // SAFETY: `group < full_groups` keeps both the code group
                    // and the query run in bounds.
                    let runs = unsafe { $unpack(codes.as_ptr().add(group * GROUP_BYTES)) };
                    for (run, codes16) in runs.into_iter().enumerate() {
                        unsafe {
                            fma16_avx2(
                                codes16,
                                ex_query.as_ptr().add(group * GROUP_DIMS + run * 16),
                                &mut acc,
                            )
                        };
                    }
                }
                let consumed = full_groups * GROUP_BYTES;
                if consumed < codes.len() && full_groups < groups {
                    // Zero-pad the final partial code group on the stack.
                    let mut padded = [0u8; GROUP_BYTES];
                    padded[..codes.len() - consumed].copy_from_slice(&codes[consumed..]);
                    let runs = unsafe { $unpack(padded.as_ptr()) };
                    for (run, codes16) in runs.into_iter().enumerate() {
                        unsafe {
                            fma16_avx2(
                                codes16,
                                ex_query.as_ptr().add(full_groups * GROUP_DIMS + run * 16),
                                &mut acc,
                            )
                        };
                    }
                }
                unsafe { reduce_add_avx2(acc) }
            }

            fn $dispatch(ex_query: &[f32], codes: &[u8]) -> f32 {
                // SAFETY: only selected when AVX2 and FMA were detected.
                unsafe { $name(ex_query, codes) }
            }
        };
    }

    macro_rules! x86_dot_kernel_avx512 {
        ($name:ident, $dispatch:ident, $unpack:ident, $ex_bits:expr, $runs:expr) => {
            #[target_feature(enable = "avx512f")]
            unsafe fn $name(ex_query: &[f32], codes: &[u8]) -> f32 {
                const GROUP_DIMS: usize = if $runs == 1 { 16 } else { 64 };
                const GROUP_BYTES: usize = GROUP_DIMS * $ex_bits / 8;
                debug_assert_eq!(ex_query.len() % super::EX_DOT_BLOCK_DIMS, 0);
                debug_assert!(codes.len() * 8 <= ex_query.len() * $ex_bits);

                let groups = ex_query.len() / GROUP_DIMS;
                let full_groups = (codes.len() / GROUP_BYTES).min(groups);
                // Alternating by group as well as run keeps two independent
                // FMA chains even for the single-run widths.
                let mut acc = [_mm512_setzero_ps(); 2];
                for group in 0..full_groups {
                    // SAFETY: `group < full_groups` keeps both the code group
                    // and the query run in bounds.
                    let runs = unsafe { $unpack(codes.as_ptr().add(group * GROUP_BYTES)) };
                    for (run, codes16) in runs.into_iter().enumerate() {
                        unsafe {
                            fma16_avx512(
                                codes16,
                                ex_query.as_ptr().add(group * GROUP_DIMS + run * 16),
                                &mut acc[(group + run) % 2],
                            )
                        };
                    }
                }
                let consumed = full_groups * GROUP_BYTES;
                if consumed < codes.len() && full_groups < groups {
                    let mut padded = [0u8; GROUP_BYTES];
                    padded[..codes.len() - consumed].copy_from_slice(&codes[consumed..]);
                    let runs = unsafe { $unpack(padded.as_ptr()) };
                    for (run, codes16) in runs.into_iter().enumerate() {
                        unsafe {
                            fma16_avx512(
                                codes16,
                                ex_query.as_ptr().add(full_groups * GROUP_DIMS + run * 16),
                                &mut acc[(full_groups + run) % 2],
                            )
                        };
                    }
                }
                _mm512_reduce_add_ps(_mm512_add_ps(acc[0], acc[1]))
            }

            fn $dispatch(ex_query: &[f32], codes: &[u8]) -> f32 {
                // SAFETY: only selected when AVX-512F was detected.
                unsafe { $name(ex_query, codes) }
            }
        };
    }

    x86_dot_kernel!(dot_u1_avx2, dot_u1_avx2_dispatch, unpack_u1, 1, 1);
    x86_dot_kernel!(dot_u2_avx2, dot_u2_avx2_dispatch, unpack_u2, 2, 4);
    x86_dot_kernel!(dot_u3_avx2, dot_u3_avx2_dispatch, unpack_u3, 3, 4);
    x86_dot_kernel!(dot_u4_avx2, dot_u4_avx2_dispatch, unpack_u4, 4, 1);
    x86_dot_kernel!(dot_u5_avx2, dot_u5_avx2_dispatch, unpack_u5, 5, 4);
    x86_dot_kernel!(dot_u6_avx2, dot_u6_avx2_dispatch, unpack_u6, 6, 4);
    x86_dot_kernel!(dot_u7_avx2, dot_u7_avx2_dispatch, unpack_u7, 7, 4);
    x86_dot_kernel!(dot_u8_avx2, dot_u8_avx2_dispatch, unpack_u8x16, 8, 1);

    x86_dot_kernel_avx512!(dot_u1_avx512, dot_u1_avx512_dispatch, unpack_u1, 1, 1);
    x86_dot_kernel_avx512!(dot_u2_avx512, dot_u2_avx512_dispatch, unpack_u2, 2, 4);
    x86_dot_kernel_avx512!(dot_u3_avx512, dot_u3_avx512_dispatch, unpack_u3, 3, 4);
    x86_dot_kernel_avx512!(dot_u4_avx512, dot_u4_avx512_dispatch, unpack_u4, 4, 1);
    x86_dot_kernel_avx512!(dot_u5_avx512, dot_u5_avx512_dispatch, unpack_u5, 5, 4);
    x86_dot_kernel_avx512!(dot_u6_avx512, dot_u6_avx512_dispatch, unpack_u6, 6, 4);
    x86_dot_kernel_avx512!(dot_u7_avx512, dot_u7_avx512_dispatch, unpack_u7, 7, 4);
    x86_dot_kernel_avx512!(dot_u8_avx512, dot_u8_avx512_dispatch, unpack_u8x16, 8, 1);
}

#[cfg(target_arch = "aarch64")]
mod neon {
    use super::ExDotFn;
    use std::arch::aarch64::*;

    pub(super) fn kernel(ex_bits: u8) -> ExDotFn {
        match ex_bits {
            1 => dot_u1_neon_dispatch,
            2 => dot_u2_neon_dispatch,
            3 => dot_u3_neon_dispatch,
            4 => dot_u4_neon_dispatch,
            5 => dot_u5_neon_dispatch,
            6 => dot_u6_neon_dispatch,
            7 => dot_u7_neon_dispatch,
            8 => dot_u8_neon_dispatch,
            _ => unreachable!("invalid RabitQ ex_bits={ex_bits}"),
        }
    }

    #[inline]
    #[target_feature(enable = "neon")]
    unsafe fn unpack_u1(ptr: *const u8) -> [uint8x16_t; 1] {
        let (b0, b1) = unsafe { (ptr.read(), ptr.add(1).read()) };
        let bytes = vcombine_u8(vdup_n_u8(b0), vdup_n_u8(b1));
        let bit_select = vreinterpretq_u8_u64(vdupq_n_u64(0x8040_2010_0804_0201));
        [vandq_u8(vtstq_u8(bytes, bit_select), vdupq_n_u8(1))]
    }

    #[inline]
    #[target_feature(enable = "neon")]
    unsafe fn unpack_u2(ptr: *const u8) -> [uint8x16_t; 4] {
        let raw = unsafe { vld1q_u8(ptr) };
        let mask = vdupq_n_u8(0b11);
        [
            vandq_u8(raw, mask),
            vandq_u8(vshrq_n_u8::<2>(raw), mask),
            vandq_u8(vshrq_n_u8::<4>(raw), mask),
            vshrq_n_u8::<6>(raw),
        ]
    }

    #[inline]
    #[target_feature(enable = "neon")]
    fn top_plane_run(plane: u64, k: usize, top_bit: usize) -> uint8x16_t {
        let lo = super::shift_plane(plane, 2 * k, top_bit);
        let hi = super::shift_plane(plane, 2 * k + 1, top_bit);
        vandq_u8(
            vreinterpretq_u8_u64(vcombine_u64(vcreate_u64(lo), vcreate_u64(hi))),
            vdupq_n_u8(1 << top_bit),
        )
    }

    #[inline]
    #[target_feature(enable = "neon")]
    unsafe fn unpack_u3(ptr: *const u8) -> [uint8x16_t; 4] {
        let mut runs = unsafe { unpack_u2(ptr) };
        let plane = unsafe { (ptr.add(16) as *const u64).read_unaligned() };
        for (k, run) in runs.iter_mut().enumerate() {
            *run = vorrq_u8(*run, top_plane_run(plane, k, 2));
        }
        runs
    }

    #[inline]
    #[target_feature(enable = "neon")]
    unsafe fn unpack_u4(ptr: *const u8) -> [uint8x16_t; 1] {
        let word = unsafe { (ptr as *const u64).read_unaligned() };
        let mask = 0x0f0f_0f0f_0f0f_0f0fu64;
        [vreinterpretq_u8_u64(vcombine_u64(
            vcreate_u64(word & mask),
            vcreate_u64((word >> 4) & mask),
        ))]
    }

    #[inline]
    #[target_feature(enable = "neon")]
    unsafe fn unpack_u5(ptr: *const u8) -> [uint8x16_t; 4] {
        let blk0 = unsafe { vld1q_u8(ptr) };
        let blk1 = unsafe { vld1q_u8(ptr.add(16)) };
        let plane = unsafe { (ptr.add(32) as *const u64).read_unaligned() };
        let mask = vdupq_n_u8(0x0f);
        let mut runs = [
            vandq_u8(blk0, mask),
            vshrq_n_u8::<4>(blk0),
            vandq_u8(blk1, mask),
            vshrq_n_u8::<4>(blk1),
        ];
        for (k, run) in runs.iter_mut().enumerate() {
            *run = vorrq_u8(*run, top_plane_run(plane, k, 4));
        }
        runs
    }

    #[inline]
    #[target_feature(enable = "neon")]
    unsafe fn unpack_u6(ptr: *const u8) -> [uint8x16_t; 4] {
        let blk0 = unsafe { vld1q_u8(ptr) };
        let blk1 = unsafe { vld1q_u8(ptr.add(16)) };
        let blk2 = unsafe { vld1q_u8(ptr.add(32)) };
        let mask6 = vdupq_n_u8(0x3f);
        let stolen = vorrq_u8(
            vorrq_u8(
                vshrq_n_u8::<6>(blk0),
                vshlq_n_u8::<2>(vshrq_n_u8::<6>(blk1)),
            ),
            vshlq_n_u8::<4>(vshrq_n_u8::<6>(blk2)),
        );
        [
            vandq_u8(blk0, mask6),
            vandq_u8(blk1, mask6),
            vandq_u8(blk2, mask6),
            stolen,
        ]
    }

    #[inline]
    #[target_feature(enable = "neon")]
    unsafe fn unpack_u7(ptr: *const u8) -> [uint8x16_t; 4] {
        let mut runs = unsafe { unpack_u6(ptr) };
        let plane = unsafe { (ptr.add(48) as *const u64).read_unaligned() };
        for (k, run) in runs.iter_mut().enumerate() {
            *run = vorrq_u8(*run, top_plane_run(plane, k, 6));
        }
        runs
    }

    #[inline]
    #[target_feature(enable = "neon")]
    unsafe fn unpack_u8x16(ptr: *const u8) -> [uint8x16_t; 1] {
        [unsafe { vld1q_u8(ptr) }]
    }

    /// FMA 16 code bytes against 16 query floats over four 4-float lanes.
    #[inline]
    #[target_feature(enable = "neon")]
    unsafe fn fma16_neon(codes: uint8x16_t, query: *const f32, acc: &mut [float32x4_t; 4]) {
        let lo = vmovl_u8(vget_low_u8(codes));
        let hi = vmovl_u8(vget_high_u8(codes));
        let c0 = vcvtq_f32_u32(vmovl_u16(vget_low_u16(lo)));
        let c1 = vcvtq_f32_u32(vmovl_u16(vget_high_u16(lo)));
        let c2 = vcvtq_f32_u32(vmovl_u16(vget_low_u16(hi)));
        let c3 = vcvtq_f32_u32(vmovl_u16(vget_high_u16(hi)));
        unsafe {
            acc[0] = vfmaq_f32(acc[0], c0, vld1q_f32(query));
            acc[1] = vfmaq_f32(acc[1], c1, vld1q_f32(query.add(4)));
            acc[2] = vfmaq_f32(acc[2], c2, vld1q_f32(query.add(8)));
            acc[3] = vfmaq_f32(acc[3], c3, vld1q_f32(query.add(12)));
        }
    }

    macro_rules! neon_dot_kernel {
        ($name:ident, $dispatch:ident, $unpack:ident, $ex_bits:expr, $runs:expr) => {
            #[target_feature(enable = "neon")]
            unsafe fn $name(ex_query: &[f32], codes: &[u8]) -> f32 {
                const GROUP_DIMS: usize = if $runs == 1 { 16 } else { 64 };
                const GROUP_BYTES: usize = GROUP_DIMS * $ex_bits / 8;
                debug_assert_eq!(ex_query.len() % super::EX_DOT_BLOCK_DIMS, 0);
                debug_assert!(codes.len() * 8 <= ex_query.len() * $ex_bits);

                let groups = ex_query.len() / GROUP_DIMS;
                let full_groups = (codes.len() / GROUP_BYTES).min(groups);
                let mut acc = [vdupq_n_f32(0.0); 4];
                for group in 0..full_groups {
                    // SAFETY: `group < full_groups` keeps both the code group
                    // and the query run in bounds.
                    let runs = unsafe { $unpack(codes.as_ptr().add(group * GROUP_BYTES)) };
                    for (run, codes16) in runs.into_iter().enumerate() {
                        unsafe {
                            fma16_neon(
                                codes16,
                                ex_query.as_ptr().add(group * GROUP_DIMS + run * 16),
                                &mut acc,
                            )
                        };
                    }
                }
                let consumed = full_groups * GROUP_BYTES;
                if consumed < codes.len() && full_groups < groups {
                    // Zero-pad the final partial code group on the stack.
                    let mut padded = [0u8; GROUP_BYTES];
                    padded[..codes.len() - consumed].copy_from_slice(&codes[consumed..]);
                    let runs = unsafe { $unpack(padded.as_ptr()) };
                    for (run, codes16) in runs.into_iter().enumerate() {
                        unsafe {
                            fma16_neon(
                                codes16,
                                ex_query.as_ptr().add(full_groups * GROUP_DIMS + run * 16),
                                &mut acc,
                            )
                        };
                    }
                }
                vaddvq_f32(vaddq_f32(
                    vaddq_f32(acc[0], acc[1]),
                    vaddq_f32(acc[2], acc[3]),
                ))
            }

            fn $dispatch(ex_query: &[f32], codes: &[u8]) -> f32 {
                // SAFETY: NEON is part of the aarch64 baseline.
                unsafe { $name(ex_query, codes) }
            }
        };
    }

    neon_dot_kernel!(dot_u1_neon, dot_u1_neon_dispatch, unpack_u1, 1, 1);
    neon_dot_kernel!(dot_u2_neon, dot_u2_neon_dispatch, unpack_u2, 2, 4);
    neon_dot_kernel!(dot_u3_neon, dot_u3_neon_dispatch, unpack_u3, 3, 4);
    neon_dot_kernel!(dot_u4_neon, dot_u4_neon_dispatch, unpack_u4, 4, 1);
    neon_dot_kernel!(dot_u5_neon, dot_u5_neon_dispatch, unpack_u5, 5, 4);
    neon_dot_kernel!(dot_u6_neon, dot_u6_neon_dispatch, unpack_u6, 6, 4);
    neon_dot_kernel!(dot_u7_neon, dot_u7_neon_dispatch, unpack_u7, 7, 4);
    neon_dot_kernel!(dot_u8_neon, dot_u8_neon_dispatch, unpack_u8x16, 8, 1);
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::rngs::SmallRng;
    use rand::{Rng, SeedableRng};
    use rstest::rstest;

    /// Bit-pack code values sequentially (LSB-first), the on-disk ex-code layout.
    fn pack_sequential(values: &[u8], ex_bits: u8) -> Vec<u8> {
        let mut out = vec![0u8; (values.len() * ex_bits as usize).div_ceil(8)];
        for (dim, &value) in values.iter().enumerate() {
            let bit_offset = dim * ex_bits as usize;
            let bits = (value as u16) << (bit_offset % 8);
            out[bit_offset / 8] |= bits as u8;
            if bits >> 8 != 0 {
                out[bit_offset / 8 + 1] |= (bits >> 8) as u8;
            }
        }
        out
    }

    fn kernel_codes(values: &[u8], dim: usize, ex_bits: u8) -> Vec<u8> {
        debug_assert_eq!(values.len(), dim);
        let mut out = vec![0u8; blocked_ex_code_bytes(dim, ex_bits)];
        pack_blocked_row(values, ex_bits, &mut out);
        out
    }

    fn available_kernels(ex_bits: u8) -> Vec<(&'static str, ExDotFn)> {
        // `mut` is only exercised on x86_64 where extra kernels may be pushed.
        #[allow(unused_mut)]
        let mut kernels = vec![
            ("scalar", scalar_kernel(ex_bits)),
            ("dispatched", ex_dot_kernel(ex_bits)),
        ];
        #[cfg(target_arch = "x86_64")]
        {
            if std::arch::is_x86_feature_detected!("avx2")
                && std::arch::is_x86_feature_detected!("fma")
            {
                kernels.push(("avx2", x86::avx2_kernel(ex_bits)));
            }
            if std::arch::is_x86_feature_detected!("avx512f") {
                kernels.push(("avx512", x86::avx512_kernel(ex_bits)));
            }
        }
        kernels
    }

    #[rstest]
    fn test_ex_dot_matches_reference(
        #[values(1, 2, 3, 4, 5, 6, 7, 8)] ex_bits: u8,
        #[values(7, 16, 60, 64, 100, 128, 1024, 1536, 2048)] dim: usize,
    ) {
        let mut rng = SmallRng::seed_from_u64(42 + ex_bits as u64 * 1000 + dim as u64);
        let max_code = ((1u16 << ex_bits) - 1) as u8;
        let values = (0..dim)
            .map(|_| rng.random_range(0..=max_code))
            .collect::<Vec<_>>();
        let query = (0..dim)
            .map(|_| rng.random_range(-1.0f32..1.0))
            .collect::<Vec<_>>();

        let expected = query
            .iter()
            .zip(values.iter())
            .map(|(q, &c)| *q as f64 * c as f64)
            .sum::<f64>();

        let codes = kernel_codes(&values, dim, ex_bits);
        let mut ex_query = vec![0.0; padded_query_len(dim)];
        pad_query_into(&query, &mut ex_query);

        let tolerance = 1e-3 * expected.abs().max(1.0);
        for (name, kernel) in available_kernels(ex_bits) {
            let actual = kernel(&ex_query, &codes) as f64;
            assert!(
                (actual - expected).abs() <= tolerance,
                "ex_bits={ex_bits} dim={dim} kernel={name}: {actual} != {expected}"
            );
        }
    }

    #[rstest]
    fn test_unpack_group_roundtrip(#[values(1, 2, 3, 4, 5, 6, 7, 8)] ex_bits: u8) {
        let mut rng = SmallRng::seed_from_u64(7 + ex_bits as u64);
        let max_code = ((1u16 << ex_bits) - 1) as u8;
        let values = (0..EX_DOT_BLOCK_DIMS)
            .map(|_| rng.random_range(0..=max_code))
            .collect::<Vec<_>>();
        let codes = kernel_codes(&values, EX_DOT_BLOCK_DIMS, ex_bits);

        // Unpacking each kernel group must reproduce the values in natural
        // dim order.
        let dims = group_dims(ex_bits);
        let bytes = group_bytes(ex_bits);
        let mut unpacked = [0u8; 64];
        for group in 0..EX_DOT_BLOCK_DIMS / dims {
            unpack_group(
                ex_bits,
                &codes[group * bytes..(group + 1) * bytes],
                &mut unpacked,
            );
            assert_eq!(
                &unpacked[..dims],
                &values[group * dims..(group + 1) * dims],
                "ex_bits={ex_bits} group={group}"
            );
        }
    }

    /// The legacy sequential rows must repack into exactly what the writer
    /// produces from the unpacked values.
    #[rstest]
    fn test_repack_sequential_matches_blocked(
        #[values(1, 2, 3, 4, 5, 6, 7, 8)] ex_bits: u8,
        #[values(7, 64, 100, 1536)] dim: usize,
    ) {
        let mut rng = SmallRng::seed_from_u64(11 + ex_bits as u64 * 100 + dim as u64);
        let max_code = ((1u16 << ex_bits) - 1) as u8;
        let values = (0..dim)
            .map(|_| rng.random_range(0..=max_code))
            .collect::<Vec<_>>();
        let seq = pack_sequential(&values, ex_bits);

        let mut repacked = vec![0u8; blocked_ex_code_bytes(dim, ex_bits)];
        repack_sequential_row(&seq, dim, ex_bits, &mut repacked);
        assert_eq!(repacked, kernel_codes(&values, dim, ex_bits));

        // For the widths where the sequential layout is already blocked
        // (modulo trailing padding), the raw row must be a prefix.
        if sequential_matches_blocked(ex_bits) {
            assert_eq!(&repacked[..seq.len()], &seq);
            assert!(repacked[seq.len()..].iter().all(|&byte| byte == 0));
        }
    }

    /// Dense dim sweep for the bit-plane widths: every tail shape within the
    /// 64-dim kernel group, plus multi-group sizes.
    #[rstest]
    fn test_ex_dot_plane_widths_dense_dims(#[values(3, 5)] ex_bits: u8) {
        let mut rng = SmallRng::seed_from_u64(97 + ex_bits as u64);
        let max_code = ((1u16 << ex_bits) - 1) as u8;
        for dim in (1..=160).chain([255, 256, 1000, 1536, 2048]) {
            let values = (0..dim)
                .map(|_| rng.random_range(0..=max_code))
                .collect::<Vec<_>>();
            let query = (0..dim)
                .map(|_| rng.random_range(-1.0f32..1.0))
                .collect::<Vec<_>>();
            let expected = query
                .iter()
                .zip(values.iter())
                .map(|(q, &c)| *q as f64 * c as f64)
                .sum::<f64>();

            let codes = kernel_codes(&values, dim, ex_bits);
            let mut ex_query = vec![0.0; padded_query_len(dim)];
            pad_query_into(&query, &mut ex_query);
            let tolerance = 1e-3 * expected.abs().max(1.0);
            for (name, kernel) in available_kernels(ex_bits) {
                let actual = kernel(&ex_query, &codes) as f64;
                assert!(
                    (actual - expected).abs() <= tolerance,
                    "ex_bits={ex_bits} dim={dim} kernel={name}: {actual} != {expected}"
                );
            }
        }
    }

    #[test]
    fn test_pad_query_pads_with_zeros() {
        let query = vec![1.0f32; 100];
        let mut padded = vec![f32::NAN; padded_query_len(query.len())];
        pad_query_into(&query, &mut padded);
        assert_eq!(padded.len(), 128);
        assert_eq!(&padded[..100], &query[..]);
        assert!(padded[100..].iter().all(|&value| value == 0.0));
    }
}
