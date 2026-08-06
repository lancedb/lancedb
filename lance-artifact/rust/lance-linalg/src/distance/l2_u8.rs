// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Unsigned int8 squared L2 distance with runtime-dispatched SIMD backends.
//!
//! Computes `Σ(a[i] - b[i])²` for u8 slices, returning a u32 result.
//! Used by Scalar Quantization (SQ) distance computation where both L2
//! and Cosine metric types operate on quantized u8 codes.
//!
//! Backends (selected at runtime, best available wins):
//!   1. scalar     — portable reference, also used for tails
//!   2. avx2       — VPSADBW-style abs diff + VPMADDWD squaring, 32 elements/iter
//!   3. avx512vnni — same approach with VPDPWSSD accumulation, 64 elements/iter
//!
//! ## Algorithm
//!
//! Each SIMD backend computes |a - b| per element using saturating
//! subtraction: `max(a,b) - min(a,b) = (a ⊖ b) | (b ⊖ a)` where ⊖
//! is unsigned saturating subtraction. The absolute differences are then
//! zero-extended to i16 and squared via VPMADDWD (which also pairwise-
//! accumulates adjacent products into i32).

use std::sync::OnceLock;

/// Portable scalar u8 squared L2 distance, also used for SIMD tail elements.
#[inline]
pub fn l2_u8_scalar(a: &[u8], b: &[u8]) -> u32 {
    debug_assert_eq!(a.len(), b.len());
    a.iter()
        .zip(b.iter())
        .map(|(&x, &y)| (x.abs_diff(y) as u32).pow(2))
        .sum()
}

#[cfg(target_arch = "x86_64")]
mod x86 {
    use std::arch::x86_64::*;

    /// Horizontal sum of all 8 × i32 lanes in a __m256i.
    #[inline(always)]
    unsafe fn hsum_epi32_avx2(v: __m256i) -> u32 {
        let lo128 = _mm256_castsi256_si128(v);
        let hi128 = _mm256_extracti128_si256(v, 1);
        let mut sum128 = _mm_add_epi32(lo128, hi128);
        sum128 = _mm_hadd_epi32(sum128, sum128);
        sum128 = _mm_hadd_epi32(sum128, sum128);
        _mm_cvtsi128_si32(sum128) as u32
    }

    /// AVX2 path: saturating-sub abs diff, unpack to i16, VPMADDWD to square.
    /// 32 elements/iter.
    #[target_feature(enable = "avx2")]
    pub unsafe fn l2_u8_avx2(a: &[u8], b: &[u8]) -> u32 {
        debug_assert_eq!(a.len(), b.len());
        let n = a.len();
        let zeros = _mm256_setzero_si256();
        let mut acc = _mm256_setzero_si256();
        let mut i = 0usize;

        while i + 32 <= n {
            let av = _mm256_loadu_si256(a.as_ptr().add(i) as *const __m256i);
            let bv = _mm256_loadu_si256(b.as_ptr().add(i) as *const __m256i);

            // |a - b| via saturating subtraction: max(a-b, 0) | max(b-a, 0)
            let abs_diff = _mm256_or_si256(_mm256_subs_epu8(av, bv), _mm256_subs_epu8(bv, av));

            // Zero-extend u8→i16 via interleave with zeros.
            // unpacklo/hi within each 128-bit lane.
            let diff_lo = _mm256_unpacklo_epi8(abs_diff, zeros);
            let diff_hi = _mm256_unpackhi_epi8(abs_diff, zeros);

            // VPMADDWD squares adjacent i16 pairs and sums into i32.
            acc = _mm256_add_epi32(acc, _mm256_madd_epi16(diff_lo, diff_lo));
            acc = _mm256_add_epi32(acc, _mm256_madd_epi16(diff_hi, diff_hi));
            i += 32;
        }

        let mut result = hsum_epi32_avx2(acc);

        // Scalar tail
        while i < n {
            let d = a[i].abs_diff(b[i]) as u32;
            result += d * d;
            i += 1;
        }
        result
    }

    /// AVX-512 VNNI path: abs diff + VPDPWSSD for fused square-accumulate.
    /// 64 elements/iter.
    #[target_feature(enable = "avx512f,avx512bw,avx512vnni")]
    pub unsafe fn l2_u8_avx512_vnni(a: &[u8], b: &[u8]) -> u32 {
        debug_assert_eq!(a.len(), b.len());
        let n = a.len();
        let zeros = _mm512_setzero_si512();
        let mut acc = _mm512_setzero_si512();
        let mut i = 0usize;

        while i + 64 <= n {
            let av = _mm512_loadu_si512(a.as_ptr().add(i) as *const __m512i);
            let bv = _mm512_loadu_si512(b.as_ptr().add(i) as *const __m512i);

            // |a - b| via saturating subtraction
            let abs_diff = _mm512_or_si512(_mm512_subs_epu8(av, bv), _mm512_subs_epu8(bv, av));

            // Zero-extend u8→i16 via interleave with zeros
            let diff_lo = _mm512_unpacklo_epi8(abs_diff, zeros);
            let diff_hi = _mm512_unpackhi_epi8(abs_diff, zeros);

            // VPDPWSSD: signed i16 pairwise multiply-add into i32 accumulator.
            // diff values are 0..255, fitting in i16 as positive, so signed is fine.
            acc = _mm512_dpwssd_epi32(acc, diff_lo, diff_lo);
            acc = _mm512_dpwssd_epi32(acc, diff_hi, diff_hi);
            i += 64;
        }

        let mut result = _mm512_reduce_add_epi32(acc) as u32;

        // Scalar tail
        while i < n {
            let d = a[i].abs_diff(b[i]) as u32;
            result += d * d;
            i += 1;
        }
        result
    }
}

type L2U8Fn = fn(&[u8], &[u8]) -> u32;

static DISPATCH: OnceLock<L2U8Fn> = OnceLock::new();

fn select_backend() -> L2U8Fn {
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx512f")
            && is_x86_feature_detected!("avx512bw")
            && is_x86_feature_detected!("avx512vnni")
        {
            return |a, b| unsafe { x86::l2_u8_avx512_vnni(a, b) };
        }

        if is_x86_feature_detected!("avx2") {
            return |a, b| unsafe { x86::l2_u8_avx2(a, b) };
        }
        // AvxFma and Avx hosts (AMD Piledriver / Steamroller, Intel Sandy
        // Bridge / Ivy Bridge) fall through to scalar: the AVX2 inner uses
        // AVX2 integer ops (`vpsubusb` / `vpmaddwd`) which neither AVX nor
        // AVX+FMA provides.
    }

    l2_u8_scalar
}

/// Dispatched u8 squared L2 distance, selecting the best available SIMD backend.
#[inline]
pub fn l2_u8(a: &[u8], b: &[u8]) -> u32 {
    (DISPATCH.get_or_init(select_backend))(a, b)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fill_random(buf: &mut [u8], seed: &mut u32) {
        for slot in buf.iter_mut() {
            *seed = seed.wrapping_mul(1103515245).wrapping_add(12345);
            *slot = (*seed >> 16) as u8;
        }
    }

    const SIZES: &[usize] = &[
        0, 1, 7, 15, 16, 31, 32, 33, 63, 64, 65, 127, 128, 255, 256, 1024, 4096, 4097,
    ];

    fn check_all_backends(a: &[u8], b: &[u8], case: &str) {
        let reference = l2_u8_scalar(a, b);

        #[cfg(target_arch = "x86_64")]
        {
            if is_x86_feature_detected!("avx2") {
                let got = unsafe { x86::l2_u8_avx2(a, b) };
                assert_eq!(got, reference, "avx2 [{case}] n={}", a.len());
            }

            if is_x86_feature_detected!("avx512f")
                && is_x86_feature_detected!("avx512bw")
                && is_x86_feature_detected!("avx512vnni")
            {
                let got = unsafe { x86::l2_u8_avx512_vnni(a, b) };
                assert_eq!(got, reference, "avx512_vnni [{case}] n={}", a.len());
            }
        }

        assert_eq!(l2_u8(a, b), reference, "dispatch [{case}] n={}", a.len());
    }

    #[test]
    fn random_inputs_across_sizes_and_seeds() {
        let mut a = vec![0u8; 4097];
        let mut b = vec![0u8; 4097];

        for seed_idx in 0..4u32 {
            let mut seed = 0xC0FFEE_u32.wrapping_add(seed_idx.wrapping_mul(7919));
            for &n in SIZES {
                fill_random(&mut a[..n], &mut seed);
                fill_random(&mut b[..n], &mut seed);
                check_all_backends(&a[..n], &b[..n], "random");
            }
        }
    }

    #[test]
    fn boundary_values() {
        let mut a = vec![0u8; 4097];
        let mut b = vec![0u8; 4097];

        for &n in SIZES {
            // max diff: |255 - 0|² × n
            a[..n].fill(u8::MAX);
            b[..n].fill(0);
            check_all_backends(&a[..n], &b[..n], "max-0");

            a[..n].fill(0);
            b[..n].fill(u8::MAX);
            check_all_backends(&a[..n], &b[..n], "0-max");

            // identical vectors: distance = 0
            a[..n].fill(u8::MAX);
            b[..n].fill(u8::MAX);
            check_all_backends(&a[..n], &b[..n], "max-max");
            assert_eq!(l2_u8_scalar(&a[..n], &b[..n]), 0);

            // zeros
            a[..n].fill(0);
            b[..n].fill(0);
            check_all_backends(&a[..n], &b[..n], "0-0");
            assert_eq!(l2_u8_scalar(&a[..n], &b[..n]), 0);

            // alternating
            for i in 0..n {
                a[i] = if i & 1 == 0 { 0 } else { u8::MAX };
                b[i] = if i & 1 == 0 { u8::MAX } else { 0 };
            }
            check_all_backends(&a[..n], &b[..n], "alt 0/max");
        }
    }

    #[test]
    fn one_sided_zeros() {
        let mut a = vec![0u8; 4097];
        let mut b = vec![0u8; 4097];

        for &n in SIZES {
            let mut seed = 0xDEAD_BEEF_u32;
            fill_random(&mut a[..n], &mut seed);
            b[..n].fill(0);
            check_all_backends(&a[..n], &b[..n], "b=0");

            a[..n].fill(0);
            fill_random(&mut b[..n], &mut seed);
            check_all_backends(&a[..n], &b[..n], "a=0");
        }
    }

    #[test]
    fn known_values() {
        // 3² + 1² = 10
        assert_eq!(l2_u8_scalar(&[10, 20], &[7, 21]), 10);
        assert_eq!(l2_u8(&[10, 20], &[7, 21]), 10);

        // max single-element distance: 255² = 65025
        assert_eq!(l2_u8(&[0], &[255]), 65025);
        assert_eq!(l2_u8(&[255], &[0]), 65025);
    }
}
