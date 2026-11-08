// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Unsigned int8 cosine distance with runtime-dispatched SIMD backends.
//!
//! Computes `1 - dot(a,b) / (‖a‖ × ‖b‖)` for u8 slices in a single
//! pass over the data. The fused kernel maintains three accumulators
//! simultaneously — `Σ(a·b)`, `Σ(a²)`, `Σ(b²)` — so memory is only
//! traversed once instead of 2-3 times (norm + dot).
//!
//! Backends (selected at runtime, best available wins):
//!   1. scalar     — portable reference, also used for tails
//!   2. avx2       — zero-extend u8→i16, triple VPMADDWD, 32 elements/iter
//!   3. avx512vnni — same with VPDPWSSD accumulation, 64 elements/iter

use super::assert_equal_lengths;

/// Intermediate results from the fused u8 cosine kernel: (dot_ab, norm_a², norm_b²).
///
/// Separated from the final normalization so SIMD backends can be tested
/// for exact integer equality before the f32 division.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CosineAccumulators {
    pub dot_ab: u32,
    pub norm_a_sq: u32,
    pub norm_b_sq: u32,
}

type CosineU8AccumKernel = unsafe fn(&[u8], &[u8]) -> CosineAccumulators;

lance_core::dispatch! {
    /// There are no AVX or AVX+FMA candidates: the SIMD kernels need AVX2
    /// integer ops, so those hosts use the scalar kernel.
    static COSINE_U8_ACCUM: Dispatch<CosineU8AccumKernel> = {
        #[cfg(target_arch = "x86_64")]
        {
            &[
                x86::COSINE_U8_ACCUM_AVX512_VNNI,
                x86::COSINE_U8_ACCUM_AVX2,
                COSINE_U8_ACCUM_SCALAR,
            ]
        }
        #[cfg(not(target_arch = "x86_64"))]
        {
            &[COSINE_U8_ACCUM_SCALAR]
        }
    };
}

lance_core::target_kernel! {
    #[target_features()]
    #[candidate(COSINE_U8_ACCUM_SCALAR: CosineU8AccumKernel)]
    /// Portable scalar fused cosine accumulation.
    #[inline]
    pub fn cosine_u8_accum_scalar(a: &[u8], b: &[u8]) -> CosineAccumulators {
        assert_equal_lengths(a.len(), b.len());
        let (mut dot_ab, mut norm_a_sq, mut norm_b_sq) = (0u32, 0u32, 0u32);
        for (&x, &y) in a.iter().zip(b.iter()) {
            let (xu, yu) = (x as u32, y as u32);
            dot_ab += xu * yu;
            norm_a_sq += xu * xu;
            norm_b_sq += yu * yu;
        }
        CosineAccumulators {
            dot_ab,
            norm_a_sq,
            norm_b_sq,
        }
    }
}

/// Convert accumulators to cosine distance: `1 - dot / (‖a‖ × ‖b‖)`.
#[inline]
fn normalize(acc: CosineAccumulators) -> f32 {
    let na = (acc.norm_a_sq as f32).sqrt();
    let nb = (acc.norm_b_sq as f32).sqrt();
    let denom = na * nb;
    if denom == 0.0 {
        // Both zero-norm → identical → distance 0.
        // One zero-norm → undefined, but 0 is a safe sentinel.
        return 0.0;
    }
    1.0 - acc.dot_ab as f32 / denom
}

/// Portable scalar u8 cosine distance.
#[inline]
pub fn cosine_u8_scalar(a: &[u8], b: &[u8]) -> f32 {
    normalize(cosine_u8_accum_scalar(a, b))
}

#[cfg(target_arch = "x86_64")]
mod x86 {
    use std::arch::x86_64::*;

    use super::{CosineAccumulators, CosineU8AccumKernel};

    /// Horizontal sum of all 8 × i32 lanes in a __m256i.
    #[target_feature(enable = "avx2")]
    #[inline]
    fn hsum_epi32_avx2(v: __m256i) -> u32 {
        let lo128 = _mm256_castsi256_si128(v);
        let hi128 = _mm256_extracti128_si256(v, 1);
        let mut sum128 = _mm_add_epi32(lo128, hi128);
        sum128 = _mm_hadd_epi32(sum128, sum128);
        sum128 = _mm_hadd_epi32(sum128, sum128);
        _mm_cvtsi128_si32(sum128) as u32
    }

    lance_core::target_kernel! {
        #[target_features("avx2")]
        #[candidate(pub(super) COSINE_U8_ACCUM_AVX2: CosineU8AccumKernel)]
        /// AVX2 fused cosine: three VPMADDWD products per half, 32 elements/iter.
        pub(super) fn cosine_u8_accum_avx2(a: &[u8], b: &[u8]) -> CosineAccumulators {
            // Callers pass equal lengths; `min` keeps the unchecked loads in
            // bounds for any input, as a safe function must.
            let n = a.len().min(b.len());
            let mut acc_dot = _mm256_setzero_si256();
            let mut acc_na = _mm256_setzero_si256();
            let mut acc_nb = _mm256_setzero_si256();
            let mut i = 0usize;

            while i + 32 <= n {
                let (av, bv) = unsafe {
                    (
                        _mm256_loadu_si256(a.as_ptr().add(i) as *const __m256i),
                        _mm256_loadu_si256(b.as_ptr().add(i) as *const __m256i),
                    )
                };

                // Zero-extend each 128-bit half to 16 × i16.
                let a_lo = _mm256_cvtepu8_epi16(_mm256_castsi256_si128(av));
                let a_hi = _mm256_cvtepu8_epi16(_mm256_extracti128_si256(av, 1));
                let b_lo = _mm256_cvtepu8_epi16(_mm256_castsi256_si128(bv));
                let b_hi = _mm256_cvtepu8_epi16(_mm256_extracti128_si256(bv, 1));

                // VPMADDWD: pairwise multiply i16 and accumulate pairs into i32.
                acc_dot = _mm256_add_epi32(acc_dot, _mm256_madd_epi16(a_lo, b_lo));
                acc_dot = _mm256_add_epi32(acc_dot, _mm256_madd_epi16(a_hi, b_hi));
                acc_na = _mm256_add_epi32(acc_na, _mm256_madd_epi16(a_lo, a_lo));
                acc_na = _mm256_add_epi32(acc_na, _mm256_madd_epi16(a_hi, a_hi));
                acc_nb = _mm256_add_epi32(acc_nb, _mm256_madd_epi16(b_lo, b_lo));
                acc_nb = _mm256_add_epi32(acc_nb, _mm256_madd_epi16(b_hi, b_hi));
                i += 32;
            }

            let mut dot_ab = hsum_epi32_avx2(acc_dot);
            let mut norm_a_sq = hsum_epi32_avx2(acc_na);
            let mut norm_b_sq = hsum_epi32_avx2(acc_nb);

            // Scalar tail
            while i < n {
                let (xu, yu) = (a[i] as u32, b[i] as u32);
                dot_ab += xu * yu;
                norm_a_sq += xu * xu;
                norm_b_sq += yu * yu;
                i += 1;
            }

            CosineAccumulators {
                dot_ab,
                norm_a_sq,
                norm_b_sq,
            }
        }
    }

    lance_core::target_kernel! {
        #[target_features("avx512f", "avx512bw", "avx512vnni")]
        #[candidate(pub(super) COSINE_U8_ACCUM_AVX512_VNNI: CosineU8AccumKernel)]
        /// AVX-512 VNNI fused cosine: VPDPWSSD for each product, 64 elements/iter.
        pub(super) fn cosine_u8_accum_avx512_vnni(a: &[u8], b: &[u8]) -> CosineAccumulators {
            // See `cosine_u8_accum_avx2`.
            let n = a.len().min(b.len());
            let zeros = _mm512_setzero_si512();
            let mut acc_dot = _mm512_setzero_si512();
            let mut acc_na = _mm512_setzero_si512();
            let mut acc_nb = _mm512_setzero_si512();
            let mut i = 0usize;

            while i + 64 <= n {
                let (av, bv) = unsafe {
                    (
                        _mm512_loadu_si512(a.as_ptr().add(i) as *const __m512i),
                        _mm512_loadu_si512(b.as_ptr().add(i) as *const __m512i),
                    )
                };

                // Zero-extend u8→i16 via interleave with zeros.
                let a_lo = _mm512_unpacklo_epi8(av, zeros);
                let a_hi = _mm512_unpackhi_epi8(av, zeros);
                let b_lo = _mm512_unpacklo_epi8(bv, zeros);
                let b_hi = _mm512_unpackhi_epi8(bv, zeros);

                // VPDPWSSD: signed i16 multiply-add into i32 accumulator.
                acc_dot = _mm512_dpwssd_epi32(acc_dot, a_lo, b_lo);
                acc_dot = _mm512_dpwssd_epi32(acc_dot, a_hi, b_hi);
                acc_na = _mm512_dpwssd_epi32(acc_na, a_lo, a_lo);
                acc_na = _mm512_dpwssd_epi32(acc_na, a_hi, a_hi);
                acc_nb = _mm512_dpwssd_epi32(acc_nb, b_lo, b_lo);
                acc_nb = _mm512_dpwssd_epi32(acc_nb, b_hi, b_hi);
                i += 64;
            }

            let mut dot_ab = _mm512_reduce_add_epi32(acc_dot) as u32;
            let mut norm_a_sq = _mm512_reduce_add_epi32(acc_na) as u32;
            let mut norm_b_sq = _mm512_reduce_add_epi32(acc_nb) as u32;

            // Scalar tail
            while i < n {
                let (xu, yu) = (a[i] as u32, b[i] as u32);
                dot_ab += xu * yu;
                norm_a_sq += xu * xu;
                norm_b_sq += yu * yu;
                i += 1;
            }

            CosineAccumulators {
                dot_ab,
                norm_a_sq,
                norm_b_sq,
            }
        }
    }
}

/// Dispatched fused u8 cosine accumulation.
#[inline]
fn cosine_u8_accum(a: &[u8], b: &[u8]) -> CosineAccumulators {
    assert_equal_lengths(a.len(), b.len());
    COSINE_U8_ACCUM.get().call(a, b)
}

/// Dispatched u8 cosine distance, selecting the best available SIMD backend.
///
/// Returns `1 - dot(a,b) / (‖a‖ × ‖b‖)` computed in a single pass.
#[inline]
pub fn cosine_u8(a: &[u8], b: &[u8]) -> f32 {
    normalize(cosine_u8_accum(a, b))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::u8_inputs::{LENGTHS, Pattern, random};

    #[rstest::rstest]
    #[case::shorter_right(64, 1)]
    #[case::longer_right(1, 64)]
    fn rejects_mismatched_lengths(#[case] a_len: usize, #[case] b_len: usize) {
        let a = vec![1; a_len];
        let b = vec![1; b_len];

        assert!(std::panic::catch_unwind(|| cosine_u8_accum_scalar(&a, &b)).is_err());
        assert!(std::panic::catch_unwind(|| cosine_u8_scalar(&a, &b)).is_err());
        assert!(std::panic::catch_unwind(|| cosine_u8(&a, &b)).is_err());
    }

    /// Every host-supported kernel, and the dispatched entry point, must
    /// produce exactly the reference integer accumulators.
    fn assert_kernels_match_reference(a: &[u8], b: &[u8], label: &str) {
        let (mut dot_ab, mut norm_a_sq, mut norm_b_sq) = (0u64, 0u64, 0u64);
        for (&x, &y) in a.iter().zip(b) {
            let (x, y) = (x as u64, y as u64);
            dot_ab += x * y;
            norm_a_sq += x * x;
            norm_b_sq += y * y;
        }
        let want = CosineAccumulators {
            dot_ab: dot_ab.try_into().unwrap(),
            norm_a_sq: norm_a_sq.try_into().unwrap(),
            norm_b_sq: norm_b_sq.try_into().unwrap(),
        };
        for candidate in COSINE_U8_ACCUM.candidates() {
            let Some(kernel) = candidate.kernel() else {
                continue;
            };
            let got = kernel.call(a, b);
            assert_eq!(got, want, "{} [{label}] n={}", candidate.name(), a.len());
        }
        let got = cosine_u8_accum(a, b);
        assert_eq!(got, want, "cosine_u8_accum [{label}] n={}", a.len());
    }

    #[rstest::rstest]
    #[case::random(Pattern::Random)]
    #[case::extremes(Pattern::Extremes)]
    #[case::one_sided_zeros(Pattern::OneSidedZeros)]
    fn test_cosine_u8_kernels_match_reference(#[case] pattern: Pattern) {
        for &len in LENGTHS {
            for (label, a, b) in pattern.pairs(len) {
                assert_kernels_match_reference(&a, &b, label);
            }
        }
    }

    /// The SIMD kernels are safe functions, so unequal lengths must stay in
    /// bounds and use the common prefix. The lengths cross both SIMD strides.
    #[rstest::rstest]
    #[case::shorter_right(129, 65)]
    #[case::longer_right(65, 129)]
    fn simd_kernels_use_common_prefix_of_unequal_lengths(
        #[case] a_len: usize,
        #[case] b_len: usize,
    ) {
        let a = random(a_len, 0);
        let b = random(b_len, 1);
        let n = a_len.min(b_len);
        let want = cosine_u8_accum_scalar(&a[..n], &b[..n]);
        for candidate in COSINE_U8_ACCUM.candidates() {
            // The scalar kernel asserts equal lengths instead.
            if candidate.features().is_empty() {
                continue;
            }
            let Some(kernel) = candidate.kernel() else {
                continue;
            };
            assert_eq!(kernel.call(&a, &b), want, "{}", candidate.name());
        }
    }

    #[test]
    fn cosine_known_values() {
        // Identical vectors → distance 0
        let v = [10u8, 20, 30, 40];
        assert_eq!(cosine_u8(&v, &v), 0.0);

        // Orthogonal-ish: one vector all in first half, other in second half
        let a = [255u8, 255, 0, 0];
        let b = [0u8, 0, 255, 255];
        assert_eq!(cosine_u8(&a, &b), 1.0); // dot=0 → distance=1

        // Zero vectors → distance 0 (by convention)
        assert_eq!(cosine_u8(&[0, 0], &[0, 0]), 0.0);
        assert_eq!(cosine_u8(&[0, 0], &[1, 2]), 0.0);
    }

    #[test]
    fn cosine_distance_symmetry() {
        let a = random(256, 0);
        let b = random(256, 1);
        assert_eq!(cosine_u8(&a, &b), cosine_u8(&b, &a));
    }

    #[test]
    fn cosine_distance_range() {
        // Cosine distance should be in [0, 1] for non-negative u8 inputs
        // (all u8 values are ≥ 0, so dot product is always ≥ 0).
        for seed in 0..8 {
            let d = cosine_u8(&random(1024, 2 * seed), &random(1024, 2 * seed + 1));
            assert!(
                (0.0..=1.0).contains(&d),
                "cosine distance {d} out of [0,1] range"
            );
        }
    }
}
