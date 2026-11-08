// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Unsigned int8 squared L2 distance with runtime-dispatched SIMD backends.
//!
//! Computes `Σ(a[i] - b[i])²` for u8 slices. The u32 entry points return the
//! low 32 bits, while [`l2_u8_u64`] chunks those kernels to produce the full
//! result used by Scalar Quantization (SQ), where both L2 and Cosine metric
//! types operate on quantized u8 codes.
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

use super::{U8_U32_ACCUMULATOR_MAX_LEN, assert_equal_lengths};

type L2U8Kernel = unsafe fn(&[u8], &[u8]) -> u32;

lance_core::dispatch! {
    /// There are no AVX or AVX+FMA candidates: the SIMD kernels need AVX2
    /// integer ops, so those hosts use the scalar kernel.
    static L2_U8: Dispatch<L2U8Kernel> = {
        #[cfg(target_arch = "x86_64")]
        {
            &[x86::L2_U8_AVX512_VNNI, x86::L2_U8_AVX2, L2_U8_SCALAR]
        }
        #[cfg(not(target_arch = "x86_64"))]
        {
            &[L2_U8_SCALAR]
        }
    };
}

lance_core::target_kernel! {
    #[target_features()]
    #[candidate(L2_U8_SCALAR: L2U8Kernel)]
    /// Portable scalar u8 squared L2 distance, also used for SIMD tail elements.
    ///
    /// The result is the low 32 bits of the exact squared distance. Use
    /// [`l2_u8_u64`] when the full result is required.
    ///
    /// # Panics
    ///
    /// Panics if `a` and `b` have different lengths.
    #[inline]
    pub fn l2_u8_scalar(a: &[u8], b: &[u8]) -> u32 {
        assert_equal_lengths(a.len(), b.len());
        a.iter().zip(b.iter()).fold(0, |sum, (&x, &y)| {
            sum.wrapping_add((x.abs_diff(y) as u32).pow(2))
        })
    }
}

#[cfg(target_arch = "x86_64")]
mod x86 {
    use std::arch::x86_64::*;

    use super::L2U8Kernel;

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
        #[candidate(pub(super) L2_U8_AVX2: L2U8Kernel)]
        /// AVX2 path: saturating-sub abs diff, unpack to i16, VPMADDWD to square.
        /// 32 elements/iter.
        pub(super) fn l2_u8_avx2(a: &[u8], b: &[u8]) -> u32 {
            // Callers pass equal lengths; `min` keeps the unchecked loads in
            // bounds for any input, as a safe function must.
            let n = a.len().min(b.len());
            let zeros = _mm256_setzero_si256();
            let mut acc = _mm256_setzero_si256();
            let mut i = 0usize;

            while i + 32 <= n {
                let (av, bv) = unsafe {
                    (
                        _mm256_loadu_si256(a.as_ptr().add(i) as *const __m256i),
                        _mm256_loadu_si256(b.as_ptr().add(i) as *const __m256i),
                    )
                };

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
                result = result.wrapping_add(d * d);
                i += 1;
            }
            result
        }
    }

    lance_core::target_kernel! {
        #[target_features("avx512f", "avx512bw", "avx512vnni")]
        #[candidate(pub(super) L2_U8_AVX512_VNNI: L2U8Kernel)]
        /// AVX-512 VNNI path: abs diff + VPDPWSSD for fused square-accumulate.
        /// 64 elements/iter.
        pub(super) fn l2_u8_avx512_vnni(a: &[u8], b: &[u8]) -> u32 {
            // See `l2_u8_avx2`.
            let n = a.len().min(b.len());
            let zeros = _mm512_setzero_si512();
            let mut acc = _mm512_setzero_si512();
            let mut i = 0usize;

            while i + 64 <= n {
                let (av, bv) = unsafe {
                    (
                        _mm512_loadu_si512(a.as_ptr().add(i) as *const __m512i),
                        _mm512_loadu_si512(b.as_ptr().add(i) as *const __m512i),
                    )
                };

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
                result = result.wrapping_add(d * d);
                i += 1;
            }
            result
        }
    }
}

/// Dispatched u8 squared L2 distance, selecting the best available SIMD backend.
///
/// The result is the low 32 bits of the exact squared distance. Use
/// [`l2_u8_u64`] when the full result is required.
///
/// # Panics
///
/// Panics if `a` and `b` have different lengths.
#[inline]
pub fn l2_u8(a: &[u8], b: &[u8]) -> u32 {
    assert_equal_lengths(a.len(), b.len());
    L2_U8.get().call(a, b)
}

/// Calculates the exact u8 squared L2 distance with a u64 accumulator.
///
/// This retains the runtime-selected SIMD kernel and widens its result between
/// chunks that are guaranteed to fit in a u32.
///
/// # Panics
///
/// Panics if `a` and `b` have different lengths.
///
/// # Example
///
/// ```
/// use lance_linalg::distance::l2_u8::l2_u8_u64;
///
/// assert_eq!(l2_u8_u64(&[10, 20], &[7, 21]), 10);
/// ```
#[inline]
pub fn l2_u8_u64(a: &[u8], b: &[u8]) -> u64 {
    assert_equal_lengths(a.len(), b.len());
    if a.len() <= U8_U32_ACCUMULATOR_MAX_LEN {
        return l2_u8(a, b) as u64;
    }
    a.chunks(U8_U32_ACCUMULATOR_MAX_LEN)
        .zip(b.chunks(U8_U32_ACCUMULATOR_MAX_LEN))
        .map(|(a, b)| l2_u8(a, b) as u64)
        .sum()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::u8_inputs::{LENGTHS, Pattern, random};

    #[test]
    fn rejects_mismatched_lengths() {
        assert!(std::panic::catch_unwind(|| l2_u8_scalar(&[1, 2], &[1])).is_err());
        assert!(std::panic::catch_unwind(|| l2_u8(&[1, 2], &[1])).is_err());
    }

    /// Every host-supported kernel, and the dispatched entry point, must equal
    /// the low 32 bits of the exact distance.
    fn assert_kernels_match_reference(a: &[u8], b: &[u8], label: &str) {
        let exact: u64 = a
            .iter()
            .zip(b)
            .map(|(&x, &y)| (x.abs_diff(y) as u64).pow(2))
            .sum();
        let want = exact as u32;
        for candidate in L2_U8.candidates() {
            let Some(kernel) = candidate.kernel() else {
                continue;
            };
            let got = kernel.call(a, b);
            assert_eq!(got, want, "{} [{label}] n={}", candidate.name(), a.len());
        }
        assert_eq!(l2_u8(a, b), want, "l2_u8 [{label}] n={}", a.len());
    }

    #[rstest::rstest]
    #[case::random(Pattern::Random)]
    #[case::extremes(Pattern::Extremes)]
    #[case::one_sided_zeros(Pattern::OneSidedZeros)]
    fn test_l2_u8_kernels_match_reference(#[case] pattern: Pattern) {
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
        let want = l2_u8_scalar(&a[..n], &b[..n]);
        for candidate in L2_U8.candidates() {
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
    fn known_values() {
        // 3² + 1² = 10
        assert_eq!(l2_u8_scalar(&[10, 20], &[7, 21]), 10);
        assert_eq!(l2_u8(&[10, 20], &[7, 21]), 10);

        // max single-element distance: 255² = 65025
        assert_eq!(l2_u8(&[0], &[255]), 65025);
        assert_eq!(l2_u8(&[255], &[0]), 65025);
    }

    #[test]
    fn overflow_is_backend_independent_and_wide_result_is_exact() {
        let len = U8_U32_ACCUMULATOR_MAX_LEN + 1;
        let a = vec![u8::MAX; len];
        let b = vec![0; len];
        let exact = u8::MAX as u64 * u8::MAX as u64 * len as u64;

        assert_kernels_match_reference(&a, &b, "u32 overflow");
        assert_eq!(l2_u8_scalar(&a, &b), exact as u32);
        assert_eq!(l2_u8_u64(&a, &b), exact);
        assert_eq!(
            crate::distance::l2::l2_distance_uint_scalar(&a, &b),
            exact as f32
        );
        assert_eq!(crate::distance::l2::l2::<u8>(&a, &b), exact as f32);
    }
}
