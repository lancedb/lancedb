// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Unsigned int8 dot product with runtime-dispatched SIMD backends.
//!
//! Used by Scalar Quantization (SQ) distance computation. SQ stores each
//! vector dimension as a u8 after linearly mapping [min, max] → [0, 255].
//! Distance computation between SQ-encoded vectors reduces to a u8 × u8
//! dot product plus precomputed per-vector scalar terms.
//! The u32 entry points return the low 32 bits, while [`dot_u8_u64`] chunks
//! those kernels to produce the full result used by SQ.
//!
//! Backends (selected at runtime, best available wins):
//!   1. scalar     — portable reference, also used for tails
//!   2. avx2       — VPMADDWD on u16-widened halves, 32 elements/iter
//!   3. avx512vnni — VPDPBUSD with XOR-0x80 bias trick, 64 elements/iter
//!
//! ## The VNNI bias trick
//!
//! VPDPBUSD expects one unsigned and one signed operand, but SQ vectors
//! are u8 × u8. We bias `b` into the signed domain via XOR 0x80 (equivalent
//! to subtracting 128 when reinterpreted as i8), feed `a` directly as
//! unsigned, and correct by adding 128·Σa at the end:
//!
//!   DPBUSD(a, b ⊕ 0x80) = Σ a·(b − 128) = Σ a·b − 128·Σa
//!
//! The Σa term uses VPSADBW, which dispatches to port 5 while VPDPBUSD
//! runs on port 0 on Intel. The two instructions execute in parallel,
//! making the correction effectively free.

use super::{U8_U32_ACCUMULATOR_MAX_LEN, assert_equal_lengths};

type DotU8Kernel = unsafe fn(&[u8], &[u8]) -> u32;

lance_core::dispatch! {
    /// There are no AVX or AVX+FMA candidates: the SIMD kernels need AVX2
    /// integer ops, so those hosts use the scalar kernel.
    static DOT_U8: Dispatch<DotU8Kernel> = {
        #[cfg(target_arch = "x86_64")]
        {
            &[x86::DOT_U8_AVX512_VNNI, x86::DOT_U8_AVX2, DOT_U8_SCALAR]
        }
        #[cfg(not(target_arch = "x86_64"))]
        {
            &[DOT_U8_SCALAR]
        }
    };
}

lance_core::target_kernel! {
    #[target_features()]
    #[candidate(DOT_U8_SCALAR: DotU8Kernel)]
    /// Portable scalar u8 dot product, also used for SIMD tail elements.
    ///
    /// The result is the low 32 bits of the exact dot product. Use
    /// [`dot_u8_u64`] when the full result is required.
    ///
    /// # Panics
    ///
    /// Panics if `a` and `b` have different lengths.
    #[inline]
    pub fn dot_u8_scalar(a: &[u8], b: &[u8]) -> u32 {
        assert_equal_lengths(a.len(), b.len());
        a.iter()
            .zip(b.iter())
            .fold(0, |sum, (&x, &y)| sum.wrapping_add(x as u32 * y as u32))
    }
}

#[cfg(target_arch = "x86_64")]
mod x86 {
    use std::arch::x86_64::*;

    use super::DotU8Kernel;

    lance_core::target_kernel! {
        #[target_features("avx2")]
        #[candidate(pub(super) DOT_U8_AVX2: DotU8Kernel)]
        /// AVX2 path: zero-extend u8→u16, then VPMADDWD. 32 elements/iter.
        pub(super) fn dot_u8_avx2(a: &[u8], b: &[u8]) -> u32 {
            // Callers pass equal lengths; `min` keeps the unchecked loads in
            // bounds for any input, as a safe function must.
            let n = a.len().min(b.len());
            let mut acc = _mm256_setzero_si256();
            let mut i = 0usize;

            while i + 32 <= n {
                let (av, bv) = unsafe {
                    (
                        _mm256_loadu_si256(a.as_ptr().add(i) as *const __m256i),
                        _mm256_loadu_si256(b.as_ptr().add(i) as *const __m256i),
                    )
                };

                // Zero-extend each 128-bit half to 16 × u16. Values ≤ 255 fit
                // in i16 as positive, so VPMADDWD gives correct results.
                let a_lo = _mm256_cvtepu8_epi16(_mm256_castsi256_si128(av));
                let a_hi = _mm256_cvtepu8_epi16(_mm256_extracti128_si256(av, 1));
                let b_lo = _mm256_cvtepu8_epi16(_mm256_castsi256_si128(bv));
                let b_hi = _mm256_cvtepu8_epi16(_mm256_extracti128_si256(bv, 1));

                acc = _mm256_add_epi32(acc, _mm256_madd_epi16(a_lo, b_lo));
                acc = _mm256_add_epi32(acc, _mm256_madd_epi16(a_hi, b_hi));
                i += 32;
            }

            let lo128 = _mm256_castsi256_si128(acc);
            let hi128 = _mm256_extracti128_si256(acc, 1);
            let mut sum128 = _mm_add_epi32(lo128, hi128);
            sum128 = _mm_hadd_epi32(sum128, sum128);
            sum128 = _mm_hadd_epi32(sum128, sum128);
            let mut result = _mm_cvtsi128_si32(sum128) as u32;

            while i < n {
                result = result.wrapping_add(a[i] as u32 * b[i] as u32);
                i += 1;
            }
            result
        }
    }

    lance_core::target_kernel! {
        #[target_features("avx512f", "avx512bw", "avx512vnni")]
        #[candidate(pub(super) DOT_U8_AVX512_VNNI: DotU8Kernel)]
        /// AVX-512 VNNI path (Ice Lake+, Zen 4+). 64 elements/iter.
        ///
        /// VPDPBUSD expects (unsigned, signed) operands but SQ stores u8×u8.
        /// We XOR b with 0x80 to map it to i8, then correct: result + 128·Σa.
        /// The Σa term (VPSADBW, port 5) runs in parallel with VPDPBUSD (port 0).
        pub(super) fn dot_u8_avx512_vnni(a: &[u8], b: &[u8]) -> u32 {
            // See `dot_u8_avx2`.
            let n = a.len().min(b.len());

            let mut acc_dot = _mm512_setzero_si512();
            let mut acc_suma = _mm512_setzero_si512();
            let sign_flip = _mm512_set1_epi8(0x80u8 as i8);
            let zeros = _mm512_setzero_si512();
            let mut i = 0usize;

            while i + 64 <= n {
                let (av, bv) = unsafe {
                    (
                        _mm512_loadu_si512(a.as_ptr().add(i) as *const __m512i),
                        _mm512_loadu_si512(b.as_ptr().add(i) as *const __m512i),
                    )
                };
                let b_biased = _mm512_xor_si512(bv, sign_flip);
                acc_dot = _mm512_dpbusd_epi32(acc_dot, av, b_biased);
                acc_suma = _mm512_add_epi32(acc_suma, _mm512_sad_epu8(av, zeros));
                i += 64;
            }

            // VPSADBW leaves the high 32 bits of each u64 lane zero, so Σa
            // can be summed in 32-bit lanes and folded into `acc_dot` for a
            // single reduction; the result is only kept mod 2^32 anyway.
            let acc = _mm512_add_epi32(acc_dot, _mm512_slli_epi32(acc_suma, 7));
            let mut result = _mm512_reduce_add_epi32(acc) as u32;

            while i < n {
                result = result.wrapping_add(a[i] as u32 * b[i] as u32);
                i += 1;
            }
            result
        }
    }
}

/// Dispatched u8 dot product, selecting the best available SIMD backend.
///
/// The result is the low 32 bits of the exact dot product. Use
/// [`dot_u8_u64`] when the full result is required.
///
/// # Panics
///
/// Panics if `a` and `b` have different lengths.
#[inline]
pub fn dot_u8(a: &[u8], b: &[u8]) -> u32 {
    assert_equal_lengths(a.len(), b.len());
    DOT_U8.get().call(a, b)
}

/// Calculates the exact u8 dot product with a u64 accumulator.
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
/// use lance_linalg::distance::dot_u8::dot_u8_u64;
///
/// assert_eq!(dot_u8_u64(&[2, 3], &[4, 5]), 23);
/// ```
#[inline]
pub fn dot_u8_u64(a: &[u8], b: &[u8]) -> u64 {
    assert_equal_lengths(a.len(), b.len());
    if a.len() <= U8_U32_ACCUMULATOR_MAX_LEN {
        return dot_u8(a, b) as u64;
    }
    a.chunks(U8_U32_ACCUMULATOR_MAX_LEN)
        .zip(b.chunks(U8_U32_ACCUMULATOR_MAX_LEN))
        .map(|(a, b)| dot_u8(a, b) as u64)
        .sum()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::u8_inputs::{LENGTHS, Pattern, random};

    #[test]
    fn rejects_mismatched_lengths() {
        assert!(std::panic::catch_unwind(|| dot_u8_scalar(&[1, 2], &[1])).is_err());
        assert!(std::panic::catch_unwind(|| dot_u8(&[1, 2], &[1])).is_err());
    }

    /// Every host-supported kernel, and the dispatched entry point, must equal
    /// the low 32 bits of the exact dot product.
    fn assert_kernels_match_reference(a: &[u8], b: &[u8], label: &str) {
        let exact: u64 = a.iter().zip(b).map(|(&x, &y)| x as u64 * y as u64).sum();
        let want = exact as u32;
        for candidate in DOT_U8.candidates() {
            let Some(kernel) = candidate.kernel() else {
                continue;
            };
            let got = kernel.call(a, b);
            assert_eq!(got, want, "{} [{label}] n={}", candidate.name(), a.len());
        }
        assert_eq!(dot_u8(a, b), want, "dot_u8 [{label}] n={}", a.len());
    }

    #[rstest::rstest]
    #[case::random(Pattern::Random)]
    #[case::extremes(Pattern::Extremes)]
    #[case::one_sided_zeros(Pattern::OneSidedZeros)]
    fn test_dot_u8_kernels_match_reference(#[case] pattern: Pattern) {
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
        let want = dot_u8_scalar(&a[..n], &b[..n]);
        for candidate in DOT_U8.candidates() {
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
        assert_eq!(dot_u8_scalar(&[2, 3], &[4, 5]), 23);
        assert_eq!(dot_u8(&[2, 3], &[4, 5]), 23);
        assert_eq!(dot_u8(&[1; 4097], &[1; 4097]), 4097);
    }

    #[test]
    fn overflow_is_backend_independent_and_wide_result_is_exact() {
        let len = U8_U32_ACCUMULATOR_MAX_LEN + 1;
        let a = vec![u8::MAX; len];
        let b = vec![u8::MAX; len];
        let exact = u8::MAX as u64 * u8::MAX as u64 * len as u64;

        assert_kernels_match_reference(&a, &b, "u32 overflow");
        assert_eq!(dot_u8_scalar(&a, &b), exact as u32);
        assert_eq!(dot_u8_u64(&a, &b), exact);
        assert_eq!(crate::distance::dot::dot::<u8>(&a, &b), exact as f32);
    }
}
