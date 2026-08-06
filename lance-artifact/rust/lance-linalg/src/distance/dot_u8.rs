// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Unsigned int8 dot product with runtime-dispatched SIMD backends.
//!
//! Used by Scalar Quantization (SQ) distance computation. SQ stores each
//! vector dimension as a u8 after linearly mapping [min, max] → [0, 255].
//! Distance computation between SQ-encoded vectors reduces to a u8 × u8
//! dot product plus precomputed per-vector scalar terms.
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

use std::sync::OnceLock;

/// Portable scalar u8 dot product, also used for SIMD tail elements.
#[inline]
pub fn dot_u8_scalar(a: &[u8], b: &[u8]) -> u32 {
    debug_assert_eq!(a.len(), b.len());
    a.iter()
        .zip(b.iter())
        .map(|(&x, &y)| x as u32 * y as u32)
        .sum()
}

#[cfg(target_arch = "x86_64")]
mod x86 {
    use std::arch::x86_64::*;

    /// AVX2 path: zero-extend u8→u16, then VPMADDWD. 32 elements/iter.
    #[target_feature(enable = "avx2")]
    pub unsafe fn dot_u8_avx2(a: &[u8], b: &[u8]) -> u32 {
        debug_assert_eq!(a.len(), b.len());
        let n = a.len();
        let mut acc = _mm256_setzero_si256();
        let mut i = 0usize;

        while i + 32 <= n {
            let av = _mm256_loadu_si256(a.as_ptr().add(i) as *const __m256i);
            let bv = _mm256_loadu_si256(b.as_ptr().add(i) as *const __m256i);

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
            result += a[i] as u32 * b[i] as u32;
            i += 1;
        }
        result
    }

    /// AVX-512 VNNI path (Ice Lake+, Zen 4+). 64 elements/iter.
    ///
    /// VPDPBUSD expects (unsigned, signed) operands but SQ stores u8×u8.
    /// We XOR b with 0x80 to map it to i8, then correct: result + 128·Σa.
    /// The Σa term (VPSADBW, port 5) runs in parallel with VPDPBUSD (port 0).
    #[target_feature(enable = "avx512f,avx512bw,avx512vnni")]
    pub unsafe fn dot_u8_avx512_vnni(a: &[u8], b: &[u8]) -> u32 {
        debug_assert_eq!(a.len(), b.len());
        let n = a.len();

        let mut acc_dot = _mm512_setzero_si512();
        let mut acc_suma = _mm512_setzero_si512();
        let sign_flip = _mm512_set1_epi8(0x80u8 as i8);
        let zeros = _mm512_setzero_si512();
        let mut i = 0usize;

        while i + 64 <= n {
            let av = _mm512_loadu_si512(a.as_ptr().add(i) as *const __m512i);
            let bv = _mm512_loadu_si512(b.as_ptr().add(i) as *const __m512i);
            let b_biased = _mm512_xor_si512(bv, sign_flip);
            acc_dot = _mm512_dpbusd_epi32(acc_dot, av, b_biased);
            acc_suma = _mm512_add_epi64(acc_suma, _mm512_sad_epu8(av, zeros));
            i += 64;
        }

        let biased_dot = _mm512_reduce_add_epi32(acc_dot);
        let sum_a = _mm512_reduce_add_epi64(acc_suma);
        let mut result = (biased_dot as i64 + 128 * sum_a) as u32;

        while i < n {
            result += a[i] as u32 * b[i] as u32;
            i += 1;
        }
        result
    }
}

type DotU8Fn = fn(&[u8], &[u8]) -> u32;

static DISPATCH: OnceLock<DotU8Fn> = OnceLock::new();

fn select_backend() -> DotU8Fn {
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx512f")
            && is_x86_feature_detected!("avx512bw")
            && is_x86_feature_detected!("avx512vnni")
        {
            return |a, b| unsafe { x86::dot_u8_avx512_vnni(a, b) };
        }

        if is_x86_feature_detected!("avx2") {
            return |a, b| unsafe { x86::dot_u8_avx2(a, b) };
        }
        // AvxFma and Avx hosts (AMD Piledriver / Steamroller, Intel Sandy
        // Bridge / Ivy Bridge) fall through to scalar: the AVX2 inner uses
        // `vpmaddubsw` / `vpmaddwd` integer ops which neither AVX nor
        // AVX+FMA provides.
    }

    dot_u8_scalar
}

/// Dispatched u8 dot product, selecting the best available SIMD backend.
#[inline]
pub fn dot_u8(a: &[u8], b: &[u8]) -> u32 {
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
        let reference = dot_u8_scalar(a, b);

        #[cfg(target_arch = "x86_64")]
        {
            if is_x86_feature_detected!("avx2") {
                let got = unsafe { x86::dot_u8_avx2(a, b) };
                assert_eq!(got, reference, "avx2 [{case}] n={}", a.len());
            }

            if is_x86_feature_detected!("avx512f")
                && is_x86_feature_detected!("avx512bw")
                && is_x86_feature_detected!("avx512vnni")
            {
                let got = unsafe { x86::dot_u8_avx512_vnni(a, b) };
                assert_eq!(got, reference, "avx512_vnni [{case}] n={}", a.len());
            }
        }

        assert_eq!(dot_u8(a, b), reference, "dispatch [{case}] n={}", a.len());
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
            a[..n].fill(u8::MAX);
            b[..n].fill(u8::MAX);
            check_all_backends(&a[..n], &b[..n], "max*max");

            a[..n].fill(u8::MAX);
            b[..n].fill(0);
            check_all_backends(&a[..n], &b[..n], "max*0");

            a[..n].fill(0);
            b[..n].fill(u8::MAX);
            check_all_backends(&a[..n], &b[..n], "0*max");

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
    fn all_ones_pattern() {
        let mut a = vec![0u8; 4097];
        let mut b = vec![0u8; 4097];

        for &n in SIZES {
            a[..n].fill(1);
            b[..n].fill(1);
            check_all_backends(&a[..n], &b[..n], "1*1");
            assert_eq!(dot_u8_scalar(&a[..n], &b[..n]), n as u32);
        }
    }
}
