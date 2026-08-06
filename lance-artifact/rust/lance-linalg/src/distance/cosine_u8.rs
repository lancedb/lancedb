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

use std::sync::OnceLock;

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

/// Portable scalar fused cosine accumulation.
#[inline]
pub fn cosine_u8_accum_scalar(a: &[u8], b: &[u8]) -> CosineAccumulators {
    debug_assert_eq!(a.len(), b.len());
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
    use super::CosineAccumulators;
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

    /// AVX2 fused cosine: three VPMADDWD products per half, 32 elements/iter.
    #[target_feature(enable = "avx2")]
    pub unsafe fn cosine_u8_accum_avx2(a: &[u8], b: &[u8]) -> CosineAccumulators {
        debug_assert_eq!(a.len(), b.len());
        let n = a.len();
        let mut acc_dot = _mm256_setzero_si256();
        let mut acc_na = _mm256_setzero_si256();
        let mut acc_nb = _mm256_setzero_si256();
        let mut i = 0usize;

        while i + 32 <= n {
            let av = _mm256_loadu_si256(a.as_ptr().add(i) as *const __m256i);
            let bv = _mm256_loadu_si256(b.as_ptr().add(i) as *const __m256i);

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

    /// AVX-512 VNNI fused cosine: VPDPWSSD for each product, 64 elements/iter.
    #[target_feature(enable = "avx512f,avx512bw,avx512vnni")]
    pub unsafe fn cosine_u8_accum_avx512_vnni(a: &[u8], b: &[u8]) -> CosineAccumulators {
        debug_assert_eq!(a.len(), b.len());
        let n = a.len();
        let zeros = _mm512_setzero_si512();
        let mut acc_dot = _mm512_setzero_si512();
        let mut acc_na = _mm512_setzero_si512();
        let mut acc_nb = _mm512_setzero_si512();
        let mut i = 0usize;

        while i + 64 <= n {
            let av = _mm512_loadu_si512(a.as_ptr().add(i) as *const __m512i);
            let bv = _mm512_loadu_si512(b.as_ptr().add(i) as *const __m512i);

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

type CosineU8AccumFn = fn(&[u8], &[u8]) -> CosineAccumulators;

static DISPATCH: OnceLock<CosineU8AccumFn> = OnceLock::new();

fn select_backend() -> CosineU8AccumFn {
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx512f")
            && is_x86_feature_detected!("avx512bw")
            && is_x86_feature_detected!("avx512vnni")
        {
            return |a, b| unsafe { x86::cosine_u8_accum_avx512_vnni(a, b) };
        }

        if is_x86_feature_detected!("avx2") {
            return |a, b| unsafe { x86::cosine_u8_accum_avx2(a, b) };
        }
        // AvxFma and Avx hosts (AMD Piledriver / Steamroller, Intel Sandy
        // Bridge / Ivy Bridge) fall through to scalar: the AVX2 inner uses
        // `vpmaddubsw` / `vpmaddwd` integer ops which neither AVX nor
        // AVX+FMA provides.
    }

    cosine_u8_accum_scalar
}

/// Dispatched fused u8 cosine accumulation.
#[inline]
fn cosine_u8_accum(a: &[u8], b: &[u8]) -> CosineAccumulators {
    (DISPATCH.get_or_init(select_backend))(a, b)
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

    fn fill_random(buf: &mut [u8], seed: &mut u32) {
        for slot in buf.iter_mut() {
            *seed = seed.wrapping_mul(1103515245).wrapping_add(12345);
            *slot = (*seed >> 16) as u8;
        }
    }

    const SIZES: &[usize] = &[
        0, 1, 7, 15, 16, 31, 32, 33, 63, 64, 65, 127, 128, 255, 256, 1024, 4096, 4097,
    ];

    /// Verify SIMD backends produce identical integer accumulators to scalar.
    fn check_all_backends_accum(a: &[u8], b: &[u8], case: &str) {
        let reference = cosine_u8_accum_scalar(a, b);

        #[cfg(target_arch = "x86_64")]
        {
            if is_x86_feature_detected!("avx2") {
                let got = unsafe { x86::cosine_u8_accum_avx2(a, b) };
                assert_eq!(got, reference, "avx2 [{case}] n={}", a.len());
            }

            if is_x86_feature_detected!("avx512f")
                && is_x86_feature_detected!("avx512bw")
                && is_x86_feature_detected!("avx512vnni")
            {
                let got = unsafe { x86::cosine_u8_accum_avx512_vnni(a, b) };
                assert_eq!(got, reference, "avx512_vnni [{case}] n={}", a.len());
            }
        }

        let dispatched = cosine_u8_accum(a, b);
        assert_eq!(dispatched, reference, "dispatch [{case}] n={}", a.len());
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
                check_all_backends_accum(&a[..n], &b[..n], "random");
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
            check_all_backends_accum(&a[..n], &b[..n], "max-max");

            a[..n].fill(u8::MAX);
            b[..n].fill(0);
            check_all_backends_accum(&a[..n], &b[..n], "max-0");

            a[..n].fill(0);
            b[..n].fill(u8::MAX);
            check_all_backends_accum(&a[..n], &b[..n], "0-max");

            a[..n].fill(0);
            b[..n].fill(0);
            check_all_backends_accum(&a[..n], &b[..n], "0-0");

            for i in 0..n {
                a[i] = if i & 1 == 0 { 0 } else { u8::MAX };
                b[i] = if i & 1 == 0 { u8::MAX } else { 0 };
            }
            check_all_backends_accum(&a[..n], &b[..n], "alt 0/max");
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
            check_all_backends_accum(&a[..n], &b[..n], "b=0");

            a[..n].fill(0);
            fill_random(&mut b[..n], &mut seed);
            check_all_backends_accum(&a[..n], &b[..n], "a=0");
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
        let mut a = vec![0u8; 256];
        let mut b = vec![0u8; 256];
        let mut seed = 0xBEEF_u32;
        fill_random(&mut a, &mut seed);
        fill_random(&mut b, &mut seed);

        let d1 = cosine_u8(&a, &b);
        let d2 = cosine_u8(&b, &a);
        assert_eq!(d1, d2);
    }

    #[test]
    fn cosine_distance_range() {
        // Cosine distance should be in [0, 1] for non-negative u8 inputs
        // (all u8 values are ≥ 0, so dot product is always ≥ 0).
        let mut a = vec![0u8; 1024];
        let mut b = vec![0u8; 1024];

        for seed_idx in 0..8u32 {
            let mut seed = 0xABCD_u32.wrapping_add(seed_idx.wrapping_mul(31));
            fill_random(&mut a, &mut seed);
            fill_random(&mut b, &mut seed);
            let d = cosine_u8(&a, &b);
            assert!(
                (0.0..=1.0).contains(&d),
                "cosine distance {d} out of [0,1] range"
            );
        }
    }
}
