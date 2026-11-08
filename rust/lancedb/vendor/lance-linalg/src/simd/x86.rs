// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Reduction helpers shared by the x86_64 AVX kernels in [`crate::distance`].
//!
//! Each distance kernel accumulates into a 256-bit register and folds it down
//! to a scalar once, after the main loop. The fold is identical across
//! `cosine`, `dot`, `l2` and `norm_l2`, so it lives here instead of being
//! copied into each kernel's private `mod x86`.
//!
//! The module itself is `pub(crate)`, which is what keeps these helpers off the
//! public API; the items are `pub` rather than `pub(crate)` only because
//! `clippy::redundant_pub_crate` fires on the narrower visibility.

use std::arch::x86_64::*;

/// Horizontal sum of the eight `f32` lanes of an `__m256`.
///
/// Folds the upper 128-bit lane into the lower one, then reduces the
/// remaining four lanes pairwise. Uses `movehl`/`shuffle` plus scalar adds
/// rather than two `vhaddps`, which is one fewer uop on most cores.
///
/// # Safety
///
/// The host must support AVX. Callers are `#[target_feature]`-annotated
/// kernels that the runtime dispatcher only selects after checking.
#[inline]
#[target_feature(enable = "avx")]
pub unsafe fn hsum256_ps(v: __m256) -> f32 {
    let lo = _mm256_castps256_ps128(v);
    let hi = _mm256_extractf128_ps(v, 1);
    let sum128 = _mm_add_ps(lo, hi);
    let sum64 = _mm_add_ps(sum128, _mm_movehl_ps(sum128, sum128));
    // 0x55 broadcasts lane 1 into lane 0, so the scalar add below lands the
    // last of the four partial sums.
    let sum32 = _mm_add_ss(sum64, _mm_shuffle_ps(sum64, sum64, 0x55));
    _mm_cvtss_f32(sum32)
}

/// Horizontal sum of the four `f64` lanes of an `__m256d`.
///
/// Folds the upper 128-bit lane into the lower one, then adds the remaining
/// pair.
///
/// # Safety
///
/// The host must support AVX. Callers are `#[target_feature]`-annotated
/// kernels that the runtime dispatcher only selects after checking.
#[inline]
#[target_feature(enable = "avx")]
pub unsafe fn hsum256_pd(v: __m256d) -> f64 {
    let lo = _mm256_castpd256_pd128(v);
    let hi = _mm256_extractf128_pd(v, 1);
    let sum128 = _mm_add_pd(lo, hi);
    let sum64 = _mm_add_pd(sum128, _mm_unpackhi_pd(sum128, sum128));
    _mm_cvtsd_f64(sum64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[rstest]
    #[case::ascending([1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0], 36.0)]
    #[case::negative_lanes([-1.5, 2.0, -3.0, 4.5, 0.0, -0.5, 1.0, 2.5], 5.0)]
    #[case::zeros([0.0; 8], 0.0)]
    #[case::cancelling([1.0, -1.0, 2.0, -2.0, 3.0, -3.0, 4.0, -4.0], 0.0)]
    fn hsum256_ps_sums_every_lane(#[case] lanes: [f32; 8], #[case] expected: f32) {
        if !std::is_x86_feature_detected!("avx") {
            return;
        }
        let sum = unsafe { hsum256_ps(_mm256_loadu_ps(lanes.as_ptr())) };
        assert_eq!(sum, expected);
    }

    #[rstest]
    #[case::ascending([1.0, 2.0, 3.0, 4.0], 10.0)]
    #[case::negative_lanes([-1.5, 2.0, -3.0, 4.5], 2.0)]
    #[case::zeros([0.0; 4], 0.0)]
    #[case::cancelling([1.0, -1.0, 2.0, -2.0], 0.0)]
    fn hsum256_pd_sums_every_lane(#[case] lanes: [f64; 4], #[case] expected: f64) {
        if !std::is_x86_feature_detected!("avx") {
            return;
        }
        let sum = unsafe { hsum256_pd(_mm256_loadu_pd(lanes.as_ptr())) };
        assert_eq!(sum, expected);
    }
}
