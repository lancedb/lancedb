// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! SIMD kernels for the RaBitQ top-k lower-bound pruning scan.
//!
//! Multi-bit IVF_RQ search gates the exact ex-code rerank with a per-row
//! distance lower bound: after the binary FastScan fills the per-row binary
//! inner products, every row of the partition is classified against the query
//! upper bound and the current top-k heap threshold, and only the survivors
//! (typically well under 1%) are reranked. The classification is the per-row
//! formula of `RabitDistCalculator::raw_query_lower_bound`:
//!
//! ```text
//! lower_bound = (binary_ip - 0.5 * sum_q) * scale_factor
//!             + add_factor + query_factor
//!             - error_factor * query_error
//! ```
//!
//! These kernels evaluate the formula and both comparisons for
//! [`PRUNE_LANES`] rows at a time, returning bit masks instead of values so
//! the caller can skip whole groups (the overwhelmingly common case) and run
//! the existing scalar rerank only for the surviving lanes.
//!
//! Correctness contract:
//!
//! - The lower bound is computed with exactly the operation order of the
//!   scalar helper — multiplies and adds, never FMA. A fused multiply-add
//!   rounds differently, which could prune a row the scalar code would have
//!   kept; with bit-identical lower bounds the masks reproduce the scalar
//!   `>=` decisions exactly, keeping heap contents and prune-stats counters
//!   unchanged.
//! - Comparisons use ordered-quiet GE predicates (`_CMP_GE_OQ`), matching
//!   scalar `>=`: a NaN lower bound is never pruned and falls through to the
//!   exact rerank.
//! - The heap threshold may be a stale snapshot (it only ever tightens); the
//!   caller re-checks surviving lanes against live values, so a stale
//!   threshold can only over-select survivors, never wrongly prune.

use std::sync::LazyLock;

/// Rows classified per kernel invocation.
pub const PRUNE_LANES: usize = 16;

/// Per-query constants of the lower-bound formula, mirroring
/// `RabitDistCalculator::raw_query_lower_bound` term by term.
#[derive(Debug, Clone, Copy)]
pub struct LowerBoundTerms {
    /// `0.5 * sum_q`, subtracted from the binary inner product.
    pub half_sum_q: f32,
    pub query_factor: f32,
    pub query_error: f32,
}

/// Classify [`PRUNE_LANES`] rows against the pruning bounds.
///
/// Arguments are the per-row binary inner products, scale factors, add
/// factors, and error factors, followed by the formula constants, the query
/// upper bound, and the heap threshold (`None` while the heap is not full,
/// which disables the heap mask).
///
/// Returns `(pruned_upper_bound, pruned_heap)` masks: bit `i` of
/// `pruned_upper_bound` is set when `lower_bound[i] >= upper_bound`, and bit
/// `i` of `pruned_heap` is set when the row is not already pruned by the
/// upper bound and `lower_bound[i] >= heap_threshold`. Surviving rows are the
/// zero bits of the OR of both masks.
pub type PruneMaskFn = fn(
    &[f32; PRUNE_LANES],
    &[f32; PRUNE_LANES],
    &[f32; PRUNE_LANES],
    &[f32; PRUNE_LANES],
    LowerBoundTerms,
    f32,
    Option<f32>,
) -> (u16, u16);

/// Resolve the prune-mask kernel for the running CPU once; the result can be
/// cached by the caller for per-partition use.
pub fn prune_mask_kernel() -> PruneMaskFn {
    static KERNEL: LazyLock<PruneMaskFn> = LazyLock::new(select_prune_mask_kernel);
    *KERNEL
}

fn select_prune_mask_kernel() -> PruneMaskFn {
    #[cfg(target_arch = "x86_64")]
    {
        if std::arch::is_x86_feature_detected!("avx512f") {
            return x86::prune_masks_avx512_dispatch;
        }
        if std::arch::is_x86_feature_detected!("avx2") {
            return x86::prune_masks_avx2_dispatch;
        }
    }
    // On aarch64 the plain 16-wide loop auto-vectorizes to NEON (part of the
    // baseline), so no dedicated kernel is needed.
    prune_masks_portable
}

/// Portable implementation; also the reference for the SIMD kernels.
fn prune_masks_portable(
    dists: &[f32; PRUNE_LANES],
    scale_factors: &[f32; PRUNE_LANES],
    add_factors: &[f32; PRUNE_LANES],
    error_factors: &[f32; PRUNE_LANES],
    terms: LowerBoundTerms,
    upper_bound: f32,
    heap_threshold: Option<f32>,
) -> (u16, u16) {
    let mut lower_bounds = [0.0f32; PRUNE_LANES];
    for lane in 0..PRUNE_LANES {
        lower_bounds[lane] = ((dists[lane] - terms.half_sum_q) * scale_factors[lane]
            + add_factors[lane]
            + terms.query_factor)
            - error_factors[lane] * terms.query_error;
    }
    let mut pruned_upper_bound = 0u16;
    for (lane, lower_bound) in lower_bounds.iter().enumerate() {
        pruned_upper_bound |= u16::from(*lower_bound >= upper_bound) << lane;
    }
    let mut pruned_heap = 0u16;
    if let Some(threshold) = heap_threshold {
        for (lane, lower_bound) in lower_bounds.iter().enumerate() {
            pruned_heap |= u16::from(*lower_bound >= threshold) << lane;
        }
        pruned_heap &= !pruned_upper_bound;
    }
    (pruned_upper_bound, pruned_heap)
}

#[cfg(target_arch = "x86_64")]
mod x86 {
    use super::{LowerBoundTerms, PRUNE_LANES};
    use std::arch::x86_64::*;

    /// Lower bounds for 8 lanes with the scalar operation order (no FMA).
    #[inline]
    #[target_feature(enable = "avx")]
    fn lower_bounds_avx(
        dists: __m256,
        scale_factors: __m256,
        add_factors: __m256,
        error_factors: __m256,
        half_sum_q: __m256,
        query_factor: __m256,
        query_error: __m256,
    ) -> __m256 {
        let binary_distance = _mm256_add_ps(
            _mm256_add_ps(
                _mm256_mul_ps(_mm256_sub_ps(dists, half_sum_q), scale_factors),
                add_factors,
            ),
            query_factor,
        );
        _mm256_sub_ps(binary_distance, _mm256_mul_ps(error_factors, query_error))
    }

    #[inline]
    #[target_feature(enable = "avx")]
    fn ge_mask_avx(lower_bounds_lo: __m256, lower_bounds_hi: __m256, bound: f32) -> u16 {
        let bound = _mm256_set1_ps(bound);
        let lo = _mm256_movemask_ps(_mm256_cmp_ps::<_CMP_GE_OQ>(lower_bounds_lo, bound));
        let hi = _mm256_movemask_ps(_mm256_cmp_ps::<_CMP_GE_OQ>(lower_bounds_hi, bound));
        (lo | (hi << 8)) as u16
    }

    #[target_feature(enable = "avx2")]
    unsafe fn prune_masks_avx2(
        dists: &[f32; PRUNE_LANES],
        scale_factors: &[f32; PRUNE_LANES],
        add_factors: &[f32; PRUNE_LANES],
        error_factors: &[f32; PRUNE_LANES],
        terms: LowerBoundTerms,
        upper_bound: f32,
        heap_threshold: Option<f32>,
    ) -> (u16, u16) {
        let half_sum_q = _mm256_set1_ps(terms.half_sum_q);
        let query_factor = _mm256_set1_ps(terms.query_factor);
        let query_error = _mm256_set1_ps(terms.query_error);
        // SAFETY: the array references guarantee 16 readable floats each.
        let lower_bounds_lo = unsafe {
            lower_bounds_avx(
                _mm256_loadu_ps(dists.as_ptr()),
                _mm256_loadu_ps(scale_factors.as_ptr()),
                _mm256_loadu_ps(add_factors.as_ptr()),
                _mm256_loadu_ps(error_factors.as_ptr()),
                half_sum_q,
                query_factor,
                query_error,
            )
        };
        let lower_bounds_hi = unsafe {
            lower_bounds_avx(
                _mm256_loadu_ps(dists.as_ptr().add(8)),
                _mm256_loadu_ps(scale_factors.as_ptr().add(8)),
                _mm256_loadu_ps(add_factors.as_ptr().add(8)),
                _mm256_loadu_ps(error_factors.as_ptr().add(8)),
                half_sum_q,
                query_factor,
                query_error,
            )
        };
        let pruned_upper_bound = ge_mask_avx(lower_bounds_lo, lower_bounds_hi, upper_bound);
        let pruned_heap = match heap_threshold {
            Some(threshold) => {
                ge_mask_avx(lower_bounds_lo, lower_bounds_hi, threshold) & !pruned_upper_bound
            }
            None => 0,
        };
        (pruned_upper_bound, pruned_heap)
    }

    pub(super) fn prune_masks_avx2_dispatch(
        dists: &[f32; PRUNE_LANES],
        scale_factors: &[f32; PRUNE_LANES],
        add_factors: &[f32; PRUNE_LANES],
        error_factors: &[f32; PRUNE_LANES],
        terms: LowerBoundTerms,
        upper_bound: f32,
        heap_threshold: Option<f32>,
    ) -> (u16, u16) {
        // SAFETY: only selected when AVX2 was detected.
        unsafe {
            prune_masks_avx2(
                dists,
                scale_factors,
                add_factors,
                error_factors,
                terms,
                upper_bound,
                heap_threshold,
            )
        }
    }

    #[target_feature(enable = "avx512f")]
    unsafe fn prune_masks_avx512(
        dists: &[f32; PRUNE_LANES],
        scale_factors: &[f32; PRUNE_LANES],
        add_factors: &[f32; PRUNE_LANES],
        error_factors: &[f32; PRUNE_LANES],
        terms: LowerBoundTerms,
        upper_bound: f32,
        heap_threshold: Option<f32>,
    ) -> (u16, u16) {
        // SAFETY: the array references guarantee 16 readable floats each.
        let (dists, scale_factors, add_factors, error_factors) = unsafe {
            (
                _mm512_loadu_ps(dists.as_ptr()),
                _mm512_loadu_ps(scale_factors.as_ptr()),
                _mm512_loadu_ps(add_factors.as_ptr()),
                _mm512_loadu_ps(error_factors.as_ptr()),
            )
        };
        let binary_distance = _mm512_add_ps(
            _mm512_add_ps(
                _mm512_mul_ps(
                    _mm512_sub_ps(dists, _mm512_set1_ps(terms.half_sum_q)),
                    scale_factors,
                ),
                add_factors,
            ),
            _mm512_set1_ps(terms.query_factor),
        );
        let lower_bounds = _mm512_sub_ps(
            binary_distance,
            _mm512_mul_ps(error_factors, _mm512_set1_ps(terms.query_error)),
        );
        let pruned_upper_bound =
            _mm512_cmp_ps_mask::<_CMP_GE_OQ>(lower_bounds, _mm512_set1_ps(upper_bound));
        let pruned_heap = match heap_threshold {
            Some(threshold) => {
                _mm512_cmp_ps_mask::<_CMP_GE_OQ>(lower_bounds, _mm512_set1_ps(threshold))
                    & !pruned_upper_bound
            }
            None => 0,
        };
        (pruned_upper_bound, pruned_heap)
    }

    pub(super) fn prune_masks_avx512_dispatch(
        dists: &[f32; PRUNE_LANES],
        scale_factors: &[f32; PRUNE_LANES],
        add_factors: &[f32; PRUNE_LANES],
        error_factors: &[f32; PRUNE_LANES],
        terms: LowerBoundTerms,
        upper_bound: f32,
        heap_threshold: Option<f32>,
    ) -> (u16, u16) {
        // SAFETY: only selected when AVX-512F was detected.
        unsafe {
            prune_masks_avx512(
                dists,
                scale_factors,
                add_factors,
                error_factors,
                terms,
                upper_bound,
                heap_threshold,
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::rngs::SmallRng;
    use rand::{Rng, SeedableRng};

    fn available_kernels() -> Vec<(&'static str, PruneMaskFn)> {
        // `mut` is only exercised on x86_64 where extra kernels may be pushed.
        #[allow(unused_mut)]
        let mut kernels = vec![
            ("portable", prune_masks_portable as PruneMaskFn),
            ("dispatched", prune_mask_kernel()),
        ];
        #[cfg(target_arch = "x86_64")]
        {
            if std::arch::is_x86_feature_detected!("avx2") {
                kernels.push(("avx2", x86::prune_masks_avx2_dispatch));
            }
            if std::arch::is_x86_feature_detected!("avx512f") {
                kernels.push(("avx512", x86::prune_masks_avx512_dispatch));
            }
        }
        kernels
    }

    /// Per-lane reference mirroring `raw_query_lower_bound` and the scalar
    /// pruning checks of the top-k scan.
    fn reference_masks(
        dists: &[f32; PRUNE_LANES],
        scale_factors: &[f32; PRUNE_LANES],
        add_factors: &[f32; PRUNE_LANES],
        error_factors: &[f32; PRUNE_LANES],
        terms: LowerBoundTerms,
        upper_bound: f32,
        heap_threshold: Option<f32>,
    ) -> (u16, u16) {
        let mut pruned_upper_bound = 0u16;
        let mut pruned_heap = 0u16;
        for lane in 0..PRUNE_LANES {
            let lower_bound = (dists[lane] - terms.half_sum_q) * scale_factors[lane]
                + add_factors[lane]
                + terms.query_factor
                - error_factors[lane] * terms.query_error;
            if lower_bound >= upper_bound {
                pruned_upper_bound |= 1 << lane;
            } else if heap_threshold.is_some_and(|threshold| lower_bound >= threshold) {
                pruned_heap |= 1 << lane;
            }
        }
        (pruned_upper_bound, pruned_heap)
    }

    #[allow(clippy::too_many_arguments)]
    fn assert_kernels_match_reference(
        dists: &[f32; PRUNE_LANES],
        scale_factors: &[f32; PRUNE_LANES],
        add_factors: &[f32; PRUNE_LANES],
        error_factors: &[f32; PRUNE_LANES],
        terms: LowerBoundTerms,
        upper_bound: f32,
        heap_threshold: Option<f32>,
        case: &str,
    ) {
        let expected = reference_masks(
            dists,
            scale_factors,
            add_factors,
            error_factors,
            terms,
            upper_bound,
            heap_threshold,
        );
        for (name, kernel) in available_kernels() {
            let actual = kernel(
                dists,
                scale_factors,
                add_factors,
                error_factors,
                terms,
                upper_bound,
                heap_threshold,
            );
            assert_eq!(
                actual, expected,
                "kernel={name} case={case}: masks {actual:04x?} != {expected:04x?}"
            );
        }
    }

    #[test]
    fn test_prune_masks_match_reference_on_random_inputs() {
        let mut rng = SmallRng::seed_from_u64(42);
        for round in 0..200 {
            let mut dists = [0.0f32; PRUNE_LANES];
            let mut scale_factors = [0.0f32; PRUNE_LANES];
            let mut add_factors = [0.0f32; PRUNE_LANES];
            let mut error_factors = [0.0f32; PRUNE_LANES];
            for lane in 0..PRUNE_LANES {
                dists[lane] = rng.random_range(-100.0f32..100.0);
                scale_factors[lane] = rng.random_range(-2.0f32..2.0);
                add_factors[lane] = rng.random_range(-10.0f32..10.0);
                error_factors[lane] = rng.random_range(0.0f32..5.0);
            }
            let terms = LowerBoundTerms {
                half_sum_q: rng.random_range(-50.0f32..50.0),
                query_factor: rng.random_range(-10.0f32..10.0),
                query_error: rng.random_range(0.0f32..2.0),
            };
            let upper_bound = rng.random_range(-50.0f32..50.0);
            let heap_threshold = if round % 3 == 0 {
                None
            } else {
                Some(rng.random_range(-50.0f32..50.0))
            };
            assert_kernels_match_reference(
                &dists,
                &scale_factors,
                &add_factors,
                &error_factors,
                terms,
                upper_bound,
                heap_threshold,
                &format!("random round {round}"),
            );
        }
    }

    #[test]
    fn test_prune_masks_exact_boundaries() {
        // With scale=1, err=0, half_sum_q=0, query_factor=0 the lower bound
        // is the input itself, so bounds can be placed exactly on lanes.
        let dists: [f32; PRUNE_LANES] = std::array::from_fn(|lane| lane as f32);
        let scale_factors = [1.0f32; PRUNE_LANES];
        let add_factors = [0.0f32; PRUNE_LANES];
        let error_factors = [0.0f32; PRUNE_LANES];
        let terms = LowerBoundTerms {
            half_sum_q: 0.0,
            query_factor: 0.0,
            query_error: 1.0,
        };
        // Equality must prune (scalar uses `>=`): lanes 3.. hit the upper
        // bound, lanes 1..3 hit only the heap threshold.
        let (pruned_upper_bound, pruned_heap) = prune_masks_portable(
            &dists,
            &scale_factors,
            &add_factors,
            &error_factors,
            terms,
            3.0,
            Some(1.0),
        );
        assert_eq!(pruned_upper_bound, 0xfff8);
        assert_eq!(pruned_heap, 0x0006);
        assert_kernels_match_reference(
            &dists,
            &scale_factors,
            &add_factors,
            &error_factors,
            terms,
            3.0,
            Some(1.0),
            "exact boundaries",
        );
        // No heap threshold: only the upper-bound mask is set.
        assert_kernels_match_reference(
            &dists,
            &scale_factors,
            &add_factors,
            &error_factors,
            terms,
            3.0,
            None,
            "no heap threshold",
        );
    }

    #[test]
    fn test_prune_masks_nan_and_infinity_semantics() {
        let mut dists = [0.0f32; PRUNE_LANES];
        dists[0] = f32::NAN;
        dists[1] = f32::INFINITY;
        dists[2] = f32::NEG_INFINITY;
        dists[3] = 1.0;
        let mut scale_factors = [1.0f32; PRUNE_LANES];
        scale_factors[4] = f32::NAN;
        let add_factors = [0.0f32; PRUNE_LANES];
        let mut error_factors = [0.0f32; PRUNE_LANES];
        error_factors[5] = f32::INFINITY;
        let terms = LowerBoundTerms {
            half_sum_q: 0.0,
            query_factor: 0.0,
            query_error: 1.0,
        };
        for (upper_bound, heap_threshold) in [
            (0.5, Some(0.0)),
            (f32::INFINITY, Some(f32::NEG_INFINITY)),
            (f32::NAN, Some(f32::NAN)),
            (0.5, None),
        ] {
            assert_kernels_match_reference(
                &dists,
                &scale_factors,
                &add_factors,
                &error_factors,
                terms,
                upper_bound,
                heap_threshold,
                &format!("special values ub={upper_bound} thr={heap_threshold:?}"),
            );
        }
        // NaN lower bounds (lane 0 via a NaN binary inner product, lane 4 via
        // a NaN scale factor) must never be pruned by either mask.
        let (pruned_upper_bound, pruned_heap) = prune_masks_portable(
            &dists,
            &scale_factors,
            &add_factors,
            &error_factors,
            terms,
            0.5,
            Some(0.0),
        );
        assert_eq!(pruned_upper_bound & 0b1_0001, 0);
        assert_eq!(pruned_heap & 0b1_0001, 0);
    }
}
