// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Dot product.

use std::iter::Sum;
use std::ops::AddAssign;
use std::sync::Arc;

use arrow_array::{Array, FixedSizeListArray, Float32Array};
use half::{bf16, f16};
#[allow(unused_imports)]
use lance_core::{
    deepsize::DeepSizeOf,
    utils::cpu::{SIMD_SUPPORT, SimdSupport},
};
use num_traits::{AsPrimitive, Num, real::Real};

use crate::Result;
#[cfg(feature = "fp16kernels")]
use crate::distance::HalfBackend;
use crate::distance::{
    BatchKernel, HALF_KERNELS_COMPILED, HalfType, PairKernel, assert_batch_output_layout,
    assert_equal_lengths, half_backend, x86_half_features,
};

/// Default implementation of dot product.
///
// The following code has been tuned for auto-vectorization.
// Please make sure run `cargo bench --bench dot` with and without AVX-512 before any change.
// Tested `target-features`: avx512f,avx512vl,f16c
#[inline(always)]
fn dot_scalar<
    T: AsPrimitive<Output>,
    Output: Real + Sum + AddAssign + 'static,
    const LANES: usize,
>(
    from: &[T],
    to: &[T],
) -> Output {
    let (x_chunks, x_remainder) = to.as_chunks::<LANES>();
    let (y_chunks, y_remainder) = from.as_chunks::<LANES>();
    let sum = if x_remainder.is_empty() {
        Output::zero()
    } else {
        x_remainder
            .iter()
            .zip(y_remainder.iter())
            .map(|(&x, &y)| x.as_() * y.as_())
            .sum::<Output>()
    };
    // Use known size to allow LLVM to kick in auto-vectorization.
    let mut sums = [Output::zero(); LANES];
    for (x, y) in x_chunks.iter().zip(y_chunks) {
        for i in 0..LANES {
            sums[i] += x[i].as_() * y[i].as_();
        }
    }
    sum + sums.iter().copied().sum::<Output>()
}

/// Dot product.
///
/// # Panics
///
/// The implementations in this crate panic if `from` and `to` have different
/// lengths. A downstream [`Dot`] implementation may define different behavior.
#[inline]
pub fn dot<T: Dot>(from: &[T], to: &[T]) -> f32 {
    T::dot(from, to)
}

/// Dot product between two f32 slices, dispatched to the widest SIMD backend
/// available at runtime. See [`crate::distance::l2::l2_f32`] for why this is
/// needed on top of the generic [`dot`].
///
/// # Panics
///
/// Panics if `x` and `y` have different lengths.
#[inline]
pub fn dot_f32(x: &[f32], y: &[f32]) -> f32 {
    f32::dot(x, y)
}

/// Negative [Dot] distance.
///
/// # Panics
///
/// The implementations in this crate panic if `from` and `to` have different
/// lengths. A downstream [`Dot`] implementation may define different behavior.
#[inline]
pub fn dot_distance<T: Dot>(from: &[T], to: &[T]) -> f32 {
    1.0 - T::dot(from, to)
}

/// Accumulate one query dimension into the Dot distances for all targets.
///
/// Keeping this in a separate function tells LLVM that `row` and `distances`
/// do not alias, allowing it to vectorize across targets on every architecture.
#[inline(never)]
fn accumulate_dot_dimension(q: f32, row: &[f32], distances: &mut [f32]) {
    for (distance, &target) in distances.iter_mut().zip(row) {
        *distance -= q * target;
    }
}

/// Pre-transposed target vectors for batched Dot distance computation.
///
/// Stores targets in SoA layout `[dimension][num_targets]`, allowing one query
/// dimension to be multiplied with many contiguous target values. Construction
/// performs the AoS-to-SoA transpose once, so callers should reuse this across
/// queries. This is intended for small target sets whose transposed values fit
/// in cache, such as PQ sub-vector codebooks.
///
/// # Example
///
/// ```
/// use lance_linalg::distance::dot::DotPrepared;
///
/// let targets = [1.0, 0.0, 0.0, 1.0];
/// let prepared = DotPrepared::new(&targets, 2);
/// assert_eq!(prepared.distances(&[1.0, 0.0]), [0.0, 1.0]);
/// ```
#[derive(Debug, Clone, DeepSizeOf)]
pub struct DotPrepared {
    transposed: Vec<f32>,
    dimension: usize,
    num_targets: usize,
}

impl DotPrepared {
    /// Transpose `targets` from AoS `[num_targets][dimension]` to SoA layout.
    pub fn new(targets: &[f32], dimension: usize) -> Self {
        let num_targets = targets.len() / dimension;
        debug_assert_eq!(targets.len(), num_targets * dimension);

        let mut transposed = vec![0.0; targets.len()];
        for target in 0..num_targets {
            for dim in 0..dimension {
                transposed[dim * num_targets + target] = targets[target * dimension + dim];
            }
        }

        Self {
            transposed,
            dimension,
            num_targets,
        }
    }

    /// Compute Dot distances from `query` to every target, writing into `out`.
    pub fn distances_into(&self, query: &[f32], out: &mut [f32]) {
        debug_assert_eq!(query.len(), self.dimension);
        debug_assert_eq!(out.len(), self.num_targets);

        out.fill(1.0);
        for (dim, &q) in query.iter().enumerate() {
            let row = &self.transposed[dim * self.num_targets..][..self.num_targets];
            accumulate_dot_dimension(q, row, out);
        }
    }

    /// Compute Dot distances from `query` to every target.
    pub fn distances(&self, query: &[f32]) -> Vec<f32> {
        let mut result = vec![0.0; self.num_targets];
        self.distances_into(query, &mut result);
        result
    }

    /// Number of targets in this set.
    pub fn num_targets(&self) -> usize {
        self.num_targets
    }

    /// Dimension of each target vector.
    pub fn dimension(&self) -> usize {
        self.dimension
    }

    /// Size of the transposed values in bytes.
    pub fn size_bytes(&self) -> usize {
        self.transposed.len() * std::mem::size_of::<f32>()
    }
}

/// Dot product
pub trait Dot: Num {
    /// Dot product.
    ///
    /// # Panics
    ///
    /// The implementations in this crate panic if `x` and `y` have different
    /// lengths. A downstream implementation may define different behavior.
    fn dot(x: &[Self], y: &[Self]) -> f32;

    /// Writes the dot product of `x` with each `dimension`-sized vector in
    /// `batch` into the matching element of `output`.
    ///
    /// The default calls [`Dot::dot`] per vector. `f32` overrides it with one
    /// batch kernel call, so the SIMD tier is chosen once for the whole batch
    /// instead of once per vector. `f64` overrides it off x86_64 to specialize
    /// the loop for the common PQ sub-vector lengths.
    ///
    /// # Panics
    ///
    /// The default implementation panics if `dimension == 0`, if
    /// `x.len() != dimension`, if `batch.len()` is not a multiple of
    /// `dimension`, or if `output.len() != batch.len() / dimension`.
    fn dot_batch_into(x: &[Self], batch: &[Self], dimension: usize, output: &mut [f32]) {
        assert_batch_output_layout(x.len(), batch.len(), dimension, output.len());
        for (product, y) in output.iter_mut().zip(batch.chunks_exact(dimension)) {
            *product = Self::dot(x, y);
        }
    }
}

#[cfg(feature = "fp16kernels")]
mod bf16_kernel {
    use half::bf16;

    // These are the `dot_bf16` function in bf16.c. Our build.rs script compiles
    // a version of this file for each SIMD level with different suffixes.
    unsafe extern "C" {
        #[cfg(target_arch = "aarch64")]
        pub fn dot_bf16_neon(ptr1: *const bf16, ptr2: *const bf16, len: u32) -> f32;
        #[cfg(all(kernel_support = "avx512_bf16", target_arch = "x86_64"))]
        pub fn dot_bf16_avx512(ptr1: *const bf16, ptr2: *const bf16, len: u32) -> f32;
        #[cfg(target_arch = "x86_64")]
        pub fn dot_bf16_avx2(ptr1: *const bf16, ptr2: *const bf16, len: u32) -> f32;
        #[cfg(target_arch = "loongarch64")]
        pub fn dot_bf16_lsx(ptr1: *const bf16, ptr2: *const bf16, len: u32) -> f32;
        #[cfg(target_arch = "loongarch64")]
        pub fn dot_bf16_lasx(ptr1: *const bf16, ptr2: *const bf16, len: u32) -> f32;
    }
}

impl Dot for bf16 {
    #[inline]
    fn dot(x: &[Self], y: &[Self]) -> f32 {
        assert_equal_lengths(x.len(), y.len());
        match half_backend(
            *SIMD_SUPPORT,
            HalfType::Bf16,
            HALF_KERNELS_COMPILED,
            cfg!(all(kernel_support = "avx512_bf16", target_arch = "x86_64")),
            x86_half_features(),
        ) {
            #[cfg(all(feature = "fp16kernels", target_arch = "aarch64"))]
            HalfBackend::Neon => unsafe {
                bf16_kernel::dot_bf16_neon(x.as_ptr(), y.as_ptr(), x.len() as u32)
            },
            #[cfg(all(
                feature = "fp16kernels",
                kernel_support = "avx512_bf16",
                target_arch = "x86_64"
            ))]
            HalfBackend::Avx512 => unsafe {
                bf16_kernel::dot_bf16_avx512(x.as_ptr(), y.as_ptr(), x.len() as u32)
            },
            #[cfg(all(feature = "fp16kernels", target_arch = "x86_64"))]
            HalfBackend::Avx2 => unsafe {
                bf16_kernel::dot_bf16_avx2(x.as_ptr(), y.as_ptr(), x.len() as u32)
            },
            #[cfg(all(feature = "fp16kernels", target_arch = "loongarch64"))]
            HalfBackend::Lasx => unsafe {
                bf16_kernel::dot_bf16_lasx(x.as_ptr(), y.as_ptr(), x.len() as u32)
            },
            #[cfg(all(feature = "fp16kernels", target_arch = "loongarch64"))]
            HalfBackend::Lsx => unsafe {
                bf16_kernel::dot_bf16_lsx(x.as_ptr(), y.as_ptr(), x.len() as u32)
            },
            // SimdSupport::AvxFma and SimdSupport::Avx fall through here:
            // the bf16 C kernels are compiled with `-march=haswell` minimum
            // (AVX2), so they cannot run on AVX-only or AVX+FMA hosts.
            _ => dot_scalar::<Self, f32, 32>(x, y),
        }
    }
}

#[cfg(feature = "fp16kernels")]
mod kernel {
    use super::*;

    // These are the `dot_f16` function in f16.c. Our build.rs script compiles
    // a version of this file for each SIMD level with different suffixes.
    unsafe extern "C" {
        #[cfg(target_arch = "aarch64")]
        pub fn dot_f16_neon(ptr1: *const f16, ptr2: *const f16, len: u32) -> f32;
        #[cfg(all(kernel_support = "avx512_f16", target_arch = "x86_64"))]
        pub fn dot_f16_avx512(ptr1: *const f16, ptr2: *const f16, len: u32) -> f32;
        #[cfg(target_arch = "x86_64")]
        pub fn dot_f16_avx2(ptr1: *const f16, ptr2: *const f16, len: u32) -> f32;
        #[cfg(target_arch = "loongarch64")]
        pub fn dot_f16_lsx(ptr1: *const f16, ptr2: *const f16, len: u32) -> f32;
        #[cfg(target_arch = "loongarch64")]
        pub fn dot_f16_lasx(ptr1: *const f16, ptr2: *const f16, len: u32) -> f32;
    }
}

impl Dot for f16 {
    #[inline]
    fn dot(x: &[Self], y: &[Self]) -> f32 {
        assert_equal_lengths(x.len(), y.len());
        match half_backend(
            *SIMD_SUPPORT,
            HalfType::F16,
            HALF_KERNELS_COMPILED,
            cfg!(all(kernel_support = "avx512_f16", target_arch = "x86_64")),
            x86_half_features(),
        ) {
            #[cfg(all(feature = "fp16kernels", target_arch = "aarch64"))]
            HalfBackend::Neon => unsafe {
                kernel::dot_f16_neon(x.as_ptr(), y.as_ptr(), x.len() as u32)
            },
            #[cfg(all(
                feature = "fp16kernels",
                kernel_support = "avx512_f16",
                target_arch = "x86_64"
            ))]
            HalfBackend::Avx512 => unsafe {
                kernel::dot_f16_avx512(x.as_ptr(), y.as_ptr(), x.len() as u32)
            },
            #[cfg(all(feature = "fp16kernels", target_arch = "x86_64"))]
            HalfBackend::Avx2 => unsafe {
                kernel::dot_f16_avx2(x.as_ptr(), y.as_ptr(), x.len() as u32)
            },
            #[cfg(all(feature = "fp16kernels", target_arch = "loongarch64"))]
            HalfBackend::Lasx => unsafe {
                kernel::dot_f16_lasx(x.as_ptr(), y.as_ptr(), x.len() as u32)
            },
            #[cfg(all(feature = "fp16kernels", target_arch = "loongarch64"))]
            HalfBackend::Lsx => unsafe {
                kernel::dot_f16_lsx(x.as_ptr(), y.as_ptr(), x.len() as u32)
            },
            // SimdSupport::AvxFma and SimdSupport::Avx retain their scalar
            // route; this fallback only extends the tiers the C kernel already
            // served to Avx512FP16 after checking F16C and FMA.
            _ => dot_scalar::<Self, f32, 32>(x, y),
        }
    }
}

impl Dot for f32 {
    #[inline]
    fn dot(x: &[Self], y: &[Self]) -> f32 {
        assert_equal_lengths(x.len(), y.len());
        DOT_F32.get().call(x, y)
    }

    fn dot_batch_into(x: &[Self], batch: &[Self], dimension: usize, output: &mut [f32]) {
        assert_batch_output_layout(x.len(), batch.len(), dimension, output.len());
        DOT_F32_BATCH.get().call(x, batch, dimension, output);
    }
}

lance_core::dispatch! {
    static DOT_F32: Dispatch<PairKernel> = {
        #[cfg(target_arch = "x86_64")]
        {
            &[
                x86::DOT_F32_AVX512,
                x86::DOT_F32_AVX_FMA,
                x86::DOT_F32_AVX,
                DOT_F32_SCALAR,
            ]
        }
        #[cfg(not(target_arch = "x86_64"))]
        {
            &[DOT_F32_SCALAR]
        }
    };
}

lance_core::dispatch! {
    /// See `L2_F32_BATCH` for why there is no AVX-512 candidate.
    static DOT_F32_BATCH: Dispatch<BatchKernel> = {
        #[cfg(target_arch = "x86_64")]
        {
            &[
                x86::DOT_F32_BATCH_AVX_FMA,
                x86::DOT_F32_BATCH_AVX,
                DOT_F32_BATCH_SCALAR,
            ]
        }
        #[cfg(not(target_arch = "x86_64"))]
        {
            &[DOT_F32_BATCH_SCALAR]
        }
    };
}

lance_core::target_kernel! {
    #[target_features()]
    #[candidate(DOT_F32_SCALAR: PairKernel)]
    /// Portable dot product for f32, relying on LLVM to auto-vectorize it.
    #[inline]
    fn dot_f32_scalar(x: &[f32], y: &[f32]) -> f32 {
        dot_scalar::<f32, f32, 16>(x, y)
    }
}

lance_core::target_kernel! {
    #[target_features()]
    #[candidate(DOT_F32_BATCH_SCALAR: BatchKernel)]
    #[inline]
    fn dot_batch_f32_scalar(x: &[f32], batch: &[f32], dimension: usize, output: &mut [f32]) {
        for (distance, y) in output.iter_mut().zip(batch.chunks_exact(dimension)) {
            *distance = dot_f32_scalar(x, y);
        }
    }
}

impl Dot for f64 {
    #[inline]
    fn dot(x: &[Self], y: &[Self]) -> f32 {
        assert_equal_lengths(x.len(), y.len());
        dot_f64_simd(x, y)
    }

    fn dot_batch_into(x: &[Self], batch: &[Self], dimension: usize, output: &mut [f32]) {
        assert_batch_output_layout(x.len(), batch.len(), dimension, output.len());
        // Interim: on x86_64 f64 keeps the per-vector loop, since its PQ dot
        // tables already beat the pre-`Dispatch` code; f64 moves onto
        // `Dispatch` batch candidates later in #10083.
        #[cfg(target_arch = "x86_64")]
        for (product, y) in output.iter_mut().zip(batch.chunks_exact(dimension)) {
            *product = dot_f64_simd(x, y);
        }
        // Fixed query lengths and the pointer walk below are for the same
        // reasons as in `L2::l2_batch_into` for f64.
        #[cfg(not(target_arch = "x86_64"))]
        if let Ok(x) = <&[Self; 8]>::try_from(x) {
            dot_batch_f64_fixed(x, batch, output);
        } else if let Ok(x) = <&[Self; 16]>::try_from(x) {
            dot_batch_f64_fixed(x, batch, output);
        } else {
            let mut y = batch.as_ptr();
            for product in output.iter_mut() {
                // SAFETY: the layout assert guarantees `batch.len() == output.len() * dimension`.
                unsafe {
                    *product = dot_f64_simd_other(x, std::slice::from_raw_parts(y, dimension));
                    y = y.add(dimension);
                }
            }
        }
    }
}

#[cfg(not(target_arch = "x86_64"))]
#[inline(always)]
fn dot_batch_f64_fixed<const N: usize>(x: &[f64; N], batch: &[f64], output: &mut [f32]) {
    for (product, y) in output.iter_mut().zip(batch.as_chunks::<N>().0) {
        *product = dot_f64_simd_other(x, y);
    }
}

/// Dot product for f64, runtime-dispatched via `SIMD_SUPPORT` on x86_64
/// (AVX-512 / AVX2+FMA / AVX+FMA / AVX / scalar). Non-x86 uses the SIMD
/// primitives in `crate::simd::f64`, unconditionally backed by NEON / LSX-LASX.
#[inline]
fn dot_f64_simd(x: &[f64], y: &[f64]) -> f32 {
    #[cfg(target_arch = "x86_64")]
    {
        match *SIMD_SUPPORT {
            SimdSupport::Avx512 | SimdSupport::Avx512FP16 => unsafe { x86::dot_f64_avx512(x, y) },
            SimdSupport::Avx2 | SimdSupport::AvxFma => unsafe { x86::dot_f64_avx_fma(x, y) },
            SimdSupport::Avx => unsafe { x86::dot_f64_avx(x, y) },
            _ => dot_f64_scalar(x, y),
        }
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        dot_f64_simd_other(x, y)
    }
}

/// Portable scalar dot product for f64. Used as the x86_64 fallback when no
/// AVX2 is detected, and exposed for cross-backend parity testing.
#[cfg(target_arch = "x86_64")]
#[inline]
fn dot_f64_scalar(x: &[f64], y: &[f64]) -> f32 {
    x.iter().zip(y.iter()).map(|(&a, &b)| a * b).sum::<f64>() as f32
}

#[cfg(target_arch = "x86_64")]
mod x86 {
    use std::arch::x86_64::*;

    use crate::distance::{BatchKernel, PairKernel};
    use crate::simd::f64::{f64x4, f64x8};
    use crate::simd::x86::hsum256_ps;
    use crate::simd::{FloatSimd, SIMD};

    lance_core::target_kernel! {
        #[target_features("avx", "fma")]
        #[candidate(pub(super) DOT_F32_BATCH_AVX_FMA: BatchKernel)]
        /// Up to 16 dimensions the scalar kernel's 16-lane chunking degenerates
        /// to its remainder loop, so the explicit AVX kernel wins; above that
        /// the auto-vectorized scalar kernel is as good or better.
        ///
        /// Unlike `l2_batch_f32_avx_fma` there is no dimension-16 fast path:
        /// that one closed a Haswell regression in PQ L2 tables, while dot at
        /// dimension 16 was already faster than before `Dispatch`.
        pub(super) fn dot_batch_f32_avx_fma(
            x: &[f32],
            batch: &[f32],
            dimension: usize,
            output: &mut [f32],
        ) {
            if dimension == 8
                && let Ok(key) = <&[f32; 8]>::try_from(x)
            {
                let key = unsafe { _mm256_loadu_ps(key.as_ptr()) };
                for (distance, y) in output.iter_mut().zip(batch.as_chunks::<8>().0) {
                    let product = _mm256_mul_ps(key, unsafe { _mm256_loadu_ps(y.as_ptr()) });
                    *distance = unsafe { hsum256_ps(product) };
                }
            } else if dimension <= 16 {
                for (distance, y) in output.iter_mut().zip(batch.chunks_exact(dimension)) {
                    *distance = dot_f32_avx_fma(x, y);
                }
            } else {
                for (distance, y) in output.iter_mut().zip(batch.chunks_exact(dimension)) {
                    *distance = super::dot_f32_scalar(x, y);
                }
            }
        }
    }

    lance_core::target_kernel! {
        #[target_features("avx")]
        #[candidate(pub(super) DOT_F32_BATCH_AVX: BatchKernel)]
        pub(super) fn dot_batch_f32_avx(
            x: &[f32],
            batch: &[f32],
            dimension: usize,
            output: &mut [f32],
        ) {
            if dimension == 8
                && let Ok(key) = <&[f32; 8]>::try_from(x)
            {
                let key = unsafe { _mm256_loadu_ps(key.as_ptr()) };
                for (distance, y) in output.iter_mut().zip(batch.as_chunks::<8>().0) {
                    let product = _mm256_mul_ps(key, unsafe { _mm256_loadu_ps(y.as_ptr()) });
                    *distance = unsafe { hsum256_ps(product) };
                }
            } else {
                for (distance, y) in output.iter_mut().zip(batch.chunks_exact(dimension)) {
                    *distance = dot_f32_avx(x, y);
                }
            }
        }
    }

    /// AVX-512 path for f64: 8-wide `__m512d` with `vfmadd231pd` per iteration.
    #[target_feature(enable = "avx512f")]
    pub unsafe fn dot_f64_avx512(x: &[f64], y: &[f64]) -> f32 {
        let dim = x.len();
        let unrolled_len = dim / 8 * 8;

        let mut acc = _mm512_setzero_pd();
        for i in (0..unrolled_len).step_by(8) {
            let a = _mm512_loadu_pd(x.as_ptr().add(i));
            let b = _mm512_loadu_pd(y.as_ptr().add(i));
            acc = _mm512_fmadd_pd(a, b, acc);
        }

        let tail: f64 = x[unrolled_len..]
            .iter()
            .zip(y[unrolled_len..].iter())
            .map(|(&a, &b)| a * b)
            .sum();

        (_mm512_reduce_add_pd(acc) + tail) as f32
    }

    /// AVX + FMA path for f64. Covers both AvxFma and AVX2 dispatch (body uses no AVX2-specific intrinsics).
    #[target_feature(enable = "avx,fma")]
    pub unsafe fn dot_f64_avx_fma(x: &[f64], y: &[f64]) -> f32 {
        let dim = x.len();
        let unrolled_len = dim / 8 * 8;

        let mut acc8 = f64x8::zeros();
        for i in (0..unrolled_len).step_by(8) {
            let a = f64x8::load_unaligned(x.as_ptr().add(i));
            let b = f64x8::load_unaligned(y.as_ptr().add(i));
            acc8.multiply_add(a, b);
        }

        let aligned_len = dim / 4 * 4;
        let mut acc4 = f64x4::zeros();
        for i in (unrolled_len..aligned_len).step_by(4) {
            let a = f64x4::load_unaligned(x.as_ptr().add(i));
            let b = f64x4::load_unaligned(y.as_ptr().add(i));
            acc4.multiply_add(a, b);
        }

        let tail: f64 = x[aligned_len..]
            .iter()
            .zip(y[aligned_len..].iter())
            .map(|(&a, &b)| a * b)
            .sum();

        (acc8.reduce_sum() + acc4.reduce_sum() + tail) as f32
    }

    /// AVX-only path for f64 (no FMA): `_mm256_mul_pd` + `_mm256_add_pd` per iteration for Sandy/Ivy Bridge.
    #[target_feature(enable = "avx")]
    pub unsafe fn dot_f64_avx(x: &[f64], y: &[f64]) -> f32 {
        let dim = x.len();
        let unrolled_len = dim / 4 * 4;

        let mut acc = _mm256_setzero_pd();
        for i in (0..unrolled_len).step_by(4) {
            let a = _mm256_loadu_pd(x.as_ptr().add(i));
            let b = _mm256_loadu_pd(y.as_ptr().add(i));
            acc = _mm256_add_pd(acc, _mm256_mul_pd(a, b));
        }

        // Horizontal sum of __m256d -> f64. Two pairwise adds across lanes.
        let lo = _mm256_castpd256_pd128(acc);
        let hi = _mm256_extractf128_pd(acc, 1);
        let sum128 = _mm_add_pd(lo, hi);
        let sum64 = _mm_add_pd(sum128, _mm_unpackhi_pd(sum128, sum128));
        let acc_sum = _mm_cvtsd_f64(sum64);

        let tail: f64 = x[unrolled_len..]
            .iter()
            .zip(y[unrolled_len..].iter())
            .map(|(&a, &b)| a * b)
            .sum();

        (acc_sum + tail) as f32
    }

    lance_core::target_kernel! {
        #[target_features("avx512f")]
        #[candidate(pub(super) DOT_F32_AVX512: PairKernel)]
        /// AVX-512 path for f32: 16-wide `__m512` with `vfmadd231ps` per iteration.
        #[inline]
        pub(super) fn dot_f32_avx512(x: &[f32], y: &[f32]) -> f32 {
            // Callers pass equal lengths; `min` keeps the unchecked loads in
            // bounds for any input, as a safe function must.
            let dim = x.len().min(y.len());
            let unrolled_len = dim / 16 * 16;

            let mut acc = _mm512_setzero_ps();
            for i in (0..unrolled_len).step_by(16) {
                let (a, b) = unsafe {
                    (
                        _mm512_loadu_ps(x.as_ptr().add(i)),
                        _mm512_loadu_ps(y.as_ptr().add(i)),
                    )
                };
                acc = _mm512_fmadd_ps(a, b, acc);
            }

            let tail: f32 = x[unrolled_len..]
                .iter()
                .zip(y[unrolled_len..].iter())
                .map(|(&a, &b)| a * b)
                .sum();

            _mm512_reduce_add_ps(acc) + tail
        }
    }

    lance_core::target_kernel! {
        #[target_features("avx", "fma")]
        #[candidate(pub(super) DOT_F32_AVX_FMA: PairKernel)]
        /// AVX + FMA path for f32. Covers both AvxFma and AVX2 hosts (body uses no AVX2-specific intrinsics).
        #[inline]
        pub(super) fn dot_f32_avx_fma(x: &[f32], y: &[f32]) -> f32 {
            // See `dot_f32_avx512` for `min`.
            let dim = x.len().min(y.len());
            let unrolled_len = dim / 8 * 8;

            let mut acc = _mm256_setzero_ps();
            for i in (0..unrolled_len).step_by(8) {
                let (a, b) = unsafe {
                    (
                        _mm256_loadu_ps(x.as_ptr().add(i)),
                        _mm256_loadu_ps(y.as_ptr().add(i)),
                    )
                };
                acc = _mm256_fmadd_ps(a, b, acc);
            }

            let tail: f32 = x[unrolled_len..]
                .iter()
                .zip(y[unrolled_len..].iter())
                .map(|(&a, &b)| a * b)
                .sum();

            let sum = unsafe { hsum256_ps(acc) };
            sum + tail
        }
    }

    lance_core::target_kernel! {
        #[target_features("avx")]
        #[candidate(pub(super) DOT_F32_AVX: PairKernel)]
        /// AVX-only path for f32 (no FMA): `_mm256_mul_ps` + `_mm256_add_ps` per iteration for Sandy/Ivy Bridge.
        #[inline]
        pub(super) fn dot_f32_avx(x: &[f32], y: &[f32]) -> f32 {
            // See `dot_f32_avx512` for `min`.
            let dim = x.len().min(y.len());
            let unrolled_len = dim / 8 * 8;

            let mut acc = _mm256_setzero_ps();
            for i in (0..unrolled_len).step_by(8) {
                let (a, b) = unsafe {
                    (
                        _mm256_loadu_ps(x.as_ptr().add(i)),
                        _mm256_loadu_ps(y.as_ptr().add(i)),
                    )
                };
                acc = _mm256_add_ps(acc, _mm256_mul_ps(a, b));
            }

            let tail: f32 = x[unrolled_len..]
                .iter()
                .zip(y[unrolled_len..].iter())
                .map(|(&a, &b)| a * b)
                .sum();

            let sum = unsafe { hsum256_ps(acc) };
            sum + tail
        }
    }
}

#[cfg(not(target_arch = "x86_64"))]
#[inline]
fn dot_f64_simd_other(x: &[f64], y: &[f64]) -> f32 {
    use crate::simd::f64::{f64x4, f64x8};
    use crate::simd::{FloatSimd, SIMD};

    let dim = x.len();
    let unrolled_len = dim / 8 * 8;

    let mut acc8 = f64x8::zeros();
    for i in (0..unrolled_len).step_by(8) {
        unsafe {
            let a = f64x8::load_unaligned(x.as_ptr().add(i));
            let b = f64x8::load_unaligned(y.as_ptr().add(i));
            acc8.multiply_add(a, b);
        }
    }

    let aligned_len = dim / 4 * 4;
    let mut acc4 = f64x4::zeros();
    for i in (unrolled_len..aligned_len).step_by(4) {
        unsafe {
            let a = f64x4::load_unaligned(x.as_ptr().add(i));
            let b = f64x4::load_unaligned(y.as_ptr().add(i));
            acc4.multiply_add(a, b);
        }
    }

    let tail: f64 = x[aligned_len..]
        .iter()
        .zip(y[aligned_len..].iter())
        .map(|(&a, &b)| a * b)
        .sum();

    (acc8.reduce_sum() + acc4.reduce_sum() + tail) as f32
}

impl Dot for u8 {
    #[inline]
    fn dot(x: &[Self], y: &[Self]) -> f32 {
        assert_equal_lengths(x.len(), y.len());
        super::dot_u8::dot_u8_u64(x, y) as f32
    }
}

/// Writes the dot distance, `1 - dot`, from `from` to each `dimension`-sized
/// vector in `to` into the matching element of `output`.
///
/// # Panics
///
/// The implementations in this crate panic if `dimension == 0`, if
/// `from.len() != dimension`, if `to.len()` is not a multiple of `dimension`,
/// or if `output.len() != to.len() / dimension`.
///
/// ```
/// # use lance_linalg::distance::dot_distance_batch_into;
/// let mut distances = [0.0; 2];
/// dot_distance_batch_into(&[1.0_f32, 2.0], &[1.0, 0.0, 0.0, 1.0], 2, &mut distances);
/// assert_eq!(distances, [0.0, -1.0]);
/// ```
#[inline]
pub fn dot_distance_batch_into<T: Dot>(from: &[T], to: &[T], dimension: usize, output: &mut [f32]) {
    T::dot_batch_into(from, to, dimension, output);
    for distance in output.iter_mut() {
        *distance = 1.0 - *distance;
    }
}

/// Compute dot distances, propagating the null buffer of `to`.
///
/// See [`super::DistanceType::batch`] for supported types and input validation.
#[deprecated(note = "Use DistanceType::Dot.batch")]
pub fn dot_distance_arrow_batch(
    from: &dyn Array,
    to: &FixedSizeListArray,
) -> Result<Arc<Float32Array>> {
    super::DistanceType::Dot.batch(from, to).map(Arc::new)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::{
        arbitrary_bf16, arbitrary_f16, arbitrary_f32, arbitrary_f64, arbitrary_vector_pair,
        dimension_shard, run_vector_pair_proptest,
    };
    use num_traits::{Float, FromPrimitive};
    use proptest::prelude::*;

    /// PQ distance tables call this per sub-vector, so Haswell-baseline
    /// builds must reach the kernel without a runtime dispatch.
    #[cfg(target_arch = "x86_64")]
    #[test]
    fn test_dot_f32_batch_is_static_on_haswell() {
        use lance_core::utils::cpu::CpuFeatures;
        let haswell =
            CpuFeatures::from_target_features(&["avx2", "fma", "f16c", "bmi1", "bmi2", "popcnt"]);
        assert!(haswell.contains(DOT_F32_BATCH.candidates()[0].features()));
    }

    #[test]
    fn test_dot_rejects_mismatched_lengths() {
        let short = [1.0_f32];
        let long = [1.0_f32, 2.0];

        assert!(std::panic::catch_unwind(|| dot(&short, &long)).is_err());
        assert!(std::panic::catch_unwind(|| dot_f32(&short, &long)).is_err());
        assert!(std::panic::catch_unwind(|| f32::dot(&short, &long)).is_err());
        assert!(std::panic::catch_unwind(|| dot_distance(&short, &long)).is_err());
        assert!(
            std::panic::catch_unwind(|| dot_distance_batch_into(&short, &long, 2, &mut [0.0]))
                .is_err()
        );
        assert!(
            std::panic::catch_unwind(|| dot_distance_batch_into(
                &long,
                &[1.0_f32; 3],
                2,
                &mut [0.0]
            ))
            .is_err()
        );
        // The f64 batch loops off x86 walk `batch` by pointer, sized by
        // `output`, so a wrong `output` length must be rejected for f64 too.
        for output_len in [1, 3] {
            assert!(
                std::panic::catch_unwind(|| dot_distance_batch_into(
                    &[1.0_f64, 2.0],
                    &[1.0_f64; 4],
                    2,
                    &mut vec![0.0; output_len]
                ))
                .is_err(),
                "f64 accepted output length {output_len}"
            );
        }
        assert!(
            std::panic::catch_unwind(|| dot_distance_batch_into::<f32>(&[], &[], 0, &mut []))
                .is_err()
        );
        assert!(
            std::panic::catch_unwind(|| dot_distance_batch_into(&long, &long, 2, &mut [0.0; 2]))
                .is_err()
        );
    }

    #[test]
    fn test_dot_f32_dispatch_matches_scalar() {
        use approx::assert_relative_eq;
        // Covers tail handling for lengths around the 16-lane AVX-512 stride.
        for dim in [1usize, 7, 15, 16, 17, 31, 33, 64, 100, 1024] {
            let x: Vec<f32> = (0..dim).map(|i| (i as f32) * 0.5 - 3.0).collect();
            let y: Vec<f32> = (0..dim).map(|i| (i as f32) * -0.25 + 1.5).collect();
            assert_relative_eq!(dot_f32(&x, &y), dot(&x, &y), max_relative = 1e-5);
        }
    }

    #[test]
    fn test_dot_prepared_matches_per_target_distance() {
        for (dimension, num_targets) in
            [(1_usize, 32_usize), (3, 20), (8, 256), (16, 17), (128, 64)]
        {
            let query = (0..dimension)
                .map(|idx| idx as f32 * 0.125 - 1.5)
                .collect::<Vec<_>>();
            let targets = (0..dimension * num_targets)
                .map(|idx| ((idx * 7) % 101) as f32 * 0.02 - 1.0)
                .collect::<Vec<_>>();
            let expected = targets
                .chunks_exact(dimension)
                .map(|target| dot_distance(&query, target))
                .collect::<Vec<_>>();

            let prepared = DotPrepared::new(&targets, dimension);
            let actual = prepared.distances(&query);

            assert_eq!(prepared.dimension(), dimension);
            assert_eq!(prepared.num_targets(), num_targets);
            assert_eq!(
                prepared.size_bytes(),
                targets.len() * std::mem::size_of::<f32>()
            );
            for (target, (actual, expected)) in actual.iter().zip(expected.iter()).enumerate() {
                assert!(
                    approx::relative_eq!(actual, expected, epsilon = 1e-4, max_relative = 1e-5),
                    "target {target}, dimension {dimension}: prepared={actual}, expected={expected}"
                );
            }
        }
    }

    #[test]
    fn test_dot_prepared_reuses_output() {
        let prepared = DotPrepared::new(&[1.0, 0.0, 0.0, 1.0], 2);
        let mut distances = vec![f32::NAN; 2];

        prepared.distances_into(&[1.0, 0.0], &mut distances);
        assert_eq!(distances, [0.0, 1.0]);
        prepared.distances_into(&[0.0, 1.0], &mut distances);
        assert_eq!(distances, [1.0, 0.0]);
    }

    #[test]
    fn test_dot() {
        let x: Vec<f32> = (0..20).map(|v| v as f32).collect();
        let y: Vec<f32> = (100..120).map(|v| v as f32).collect();

        assert_eq!(f32::dot(&x, &y), dot(&x, &y));

        let x: Vec<f32> = (0..512).map(|v| v as f32).collect();
        let y: Vec<f32> = (100..612).map(|v| v as f32).collect();

        assert_eq!(f32::dot(&x, &y), dot(&x, &y));

        let x: Vec<f16> = (0..20).map(|v| f16::from_i32(v).unwrap()).collect();
        let y: Vec<f16> = (100..120).map(|v| f16::from_i32(v).unwrap()).collect();
        assert_eq!(f16::dot(&x, &y), dot(&x, &y));

        let x: Vec<f64> = (20..40).map(|v| f64::from_i32(v).unwrap()).collect();
        let y: Vec<f64> = (120..140).map(|v| f64::from_i32(v).unwrap()).collect();
        assert_eq!(f64::dot(&x, &y), dot(&x, &y));
    }

    /// Reference implementation of dot product.
    fn dot_scalar_ref(x: &[f64], y: &[f64]) -> f32 {
        x.iter().zip(y.iter()).map(|(&x, &y)| x * y).sum::<f64>() as f32
    }

    /// Error bound for vector dot product
    /// http://ftp.demec.ufpr.br/CFD/bibliografia/Higham_2002_Accuracy%20and%20Stability%20of%20Numerical%20Algorithms.pdf
    /// Chapter 3 (page 61) equation 3.5
    /// A float point calculation error is bounded by:
    /// (kє/(1-kє)) Sum_i(|x_i||y_i|) if kє < 1
    /// We are currently using a SIMD version of naive product and summation.
    /// Therefore, k = 2n-1 (n multiplications, n-1 additions).
    /// For f16 and bf16, kє can be >=1.
    /// When that happens, we will use a simpler estimation method:
    /// Imagine that each `x_i` can vary by `є * |x_i|`, similarly for `y_i`.
    /// (Basically, it's accurate to ±(1 + є) * |x_i|).
    /// Error for `sum(x, y)` is `є_x + є_y`.
    /// Error for multiple is `є_x * x + є_y * y + є_x * є_y`,
    /// which simplifies to `є_x * x + є_y * y`
    /// See: https://www.geol.lsu.edu/jlorenzo/geophysics/uncertainties/Uncertaintiespart2.html
    /// The multiplication of `x_i` and `y_i` can vary by `є|x_i||y_i| + є|y_i||x_i|`.
    /// This simplifies to `2є|x_i||y_i|`.
    /// So the error for the sum of all the multiplications is `2є Sum_i(|x_i||y_i|)`.
    fn max_error<T: Float + AsPrimitive<f64>>(x: &[f64], y: &[f64]) -> f32 {
        let dot = x
            .iter()
            .cloned()
            .zip(y.iter().cloned())
            .map(|(x, y)| x.abs() * y.abs())
            .sum::<f64>();
        let k = ((2 * x.len()) - 1) as f64;
        let k_epsilon = k * T::epsilon().as_();

        let error = if k_epsilon < 1.0 {
            k_epsilon * dot
        } else {
            2.0 * T::epsilon().as_() * dot
        };

        // Near the subnormal range the analytical error can underflow to zero,
        // but f32 accumulation can still differ by a few subnormal ULPs.
        let subnormal_rounding_floor = x.len() as f64 * f64::from(f32::from_bits(1));
        error.max(subnormal_rounding_floor) as f32
    }

    fn do_dot_test<T: Dot + AsPrimitive<f64> + Float>(
        x: &[T],
        y: &[T],
    ) -> std::result::Result<(), TestCaseError> {
        let f64_x = x.iter().map(|&v| v.as_()).collect::<Vec<f64>>();
        let f64_y = y.iter().map(|&v| v.as_()).collect::<Vec<f64>>();

        let expected = dot_scalar_ref(&f64_x, &f64_y);
        let result = dot(x, y);

        let max_error = max_error::<T>(&f64_x, &f64_y);

        prop_assert!(approx::relative_eq!(expected, result, epsilon = max_error));
        Ok(())
    }

    #[rstest::rstest]
    fn test_dot_f32(#[values(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15)] shard: usize) {
        run_vector_pair_proptest(arbitrary_f32, dimension_shard(shard), |x, y| {
            do_dot_test(&x, &y)
        });
    }

    #[rstest::rstest]
    fn test_dot_f64(#[values(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15)] shard: usize) {
        run_vector_pair_proptest(arbitrary_f64, dimension_shard(shard), |x, y| {
            do_dot_test(&x, &y)
        });
    }

    /// Every host-supported f32 kernel, single-pair and batch, must agree with
    /// an f64 reference. Prefixes of length 8 and 16 pin the batch kernels'
    /// PQ-width fast paths regardless of the generated dimension.
    #[rstest::rstest]
    fn test_dot_f32_kernels_match_reference(
        #[values(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15)] shard: usize,
    ) {
        run_vector_pair_proptest(arbitrary_f32, dimension_shard(shard), |x, y| {
            for dimension in [8, 16, x.len()].into_iter().filter(|&d| d <= x.len()) {
                let (x, y) = (&x[..dimension], &y[..dimension]);
                let x_f64: Vec<f64> = x.iter().map(|&v| v as f64).collect();
                let y_f64: Vec<f64> = y.iter().map(|&v| v as f64).collect();
                let want = dot_scalar_ref(&x_f64, &y_f64);
                let max_error = max_error::<f32>(&x_f64, &y_f64);

                for candidate in DOT_F32.candidates() {
                    let Some(kernel) = candidate.kernel() else {
                        continue;
                    };
                    let got = kernel.call(x, y);
                    prop_assert!(
                        approx::relative_eq!(got, want, epsilon = max_error),
                        "{} at dim {dimension}: {got} != {want}",
                        candidate.name()
                    );
                }

                // Where an x86 batch kernel reuses its tier's pair kernel or a
                // fast path for it, the two must agree bit for bit: up to 16
                // dimensions for AVX+FMA (above that it uses the scalar
                // kernel), and at every dimension for AVX. The one allowed
                // difference is the sign of a zero result: the pair kernels
                // add products to a +0.0 accumulator, which turns -0.0 into
                // +0.0, while the dimension-8 fast paths sum the products
                // directly.
                #[cfg(target_arch = "x86_64")]
                for (pair, batch, max_dimension) in [
                    (x86::DOT_F32_AVX_FMA, x86::DOT_F32_BATCH_AVX_FMA, 16),
                    (x86::DOT_F32_AVX, x86::DOT_F32_BATCH_AVX, usize::MAX),
                ] {
                    if dimension > max_dimension {
                        continue;
                    }
                    let (Some(pair_kernel), Some(batch_kernel)) = (pair.kernel(), batch.kernel())
                    else {
                        continue;
                    };
                    let mut got = [f32::NAN];
                    batch_kernel.call(x, y, dimension, &mut got);
                    let want = pair_kernel.call(x, y);
                    prop_assert!(
                        got[0].to_bits() == want.to_bits() || (got[0] == 0.0 && want == 0.0),
                        "{} at dim {dimension}: {} != {want}",
                        batch.name(),
                        got[0]
                    );
                }

                // The zero vector pins the second output to exactly 0.
                let batch = [y, &vec![0.0; dimension]].concat();
                for candidate in DOT_F32_BATCH.candidates() {
                    let Some(kernel) = candidate.kernel() else {
                        continue;
                    };
                    let mut got = [f32::NAN; 2];
                    kernel.call(x, &batch, dimension, &mut got);
                    prop_assert!(
                        approx::relative_eq!(got[0], want, epsilon = max_error) && got[1] == 0.0,
                        "{} at dim {dimension}: {got:?} != [{want}, 0]",
                        candidate.name()
                    );
                }
            }
            Ok(())
        });
    }

    proptest::proptest! {
        #[test]
        fn test_dot_f16((x, y) in arbitrary_vector_pair(arbitrary_f16, 4..4048)) {
            do_dot_test(&x, &y)?;
        }

        #[test]
        fn test_dot_bf16((x, y) in arbitrary_vector_pair(arbitrary_bf16, 4..4048)){
            do_dot_test(&x, &y)?;
        }

        /// Cross-backend parity: scalar fallback must match the dispatched
        /// SIMD path within numerical tolerance. Exercises `dot_f64_scalar`
        /// directly so the runtime fallback is exercised even on AVX2-capable
        /// CI hosts.
        #[cfg(target_arch = "x86_64")]
        #[test]
        fn test_dot_f64_scalar_simd_parity(
            (x, y) in arbitrary_vector_pair(arbitrary_f64, 4..4048)
        ) {
            let scalar = dot_f64_scalar(&x, &y);
            let simd = dot_f64_simd(&x, &y);
            let max_error = max_error::<f64>(&x, &y);
            prop_assert!(approx::relative_eq!(scalar, simd, epsilon = max_error));
        }

        /// AVX-512-direct parity: explicitly compares the scalar fallback
        /// against the native AVX-512 inner kernel on AVX-512F-capable hosts
        /// (Skylake-X+, Ice Lake, Sapphire Rapids, Zen 4). Early-returns on
        /// hosts without AVX-512F.
        #[cfg(target_arch = "x86_64")]
        #[test]
        fn test_dot_f64_scalar_vs_avx512_parity(
            (x, y) in arbitrary_vector_pair(arbitrary_f64, 4..4048)
        ) {
            if !std::is_x86_feature_detected!("avx512f") {
                return Ok(());
            }
            let scalar = dot_f64_scalar(&x, &y);
            let avx512 = unsafe { x86::dot_f64_avx512(&x, &y) };
            let max_error = max_error::<f64>(&x, &y);
            prop_assert!(approx::relative_eq!(scalar, avx512, epsilon = max_error));
        }

        /// AVX + FMA-direct parity for the f64 dot kernel. Covers the AMD
        /// Piledriver / Steamroller / FX-7500 tier. Early-returns on hosts
        /// without both AVX and FMA.
        #[cfg(target_arch = "x86_64")]
        #[test]
        fn test_dot_f64_scalar_vs_avx_fma_parity(
            (x, y) in arbitrary_vector_pair(arbitrary_f64, 4..4048)
        ) {
            if !(std::is_x86_feature_detected!("avx") && std::is_x86_feature_detected!("fma")) {
                return Ok(());
            }
            let scalar = dot_f64_scalar(&x, &y);
            let avx_fma = unsafe { x86::dot_f64_avx_fma(&x, &y) };
            let max_error = max_error::<f64>(&x, &y);
            prop_assert!(approx::relative_eq!(scalar, avx_fma, epsilon = max_error));
        }

        /// AVX-only-direct parity for the f64 dot kernel. Covers the Intel
        /// Sandy Bridge / Ivy Bridge tier (AVX without FMA). Early-returns
        /// on hosts without AVX.
        #[cfg(target_arch = "x86_64")]
        #[test]
        fn test_dot_f64_scalar_vs_avx_parity(
            (x, y) in arbitrary_vector_pair(arbitrary_f64, 4..4048)
        ) {
            if !std::is_x86_feature_detected!("avx") {
                return Ok(());
            }
            let scalar = dot_f64_scalar(&x, &y);
            let avx = unsafe { x86::dot_f64_avx(&x, &y) };
            let max_error = max_error::<f64>(&x, &y);
            prop_assert!(approx::relative_eq!(scalar, avx, epsilon = max_error));
        }
    }

    /// An empty batch with a valid dimension, e.g. no centroids, writes
    /// nothing and does not panic. 8 and 16 take the fixed-length paths.
    #[rstest::rstest]
    #[case::dim_8(8)]
    #[case::dim_16(16)]
    #[case::dim_13(13)]
    fn test_dot_batch_into_accepts_empty_batch(#[case] dimension: usize) {
        fn accepts<T: Dot + FromPrimitive + Copy>(dimension: usize) {
            let x = vec![T::from_f32(1.0).unwrap(); dimension];
            dot_distance_batch_into::<T>(&x, &[], dimension, &mut []);
        }
        accepts::<f16>(dimension);
        accepts::<f32>(dimension);
        accepts::<f64>(dimension);
    }

    /// `dot_batch_into` must agree with the per-vector `dot`. The f32 batch
    /// kernel may differ in rounding; the f64 batch loops run the pair
    /// kernel's arithmetic, so f64 must match bit for bit.
    #[rstest::rstest]
    #[case::dim_8(8)]
    #[case::dim_12(12)]
    #[case::dim_13(13)]
    #[case::dim_16(16)]
    #[case::dim_32(32)]
    #[case::dim_1024(1024)]
    fn test_dot_batch_into_matches_per_vector_dot(#[case] dimension: usize) {
        fn check<T: Dot + FromPrimitive + Copy>(dimension: usize, bit_exact: bool) {
            let num_vectors = 130;
            // Values that are not multiples of a power of two, so the sums
            // round and a different summation order changes the result.
            let x: Vec<T> = (0..dimension)
                .map(|i| T::from_f32((i as f32 * 0.37).sin()).unwrap())
                .collect();
            let batch: Vec<T> = (0..dimension * num_vectors)
                .map(|i| T::from_f32((i as f32 * 0.73).cos() * 2.0).unwrap())
                .collect();

            let mut got = vec![f32::NAN; num_vectors];
            T::dot_batch_into(&x, &batch, dimension, &mut got);

            for (g, y) in got.iter().zip(batch.chunks_exact(dimension)) {
                let w = T::dot(&x, y);
                if bit_exact {
                    assert_eq!(
                        g.to_bits(),
                        w.to_bits(),
                        "dim {dimension}: batch {g} != per-vector {w}"
                    );
                } else {
                    assert!(
                        approx::relative_eq!(*g, w, epsilon = 1e-4, max_relative = 1e-5),
                        "dim {dimension}: batch {g} != per-vector {w}"
                    );
                }
            }
        }
        check::<f32>(dimension, false);
        check::<f64>(dimension, true);
    }

    /// `dot_distance_batch_into` writes `1.0 - dot`.
    #[test]
    fn test_dot_distance_batch_into_preserves_distance_semantics() {
        let dimension = 32;
        let x: Vec<f32> = (0..dimension).map(|i| (i as f32) * 0.1).collect();
        let batch: Vec<f32> = (0..dimension * 3).map(|i| (i as f32) * 0.05).collect();

        let mut got = vec![f32::NAN; 3];
        dot_distance_batch_into(&x, &batch, dimension, &mut got);
        for (chunk, &g) in batch.chunks_exact(dimension).zip(got.iter()) {
            assert!(approx::relative_eq!(
                g,
                1.0 - f32::dot(&x, chunk),
                epsilon = 1e-5
            ));
        }
    }
}
