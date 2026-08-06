// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Dot product.

use std::iter::Sum;
use std::ops::AddAssign;
use std::sync::Arc;

use crate::Error;
use arrow_array::types::{Float16Type, Float64Type, Int8Type};
use arrow_array::{Array, FixedSizeListArray, Float32Array, cast::AsArray, types::Float32Type};
use arrow_schema::DataType;
use half::{bf16, f16};
use lance_arrow::{ArrowFloatType, FixedSizeListArrayExt, FloatArray};
use lance_core::assume_eq;
#[allow(unused_imports)]
use lance_core::utils::cpu::{SIMD_SUPPORT, SimdSupport};
use num_traits::{AsPrimitive, Num, real::Real};

use crate::Result;
#[cfg(all(
    target_arch = "x86_64",
    not(all(target_feature = "avx2", target_feature = "fma"))
))]
use crate::distance::BatchIter;

/// Default implementation of dot product.
///
// The following code has been tuned for auto-vectorization.
// Please make sure run `cargo bench --bench dot` with and without AVX-512 before any change.
// Tested `target-features`: avx512f,avx512vl,f16c
#[inline]
fn dot_scalar<
    T: AsPrimitive<Output>,
    Output: Real + Sum + AddAssign + 'static,
    const LANES: usize,
>(
    from: &[T],
    to: &[T],
) -> Output {
    let x_chunks = to.chunks_exact(LANES);
    let y_chunks = from.chunks_exact(LANES);
    let sum = if x_chunks.remainder().is_empty() {
        Output::zero()
    } else {
        x_chunks
            .remainder()
            .iter()
            .zip(y_chunks.remainder().iter())
            .map(|(&x, &y)| x.as_() * y.as_())
            .sum::<Output>()
    };
    // Use known size to allow LLVM to kick in auto-vectorization.
    let mut sums = [Output::zero(); LANES];
    for (x, y) in x_chunks.zip(y_chunks) {
        for i in 0..LANES {
            sums[i] += x[i].as_() * y[i].as_();
        }
    }
    sum + sums.iter().copied().sum::<Output>()
}

/// Dot product.
#[inline]
pub fn dot<T: Dot>(from: &[T], to: &[T]) -> f32 {
    T::dot(from, to)
}

/// Dot product between two f32 slices, dispatched to the widest SIMD backend
/// available at runtime. See [`crate::distance::l2::l2_f32`] for why this is
/// needed on top of the generic [`dot`].
#[inline]
pub fn dot_f32(x: &[f32], y: &[f32]) -> f32 {
    #[cfg(target_arch = "x86_64")]
    {
        use lance_core::utils::cpu::SimdSupport;
        if matches!(*SIMD_SUPPORT, SimdSupport::Avx512 | SimdSupport::Avx512FP16) {
            // SAFETY: guarded by the runtime AVX-512 detection above.
            return unsafe { dot_f32_avx512(x, y) };
        }
    }
    dot(x, y)
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f")]
unsafe fn dot_f32_avx512(x: &[f32], y: &[f32]) -> f32 {
    use std::arch::x86_64::*;
    debug_assert_eq!(x.len(), y.len());
    let n = x.len();
    let mut acc = _mm512_setzero_ps();
    let mut i = 0usize;
    while i + 16 <= n {
        let a = _mm512_loadu_ps(x.as_ptr().add(i));
        let b = _mm512_loadu_ps(y.as_ptr().add(i));
        acc = _mm512_fmadd_ps(a, b, acc);
        i += 16;
    }
    let mut sum = _mm512_reduce_add_ps(acc);
    while i < n {
        sum += x[i] * y[i];
        i += 1;
    }
    sum
}

/// Negative [Dot] distance.
#[inline]
pub fn dot_distance<T: Dot>(from: &[T], to: &[T]) -> f32 {
    1.0 - T::dot(from, to)
}

/// Dot product
pub trait Dot: Num {
    /// Dot product.
    fn dot(x: &[Self], y: &[Self]) -> f32;

    /// Dot product of `x` against each `dimension`-sized vector in `batch`.
    ///
    /// The default calls [`Dot::dot`] per vector. `f32` overrides it so the
    /// SIMD tier is chosen once for the whole batch instead of once per
    /// vector — on a build whose baseline already implies AVX2, per-vector
    /// dispatch costs more than the kernel it selects.
    ///
    /// Returns `impl Iterator` rather than a trait object: hot consumers drive
    /// this one element at a time, so a `Box<dyn Iterator>` would cost a
    /// virtual call per element and an allocation per batch.
    fn dot_batch<'a>(
        x: &'a [Self],
        batch: &'a [Self],
        dimension: usize,
    ) -> impl Iterator<Item = f32> + 'a {
        batch.chunks_exact(dimension).map(move |y| Self::dot(x, y))
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
        match *SIMD_SUPPORT {
            #[cfg(all(feature = "fp16kernels", target_arch = "aarch64"))]
            SimdSupport::Neon => unsafe {
                bf16_kernel::dot_bf16_neon(x.as_ptr(), y.as_ptr(), x.len() as u32)
            },
            #[cfg(all(
                feature = "fp16kernels",
                kernel_support = "avx512_bf16",
                target_arch = "x86_64"
            ))]
            SimdSupport::Avx512FP16 => unsafe {
                bf16_kernel::dot_bf16_avx512(x.as_ptr(), y.as_ptr(), x.len() as u32)
            },
            #[cfg(all(feature = "fp16kernels", target_arch = "x86_64"))]
            SimdSupport::Avx2 | SimdSupport::Avx512 => unsafe {
                bf16_kernel::dot_bf16_avx2(x.as_ptr(), y.as_ptr(), x.len() as u32)
            },
            #[cfg(all(feature = "fp16kernels", target_arch = "loongarch64"))]
            SimdSupport::Lasx => unsafe {
                bf16_kernel::dot_bf16_lasx(x.as_ptr(), y.as_ptr(), x.len() as u32)
            },
            #[cfg(all(feature = "fp16kernels", target_arch = "loongarch64"))]
            SimdSupport::Lsx => unsafe {
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
        match *SIMD_SUPPORT {
            #[cfg(all(feature = "fp16kernels", target_arch = "aarch64"))]
            SimdSupport::Neon => unsafe {
                kernel::dot_f16_neon(x.as_ptr(), y.as_ptr(), x.len() as u32)
            },
            #[cfg(all(
                feature = "fp16kernels",
                kernel_support = "avx512_f16",
                target_arch = "x86_64"
            ))]
            SimdSupport::Avx512FP16 => unsafe {
                kernel::dot_f16_avx512(x.as_ptr(), y.as_ptr(), x.len() as u32)
            },
            #[cfg(all(feature = "fp16kernels", target_arch = "x86_64"))]
            SimdSupport::Avx2 | SimdSupport::Avx512 => unsafe {
                kernel::dot_f16_avx2(x.as_ptr(), y.as_ptr(), x.len() as u32)
            },
            #[cfg(all(feature = "fp16kernels", target_arch = "loongarch64"))]
            SimdSupport::Lasx => unsafe {
                kernel::dot_f16_lasx(x.as_ptr(), y.as_ptr(), x.len() as u32)
            },
            #[cfg(all(feature = "fp16kernels", target_arch = "loongarch64"))]
            SimdSupport::Lsx => unsafe {
                kernel::dot_f16_lsx(x.as_ptr(), y.as_ptr(), x.len() as u32)
            },
            // SimdSupport::AvxFma and SimdSupport::Avx fall through here:
            // the f16 C kernels are compiled with `-march=haswell` minimum
            // (AVX2), so they cannot run on AVX-only or AVX+FMA hosts.
            _ => dot_scalar::<Self, f32, 32>(x, y),
        }
    }
}

impl Dot for f32 {
    #[inline]
    fn dot(x: &[Self], y: &[Self]) -> f32 {
        // Trait methods cannot carry `#[target_feature]` attributes, so the body
        // lives in a free function that runtime-dispatches via `*SIMD_SUPPORT`
        // to an AVX2 or AVX-512 inner kernel on capable hosts, or a portable
        // scalar fallback. Same shape as the f64 sibling and the existing
        // u8 distance kernels in `dot_u8.rs`.
        dot_f32_dispatched(x, y)
    }

    fn dot_batch<'a>(
        x: &'a [Self],
        batch: &'a [Self],
        dimension: usize,
    ) -> impl Iterator<Item = Self> + 'a {
        // Exactly one arm compiles. Keeping each a tail expression (rather than
        // an early `return` guarded by `cfg`) mirrors `dot_f32_dispatched` and
        // avoids an unreachable tail on AVX2-baseline builds.
        // AVX2-baseline build (the default `haswell` wheel). Hoist the tier
        // choice out of the loop, but keep the SIMD kernel: the baseline already
        // guarantees avx2+fma, so call the AVX+FMA kernel directly rather than
        // re-checking per vector. Falling back to the scalar kernel here would
        // lose ~4x at small dimensions, which is where batch calls live (PQ
        // sub-vectors are 8 wide).
        //
        // The iterator is a bare `Map`: `Map<ChunksExact, _>` is `TrustedLen`,
        // so `.collect()` preallocates, and `Map::fold` drives `ChunksExact` in
        // one inlined loop. Any wrapper — trait object or enum — loses both.
        #[cfg(all(
            target_arch = "x86_64",
            target_feature = "avx2",
            target_feature = "fma"
        ))]
        {
            // See `L2::l2_batch` for f32: below 16 lanes `dot_scalar`'s chunking
            // degenerates to a scalar remainder loop, so the explicit AVX kernel
            // wins big; above it the autovectorizer is already good and the
            // 8-wide kernel can lose, so keep the pre-dispatch kernel exactly.
            //
            // SAFETY: avx2+fma are enabled for the whole crate by the build
            // baseline, so the kernel's `#[target_feature]` contract holds
            // statically.
            let narrow = dimension <= 16;
            batch.chunks_exact(dimension).map(move |y| {
                if narrow {
                    unsafe { x86::dot_f32_avx_fma(x, y) }
                } else {
                    dot_f32_scalar(x, y)
                }
            })
        }
        #[cfg(all(
            target_arch = "x86_64",
            not(all(target_feature = "avx2", target_feature = "fma"))
        ))]
        {
            dot_batch_f32_runtime_dispatch(x, batch, dimension)
        }
        #[cfg(not(target_arch = "x86_64"))]
        {
            batch.chunks_exact(dimension).map(move |y| Self::dot(x, y))
        }
    }
}

/// Sub-AVX2 builds: the scalar kernel cannot reach the wide registers, so pick
/// a `#[target_feature]` kernel — once for the batch, not once per vector.
#[cfg(all(
    target_arch = "x86_64",
    not(all(target_feature = "avx2", target_feature = "fma"))
))]
#[inline]
fn dot_batch_f32_runtime_dispatch<'a>(
    x: &'a [f32],
    batch: &'a [f32],
    dimension: usize,
) -> impl Iterator<Item = f32> + 'a {
    // SAFETY: each kernel is entered only under its matching runtime detection.
    match *SIMD_SUPPORT {
        SimdSupport::Avx512 | SimdSupport::Avx512FP16 => {
            BatchIter::Eager(unsafe { x86::dot_batch_f32_avx512(x, batch, dimension) }.into_iter())
        }
        SimdSupport::Avx2 | SimdSupport::AvxFma => {
            BatchIter::Eager(unsafe { x86::dot_batch_f32_avx_fma(x, batch, dimension) }.into_iter())
        }
        SimdSupport::Avx => {
            BatchIter::Eager(unsafe { x86::dot_batch_f32_avx(x, batch, dimension) }.into_iter())
        }
        _ => BatchIter::Lazy(
            batch
                .chunks_exact(dimension)
                .map(move |y| dot_f32_scalar(x, y)),
        ),
    }
}

/// Dot product for f32, runtime-dispatched via `SIMD_SUPPORT` on x86_64
/// (AVX-512 / AVX2+FMA / AVX+FMA / AVX / scalar). Non-x86 uses the
/// auto-vectorised scalar loop.
#[inline]
fn dot_f32_dispatched(x: &[f32], y: &[f32]) -> f32 {
    #[cfg(target_arch = "x86_64")]
    {
        match *SIMD_SUPPORT {
            SimdSupport::Avx512 | SimdSupport::Avx512FP16 => unsafe { x86::dot_f32_avx512(x, y) },
            SimdSupport::Avx2 | SimdSupport::AvxFma => unsafe { x86::dot_f32_avx_fma(x, y) },
            SimdSupport::Avx => unsafe { x86::dot_f32_avx(x, y) },
            _ => dot_f32_scalar(x, y),
        }
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        dot_f32_scalar(x, y)
    }
}

/// Portable scalar dot product for f32. Used as the x86_64 fallback when no
/// AVX2 is detected, and as the only path on non-x86 architectures. The
/// `LANES = 16` chunking matches the explicit-SIMD inner kernels above.
#[inline]
fn dot_f32_scalar(x: &[f32], y: &[f32]) -> f32 {
    dot_scalar::<f32, f32, 16>(x, y)
}

impl Dot for f64 {
    #[inline]
    fn dot(x: &[Self], y: &[Self]) -> f32 {
        dot_f64_simd(x, y)
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

    use crate::simd::f64::{f64x4, f64x8};
    use crate::simd::x86::hsum256_ps;
    use crate::simd::{FloatSimd, SIMD};

    /// Dot product of `x` against every `dimension`-sized vector in `batch`,
    /// entering the AVX-512 tier once for the whole batch rather than once per
    /// vector.
    ///
    /// # Safety
    /// The host must support AVX-512F.
    ///
    /// Only compiled for builds whose baseline is below avx2+fma; at or above
    /// that baseline `dot_batch` inlines the kernel directly and never runtime-
    /// dispatches, so this wrapper would be dead code (see `dot_batch`).
    #[cfg(not(all(target_feature = "avx2", target_feature = "fma")))]
    #[target_feature(enable = "avx512f")]
    pub(super) unsafe fn dot_batch_f32_avx512(
        x: &[f32],
        batch: &[f32],
        dimension: usize,
    ) -> Vec<f32> {
        batch
            .chunks_exact(dimension)
            .map(|y| unsafe { dot_f32_avx512(x, y) })
            .collect()
    }

    /// As [`dot_batch_f32_avx512`], for the AVX2 and AVX+FMA tiers.
    ///
    /// # Safety
    /// The host must support AVX and FMA.
    #[cfg(not(all(target_feature = "avx2", target_feature = "fma")))]
    #[target_feature(enable = "avx,fma")]
    pub(super) unsafe fn dot_batch_f32_avx_fma(
        x: &[f32],
        batch: &[f32],
        dimension: usize,
    ) -> Vec<f32> {
        batch
            .chunks_exact(dimension)
            .map(|y| unsafe { dot_f32_avx_fma(x, y) })
            .collect()
    }

    /// As [`dot_batch_f32_avx512`], for the AVX-without-FMA tier.
    ///
    /// # Safety
    /// The host must support AVX.
    #[cfg(not(all(target_feature = "avx2", target_feature = "fma")))]
    #[target_feature(enable = "avx")]
    pub(super) unsafe fn dot_batch_f32_avx(x: &[f32], batch: &[f32], dimension: usize) -> Vec<f32> {
        batch
            .chunks_exact(dimension)
            .map(|y| unsafe { dot_f32_avx(x, y) })
            .collect()
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

    /// AVX-512 path for f32: 16-wide `__m512` with `vfmadd231ps` per iteration.
    #[target_feature(enable = "avx512f")]
    pub unsafe fn dot_f32_avx512(x: &[f32], y: &[f32]) -> f32 {
        let dim = x.len();
        let unrolled_len = dim / 16 * 16;

        let mut acc = _mm512_setzero_ps();
        for i in (0..unrolled_len).step_by(16) {
            let a = _mm512_loadu_ps(x.as_ptr().add(i));
            let b = _mm512_loadu_ps(y.as_ptr().add(i));
            acc = _mm512_fmadd_ps(a, b, acc);
        }

        let tail: f32 = x[unrolled_len..]
            .iter()
            .zip(y[unrolled_len..].iter())
            .map(|(&a, &b)| a * b)
            .sum();

        _mm512_reduce_add_ps(acc) + tail
    }

    /// AVX + FMA path for f32. Covers both AvxFma and AVX2 dispatch (body uses no AVX2-specific intrinsics).
    #[target_feature(enable = "avx,fma")]
    pub unsafe fn dot_f32_avx_fma(x: &[f32], y: &[f32]) -> f32 {
        let dim = x.len();
        let unrolled_len = dim / 8 * 8;

        let mut acc = _mm256_setzero_ps();
        for i in (0..unrolled_len).step_by(8) {
            let a = _mm256_loadu_ps(x.as_ptr().add(i));
            let b = _mm256_loadu_ps(y.as_ptr().add(i));
            acc = _mm256_fmadd_ps(a, b, acc);
        }

        let tail: f32 = x[unrolled_len..]
            .iter()
            .zip(y[unrolled_len..].iter())
            .map(|(&a, &b)| a * b)
            .sum();

        hsum256_ps(acc) + tail
    }

    /// AVX-only path for f32 (no FMA): `_mm256_mul_ps` + `_mm256_add_ps` per iteration for Sandy/Ivy Bridge.
    #[target_feature(enable = "avx")]
    pub unsafe fn dot_f32_avx(x: &[f32], y: &[f32]) -> f32 {
        let dim = x.len();
        let unrolled_len = dim / 8 * 8;

        let mut acc = _mm256_setzero_ps();
        for i in (0..unrolled_len).step_by(8) {
            let a = _mm256_loadu_ps(x.as_ptr().add(i));
            let b = _mm256_loadu_ps(y.as_ptr().add(i));
            acc = _mm256_add_ps(acc, _mm256_mul_ps(a, b));
        }

        let tail: f32 = x[unrolled_len..]
            .iter()
            .zip(y[unrolled_len..].iter())
            .map(|(&a, &b)| a * b)
            .sum();

        hsum256_ps(acc) + tail
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
        super::dot_u8::dot_u8(x, y) as f32
    }
}

/// Negative dot product, to present the relative order of dot distance.
pub fn dot_distance_batch<'a, T: Dot>(
    from: &'a [T],
    to: &'a [T],
    dimension: usize,
) -> Box<dyn Iterator<Item = f32> + 'a> {
    assume_eq!(from.len(), dimension);
    assume_eq!(to.len() % dimension, 0);
    Box::new(T::dot_batch(from, to, dimension).map(|d| 1.0 - d))
}

fn do_dot_distance_arrow_batch<T: ArrowFloatType>(
    from: &T::ArrayType,
    to: &FixedSizeListArray,
) -> Result<Arc<Float32Array>>
where
    T::Native: Dot,
{
    let dimension = to.value_length() as usize;
    debug_assert_eq!(from.len(), dimension);

    // TODO: if we detect there is a run of nulls, should we skip those?
    let to_values =
        to.values()
            .as_any()
            .downcast_ref::<T::ArrayType>()
            .ok_or(Error::InvalidArgumentError(format!(
                "Invalid type: expect {:?} got {:?}",
                from.data_type(),
                to.value_type()
            )))?;

    // Route through `dot_distance_batch` rather than mapping `dot_distance` per
    // vector, so this entry point gets the same hoisted dispatch.
    let dists = dot_distance_batch(from.as_slice(), to_values.as_slice(), dimension);

    Ok(Arc::new(Float32Array::new(
        dists.collect(),
        to.nulls().cloned(),
    )))
}

/// Compute negative dot product distance between a vector and a batch of vectors.
///
/// Null buffer of `to` is propagated to the returned array.
///
/// Parameters
///
/// - `from`: the vector to compute distance from.
/// - `to`: a list of vectors to compute distance to.
///
/// # Panics
///
/// Panics if the length of `from` is not equal to the dimension (value length) of `to`.
pub fn dot_distance_arrow_batch(
    from: &dyn Array,
    to: &FixedSizeListArray,
) -> Result<Arc<Float32Array>> {
    let dimension = to.value_length() as usize;
    debug_assert_eq!(from.len(), dimension);

    match *from.data_type() {
        DataType::Float16 => do_dot_distance_arrow_batch::<Float16Type>(from.as_primitive(), to),
        DataType::Float32 => do_dot_distance_arrow_batch::<Float32Type>(from.as_primitive(), to),
        DataType::Float64 => do_dot_distance_arrow_batch::<Float64Type>(from.as_primitive(), to),
        DataType::Int8 => do_dot_distance_arrow_batch::<Float32Type>(
            &from
                .as_primitive::<Int8Type>()
                .into_iter()
                .map(|x| x.unwrap() as f32)
                .collect(),
            &to.convert_to_floating_point()?,
        ),
        _ => Err(Error::InvalidArgumentError(format!(
            "Unsupported data type: {:?}",
            from.data_type()
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::{
        arbitrary_bf16, arbitrary_f16, arbitrary_f32, arbitrary_f64, arbitrary_vector_pair,
    };
    use num_traits::{Float, FromPrimitive};
    use proptest::prelude::*;

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

    proptest::proptest! {
        #[test]
        fn test_dot_f16((x, y) in arbitrary_vector_pair(arbitrary_f16, 4..4048)) {
            do_dot_test(&x, &y)?;
        }

        #[test]
        fn test_dot_bf16((x, y) in arbitrary_vector_pair(arbitrary_bf16, 4..4048)){
            do_dot_test(&x, &y)?;
        }

        #[test]
        fn test_dot_f32((x, y) in arbitrary_vector_pair(arbitrary_f32, 4..4048)){
            do_dot_test(&x, &y)?;
        }

        #[test]
        fn test_dot_f64((x, y) in arbitrary_vector_pair(arbitrary_f64, 4..4048)){
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

        /// Parity check for `dot_f32_dispatched` (Branch B exclusive: the
        /// auto-vectorised scalar dot path). The dispatched kernel must
        /// agree with a portable f64-precision scalar reference within
        /// numerical tolerance. The reference is hand-rolled here to keep
        /// this test architecture-agnostic (the x86_64-only `dot_f64_scalar`
        /// helper is gated above).
        #[test]
        fn test_dot_f32_scalar_simd_parity(
            (x, y) in arbitrary_vector_pair(arbitrary_f32, 4..4048)
        ) {
            let x_f64: Vec<f64> = x.iter().map(|&v| v as f64).collect();
            let y_f64: Vec<f64> = y.iter().map(|&v| v as f64).collect();
            let scalar = x_f64
                .iter()
                .zip(y_f64.iter())
                .map(|(&a, &b)| a * b)
                .sum::<f64>() as f32;
            let simd = <f32 as Dot>::dot(&x, &y);
            let max_error = max_error::<f32>(&x_f64, &y_f64);
            prop_assert!(approx::relative_eq!(scalar, simd, epsilon = max_error));
        }

        /// AVX-512-direct parity for f32: explicitly compares the scalar
        /// fallback against the native f32 AVX-512 inner kernel on
        /// AVX-512F-capable hosts. Early-returns on hosts without AVX-512F.
        #[cfg(target_arch = "x86_64")]
        #[test]
        fn test_dot_f32_scalar_vs_avx512_parity(
            (x, y) in arbitrary_vector_pair(arbitrary_f32, 4..4048)
        ) {
            if !std::is_x86_feature_detected!("avx512f") {
                return Ok(());
            }
            let scalar = dot_f32_scalar(&x, &y);
            let avx512 = unsafe { x86::dot_f32_avx512(&x, &y) };
            let x_f64: Vec<f64> = x.iter().map(|&v| v as f64).collect();
            let y_f64: Vec<f64> = y.iter().map(|&v| v as f64).collect();
            let max_error = max_error::<f32>(&x_f64, &y_f64);
            prop_assert!(approx::relative_eq!(scalar, avx512, epsilon = max_error));
        }

        /// AVX + FMA-direct parity for the f32 dot kernel. Covers the AMD
        /// Piledriver / Steamroller / FX-7500 tier. Early-returns on hosts
        /// without both AVX and FMA.
        #[cfg(target_arch = "x86_64")]
        #[test]
        fn test_dot_f32_scalar_vs_avx_fma_parity(
            (x, y) in arbitrary_vector_pair(arbitrary_f32, 4..4048)
        ) {
            if !(std::is_x86_feature_detected!("avx") && std::is_x86_feature_detected!("fma")) {
                return Ok(());
            }
            let scalar = dot_f32_scalar(&x, &y);
            let avx_fma = unsafe { x86::dot_f32_avx_fma(&x, &y) };
            let x_f64: Vec<f64> = x.iter().map(|&v| v as f64).collect();
            let y_f64: Vec<f64> = y.iter().map(|&v| v as f64).collect();
            let max_error = max_error::<f32>(&x_f64, &y_f64);
            prop_assert!(approx::relative_eq!(scalar, avx_fma, epsilon = max_error));
        }

        /// AVX-only-direct parity for the f32 dot kernel. Covers the Intel
        /// Sandy Bridge / Ivy Bridge tier. Early-returns on hosts without
        /// AVX.
        #[cfg(target_arch = "x86_64")]
        #[test]
        fn test_dot_f32_scalar_vs_avx_parity(
            (x, y) in arbitrary_vector_pair(arbitrary_f32, 4..4048)
        ) {
            if !std::is_x86_feature_detected!("avx") {
                return Ok(());
            }
            let scalar = dot_f32_scalar(&x, &y);
            let avx = unsafe { x86::dot_f32_avx(&x, &y) };
            let x_f64: Vec<f64> = x.iter().map(|&v| v as f64).collect();
            let y_f64: Vec<f64> = y.iter().map(|&v| v as f64).collect();
            let max_error = max_error::<f32>(&x_f64, &y_f64);
            prop_assert!(approx::relative_eq!(scalar, avx, epsilon = max_error));
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

    /// `dot_batch` must agree with the per-vector `dot` it replaced, on every
    /// build: AVX2-baseline, hoisted-dispatch, and portable fallback all
    /// funnel through here.
    #[rstest::rstest]
    #[case::dim_8(8)]
    #[case::dim_16(16)]
    #[case::dim_32(32)]
    #[case::dim_1024(1024)]
    fn test_dot_batch_f32_matches_per_vector_dot(#[case] dimension: usize) {
        let num_vectors = 5;
        let x: Vec<f32> = (0..dimension)
            .map(|i| ((i % 13) as f32) * 0.25 + 1.0)
            .collect();
        let batch: Vec<f32> = (0..dimension * num_vectors)
            .map(|i| ((i % 11) as f32) * 0.5 - 2.0)
            .collect();

        let got: Vec<f32> = f32::dot_batch(&x, &batch, dimension).collect();
        let want: Vec<f32> = batch
            .chunks_exact(dimension)
            .map(|y| f32::dot(&x, y))
            .collect();

        assert_eq!(got.len(), num_vectors);
        for (g, w) in got.iter().zip(want.iter()) {
            assert!(
                approx::relative_eq!(g, w, epsilon = 1e-4),
                "dim {dimension}: batch {g} != per-vector {w}"
            );
        }
    }

    /// `dot_distance_batch` still yields `1.0 - dot`, unchanged by the hoist.
    #[test]
    fn test_dot_distance_batch_preserves_distance_semantics() {
        let dimension = 32;
        let x: Vec<f32> = (0..dimension).map(|i| (i as f32) * 0.1).collect();
        let batch: Vec<f32> = (0..dimension * 3).map(|i| (i as f32) * 0.05).collect();

        let got: Vec<f32> = dot_distance_batch(&x, &batch, dimension).collect();
        for (chunk, &g) in batch.chunks_exact(dimension).zip(got.iter()) {
            assert!(approx::relative_eq!(
                g,
                1.0 - f32::dot(&x, chunk),
                epsilon = 1e-5
            ));
        }
    }

    /// The per-batch `#[target_feature]` kernels are only reached on sub-AVX2
    /// builds or AVX-512 hosts, so call them directly to cover them.
    #[cfg(all(
        target_arch = "x86_64",
        not(all(target_feature = "avx2", target_feature = "fma"))
    ))]
    fn check_dot_batch_kernel(kernel: unsafe fn(&[f32], &[f32], usize) -> Vec<f32>) {
        for dimension in [8_usize, 16, 40] {
            let num_vectors = 3;
            let x: Vec<f32> = (0..dimension).map(|i| (i as f32) * 0.5 + 1.0).collect();
            let batch: Vec<f32> = (0..dimension * num_vectors)
                .map(|i| ((i % 7) as f32) + 1.0)
                .collect();

            let got = unsafe { kernel(&x, &batch, dimension) };
            assert_eq!(got.len(), num_vectors);
            for (chunk, &g) in batch.chunks_exact(dimension).zip(got.iter()) {
                let want = dot_scalar::<f32, f32, 16>(&x, chunk);
                assert!(
                    approx::relative_eq!(g, want, epsilon = 1e-4),
                    "dim {dimension}: kernel {g} != scalar {want}"
                );
            }
        }
    }

    // The runtime-dispatch batch kernels only exist in sub-avx2+fma builds
    // (see `x86::dot_batch_f32_avx512`), so gate their tests the same way.
    #[cfg(all(
        target_arch = "x86_64",
        not(all(target_feature = "avx2", target_feature = "fma"))
    ))]
    #[test]
    fn test_dot_batch_avx_fma_matches_scalar() {
        if !std::is_x86_feature_detected!("avx") || !std::is_x86_feature_detected!("fma") {
            return;
        }
        check_dot_batch_kernel(x86::dot_batch_f32_avx_fma);
    }

    #[cfg(all(
        target_arch = "x86_64",
        not(all(target_feature = "avx2", target_feature = "fma"))
    ))]
    #[test]
    fn test_dot_batch_avx_matches_scalar() {
        if !std::is_x86_feature_detected!("avx") {
            return;
        }
        check_dot_batch_kernel(x86::dot_batch_f32_avx);
    }

    #[cfg(all(
        target_arch = "x86_64",
        not(all(target_feature = "avx2", target_feature = "fma"))
    ))]
    #[test]
    fn test_dot_batch_avx512_matches_scalar() {
        if !std::is_x86_feature_detected!("avx512f") {
            return;
        }
        check_dot_batch_kernel(x86::dot_batch_f32_avx512);
    }
}
