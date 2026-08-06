// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! L2 (Euclidean) distance.
//!

use std::iter::Sum;
use std::ops::AddAssign;
use std::sync::Arc;

use crate::{Error, Result};
use arrow_array::{
    Array, FixedSizeListArray, Float32Array,
    cast::AsArray,
    types::{Float16Type, Float32Type, Float64Type, Int8Type},
};
use arrow_schema::DataType;
use half::{bf16, f16};
use lance_arrow::{ArrowFloatType, FixedSizeListArrayExt, FloatArray};
use lance_core::assume_eq;
use lance_core::deepsize::DeepSizeOf;
use lance_core::utils::cpu::SIMD_SUPPORT;
// Named tiers are only matched on x86_64, or by the fp16 kernels on the other
// architectures; without either, nothing below names a `SimdSupport` variant.
#[cfg(any(feature = "fp16kernels", target_arch = "x86_64"))]
use lance_core::utils::cpu::SimdSupport;
use num_traits::{AsPrimitive, Num};

#[cfg(all(
    target_arch = "x86_64",
    not(all(target_feature = "avx2", target_feature = "fma"))
))]
use crate::distance::BatchIter;

/// Calculate the L2 distance between two vectors.
///
pub trait L2: Num {
    /// Calculate the L2 distance between two vectors.
    fn l2(x: &[Self], y: &[Self]) -> f32;

    /// L2 distance from `x` to each `dimension`-sized vector in `y`.
    ///
    /// The default calls [`L2::l2`] per vector. `f32` overrides it so the SIMD
    /// tier is chosen once for the whole batch instead of once per vector —
    /// on a build whose baseline already implies AVX2, per-vector dispatch
    /// costs more than the kernel it selects.
    ///
    /// Returns `impl Iterator` rather than a trait object: the k-means
    /// assignment loop drives this one element at a time, so a
    /// `Box<dyn Iterator>` would cost a virtual call per element and an
    /// allocation per batch.
    fn l2_batch<'a>(
        x: &'a [Self],
        y: &'a [Self],
        dimension: usize,
    ) -> impl Iterator<Item = f32> + 'a {
        y.chunks_exact(dimension).map(move |v| Self::l2(x, v))
    }
}

#[inline]
pub fn l2<T: L2>(from: &[T], to: &[T]) -> f32 {
    T::l2(from, to)
}

/// L2 distance between two f32 slices, dispatched to the widest SIMD backend
/// available at runtime.
///
/// On x86_64 with AVX-512 this uses 16-wide f32 lanes; otherwise it falls back
/// to [`l2`], which auto-vectorizes to the compiled target (AVX2 on the default
/// `haswell` build). Lance ships an AVX2-baseline binary, so the generic
/// [`l2`] never emits AVX-512 even on capable CPUs — this dispatcher recovers
/// that throughput for callers in the hot path (e.g. the in-memory HNSW index).
#[inline]
pub fn l2_f32(x: &[f32], y: &[f32]) -> f32 {
    #[cfg(target_arch = "x86_64")]
    {
        if matches!(*SIMD_SUPPORT, SimdSupport::Avx512 | SimdSupport::Avx512FP16) {
            // SAFETY: guarded by the runtime AVX-512 detection above.
            return unsafe { l2_f32_avx512(x, y) };
        }
    }
    l2(x, y)
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f")]
unsafe fn l2_f32_avx512(x: &[f32], y: &[f32]) -> f32 {
    use std::arch::x86_64::*;
    debug_assert_eq!(x.len(), y.len());
    let n = x.len();
    let mut acc = _mm512_setzero_ps();
    let mut i = 0usize;
    while i + 16 <= n {
        let a = _mm512_loadu_ps(x.as_ptr().add(i));
        let b = _mm512_loadu_ps(y.as_ptr().add(i));
        let diff = _mm512_sub_ps(a, b);
        acc = _mm512_fmadd_ps(diff, diff, acc);
        i += 16;
    }
    let mut sum = _mm512_reduce_add_ps(acc);
    while i < n {
        let diff = x[i] - y[i];
        sum += diff * diff;
        i += 1;
    }
    sum
}

/// Calculate L2 distance between two uint8 slices.
#[inline]
pub fn l2_distance_uint_scalar(key: &[u8], target: &[u8]) -> f32 {
    key.iter()
        .zip(target.iter())
        .map(|(&x, &y)| (x.abs_diff(y) as u32).pow(2))
        .sum::<u32>() as f32
}

/// Calculate the L2 distance between two vectors, using scalar operations.
///
/// It relies on LLVM for auto-vectorization and unrolling.
///
/// This is pub for test/benchmark only. use [l2] instead.
#[inline]
pub fn l2_scalar<
    T: AsPrimitive<Output>,
    Output: Num + Copy + Sum + AddAssign + 'static,
    const LANES: usize,
>(
    from: &[T],
    to: &[T],
) -> Output {
    let x_chunks = from.chunks_exact(LANES);
    let y_chunks = to.chunks_exact(LANES);

    let s = if !x_chunks.remainder().is_empty() {
        x_chunks
            .remainder()
            .iter()
            .zip(y_chunks.remainder())
            .map(|(&x, &y)| {
                let diff = x.as_() - y.as_();
                diff * diff
            })
            .sum::<Output>()
    } else {
        Output::zero()
    };

    let mut sums = [Output::zero(); LANES];
    for (x, y) in x_chunks.zip(y_chunks) {
        for i in 0..LANES {
            let diff = x[i].as_() - y[i].as_();
            sums[i] += diff * diff;
        }
    }

    s + sums.iter().copied().sum()
}

impl L2 for u8 {
    #[inline]
    fn l2(x: &[Self], y: &[Self]) -> f32 {
        super::l2_u8::l2_u8(x, y) as f32
    }
}

#[cfg(feature = "fp16kernels")]
mod bf16_kernel {
    use half::bf16;

    // These are the `l2_bf16` function in bf16.c. Our build.rs script compiles
    // a version of this file for each SIMD level with different suffixes.
    unsafe extern "C" {
        #[cfg(target_arch = "aarch64")]
        pub fn l2_bf16_neon(ptr1: *const bf16, ptr2: *const bf16, len: u32) -> f32;
        #[cfg(all(kernel_support = "avx512_bf16", target_arch = "x86_64"))]
        pub fn l2_bf16_avx512(ptr1: *const bf16, ptr2: *const bf16, len: u32) -> f32;
        #[cfg(target_arch = "x86_64")]
        pub fn l2_bf16_avx2(ptr1: *const bf16, ptr2: *const bf16, len: u32) -> f32;
        #[cfg(target_arch = "loongarch64")]
        pub fn l2_bf16_lsx(ptr1: *const bf16, ptr2: *const bf16, len: u32) -> f32;
        #[cfg(target_arch = "loongarch64")]
        pub fn l2_bf16_lasx(ptr1: *const bf16, ptr2: *const bf16, len: u32) -> f32;
    }
}

impl L2 for bf16 {
    #[inline]
    fn l2(x: &[Self], y: &[Self]) -> f32 {
        match *SIMD_SUPPORT {
            #[cfg(all(feature = "fp16kernels", target_arch = "aarch64"))]
            SimdSupport::Neon => unsafe {
                bf16_kernel::l2_bf16_neon(x.as_ptr(), y.as_ptr(), x.len() as u32)
            },
            #[cfg(all(
                feature = "fp16kernels",
                kernel_support = "avx512_bf16",
                target_arch = "x86_64"
            ))]
            SimdSupport::Avx512FP16 => unsafe {
                bf16_kernel::l2_bf16_avx512(x.as_ptr(), y.as_ptr(), x.len() as u32)
            },
            #[cfg(all(feature = "fp16kernels", target_arch = "x86_64"))]
            SimdSupport::Avx2 | SimdSupport::Avx512 => unsafe {
                bf16_kernel::l2_bf16_avx2(x.as_ptr(), y.as_ptr(), x.len() as u32)
            },
            #[cfg(all(feature = "fp16kernels", target_arch = "loongarch64"))]
            SimdSupport::Lasx => unsafe {
                bf16_kernel::l2_bf16_lasx(x.as_ptr(), y.as_ptr(), x.len() as u32)
            },
            #[cfg(all(feature = "fp16kernels", target_arch = "loongarch64"))]
            SimdSupport::Lsx => unsafe {
                bf16_kernel::l2_bf16_lsx(x.as_ptr(), y.as_ptr(), x.len() as u32)
            },
            // SimdSupport::AvxFma and SimdSupport::Avx fall through here:
            // the bf16 C kernels are compiled with `-march=haswell` minimum
            // (AVX2), so they cannot run on AVX-only or AVX+FMA hosts.
            _ => l2_scalar::<Self, f32, 16>(x, y),
        }
    }
}

#[cfg(feature = "fp16kernels")]
mod kernel {
    use super::*;

    // These are the `l2_f16` function in f16.c. Our build.rs script compiles
    // a version of this file for each SIMD level with different suffixes.
    unsafe extern "C" {
        #[cfg(target_arch = "aarch64")]
        pub fn l2_f16_neon(ptr1: *const f16, ptr2: *const f16, len: u32) -> f32;
        #[cfg(all(kernel_support = "avx512_f16", target_arch = "x86_64"))]
        pub fn l2_f16_avx512(ptr1: *const f16, ptr2: *const f16, len: u32) -> f32;
        #[cfg(target_arch = "x86_64")]
        pub fn l2_f16_avx2(ptr1: *const f16, ptr2: *const f16, len: u32) -> f32;
        #[cfg(target_arch = "loongarch64")]
        pub fn l2_f16_lsx(ptr1: *const f16, ptr2: *const f16, len: u32) -> f32;
        #[cfg(target_arch = "loongarch64")]
        pub fn l2_f16_lasx(ptr1: *const f16, ptr2: *const f16, len: u32) -> f32;
    }
}

impl L2 for f16 {
    #[inline]
    fn l2(x: &[Self], y: &[Self]) -> f32 {
        match *SIMD_SUPPORT {
            #[cfg(all(feature = "fp16kernels", target_arch = "aarch64"))]
            SimdSupport::Neon => unsafe {
                kernel::l2_f16_neon(x.as_ptr(), y.as_ptr(), x.len() as u32)
            },
            #[cfg(all(
                feature = "fp16kernels",
                kernel_support = "avx512_f16",
                target_arch = "x86_64"
            ))]
            SimdSupport::Avx512FP16 => unsafe {
                kernel::l2_f16_avx512(x.as_ptr(), y.as_ptr(), x.len() as u32)
            },
            #[cfg(all(feature = "fp16kernels", target_arch = "x86_64"))]
            SimdSupport::Avx2 | SimdSupport::Avx512 => unsafe {
                kernel::l2_f16_avx2(x.as_ptr(), y.as_ptr(), x.len() as u32)
            },
            #[cfg(all(feature = "fp16kernels", target_arch = "loongarch64"))]
            SimdSupport::Lasx => unsafe {
                kernel::l2_f16_lasx(x.as_ptr(), y.as_ptr(), x.len() as u32)
            },
            #[cfg(all(feature = "fp16kernels", target_arch = "loongarch64"))]
            SimdSupport::Lsx => unsafe {
                kernel::l2_f16_lsx(x.as_ptr(), y.as_ptr(), x.len() as u32)
            },
            // SimdSupport::AvxFma and SimdSupport::Avx fall through here:
            // the f16 C kernels are compiled with `-march=haswell` minimum
            // (AVX2), so they cannot run on AVX-only or AVX+FMA hosts.
            _ => l2_scalar::<Self, f32, 16>(x, y),
        }
    }
}

impl L2 for f32 {
    #[inline]
    fn l2(x: &[Self], y: &[Self]) -> f32 {
        // Trait methods cannot carry `#[target_feature]` attributes, so the body
        // lives in a free function that runtime-dispatches via `*SIMD_SUPPORT`
        // to an AVX2 or AVX-512 inner kernel on capable hosts, or a portable
        // scalar fallback.
        l2_f32_dispatched(x, y)
    }

    fn l2_batch<'a>(
        x: &'a [Self],
        y: &'a [Self],
        dimension: usize,
    ) -> impl Iterator<Item = Self> + 'a {
        // Exactly one arm compiles; see `Dot::dot_batch` for f32.
        // See `Dot::dot_batch` for f32.
        #[cfg(all(
            target_arch = "x86_64",
            target_feature = "avx2",
            target_feature = "fma"
        ))]
        {
            // `l2_scalar::<_, _, 16>` chunks the vector by 16 lanes. At or below
            // that width the chunking degenerates to its scalar remainder loop
            // and vectorizes nothing, so the explicit AVX kernel is worth ~40%.
            // Above it the autovectorizer already does well and the 8-wide
            // kernel can lose, so keep the exact kernel the pre-dispatch code
            // used and stay non-regressing by construction.
            //
            // SAFETY: the build baseline enables avx2+fma, which imply avx+fma,
            // so the kernel's `#[target_feature]` contract is met statically.
            let narrow = dimension <= 16;
            y.chunks_exact(dimension).map(move |v| {
                if narrow {
                    unsafe { x86::l2_f32_avx_fma(x, v) }
                } else {
                    l2_f32_scalar(x, v)
                }
            })
        }
        #[cfg(all(
            target_arch = "x86_64",
            not(all(target_feature = "avx2", target_feature = "fma"))
        ))]
        {
            l2_batch_f32_runtime_dispatch(x, y, dimension)
        }
        #[cfg(not(target_arch = "x86_64"))]
        {
            y.chunks_exact(dimension).map(move |v| Self::l2(x, v))
        }
    }
}

/// Sub-AVX2 builds: pick a `#[target_feature]` kernel once for the batch.
#[cfg(all(
    target_arch = "x86_64",
    not(all(target_feature = "avx2", target_feature = "fma"))
))]
#[inline]
fn l2_batch_f32_runtime_dispatch<'a>(
    x: &'a [f32],
    y: &'a [f32],
    dimension: usize,
) -> impl Iterator<Item = f32> + 'a {
    // SAFETY: each kernel is entered only under its matching runtime detection.
    match *SIMD_SUPPORT {
        SimdSupport::Avx512 | SimdSupport::Avx512FP16 => {
            BatchIter::Eager(unsafe { x86::l2_batch_f32_avx512(x, y, dimension) }.into_iter())
        }
        SimdSupport::Avx2 | SimdSupport::AvxFma => {
            BatchIter::Eager(unsafe { x86::l2_batch_f32_avx_fma(x, y, dimension) }.into_iter())
        }
        SimdSupport::Avx => {
            BatchIter::Eager(unsafe { x86::l2_batch_f32_avx(x, y, dimension) }.into_iter())
        }
        _ => BatchIter::Lazy(y.chunks_exact(dimension).map(move |v| l2_f32_scalar(x, v))),
    }
}

/// L2 distance for f32, runtime-dispatched via `SIMD_SUPPORT` on x86_64
/// (AVX-512 / AVX2+FMA / AVX+FMA / AVX / scalar). Non-x86 uses the
/// auto-vectorised scalar loop.
#[inline]
fn l2_f32_dispatched(x: &[f32], y: &[f32]) -> f32 {
    #[cfg(target_arch = "x86_64")]
    {
        match *SIMD_SUPPORT {
            SimdSupport::Avx512 | SimdSupport::Avx512FP16 => unsafe { x86::l2_f32_avx512(x, y) },
            SimdSupport::Avx2 | SimdSupport::AvxFma => unsafe { x86::l2_f32_avx_fma(x, y) },
            SimdSupport::Avx => unsafe { x86::l2_f32_avx(x, y) },
            _ => l2_f32_scalar(x, y),
        }
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        l2_f32_scalar(x, y)
    }
}

/// Portable scalar L2 distance for f32. Used as the x86_64 fallback when no
/// AVX2 is detected, and as the only path on non-x86 architectures. The
/// `LANES = 16` chunking matches the explicit-SIMD inner kernels above.
#[inline]
fn l2_f32_scalar(x: &[f32], y: &[f32]) -> f32 {
    // 16 = 512 (avx512) / 8 bits / 4 (sizeof(f32))
    // See https://github.com/lance-format/lance/pull/2450.
    l2_scalar::<f32, f32, 16>(x, y)
}

impl L2 for f64 {
    #[inline]
    fn l2(x: &[Self], y: &[Self]) -> f32 {
        l2_f64_simd(x, y)
    }
}

/// L2 distance for f64, runtime-dispatched via `SIMD_SUPPORT` on x86_64
/// (AVX-512 / AVX2+FMA / AVX+FMA / AVX / scalar). Non-x86 uses the SIMD
/// primitives in `crate::simd::f64`, unconditionally backed by NEON / LSX-LASX.
#[inline]
fn l2_f64_simd(x: &[f64], y: &[f64]) -> f32 {
    #[cfg(target_arch = "x86_64")]
    {
        match *SIMD_SUPPORT {
            SimdSupport::Avx512 | SimdSupport::Avx512FP16 => unsafe { x86::l2_f64_avx512(x, y) },
            SimdSupport::Avx2 | SimdSupport::AvxFma => unsafe { x86::l2_f64_avx_fma(x, y) },
            SimdSupport::Avx => unsafe { x86::l2_f64_avx(x, y) },
            _ => l2_f64_scalar(x, y),
        }
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        l2_f64_simd_other(x, y)
    }
}

/// Portable scalar L2 distance for f64. Used as the x86_64 fallback when no
/// AVX2 is detected, and exposed for cross-backend parity testing.
#[cfg(target_arch = "x86_64")]
#[inline]
fn l2_f64_scalar(x: &[f64], y: &[f64]) -> f32 {
    x.iter()
        .zip(y.iter())
        .map(|(&a, &b)| {
            let diff = a - b;
            diff * diff
        })
        .sum::<f64>() as f32
}

#[cfg(target_arch = "x86_64")]
mod x86 {
    use std::arch::x86_64::*;

    use crate::simd::f64::{f64x4, f64x8};
    use crate::simd::x86::hsum256_ps;
    use crate::simd::{FloatSimd, SIMD};

    /// L2 distance from `x` to every `dimension`-sized vector in `batch`, with
    /// the AVX-512 tier entered once for the whole batch rather than once per
    /// vector.
    ///
    /// # Safety
    /// The host must support AVX-512F.
    ///
    /// Only compiled for builds whose baseline is below avx2+fma; at or above
    /// that baseline `l2_batch` inlines the kernel directly and never runtime-
    /// dispatches, so this wrapper would be dead code (see `l2_batch`).
    #[cfg(not(all(target_feature = "avx2", target_feature = "fma")))]
    #[target_feature(enable = "avx512f")]
    pub(super) unsafe fn l2_batch_f32_avx512(
        x: &[f32],
        batch: &[f32],
        dimension: usize,
    ) -> Vec<f32> {
        batch
            .chunks_exact(dimension)
            .map(|y| unsafe { l2_f32_avx512(x, y) })
            .collect()
    }

    /// As [`l2_batch_f32_avx512`], for the AVX+FMA and AVX2 tiers.
    ///
    /// # Safety
    /// The host must support AVX and FMA.
    #[cfg(not(all(target_feature = "avx2", target_feature = "fma")))]
    #[target_feature(enable = "avx,fma")]
    pub(super) unsafe fn l2_batch_f32_avx_fma(
        x: &[f32],
        batch: &[f32],
        dimension: usize,
    ) -> Vec<f32> {
        batch
            .chunks_exact(dimension)
            .map(|y| unsafe { l2_f32_avx_fma(x, y) })
            .collect()
    }

    /// As [`l2_batch_f32_avx512`], for the AVX-without-FMA tier.
    ///
    /// # Safety
    /// The host must support AVX.
    #[cfg(not(all(target_feature = "avx2", target_feature = "fma")))]
    #[target_feature(enable = "avx")]
    pub(super) unsafe fn l2_batch_f32_avx(x: &[f32], batch: &[f32], dimension: usize) -> Vec<f32> {
        batch
            .chunks_exact(dimension)
            .map(|y| unsafe { l2_f32_avx(x, y) })
            .collect()
    }

    /// AVX-512 path for f64: 8-wide `__m512d` with `vsubpd` + `vfmadd231pd` per iteration.
    #[target_feature(enable = "avx512f")]
    pub unsafe fn l2_f64_avx512(x: &[f64], y: &[f64]) -> f32 {
        let dim = x.len();
        let unrolled_len = dim / 8 * 8;

        let mut acc = _mm512_setzero_pd();
        for i in (0..unrolled_len).step_by(8) {
            let a = _mm512_loadu_pd(x.as_ptr().add(i));
            let b = _mm512_loadu_pd(y.as_ptr().add(i));
            let diff = _mm512_sub_pd(a, b);
            acc = _mm512_fmadd_pd(diff, diff, acc);
        }

        let tail: f64 = x[unrolled_len..]
            .iter()
            .zip(y[unrolled_len..].iter())
            .map(|(&a, &b)| {
                let diff = a - b;
                diff * diff
            })
            .sum();

        (_mm512_reduce_add_pd(acc) + tail) as f32
    }

    /// AVX + FMA path for f64. Covers both AvxFma and AVX2 dispatch (body uses no AVX2-specific intrinsics).
    #[target_feature(enable = "avx,fma")]
    pub unsafe fn l2_f64_avx_fma(x: &[f64], y: &[f64]) -> f32 {
        let dim = x.len();
        let unrolled_len = dim / 8 * 8;

        let mut acc8 = f64x8::zeros();
        for i in (0..unrolled_len).step_by(8) {
            let a = f64x8::load_unaligned(x.as_ptr().add(i));
            let b = f64x8::load_unaligned(y.as_ptr().add(i));
            let diff = a - b;
            acc8.multiply_add(diff, diff);
        }

        let aligned_len = dim / 4 * 4;
        let mut acc4 = f64x4::zeros();
        for i in (unrolled_len..aligned_len).step_by(4) {
            let a = f64x4::load_unaligned(x.as_ptr().add(i));
            let b = f64x4::load_unaligned(y.as_ptr().add(i));
            let diff = a - b;
            acc4.multiply_add(diff, diff);
        }

        let tail: f64 = x[aligned_len..]
            .iter()
            .zip(y[aligned_len..].iter())
            .map(|(&a, &b)| {
                let diff = a - b;
                diff * diff
            })
            .sum();

        (acc8.reduce_sum() + acc4.reduce_sum() + tail) as f32
    }

    /// AVX-only path for f64 (no FMA): squared diff via `_mm256_mul_pd` + `_mm256_add_pd` for Sandy/Ivy Bridge.
    #[target_feature(enable = "avx")]
    pub unsafe fn l2_f64_avx(x: &[f64], y: &[f64]) -> f32 {
        let dim = x.len();
        let unrolled_len = dim / 4 * 4;

        let mut acc = _mm256_setzero_pd();
        for i in (0..unrolled_len).step_by(4) {
            let a = _mm256_loadu_pd(x.as_ptr().add(i));
            let b = _mm256_loadu_pd(y.as_ptr().add(i));
            let diff = _mm256_sub_pd(a, b);
            acc = _mm256_add_pd(acc, _mm256_mul_pd(diff, diff));
        }

        // Horizontal sum of __m256d -> f64.
        let lo = _mm256_castpd256_pd128(acc);
        let hi = _mm256_extractf128_pd(acc, 1);
        let sum128 = _mm_add_pd(lo, hi);
        let sum64 = _mm_add_pd(sum128, _mm_unpackhi_pd(sum128, sum128));
        let acc_sum = _mm_cvtsd_f64(sum64);

        let tail: f64 = x[unrolled_len..]
            .iter()
            .zip(y[unrolled_len..].iter())
            .map(|(&a, &b)| {
                let diff = a - b;
                diff * diff
            })
            .sum();

        (acc_sum + tail) as f32
    }

    /// AVX-512 path for f32: 16-wide `__m512` with `vsubps` + `vfmadd231ps` per iteration.
    #[target_feature(enable = "avx512f")]
    pub unsafe fn l2_f32_avx512(x: &[f32], y: &[f32]) -> f32 {
        let dim = x.len();
        let unrolled_len = dim / 16 * 16;

        let mut acc = _mm512_setzero_ps();
        for i in (0..unrolled_len).step_by(16) {
            let a = _mm512_loadu_ps(x.as_ptr().add(i));
            let b = _mm512_loadu_ps(y.as_ptr().add(i));
            let diff = _mm512_sub_ps(a, b);
            acc = _mm512_fmadd_ps(diff, diff, acc);
        }

        let tail: f32 = x[unrolled_len..]
            .iter()
            .zip(y[unrolled_len..].iter())
            .map(|(&a, &b)| {
                let diff = a - b;
                diff * diff
            })
            .sum();

        _mm512_reduce_add_ps(acc) + tail
    }

    /// AVX + FMA path for f32. Covers both AvxFma and AVX2 dispatch (body uses no AVX2-specific intrinsics).
    #[target_feature(enable = "avx,fma")]
    pub unsafe fn l2_f32_avx_fma(x: &[f32], y: &[f32]) -> f32 {
        let dim = x.len();
        let unrolled_len = dim / 8 * 8;

        let mut acc = _mm256_setzero_ps();
        for i in (0..unrolled_len).step_by(8) {
            let a = _mm256_loadu_ps(x.as_ptr().add(i));
            let b = _mm256_loadu_ps(y.as_ptr().add(i));
            let diff = _mm256_sub_ps(a, b);
            acc = _mm256_fmadd_ps(diff, diff, acc);
        }

        let tail: f32 = x[unrolled_len..]
            .iter()
            .zip(y[unrolled_len..].iter())
            .map(|(&a, &b)| {
                let diff = a - b;
                diff * diff
            })
            .sum();

        hsum256_ps(acc) + tail
    }

    /// AVX-only path for f32 (no FMA): squared diff via `_mm256_mul_ps` + `_mm256_add_ps` for Sandy/Ivy Bridge.
    #[target_feature(enable = "avx")]
    pub unsafe fn l2_f32_avx(x: &[f32], y: &[f32]) -> f32 {
        let dim = x.len();
        let unrolled_len = dim / 8 * 8;

        let mut acc = _mm256_setzero_ps();
        for i in (0..unrolled_len).step_by(8) {
            let a = _mm256_loadu_ps(x.as_ptr().add(i));
            let b = _mm256_loadu_ps(y.as_ptr().add(i));
            let diff = _mm256_sub_ps(a, b);
            acc = _mm256_add_ps(acc, _mm256_mul_ps(diff, diff));
        }

        let tail: f32 = x[unrolled_len..]
            .iter()
            .zip(y[unrolled_len..].iter())
            .map(|(&a, &b)| {
                let diff = a - b;
                diff * diff
            })
            .sum();

        hsum256_ps(acc) + tail
    }
}

#[cfg(not(target_arch = "x86_64"))]
#[inline]
fn l2_f64_simd_other(x: &[f64], y: &[f64]) -> f32 {
    use crate::simd::f64::{f64x4, f64x8};
    use crate::simd::{FloatSimd, SIMD};

    let dim = x.len();
    let unrolled_len = dim / 8 * 8;

    let mut acc8 = f64x8::zeros();
    for i in (0..unrolled_len).step_by(8) {
        unsafe {
            let a = f64x8::load_unaligned(x.as_ptr().add(i));
            let b = f64x8::load_unaligned(y.as_ptr().add(i));
            let diff = a - b;
            acc8.multiply_add(diff, diff);
        }
    }

    let aligned_len = dim / 4 * 4;
    let mut acc4 = f64x4::zeros();
    for i in (unrolled_len..aligned_len).step_by(4) {
        unsafe {
            let a = f64x4::load_unaligned(x.as_ptr().add(i));
            let b = f64x4::load_unaligned(y.as_ptr().add(i));
            let diff = a - b;
            acc4.multiply_add(diff, diff);
        }
    }

    let tail: f64 = x[aligned_len..]
        .iter()
        .zip(y[aligned_len..].iter())
        .map(|(&a, &b)| {
            let diff = a - b;
            diff * diff
        })
        .sum();

    (acc8.reduce_sum() + acc4.reduce_sum() + tail) as f32
}

/// Accumulate squared differences for one dimension into per-target results.
///
/// Separated into its own function so that LLVM sees `row` and `result`
/// as non-aliasing via the function signature (`&[f32]` vs `&mut [f32]`),
/// enabling packed SIMD vectorization (vbroadcastss + vsubps + vfmadd231ps).
#[inline(never)]
fn accumulate_l2_dimension(q: f32, row: &[f32], result: &mut [f32]) {
    for (dist, &target) in result.iter_mut().zip(row.iter()) {
        let diff = q - target;
        *dist += diff * diff;
    }
}

/// Pre-transposed target vectors for batched L2 distance computation.
///
/// Stores targets in SoA layout `[dimension][num_targets]` so the inner
/// distance loop iterates over targets contiguously. The AoS-to-SoA
/// transpose is done once at construction; callers should reuse the
/// struct across many queries to amortize that cost.
///
/// **Cache constraint**: this is designed for cases where
/// `num_targets × dimension × 4` fits in L1 cache (~32 KB), such as PQ
/// sub-vector codebooks (e.g. 256 centroids × 16 dims = 16 KB).
/// For large target sets the SoA layout causes L1 thrashing and
/// [`l2_distance_batch`] with its AoS per-target locality is faster.
#[derive(Debug, Clone, DeepSizeOf)]
pub struct L2Prepared {
    transposed: Vec<f32>,
    dimension: usize,
    num_targets: usize,
}

impl L2Prepared {
    /// Transpose `targets` from AoS `[num_targets][dimension]` to SoA layout.
    pub fn new(targets: &[f32], dimension: usize) -> Self {
        let num_targets = targets.len() / dimension;
        debug_assert_eq!(targets.len(), num_targets * dimension);

        let mut transposed = vec![0.0f32; targets.len()];
        for t in 0..num_targets {
            for d in 0..dimension {
                transposed[d * num_targets + t] = targets[t * dimension + d];
            }
        }

        Self {
            transposed,
            dimension,
            num_targets,
        }
    }

    /// Compute L2 distances from `query` to every target, writing into `out`.
    ///
    /// `out` must have length `num_targets`. It will be zeroed before accumulation.
    pub fn distances_into(&self, query: &[f32], out: &mut [f32]) {
        debug_assert_eq!(query.len(), self.dimension);
        debug_assert_eq!(out.len(), self.num_targets);

        out.fill(0.0);
        for (d, &q) in query.iter().enumerate() {
            let row = &self.transposed[d * self.num_targets..][..self.num_targets];
            accumulate_l2_dimension(q, row, out);
        }
    }

    /// Compute L2 distances from `query` to every target.
    pub fn distances(&self, query: &[f32]) -> Vec<f32> {
        let mut result = vec![0.0f32; self.num_targets];
        self.distances_into(query, &mut result);
        result
    }

    /// Return the index of the nearest target to `query`, using `buf` as scratch space.
    ///
    /// `buf` must have length `num_targets`.
    pub fn nearest_into(&self, query: &[f32], buf: &mut [f32]) -> Option<u32> {
        self.distances_into(query, buf);
        crate::kernels::argmin_value_float(buf.iter().copied()).map(|(idx, _)| idx)
    }

    /// Return the index of the nearest target to `query`.
    pub fn nearest(&self, query: &[f32]) -> Option<u32> {
        self.nearest_into(query, &mut vec![0.0f32; self.num_targets])
    }

    /// Number of targets in this set.
    pub fn num_targets(&self) -> usize {
        self.num_targets
    }

    /// Dimension of each target vector.
    pub fn dimension(&self) -> usize {
        self.dimension
    }

    /// Size of the internal buffer in bytes.
    pub fn size_bytes(&self) -> usize {
        self.transposed.len() * std::mem::size_of::<f32>()
    }
}

/// Compute L2 distance between two vectors.
#[inline]
pub fn l2_distance(from: &[f32], to: &[f32]) -> f32 {
    l2(from, to)
}

/// Compute L2 distance between a vector and a batch of vectors.
///
/// Parameters
///
/// - `from`: the vector to compute distance from.
/// - `to`: a list of vectors to compute distance to.
/// - `dimension`: the dimension of the vectors.
///
/// Returns
///
/// An iterator of pair-wise distance between `from` vector to each vector in the batch.
pub fn l2_distance_batch<'a, T: L2>(
    from: &'a [T],
    to: &'a [T],
    dimension: usize,
) -> impl Iterator<Item = f32> + 'a {
    assume_eq!(from.len(), dimension);
    assume_eq!(to.len() % dimension, 0);

    T::l2_batch(from, to, dimension)
}

fn do_l2_distance_arrow_batch<T: ArrowFloatType>(
    from: &T::ArrayType,
    to: &FixedSizeListArray,
) -> Result<Arc<Float32Array>>
where
    T::Native: L2,
{
    let dimension = to.value_length() as usize;
    debug_assert_eq!(from.len(), dimension);

    // TODO: if we detect there is a run of nulls, should we skip those?
    let to_values =
        to.values()
            .as_any()
            .downcast_ref::<T::ArrayType>()
            .ok_or(Error::ComputeError(format!(
                "Cannot downcast to the same type: {} != {}",
                T::FLOAT_TYPE,
                to.value_type()
            )))?;
    let dists = l2_distance_batch(from.as_slice(), to_values.as_slice(), dimension);

    Ok(Arc::new(Float32Array::new(
        dists.collect(),
        to.nulls().cloned(),
    )))
}

/// Compute L2 distance between a vector and a batch of vectors.
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
pub fn l2_distance_arrow_batch(
    from: &dyn Array,
    to: &FixedSizeListArray,
) -> Result<Arc<Float32Array>> {
    match *from.data_type() {
        DataType::Float16 => do_l2_distance_arrow_batch::<Float16Type>(from.as_primitive(), to),
        DataType::Float32 => do_l2_distance_arrow_batch::<Float32Type>(from.as_primitive(), to),
        DataType::Float64 => do_l2_distance_arrow_batch::<Float64Type>(from.as_primitive(), to),
        DataType::Int8 => do_l2_distance_arrow_batch::<Float32Type>(
            &from
                .as_primitive::<Int8Type>()
                .into_iter()
                .map(|x| x.unwrap() as f32)
                .collect(),
            &to.convert_to_floating_point()?,
        ),
        _ => Err(Error::ComputeError(format!(
            "Unsupported data type: {}",
            from.data_type()
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use approx::assert_relative_eq;
    use num_traits::ToPrimitive;
    use proptest::prelude::*;

    use crate::test_utils::{
        arbitrary_bf16, arbitrary_f16, arbitrary_f32, arbitrary_f64, arbitrary_vector_pair,
    };

    #[test]
    fn test_l2_f32_dispatch_matches_scalar() {
        // Covers tail handling for lengths around the 16-lane AVX-512 stride.
        for dim in [1usize, 7, 15, 16, 17, 31, 33, 64, 100, 1024] {
            let x: Vec<f32> = (0..dim).map(|i| (i as f32) * 0.5 - 3.0).collect();
            let y: Vec<f32> = (0..dim).map(|i| (i as f32) * -0.25 + 1.5).collect();
            assert_relative_eq!(l2_f32(&x, &y), l2(&x, &y), max_relative = 1e-5);
        }
    }

    #[test]
    fn test_euclidean_distance() {
        let mat = FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
            vec![
                Some((0..8).map(|v| Some(v as f32)).collect::<Vec<_>>()),
                Some((1..9).map(|v| Some(v as f32)).collect::<Vec<_>>()),
                Some((2..10).map(|v| Some(v as f32)).collect::<Vec<_>>()),
                Some((3..11).map(|v| Some(v as f32)).collect::<Vec<_>>()),
            ],
            8,
        );
        let point = Float32Array::from((2..10).map(|v| Some(v as f32)).collect::<Vec<_>>());
        let distances = l2_distance_batch(
            point.values(),
            mat.values().as_primitive::<Float32Type>().values(),
            8,
        )
        .collect::<Vec<_>>();

        assert_eq!(distances, vec![32.0, 8.0, 0.0, 8.0]);
    }

    #[test]
    fn test_not_aligned() {
        let mat = (0..6)
            .chain(0..8)
            .chain(1..9)
            .chain(2..10)
            .chain(3..11)
            .map(|v| v as f32)
            .collect::<Vec<_>>();
        let point = Float32Array::from((0..10).map(|v| Some(v as f32)).collect::<Vec<_>>());
        let distances = l2_distance_batch(&point.values()[2..], &mat[6..], 8).collect::<Vec<_>>();

        assert_eq!(distances, vec![32.0, 8.0, 0.0, 8.0]);
    }

    #[test]
    fn test_odd_length_vector() {
        let mat = Float32Array::from_iter((0..5).map(|v| Some(v as f32)));
        let point = Float32Array::from((2..7).map(|v| Some(v as f32)).collect::<Vec<_>>());
        let distances = l2_distance_batch(point.values(), mat.values(), 5).collect::<Vec<_>>();

        assert_eq!(distances, vec![20.0]);
    }

    #[test]
    fn test_l2_distance_cases() {
        let values: Float32Array = vec![
            0.25335717, 0.24663818, 0.26330215, 0.14988247, 0.06042378, 0.21077952, 0.26687378,
            0.22145681, 0.18319066, 0.18688454, 0.05216244, 0.11470364, 0.10554603, 0.19964123,
            0.06387895, 0.18992095, 0.00123718, 0.13500804, 0.09516747, 0.19508345, 0.2582458,
            0.1211653, 0.21121833, 0.24809816, 0.04078768, 0.19586588, 0.16496408, 0.14766085,
            0.04898421, 0.14728612, 0.21263947, 0.16763233,
        ]
        .into();

        let q: Float32Array = vec![
            0.18549609,
            0.29954708,
            0.28318876,
            0.05424477,
            0.093134984,
            0.21580857,
            0.2951282,
            0.19866848,
            0.13868214,
            0.19819534,
            0.23271298,
            0.047727287,
            0.14394054,
            0.023316395,
            0.18589257,
            0.037315924,
            0.07037327,
            0.32609823,
            0.07344752,
            0.020155912,
            0.18485495,
            0.32763934,
            0.14296658,
            0.04498596,
            0.06254237,
            0.24348071,
            0.16009757,
            0.053892266,
            0.05918874,
            0.040363103,
            0.19913352,
            0.14545348,
        ]
        .into();

        let d = l2_distance_batch(q.values(), values.values(), 32).collect::<Vec<_>>();
        assert_relative_eq!(0.319_357_84, d[0]);
    }

    /// Reference implementation of L2 distance.
    ///
    /// Note that we skip the final square root step for performance reasons.
    fn l2_distance_reference(x: &[f64], y: &[f64]) -> f64 {
        x.iter()
            .zip(y.iter())
            .map(|(x, y)| (*x - *y).powi(2))
            .sum::<f64>()
    }

    fn do_l2_test<T: L2 + ToPrimitive>(x: &[T], y: &[T]) -> std::result::Result<(), TestCaseError> {
        let x_f64 = x.iter().map(|v| v.to_f64().unwrap()).collect::<Vec<f64>>();
        let y_f64 = y.iter().map(|v| v.to_f64().unwrap()).collect::<Vec<f64>>();

        let result = l2(x, y);
        let reference = l2_distance_reference(&x_f64, &y_f64) as f32;

        prop_assert!(approx::relative_eq!(result, reference, max_relative = 1e-6));
        Ok(())
    }

    #[test]
    fn test_l2_distance_f16_max() {
        let x = vec![f16::MAX; 4048];
        let y = vec![-f16::MAX; 4048];
        do_l2_test(&x, &y).unwrap();
    }

    // Test L2 distance over different types.
    // * L2 is valid over the entire range of f16.
    // * L2 is valid over f32 and bf16 in the range of +-1e12.
    // * L2 for f64 should match the reference implementation.
    proptest::proptest! {
        #[test]
        fn test_l2_distance_f16((x, y) in arbitrary_vector_pair(arbitrary_f16, 4..4048)) {
            do_l2_test(&x, &y)?;
        }

        #[test]
        fn test_l2_distance_bf16((x, y) in arbitrary_vector_pair(arbitrary_bf16, 4..4048)){
            do_l2_test(&x, &y)?;
        }

        #[test]
        fn test_l2_distance_f32((x, y) in arbitrary_vector_pair(arbitrary_f32, 4..4048)){
            do_l2_test(&x, &y)?;
        }

        #[test]
        fn test_l2_distance_f64((x, y) in arbitrary_vector_pair(arbitrary_f64, 4..4048)){
            do_l2_test(&x, &y)?;
        }

        /// Cross-backend parity: scalar fallback must match the dispatched
        /// SIMD path within numerical tolerance. Exercises `l2_f64_scalar`
        /// directly so the runtime fallback is exercised even on AVX2-capable
        /// CI hosts.
        #[cfg(target_arch = "x86_64")]
        #[test]
        fn test_l2_f64_scalar_simd_parity(
            (x, y) in arbitrary_vector_pair(arbitrary_f64, 4..4048)
        ) {
            let scalar = l2_f64_scalar(&x, &y);
            let simd = l2_f64_simd(&x, &y);
            prop_assert!(approx::relative_eq!(scalar, simd, max_relative = 1e-6));
        }

        /// Parity check for `l2_f32_dispatched` (Branch B exclusive: the
        /// auto-vectorised scalar L2 path). The dispatched kernel must
        /// agree with a portable f64-precision scalar reference within
        /// numerical tolerance. The reference is hand-rolled here to keep
        /// this test architecture-agnostic (the x86_64-only `l2_f64_scalar`
        /// helper is gated above).
        #[test]
        fn test_l2_f32_scalar_simd_parity(
            (x, y) in arbitrary_vector_pair(arbitrary_f32, 4..4048)
        ) {
            let scalar = x
                .iter()
                .zip(y.iter())
                .map(|(&a, &b)| ((a as f64) - (b as f64)).powi(2))
                .sum::<f64>() as f32;
            let simd = <f32 as L2>::l2(&x, &y);
            prop_assert!(approx::relative_eq!(scalar, simd, max_relative = 1e-3));
        }

        /// AVX-512-direct parity: explicitly compares the scalar fallback
        /// against the native AVX-512 inner kernel on AVX-512F-capable hosts
        /// (Skylake-X+, Ice Lake, Sapphire Rapids, Zen 4). Early-returns on
        /// hosts without AVX-512F.
        #[cfg(target_arch = "x86_64")]
        #[test]
        fn test_l2_f64_scalar_vs_avx512_parity(
            (x, y) in arbitrary_vector_pair(arbitrary_f64, 4..4048)
        ) {
            if !std::is_x86_feature_detected!("avx512f") {
                return Ok(());
            }
            let scalar = l2_f64_scalar(&x, &y);
            let avx512 = unsafe { x86::l2_f64_avx512(&x, &y) };
            prop_assert!(approx::relative_eq!(scalar, avx512, max_relative = 1e-6));
        }

        /// AVX + FMA-direct parity for the f64 L2 kernel. Covers the AMD
        /// Piledriver / Steamroller / FX-7500 tier. Early-returns on hosts
        /// without both AVX and FMA.
        #[cfg(target_arch = "x86_64")]
        #[test]
        fn test_l2_f64_scalar_vs_avx_fma_parity(
            (x, y) in arbitrary_vector_pair(arbitrary_f64, 4..4048)
        ) {
            if !(std::is_x86_feature_detected!("avx") && std::is_x86_feature_detected!("fma")) {
                return Ok(());
            }
            let scalar = l2_f64_scalar(&x, &y);
            let avx_fma = unsafe { x86::l2_f64_avx_fma(&x, &y) };
            prop_assert!(approx::relative_eq!(scalar, avx_fma, max_relative = 1e-6));
        }

        /// AVX-only-direct parity for the f64 L2 kernel. Covers the Intel
        /// Sandy Bridge / Ivy Bridge tier. Early-returns on hosts without
        /// AVX.
        #[cfg(target_arch = "x86_64")]
        #[test]
        fn test_l2_f64_scalar_vs_avx_parity(
            (x, y) in arbitrary_vector_pair(arbitrary_f64, 4..4048)
        ) {
            if !std::is_x86_feature_detected!("avx") {
                return Ok(());
            }
            let scalar = l2_f64_scalar(&x, &y);
            let avx = unsafe { x86::l2_f64_avx(&x, &y) };
            prop_assert!(approx::relative_eq!(scalar, avx, max_relative = 1e-6));
        }

        /// AVX-512-direct parity for f32: explicitly compares the scalar
        /// fallback against the native f32 AVX-512 inner kernel on
        /// AVX-512F-capable hosts.
        #[cfg(target_arch = "x86_64")]
        #[test]
        fn test_l2_f32_scalar_vs_avx512_parity(
            (x, y) in arbitrary_vector_pair(arbitrary_f32, 4..4048)
        ) {
            if !std::is_x86_feature_detected!("avx512f") {
                return Ok(());
            }
            let scalar = l2_f32_scalar(&x, &y);
            let avx512 = unsafe { x86::l2_f32_avx512(&x, &y) };
            prop_assert!(approx::relative_eq!(scalar, avx512, max_relative = 1e-3));
        }

        /// AVX + FMA-direct parity for the f32 L2 kernel. Covers the AMD
        /// Piledriver / Steamroller / FX-7500 tier. Early-returns on hosts
        /// without both AVX and FMA.
        #[cfg(target_arch = "x86_64")]
        #[test]
        fn test_l2_f32_scalar_vs_avx_fma_parity(
            (x, y) in arbitrary_vector_pair(arbitrary_f32, 4..4048)
        ) {
            if !(std::is_x86_feature_detected!("avx") && std::is_x86_feature_detected!("fma")) {
                return Ok(());
            }
            let scalar = l2_f32_scalar(&x, &y);
            let avx_fma = unsafe { x86::l2_f32_avx_fma(&x, &y) };
            prop_assert!(approx::relative_eq!(scalar, avx_fma, max_relative = 1e-3));
        }

        /// AVX-only-direct parity for the f32 L2 kernel. Covers the Intel
        /// Sandy Bridge / Ivy Bridge tier. Early-returns on hosts without
        /// AVX.
        #[cfg(target_arch = "x86_64")]
        #[test]
        fn test_l2_f32_scalar_vs_avx_parity(
            (x, y) in arbitrary_vector_pair(arbitrary_f32, 4..4048)
        ) {
            if !std::is_x86_feature_detected!("avx") {
                return Ok(());
            }
            let scalar = l2_f32_scalar(&x, &y);
            let avx = unsafe { x86::l2_f32_avx(&x, &y) };
            prop_assert!(approx::relative_eq!(scalar, avx, max_relative = 1e-3));
        }
    }

    #[test]
    fn test_uint8_l2_edge_cases() {
        let q = vec![0_u8; 2048];
        let v = vec![0_u8; 2048];
        assert_eq!(l2_distance_uint_scalar(&q, &v), 0.0);

        let q = vec![0_u8; 2048];
        let v = vec![255_u8; 2048];
        assert_eq!(
            l2_distance_uint_scalar(&q, &v),
            (255_u32.pow(2) * 2048) as f32
        );
        assert_eq!(
            l2_distance_uint_scalar(&v, &q),
            (255_u32.pow(2) * 2048) as f32
        );
    }

    #[test]
    fn test_l2_targets_matches_scalar() {
        let cases = vec![
            (16, 8),   // small target count
            (16, 16),  // exact SIMD width
            (16, 256), // PQ-like: 256 centroids, 16-dim sub-vectors
            (16, 17),  // one remainder
            (16, 31),  // 15 remainder
            (1, 32),   // dim=1
            (3, 20),   // odd dimension
            (128, 64), // larger dimension
        ];

        for (dim, num_targets) in cases {
            let query: Vec<f32> = (0..dim).map(|i| (i as f32) * 0.1 + 0.05).collect();
            let targets: Vec<f32> = (0..dim * num_targets)
                .map(|i| ((i * 7 + 3) % 100) as f32 * 0.01)
                .collect();

            let expected: Vec<f32> = targets
                .chunks_exact(dim)
                .map(|v| l2_scalar::<f32, f32, 16>(&query, v))
                .collect();

            let prepared = L2Prepared::new(&targets, dim);
            let actual = prepared.distances(&query);

            assert_eq!(
                actual.len(),
                expected.len(),
                "length mismatch for dim={dim}, num_targets={num_targets}"
            );
            for (i, (a, e)) in actual.iter().zip(expected.iter()).enumerate() {
                assert!(
                    approx::relative_eq!(a, e, max_relative = 1e-6),
                    "mismatch at index {i} for dim={dim}, num_targets={num_targets}: \
                     prepared={a}, scalar={e}"
                );
            }
        }
    }

    #[test]
    fn test_l2_targets_zeros() {
        let dim = 16;
        let num_targets = 32;
        let query = vec![0.0f32; dim];
        let targets = vec![0.0f32; dim * num_targets];

        let prepared = L2Prepared::new(&targets, dim);
        let distances = prepared.distances(&query);
        assert_eq!(distances.len(), num_targets);
        for d in &distances {
            assert_eq!(*d, 0.0);
        }
    }

    #[test]
    fn test_l2_targets_known_values() {
        let dim = 2;
        let query = vec![1.0f32, 0.0];

        // 16 targets: [1,0], [0,1], [2,0], [0,0], then 12x [0,0]
        let mut targets = vec![1.0, 0.0, 0.0, 1.0, 2.0, 0.0, 0.0, 0.0];
        for _ in 4..16 {
            targets.extend_from_slice(&[0.0, 0.0]);
        }

        let prepared = L2Prepared::new(&targets, dim);
        let distances = prepared.distances(&query);
        assert_eq!(distances.len(), 16);
        assert_relative_eq!(distances[0], 0.0);
        assert_relative_eq!(distances[1], 2.0);
        assert_relative_eq!(distances[2], 1.0);
        assert_relative_eq!(distances[3], 1.0);
        for d in &distances[4..] {
            assert_relative_eq!(*d, 1.0);
        }
    }

    #[test]
    fn test_l2_targets_reuse() {
        // Verify that the same L2Prepared can be queried multiple times
        let dim = 4;
        let targets = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0];
        let prepared = L2Prepared::new(&targets, dim);

        let q1 = vec![1.0, 2.0, 3.0, 4.0];
        let q2 = vec![5.0, 6.0, 7.0, 8.0];

        let d1 = prepared.distances(&q1);
        let d2 = prepared.distances(&q2);

        assert_relative_eq!(d1[0], 0.0); // q1 == target[0]
        assert_relative_eq!(d2[1], 0.0); // q2 == target[1]
    }

    /// `l2_batch` must agree with the per-vector `l2` it replaced, on every
    /// build: the AVX2-baseline path, the hoisted-dispatch path, and the
    /// portable fallback all funnel through here.
    #[rstest::rstest]
    #[case::dim_8(8)]
    #[case::dim_16(16)]
    #[case::dim_32(32)]
    #[case::dim_1024(1024)]
    fn test_l2_batch_f32_matches_per_vector_l2(#[case] dimension: usize) {
        let num_vectors = 5;
        let x: Vec<f32> = (0..dimension)
            .map(|i| ((i % 13) as f32) * 0.25 + 1.0)
            .collect();
        let batch: Vec<f32> = (0..dimension * num_vectors)
            .map(|i| ((i % 11) as f32) * 0.5 - 2.0)
            .collect();

        let got: Vec<f32> = f32::l2_batch(&x, &batch, dimension).collect();
        let want: Vec<f32> = batch
            .chunks_exact(dimension)
            .map(|y| f32::l2(&x, y))
            .collect();

        assert_eq!(got.len(), num_vectors);
        for (g, w) in got.iter().zip(want.iter()) {
            assert!(
                approx::relative_eq!(g, w, epsilon = 1e-4),
                "dim {dimension}: batch {g} != per-vector {w}"
            );
        }
    }

    /// The per-batch `#[target_feature]` kernels are only reached on sub-AVX2
    /// builds or AVX-512 hosts, so call them directly to cover them.
    #[cfg(all(
        target_arch = "x86_64",
        not(all(target_feature = "avx2", target_feature = "fma"))
    ))]
    fn check_l2_batch_kernel(kernel: unsafe fn(&[f32], &[f32], usize) -> Vec<f32>) {
        for dimension in [8_usize, 16, 40] {
            let num_vectors = 3;
            let x: Vec<f32> = (0..dimension).map(|i| (i as f32) * 0.5 + 1.0).collect();
            let batch: Vec<f32> = (0..dimension * num_vectors)
                .map(|i| ((i % 7) as f32) + 1.0)
                .collect();

            let got = unsafe { kernel(&x, &batch, dimension) };
            assert_eq!(got.len(), num_vectors);
            for (chunk, &g) in batch.chunks_exact(dimension).zip(got.iter()) {
                let want = l2_scalar::<f32, f32, 16>(&x, chunk);
                assert!(
                    approx::relative_eq!(g, want, epsilon = 1e-4),
                    "dim {dimension}: kernel {g} != scalar {want}"
                );
            }
        }
    }

    // The runtime-dispatch batch kernels only exist in sub-avx2+fma builds
    // (see `x86::l2_batch_f32_avx512`), so gate their tests the same way.
    #[cfg(all(
        target_arch = "x86_64",
        not(all(target_feature = "avx2", target_feature = "fma"))
    ))]
    #[test]
    fn test_l2_batch_avx_fma_matches_scalar() {
        if !std::is_x86_feature_detected!("avx") || !std::is_x86_feature_detected!("fma") {
            return;
        }
        check_l2_batch_kernel(x86::l2_batch_f32_avx_fma);
    }

    #[cfg(all(
        target_arch = "x86_64",
        not(all(target_feature = "avx2", target_feature = "fma"))
    ))]
    #[test]
    fn test_l2_batch_avx_matches_scalar() {
        if !std::is_x86_feature_detected!("avx") {
            return;
        }
        check_l2_batch_kernel(x86::l2_batch_f32_avx);
    }

    #[cfg(all(
        target_arch = "x86_64",
        not(all(target_feature = "avx2", target_feature = "fma"))
    ))]
    #[test]
    fn test_l2_batch_avx512_matches_scalar() {
        if !std::is_x86_feature_detected!("avx512f") {
            return;
        }
        check_l2_batch_kernel(x86::l2_batch_f32_avx512);
    }
}
