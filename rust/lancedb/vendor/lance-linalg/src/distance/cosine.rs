// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Cosine distance
//!
//! <https://en.wikipedia.org/wiki/Cosine_similarity>
//!
//! `bf16, f16, f32, f64` types are supported.

use std::sync::Arc;

use arrow_array::{Array, FixedSizeListArray, Float32Array};
use half::{bf16, f16};
use lance_core::utils::cpu::SIMD_SUPPORT;

#[cfg(feature = "fp16kernels")]
use super::HalfBackend;
use super::{
    BatchKernel, Dot, HALF_KERNELS_COMPILED, HalfType, Normalize, assert_batch_output_layout,
    assert_equal_lengths, dot::dot, half_backend, norm_l2::norm_l2, x86_half_features,
};
use crate::Result;
#[cfg(not(target_arch = "x86_64"))]
use crate::simd::{
    FloatSimd, SIMD,
    f32::{f32x8, f32x16},
};

/// Cosine Distance
pub trait Cosine: Dot + Normalize {
    /// Cosine distance between two vectors.
    #[inline]
    fn cosine(x: &[Self], other: &[Self]) -> f32 {
        let x_norm = norm_l2(x);
        Self::cosine_fast(x, x_norm, other)
    }

    /// Fast cosine function, that assumes that the norm of the first vector is already known.
    #[inline]
    fn cosine_fast(x: &[Self], x_norm: f32, y: &[Self]) -> f32 {
        cosine_scalar(x, x_norm, y)
    }

    /// Cosine between two vectors, with the L2 norms of both vectors already known.
    #[inline]
    fn cosine_with_norms(x: &[Self], x_norm: f32, y_norm: f32, y: &[Self]) -> f32 {
        cosine_scalar_fast(x, x_norm, y, y_norm)
    }

    /// Writes the cosine distance from `x` to each `dimension`-sized vector
    /// in `batch` into the matching element of `output`.
    ///
    /// The default computes the norm of `x` once and calls
    /// [`Cosine::cosine_fast`] per vector. `f32` overrides it with one batch
    /// kernel call.
    ///
    /// # Panics
    ///
    /// Panics if `x.len() != dimension`, if `batch.len()` is not a multiple of
    /// `dimension`, or if `output.len() != batch.len() / dimension`.
    fn cosine_batch_into(x: &[Self], batch: &[Self], dimension: usize, output: &mut [f32]) {
        assert_batch_output_layout(x.len(), batch.len(), dimension, output.len());
        let x_norm = norm_l2(x);
        for (distance, y) in output.iter_mut().zip(batch.chunks_exact(dimension)) {
            *distance = Self::cosine_fast(x, x_norm, y);
        }
    }
}

impl Cosine for u8 {
    #[inline]
    fn cosine(x: &[Self], other: &[Self]) -> f32 {
        super::cosine_u8::cosine_u8(x, other)
    }
}

#[cfg(feature = "fp16kernels")]
mod bf16_kernel {
    use half::bf16;

    // These are the `cosine_bf16` function in bf16.c. Our build.rs script compiles
    // a version of this file for each SIMD level with different suffixes.
    unsafe extern "C" {
        #[cfg(target_arch = "aarch64")]
        pub fn cosine_bf16_neon(x: *const bf16, x_norm: f32, y: *const bf16, dimension: u32)
        -> f32;
        #[cfg(all(kernel_support = "avx512_bf16", target_arch = "x86_64"))]
        pub fn cosine_bf16_avx512(
            x: *const bf16,
            x_norm: f32,
            y: *const bf16,
            dimension: u32,
        ) -> f32;
        #[cfg(target_arch = "x86_64")]
        pub fn cosine_bf16_avx2(x: *const bf16, x_norm: f32, y: *const bf16, dimension: u32)
        -> f32;
        #[cfg(target_arch = "loongarch64")]
        pub fn cosine_bf16_lsx(x: *const bf16, x_norm: f32, y: *const bf16, dimension: u32) -> f32;
        #[cfg(target_arch = "loongarch64")]
        pub fn cosine_bf16_lasx(x: *const bf16, x_norm: f32, y: *const bf16, dimension: u32)
        -> f32;
    }
}

impl Cosine for bf16 {
    fn cosine_fast(x: &[Self], x_norm: f32, y: &[Self]) -> f32 {
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
                bf16_kernel::cosine_bf16_neon(x.as_ptr(), x_norm, y.as_ptr(), y.len() as u32)
            },
            #[cfg(all(
                feature = "fp16kernels",
                kernel_support = "avx512_bf16",
                target_arch = "x86_64"
            ))]
            HalfBackend::Avx512 => unsafe {
                bf16_kernel::cosine_bf16_avx512(x.as_ptr(), x_norm, y.as_ptr(), y.len() as u32)
            },
            #[cfg(all(feature = "fp16kernels", target_arch = "x86_64"))]
            HalfBackend::Avx2 => unsafe {
                bf16_kernel::cosine_bf16_avx2(x.as_ptr(), x_norm, y.as_ptr(), y.len() as u32)
            },
            #[cfg(all(feature = "fp16kernels", target_arch = "loongarch64"))]
            HalfBackend::Lasx => unsafe {
                bf16_kernel::cosine_bf16_lasx(x.as_ptr(), x_norm, y.as_ptr(), y.len() as u32)
            },
            #[cfg(all(feature = "fp16kernels", target_arch = "loongarch64"))]
            HalfBackend::Lsx => unsafe {
                bf16_kernel::cosine_bf16_lsx(x.as_ptr(), x_norm, y.as_ptr(), y.len() as u32)
            },
            // SimdSupport::AvxFma and SimdSupport::Avx fall through here:
            // the bf16 C kernels in `bf16_kernel::*` are compiled with
            // `-march=haswell` minimum (which requires AVX2), so they cannot
            // run on AVX-only or AVX+FMA hosts. Scalar is the correct route.
            _ => cosine_scalar(x, x_norm, y),
        }
    }
}

#[cfg(feature = "fp16kernels")]
mod kernel {
    use super::*;

    // These are the `cosine_f16` function in f16.c. Our build.rs script compiles
    // a version of this file for each SIMD level with different suffixes.
    unsafe extern "C" {
        #[cfg(target_arch = "aarch64")]
        pub fn cosine_f16_neon(x: *const f16, x_norm: f32, y: *const f16, dimension: u32) -> f32;
        #[cfg(all(kernel_support = "avx512_f16", target_arch = "x86_64"))]
        pub fn cosine_f16_avx512(x: *const f16, x_norm: f32, y: *const f16, dimension: u32) -> f32;
        #[cfg(target_arch = "x86_64")]
        pub fn cosine_f16_avx2(x: *const f16, x_norm: f32, y: *const f16, dimension: u32) -> f32;
        #[cfg(target_arch = "loongarch64")]
        pub fn cosine_f16_lsx(x: *const f16, x_norm: f32, y: *const f16, dimension: u32) -> f32;
        #[cfg(target_arch = "loongarch64")]
        pub fn cosine_f16_lasx(x: *const f16, x_norm: f32, y: *const f16, dimension: u32) -> f32;
    }
}

impl Cosine for f16 {
    fn cosine_fast(x: &[Self], x_norm: f32, y: &[Self]) -> f32 {
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
                kernel::cosine_f16_neon(x.as_ptr(), x_norm, y.as_ptr(), y.len() as u32)
            },
            #[cfg(all(
                feature = "fp16kernels",
                kernel_support = "avx512_f16",
                target_arch = "x86_64"
            ))]
            HalfBackend::Avx512 => unsafe {
                kernel::cosine_f16_avx512(x.as_ptr(), x_norm, y.as_ptr(), y.len() as u32)
            },
            #[cfg(all(feature = "fp16kernels", target_arch = "x86_64"))]
            HalfBackend::Avx2 => unsafe {
                kernel::cosine_f16_avx2(x.as_ptr(), x_norm, y.as_ptr(), y.len() as u32)
            },
            #[cfg(all(feature = "fp16kernels", target_arch = "loongarch64"))]
            HalfBackend::Lasx => unsafe {
                kernel::cosine_f16_lasx(x.as_ptr(), x_norm, y.as_ptr(), y.len() as u32)
            },
            #[cfg(all(feature = "fp16kernels", target_arch = "loongarch64"))]
            HalfBackend::Lsx => unsafe {
                kernel::cosine_f16_lsx(x.as_ptr(), x_norm, y.as_ptr(), y.len() as u32)
            },
            // SimdSupport::AvxFma and SimdSupport::Avx retain their scalar
            // route; this fallback only extends the tiers the C kernel already
            // served to Avx512FP16 after checking F16C and FMA.
            _ => cosine_scalar(x, x_norm, y),
        }
    }
}

/// Cosine distance from `x`, whose L2 norm is `x_norm`, to `y`.
type CosineKernel<T> = unsafe fn(&[T], f32, &[T]) -> f32;

/// Cosine distance between `x` and `y` given both L2 norms, in the argument
/// order of [`Cosine::cosine_with_norms`].
type CosineWithNormsKernel = unsafe fn(&[f32], f32, f32, &[f32]) -> f32;

impl Cosine for f32 {
    #[inline]
    fn cosine_fast(x: &[Self], x_norm: Self, other: &[Self]) -> f32 {
        assert_equal_lengths(x.len(), other.len());
        COSINE_F32.get().call(x, x_norm, other)
    }

    #[inline]
    fn cosine_with_norms(x: &[Self], x_norm: Self, y_norm: Self, y: &[Self]) -> Self {
        assert_equal_lengths(x.len(), y.len());
        COSINE_WITH_NORMS_F32.get().call(x, x_norm, y_norm, y)
    }

    fn cosine_batch_into(x: &[Self], batch: &[Self], dimension: usize, output: &mut [f32]) {
        assert_batch_output_layout(x.len(), batch.len(), dimension, output.len());
        COSINE_F32_BATCH.get().call(x, batch, dimension, output);
    }
}

impl Cosine for f64 {
    #[inline]
    fn cosine_fast(x: &[Self], x_norm: f32, y: &[Self]) -> f32 {
        assert_equal_lengths(x.len(), y.len());
        COSINE_F64.get().call(x, x_norm, y)
    }
}

lance_core::dispatch! {
    static COSINE_F32: Dispatch<CosineKernel<f32>> = {
        #[cfg(target_arch = "x86_64")]
        {
            &[
                x86::COSINE_F32_AVX512,
                x86::COSINE_F32_AVX_FMA,
                x86::COSINE_F32_AVX,
                COSINE_F32_PORTABLE,
            ]
        }
        #[cfg(not(target_arch = "x86_64"))]
        {
            &[COSINE_F32_PORTABLE]
        }
    };
}

lance_core::dispatch! {
    static COSINE_WITH_NORMS_F32: Dispatch<CosineWithNormsKernel> = {
        #[cfg(target_arch = "x86_64")]
        {
            &[
                x86::COSINE_WITH_NORMS_F32_AVX512,
                x86::COSINE_WITH_NORMS_F32_AVX_FMA,
                x86::COSINE_WITH_NORMS_F32_AVX,
                COSINE_WITH_NORMS_F32_PORTABLE,
            ]
        }
        #[cfg(not(target_arch = "x86_64"))]
        {
            &[COSINE_WITH_NORMS_F32_PORTABLE]
        }
    };
}

lance_core::dispatch! {
    /// Unlike `L2_F32_BATCH`, this has an AVX-512 candidate: Haswell builds
    /// have always used the AVX-512 kernel on AVX-512 hosts outside the 8- and
    /// 16-dimension paths. Keeping those results means Haswell builds select
    /// at runtime, at one indirect call per batch.
    static COSINE_F32_BATCH: Dispatch<BatchKernel> = {
        #[cfg(target_arch = "x86_64")]
        {
            &[
                x86::COSINE_F32_BATCH_AVX512,
                x86::COSINE_F32_BATCH_AVX_FMA,
                x86::COSINE_F32_BATCH_AVX,
                COSINE_F32_BATCH_PORTABLE,
            ]
        }
        #[cfg(not(target_arch = "x86_64"))]
        {
            &[COSINE_F32_BATCH_PORTABLE]
        }
    };
}

lance_core::dispatch! {
    static COSINE_F64: Dispatch<CosineKernel<f64>> = {
        #[cfg(target_arch = "x86_64")]
        {
            &[
                x86::COSINE_F64_AVX512,
                x86::COSINE_F64_AVX_FMA,
                x86::COSINE_F64_AVX,
                COSINE_F64_PORTABLE,
            ]
        }
        #[cfg(not(target_arch = "x86_64"))]
        {
            &[COSINE_F64_PORTABLE]
        }
    };
}

#[cfg(target_arch = "x86_64")]
lance_core::target_kernel! {
    #[target_features()]
    #[candidate(COSINE_F32_PORTABLE: CosineKernel<f32>)]
    #[inline]
    fn cosine_f32_portable(x: &[f32], x_norm: f32, y: &[f32]) -> f32 {
        cosine_f32_from_sums(x, x_norm, y, dot(x, y), dot(y, y))
    }
}

#[cfg(target_arch = "x86_64")]
lance_core::target_kernel! {
    #[target_features()]
    #[candidate(COSINE_WITH_NORMS_F32_PORTABLE: CosineWithNormsKernel)]
    #[inline]
    fn cosine_with_norms_f32_portable(x: &[f32], x_norm: f32, y_norm: f32, y: &[f32]) -> f32 {
        cosine_f32_with_norms(x, x_norm, y, dot(x, y), y_norm)
    }
}

#[cfg(target_arch = "x86_64")]
lance_core::target_kernel! {
    #[target_features()]
    #[candidate(COSINE_F32_BATCH_PORTABLE: BatchKernel)]
    fn cosine_batch_f32_portable(x: &[f32], batch: &[f32], dimension: usize, output: &mut [f32]) {
        let x_norm = norm_l2(x);
        // Dim 8 uses the AVX candidates' SSE path so results stay bit-identical
        // across candidates. Dim 16 can't use their `f32x16` without AVX, so
        // it folds in scalar instead.
        if dimension == 8
            && let Ok(key) = <&[f32; 8]>::try_from(x)
        {
            let key = x86::Sse8::new(key);
            for (distance, y) in output.iter_mut().zip(batch.as_chunks::<8>().0) {
                *distance = key.cosine(x_norm, y);
            }
        } else if dimension == 16
            && let Ok(key) = <&[f32; 16]>::try_from(x)
        {
            for (distance, y) in output.iter_mut().zip(batch.as_chunks::<16>().0) {
                let (xy, y2) = key
                    .iter()
                    .zip(y)
                    .fold((0.0f32, 0.0f32), |(xy, y2), (&a, &b)| (xy + a * b, y2 + b * b));
                *distance = cosine_f32_from_sums(x, x_norm, y, xy, y2);
            }
        } else {
            for (distance, y) in output.iter_mut().zip(batch.chunks_exact(dimension)) {
                *distance = cosine_f32_from_sums(x, x_norm, y, dot(x, y), dot(y, y));
            }
        }
    }
}

#[cfg(target_arch = "x86_64")]
lance_core::target_kernel! {
    #[target_features()]
    #[candidate(COSINE_F64_PORTABLE: CosineKernel<f64>)]
    #[inline]
    fn cosine_f64_portable(x: &[f64], x_norm: f32, y: &[f64]) -> f32 {
        cosine_scalar(x, x_norm, y)
    }
}

#[cfg(target_arch = "x86_64")]
mod x86 {
    use std::arch::x86_64::*;

    use super::{
        CosineKernel, CosineWithNormsKernel, cosine_f32_from_sums, cosine_f32_with_norms, dot,
        norm_l2,
    };
    use crate::distance::BatchKernel;
    use crate::simd::f32::{f32x8, f32x16};
    use crate::simd::f64::{f64x4, f64x8};
    use crate::simd::x86::{hsum256_pd, hsum256_ps};
    use crate::simd::{FloatSimd, SIMD};

    /// A preloaded 8-dimension key for every x86 batch kernel. SSE is part of
    /// the x86_64 baseline, so this needs no runtime selection. Its reduction
    /// order matches `f32x8::reduce_sum`.
    pub(super) struct Sse8<'a> {
        key: &'a [f32; 8],
        lo: __m128,
        hi: __m128,
    }

    impl<'a> Sse8<'a> {
        #[inline]
        pub(super) fn new(key: &'a [f32; 8]) -> Self {
            // SAFETY: both loads read within the eight-element array, and
            // x86_64 guarantees SSE.
            unsafe {
                Self {
                    key,
                    lo: _mm_loadu_ps(key.as_ptr()),
                    hi: _mm_loadu_ps(key.as_ptr().add(4)),
                }
            }
        }

        #[inline]
        pub(super) fn cosine(&self, key_norm: f32, y: &[f32; 8]) -> f32 {
            // SAFETY: both loads read within the eight-element array, and
            // x86_64 guarantees SSE.
            unsafe {
                let (lo, hi) = (_mm_loadu_ps(y.as_ptr()), _mm_loadu_ps(y.as_ptr().add(4)));
                let xy = _mm_add_ps(_mm_mul_ps(self.lo, lo), _mm_mul_ps(self.hi, hi));
                let y2 = _mm_add_ps(_mm_mul_ps(lo, lo), _mm_mul_ps(hi, hi));
                cosine_f32_from_sums(self.key, key_norm, y, hsum128_ps(xy), hsum128_ps(y2))
            }
        }
    }

    /// # Safety
    ///
    /// None beyond SSE, which x86_64 guarantees; `unsafe` only because the
    /// intrinsics are `#[target_feature(enable = "sse")]`.
    #[inline]
    unsafe fn hsum128_ps(values: __m128) -> f32 {
        unsafe {
            let sum64 = _mm_add_ps(values, _mm_movehl_ps(values, values));
            let sum32 = _mm_add_ss(sum64, _mm_shuffle_ps(sum64, sum64, 0x55));
            _mm_cvtss_f32(sum32)
        }
    }

    /// Fills `output` for the 8- and 16-dimension PQ widths shared by every
    /// AVX batch kernel, returning `false` for any other layout.
    #[target_feature(enable = "avx")]
    #[inline]
    fn cosine_batch_f32_narrow(
        x: &[f32],
        x_norm: f32,
        batch: &[f32],
        dimension: usize,
        output: &mut [f32],
    ) -> bool {
        if dimension == 8
            && let Ok(key) = <&[f32; 8]>::try_from(x)
        {
            // The SSE path benchmarks faster than `f32x8` at this width and
            // keeps every candidate bit-identical.
            let key = Sse8::new(key);
            for (distance, y) in output.iter_mut().zip(batch.as_chunks::<8>().0) {
                *distance = key.cosine(x_norm, y);
            }
            true
        } else if dimension == 16
            && let Ok(key) = <&[f32; 16]>::try_from(x)
        {
            // SAFETY: the load reads within the sixteen-element array.
            let key = unsafe { f32x16::load_unaligned(key.as_ptr()) };
            for (distance, y) in output.iter_mut().zip(batch.as_chunks::<16>().0) {
                // SAFETY: the load reads within the sixteen-element array.
                let yv = unsafe { f32x16::load_unaligned(y.as_ptr()) };
                let xy = key * yv;
                let y2 = yv * yv;
                *distance = cosine_f32_from_sums(x, x_norm, y, xy.reduce_sum(), y2.reduce_sum());
            }
            true
        } else {
            false
        }
    }

    lance_core::target_kernel! {
        #[target_features("avx512f")]
        #[candidate(pub(super) COSINE_F32_BATCH_AVX512: BatchKernel)]
        pub(super) fn cosine_batch_f32_avx512(
            x: &[f32],
            batch: &[f32],
            dimension: usize,
            output: &mut [f32],
        ) {
            let x_norm = norm_l2(x);
            if !cosine_batch_f32_narrow(x, x_norm, batch, dimension, output) {
                for (distance, y) in output.iter_mut().zip(batch.chunks_exact(dimension)) {
                    *distance = cosine_f32_avx512(x, x_norm, y);
                }
            }
        }
    }

    lance_core::target_kernel! {
        #[target_features("avx", "fma")]
        #[candidate(pub(super) COSINE_F32_BATCH_AVX_FMA: BatchKernel)]
        pub(super) fn cosine_batch_f32_avx_fma(
            x: &[f32],
            batch: &[f32],
            dimension: usize,
            output: &mut [f32],
        ) {
            let x_norm = norm_l2(x);
            if !cosine_batch_f32_narrow(x, x_norm, batch, dimension, output) {
                for (distance, y) in output.iter_mut().zip(batch.chunks_exact(dimension)) {
                    *distance = cosine_f32_avx_fma(x, x_norm, y);
                }
            }
        }
    }

    lance_core::target_kernel! {
        #[target_features("avx")]
        #[candidate(pub(super) COSINE_F32_BATCH_AVX: BatchKernel)]
        pub(super) fn cosine_batch_f32_avx(
            x: &[f32],
            batch: &[f32],
            dimension: usize,
            output: &mut [f32],
        ) {
            let x_norm = norm_l2(x);
            if !cosine_batch_f32_narrow(x, x_norm, batch, dimension, output) {
                for (distance, y) in output.iter_mut().zip(batch.chunks_exact(dimension)) {
                    *distance = cosine_f32_avx(x, x_norm, y);
                }
            }
        }
    }

    lance_core::target_kernel! {
        #[target_features("avx512f")]
        #[candidate(pub(super) COSINE_F32_AVX512: CosineKernel<f32>)]
        /// 16-wide `__m512` xy/yy with `vfmadd231ps` per iteration.
        #[inline]
        pub(super) fn cosine_f32_avx512(x: &[f32], x_norm: f32, other: &[f32]) -> f32 {
            // Callers pass equal lengths; `min` keeps the unchecked loads in
            // bounds for any input, as a safe function must.
            let dim = x.len().min(other.len());
            let unrolled_len = dim / 16 * 16;

            let mut acc_xy = _mm512_setzero_ps();
            let mut acc_yy = _mm512_setzero_ps();
            for i in (0..unrolled_len).step_by(16) {
                // SAFETY: `i + 16 <= dim`, which is in bounds for both slices.
                let (xv, yv) = unsafe {
                    (
                        _mm512_loadu_ps(x.as_ptr().add(i)),
                        _mm512_loadu_ps(other.as_ptr().add(i)),
                    )
                };
                acc_xy = _mm512_fmadd_ps(xv, yv, acc_xy);
                acc_yy = _mm512_fmadd_ps(yv, yv, acc_yy);
            }

            let mut xy = _mm512_reduce_add_ps(acc_xy);
            let mut yy = _mm512_reduce_add_ps(acc_yy);
            for i in unrolled_len..dim {
                xy += x[i] * other[i];
                yy += other[i] * other[i];
            }

            cosine_f32_from_sums(x, x_norm, other, xy, yy)
        }
    }

    lance_core::target_kernel! {
        #[target_features("avx", "fma")]
        #[candidate(pub(super) COSINE_F32_AVX_FMA: CosineKernel<f32>)]
        /// Covers both AVX+FMA and AVX2 hosts (body uses no AVX2-specific intrinsics).
        #[inline]
        pub(super) fn cosine_f32_avx_fma(x: &[f32], x_norm: f32, other: &[f32]) -> f32 {
            // See `cosine_f32_avx512` for `min`.
            let dim = x.len().min(other.len());
            let unrolled_len = dim / 16 * 16;
            let mut y_norm16 = f32x16::zeros();
            let mut xy16 = f32x16::zeros();
            for i in (0..unrolled_len).step_by(16) {
                // SAFETY: `i + 16 <= dim`, which is in bounds for both slices.
                let (xv, yv) = unsafe {
                    (
                        f32x16::load_unaligned(x.as_ptr().add(i)),
                        f32x16::load_unaligned(other.as_ptr().add(i)),
                    )
                };
                xy16.multiply_add(xv, yv);
                y_norm16.multiply_add(yv, yv);
            }
            let aligned_len = dim / 8 * 8;
            let mut y_norm8 = f32x8::zeros();
            let mut xy8 = f32x8::zeros();
            for i in (unrolled_len..aligned_len).step_by(8) {
                // SAFETY: `i + 8 <= dim`, which is in bounds for both slices.
                let (xv, yv) = unsafe {
                    (
                        f32x8::load_unaligned(x.as_ptr().add(i)),
                        f32x8::load_unaligned(other.as_ptr().add(i)),
                    )
                };
                xy8.multiply_add(xv, yv);
                y_norm8.multiply_add(yv, yv);
            }
            let y_norm =
                y_norm16.reduce_sum() + y_norm8.reduce_sum() + norm_l2(&other[aligned_len..]).powi(2);
            let xy =
                xy16.reduce_sum() + xy8.reduce_sum() + dot(&x[aligned_len..], &other[aligned_len..]);
            cosine_f32_from_sums(x, x_norm, other, xy, y_norm)
        }
    }

    lance_core::target_kernel! {
        #[target_features("avx")]
        #[candidate(pub(super) COSINE_F32_AVX: CosineKernel<f32>)]
        /// AVX-only path (no FMA) for Sandy/Ivy Bridge.
        #[inline]
        pub(super) fn cosine_f32_avx(x: &[f32], x_norm: f32, other: &[f32]) -> f32 {
            // See `cosine_f32_avx512` for `min`.
            let dim = x.len().min(other.len());
            let aligned_len = dim / 8 * 8;

            let mut acc_xy = _mm256_setzero_ps();
            let mut acc_yy = _mm256_setzero_ps();
            for i in (0..aligned_len).step_by(8) {
                // SAFETY: `i + 8 <= dim`, which is in bounds for both slices.
                let (xv, yv) = unsafe {
                    (
                        _mm256_loadu_ps(x.as_ptr().add(i)),
                        _mm256_loadu_ps(other.as_ptr().add(i)),
                    )
                };
                acc_xy = _mm256_add_ps(acc_xy, _mm256_mul_ps(xv, yv));
                acc_yy = _mm256_add_ps(acc_yy, _mm256_mul_ps(yv, yv));
            }

            // SAFETY: AVX is enabled for this function.
            let (xy_main, yy_main) = unsafe { (hsum256_ps(acc_xy), hsum256_ps(acc_yy)) };

            let y_norm = yy_main + norm_l2(&other[aligned_len..]).powi(2);
            let xy = xy_main + dot(&x[aligned_len..], &other[aligned_len..]);
            cosine_f32_from_sums(x, x_norm, other, xy, y_norm)
        }
    }

    lance_core::target_kernel! {
        #[target_features("avx512f")]
        #[candidate(pub(super) COSINE_WITH_NORMS_F32_AVX512: CosineWithNormsKernel)]
        /// 16-wide `__m512` with `vfmadd231ps` per iteration.
        #[inline]
        pub(super) fn cosine_with_norms_f32_avx512(
            x: &[f32],
            x_norm: f32,
            y_norm: f32,
            y: &[f32],
        ) -> f32 {
            // See `cosine_f32_avx512` for `min`.
            let dim = x.len().min(y.len());
            let unrolled_len = dim / 16 * 16;

            let mut acc = _mm512_setzero_ps();
            for i in (0..unrolled_len).step_by(16) {
                // SAFETY: `i + 16 <= dim`, which is in bounds for both slices.
                let (xv, yv) = unsafe {
                    (
                        _mm512_loadu_ps(x.as_ptr().add(i)),
                        _mm512_loadu_ps(y.as_ptr().add(i)),
                    )
                };
                acc = _mm512_fmadd_ps(xv, yv, acc);
            }

            let mut xy = _mm512_reduce_add_ps(acc);
            for i in unrolled_len..dim {
                xy += x[i] * y[i];
            }

            cosine_f32_with_norms(x, x_norm, y, xy, y_norm)
        }
    }

    lance_core::target_kernel! {
        #[target_features("avx", "fma")]
        #[candidate(pub(super) COSINE_WITH_NORMS_F32_AVX_FMA: CosineWithNormsKernel)]
        /// Covers both AVX+FMA and AVX2 hosts (body uses no AVX2-specific intrinsics).
        #[inline]
        pub(super) fn cosine_with_norms_f32_avx_fma(
            x: &[f32],
            x_norm: f32,
            y_norm: f32,
            y: &[f32],
        ) -> f32 {
            // See `cosine_f32_avx512` for `min`.
            let dim = x.len().min(y.len());
            let unrolled_len = dim / 16 * 16;
            let mut xy16 = f32x16::zeros();
            for i in (0..unrolled_len).step_by(16) {
                // SAFETY: `i + 16 <= dim`, which is in bounds for both slices.
                let (xv, yv) = unsafe {
                    (
                        f32x16::load_unaligned(x.as_ptr().add(i)),
                        f32x16::load_unaligned(y.as_ptr().add(i)),
                    )
                };
                xy16.multiply_add(xv, yv);
            }
            let aligned_len = dim / 8 * 8;
            let mut xy8 = f32x8::zeros();
            for i in (unrolled_len..aligned_len).step_by(8) {
                // SAFETY: `i + 8 <= dim`, which is in bounds for both slices.
                let (xv, yv) = unsafe {
                    (
                        f32x8::load_unaligned(x.as_ptr().add(i)),
                        f32x8::load_unaligned(y.as_ptr().add(i)),
                    )
                };
                xy8.multiply_add(xv, yv);
            }
            let xy =
                xy16.reduce_sum() + xy8.reduce_sum() + dot(&x[aligned_len..], &y[aligned_len..]);
            cosine_f32_with_norms(x, x_norm, y, xy, y_norm)
        }
    }

    lance_core::target_kernel! {
        #[target_features("avx")]
        #[candidate(pub(super) COSINE_WITH_NORMS_F32_AVX: CosineWithNormsKernel)]
        /// AVX-only path (no FMA) for Sandy/Ivy Bridge.
        #[inline]
        pub(super) fn cosine_with_norms_f32_avx(
            x: &[f32],
            x_norm: f32,
            y_norm: f32,
            y: &[f32],
        ) -> f32 {
            // See `cosine_f32_avx512` for `min`.
            let dim = x.len().min(y.len());
            let aligned_len = dim / 8 * 8;

            let mut acc = _mm256_setzero_ps();
            for i in (0..aligned_len).step_by(8) {
                // SAFETY: `i + 8 <= dim`, which is in bounds for both slices.
                let (xv, yv) = unsafe {
                    (
                        _mm256_loadu_ps(x.as_ptr().add(i)),
                        _mm256_loadu_ps(y.as_ptr().add(i)),
                    )
                };
                acc = _mm256_add_ps(acc, _mm256_mul_ps(xv, yv));
            }

            // SAFETY: AVX is enabled for this function.
            let xy_main = unsafe { hsum256_ps(acc) };
            let xy = xy_main + dot(&x[aligned_len..], &y[aligned_len..]);
            cosine_f32_with_norms(x, x_norm, y, xy, y_norm)
        }
    }

    lance_core::target_kernel! {
        #[target_features("avx512f")]
        #[candidate(pub(super) COSINE_F64_AVX512: CosineKernel<f64>)]
        /// 8-wide `__m512d` xy/yy with `vfmadd231pd` per iteration.
        #[inline]
        pub(super) fn cosine_f64_avx512(x: &[f64], x_norm: f32, y: &[f64]) -> f32 {
            // See `cosine_f32_avx512` for `min`.
            let dim = x.len().min(y.len());
            let unrolled_len = dim / 8 * 8;

            let mut acc_xy = _mm512_setzero_pd();
            let mut acc_yy = _mm512_setzero_pd();
            for i in (0..unrolled_len).step_by(8) {
                // SAFETY: `i + 8 <= dim`, which is in bounds for both slices.
                let (xv, yv) = unsafe {
                    (
                        _mm512_loadu_pd(x.as_ptr().add(i)),
                        _mm512_loadu_pd(y.as_ptr().add(i)),
                    )
                };
                acc_xy = _mm512_fmadd_pd(xv, yv, acc_xy);
                acc_yy = _mm512_fmadd_pd(yv, yv, acc_yy);
            }

            let mut xy = _mm512_reduce_add_pd(acc_xy);
            let mut yy = _mm512_reduce_add_pd(acc_yy);
            for i in unrolled_len..dim {
                xy += x[i] * y[i];
                yy += y[i] * y[i];
            }

            let y_norm_sq = yy as f32;
            let xy_f32 = xy as f32;
            1.0 - xy_f32 / x_norm / y_norm_sq.sqrt()
        }
    }

    lance_core::target_kernel! {
        #[target_features("avx", "fma")]
        #[candidate(pub(super) COSINE_F64_AVX_FMA: CosineKernel<f64>)]
        /// Covers both AVX+FMA and AVX2 hosts (body uses no AVX2-specific intrinsics).
        #[inline]
        pub(super) fn cosine_f64_avx_fma(x: &[f64], x_norm: f32, y: &[f64]) -> f32 {
            // See `cosine_f32_avx512` for `min`.
            let dim = x.len().min(y.len());
            let unrolled_len = dim / 8 * 8;
            let mut y_norm8 = f64x8::zeros();
            let mut xy8 = f64x8::zeros();
            for i in (0..unrolled_len).step_by(8) {
                // SAFETY: `i + 8 <= dim`, which is in bounds for both slices.
                let (xv, yv) = unsafe {
                    (
                        f64x8::load_unaligned(x.as_ptr().add(i)),
                        f64x8::load_unaligned(y.as_ptr().add(i)),
                    )
                };
                xy8.multiply_add(xv, yv);
                y_norm8.multiply_add(yv, yv);
            }
            let aligned_len = dim / 4 * 4;
            let mut y_norm4 = f64x4::zeros();
            let mut xy4 = f64x4::zeros();
            for i in (unrolled_len..aligned_len).step_by(4) {
                // SAFETY: `i + 4 <= dim`, which is in bounds for both slices.
                let (xv, yv) = unsafe {
                    (
                        f64x4::load_unaligned(x.as_ptr().add(i)),
                        f64x4::load_unaligned(y.as_ptr().add(i)),
                    )
                };
                xy4.multiply_add(xv, yv);
                y_norm4.multiply_add(yv, yv);
            }
            let tail_y_norm: f64 = y[aligned_len..].iter().map(|&v| v * v).sum();
            let tail_xy: f64 = x[aligned_len..]
                .iter()
                .zip(y[aligned_len..].iter())
                .map(|(&a, &b)| a * b)
                .sum();

            let y_norm_sq = (y_norm8.reduce_sum() + y_norm4.reduce_sum() + tail_y_norm) as f32;
            let xy = (xy8.reduce_sum() + xy4.reduce_sum() + tail_xy) as f32;
            1.0 - xy / x_norm / y_norm_sq.sqrt()
        }
    }

    lance_core::target_kernel! {
        #[target_features("avx")]
        #[candidate(pub(super) COSINE_F64_AVX: CosineKernel<f64>)]
        /// AVX-only path (no FMA) for Sandy/Ivy Bridge.
        #[inline]
        pub(super) fn cosine_f64_avx(x: &[f64], x_norm: f32, y: &[f64]) -> f32 {
            // See `cosine_f32_avx512` for `min`.
            let dim = x.len().min(y.len());
            let aligned_len = dim / 4 * 4;

            let mut acc_xy = _mm256_setzero_pd();
            let mut acc_yy = _mm256_setzero_pd();
            for i in (0..aligned_len).step_by(4) {
                // SAFETY: `i + 4 <= dim`, which is in bounds for both slices.
                let (xv, yv) = unsafe {
                    (
                        _mm256_loadu_pd(x.as_ptr().add(i)),
                        _mm256_loadu_pd(y.as_ptr().add(i)),
                    )
                };
                acc_xy = _mm256_add_pd(acc_xy, _mm256_mul_pd(xv, yv));
                acc_yy = _mm256_add_pd(acc_yy, _mm256_mul_pd(yv, yv));
            }

            // SAFETY: AVX is enabled for this function.
            let (xy_main, yy_main) = unsafe { (hsum256_pd(acc_xy), hsum256_pd(acc_yy)) };

            let tail_y_norm: f64 = y[aligned_len..].iter().map(|&v| v * v).sum();
            let tail_xy: f64 = x[aligned_len..]
                .iter()
                .zip(y[aligned_len..].iter())
                .map(|(&a, &b)| a * b)
                .sum();

            let y_norm_sq = (yy_main + tail_y_norm) as f32;
            let xy = (xy_main + tail_xy) as f32;
            1.0 - xy / x_norm / y_norm_sq.sqrt()
        }
    }
}

#[cfg(not(target_arch = "x86_64"))]
lance_core::target_kernel! {
    #[target_features()]
    #[candidate(COSINE_F32_PORTABLE: CosineKernel<f32>)]
    /// `simd::f32` is backed by the baseline NEON or LSX on these architectures.
    #[inline]
    fn cosine_f32_portable(x: &[f32], x_norm: f32, other: &[f32]) -> f32 {
        // Callers pass equal lengths; `min` keeps the unchecked loads in
        // bounds for any input, as a safe function must.
        let dim = x.len().min(other.len());
        let unrolled_len = dim / 16 * 16;
        let mut y_norm16 = f32x16::zeros();
        let mut xy16 = f32x16::zeros();
        for i in (0..unrolled_len).step_by(16) {
            // SAFETY: `i + 16 <= dim`, which is in bounds for both slices.
            let (xv, yv) = unsafe {
                (
                    f32x16::load_unaligned(x.as_ptr().add(i)),
                    f32x16::load_unaligned(other.as_ptr().add(i)),
                )
            };
            xy16.multiply_add(xv, yv);
            y_norm16.multiply_add(yv, yv);
        }
        let aligned_len = dim / 8 * 8;
        let mut y_norm8 = f32x8::zeros();
        let mut xy8 = f32x8::zeros();
        for i in (unrolled_len..aligned_len).step_by(8) {
            // SAFETY: `i + 8 <= dim`, which is in bounds for both slices.
            let (xv, yv) = unsafe {
                (
                    f32x8::load_unaligned(x.as_ptr().add(i)),
                    f32x8::load_unaligned(other.as_ptr().add(i)),
                )
            };
            xy8.multiply_add(xv, yv);
            y_norm8.multiply_add(yv, yv);
        }
        let y_norm =
            y_norm16.reduce_sum() + y_norm8.reduce_sum() + norm_l2(&other[aligned_len..]).powi(2);
        let xy = xy16.reduce_sum() + xy8.reduce_sum() + dot(&x[aligned_len..], &other[aligned_len..]);
        cosine_f32_from_sums(x, x_norm, other, xy, y_norm)
    }
}

#[cfg(not(target_arch = "x86_64"))]
lance_core::target_kernel! {
    #[target_features()]
    #[candidate(COSINE_WITH_NORMS_F32_PORTABLE: CosineWithNormsKernel)]
    /// See `cosine_f32_portable` for the SIMD backends.
    #[inline]
    fn cosine_with_norms_f32_portable(x: &[f32], x_norm: f32, y_norm: f32, y: &[f32]) -> f32 {
        // See `cosine_f32_portable` for `min`.
        let dim = x.len().min(y.len());
        let unrolled_len = dim / 16 * 16;
        let mut xy16 = f32x16::zeros();
        for i in (0..unrolled_len).step_by(16) {
            // SAFETY: `i + 16 <= dim`, which is in bounds for both slices.
            let (xv, yv) = unsafe {
                (
                    f32x16::load_unaligned(x.as_ptr().add(i)),
                    f32x16::load_unaligned(y.as_ptr().add(i)),
                )
            };
            xy16.multiply_add(xv, yv);
        }
        let aligned_len = dim / 8 * 8;
        let mut xy8 = f32x8::zeros();
        for i in (unrolled_len..aligned_len).step_by(8) {
            // SAFETY: `i + 8 <= dim`, which is in bounds for both slices.
            let (xv, yv) = unsafe {
                (
                    f32x8::load_unaligned(x.as_ptr().add(i)),
                    f32x8::load_unaligned(y.as_ptr().add(i)),
                )
            };
            xy8.multiply_add(xv, yv);
        }
        let xy = xy16.reduce_sum() + xy8.reduce_sum() + dot(&x[aligned_len..], &y[aligned_len..]);
        cosine_f32_with_norms(x, x_norm, y, xy, y_norm)
    }
}

#[cfg(not(target_arch = "x86_64"))]
lance_core::target_kernel! {
    #[target_features()]
    #[candidate(COSINE_F32_BATCH_PORTABLE: BatchKernel)]
    /// See `cosine_f32_portable` for the SIMD backends.
    fn cosine_batch_f32_portable(x: &[f32], batch: &[f32], dimension: usize, output: &mut [f32]) {
        let x_norm = norm_l2(x);
        if dimension == 8
            && let Ok(key) = <&[f32; 8]>::try_from(x)
        {
            // SAFETY: the load reads within the eight-element array.
            let key = unsafe { f32x8::load_unaligned(key.as_ptr()) };
            for (distance, y) in output.iter_mut().zip(batch.as_chunks::<8>().0) {
                // SAFETY: the load reads within the eight-element array.
                let yv = unsafe { f32x8::load_unaligned(y.as_ptr()) };
                let xy = key * yv;
                let y2 = yv * yv;
                *distance = cosine_f32_from_sums(x, x_norm, y, xy.reduce_sum(), y2.reduce_sum());
            }
        } else if dimension == 16
            && let Ok(key) = <&[f32; 16]>::try_from(x)
        {
            // SAFETY: the load reads within the sixteen-element array.
            let key = unsafe { f32x16::load_unaligned(key.as_ptr()) };
            for (distance, y) in output.iter_mut().zip(batch.as_chunks::<16>().0) {
                // SAFETY: the load reads within the sixteen-element array.
                let yv = unsafe { f32x16::load_unaligned(y.as_ptr()) };
                let xy = key * yv;
                let y2 = yv * yv;
                *distance = cosine_f32_from_sums(x, x_norm, y, xy.reduce_sum(), y2.reduce_sum());
            }
        } else {
            for (distance, y) in output.iter_mut().zip(batch.chunks_exact(dimension)) {
                *distance = cosine_f32_portable(x, x_norm, y);
            }
        }
    }
}

#[cfg(not(target_arch = "x86_64"))]
lance_core::target_kernel! {
    #[target_features()]
    #[candidate(COSINE_F64_PORTABLE: CosineKernel<f64>)]
    /// `simd::f64` is backed by the baseline NEON or LSX on these architectures.
    #[inline]
    fn cosine_f64_portable(x: &[f64], x_norm: f32, y: &[f64]) -> f32 {
        use crate::simd::f64::{f64x4, f64x8};

        // See `cosine_f32_portable` for `min`.
        let dim = x.len().min(y.len());
        let unrolled_len = dim / 8 * 8;
        let mut y_norm8 = f64x8::zeros();
        let mut xy8 = f64x8::zeros();
        for i in (0..unrolled_len).step_by(8) {
            // SAFETY: `i + 8 <= dim`, which is in bounds for both slices.
            let (xv, yv) = unsafe {
                (
                    f64x8::load_unaligned(x.as_ptr().add(i)),
                    f64x8::load_unaligned(y.as_ptr().add(i)),
                )
            };
            xy8.multiply_add(xv, yv);
            y_norm8.multiply_add(yv, yv);
        }
        let aligned_len = dim / 4 * 4;
        let mut y_norm4 = f64x4::zeros();
        let mut xy4 = f64x4::zeros();
        for i in (unrolled_len..aligned_len).step_by(4) {
            // SAFETY: `i + 4 <= dim`, which is in bounds for both slices.
            let (xv, yv) = unsafe {
                (
                    f64x4::load_unaligned(x.as_ptr().add(i)),
                    f64x4::load_unaligned(y.as_ptr().add(i)),
                )
            };
            xy4.multiply_add(xv, yv);
            y_norm4.multiply_add(yv, yv);
        }
        let tail_y_norm: f64 = y[aligned_len..].iter().map(|&v| v * v).sum();
        let tail_xy: f64 = x[aligned_len..]
            .iter()
            .zip(y[aligned_len..].iter())
            .map(|(&a, &b)| a * b)
            .sum();

        let y_norm_sq = (y_norm8.reduce_sum() + y_norm4.reduce_sum() + tail_y_norm) as f32;
        let xy = (xy8.reduce_sum() + xy4.reduce_sum() + tail_xy) as f32;
        1.0 - xy / x_norm / y_norm_sq.sqrt()
    }
}

/// Keep ordinary vectors on the SIMD path. If squared norms approach the
/// underflow boundary or overflow, recompute from the original coordinates.
#[inline]
fn cosine_f32_from_sums(x: &[f32], x_norm: f32, y: &[f32], xy: f32, yy: f32) -> f32 {
    if cosine_f32_norm_in_range(x_norm * x_norm, x.len())
        && cosine_f32_norm_in_range(yy, y.len())
        && xy.is_finite()
    {
        1.0 - xy / x_norm / yy.sqrt()
    } else {
        cosine_f32_wide(x, y)
    }
}

#[inline]
fn cosine_f32_with_norms(x: &[f32], x_norm: f32, y: &[f32], xy: f32, y_norm: f32) -> f32 {
    if cosine_f32_norm_in_range(x_norm * x_norm, x.len())
        && cosine_f32_norm_in_range(y_norm * y_norm, y.len())
        && xy.is_finite()
    {
        1.0 - xy / x_norm / y_norm
    } else {
        cosine_f32_wide(x, y)
    }
}

#[inline]
fn cosine_f32_norm_in_range(squared_norm: f32, dimension: usize) -> bool {
    // A sum of many subnormal squares can itself be normal. Allow room for
    // their accumulated rounding error instead of checking only is_normal().
    squared_norm.is_normal() && squared_norm >= f32::MIN_POSITIVE * dimension as f32
}

/// Every finite f32 coordinate product and sum fits in f64, even for subnormal
/// coordinates. Widen before multiplication, and narrow only the final distance.
#[cold]
fn cosine_f32_wide(x: &[f32], y: &[f32]) -> f32 {
    let (mut xx, mut xy, mut yy) = (0.0_f64, 0.0_f64, 0.0_f64);
    for (&a, &b) in x.iter().zip(y) {
        let (a, b) = (f64::from(a), f64::from(b));
        xx += a * a;
        xy += a * b;
        yy += b * b;
    }
    // Limit roundoff for self matches; zero vectors still produce NaN.
    (1.0 - (xy / xx.sqrt() / yy.sqrt()).clamp(-1.0, 1.0)) as f32
}

/// Fallback non-SIMD implementation
#[inline]
fn cosine_scalar<T: Dot>(x: &[T], x_norm: f32, y: &[T]) -> f32 {
    let y_sq = dot(y, y);
    let xy = dot(x, y);
    // 1 - xy / (sqrt(x_sq) * sqrt(y_sq))
    1.0 - xy / (x_norm * y_sq.sqrt())
}

#[inline]
pub(crate) fn cosine_scalar_fast<T: Dot>(x: &[T], x_norm: f32, y: &[T], y_norm: f32) -> f32 {
    let xy = dot(x, y);
    // 1 - xy / (sqrt(x_sq) * sqrt(y_sq))
    // use f64 for overflow protection.
    1.0 - (xy / (x_norm * y_norm))
}

/// Cosine distance function between two vectors.
pub fn cosine_distance<T: Cosine>(from: &[T], to: &[T]) -> f32 {
    T::cosine(from, to)
}

/// Writes the [cosine distance](https://en.wikipedia.org/wiki/Cosine_similarity)
/// from `from` to each `dimension`-sized vector in `batch` into the matching
/// element of `output`.
///
/// # Panics
///
/// Panics if `from.len() != dimension`, if `batch.len()` is not a multiple of
/// `dimension`, or if `output.len() != batch.len() / dimension`.
///
/// ```
/// # use lance_linalg::distance::cosine_distance_batch_into;
/// let mut distances = [0.0; 2];
/// cosine_distance_batch_into(&[1.0_f32, 0.0], &[2.0, 0.0, 0.0, 3.0], 2, &mut distances);
/// assert_eq!(distances, [0.0, 1.0]);
/// ```
#[inline]
pub fn cosine_distance_batch_into<T: Cosine>(
    from: &[T],
    batch: &[T],
    dimension: usize,
    output: &mut [f32],
) {
    T::cosine_batch_into(from, batch, dimension, output);
}

/// Compute cosine distances, propagating the null buffer of `to`.
///
/// See [`super::DistanceType::batch`] for supported types and input validation.
#[deprecated(note = "Use DistanceType::Cosine.batch")]
pub fn cosine_distance_arrow_batch(
    from: &dyn Array,
    to: &FixedSizeListArray,
) -> Result<Arc<Float32Array>> {
    super::DistanceType::Cosine.batch(from, to).map(Arc::new)
}

/// Portable scalar reference cosine over f64 inputs. Used by tests to compare
/// against every cosine kernel candidate. Computes
/// `1 - xy / (x_norm * y_norm_sq.sqrt())` in f64 then casts to f32, matching
/// the reduction order of the dispatched kernels.
#[cfg(test)]
fn cosine_fast_scalar(x: &[f64], x_norm: f32, y: &[f64]) -> f32 {
    let xy: f64 = x.iter().zip(y.iter()).map(|(&a, &b)| a * b).sum();
    let y_norm_sq: f64 = y.iter().map(|&v| v * v).sum();
    1.0 - (xy as f32) / x_norm / (y_norm_sq as f32).sqrt()
}

/// Portable scalar reference cosine when both norms are known. Mirrors
/// `Cosine::cosine_with_norms` for testing.
#[cfg(test)]
fn cosine_with_norms_scalar(x: &[f64], x_norm: f32, y_norm: f32, y: &[f64]) -> f32 {
    let xy: f64 = x.iter().zip(y.iter()).map(|(&a, &b)| a * b).sum();
    1.0 - (xy as f32) / x_norm / y_norm
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::test_utils::{
        arbitrary_bf16, arbitrary_f16, arbitrary_f32, arbitrary_f64, arbitrary_vector_pair,
        dimension_shard, run_vector_pair_proptest,
    };
    use approx::assert_relative_eq;
    use num_traits::AsPrimitive;
    use proptest::prelude::*;

    fn cosine_dist_brute_force(x: &[f32], y: &[f32]) -> f32 {
        let xy = x
            .iter()
            .zip(y.iter())
            .map(|(&xi, &yi)| xi * yi)
            .sum::<f32>();
        let x_sq = x.iter().map(|&xi| xi * xi).sum::<f32>().sqrt();
        let y_sq = y.iter().map(|&yi| yi * yi).sum::<f32>().sqrt();
        1.0 - xy / x_sq / y_sq
    }

    #[test]
    fn half_cosine_rejects_mismatched_lengths_before_ffi() {
        let f16_x = [f16::from_f32(1.0)];
        let f16_y = [f16::from_f32(1.0), f16::from_f32(2.0)];
        assert!(
            std::panic::catch_unwind(|| <f16 as Cosine>::cosine_fast(&f16_x, 1.0, &f16_y)).is_err()
        );

        let bf16_x = [bf16::from_f32(1.0)];
        let bf16_y = [bf16::from_f32(1.0), bf16::from_f32(2.0)];
        assert!(
            std::panic::catch_unwind(|| <bf16 as Cosine>::cosine_fast(&bf16_x, 1.0, &bf16_y))
                .is_err()
        );
    }

    #[test]
    fn float_cosine_rejects_mismatched_lengths() {
        let f32_x = [1.0_f32];
        let f32_y = [1.0_f32, 2.0];
        assert!(
            std::panic::catch_unwind(|| <f32 as Cosine>::cosine_fast(&f32_x, 1.0, &f32_y)).is_err()
        );
        assert!(
            std::panic::catch_unwind(|| <f32 as Cosine>::cosine_with_norms(
                &f32_x, 1.0, 1.0, &f32_y
            ))
            .is_err()
        );

        let f64_x = [1.0_f64];
        let f64_y = [1.0_f64, 2.0];
        assert!(
            std::panic::catch_unwind(|| <f64 as Cosine>::cosine_fast(&f64_x, 1.0, &f64_y)).is_err()
        );
    }

    #[test]
    fn test_cosine() {
        let x: Float32Array = (1..9).map(|v| v as f32).collect();
        let y: Float32Array = (100..108).map(|v| v as f32).collect();
        let d = cosine_distance_batch_vec(x.values(), y.values(), 8);
        // from scipy.spatial.distance.cosine
        assert_relative_eq!(d[0], 1.0 - 0.900_957);

        let x = Float32Array::from_iter_values([3.0, 45.0, 7.0, 2.0, 5.0, 20.0, 13.0, 12.0]);
        let y = Float32Array::from_iter_values([2.0, 54.0, 13.0, 15.0, 22.0, 34.0, 50.0, 1.0]);
        let d = cosine_distance_batch_vec(x.values(), y.values(), 8);
        // from sklearn.metrics.pairwise import cosine_similarity
        assert_relative_eq!(d[0], 1.0 - 0.873_580_63);
    }

    #[test]
    fn test_cosine_large() {
        let total = 1024;
        let x = (0..total).map(|v| v as f32).collect::<Vec<_>>();
        let y = (1024..1024 + total).map(|v| v as f32).collect::<Vec<_>>();
        let d = cosine_distance_batch_vec(&x, &y, total);
        assert_relative_eq!(d[0], cosine_dist_brute_force(&x, &y));
    }

    #[test]
    fn test_cosine_not_aligned() {
        let x: Float32Array = vec![16_f32, 32_f32].into();
        let y: Float32Array = vec![1_f32, 2_f32, 4_f32, 8_f32].into();
        let d = cosine_distance_batch_vec(x.values(), y.values(), 2);
        assert_relative_eq!(d[0], 0.0);
        assert_relative_eq!(d[0], 0.0);
    }

    /// Reference implementation of cosine distance, plus error propagation.
    ///
    /// Pass `rel_err` to provide the allowed relative error in the dot product
    /// results. This function will then compute the expected absolute error.
    fn cosine_ref(x: &[f64], y: &[f64], rel_err: f64) -> (f32, f32) {
        let xy = x
            .iter()
            .zip(y.iter())
            .map(|(&xi, &yi)| xi * yi)
            .sum::<f64>();
        let x_sq = x.iter().map(|&xi| xi * xi).sum::<f64>().sqrt();
        let y_sq = y.iter().map(|&yi| yi * yi).sum::<f64>().sqrt();
        let expected = (1.0 - xy / x_sq / y_sq) as f32;

        let factor = 1.0 + rel_err;
        let low = (1.0 - (xy * factor) / (x_sq / factor) / (y_sq / factor)) as f32;
        let high = (1.0 - (xy / factor) / (x_sq * factor) / (y_sq * factor)) as f32;
        let low = (expected - low).abs();
        let high = (expected - high).abs();
        let error = low.max(high);

        (expected, error)
    }

    fn do_cosine_test<T: Cosine + AsPrimitive<f64>>(
        x: &[T],
        y: &[T],
    ) -> std::result::Result<(), TestCaseError> {
        let x_f64 = x.iter().map(|&v| v.as_()).collect::<Vec<_>>();
        let y_f64 = y.iter().map(|&v| v.as_()).collect::<Vec<_>>();

        let (expected, max_error) = cosine_ref(&x_f64, &y_f64, 1e-6);
        let result = T::cosine(x, y);

        prop_assert!(approx::relative_eq!(result, expected, epsilon = max_error));
        Ok(())
    }

    #[rstest::rstest]
    fn test_cosine_f32(
        #[values(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15)] shard: usize,
    ) {
        run_vector_pair_proptest(arbitrary_f32, dimension_shard(shard), |x, y| {
            prop_assume!(norm_l2(&x) > 1e-10);
            prop_assume!(norm_l2(&y) > 1e-10);
            do_cosine_test(&x, &y)
        });
    }

    #[rstest::rstest]
    fn test_cosine_f64(
        #[values(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15)] shard: usize,
    ) {
        run_vector_pair_proptest(arbitrary_f64, dimension_shard(shard), |x, y| {
            prop_assume!(norm_l2(&x) > 1e-20);
            prop_assume!(norm_l2(&y) > 1e-20);
            do_cosine_test(&x, &y)
        });
    }

    fn assert_extreme_cosine_kernels(x: &[f32], vectors: &[Vec<f32>]) {
        let expected: Vec<f32> = vectors
            .iter()
            .map(|y| {
                let xy: f64 = x
                    .iter()
                    .zip(y)
                    .map(|(&a, &b)| f64::from(a) * f64::from(b))
                    .sum();
                let xx: f64 = x.iter().map(|&a| f64::from(a).powi(2)).sum();
                let yy: f64 = y.iter().map(|&b| f64::from(b).powi(2)).sum();
                (1.0 - xy / (xx * yy).sqrt()) as f32
            })
            .collect();
        let x_norm = norm_l2(x);
        for (y, &want) in vectors.iter().zip(&expected) {
            assert_relative_eq!(f32::cosine(x, y), want, epsilon = 1e-6);
            assert_relative_eq!(
                f32::cosine_with_norms(x, x_norm, norm_l2(y), y),
                want,
                epsilon = 1e-6
            );
            for candidate in COSINE_F32.candidates() {
                if let Some(kernel) = candidate.kernel() {
                    let got = kernel.call(x, x_norm, y);
                    assert_relative_eq!(got, want, epsilon = 1e-6);
                }
            }
            for candidate in COSINE_WITH_NORMS_F32.candidates() {
                if let Some(kernel) = candidate.kernel() {
                    let got = kernel.call(x, x_norm, norm_l2(y), y);
                    assert_relative_eq!(got, want, epsilon = 1e-6);
                }
            }
        }
        let batch: Vec<_> = vectors.iter().flat_map(|y| y.iter().copied()).collect();
        for candidate in COSINE_F32_BATCH.candidates() {
            if let Some(kernel) = candidate.kernel() {
                let mut got = vec![f32::NAN; vectors.len()];
                kernel.call(x, &batch, x.len(), &mut got);
                for (&got, &want) in got.iter().zip(&expected) {
                    assert_relative_eq!(got, want, epsilon = 1e-6);
                }
            }
        }
    }

    #[rstest::rstest]
    fn test_cosine_f32_extreme_magnitudes(#[values(2, 8, 16, 33, 1024)] dimension: usize) {
        let scales = [
            f32::from_bits(1),
            f32::MIN_POSITIVE,
            1e-30,
            1e-20,
            1.0,
            1e20,
            f32::MAX,
        ];
        for x_scale in scales {
            for y_scale in scales {
                let mut x = vec![0.0; dimension];
                x[0] = x_scale;
                let mut y = vec![0.0; dimension];
                y[0] = y_scale;
                let mut angled = vec![0.0; dimension];
                angled[0] = 0.5 * y_scale;
                angled[1] = 0.8660254 * y_scale;
                let opposite = y.iter().map(|v| -v).collect();
                let mut orthogonal = vec![0.0; dimension];
                orthogonal[1] = y_scale;
                assert_extreme_cosine_kernels(&x, &[y, angled, opposite, orthogonal]);
            }
        }
    }

    #[rstest::rstest]
    fn test_cosine_f32_dense_extreme_magnitudes(#[values(8, 16, 33, 1024)] dimension: usize) {
        for scale in [1e-22, 1e-21, 1e-20, 1e18, 1e19, 1e20] {
            let x = vec![scale; dimension];
            let y = (0..dimension)
                .map(|i| {
                    if i % 2 == 0 {
                        scale * 0.5
                    } else {
                        -scale * 0.75
                    }
                })
                .collect();
            assert_extreme_cosine_kernels(&x, &[x.clone(), y]);
        }
    }

    /// Checks one cosine distance against an f64 reference. The absolute
    /// `epsilon` covers inputs with extreme dynamic range, where a near-zero
    /// result differs between f32 and f64 accumulation by more than
    /// `max_relative` of itself.
    fn check_cosine(
        got: f32,
        want: f32,
        kernel: &str,
        dimension: usize,
    ) -> std::result::Result<(), TestCaseError> {
        prop_assert!(
            approx::relative_eq!(got, want, max_relative = 1e-3, epsilon = 1e-6),
            "{kernel} at dim {dimension}: {got} != {want}"
        );
        Ok(())
    }

    /// Every host-supported f32 kernel, single-pair and batch, must agree with
    /// an f64 reference. Prefixes of length 8 and 16 pin the batch kernels'
    /// PQ-width fast paths regardless of the generated dimension.
    #[rstest::rstest]
    fn test_cosine_f32_kernels_match_reference(
        #[values(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15)] shard: usize,
    ) {
        run_vector_pair_proptest(arbitrary_f32, dimension_shard(shard), |x, y| {
            for dimension in [8, 16, x.len()].into_iter().filter(|&d| d <= x.len()) {
                let (x, y) = (&x[..dimension], &y[..dimension]);
                let (x_norm, y_norm) = (norm_l2(x), norm_l2(y));
                if x_norm <= 1e-10 || y_norm <= 1e-10 {
                    continue;
                }
                let x_f64: Vec<f64> = x.iter().map(|&v| v as f64).collect();
                let y_f64: Vec<f64> = y.iter().map(|&v| v as f64).collect();
                let want = cosine_fast_scalar(&x_f64, x_norm, &y_f64);
                let want_with_norms = cosine_with_norms_scalar(&x_f64, x_norm, y_norm, &y_f64);
                let want_self = cosine_fast_scalar(&x_f64, x_norm, &x_f64);

                for candidate in COSINE_F32.candidates() {
                    let Some(kernel) = candidate.kernel() else {
                        continue;
                    };
                    check_cosine(kernel.call(x, x_norm, y), want, candidate.name(), dimension)?;
                }

                for candidate in COSINE_WITH_NORMS_F32.candidates() {
                    let Some(kernel) = candidate.kernel() else {
                        continue;
                    };
                    let got = kernel.call(x, x_norm, y_norm, y);
                    check_cosine(got, want_with_norms, candidate.name(), dimension)?;
                }

                let batch = [y, x].concat();
                let mut pq_width_result = None;
                for candidate in COSINE_F32_BATCH.candidates() {
                    let Some(kernel) = candidate.kernel() else {
                        continue;
                    };
                    let mut got = [f32::NAN; 2];
                    kernel.call(x, &batch, dimension, &mut got);
                    check_cosine(got[0], want, candidate.name(), dimension)?;
                    check_cosine(got[1], want_self, candidate.name(), dimension)?;

                    // At the PQ widths every candidate shares one reduction
                    // order, except the x86 portable kernel at 16 dimensions,
                    // which cannot use the AVX `f32x16`.
                    let shares_pq_path = dimension == 8
                        || (dimension == 16
                            && (cfg!(not(target_arch = "x86_64"))
                                || !candidate.features().is_empty()));
                    if shares_pq_path {
                        let bits = got.map(f32::to_bits);
                        let (first, first_bits) =
                            *pq_width_result.get_or_insert((candidate.name(), bits));
                        prop_assert_eq!(
                            bits,
                            first_bits,
                            "{} and {} differ at dim {}",
                            candidate.name(),
                            first,
                            dimension
                        );
                    }
                }
            }
            Ok(())
        });
    }

    /// Every host-supported f64 kernel must agree with an f64 reference.
    #[rstest::rstest]
    fn test_cosine_f64_kernels_match_reference(
        #[values(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15)] shard: usize,
    ) {
        run_vector_pair_proptest(arbitrary_f64, dimension_shard(shard), |x, y| {
            prop_assume!(norm_l2(&x) > 1e-20);
            prop_assume!(norm_l2(&y) > 1e-20);
            let x_norm = norm_l2(&x);
            let want = cosine_fast_scalar(&x, x_norm, &y);
            for candidate in COSINE_F64.candidates() {
                let Some(kernel) = candidate.kernel() else {
                    continue;
                };
                check_cosine(kernel.call(&x, x_norm, &y), want, candidate.name(), x.len())?;
            }
            Ok(())
        });
    }

    proptest::proptest! {
        #[test]
        fn test_cosine_f16((x, y) in arbitrary_vector_pair(arbitrary_f16, 4..4048)) {
            // Cosine requires non-zero vectors
            prop_assume!(norm_l2(&x) > 1e-6);
            prop_assume!(norm_l2(&y) > 1e-6);
            do_cosine_test(&x, &y)?;
        }

        #[test]
        fn test_cosine_bf16((x, y) in arbitrary_vector_pair(arbitrary_bf16, 4..4048)){
            prop_assume!(norm_l2(&x) > 1e-6);
            prop_assume!(norm_l2(&y) > 1e-6);
            do_cosine_test(&x, &y)?;
        }
    }

    /// The f32 arrow path fills every distance with one batch kernel call; it
    /// must match the per-vector `cosine` and keep the targets' nulls.
    #[rstest::rstest]
    #[case::dim_8(8)]
    #[case::dim_40(40)]
    fn test_cosine_arrow_batch_f32_matches_per_vector(#[case] dimension: usize) {
        let num_vectors = 70;
        let x: Vec<f32> = (0..dimension)
            .map(|index| (index % 13) as f32 * 0.25 + 1.0)
            .collect();
        let batch: Vec<f32> = (0..dimension * num_vectors)
            .map(|index| (index % 11) as f32 * 0.5 - 2.0)
            .collect();
        let nulls = arrow_buffer::NullBuffer::from_iter((0..num_vectors).map(|i| i % 3 != 0));
        let to = FixedSizeListArray::try_new(
            Arc::new(arrow_schema::Field::new(
                "item",
                arrow_schema::DataType::Float32,
                true,
            )),
            dimension as i32,
            Arc::new(Float32Array::from(batch.clone())),
            Some(nulls.clone()),
        )
        .unwrap();

        let got = crate::distance::DistanceType::Cosine
            .batch(&Float32Array::from(x.clone()), &to)
            .unwrap();

        assert_eq!(got.nulls(), Some(&nulls));
        for (got, vector) in got.values().iter().zip(batch.chunks_exact(dimension)) {
            assert_relative_eq!(*got, f32::cosine(&x, vector), max_relative = 1e-5);
        }
    }

    fn cosine_distance_batch_vec<T: Cosine>(from: &[T], to: &[T], dimension: usize) -> Vec<f32> {
        let mut distances = vec![0.0; to.len() / dimension];
        cosine_distance_batch_into(from, to, dimension, &mut distances);
        distances
    }

    /// `cosine_batch_into` must agree with the per-vector `cosine`, both for
    /// the f32 batch kernel and for the per-vector default that f64 uses.
    #[rstest::rstest]
    #[case::dim_8(8)]
    #[case::dim_12(12)]
    #[case::dim_16(16)]
    #[case::dim_40(40)]
    #[case::dim_1024(1024)]
    fn test_cosine_batch_into_matches_per_vector(#[case] dimension: usize) {
        fn check<T: Cosine + num_traits::FromPrimitive + Copy>(dimension: usize) {
            let num_vectors = 130;
            let x: Vec<T> = (0..dimension)
                .map(|index| T::from_f32((index % 13) as f32 * 0.25 + 1.0).unwrap())
                .collect();
            let batch: Vec<T> = (0..dimension * num_vectors)
                .map(|index| T::from_f32((index % 11) as f32 * 0.5 - 2.0).unwrap())
                .collect();

            let mut got = vec![f32::NAN; num_vectors];
            T::cosine_batch_into(&x, &batch, dimension, &mut got);

            for (distance, vector) in got.iter().zip(batch.chunks_exact(dimension)) {
                let expected = T::cosine(&x, vector);
                assert_relative_eq!(*distance, expected, max_relative = 1e-5, epsilon = 1e-6);
            }
        }
        check::<f32>(dimension);
        check::<f64>(dimension);
    }

    #[rstest::rstest]
    #[case::short(1)]
    #[case::long(3)]
    fn test_cosine_batch_into_rejects_wrong_output_len(#[case] output_len: usize) {
        let batch = [1.0_f32; 4];
        let mut output = vec![0.0; output_len];
        let result = std::panic::catch_unwind(move || {
            cosine_distance_batch_into(&[1.0_f32, 2.0], &batch, 2, &mut output)
        });
        assert!(result.is_err());
    }
}
