// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Cosine distance
//!
//! <https://en.wikipedia.org/wiki/Cosine_similarity>
//!
//! `bf16, f16, f32, f64` types are supported.

use std::sync::Arc;

use arrow_array::{
    Array, FixedSizeListArray, Float32Array,
    cast::AsArray,
    types::{Float16Type, Float32Type, Float64Type, Int8Type},
};
use arrow_schema::DataType;
use half::{bf16, f16};
use lance_arrow::{ArrowFloatType, FixedSizeListArrayExt, FloatArray};
#[allow(unused_imports)]
use lance_core::utils::cpu::{SIMD_SUPPORT, SimdSupport};

use super::{Dot, norm_l2::norm_l2};
use super::{Normalize, dot::dot};
#[allow(unused_imports)]
use crate::simd::{
    FloatSimd, SIMD,
    f32::{f32x8, f32x16},
};
use crate::{Error, Result};

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

    fn cosine_batch<'a>(
        x: &'a [Self],
        batch: &'a [Self],
        dimension: usize,
    ) -> Box<dyn Iterator<Item = f32> + 'a> {
        let x_norm = norm_l2(x);

        Box::new(
            batch
                .chunks_exact(dimension)
                .map(move |y| Self::cosine_fast(x, x_norm, y)),
        )
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
        match *SIMD_SUPPORT {
            #[cfg(all(feature = "fp16kernels", target_arch = "aarch64"))]
            SimdSupport::Neon => unsafe {
                bf16_kernel::cosine_bf16_neon(x.as_ptr(), x_norm, y.as_ptr(), y.len() as u32)
            },
            #[cfg(all(
                feature = "fp16kernels",
                kernel_support = "avx512_bf16",
                target_arch = "x86_64"
            ))]
            SimdSupport::Avx512FP16 => unsafe {
                bf16_kernel::cosine_bf16_avx512(x.as_ptr(), x_norm, y.as_ptr(), y.len() as u32)
            },
            #[cfg(all(feature = "fp16kernels", target_arch = "x86_64"))]
            SimdSupport::Avx2 | SimdSupport::Avx512 => unsafe {
                bf16_kernel::cosine_bf16_avx2(x.as_ptr(), x_norm, y.as_ptr(), y.len() as u32)
            },
            #[cfg(all(feature = "fp16kernels", target_arch = "loongarch64"))]
            SimdSupport::Lasx => unsafe {
                bf16_kernel::cosine_bf16_lasx(x.as_ptr(), x_norm, y.as_ptr(), y.len() as u32)
            },
            #[cfg(all(feature = "fp16kernels", target_arch = "loongarch64"))]
            SimdSupport::Lsx => unsafe {
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
        match *SIMD_SUPPORT {
            #[cfg(all(feature = "fp16kernels", target_arch = "aarch64"))]
            SimdSupport::Neon => unsafe {
                kernel::cosine_f16_neon(x.as_ptr(), x_norm, y.as_ptr(), y.len() as u32)
            },
            #[cfg(all(
                feature = "fp16kernels",
                kernel_support = "avx512_f16",
                target_arch = "x86_64"
            ))]
            SimdSupport::Avx512FP16 => unsafe {
                kernel::cosine_f16_avx512(x.as_ptr(), x_norm, y.as_ptr(), y.len() as u32)
            },
            #[cfg(all(feature = "fp16kernels", target_arch = "x86_64"))]
            SimdSupport::Avx2 | SimdSupport::Avx512 => unsafe {
                kernel::cosine_f16_avx2(x.as_ptr(), x_norm, y.as_ptr(), y.len() as u32)
            },
            #[cfg(all(feature = "fp16kernels", target_arch = "loongarch64"))]
            SimdSupport::Lasx => unsafe {
                kernel::cosine_f16_lasx(x.as_ptr(), x_norm, y.as_ptr(), y.len() as u32)
            },
            #[cfg(all(feature = "fp16kernels", target_arch = "loongarch64"))]
            SimdSupport::Lsx => unsafe {
                kernel::cosine_f16_lsx(x.as_ptr(), x_norm, y.as_ptr(), y.len() as u32)
            },
            // SimdSupport::AvxFma and SimdSupport::Avx fall through here:
            // the f16 C kernels are compiled with `-march=haswell` minimum
            // (AVX2), so they cannot run on AVX-only or AVX+FMA hosts.
            _ => cosine_scalar(x, x_norm, y),
        }
    }
}

/// f32 single-vector cosine helpers used by `cosine_batch` for fixed
/// dimensions 8 and 16.
///
/// These were previously a single generic `cosine_once<S, N>` but the
/// monomorphizations have to dispatch on `SIMD_SUPPORT` for the SIMD path
/// to stay correct under any compile baseline. Splitting them into two
/// concrete entry points keeps the dispatch site flat and lets each width
/// route to a `#[target_feature]` AVX2 inner function.
mod f32 {
    use super::*;

    #[inline]
    pub(super) fn cosine_once_8(x: &[f32], x_norm: f32, y: &[f32]) -> f32 {
        #[cfg(target_arch = "x86_64")]
        {
            match *SIMD_SUPPORT {
                SimdSupport::Avx512 | SimdSupport::Avx512FP16 => unsafe {
                    cosine_once_x86::cosine_once_8_avx512(x, x_norm, y)
                },
                SimdSupport::Avx2 | SimdSupport::AvxFma => unsafe {
                    cosine_once_x86::cosine_once_8_avx_fma(x, x_norm, y)
                },
                SimdSupport::Avx => unsafe { cosine_once_x86::cosine_once_8_avx(x, x_norm, y) },
                _ => cosine_once_8_scalar(x, x_norm, y),
            }
        }
        #[cfg(not(target_arch = "x86_64"))]
        {
            cosine_once_8_other(x, x_norm, y)
        }
    }

    #[inline]
    pub(super) fn cosine_once_16(x: &[f32], x_norm: f32, y: &[f32]) -> f32 {
        #[cfg(target_arch = "x86_64")]
        {
            match *SIMD_SUPPORT {
                SimdSupport::Avx512 | SimdSupport::Avx512FP16 => unsafe {
                    cosine_once_x86::cosine_once_16_avx512(x, x_norm, y)
                },
                SimdSupport::Avx2 | SimdSupport::AvxFma => unsafe {
                    cosine_once_x86::cosine_once_16_avx_fma(x, x_norm, y)
                },
                SimdSupport::Avx => unsafe { cosine_once_x86::cosine_once_16_avx(x, x_norm, y) },
                _ => cosine_once_16_scalar(x, x_norm, y),
            }
        }
        #[cfg(not(target_arch = "x86_64"))]
        {
            cosine_once_16_other(x, x_norm, y)
        }
    }

    /// Portable scalar `cosine_once` for length-8 vectors. Matches the SIMD
    /// path modulo summation order.
    #[cfg(target_arch = "x86_64")]
    #[inline]
    pub(super) fn cosine_once_8_scalar(x: &[f32], x_norm: f32, y: &[f32]) -> f32 {
        let mut xy = 0.0f32;
        let mut y2 = 0.0f32;
        for i in 0..8 {
            xy += x[i] * y[i];
            y2 += y[i] * y[i];
        }
        1.0 - xy / x_norm / y2.sqrt()
    }

    #[cfg(target_arch = "x86_64")]
    #[inline]
    pub(super) fn cosine_once_16_scalar(x: &[f32], x_norm: f32, y: &[f32]) -> f32 {
        let mut xy = 0.0f32;
        let mut y2 = 0.0f32;
        for i in 0..16 {
            xy += x[i] * y[i];
            y2 += y[i] * y[i];
        }
        1.0 - xy / x_norm / y2.sqrt()
    }

    #[cfg(target_arch = "x86_64")]
    pub(super) mod cosine_once_x86 {
        use std::arch::x86_64::*;

        use super::{f32x8, f32x16};
        use crate::simd::SIMD;

        /// AVX + FMA path for 8-lane cosine. Covers both AvxFma and AVX2 dispatch (body uses no AVX2-specific intrinsics).
        #[target_feature(enable = "avx,fma")]
        pub unsafe fn cosine_once_8_avx_fma(x: &[f32], x_norm: f32, y: &[f32]) -> f32 {
            let xv = f32x8::load_unaligned(x.as_ptr());
            let yv = f32x8::load_unaligned(y.as_ptr());
            let y2 = yv * yv;
            let xy = xv * yv;
            1.0 - xy.reduce_sum() / x_norm / y2.reduce_sum().sqrt()
        }

        /// AVX + FMA path for 16-lane cosine. Covers both AvxFma and AVX2 dispatch (body uses no AVX2-specific intrinsics).
        #[target_feature(enable = "avx,fma")]
        pub unsafe fn cosine_once_16_avx_fma(x: &[f32], x_norm: f32, y: &[f32]) -> f32 {
            let xv = f32x16::load_unaligned(x.as_ptr());
            let yv = f32x16::load_unaligned(y.as_ptr());
            let y2 = yv * yv;
            let xy = xv * yv;
            1.0 - xy.reduce_sum() / x_norm / y2.reduce_sum().sqrt()
        }

        /// AVX-only path for 8-lane cosine (no FMA): body unchanged from AVX2 path; gated on Sandy/Ivy Bridge.
        #[target_feature(enable = "avx")]
        pub unsafe fn cosine_once_8_avx(x: &[f32], x_norm: f32, y: &[f32]) -> f32 {
            let xv = f32x8::load_unaligned(x.as_ptr());
            let yv = f32x8::load_unaligned(y.as_ptr());
            let y2 = yv * yv;
            let xy = xv * yv;
            1.0 - xy.reduce_sum() / x_norm / y2.reduce_sum().sqrt()
        }

        /// AVX-only path for 16-lane cosine (no FMA): body unchanged from AVX2 path; gated on Sandy/Ivy Bridge.
        #[target_feature(enable = "avx")]
        pub unsafe fn cosine_once_16_avx(x: &[f32], x_norm: f32, y: &[f32]) -> f32 {
            let xv = f32x16::load_unaligned(x.as_ptr());
            let yv = f32x16::load_unaligned(y.as_ptr());
            let y2 = yv * yv;
            let xy = xv * yv;
            1.0 - xy.reduce_sum() / x_norm / y2.reduce_sum().sqrt()
        }

        /// AVX-512 path for 8-lane cosine: masked load into a `__m512` lower half, reduce.
        #[target_feature(enable = "avx512f")]
        pub unsafe fn cosine_once_8_avx512(x: &[f32], x_norm: f32, y: &[f32]) -> f32 {
            // mask 0x00FF: load the lower 8 f32 lanes, zero the upper 8.
            let mask: __mmask16 = 0x00FF;
            let xv = _mm512_maskz_loadu_ps(mask, x.as_ptr());
            let yv = _mm512_maskz_loadu_ps(mask, y.as_ptr());
            let xy = _mm512_mul_ps(xv, yv);
            let y2 = _mm512_mul_ps(yv, yv);
            let xy_sum = _mm512_reduce_add_ps(xy);
            let y2_sum = _mm512_reduce_add_ps(y2);
            1.0 - xy_sum / x_norm / y2_sum.sqrt()
        }

        /// AVX-512 path for 16-lane cosine: single full-width `__m512` load (16 f32 fits one `zmm`).
        #[target_feature(enable = "avx512f")]
        pub unsafe fn cosine_once_16_avx512(x: &[f32], x_norm: f32, y: &[f32]) -> f32 {
            let xv = _mm512_loadu_ps(x.as_ptr());
            let yv = _mm512_loadu_ps(y.as_ptr());
            let xy = _mm512_mul_ps(xv, yv);
            let y2 = _mm512_mul_ps(yv, yv);
            let xy_sum = _mm512_reduce_add_ps(xy);
            let y2_sum = _mm512_reduce_add_ps(y2);
            1.0 - xy_sum / x_norm / y2_sum.sqrt()
        }
    }

    #[cfg(not(target_arch = "x86_64"))]
    #[inline]
    fn cosine_once_8_other(x: &[f32], x_norm: f32, y: &[f32]) -> f32 {
        let xv = unsafe { f32x8::load_unaligned(x.as_ptr()) };
        let yv = unsafe { f32x8::load_unaligned(y.as_ptr()) };
        let y2 = yv * yv;
        let xy = xv * yv;
        1.0 - xy.reduce_sum() / x_norm / y2.reduce_sum().sqrt()
    }

    #[cfg(not(target_arch = "x86_64"))]
    #[inline]
    fn cosine_once_16_other(x: &[f32], x_norm: f32, y: &[f32]) -> f32 {
        let xv = unsafe { f32x16::load_unaligned(x.as_ptr()) };
        let yv = unsafe { f32x16::load_unaligned(y.as_ptr()) };
        let y2 = yv * yv;
        let xy = xv * yv;
        1.0 - xy.reduce_sum() / x_norm / y2.reduce_sum().sqrt()
    }

    /// Batch-level SIMD dispatch: the tier is chosen once by the caller, and the
    /// whole `chunks_exact` loop runs inside one `#[target_feature]` context so
    /// the per-vector `cosine_once_*` / `cosine_fast` kernels inline (no
    /// per-vector `*SIMD_SUPPORT` branch, no per-vector call boundary). Used for
    /// the AVX-512 path on the default wheel and for all tiers on sub-AVX2 builds.
    #[cfg(target_arch = "x86_64")]
    #[target_feature(enable = "avx,fma")]
    pub(super) unsafe fn cosine_batch_avx_fma(
        x: &[f32],
        x_norm: f32,
        batch: &[f32],
        dimension: usize,
    ) -> Vec<f32> {
        match dimension {
            8 => batch
                .chunks_exact(8)
                .map(|y| unsafe { cosine_once_x86::cosine_once_8_avx_fma(x, x_norm, y) })
                .collect(),
            16 => batch
                .chunks_exact(16)
                .map(|y| unsafe { cosine_once_x86::cosine_once_16_avx_fma(x, x_norm, y) })
                .collect(),
            _ => batch
                .chunks_exact(dimension)
                .map(|y| unsafe { super::f32_x86::cosine_fast_avx_fma(x, x_norm, y) })
                .collect(),
        }
    }

    #[cfg(target_arch = "x86_64")]
    #[target_feature(enable = "avx512f")]
    pub(super) unsafe fn cosine_batch_avx512(
        x: &[f32],
        x_norm: f32,
        batch: &[f32],
        dimension: usize,
    ) -> Vec<f32> {
        match dimension {
            8 => batch
                .chunks_exact(8)
                .map(|y| unsafe { cosine_once_x86::cosine_once_8_avx512(x, x_norm, y) })
                .collect(),
            16 => batch
                .chunks_exact(16)
                .map(|y| unsafe { cosine_once_x86::cosine_once_16_avx512(x, x_norm, y) })
                .collect(),
            _ => batch
                .chunks_exact(dimension)
                .map(|y| unsafe { super::f32_x86::cosine_fast_avx512(x, x_norm, y) })
                .collect(),
        }
    }

    #[cfg(target_arch = "x86_64")]
    #[target_feature(enable = "avx")]
    pub(super) unsafe fn cosine_batch_avx(
        x: &[f32],
        x_norm: f32,
        batch: &[f32],
        dimension: usize,
    ) -> Vec<f32> {
        match dimension {
            8 => batch
                .chunks_exact(8)
                .map(|y| unsafe { cosine_once_x86::cosine_once_8_avx(x, x_norm, y) })
                .collect(),
            16 => batch
                .chunks_exact(16)
                .map(|y| unsafe { cosine_once_x86::cosine_once_16_avx(x, x_norm, y) })
                .collect(),
            _ => batch
                .chunks_exact(dimension)
                .map(|y| unsafe { super::f32_x86::cosine_fast_avx(x, x_norm, y) })
                .collect(),
        }
    }
}

/// Inlined f32 cosine kernels for builds whose baseline already guarantees AVX2
/// (the default `haswell` wheel). No `#[target_feature]`, no runtime dispatch:
/// under `target-feature=+avx2,+fma` these compile to AVX2 and inline into the
/// batch loop exactly like the pre-PR code, so the modern path is not taxed by
/// the runtime-dispatch machinery (only needed below the AVX2 baseline).
#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
mod f32_baseline {
    use super::{dot, f32x8, f32x16, norm_l2};
    use crate::simd::{FloatSimd, SIMD};

    #[inline]
    pub fn cosine_once_8(x: &[f32], x_norm: f32, y: &[f32]) -> f32 {
        unsafe {
            let xv = f32x8::load_unaligned(x.as_ptr());
            let yv = f32x8::load_unaligned(y.as_ptr());
            let y2 = yv * yv;
            let xy = xv * yv;
            1.0 - xy.reduce_sum() / x_norm / y2.reduce_sum().sqrt()
        }
    }

    #[inline]
    pub fn cosine_once_16(x: &[f32], x_norm: f32, y: &[f32]) -> f32 {
        unsafe {
            let xv = f32x16::load_unaligned(x.as_ptr());
            let yv = f32x16::load_unaligned(y.as_ptr());
            let y2 = yv * yv;
            let xy = xv * yv;
            1.0 - xy.reduce_sum() / x_norm / y2.reduce_sum().sqrt()
        }
    }

    #[inline]
    pub fn cosine_fast(x: &[f32], x_norm: f32, other: &[f32]) -> f32 {
        unsafe {
            let dim = x.len();
            let unrolled_len = dim / 16 * 16;
            let mut y_norm16 = f32x16::zeros();
            let mut xy16 = f32x16::zeros();
            for i in (0..unrolled_len).step_by(16) {
                let xv = f32x16::load_unaligned(x.as_ptr().add(i));
                let yv = f32x16::load_unaligned(other.as_ptr().add(i));
                xy16.multiply_add(xv, yv);
                y_norm16.multiply_add(yv, yv);
            }
            let aligned_len = dim / 8 * 8;
            let mut y_norm8 = f32x8::zeros();
            let mut xy8 = f32x8::zeros();
            for i in (unrolled_len..aligned_len).step_by(8) {
                let xv = f32x8::load_unaligned(x.as_ptr().add(i));
                let yv = f32x8::load_unaligned(other.as_ptr().add(i));
                xy8.multiply_add(xv, yv);
                y_norm8.multiply_add(yv, yv);
            }
            let y_norm = y_norm16.reduce_sum()
                + y_norm8.reduce_sum()
                + norm_l2(&other[aligned_len..]).powi(2);
            let xy = xy16.reduce_sum()
                + xy8.reduce_sum()
                + dot(&x[aligned_len..], &other[aligned_len..]);
            1.0 - xy / x_norm / y_norm.sqrt()
        }
    }
}

impl Cosine for f32 {
    #[inline]
    fn cosine_fast(x: &[Self], x_norm: Self, other: &[Self]) -> f32 {
        // Trait methods cannot carry `#[target_feature]` attributes, so the body
        // lives in a free function that runtime-dispatches via `*SIMD_SUPPORT`
        // to an AVX2 inner kernel on capable hosts, or a portable scalar fallback.
        cosine_fast_f32_dispatched(x, x_norm, other)
    }

    #[inline]
    fn cosine_with_norms(x: &[Self], x_norm: Self, y_norm: Self, y: &[Self]) -> Self {
        // Trait methods cannot carry `#[target_feature]` attributes, so the body
        // lives in a free function that runtime-dispatches via `*SIMD_SUPPORT`
        // to an AVX2 inner kernel on capable hosts, or a portable scalar fallback.
        cosine_with_norms_f32_dispatched(x, x_norm, y_norm, y)
    }

    #[allow(unreachable_code)]
    fn cosine_batch<'a>(
        x: &'a [Self],
        batch: &'a [Self],
        dimension: usize,
    ) -> Box<dyn Iterator<Item = f32> + 'a> {
        let x_norm = norm_l2(x);

        // On a build whose baseline already guarantees AVX2 (the default
        // `haswell` wheel), avoid the per-vector runtime dispatch + `#[target_feature]`
        // wrapping that taxes the modern path. Dispatch ONCE per batch: AVX-512
        // hosts get the wide kernel; everyone else uses the inlined AVX2 baseline
        // path (base-equivalent). The runtime-dispatch path below is only
        // compiled/reached when the baseline is below AVX2 (pre-Haswell builds).
        #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
        {
            // dim 8/16 always use the inlined AVX2 baseline: AVX-512 gives no
            // benefit for such tiny vectors (a masked 512-bit load is slower than
            // a plain AVX2 load) and only adds dispatch + eager-collect overhead.
            // Only the larger-dim path routes to AVX-512 on capable hosts — that's
            // where the wider lanes actually pay off.
            return match dimension {
                8 => Box::new(
                    batch
                        .chunks_exact(8)
                        .map(move |y| f32_baseline::cosine_once_8(x, x_norm, y)),
                ),
                16 => Box::new(
                    batch
                        .chunks_exact(16)
                        .map(move |y| f32_baseline::cosine_once_16(x, x_norm, y)),
                ),
                _ => {
                    if matches!(*SIMD_SUPPORT, SimdSupport::Avx512 | SimdSupport::Avx512FP16) {
                        Box::new(
                            unsafe { f32::cosine_batch_avx512(x, x_norm, batch, dimension) }
                                .into_iter(),
                        )
                    } else {
                        Box::new(
                            batch
                                .chunks_exact(dimension)
                                .map(move |y| f32_baseline::cosine_fast(x, x_norm, y)),
                        )
                    }
                }
            };
        }

        // Sub-AVX2 / non-x86 build: hoisted per-batch runtime dispatch.
        #[cfg(target_arch = "x86_64")]
        {
            match *SIMD_SUPPORT {
                SimdSupport::Avx512 | SimdSupport::Avx512FP16 => {
                    return Box::new(
                        unsafe { f32::cosine_batch_avx512(x, x_norm, batch, dimension) }
                            .into_iter(),
                    );
                }
                SimdSupport::Avx2 | SimdSupport::AvxFma => {
                    return Box::new(
                        unsafe { f32::cosine_batch_avx_fma(x, x_norm, batch, dimension) }
                            .into_iter(),
                    );
                }
                SimdSupport::Avx => {
                    return Box::new(
                        unsafe { f32::cosine_batch_avx(x, x_norm, batch, dimension) }.into_iter(),
                    );
                }
                _ => {}
            }
        }

        // Scalar / non-x86 fallback.
        match dimension {
            8 => Box::new(
                batch
                    .chunks_exact(dimension)
                    .map(move |y| f32::cosine_once_8(x, x_norm, y)),
            ),
            16 => Box::new(
                batch
                    .chunks_exact(dimension)
                    .map(move |y| f32::cosine_once_16(x, x_norm, y)),
            ),
            _ => Box::new(
                batch
                    .chunks_exact(dimension)
                    .map(move |y| Self::cosine_fast(x, x_norm, y)),
            ),
        }
    }
}

impl Cosine for f64 {
    #[inline]
    fn cosine_fast(x: &[Self], x_norm: f32, y: &[Self]) -> f32 {
        // Trait methods cannot carry `#[target_feature]` attributes, so the body
        // lives in a free function that runtime-dispatches via `*SIMD_SUPPORT`
        // to an AVX2 inner kernel on capable hosts, or a portable scalar fallback.
        cosine_fast_f64_dispatched(x, x_norm, y)
    }
}

/// Fast cosine for f64, runtime-dispatched via `SIMD_SUPPORT` on x86_64
/// (AVX-512 / AVX2+FMA / AVX+FMA / AVX / scalar). Non-x86 uses the SIMD
/// primitives in `crate::simd::f64`.
#[inline]
fn cosine_fast_f64_dispatched(x: &[f64], x_norm: f32, y: &[f64]) -> f32 {
    #[cfg(target_arch = "x86_64")]
    {
        match *SIMD_SUPPORT {
            SimdSupport::Avx512 | SimdSupport::Avx512FP16 => unsafe {
                f64_x86::cosine_fast_avx512(x, x_norm, y)
            },
            SimdSupport::Avx2 | SimdSupport::AvxFma => unsafe {
                f64_x86::cosine_fast_avx_fma(x, x_norm, y)
            },
            SimdSupport::Avx => unsafe { f64_x86::cosine_fast_avx(x, x_norm, y) },
            _ => cosine_scalar(x, x_norm, y),
        }
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        cosine_fast_f64_simd_other(x, x_norm, y)
    }
}

/// AVX2 + FMA implementation of the f64 cosine_fast kernel.
///
/// Lives in a `#[target_feature]`-annotated function so the SIMD primitives
/// in `crate::simd::f64` (which use raw AVX intrinsics) inline correctly
/// even when the compile baseline does not have AVX2 enabled. Caller must
/// ensure the host supports AVX2 + FMA.
#[cfg(target_arch = "x86_64")]
mod f64_x86 {
    use std::arch::x86_64::*;

    use crate::simd::f64::{f64x4, f64x8};
    use crate::simd::x86::hsum256_pd;
    use crate::simd::{FloatSimd, SIMD};

    /// AVX-512 path for f64 fast cosine: 8-wide `__m512d` xy/yy with `vfmadd231pd` per iteration.
    #[target_feature(enable = "avx512f")]
    pub unsafe fn cosine_fast_avx512(x: &[f64], x_norm: f32, y: &[f64]) -> f32 {
        let dim = x.len();
        let unrolled_len = dim / 8 * 8;

        let mut acc_xy = _mm512_setzero_pd();
        let mut acc_yy = _mm512_setzero_pd();
        for i in (0..unrolled_len).step_by(8) {
            let xv = _mm512_loadu_pd(x.as_ptr().add(i));
            let yv = _mm512_loadu_pd(y.as_ptr().add(i));
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

    /// AVX + FMA path for f64 fast cosine. Covers both AvxFma and AVX2 dispatch (body uses no AVX2-specific intrinsics).
    #[target_feature(enable = "avx,fma")]
    pub unsafe fn cosine_fast_avx_fma(x: &[f64], x_norm: f32, y: &[f64]) -> f32 {
        let dim = x.len();
        let unrolled_len = dim / 8 * 8;
        let mut y_norm8 = f64x8::zeros();
        let mut xy8 = f64x8::zeros();
        for i in (0..unrolled_len).step_by(8) {
            let xv = f64x8::load_unaligned(x.as_ptr().add(i));
            let yv = f64x8::load_unaligned(y.as_ptr().add(i));
            xy8.multiply_add(xv, yv);
            y_norm8.multiply_add(yv, yv);
        }
        let aligned_len = dim / 4 * 4;
        let mut y_norm4 = f64x4::zeros();
        let mut xy4 = f64x4::zeros();
        for i in (unrolled_len..aligned_len).step_by(4) {
            let xv = f64x4::load_unaligned(x.as_ptr().add(i));
            let yv = f64x4::load_unaligned(y.as_ptr().add(i));
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

    /// AVX-only path for f64 fast cosine (no FMA): `_mm256_mul_pd` + `_mm256_add_pd` per iteration; tail handled inline.
    #[target_feature(enable = "avx")]
    pub unsafe fn cosine_fast_avx(x: &[f64], x_norm: f32, y: &[f64]) -> f32 {
        let dim = x.len();
        let aligned_len = dim / 4 * 4;

        let mut acc_xy = _mm256_setzero_pd();
        let mut acc_yy = _mm256_setzero_pd();
        for i in (0..aligned_len).step_by(4) {
            let xv = _mm256_loadu_pd(x.as_ptr().add(i));
            let yv = _mm256_loadu_pd(y.as_ptr().add(i));
            acc_xy = _mm256_add_pd(acc_xy, _mm256_mul_pd(xv, yv));
            acc_yy = _mm256_add_pd(acc_yy, _mm256_mul_pd(yv, yv));
        }

        let xy_main = hsum256_pd(acc_xy);
        let yy_main = hsum256_pd(acc_yy);

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

#[cfg(not(target_arch = "x86_64"))]
#[inline]
fn cosine_fast_f64_simd_other(x: &[f64], x_norm: f32, y: &[f64]) -> f32 {
    use crate::simd::f64::{f64x4, f64x8};
    use crate::simd::{FloatSimd, SIMD};

    let dim = x.len();
    let unrolled_len = dim / 8 * 8;
    let mut y_norm8 = f64x8::zeros();
    let mut xy8 = f64x8::zeros();
    for i in (0..unrolled_len).step_by(8) {
        unsafe {
            let xv = f64x8::load_unaligned(x.as_ptr().add(i));
            let yv = f64x8::load_unaligned(y.as_ptr().add(i));
            xy8.multiply_add(xv, yv);
            y_norm8.multiply_add(yv, yv);
        }
    }
    let aligned_len = dim / 4 * 4;
    let mut y_norm4 = f64x4::zeros();
    let mut xy4 = f64x4::zeros();
    for i in (unrolled_len..aligned_len).step_by(4) {
        unsafe {
            let xv = f64x4::load_unaligned(x.as_ptr().add(i));
            let yv = f64x4::load_unaligned(y.as_ptr().add(i));
            xy4.multiply_add(xv, yv);
            y_norm4.multiply_add(yv, yv);
        }
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

/// Cosine for f32 with known norms, runtime-dispatched via `SIMD_SUPPORT`
/// on x86_64 (AVX-512 / AVX2+FMA / AVX+FMA / AVX / scalar). Non-x86 uses
/// the auto-vectorised scalar loop.
#[inline]
fn cosine_with_norms_f32_dispatched(x: &[f32], x_norm: f32, y_norm: f32, y: &[f32]) -> f32 {
    #[cfg(target_arch = "x86_64")]
    {
        match *SIMD_SUPPORT {
            SimdSupport::Avx512 | SimdSupport::Avx512FP16 => unsafe {
                f32_x86::cosine_with_norms_avx512(x, x_norm, y_norm, y)
            },
            SimdSupport::Avx2 | SimdSupport::AvxFma => unsafe {
                f32_x86::cosine_with_norms_avx_fma(x, x_norm, y_norm, y)
            },
            SimdSupport::Avx => unsafe { f32_x86::cosine_with_norms_avx(x, x_norm, y_norm, y) },
            _ => cosine_scalar_fast(x, x_norm, y, y_norm),
        }
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        cosine_with_norms_f32_simd_other(x, x_norm, y_norm, y)
    }
}

#[cfg(not(target_arch = "x86_64"))]
#[inline]
fn cosine_with_norms_f32_simd_other(x: &[f32], x_norm: f32, y_norm: f32, y: &[f32]) -> f32 {
    let dim = x.len();
    let unrolled_len = dim / 16 * 16;
    let mut xy16 = f32x16::zeros();
    for i in (0..unrolled_len).step_by(16) {
        unsafe {
            let xv = f32x16::load_unaligned(x.as_ptr().add(i));
            let yv = f32x16::load_unaligned(y.as_ptr().add(i));
            xy16.multiply_add(xv, yv);
        }
    }
    let aligned_len = dim / 8 * 8;
    let mut xy8 = f32x8::zeros();
    for i in (unrolled_len..aligned_len).step_by(8) {
        unsafe {
            let xv = f32x8::load_unaligned(x.as_ptr().add(i));
            let yv = f32x8::load_unaligned(y.as_ptr().add(i));
            xy8.multiply_add(xv, yv);
        }
    }
    let xy = xy16.reduce_sum() + xy8.reduce_sum() + dot(&x[aligned_len..], &y[aligned_len..]);
    1.0 - xy / x_norm / y_norm
}

/// Fast cosine for f32, runtime-dispatched via `SIMD_SUPPORT` on x86_64
/// (AVX-512 / AVX2+FMA / AVX+FMA / AVX / scalar). Non-x86 uses the
/// `simd::f32` primitives, unconditionally backed by NEON / LSX.
#[inline]
fn cosine_fast_f32_dispatched(x: &[f32], x_norm: f32, other: &[f32]) -> f32 {
    #[cfg(target_arch = "x86_64")]
    {
        match *SIMD_SUPPORT {
            SimdSupport::Avx512 | SimdSupport::Avx512FP16 => unsafe {
                f32_x86::cosine_fast_avx512(x, x_norm, other)
            },
            SimdSupport::Avx2 | SimdSupport::AvxFma => unsafe {
                f32_x86::cosine_fast_avx_fma(x, x_norm, other)
            },
            SimdSupport::Avx => unsafe { f32_x86::cosine_fast_avx(x, x_norm, other) },
            _ => cosine_scalar(x, x_norm, other),
        }
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        cosine_fast_f32_simd_other(x, x_norm, other)
    }
}

/// AVX2 + FMA implementation of the f32 fast cosine kernel.
///
/// Lives in a `#[target_feature]`-annotated function so the SIMD primitives
/// in `crate::simd::f32` (which use raw AVX intrinsics) inline correctly
/// even when the compile baseline does not have AVX2 enabled. Caller must
/// ensure the host supports AVX2 + FMA.
#[cfg(target_arch = "x86_64")]
mod f32_x86 {
    use std::arch::x86_64::*;

    use super::{dot, f32x8, f32x16, norm_l2};
    use crate::simd::x86::hsum256_ps;
    use crate::simd::{FloatSimd, SIMD};

    /// AVX + FMA path for f32 fast cosine. Covers both AvxFma and AVX2 dispatch (body uses no AVX2-specific intrinsics).
    #[target_feature(enable = "avx,fma")]
    pub unsafe fn cosine_fast_avx_fma(x: &[f32], x_norm: f32, other: &[f32]) -> f32 {
        let dim = x.len();
        let unrolled_len = dim / 16 * 16;
        let mut y_norm16 = f32x16::zeros();
        let mut xy16 = f32x16::zeros();
        for i in (0..unrolled_len).step_by(16) {
            let xv = f32x16::load_unaligned(x.as_ptr().add(i));
            let yv = f32x16::load_unaligned(other.as_ptr().add(i));
            xy16.multiply_add(xv, yv);
            y_norm16.multiply_add(yv, yv);
        }
        let aligned_len = dim / 8 * 8;
        let mut y_norm8 = f32x8::zeros();
        let mut xy8 = f32x8::zeros();
        for i in (unrolled_len..aligned_len).step_by(8) {
            let xv = f32x8::load_unaligned(x.as_ptr().add(i));
            let yv = f32x8::load_unaligned(other.as_ptr().add(i));
            xy8.multiply_add(xv, yv);
            y_norm8.multiply_add(yv, yv);
        }
        let y_norm =
            y_norm16.reduce_sum() + y_norm8.reduce_sum() + norm_l2(&other[aligned_len..]).powi(2);
        let xy =
            xy16.reduce_sum() + xy8.reduce_sum() + dot(&x[aligned_len..], &other[aligned_len..]);
        1.0 - xy / x_norm / y_norm.sqrt()
    }

    /// AVX-only path for f32 fast cosine (no FMA): `_mm256_mul_ps` + `_mm256_add_ps` per iteration; tail via trait-routed `dot`/`norm_l2`.
    #[target_feature(enable = "avx")]
    pub unsafe fn cosine_fast_avx(x: &[f32], x_norm: f32, other: &[f32]) -> f32 {
        let dim = x.len();
        let aligned_len = dim / 8 * 8;

        let mut acc_xy = _mm256_setzero_ps();
        let mut acc_yy = _mm256_setzero_ps();
        for i in (0..aligned_len).step_by(8) {
            let xv = _mm256_loadu_ps(x.as_ptr().add(i));
            let yv = _mm256_loadu_ps(other.as_ptr().add(i));
            acc_xy = _mm256_add_ps(acc_xy, _mm256_mul_ps(xv, yv));
            acc_yy = _mm256_add_ps(acc_yy, _mm256_mul_ps(yv, yv));
        }

        let xy_main = hsum256_ps(acc_xy);
        let yy_main = hsum256_ps(acc_yy);

        let y_norm = yy_main + norm_l2(&other[aligned_len..]).powi(2);
        let xy = xy_main + dot(&x[aligned_len..], &other[aligned_len..]);
        1.0 - xy / x_norm / y_norm.sqrt()
    }

    /// AVX-512 path for f32 fast cosine: 16-wide `__m512` xy/yy with `vfmadd231ps` per iteration.
    #[target_feature(enable = "avx512f")]
    pub unsafe fn cosine_fast_avx512(x: &[f32], x_norm: f32, other: &[f32]) -> f32 {
        let dim = x.len();
        let unrolled_len = dim / 16 * 16;

        let mut acc_xy = _mm512_setzero_ps();
        let mut acc_yy = _mm512_setzero_ps();
        for i in (0..unrolled_len).step_by(16) {
            let xv = _mm512_loadu_ps(x.as_ptr().add(i));
            let yv = _mm512_loadu_ps(other.as_ptr().add(i));
            acc_xy = _mm512_fmadd_ps(xv, yv, acc_xy);
            acc_yy = _mm512_fmadd_ps(yv, yv, acc_yy);
        }

        let mut xy = _mm512_reduce_add_ps(acc_xy);
        let mut yy = _mm512_reduce_add_ps(acc_yy);
        for i in unrolled_len..dim {
            xy += x[i] * other[i];
            yy += other[i] * other[i];
        }

        1.0 - xy / x_norm / yy.sqrt()
    }

    /// AVX-512 path for f32 cosine with known norms: 16-wide `__m512` with `vfmadd231ps` per iteration.
    #[target_feature(enable = "avx512f")]
    pub unsafe fn cosine_with_norms_avx512(x: &[f32], x_norm: f32, y_norm: f32, y: &[f32]) -> f32 {
        let dim = x.len();
        let unrolled_len = dim / 16 * 16;

        let mut acc = _mm512_setzero_ps();
        for i in (0..unrolled_len).step_by(16) {
            let xv = _mm512_loadu_ps(x.as_ptr().add(i));
            let yv = _mm512_loadu_ps(y.as_ptr().add(i));
            acc = _mm512_fmadd_ps(xv, yv, acc);
        }

        let mut xy = _mm512_reduce_add_ps(acc);
        for i in unrolled_len..dim {
            xy += x[i] * y[i];
        }

        1.0 - xy / x_norm / y_norm
    }

    /// AVX + FMA path for f32 cosine with known norms. Covers both AvxFma and AVX2 dispatch (body uses no AVX2-specific intrinsics).
    #[target_feature(enable = "avx,fma")]
    pub unsafe fn cosine_with_norms_avx_fma(x: &[f32], x_norm: f32, y_norm: f32, y: &[f32]) -> f32 {
        let dim = x.len();
        let unrolled_len = dim / 16 * 16;
        let mut xy16 = f32x16::zeros();
        for i in (0..unrolled_len).step_by(16) {
            let xv = f32x16::load_unaligned(x.as_ptr().add(i));
            let yv = f32x16::load_unaligned(y.as_ptr().add(i));
            xy16.multiply_add(xv, yv);
        }
        let aligned_len = dim / 8 * 8;
        let mut xy8 = f32x8::zeros();
        for i in (unrolled_len..aligned_len).step_by(8) {
            let xv = f32x8::load_unaligned(x.as_ptr().add(i));
            let yv = f32x8::load_unaligned(y.as_ptr().add(i));
            xy8.multiply_add(xv, yv);
        }
        let xy = xy16.reduce_sum() + xy8.reduce_sum() + dot(&x[aligned_len..], &y[aligned_len..]);
        1.0 - xy / x_norm / y_norm
    }

    /// AVX-only path for f32 cosine with known norms (no FMA): `_mm256_mul_ps` + `_mm256_add_ps` per iteration; tail via trait-routed `dot`.
    #[target_feature(enable = "avx")]
    pub unsafe fn cosine_with_norms_avx(x: &[f32], x_norm: f32, y_norm: f32, y: &[f32]) -> f32 {
        let dim = x.len();
        let aligned_len = dim / 8 * 8;

        let mut acc = _mm256_setzero_ps();
        for i in (0..aligned_len).step_by(8) {
            let xv = _mm256_loadu_ps(x.as_ptr().add(i));
            let yv = _mm256_loadu_ps(y.as_ptr().add(i));
            acc = _mm256_add_ps(acc, _mm256_mul_ps(xv, yv));
        }

        let xy_main = hsum256_ps(acc);
        let xy = xy_main + dot(&x[aligned_len..], &y[aligned_len..]);
        1.0 - xy / x_norm / y_norm
    }
}

#[cfg(not(target_arch = "x86_64"))]
#[inline]
fn cosine_fast_f32_simd_other(x: &[f32], x_norm: f32, other: &[f32]) -> f32 {
    let dim = x.len();
    let unrolled_len = dim / 16 * 16;
    let mut y_norm16 = f32x16::zeros();
    let mut xy16 = f32x16::zeros();
    for i in (0..unrolled_len).step_by(16) {
        unsafe {
            let xv = f32x16::load_unaligned(x.as_ptr().add(i));
            let yv = f32x16::load_unaligned(other.as_ptr().add(i));
            xy16.multiply_add(xv, yv);
            y_norm16.multiply_add(yv, yv);
        }
    }
    let aligned_len = dim / 8 * 8;
    let mut y_norm8 = f32x8::zeros();
    let mut xy8 = f32x8::zeros();
    for i in (unrolled_len..aligned_len).step_by(8) {
        unsafe {
            let xv = f32x8::load_unaligned(x.as_ptr().add(i));
            let yv = f32x8::load_unaligned(other.as_ptr().add(i));
            xy8.multiply_add(xv, yv);
            y_norm8.multiply_add(yv, yv);
        }
    }
    let y_norm =
        y_norm16.reduce_sum() + y_norm8.reduce_sum() + norm_l2(&other[aligned_len..]).powi(2);
    let xy = xy16.reduce_sum() + xy8.reduce_sum() + dot(&x[aligned_len..], &other[aligned_len..]);
    1.0 - xy / x_norm / y_norm.sqrt()
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

/// Cosine Distance
///
/// <https://en.wikipedia.org/wiki/Cosine_similarity>
///
/// Parameters
/// -----------
///
/// - *from*: the vector to compute distance from.
/// - *to*: the batch of vectors to compute distance to.
/// - *dimension*: the dimension of the vector.
///
/// Returns
/// -------
/// An iterator of pair-wise cosine distance between from vector to each vector in the batch.
///
pub fn cosine_distance_batch<'a, T: Cosine>(
    from: &'a [T],
    batch: &'a [T],
    dimension: usize,
) -> Box<dyn Iterator<Item = f32> + 'a> {
    T::cosine_batch(from, batch, dimension)
}

fn do_cosine_distance_arrow_batch<T: ArrowFloatType>(
    from: &T::ArrayType,
    to: &FixedSizeListArray,
) -> Result<Arc<Float32Array>>
where
    T::Native: Cosine,
{
    let dimension = to.value_length() as usize;
    debug_assert_eq!(from.len(), dimension);

    // TODO: if we detect there is a run of nulls, should we skip those?
    let to_values =
        to.values()
            .as_any()
            .downcast_ref::<T::ArrayType>()
            .ok_or(Error::InvalidArgumentError(format!(
                "Unsupported data type {:?}",
                to.values().data_type()
            )))?;
    let dists = cosine_distance_batch(from.as_slice(), to_values.as_slice(), dimension);

    Ok(Arc::new(Float32Array::new(
        dists.collect(),
        to.nulls().cloned(),
    )))
}

/// Compute Cosine distance between a vector and a batch of vectors.
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
pub fn cosine_distance_arrow_batch(
    from: &dyn Array,
    to: &FixedSizeListArray,
) -> Result<Arc<Float32Array>> {
    match *from.data_type() {
        DataType::Float16 => do_cosine_distance_arrow_batch::<Float16Type>(from.as_primitive(), to),
        DataType::Float32 => do_cosine_distance_arrow_batch::<Float32Type>(from.as_primitive(), to),
        DataType::Float64 => do_cosine_distance_arrow_batch::<Float64Type>(from.as_primitive(), to),
        DataType::Int8 => do_cosine_distance_arrow_batch::<Float32Type>(
            &from
                .as_primitive::<Int8Type>()
                .into_iter()
                .map(|x| x.unwrap() as f32)
                .collect(),
            &to.convert_to_floating_point()?,
        ),
        _ => Err(Error::InvalidArgumentError(format!(
            "Unsupported data type {:?}",
            from.data_type()
        ))),
    }
}

/// Portable scalar reference cosine over f64 inputs. Used by parity tests
/// to compare against every dispatched per-tier inner kernel. Computes
/// `1 - xy / (x_norm * y_norm_sq.sqrt())` in f64 then casts to f32, matching
/// the reduction order of the dispatched kernels.
#[cfg(test)]
fn cosine_fast_scalar(x: &[f64], x_norm: f32, y: &[f64]) -> f32 {
    let xy: f64 = x.iter().zip(y.iter()).map(|(&a, &b)| a * b).sum();
    let y_norm_sq: f64 = y.iter().map(|&v| v * v).sum();
    1.0 - (xy as f32) / x_norm / (y_norm_sq as f32).sqrt()
}

/// Portable scalar reference cosine when both norms are known. Mirrors
/// `cosine_with_norms_f32_dispatched` for parity testing.
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
    fn test_cosine() {
        let x: Float32Array = (1..9).map(|v| v as f32).collect();
        let y: Float32Array = (100..108).map(|v| v as f32).collect();
        let d = cosine_distance_batch(x.values(), y.values(), 8).collect::<Vec<_>>();
        // from scipy.spatial.distance.cosine
        assert_relative_eq!(d[0], 1.0 - 0.900_957);

        let x = Float32Array::from_iter_values([3.0, 45.0, 7.0, 2.0, 5.0, 20.0, 13.0, 12.0]);
        let y = Float32Array::from_iter_values([2.0, 54.0, 13.0, 15.0, 22.0, 34.0, 50.0, 1.0]);
        let d = cosine_distance_batch(x.values(), y.values(), 8).collect::<Vec<_>>();
        // from sklearn.metrics.pairwise import cosine_similarity
        assert_relative_eq!(d[0], 1.0 - 0.873_580_63);
    }

    #[test]
    fn test_cosine_large() {
        let total = 1024;
        let x = (0..total).map(|v| v as f32).collect::<Vec<_>>();
        let y = (1024..1024 + total).map(|v| v as f32).collect::<Vec<_>>();
        let d = cosine_distance_batch(&x, &y, total).collect::<Vec<_>>();
        assert_relative_eq!(d[0], cosine_dist_brute_force(&x, &y));
    }

    #[test]
    fn test_cosine_not_aligned() {
        let x: Float32Array = vec![16_f32, 32_f32].into();
        let y: Float32Array = vec![1_f32, 2_f32, 4_f32, 8_f32].into();
        let d = cosine_distance_batch(x.values(), y.values(), 2).collect::<Vec<_>>();
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

        #[test]
        fn test_cosine_f32((x, y) in arbitrary_vector_pair(arbitrary_f32, 4..4048)){
            prop_assume!(norm_l2(&x) > 1e-10);
            prop_assume!(norm_l2(&y) > 1e-10);
            do_cosine_test(&x, &y)?;
        }

        #[test]
        fn test_cosine_f64((x, y) in arbitrary_vector_pair(arbitrary_f64, 4..4048)){
            prop_assume!(norm_l2(&x) > 1e-20);
            prop_assume!(norm_l2(&y) > 1e-20);
            do_cosine_test(&x, &y)?;
        }

        /// Cross-backend parity for the f32 cosine_fast kernel. Exercises the
        /// scalar fallback (`cosine_scalar`) against the dispatched SIMD path
        /// so the runtime fallback is exercised even on AVX2-capable CI hosts.
        #[test]
        fn test_cosine_fast_f32_scalar_simd_parity(
            (x, y) in arbitrary_vector_pair(arbitrary_f32, 4..4048)
        ) {
            prop_assume!(norm_l2(&x) > 1e-10);
            prop_assume!(norm_l2(&y) > 1e-10);
            let x_norm = norm_l2(&x);
            let x_f64: Vec<f64> = x.iter().map(|&v| v as f64).collect();
            let y_f64: Vec<f64> = y.iter().map(|&v| v as f64).collect();
            let scalar = cosine_fast_scalar(&x_f64, x_norm, &y_f64);
            let simd = <f32 as Cosine>::cosine_fast(&x, x_norm, &y);
            prop_assert!(approx::relative_eq!(scalar, simd, max_relative = 1e-3));
        }

        /// AVX-512-direct parity for the f32 cosine_fast kernel. Early-returns
        /// on hosts without AVX-512F so the test stays portable.
        #[cfg(target_arch = "x86_64")]
        #[test]
        fn test_cosine_fast_f32_scalar_vs_avx512_parity(
            (x, y) in arbitrary_vector_pair(arbitrary_f32, 4..4048)
        ) {
            if !std::is_x86_feature_detected!("avx512f") {
                return Ok(());
            }
            prop_assume!(norm_l2(&x) > 1e-10);
            prop_assume!(norm_l2(&y) > 1e-10);
            let x_norm = norm_l2(&x);
            let scalar = cosine_scalar(&x, x_norm, &y);
            let avx512 = unsafe { f32_x86::cosine_fast_avx512(&x, x_norm, &y) };
            prop_assert!(approx::relative_eq!(scalar, avx512, max_relative = 1e-5));
        }

        /// AVX + FMA-direct parity for the f32 cosine_fast kernel. Covers
        /// the AMD Piledriver / Steamroller / FX-7500 tier. Early-returns
        /// on hosts without both AVX and FMA.
        #[cfg(target_arch = "x86_64")]
        #[test]
        fn test_cosine_fast_f32_scalar_vs_avx_fma_parity(
            (x, y) in arbitrary_vector_pair(arbitrary_f32, 4..4048)
        ) {
            if !(std::is_x86_feature_detected!("avx") && std::is_x86_feature_detected!("fma")) {
                return Ok(());
            }
            prop_assume!(norm_l2(&x) > 1e-10);
            prop_assume!(norm_l2(&y) > 1e-10);
            let x_norm = norm_l2(&x);
            let scalar = cosine_scalar(&x, x_norm, &y);
            let avx_fma = unsafe { f32_x86::cosine_fast_avx_fma(&x, x_norm, &y) };
            prop_assert!(approx::relative_eq!(scalar, avx_fma, max_relative = 1e-5));
        }

        /// AVX-only-direct parity for the f32 cosine_fast kernel. Covers
        /// the Intel Sandy Bridge / Ivy Bridge tier. Early-returns on
        /// hosts without AVX.
        #[cfg(target_arch = "x86_64")]
        #[test]
        fn test_cosine_fast_f32_scalar_vs_avx_parity(
            (x, y) in arbitrary_vector_pair(arbitrary_f32, 4..4048)
        ) {
            if !std::is_x86_feature_detected!("avx") {
                return Ok(());
            }
            prop_assume!(norm_l2(&x) > 1e-10);
            prop_assume!(norm_l2(&y) > 1e-10);
            let x_norm = norm_l2(&x);
            let scalar = cosine_scalar(&x, x_norm, &y);
            let avx = unsafe { f32_x86::cosine_fast_avx(&x, x_norm, &y) };
            prop_assert!(approx::relative_eq!(scalar, avx, max_relative = 1e-5));
        }

        /// Cross-backend parity for the f32 cosine_with_norms kernel.
        /// Exercises the scalar fallback (`cosine_scalar_fast`) against the
        /// dispatched SIMD path so the runtime fallback is exercised even on
        /// AVX2-capable CI hosts.
        #[test]
        fn test_cosine_with_norms_f32_scalar_simd_parity(
            (x, y) in arbitrary_vector_pair(arbitrary_f32, 4..4048)
        ) {
            prop_assume!(norm_l2(&x) > 1e-10);
            prop_assume!(norm_l2(&y) > 1e-10);
            let x_norm = norm_l2(&x);
            let y_norm = norm_l2(&y);
            let x_f64: Vec<f64> = x.iter().map(|&v| v as f64).collect();
            let y_f64: Vec<f64> = y.iter().map(|&v| v as f64).collect();
            let scalar = cosine_with_norms_scalar(&x_f64, x_norm, y_norm, &y_f64);
            let simd = <f32 as Cosine>::cosine_with_norms(&x, x_norm, y_norm, &y);
            prop_assert!(approx::relative_eq!(scalar, simd, max_relative = 1e-3));
        }

        /// AVX-512-direct parity for the f32 cosine_with_norms kernel.
        /// Early-returns on hosts without AVX-512F.
        #[cfg(target_arch = "x86_64")]
        #[test]
        fn test_cosine_with_norms_f32_scalar_vs_avx512_parity(
            (x, y) in arbitrary_vector_pair(arbitrary_f32, 4..4048)
        ) {
            if !std::is_x86_feature_detected!("avx512f") {
                return Ok(());
            }
            prop_assume!(norm_l2(&x) > 1e-10);
            prop_assume!(norm_l2(&y) > 1e-10);
            let x_norm = norm_l2(&x);
            let y_norm = norm_l2(&y);
            let scalar = cosine_scalar_fast(&x, x_norm, &y, y_norm);
            let avx512 = unsafe { f32_x86::cosine_with_norms_avx512(&x, x_norm, y_norm, &y) };
            prop_assert!(approx::relative_eq!(scalar, avx512, max_relative = 1e-5));
        }

        /// AVX + FMA-direct parity for the f32 cosine_with_norms kernel.
        /// Covers the AMD Piledriver / Steamroller / FX-7500 tier.
        /// Early-returns on hosts without both AVX and FMA.
        #[cfg(target_arch = "x86_64")]
        #[test]
        fn test_cosine_with_norms_f32_scalar_vs_avx_fma_parity(
            (x, y) in arbitrary_vector_pair(arbitrary_f32, 4..4048)
        ) {
            if !(std::is_x86_feature_detected!("avx") && std::is_x86_feature_detected!("fma")) {
                return Ok(());
            }
            prop_assume!(norm_l2(&x) > 1e-10);
            prop_assume!(norm_l2(&y) > 1e-10);
            let x_norm = norm_l2(&x);
            let y_norm = norm_l2(&y);
            let scalar = cosine_scalar_fast(&x, x_norm, &y, y_norm);
            let avx_fma = unsafe { f32_x86::cosine_with_norms_avx_fma(&x, x_norm, y_norm, &y) };
            prop_assert!(approx::relative_eq!(scalar, avx_fma, max_relative = 1e-5));
        }

        /// AVX-only-direct parity for the f32 cosine_with_norms kernel.
        /// Covers the Intel Sandy Bridge / Ivy Bridge tier. Early-returns
        /// on hosts without AVX.
        #[cfg(target_arch = "x86_64")]
        #[test]
        fn test_cosine_with_norms_f32_scalar_vs_avx_parity(
            (x, y) in arbitrary_vector_pair(arbitrary_f32, 4..4048)
        ) {
            if !std::is_x86_feature_detected!("avx") {
                return Ok(());
            }
            prop_assume!(norm_l2(&x) > 1e-10);
            prop_assume!(norm_l2(&y) > 1e-10);
            let x_norm = norm_l2(&x);
            let y_norm = norm_l2(&y);
            let scalar = cosine_scalar_fast(&x, x_norm, &y, y_norm);
            let avx = unsafe { f32_x86::cosine_with_norms_avx(&x, x_norm, y_norm, &y) };
            prop_assert!(approx::relative_eq!(scalar, avx, max_relative = 1e-5));
        }

        /// Cross-backend parity for the f64 cosine_fast kernel. Uses the
        /// hand-rolled `cosine_fast_scalar` (not the trait-routed
        /// `cosine_scalar`, which would itself dispatch through `dot::<f64>`)
        /// so the reference stays free of any AVX path on AVX2-capable hosts.
        #[test]
        fn test_cosine_fast_f64_scalar_simd_parity(
            (x, y) in arbitrary_vector_pair(arbitrary_f64, 4..4048)
        ) {
            prop_assume!(norm_l2(&x) > 1e-20);
            prop_assume!(norm_l2(&y) > 1e-20);
            let x_norm = norm_l2(&x);
            let scalar = cosine_fast_scalar(&x, x_norm, &y);
            let simd = <f64 as Cosine>::cosine_fast(&x, x_norm, &y);
            prop_assert!(approx::relative_eq!(scalar, simd, max_relative = 1e-3));
        }

        /// AVX-512-direct parity for the f64 cosine_fast kernel. Early-returns
        /// on hosts without AVX-512F.
        #[cfg(target_arch = "x86_64")]
        #[test]
        fn test_cosine_fast_f64_scalar_vs_avx512_parity(
            (x, y) in arbitrary_vector_pair(arbitrary_f64, 4..4048)
        ) {
            if !std::is_x86_feature_detected!("avx512f") {
                return Ok(());
            }
            prop_assume!(norm_l2(&x) > 1e-20);
            prop_assume!(norm_l2(&y) > 1e-20);
            let x_norm = norm_l2(&x);
            let scalar = cosine_fast_scalar(&x, x_norm, &y);
            let avx512 = unsafe { f64_x86::cosine_fast_avx512(&x, x_norm, &y) };
            prop_assert!(approx::relative_eq!(scalar, avx512, max_relative = 1e-5));
        }

        /// AVX + FMA-direct parity for the f64 cosine_fast kernel. Covers
        /// the AMD Piledriver / Steamroller / FX-7500 tier. Early-returns
        /// on hosts without both AVX and FMA.
        #[cfg(target_arch = "x86_64")]
        #[test]
        fn test_cosine_fast_f64_scalar_vs_avx_fma_parity(
            (x, y) in arbitrary_vector_pair(arbitrary_f64, 4..4048)
        ) {
            if !(std::is_x86_feature_detected!("avx") && std::is_x86_feature_detected!("fma")) {
                return Ok(());
            }
            prop_assume!(norm_l2(&x) > 1e-20);
            prop_assume!(norm_l2(&y) > 1e-20);
            let x_norm = norm_l2(&x);
            let scalar = cosine_fast_scalar(&x, x_norm, &y);
            let avx_fma = unsafe { f64_x86::cosine_fast_avx_fma(&x, x_norm, &y) };
            prop_assert!(approx::relative_eq!(scalar, avx_fma, max_relative = 1e-5));
        }

        /// AVX-only-direct parity for the f64 cosine_fast kernel. Covers
        /// the Intel Sandy Bridge / Ivy Bridge tier. Early-returns on
        /// hosts without AVX.
        #[cfg(target_arch = "x86_64")]
        #[test]
        fn test_cosine_fast_f64_scalar_vs_avx_parity(
            (x, y) in arbitrary_vector_pair(arbitrary_f64, 4..4048)
        ) {
            if !std::is_x86_feature_detected!("avx") {
                return Ok(());
            }
            prop_assume!(norm_l2(&x) > 1e-20);
            prop_assume!(norm_l2(&y) > 1e-20);
            let x_norm = norm_l2(&x);
            let scalar = cosine_fast_scalar(&x, x_norm, &y);
            let avx = unsafe { f64_x86::cosine_fast_avx(&x, x_norm, &y) };
            prop_assert!(approx::relative_eq!(scalar, avx, max_relative = 1e-5));
        }

        /// Parity check for `cosine_once_8` (despecialised 8-lane width).
        ///
        /// The `epsilon = 1e-6` clause handles the case where the proptest
        /// generator produces inputs with extreme dynamic range (e.g., mixing
        /// `1e-43` with `1e7` in the same vector). When the dot product is
        /// dominated by one large term and the cosine result is near zero,
        /// the f32-precision SIMD path and the f64-precision scalar reference
        /// can legitimately differ by more than `max_relative = 1e-3` of the
        /// (near-zero) result. The absolute epsilon catches these without
        /// masking real bugs (where the absolute error would be > 1e-6).
        #[test]
        fn test_cosine_once_8_scalar_simd_parity(
            (x, y) in arbitrary_vector_pair(arbitrary_f32, 8..9)
        ) {
            prop_assume!(norm_l2(&x) > 1e-10);
            prop_assume!(norm_l2(&y) > 1e-10);
            let x_norm = norm_l2(&x);
            let x_f64: Vec<f64> = x.iter().map(|&v| v as f64).collect();
            let y_f64: Vec<f64> = y.iter().map(|&v| v as f64).collect();
            let scalar = cosine_fast_scalar(&x_f64, x_norm, &y_f64);
            let simd = f32::cosine_once_8(&x, x_norm, &y);
            prop_assert!(approx::relative_eq!(scalar, simd, max_relative = 1e-3, epsilon = 1e-6));
        }

        /// Parity check for `cosine_once_16` (despecialised 16-lane width).
        /// See `test_cosine_once_8_scalar_simd_parity` for `epsilon` rationale.
        #[test]
        fn test_cosine_once_16_scalar_simd_parity(
            (x, y) in arbitrary_vector_pair(arbitrary_f32, 16..17)
        ) {
            prop_assume!(norm_l2(&x) > 1e-10);
            prop_assume!(norm_l2(&y) > 1e-10);
            let x_norm = norm_l2(&x);
            let x_f64: Vec<f64> = x.iter().map(|&v| v as f64).collect();
            let y_f64: Vec<f64> = y.iter().map(|&v| v as f64).collect();
            let scalar = cosine_fast_scalar(&x_f64, x_norm, &y_f64);
            let simd = f32::cosine_once_16(&x, x_norm, &y);
            prop_assert!(approx::relative_eq!(scalar, simd, max_relative = 1e-3, epsilon = 1e-6));
        }

        /// AVX-512-direct parity for the 8-lane cosine_once kernel. Verifies
        /// the masked-load (mask 0x00FF) AVX-512 implementation produces
        /// the same result as the scalar reference. Early-returns on hosts
        /// without AVX-512F.
        #[cfg(target_arch = "x86_64")]
        #[test]
        fn test_cosine_once_8_scalar_vs_avx512_parity(
            (x, y) in arbitrary_vector_pair(arbitrary_f32, 8..9)
        ) {
            if !std::is_x86_feature_detected!("avx512f") {
                return Ok(());
            }
            prop_assume!(norm_l2(&x) > 1e-10);
            prop_assume!(norm_l2(&y) > 1e-10);
            let x_norm = norm_l2(&x);
            let scalar = super::f32::cosine_once_8_scalar(&x, x_norm, &y);
            let avx512 =
                unsafe { super::f32::cosine_once_x86::cosine_once_8_avx512(&x, x_norm, &y) };
            prop_assert!(approx::relative_eq!(scalar, avx512, max_relative = 1e-5));
        }

        /// AVX-512-direct parity for the 16-lane cosine_once kernel. Verifies
        /// the full-width `__m512` load implementation produces the same
        /// result as the scalar reference. Early-returns on hosts without
        /// AVX-512F.
        #[cfg(target_arch = "x86_64")]
        #[test]
        fn test_cosine_once_16_scalar_vs_avx512_parity(
            (x, y) in arbitrary_vector_pair(arbitrary_f32, 16..17)
        ) {
            if !std::is_x86_feature_detected!("avx512f") {
                return Ok(());
            }
            prop_assume!(norm_l2(&x) > 1e-10);
            prop_assume!(norm_l2(&y) > 1e-10);
            let x_norm = norm_l2(&x);
            let scalar = super::f32::cosine_once_16_scalar(&x, x_norm, &y);
            let avx512 =
                unsafe { super::f32::cosine_once_x86::cosine_once_16_avx512(&x, x_norm, &y) };
            prop_assert!(approx::relative_eq!(scalar, avx512, max_relative = 1e-5));
        }

        /// AVX + FMA-direct parity for the 8-lane cosine_once kernel.
        /// Covers the AMD Piledriver / Steamroller / FX-7500 tier.
        /// Early-returns on hosts without both AVX and FMA.
        #[cfg(target_arch = "x86_64")]
        #[test]
        fn test_cosine_once_8_scalar_vs_avx_fma_parity(
            (x, y) in arbitrary_vector_pair(arbitrary_f32, 8..9)
        ) {
            if !(std::is_x86_feature_detected!("avx") && std::is_x86_feature_detected!("fma")) {
                return Ok(());
            }
            prop_assume!(norm_l2(&x) > 1e-10);
            prop_assume!(norm_l2(&y) > 1e-10);
            let x_norm = norm_l2(&x);
            let scalar = super::f32::cosine_once_8_scalar(&x, x_norm, &y);
            let avx_fma =
                unsafe { super::f32::cosine_once_x86::cosine_once_8_avx_fma(&x, x_norm, &y) };
            prop_assert!(approx::relative_eq!(scalar, avx_fma, max_relative = 1e-5));
        }

        /// AVX + FMA-direct parity for the 16-lane cosine_once kernel.
        /// Covers the AMD Piledriver / Steamroller / FX-7500 tier.
        /// Early-returns on hosts without both AVX and FMA.
        #[cfg(target_arch = "x86_64")]
        #[test]
        fn test_cosine_once_16_scalar_vs_avx_fma_parity(
            (x, y) in arbitrary_vector_pair(arbitrary_f32, 16..17)
        ) {
            if !(std::is_x86_feature_detected!("avx") && std::is_x86_feature_detected!("fma")) {
                return Ok(());
            }
            prop_assume!(norm_l2(&x) > 1e-10);
            prop_assume!(norm_l2(&y) > 1e-10);
            let x_norm = norm_l2(&x);
            let scalar = super::f32::cosine_once_16_scalar(&x, x_norm, &y);
            let avx_fma =
                unsafe { super::f32::cosine_once_x86::cosine_once_16_avx_fma(&x, x_norm, &y) };
            prop_assert!(approx::relative_eq!(scalar, avx_fma, max_relative = 1e-5));
        }

        /// AVX-only-direct parity for the 8-lane cosine_once kernel.
        /// Covers the Intel Sandy Bridge / Ivy Bridge tier. Early-returns
        /// on hosts without AVX.
        #[cfg(target_arch = "x86_64")]
        #[test]
        fn test_cosine_once_8_scalar_vs_avx_parity(
            (x, y) in arbitrary_vector_pair(arbitrary_f32, 8..9)
        ) {
            if !std::is_x86_feature_detected!("avx") {
                return Ok(());
            }
            prop_assume!(norm_l2(&x) > 1e-10);
            prop_assume!(norm_l2(&y) > 1e-10);
            let x_norm = norm_l2(&x);
            let scalar = super::f32::cosine_once_8_scalar(&x, x_norm, &y);
            let avx = unsafe { super::f32::cosine_once_x86::cosine_once_8_avx(&x, x_norm, &y) };
            prop_assert!(approx::relative_eq!(scalar, avx, max_relative = 1e-5));
        }

        /// AVX-only-direct parity for the 16-lane cosine_once kernel.
        /// Covers the Intel Sandy Bridge / Ivy Bridge tier. Early-returns
        /// on hosts without AVX.
        #[cfg(target_arch = "x86_64")]
        #[test]
        fn test_cosine_once_16_scalar_vs_avx_parity(
            (x, y) in arbitrary_vector_pair(arbitrary_f32, 16..17)
        ) {
            if !std::is_x86_feature_detected!("avx") {
                return Ok(());
            }
            prop_assume!(norm_l2(&x) > 1e-10);
            prop_assume!(norm_l2(&y) > 1e-10);
            let x_norm = norm_l2(&x);
            let scalar = super::f32::cosine_once_16_scalar(&x, x_norm, &y);
            let avx = unsafe { super::f32::cosine_once_x86::cosine_once_16_avx(&x, x_norm, &y) };
            prop_assert!(approx::relative_eq!(scalar, avx, max_relative = 1e-5));
        }
    }

    /// Asserts a batch-level f32 cosine SIMD kernel matches the scalar
    /// `cosine_fast` reference for every vector in a multi-vector batch. Runs
    /// each of the kernel's three internal dimension arms (8, 16, and the
    /// general `chunks_exact` path). The batch kernels only run at runtime on
    /// sub-AVX2 builds, so a direct call is the only way they get covered.
    #[cfg(target_arch = "x86_64")]
    fn check_cosine_batch_kernel(kernel: unsafe fn(&[f32], f32, &[f32], usize) -> Vec<f32>) {
        for dimension in [8_usize, 16, 40] {
            let x: Vec<f32> = (0..dimension).map(|i| (i as f32) * 0.5 + 1.0).collect();
            let x_norm = norm_l2(&x);
            let num_vectors = 3;
            let batch: Vec<f32> = (0..dimension * num_vectors)
                .map(|i| ((i % 7) as f32) + 1.0)
                .collect();

            let got = unsafe { kernel(&x, x_norm, &batch, dimension) };
            assert_eq!(got.len(), num_vectors);

            let x_f64: Vec<f64> = x.iter().map(|&v| v as f64).collect();
            for (chunk, &g) in batch.chunks_exact(dimension).zip(got.iter()) {
                let y_f64: Vec<f64> = chunk.iter().map(|&v| v as f64).collect();
                let expected = cosine_fast_scalar(&x_f64, x_norm, &y_f64);
                assert_relative_eq!(g, expected, max_relative = 1e-3, epsilon = 1e-6);
            }
        }
    }

    /// AVX + FMA batch kernel parity (AVX2 / AVX+FMA tiers). Runs on any
    /// Haswell-or-newer host; early-returns without AVX and FMA.
    #[cfg(target_arch = "x86_64")]
    #[test]
    fn test_cosine_batch_avx_fma_matches_scalar() {
        if !(std::is_x86_feature_detected!("avx") && std::is_x86_feature_detected!("fma")) {
            return;
        }
        check_cosine_batch_kernel(super::f32::cosine_batch_avx_fma);
    }

    /// AVX-only batch kernel parity (Sandy Bridge / Ivy Bridge tier).
    /// Early-returns on hosts without AVX.
    #[cfg(target_arch = "x86_64")]
    #[test]
    fn test_cosine_batch_avx_matches_scalar() {
        if !std::is_x86_feature_detected!("avx") {
            return;
        }
        check_cosine_batch_kernel(super::f32::cosine_batch_avx);
    }

    /// AVX-512 batch kernel parity. Early-returns on hosts without AVX-512F.
    #[cfg(target_arch = "x86_64")]
    #[test]
    fn test_cosine_batch_avx512_matches_scalar() {
        if !std::is_x86_feature_detected!("avx512f") {
            return;
        }
        check_cosine_batch_kernel(super::f32::cosine_batch_avx512);
    }
}
