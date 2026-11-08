// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! L2 (Euclidean) distance.
//!

use std::iter::Sum;
use std::ops::AddAssign;
use std::sync::Arc;

use crate::Result;
use argminmax::ArgMinMax;
use arrow_array::{Array, FixedSizeListArray, Float32Array};
use half::{bf16, f16};
use lance_core::deepsize::DeepSizeOf;
use lance_core::utils::cpu::SIMD_SUPPORT;
#[cfg(target_arch = "x86_64")]
use lance_core::utils::cpu::SimdSupport;
use num_traits::{AsPrimitive, Num};

use crate::distance::{
    BatchKernel, HALF_KERNELS_COMPILED, HalfBackend, HalfType, PairKernel,
    U8_U32_ACCUMULATOR_MAX_LEN, assert_batch_output_layout, assert_equal_lengths, half_backend,
    x86_half_features,
};

/// Calculate the L2 distance between two vectors.
///
pub trait L2: Num {
    /// Calculate the L2 distance between two vectors.
    ///
    /// # Panics
    ///
    /// The implementations in this crate panic if `x` and `y` have different
    /// lengths. A downstream implementation may define different behavior.
    fn l2(x: &[Self], y: &[Self]) -> f32;

    /// Writes the L2 distance from `x` to each `dimension`-sized vector in
    /// `batch` into the matching element of `output`.
    ///
    /// The default calls [`L2::l2`] per vector. `f32` overrides it with one
    /// batch kernel call and `f16` selects its backend once per batch, so
    /// neither chooses a kernel per vector. `f64` picks its x86_64 SIMD tier
    /// once per batch, and on other targets specializes the loop for the
    /// common PQ sub-vector lengths.
    ///
    /// # Panics
    ///
    /// The default implementation panics if `dimension == 0`, if
    /// `x.len() != dimension`, if `batch.len()` is not a multiple of
    /// `dimension`, or if `output.len() != batch.len() / dimension`.
    fn l2_batch_into(x: &[Self], batch: &[Self], dimension: usize, output: &mut [f32]) {
        assert_batch_output_layout(x.len(), batch.len(), dimension, output.len());
        for (distance, y) in output.iter_mut().zip(batch.chunks_exact(dimension)) {
            *distance = Self::l2(x, y);
        }
    }
}

/// Squared L2 distance between two vectors of any [`L2`] element type.
///
/// # Panics
///
/// The implementations in this crate panic if `from` and `to` have different
/// lengths. A downstream [`L2`] implementation may define different behavior.
#[inline]
pub fn l2<T: L2>(from: &[T], to: &[T]) -> f32 {
    T::l2(from, to)
}

/// L2 distance between two f32 slices, dispatched to the widest SIMD backend
/// available at runtime.
///
/// On x86_64 with AVX-512 this uses 16-wide f32 lanes; otherwise it falls back
/// to [`l2`], whose x86_64 implementation selects the best runtime-supported
/// kernel. This entry point gives hot-path callers such as the in-memory HNSW
/// index an explicit f32 API.
///
/// # Panics
///
/// Panics if `x` and `y` have different lengths.
#[inline]
pub fn l2_f32(x: &[f32], y: &[f32]) -> f32 {
    f32::l2(x, y)
}

/// Calculate L2 distance between two uint8 slices.
///
/// # Panics
///
/// Panics if `key` and `target` have different lengths.
#[inline]
pub fn l2_distance_uint_scalar(key: &[u8], target: &[u8]) -> f32 {
    assert_equal_lengths(key.len(), target.len());
    // Keep the common path on a u32 accumulator so LLVM can auto-vectorize it
    // efficiently. Longer inputs are widened between overflow-safe chunks.
    if key.len() <= U8_U32_ACCUMULATOR_MAX_LEN {
        return key
            .iter()
            .zip(target.iter())
            .map(|(&x, &y)| (x.abs_diff(y) as u32).pow(2))
            .sum::<u32>() as f32;
    }

    key.chunks(U8_U32_ACCUMULATOR_MAX_LEN)
        .zip(target.chunks(U8_U32_ACCUMULATOR_MAX_LEN))
        .map(|(key, target)| {
            key.iter()
                .zip(target.iter())
                .map(|(&x, &y)| (x.abs_diff(y) as u32).pow(2))
                .sum::<u32>() as u64
        })
        .sum::<u64>() as f32
}

/// Calculate the L2 distance between two vectors, using scalar operations.
///
/// It relies on LLVM for auto-vectorization and unrolling.
///
/// This is pub for test/benchmark only. use [l2] instead.
///
/// # Panics
///
/// Panics if `from` and `to` have different lengths, and separately if `LANES`
/// is zero, which `as_chunks` rejects.
#[inline(always)]
pub fn l2_scalar<
    T: AsPrimitive<Output>,
    Output: Num + Copy + Sum + AddAssign + 'static,
    const LANES: usize,
>(
    from: &[T],
    to: &[T],
) -> Output {
    l2_scalar_mixed::<T, T, Output, LANES>(from, to)
}

/// [`l2_scalar`] with differently typed inputs, so a caller can convert a
/// query once and reuse it across a batch. Converting to `Output` up front
/// gives the same values as converting inside the loop, so results match
/// [`l2_scalar`] on the unconverted inputs bit for bit.
#[inline(always)]
fn l2_scalar_mixed<
    X: AsPrimitive<Output>,
    Y: AsPrimitive<Output>,
    Output: Num + Copy + Sum + AddAssign + 'static,
    const LANES: usize,
>(
    from: &[X],
    to: &[Y],
) -> Output {
    assert_equal_lengths(from.len(), to.len());
    let (x_chunks, x_remainder) = from.as_chunks::<LANES>();
    let (y_chunks, y_remainder) = to.as_chunks::<LANES>();

    let s = if !x_remainder.is_empty() {
        x_remainder
            .iter()
            .zip(y_remainder)
            .map(|(&x, &y)| {
                let diff = x.as_() - y.as_();
                diff * diff
            })
            .sum::<Output>()
    } else {
        Output::zero()
    };

    let mut sums = [Output::zero(); LANES];
    for (x, y) in x_chunks.iter().zip(y_chunks) {
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
        assert_equal_lengths(x.len(), y.len());
        super::l2_u8::l2_u8_u64(x, y) as f32
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
                bf16_kernel::l2_bf16_neon(x.as_ptr(), y.as_ptr(), x.len() as u32)
            },
            #[cfg(all(
                feature = "fp16kernels",
                kernel_support = "avx512_bf16",
                target_arch = "x86_64"
            ))]
            HalfBackend::Avx512 => unsafe {
                bf16_kernel::l2_bf16_avx512(x.as_ptr(), y.as_ptr(), x.len() as u32)
            },
            #[cfg(all(feature = "fp16kernels", target_arch = "x86_64"))]
            HalfBackend::Avx2 => unsafe {
                bf16_kernel::l2_bf16_avx2(x.as_ptr(), y.as_ptr(), x.len() as u32)
            },
            #[cfg(all(feature = "fp16kernels", target_arch = "loongarch64"))]
            HalfBackend::Lasx => unsafe {
                bf16_kernel::l2_bf16_lasx(x.as_ptr(), y.as_ptr(), x.len() as u32)
            },
            #[cfg(all(feature = "fp16kernels", target_arch = "loongarch64"))]
            HalfBackend::Lsx => unsafe {
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

fn f16_backend() -> HalfBackend {
    half_backend(
        *SIMD_SUPPORT,
        HalfType::F16,
        HALF_KERNELS_COMPILED,
        cfg!(all(kernel_support = "avx512_f16", target_arch = "x86_64")),
        x86_half_features(),
    )
}

impl L2 for f16 {
    #[inline]
    fn l2(x: &[Self], y: &[Self]) -> f32 {
        assert_equal_lengths(x.len(), y.len());
        l2_f16_with(f16_backend(), x, y)
    }

    fn l2_batch_into(x: &[Self], batch: &[Self], dimension: usize, output: &mut [f32]) {
        assert_batch_output_layout(x.len(), batch.len(), dimension, output.len());
        // Selecting the backend reads several runtime feature flags, which
        // costs as much as a dimension-8 distance, so select it once per batch.
        // The scalar backend converts the query to f32 once, instead of once
        // per vector, and gets its own loop so the scalar kernel inlines.
        // Interim: f16 still selects through `half_backend`, not `Dispatch`;
        // it moves onto `Dispatch` batch candidates later in #10083, which
        // must keep the convert-the-query-once step.
        let backend = f16_backend();
        #[cfg(all(feature = "fp16kernels", target_arch = "aarch64"))]
        if backend == HalfBackend::Neon {
            return l2_f16_batch_neon(x, batch, dimension, output);
        }
        if backend == HalfBackend::Scalar {
            let x: Vec<f32> = x.iter().map(|v| v.to_f32()).collect();
            for (distance, y) in output.iter_mut().zip(batch.chunks_exact(dimension)) {
                *distance = l2_scalar_mixed::<f32, Self, f32, 16>(&x, y);
            }
        } else {
            for (distance, y) in output.iter_mut().zip(batch.chunks_exact(dimension)) {
                *distance = l2_f16_with(backend, x, y);
            }
        }
    }
}

/// Out of line so the loop gets registers of its own: inlined into
/// `l2_batch_into`, LLVM spilled the batch pointer to the stack, which put a
/// store-to-load round trip on every dimension-8 distance.
#[cfg(all(feature = "fp16kernels", target_arch = "aarch64"))]
#[inline(never)]
fn l2_f16_batch_neon(x: &[f16], batch: &[f16], dimension: usize, output: &mut [f32]) {
    for (distance, y) in output.iter_mut().zip(batch.chunks_exact(dimension)) {
        *distance = l2_f16_with(HalfBackend::Neon, x, y);
    }
}

#[inline(always)]
fn l2_f16_with(backend: HalfBackend, x: &[f16], y: &[f16]) -> f32 {
    match backend {
        #[cfg(all(feature = "fp16kernels", target_arch = "aarch64"))]
        HalfBackend::Neon => unsafe { kernel::l2_f16_neon(x.as_ptr(), y.as_ptr(), x.len() as u32) },
        #[cfg(all(
            feature = "fp16kernels",
            kernel_support = "avx512_f16",
            target_arch = "x86_64"
        ))]
        HalfBackend::Avx512 => unsafe {
            kernel::l2_f16_avx512(x.as_ptr(), y.as_ptr(), x.len() as u32)
        },
        #[cfg(all(feature = "fp16kernels", target_arch = "x86_64"))]
        HalfBackend::Avx2 => unsafe { kernel::l2_f16_avx2(x.as_ptr(), y.as_ptr(), x.len() as u32) },
        #[cfg(all(feature = "fp16kernels", target_arch = "loongarch64"))]
        HalfBackend::Lasx => unsafe { kernel::l2_f16_lasx(x.as_ptr(), y.as_ptr(), x.len() as u32) },
        #[cfg(all(feature = "fp16kernels", target_arch = "loongarch64"))]
        HalfBackend::Lsx => unsafe { kernel::l2_f16_lsx(x.as_ptr(), y.as_ptr(), x.len() as u32) },
        // SimdSupport::AvxFma and SimdSupport::Avx retain their scalar
        // route; this fallback only extends the tiers the C kernel already
        // served to Avx512FP16 after checking F16C and FMA.
        _ => l2_scalar::<f16, f32, 16>(x, y),
    }
}

impl L2 for f32 {
    #[inline]
    fn l2(x: &[Self], y: &[Self]) -> f32 {
        assert_equal_lengths(x.len(), y.len());
        L2_F32.get().call(x, y)
    }

    fn l2_batch_into(x: &[Self], batch: &[Self], dimension: usize, output: &mut [f32]) {
        assert_batch_output_layout(x.len(), batch.len(), dimension, output.len());
        L2_F32_BATCH.get().call(x, batch, dimension, output);
    }
}

lance_core::dispatch! {
    static L2_F32: Dispatch<PairKernel> = {
        #[cfg(target_arch = "x86_64")]
        {
            &[
                x86::L2_F32_AVX512,
                x86::L2_F32_AVX_FMA,
                x86::L2_F32_AVX,
                L2_F32_SCALAR,
            ]
        }
        #[cfg(not(target_arch = "x86_64"))]
        {
            &[L2_F32_SCALAR]
        }
    };
}

lance_core::dispatch! {
    /// There is no AVX-512 candidate: the AVX+FMA kernel is the best one so
    /// that Haswell-baseline builds select it at compile time, with no
    /// per-batch dispatch at the 8-wide PQ dimension. Its dimension-dependent
    /// choice mirrors what those builds have always computed.
    static L2_F32_BATCH: Dispatch<BatchKernel> = {
        #[cfg(target_arch = "x86_64")]
        {
            &[
                x86::L2_F32_BATCH_AVX_FMA,
                x86::L2_F32_BATCH_AVX,
                L2_F32_BATCH_SCALAR,
            ]
        }
        #[cfg(not(target_arch = "x86_64"))]
        {
            &[L2_F32_BATCH_SCALAR]
        }
    };
}

lance_core::target_kernel! {
    #[target_features()]
    #[candidate(L2_F32_SCALAR: PairKernel)]
    /// Portable L2 distance for f32, relying on LLVM to auto-vectorize it.
    #[inline]
    fn l2_f32_scalar(x: &[f32], y: &[f32]) -> f32 {
        // Below one 16-lane chunk (e.g. dimension-8 PQ sub-vectors) the
        // 16-lane kernel is all scalar remainder: a serial add chain whose
        // speed swings with code layout. 8 lanes vectorizes it.
        if x.len() < 16 {
            return l2_scalar::<f32, f32, 8>(x, y);
        }
        // 16 = 512 (avx512) / 8 bits / 4 (sizeof(f32))
        // See https://github.com/lance-format/lance/pull/2450.
        l2_scalar::<f32, f32, 16>(x, y)
    }
}

lance_core::target_kernel! {
    #[target_features()]
    #[candidate(L2_F32_BATCH_SCALAR: BatchKernel)]
    #[inline]
    fn l2_batch_f32_scalar(x: &[f32], batch: &[f32], dimension: usize, output: &mut [f32]) {
        for (distance, y) in output.iter_mut().zip(batch.chunks_exact(dimension)) {
            *distance = l2_f32_scalar(x, y);
        }
    }
}

impl L2 for f64 {
    #[inline]
    fn l2(x: &[Self], y: &[Self]) -> f32 {
        assert_equal_lengths(x.len(), y.len());
        l2_f64_simd(x, y)
    }

    fn l2_batch_into(x: &[Self], batch: &[Self], dimension: usize, output: &mut [f32]) {
        assert_batch_output_layout(x.len(), batch.len(), dimension, output.len());
        // Reading `SIMD_SUPPORT` per vector costs a large share of a
        // dimension-8 distance, so pick the tier once per batch. Each tier's
        // loop carries that tier's target features, so its kernel inlines.
        // Interim: f64 still selects through `SIMD_SUPPORT`, not `Dispatch`;
        // these per-tier loops become its `Dispatch` batch candidates later
        // in #10083.
        //
        // SAFETY (all three calls): `SIMD_SUPPORT` reports a tier only when
        // the host has its features: avx512f for the AVX-512 tiers, avx and
        // fma for `Avx2`/`AvxFma`, and avx for `Avx`.
        #[cfg(target_arch = "x86_64")]
        match *SIMD_SUPPORT {
            SimdSupport::Avx512 | SimdSupport::Avx512FP16 => unsafe {
                x86::l2_batch_f64_avx512(x, batch, dimension, output)
            },
            SimdSupport::Avx2 | SimdSupport::AvxFma => unsafe {
                x86::l2_batch_f64_avx_fma(x, batch, dimension, output)
            },
            SimdSupport::Avx => unsafe { x86::l2_batch_f64_avx(x, batch, dimension, output) },
            _ => {
                for (distance, y) in output.iter_mut().zip(batch.chunks_exact(dimension)) {
                    *distance = l2_f64_scalar(x, y);
                }
            }
        }
        // A compile-time query length lets LLVM keep the query in registers
        // and drop the kernel's 4-lane and scalar tail stages, which it
        // otherwise doesn't reliably unswitch out of the loop. 8 and 16 are
        // the PQ sub-vector lengths of 128-dimension vectors.
        #[cfg(not(target_arch = "x86_64"))]
        if let Ok(x) = <&[Self; 8]>::try_from(x) {
            l2_batch_f64_fixed(x, batch, output);
        } else if let Ok(x) = <&[Self; 16]>::try_from(x) {
            l2_batch_f64_fixed(x, batch, output);
        } else {
            // A pointer walk rather than `zip` with `chunks_exact`: on aarch64
            // the zip indexed each vector with a multiply, which measurably
            // slowed short vectors.
            let mut y = batch.as_ptr();
            for distance in output.iter_mut() {
                // SAFETY: the layout assert guarantees `batch.len() == output.len() * dimension`.
                unsafe {
                    *distance = l2_f64_simd_other(x, std::slice::from_raw_parts(y, dimension));
                    y = y.add(dimension);
                }
            }
        }
    }
}

#[cfg(not(target_arch = "x86_64"))]
#[inline(always)]
fn l2_batch_f64_fixed<const N: usize>(x: &[f64; N], batch: &[f64], output: &mut [f32]) {
    for (distance, y) in output.iter_mut().zip(batch.as_chunks::<N>().0) {
        *distance = l2_f64_simd_other(x, y);
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

    use crate::distance::{BatchKernel, PairKernel};
    use crate::simd::f64::{f64x4, f64x8};
    use crate::simd::x86::hsum256_ps;
    use crate::simd::{FloatSimd, SIMD};

    lance_core::target_kernel! {
        #[target_features("avx", "fma")]
        #[candidate(pub(super) L2_F32_BATCH_AVX_FMA: BatchKernel)]
        /// Up to 16 dimensions the scalar kernel's 16-lane chunking degenerates
        /// to its remainder loop, so the explicit AVX kernel wins; above that
        /// the auto-vectorized scalar kernel is as good or better.
        pub(super) fn l2_batch_f32_avx_fma(
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
                    let diff = _mm256_sub_ps(key, unsafe { _mm256_loadu_ps(y.as_ptr()) });
                    *distance = unsafe { hsum256_ps(_mm256_mul_ps(diff, diff)) };
                }
            } else if dimension == 16
                && let Ok(key) = <&[f32; 16]>::try_from(x)
            {
                // Same sums as `l2_f32_avx_fma`, whose first FMA adds to zero,
                // with the key loaded once per batch.
                let (key_lo, key_hi) =
                    unsafe { (_mm256_loadu_ps(key.as_ptr()), _mm256_loadu_ps(key.as_ptr().add(8))) };
                for (distance, y) in output.iter_mut().zip(batch.as_chunks::<16>().0) {
                    let (y_lo, y_hi) =
                        unsafe { (_mm256_loadu_ps(y.as_ptr()), _mm256_loadu_ps(y.as_ptr().add(8))) };
                    let (diff_lo, diff_hi) = (_mm256_sub_ps(key_lo, y_lo), _mm256_sub_ps(key_hi, y_hi));
                    let sum = _mm256_fmadd_ps(diff_hi, diff_hi, _mm256_mul_ps(diff_lo, diff_lo));
                    *distance = unsafe { hsum256_ps(sum) };
                }
            } else if dimension <= 16 {
                for (distance, y) in output.iter_mut().zip(batch.chunks_exact(dimension)) {
                    *distance = l2_f32_avx_fma(x, y);
                }
            } else {
                for (distance, y) in output.iter_mut().zip(batch.chunks_exact(dimension)) {
                    *distance = super::l2_f32_scalar(x, y);
                }
            }
        }
    }

    lance_core::target_kernel! {
        #[target_features("avx")]
        #[candidate(pub(super) L2_F32_BATCH_AVX: BatchKernel)]
        pub(super) fn l2_batch_f32_avx(
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
                    let diff = _mm256_sub_ps(key, unsafe { _mm256_loadu_ps(y.as_ptr()) });
                    *distance = unsafe { hsum256_ps(_mm256_mul_ps(diff, diff)) };
                }
            } else {
                for (distance, y) in output.iter_mut().zip(batch.chunks_exact(dimension)) {
                    *distance = l2_f32_avx(x, y);
                }
            }
        }
    }

    /// # Safety
    ///
    /// The host must support AVX-512F.
    #[target_feature(enable = "avx512f")]
    pub(super) unsafe fn l2_batch_f64_avx512(
        x: &[f64],
        batch: &[f64],
        dimension: usize,
        output: &mut [f32],
    ) {
        for (distance, y) in output.iter_mut().zip(batch.chunks_exact(dimension)) {
            *distance = unsafe { l2_f64_avx512(x, y) };
        }
    }

    /// # Safety
    ///
    /// The host must support AVX and FMA.
    #[target_feature(enable = "avx,fma")]
    pub(super) unsafe fn l2_batch_f64_avx_fma(
        x: &[f64],
        batch: &[f64],
        dimension: usize,
        output: &mut [f32],
    ) {
        for (distance, y) in output.iter_mut().zip(batch.chunks_exact(dimension)) {
            *distance = unsafe { l2_f64_avx_fma(x, y) };
        }
    }

    /// # Safety
    ///
    /// The host must support AVX.
    #[target_feature(enable = "avx")]
    pub(super) unsafe fn l2_batch_f64_avx(
        x: &[f64],
        batch: &[f64],
        dimension: usize,
        output: &mut [f32],
    ) {
        for (distance, y) in output.iter_mut().zip(batch.chunks_exact(dimension)) {
            *distance = unsafe { l2_f64_avx(x, y) };
        }
    }

    /// AVX-512 path for f64: 8-wide `__m512d` with `vsubpd` + `vfmadd231pd` per iteration.
    #[target_feature(enable = "avx512f")]
    #[inline]
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
    #[inline]
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
    #[inline]
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

    lance_core::target_kernel! {
        #[target_features("avx512f")]
        #[candidate(pub(super) L2_F32_AVX512: PairKernel)]
        /// AVX-512 path for f32: 16-wide `__m512` with `vsubps` + `vfmadd231ps` per iteration.
        #[inline]
        pub(super) fn l2_f32_avx512(x: &[f32], y: &[f32]) -> f32 {
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
    }

    lance_core::target_kernel! {
        #[target_features("avx", "fma")]
        #[candidate(pub(super) L2_F32_AVX_FMA: PairKernel)]
        /// AVX + FMA path for f32. Covers both AvxFma and AVX2 hosts (body uses no AVX2-specific intrinsics).
        #[inline]
        pub(super) fn l2_f32_avx_fma(x: &[f32], y: &[f32]) -> f32 {
            // See `l2_f32_avx512` for `min`.
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

            let sum = unsafe { hsum256_ps(acc) };
            sum + tail
        }
    }

    lance_core::target_kernel! {
        #[target_features("avx")]
        #[candidate(pub(super) L2_F32_AVX: PairKernel)]
        /// AVX-only path for f32 (no FMA): squared diff via `_mm256_mul_ps` + `_mm256_add_ps` for Sandy/Ivy Bridge.
        #[inline]
        pub(super) fn l2_f32_avx(x: &[f32], y: &[f32]) -> f32 {
            // See `l2_f32_avx512` for `min`.
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

            let sum = unsafe { hsum256_ps(acc) };
            sum + tail
        }
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
/// [`l2_distance_batch_into`] with its AoS per-target locality is faster.
#[derive(Debug, Clone, DeepSizeOf)]
pub struct L2Prepared {
    transposed: Vec<f32>,
    dimension: usize,
    num_targets: usize,
}

impl L2Prepared {
    /// Transpose `targets` from AoS `[num_targets][dimension]` to SoA layout.
    ///
    /// `targets.len()` must be a multiple of `dimension`.
    ///
    /// # Panics
    ///
    /// Panics if `dimension` is zero. With debug assertions on, also panics if
    /// `targets.len()` is not a multiple of `dimension`; without them the
    /// trailing partial vector is dropped.
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
    /// `query` must have length `dimension` and `out` must have length
    /// `num_targets`. `out` will be zeroed before accumulation.
    ///
    /// # Panics
    ///
    /// With debug assertions on, panics unless both lengths match. A `query`
    /// longer than `dimension` can also panic on a slice range without them.
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
    ///
    /// `query` must have length `dimension`.
    ///
    /// # Panics
    ///
    /// Panics under the conditions [`Self::distances_into`] documents, with the
    /// same debug/release behavior.
    pub fn distances(&self, query: &[f32]) -> Vec<f32> {
        let mut result = vec![0.0f32; self.num_targets];
        self.distances_into(query, &mut result);
        result
    }

    /// Return the index of the nearest target to `query`, using `buf` as scratch space.
    ///
    /// `query` must have length `dimension`, and `buf` must have length
    /// `num_targets`. NaN distances are ignored; on ties the lowest index wins.
    /// Returns `None` if there are no targets or no distance is finite.
    ///
    /// # Panics
    ///
    /// Panics under the conditions [`Self::distances_into`] documents, with the
    /// same debug/release behavior.
    pub fn nearest_into(&self, query: &[f32], buf: &mut [f32]) -> Option<u32> {
        self.distances_into(query, buf);
        let distances: &[f32] = buf;
        // `argmin` panics on an empty slice, and returns an index even when no
        // distance is finite (all NaN or infinite).
        if distances.is_empty() {
            return None;
        }
        let idx = distances.argmin();
        (distances[idx] < f32::INFINITY).then_some(idx as u32)
    }

    /// Return the index of the nearest target to `query`.
    ///
    /// `query` must have length `dimension`.
    ///
    /// # Panics
    ///
    /// Panics under the conditions [`Self::distances_into`] documents, with the
    /// same debug/release behavior.
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
///
/// # Panics
///
/// Panics if `from` and `to` have different lengths.
#[inline]
pub fn l2_distance(from: &[f32], to: &[f32]) -> f32 {
    l2(from, to)
}

/// Writes the L2 distance from `from` to each `dimension`-sized vector in `to`
/// into the matching element of `output`.
///
/// # Panics
///
/// The implementations in this crate panic if `dimension == 0`, if
/// `from.len() != dimension`, if `to.len()` is not a multiple of `dimension`,
/// or if `output.len() != to.len() / dimension`.
///
/// ```
/// # use lance_linalg::distance::l2_distance_batch_into;
/// let mut distances = [0.0; 2];
/// l2_distance_batch_into(&[1.0_f32, 2.0], &[1.0, 2.0, 3.0, 4.0], 2, &mut distances);
/// assert_eq!(distances, [0.0, 8.0]);
/// ```
#[inline]
pub fn l2_distance_batch_into<T: L2>(from: &[T], to: &[T], dimension: usize, output: &mut [f32]) {
    T::l2_batch_into(from, to, dimension, output);
}

/// Compute l2 distances, propagating the null buffer of `to`.
///
/// See [`super::DistanceType::batch`] for supported types and input validation.
#[deprecated(note = "Use DistanceType::L2.batch")]
pub fn l2_distance_arrow_batch(
    from: &dyn Array,
    to: &FixedSizeListArray,
) -> Result<Arc<Float32Array>> {
    super::DistanceType::L2.batch(from, to).map(Arc::new)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{cast::AsArray, types::Float32Type};

    use approx::assert_relative_eq;
    use num_traits::ToPrimitive;
    use proptest::prelude::*;

    #[test]
    fn test_l2_rejects_mismatched_lengths() {
        let short = [1.0_f32];
        let long = [1.0_f32, 2.0];

        assert!(std::panic::catch_unwind(|| l2(&short, &long)).is_err());
        assert!(std::panic::catch_unwind(|| l2_f32(&short, &long)).is_err());
        assert!(std::panic::catch_unwind(|| f32::l2(&short, &long)).is_err());
        assert!(std::panic::catch_unwind(|| l2_distance(&short, &long)).is_err());
        assert!(std::panic::catch_unwind(|| l2_distance_batch_vec(&short, &long, 2)).is_err());
        assert!(
            std::panic::catch_unwind(|| l2_distance_batch_vec(&long, &[1.0_f32; 3], 2)).is_err()
        );
        assert!(
            std::panic::catch_unwind(|| l2_distance_batch_into::<f32>(&[], &[], 0, &mut []))
                .is_err()
        );
        assert!(std::panic::catch_unwind(|| l2_distance_uint_scalar(&[1, 2], &[1])).is_err());
        assert!(std::panic::catch_unwind(|| l2_scalar::<u8, f32, 16>(&[1, 2], &[1])).is_err());
    }

    use crate::test_utils::{
        arbitrary_bf16, arbitrary_f16, arbitrary_f32, arbitrary_f64, arbitrary_vector_pair,
        dimension_shard, run_vector_pair_proptest,
    };

    fn l2_distance_batch_vec<T: L2>(from: &[T], to: &[T], dimension: usize) -> Vec<f32> {
        let mut distances = vec![0.0; to.len() / dimension];
        l2_distance_batch_into(from, to, dimension, &mut distances);
        distances
    }

    #[test]
    fn test_l2_f32_dispatch_matches_scalar() {
        // Covers tail handling for lengths around the 16-lane AVX-512 stride.
        for dim in [1usize, 7, 15, 16, 17, 31, 33, 64, 100, 1024] {
            let x: Vec<f32> = (0..dim).map(|i| (i as f32) * 0.5 - 3.0).collect();
            let y: Vec<f32> = (0..dim).map(|i| (i as f32) * -0.25 + 1.5).collect();
            assert_relative_eq!(l2_f32(&x, &y), l2(&x, &y), max_relative = 1e-5);
        }
    }

    /// PQ distance tables call this per sub-vector, so Haswell-baseline
    /// builds must reach the kernel without a runtime dispatch.
    #[cfg(target_arch = "x86_64")]
    #[test]
    fn test_l2_f32_batch_is_static_on_haswell() {
        use lance_core::utils::cpu::CpuFeatures;
        let haswell =
            CpuFeatures::from_target_features(&["avx2", "fma", "f16c", "bmi1", "bmi2", "popcnt"]);
        assert!(haswell.contains(L2_F32_BATCH.candidates()[0].features()));
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
        let distances = l2_distance_batch_vec(
            point.values(),
            mat.values().as_primitive::<Float32Type>().values(),
            8,
        );

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
        let distances = l2_distance_batch_vec(&point.values()[2..], &mat[6..], 8);

        assert_eq!(distances, vec![32.0, 8.0, 0.0, 8.0]);
    }

    #[test]
    fn test_odd_length_vector() {
        let mat = Float32Array::from_iter((0..5).map(|v| Some(v as f32)));
        let point = Float32Array::from((2..7).map(|v| Some(v as f32)).collect::<Vec<_>>());
        let distances = l2_distance_batch_vec(point.values(), mat.values(), 5);

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

        let d = l2_distance_batch_vec(q.values(), values.values(), 32);
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

    #[rstest::rstest]
    fn test_l2_distance_f32(
        #[values(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15)] shard: usize,
    ) {
        run_vector_pair_proptest(arbitrary_f32, dimension_shard(shard), |x, y| {
            do_l2_test(&x, &y)
        });
    }

    #[rstest::rstest]
    fn test_l2_distance_f64(
        #[values(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15)] shard: usize,
    ) {
        run_vector_pair_proptest(arbitrary_f64, dimension_shard(shard), |x, y| {
            do_l2_test(&x, &y)
        });
    }

    /// Every host-supported f32 kernel, single-pair and batch, must agree with
    /// an f64 reference. Prefixes of length 8 and 16 pin the batch kernels'
    /// PQ-width fast paths regardless of the generated dimension.
    #[rstest::rstest]
    fn test_l2_f32_kernels_match_reference(
        #[values(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15)] shard: usize,
    ) {
        run_vector_pair_proptest(arbitrary_f32, dimension_shard(shard), |x, y| {
            for dimension in [8, 16, x.len()].into_iter().filter(|&d| d <= x.len()) {
                let (x, y) = (&x[..dimension], &y[..dimension]);
                let x_f64 = x.iter().map(|&v| v as f64).collect::<Vec<_>>();
                let y_f64 = y.iter().map(|&v| v as f64).collect::<Vec<_>>();
                let want = l2_distance_reference(&x_f64, &y_f64) as f32;

                for candidate in L2_F32.candidates() {
                    let Some(kernel) = candidate.kernel() else {
                        continue;
                    };
                    let got = kernel.call(x, y);
                    prop_assert!(
                        approx::relative_eq!(got, want, max_relative = 1e-3),
                        "{} at dim {dimension}: {got} != {want}",
                        candidate.name()
                    );
                }

                // Where an x86 batch kernel reuses its tier's pair kernel or a
                // fast path for it, the two must match bit for bit: up to 16
                // dimensions for AVX+FMA (above that it uses the scalar
                // kernel), and at every dimension for AVX.
                #[cfg(target_arch = "x86_64")]
                for (pair, batch, max_dimension) in [
                    (x86::L2_F32_AVX_FMA, x86::L2_F32_BATCH_AVX_FMA, 16),
                    (x86::L2_F32_AVX, x86::L2_F32_BATCH_AVX, usize::MAX),
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
                    prop_assert_eq!(
                        got[0].to_bits(),
                        pair_kernel.call(x, y).to_bits(),
                        "{} at dim {}",
                        batch.name(),
                        dimension
                    );
                }

                // The second vector equals the key, so its distance is exactly 0.
                let batch = [y, x].concat();
                for candidate in L2_F32_BATCH.candidates() {
                    let Some(kernel) = candidate.kernel() else {
                        continue;
                    };
                    let mut got = [f32::NAN; 2];
                    kernel.call(x, &batch, dimension, &mut got);
                    prop_assert!(
                        approx::relative_eq!(got[0], want, max_relative = 1e-3) && got[1] == 0.0,
                        "{} at dim {dimension}: {got:?} != [{want}, 0]",
                        candidate.name()
                    );
                }
            }
            Ok(())
        });
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

    /// With dimension 1 and a zero query, the distance to target `t` is `t * t`.
    /// PQ uses 16 (4-bit) or 256 (8-bit) targets. The 258 targets of `first_of_equal`
    /// put 2 targets after the last full SIMD vector, so the tie spans both parts.
    #[rstest::rstest]
    #[case::last_of_16([vec![3.0; 15], vec![2.0]].concat(), Some(15))]
    #[case::first_of_equal([3.0, 1.0, -1.0].repeat(86), Some(1))]
    #[case::skips_nan([vec![f32::NAN; 255], vec![2.0]].concat(), Some(255))]
    #[case::all_nan(vec![f32::NAN; 16], None)]
    #[case::all_infinite(vec![f32::INFINITY; 256], None)]
    #[case::nan_and_infinite([f32::NAN, f32::INFINITY].repeat(8), None)]
    #[case::no_targets(vec![], None)]
    fn test_l2_targets_nearest(#[case] targets: Vec<f32>, #[case] expected: Option<u32>) {
        let prepared = L2Prepared::new(&targets, 1);
        assert_eq!(prepared.nearest(&[0.0]), expected);
    }

    /// `l2_batch_into` must agree with the per-vector `l2` for the f32 batch
    /// kernel and for the f64 and f16 loops that select their backend once
    /// per batch.
    #[rstest::rstest]
    #[case::dim_8(8)]
    #[case::dim_12(12)]
    #[case::dim_13(13)]
    #[case::dim_16(16)]
    #[case::dim_32(32)]
    #[case::dim_1024(1024)]
    fn test_l2_batch_into_matches_per_vector_l2(#[case] dimension: usize) {
        fn check<T: L2 + num_traits::FromPrimitive + Copy>(dimension: usize, bit_exact: bool) {
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
            T::l2_batch_into(&x, &batch, dimension, &mut got);

            for (g, y) in got.iter().zip(batch.chunks_exact(dimension)) {
                let w = T::l2(&x, y);
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
        // Above dimension 16 the f32 batch kernel is a different kernel from
        // the pair kernel, so only f32 is compared with a tolerance. The f64
        // and f16 batch loops run the pair kernel's arithmetic.
        check::<f32>(dimension, false);
        check::<f64>(dimension, true);
        check::<f16>(dimension, true);
    }

    /// Each x86 f64 batch loop must match its pair kernel bit for bit,
    /// including the tiers this host's `SIMD_SUPPORT` doesn't select.
    #[cfg(target_arch = "x86_64")]
    #[rstest::rstest]
    #[case::dim_8(8)]
    #[case::dim_13(13)]
    #[case::dim_1024(1024)]
    fn test_l2_batch_f64_tiers_match_pair_kernels(#[case] dimension: usize) {
        type BatchFn = unsafe fn(&[f64], &[f64], usize, &mut [f32]);
        type PairFn = unsafe fn(&[f64], &[f64]) -> f32;

        let num_vectors = 37;
        let x: Vec<f64> = (0..dimension).map(|i| (i as f64 * 0.37).sin()).collect();
        let batch: Vec<f64> = (0..dimension * num_vectors)
            .map(|i| (i as f64 * 0.73).cos() * 2.0)
            .collect();

        let tiers: [(&str, bool, BatchFn, PairFn); 3] = [
            (
                "avx512",
                std::is_x86_feature_detected!("avx512f"),
                x86::l2_batch_f64_avx512,
                x86::l2_f64_avx512,
            ),
            (
                "avx_fma",
                std::is_x86_feature_detected!("avx") && std::is_x86_feature_detected!("fma"),
                x86::l2_batch_f64_avx_fma,
                x86::l2_f64_avx_fma,
            ),
            (
                "avx",
                std::is_x86_feature_detected!("avx"),
                x86::l2_batch_f64_avx,
                x86::l2_f64_avx,
            ),
        ];
        for (name, is_supported, batch_fn, pair_fn) in tiers {
            if !is_supported {
                continue;
            }
            let mut got = vec![f32::NAN; num_vectors];
            // SAFETY: the host supports this tier's target features.
            unsafe { batch_fn(&x, &batch, dimension, &mut got) };
            for (g, y) in got.iter().zip(batch.chunks_exact(dimension)) {
                // SAFETY: as above.
                let want = unsafe { pair_fn(&x, y) };
                assert_eq!(
                    g.to_bits(),
                    want.to_bits(),
                    "{name} dim {dimension}: batch {g} != pair {want}"
                );
            }
        }
    }

    /// The f64 batch loops off x86 walk `batch` by pointer, sized by `output`,
    /// so the layout check must reject a wrong `output` length for every type.
    #[rstest::rstest]
    #[case::short(1)]
    #[case::long(3)]
    fn test_l2_batch_into_rejects_wrong_output_len(#[case] output_len: usize) {
        fn rejects<T: L2 + num_traits::FromPrimitive + Copy>(output_len: usize) {
            let x = [T::from_f32(1.0).unwrap(), T::from_f32(2.0).unwrap()];
            let batch = [T::from_f32(1.0).unwrap(); 4];
            let mut output = vec![0.0; output_len];
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                l2_distance_batch_into(&x, &batch, 2, &mut output)
            }));
            assert!(
                result.is_err(),
                "{} accepted output length {output_len}",
                std::any::type_name::<T>()
            );
        }
        rejects::<f16>(output_len);
        rejects::<f32>(output_len);
        rejects::<f64>(output_len);
    }

    /// An empty batch with a valid dimension, e.g. no centroids, writes
    /// nothing and does not panic. 8 and 16 take the fixed-length paths.
    #[rstest::rstest]
    #[case::dim_8(8)]
    #[case::dim_16(16)]
    #[case::dim_13(13)]
    fn test_l2_batch_into_accepts_empty_batch(#[case] dimension: usize) {
        fn accepts<T: L2 + num_traits::FromPrimitive + Copy>(dimension: usize) {
            let x = vec![T::from_f32(1.0).unwrap(); dimension];
            l2_distance_batch_into::<T>(&x, &[], dimension, &mut []);
        }
        accepts::<f16>(dimension);
        accepts::<f32>(dimension);
        accepts::<f64>(dimension);
    }
}
