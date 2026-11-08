// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::{iter::Sum, ops::AddAssign};

use arrow_array::{FixedSizeListArray, Float32Array};
use half::{bf16, f16};

use super::arrow::{Norm, Operation, dispatch};
#[allow(unused_imports)]
use lance_core::utils::cpu::{SIMD_SUPPORT, SimdSupport};
use num_traits::{AsPrimitive, Float, Num};

#[cfg(feature = "fp16kernels")]
use super::HalfBackend;
use super::{HALF_KERNELS_COMPILED, HalfType, half_backend, x86_half_features};

/// L2 normalization
pub trait Normalize: Num {
    /// L2 Normalization over a Vector.
    fn norm_l2(vector: &[Self]) -> f32;
}

#[cfg(feature = "fp16kernels")]
mod kernel {
    use super::*;

    // These are the `norm_l2_f16` function in f16.c. Our build.rs script compiles
    // a version of this file for each SIMD level with different suffixes.
    unsafe extern "C" {
        #[cfg(target_arch = "aarch64")]
        pub fn norm_l2_f16_neon(ptr: *const f16, len: u32) -> f32;
        #[cfg(all(kernel_support = "avx512_f16", target_arch = "x86_64"))]
        pub fn norm_l2_f16_avx512(ptr: *const f16, len: u32) -> f32;
        #[cfg(target_arch = "x86_64")]
        pub fn norm_l2_f16_avx2(ptr: *const f16, len: u32) -> f32;
        #[cfg(target_arch = "loongarch64")]
        pub fn norm_l2_f16_lsx(ptr: *const f16, len: u32) -> f32;
        #[cfg(target_arch = "loongarch64")]
        pub fn norm_l2_f16_lasx(ptr: *const f16, len: u32) -> f32;
    }
}

impl Normalize for u8 {
    #[inline]
    fn norm_l2(vector: &[Self]) -> f32 {
        norm_l2_impl::<Self, f32, 16>(vector)
    }
}

impl Normalize for f16 {
    #[inline]
    fn norm_l2(vector: &[Self]) -> f32 {
        match half_backend(
            *SIMD_SUPPORT,
            HalfType::F16,
            HALF_KERNELS_COMPILED,
            cfg!(all(kernel_support = "avx512_f16", target_arch = "x86_64")),
            x86_half_features(),
        ) {
            #[cfg(all(feature = "fp16kernels", target_arch = "aarch64"))]
            HalfBackend::Neon => unsafe {
                kernel::norm_l2_f16_neon(vector.as_ptr(), vector.len() as u32)
            },
            #[cfg(all(
                feature = "fp16kernels",
                kernel_support = "avx512_f16",
                target_arch = "x86_64"
            ))]
            HalfBackend::Avx512 => unsafe {
                kernel::norm_l2_f16_avx512(vector.as_ptr(), vector.len() as u32)
            },
            #[cfg(all(feature = "fp16kernels", target_arch = "x86_64"))]
            HalfBackend::Avx2 => unsafe {
                kernel::norm_l2_f16_avx2(vector.as_ptr(), vector.len() as u32)
            },
            #[cfg(all(feature = "fp16kernels", target_arch = "loongarch64"))]
            HalfBackend::Lasx => unsafe {
                kernel::norm_l2_f16_lasx(vector.as_ptr(), vector.len() as u32)
            },
            #[cfg(all(feature = "fp16kernels", target_arch = "loongarch64"))]
            HalfBackend::Lsx => unsafe {
                kernel::norm_l2_f16_lsx(vector.as_ptr(), vector.len() as u32)
            },
            // SimdSupport::AvxFma and SimdSupport::Avx retain their scalar
            // route; this fallback only extends the tiers the C kernel already
            // served to Avx512FP16 after checking F16C and FMA.
            _ => norm_l2_impl::<Self, f32, 32>(vector),
        }
    }
}

#[cfg(feature = "fp16kernels")]
mod bf16_kernel {
    use half::bf16;

    unsafe extern "C" {
        #[cfg(target_arch = "aarch64")]
        pub fn norm_l2_bf16_neon(ptr: *const bf16, len: u32) -> f32;
        #[cfg(all(kernel_support = "avx512_bf16", target_arch = "x86_64"))]
        pub fn norm_l2_bf16_avx512(ptr: *const bf16, len: u32) -> f32;
        #[cfg(target_arch = "x86_64")]
        pub fn norm_l2_bf16_avx2(ptr: *const bf16, len: u32) -> f32;
        #[cfg(target_arch = "loongarch64")]
        pub fn norm_l2_bf16_lsx(ptr: *const bf16, len: u32) -> f32;
        #[cfg(target_arch = "loongarch64")]
        pub fn norm_l2_bf16_lasx(ptr: *const bf16, len: u32) -> f32;
    }
}

impl Normalize for bf16 {
    #[inline]
    fn norm_l2(vector: &[Self]) -> f32 {
        match half_backend(
            *SIMD_SUPPORT,
            HalfType::Bf16,
            HALF_KERNELS_COMPILED,
            cfg!(all(kernel_support = "avx512_bf16", target_arch = "x86_64")),
            x86_half_features(),
        ) {
            #[cfg(all(feature = "fp16kernels", target_arch = "aarch64"))]
            HalfBackend::Neon => unsafe {
                bf16_kernel::norm_l2_bf16_neon(vector.as_ptr(), vector.len() as u32)
            },
            #[cfg(all(
                feature = "fp16kernels",
                kernel_support = "avx512_bf16",
                target_arch = "x86_64"
            ))]
            HalfBackend::Avx512 => unsafe {
                bf16_kernel::norm_l2_bf16_avx512(vector.as_ptr(), vector.len() as u32)
            },
            #[cfg(all(feature = "fp16kernels", target_arch = "x86_64"))]
            HalfBackend::Avx2 => unsafe {
                bf16_kernel::norm_l2_bf16_avx2(vector.as_ptr(), vector.len() as u32)
            },
            #[cfg(all(feature = "fp16kernels", target_arch = "loongarch64"))]
            HalfBackend::Lasx => unsafe {
                bf16_kernel::norm_l2_bf16_lasx(vector.as_ptr(), vector.len() as u32)
            },
            #[cfg(all(feature = "fp16kernels", target_arch = "loongarch64"))]
            HalfBackend::Lsx => unsafe {
                bf16_kernel::norm_l2_bf16_lsx(vector.as_ptr(), vector.len() as u32)
            },
            // SimdSupport::AvxFma and SimdSupport::Avx fall through here:
            // the bf16 C kernels are compiled with `-march=haswell` minimum
            // (AVX2), so they cannot run on AVX-only or AVX+FMA hosts.
            _ => norm_l2_impl::<Self, f32, 32>(vector),
        }
    }
}

impl Normalize for f32 {
    #[inline]
    fn norm_l2(vector: &[Self]) -> f32 {
        norm_l2_f32_dispatched(vector)
    }
}

/// L2 norm for f32, runtime-dispatched via `SIMD_SUPPORT` on x86_64
/// (AVX-512 / AVX2+FMA / AVX+FMA / AVX / scalar). Non-x86 uses the
/// auto-vectorised scalar loop.
#[inline]
fn norm_l2_f32_dispatched(vector: &[f32]) -> f32 {
    #[cfg(target_arch = "x86_64")]
    {
        match *SIMD_SUPPORT {
            SimdSupport::Avx512 | SimdSupport::Avx512FP16 => unsafe {
                x86::norm_l2_f32_avx512(vector)
            },
            SimdSupport::Avx2 | SimdSupport::AvxFma => unsafe { x86::norm_l2_f32_avx_fma(vector) },
            SimdSupport::Avx => unsafe { x86::norm_l2_f32_avx(vector) },
            _ => norm_l2_f32_scalar(vector),
        }
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        norm_l2_f32_scalar(vector)
    }
}

/// Portable scalar L2 norm for f32. Used as the x86_64 fallback when no
/// AVX2 is detected, and as the only path on non-x86 architectures. The
/// `LANES = 16` chunking matches the explicit-SIMD inner kernels above.
#[inline]
fn norm_l2_f32_scalar(vector: &[f32]) -> f32 {
    norm_l2_impl::<f32, f32, 16>(vector)
}

impl Normalize for f64 {
    #[inline]
    fn norm_l2(vector: &[Self]) -> f32 {
        norm_l2_f64_simd(vector)
    }
}

/// L2 norm for f64. Runtime-dispatched to the best available backend.
///
/// On x86_64, dispatches via `SIMD_SUPPORT` to a native AVX-512 inner kernel
/// (Skylake-X+, Ice Lake, Sapphire Rapids, Zen 4), an AVX2 + FMA kernel
/// (Haswell+), an AVX + FMA kernel (AMD Piledriver / Steamroller), an
/// AVX-only kernel (Intel Sandy Bridge / Ivy Bridge), or a portable scalar
/// fallback. The per-tier inner functions each carry their own
/// `#[target_feature]` so they stay correct under any compile baseline.
/// On aarch64 and loongarch64, the SIMD primitives in `crate::simd::f64`
/// are unconditionally backed by NEON / LSX-LASX respectively, so no
/// runtime gate is required.
#[inline]
pub fn norm_l2_f64_simd(vector: &[f64]) -> f32 {
    #[cfg(target_arch = "x86_64")]
    {
        match *SIMD_SUPPORT {
            SimdSupport::Avx512 | SimdSupport::Avx512FP16 => unsafe {
                x86::norm_l2_f64_avx512(vector)
            },
            SimdSupport::Avx2 | SimdSupport::AvxFma => unsafe { x86::norm_l2_f64_avx_fma(vector) },
            SimdSupport::Avx => unsafe { x86::norm_l2_f64_avx(vector) },
            _ => norm_l2_f64_scalar(vector),
        }
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        norm_l2_f64_simd_other(vector)
    }
}

/// Portable scalar L2 norm. Used as the x86_64 fallback when no AVX2 is
/// detected, and exposed for cross-backend parity testing.
#[cfg(target_arch = "x86_64")]
#[inline]
fn norm_l2_f64_scalar(vector: &[f64]) -> f32 {
    vector.iter().map(|v| v * v).sum::<f64>().sqrt() as f32
}

#[cfg(target_arch = "x86_64")]
mod x86 {
    use std::arch::x86_64::*;

    use crate::simd::f64::{f64x4, f64x8};
    use crate::simd::x86::hsum256_ps;
    use crate::simd::{FloatSimd, SIMD};

    /// AVX-512 path for f64: 8-wide `__m512d` with `vfmadd231pd` per iteration.
    #[target_feature(enable = "avx512f")]
    pub unsafe fn norm_l2_f64_avx512(vector: &[f64]) -> f32 {
        let dim = vector.len();
        let unrolled_len = dim / 8 * 8;

        let mut acc = _mm512_setzero_pd();
        for i in (0..unrolled_len).step_by(8) {
            let v = _mm512_loadu_pd(vector.as_ptr().add(i));
            acc = _mm512_fmadd_pd(v, v, acc);
        }

        let tail: f64 = vector[unrolled_len..].iter().map(|&v| v * v).sum();
        (_mm512_reduce_add_pd(acc) + tail).sqrt() as f32
    }

    /// AVX + FMA path for f64. Covers both AvxFma and AVX2 dispatch (body uses no AVX2-specific intrinsics).
    #[target_feature(enable = "avx,fma")]
    pub unsafe fn norm_l2_f64_avx_fma(vector: &[f64]) -> f32 {
        let dim = vector.len();
        let unrolled_len = dim / 8 * 8;

        let mut acc8 = f64x8::zeros();
        for i in (0..unrolled_len).step_by(8) {
            let v = f64x8::load_unaligned(vector.as_ptr().add(i));
            acc8.multiply_add(v, v);
        }

        let aligned_len = dim / 4 * 4;
        let mut acc4 = f64x4::zeros();
        for i in (unrolled_len..aligned_len).step_by(4) {
            let v = f64x4::load_unaligned(vector.as_ptr().add(i));
            acc4.multiply_add(v, v);
        }

        let tail: f64 = vector[aligned_len..].iter().map(|&v| v * v).sum();
        (acc8.reduce_sum() + acc4.reduce_sum() + tail).sqrt() as f32
    }

    /// AVX-only path for f64 (no FMA): `_mm256_mul_pd` + `_mm256_add_pd` per iteration for Sandy/Ivy Bridge.
    #[target_feature(enable = "avx")]
    pub unsafe fn norm_l2_f64_avx(vector: &[f64]) -> f32 {
        let dim = vector.len();
        let unrolled_len = dim / 4 * 4;

        let mut acc = _mm256_setzero_pd();
        for i in (0..unrolled_len).step_by(4) {
            let v = _mm256_loadu_pd(vector.as_ptr().add(i));
            acc = _mm256_add_pd(acc, _mm256_mul_pd(v, v));
        }

        // Horizontal sum of __m256d -> f64. Two pairwise adds across lanes.
        let lo = _mm256_castpd256_pd128(acc);
        let hi = _mm256_extractf128_pd(acc, 1);
        let sum128 = _mm_add_pd(lo, hi);
        let sum64 = _mm_add_pd(sum128, _mm_unpackhi_pd(sum128, sum128));
        let acc_sum = _mm_cvtsd_f64(sum64);

        let tail: f64 = vector[unrolled_len..].iter().map(|&v| v * v).sum();
        (acc_sum + tail).sqrt() as f32
    }

    /// AVX-512 path for f32: 16-wide `__m512` with `vfmadd231ps` per iteration.
    #[target_feature(enable = "avx512f")]
    pub unsafe fn norm_l2_f32_avx512(vector: &[f32]) -> f32 {
        let dim = vector.len();
        let unrolled_len = dim / 16 * 16;

        let mut acc = _mm512_setzero_ps();
        for i in (0..unrolled_len).step_by(16) {
            let v = _mm512_loadu_ps(vector.as_ptr().add(i));
            acc = _mm512_fmadd_ps(v, v, acc);
        }

        let tail: f32 = vector[unrolled_len..].iter().map(|&v| v * v).sum();
        (_mm512_reduce_add_ps(acc) + tail).sqrt()
    }

    /// AVX + FMA path for f32. Covers both AvxFma and AVX2 dispatch (body uses no AVX2-specific intrinsics).
    #[target_feature(enable = "avx,fma")]
    pub unsafe fn norm_l2_f32_avx_fma(vector: &[f32]) -> f32 {
        let dim = vector.len();
        let unrolled_len = dim / 8 * 8;

        let mut acc = _mm256_setzero_ps();
        for i in (0..unrolled_len).step_by(8) {
            let v = _mm256_loadu_ps(vector.as_ptr().add(i));
            acc = _mm256_fmadd_ps(v, v, acc);
        }

        let tail: f32 = vector[unrolled_len..].iter().map(|&v| v * v).sum();
        (hsum256_ps(acc) + tail).sqrt()
    }

    /// AVX-only path for f32 (no FMA): `_mm256_mul_ps` + `_mm256_add_ps` per iteration for Sandy/Ivy Bridge.
    #[target_feature(enable = "avx")]
    pub unsafe fn norm_l2_f32_avx(vector: &[f32]) -> f32 {
        let dim = vector.len();
        let unrolled_len = dim / 8 * 8;

        let mut acc = _mm256_setzero_ps();
        for i in (0..unrolled_len).step_by(8) {
            let v = _mm256_loadu_ps(vector.as_ptr().add(i));
            acc = _mm256_add_ps(acc, _mm256_mul_ps(v, v));
        }

        let tail: f32 = vector[unrolled_len..].iter().map(|&v| v * v).sum();
        (hsum256_ps(acc) + tail).sqrt()
    }
}

#[cfg(not(target_arch = "x86_64"))]
#[inline]
fn norm_l2_f64_simd_other(vector: &[f64]) -> f32 {
    use crate::simd::f64::{f64x4, f64x8};
    use crate::simd::{FloatSimd, SIMD};

    let dim = vector.len();
    let unrolled_len = dim / 8 * 8;

    let mut acc8 = f64x8::zeros();
    for i in (0..unrolled_len).step_by(8) {
        unsafe {
            let v = f64x8::load_unaligned(vector.as_ptr().add(i));
            acc8.multiply_add(v, v);
        }
    }

    let aligned_len = dim / 4 * 4;
    let mut acc4 = f64x4::zeros();
    for i in (unrolled_len..aligned_len).step_by(4) {
        unsafe {
            let v = f64x4::load_unaligned(vector.as_ptr().add(i));
            acc4.multiply_add(v, v);
        }
    }

    let tail: f64 = vector[aligned_len..].iter().map(|&v| v * v).sum();
    (acc8.reduce_sum() + acc4.reduce_sum() + tail).sqrt() as f32
}

/// NOTE: this is only pub for benchmarking purposes
#[inline]
pub fn norm_l2_impl<
    T: AsPrimitive<Output>,
    Output: Float + Sum + 'static + AddAssign,
    const LANES: usize,
>(
    vector: &[T],
) -> Output {
    let (chunks, remainder) = vector.as_chunks::<LANES>();
    let sum = if remainder.is_empty() {
        Output::zero()
    } else {
        remainder.iter().map(|&v| v.as_().powi(2)).sum::<Output>()
    };
    let mut sums = [Output::zero(); LANES];
    for chunk in chunks {
        for i in 0..LANES {
            sums[i] += chunk[i].as_().powi(2);
        }
    }
    (sum + sums.iter().copied().sum::<Output>()).sqrt()
}

/// Normalize a vector.
///
/// The parameters must be cache line aligned. For example, from
/// Arrow Arrays, i.e., Float32Array
#[inline]
pub fn norm_l2<T: Normalize>(vector: &[T]) -> f32 {
    T::norm_l2(vector)
}

/// L2 norm of every vector in a [FixedSizeListArray], Returns one norm per row.
pub fn norm_l2_fsl(fsl: &FixedSizeListArray) -> crate::Result<Float32Array> {
    dispatch(Operation::Norm(Norm::L2), None, fsl).map(Float32Array::from)
}

/// Squared L2 norm of every vector in a [`FixedSizeListArray`].
///
/// Supports Float16/Float32/Float64 and rejects zero dimensions. Each square is
/// accumulated in `f32` (or wider) rather than the element type, avoiding f16
/// overflow and underflow. Like [`norm_l2_fsl`], reads the values of null rows.
///
/// ```
/// use arrow_array::{FixedSizeListArray, types::Float32Type};
/// use lance_linalg::distance::try_norm_squared_fsl;
/// let vectors = FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
///     [Some(vec![Some(3.0), Some(4.0)])], 2);
/// assert_eq!(try_norm_squared_fsl(&vectors)?, vec![25.0]);
/// # Ok::<(), arrow_schema::ArrowError>(())
/// ```
pub fn try_norm_squared_fsl(fsl: &FixedSizeListArray) -> crate::Result<Vec<f32>> {
    dispatch(Operation::Norm(Norm::Squared), None, fsl)
}

/// Squared L2 norms through the legacy infallible API.
///
/// # Panics
/// Panics for unsupported element types or zero dimensions. Use
/// [`try_norm_squared_fsl`] to receive an error instead.
#[deprecated(note = "Use try_norm_squared_fsl")]
pub fn norm_squared_fsl(fsl: &FixedSizeListArray) -> Vec<f32> {
    // Retain the public signature while callers migrate to the fallible API.
    try_norm_squared_fsl(fsl)
        .expect("squared L2 norms require float vectors with positive dimension")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::{
        arbitrary_bf16, arbitrary_f16, arbitrary_f32, arbitrary_f64, dimension_shard,
        run_vector_proptest,
    };
    use arrow_array::{Float16Array, Float64Array, UInt8Array};
    use arrow_schema::{ArrowError, DataType};
    use lance_arrow::FixedSizeListArrayExt;
    use num_traits::ToPrimitive;
    use proptest::prelude::*;
    use std::sync::Arc;

    /// Reference implementation of L2 norm.
    fn norm_l2_reference(data: &[f64]) -> f32 {
        data.iter().map(|v| *v * *v).sum::<f64>().sqrt() as f32
    }

    fn do_norm_l2_test<T: Normalize + ToPrimitive>(
        data: &[T],
    ) -> std::result::Result<(), TestCaseError> {
        let f64_data = data
            .iter()
            .map(|v| v.to_f64().unwrap())
            .collect::<Vec<f64>>();

        let result = norm_l2(data);
        let reference = norm_l2_reference(&f64_data);

        prop_assert!(approx::relative_eq!(result, reference, max_relative = 1e-6));
        Ok(())
    }

    #[rstest::rstest]
    fn test_l2_norm_f32(
        #[values(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15)] shard: usize,
    ) {
        run_vector_proptest(arbitrary_f32, dimension_shard(shard), |data| {
            do_norm_l2_test(&data)
        });
    }

    #[rstest::rstest]
    fn test_l2_norm_f64(
        #[values(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15)] shard: usize,
    ) {
        run_vector_proptest(arbitrary_f64, dimension_shard(shard), |data| {
            do_norm_l2_test(&data)
        });
    }

    #[rstest::rstest]
    fn test_l2_norm_f32_scalar_simd_parity(
        #[values(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15)] shard: usize,
    ) {
        run_vector_proptest(arbitrary_f32, dimension_shard(shard), |data| {
            let scalar = data.iter().map(|&v| (v as f64).powi(2)).sum::<f64>().sqrt() as f32;
            let simd = <f32 as Normalize>::norm_l2(&data);
            prop_assert!(approx::relative_eq!(scalar, simd, max_relative = 1e-3));
            Ok(())
        });
    }

    proptest::proptest! {
        #[test]
        fn test_l2_norm_f16(data in prop::collection::vec(arbitrary_f16(), 4..4048)) {
            do_norm_l2_test(&data)?;
        }

        #[test]
        fn test_l2_norm_bf16(data in prop::collection::vec(arbitrary_bf16(), 4..4048)){
            do_norm_l2_test(&data)?;
        }

        /// Cross-backend parity: scalar fallback must match the dispatched
        /// SIMD path within numerical tolerance. Exercises `norm_l2_f64_scalar`
        /// directly so the runtime fallback is exercised even on AVX2-capable
        /// CI hosts.
        #[cfg(target_arch = "x86_64")]
        #[test]
        fn test_l2_norm_f64_scalar_simd_parity(
            data in prop::collection::vec(arbitrary_f64(), 4..4048)
        ) {
            let scalar = norm_l2_f64_scalar(&data);
            let simd = norm_l2_f64_simd(&data);
            prop_assert!(approx::relative_eq!(scalar, simd, max_relative = 1e-6));
        }

        /// AVX-512-direct parity: explicitly compares the scalar fallback
        /// against the native AVX-512 inner kernel on AVX-512F-capable hosts
        /// (Skylake-X+, Ice Lake, Sapphire Rapids, Zen 4). Early-returns on
        /// hosts without AVX-512F so the test stays portable; CI runners with
        /// AVX-512F exercise the `_mm512_*` path.
        #[cfg(target_arch = "x86_64")]
        #[test]
        fn test_l2_norm_f64_scalar_vs_avx512_parity(
            data in prop::collection::vec(arbitrary_f64(), 4..4048)
        ) {
            if !std::is_x86_feature_detected!("avx512f") {
                return Ok(());
            }
            let scalar = norm_l2_f64_scalar(&data);
            let avx512 = unsafe { x86::norm_l2_f64_avx512(&data) };
            prop_assert!(approx::relative_eq!(scalar, avx512, max_relative = 1e-6));
        }

        /// AVX + FMA-direct parity for the f64 L2-norm kernel. Covers the
        /// AMD Piledriver / Steamroller / FX-7500 tier. Early-returns on
        /// hosts without both AVX and FMA.
        #[cfg(target_arch = "x86_64")]
        #[test]
        fn test_l2_norm_f64_scalar_vs_avx_fma_parity(
            data in prop::collection::vec(arbitrary_f64(), 4..4048)
        ) {
            if !(std::is_x86_feature_detected!("avx") && std::is_x86_feature_detected!("fma")) {
                return Ok(());
            }
            let scalar = norm_l2_f64_scalar(&data);
            let avx_fma = unsafe { x86::norm_l2_f64_avx_fma(&data) };
            prop_assert!(approx::relative_eq!(scalar, avx_fma, max_relative = 1e-6));
        }

        /// AVX-only-direct parity for the f64 L2-norm kernel. Covers the
        /// Intel Sandy Bridge / Ivy Bridge tier (AVX without FMA).
        /// Early-returns on hosts without AVX.
        #[cfg(target_arch = "x86_64")]
        #[test]
        fn test_l2_norm_f64_scalar_vs_avx_parity(
            data in prop::collection::vec(arbitrary_f64(), 4..4048)
        ) {
            if !std::is_x86_feature_detected!("avx") {
                return Ok(());
            }
            let scalar = norm_l2_f64_scalar(&data);
            let avx = unsafe { x86::norm_l2_f64_avx(&data) };
            prop_assert!(approx::relative_eq!(scalar, avx, max_relative = 1e-6));
        }

        /// AVX-512-direct parity for the f32 L2-norm kernel. Explicitly
        /// compares the scalar fallback against the native AVX-512 inner
        /// kernel on AVX-512F-capable hosts. Early-returns on hosts without
        /// AVX-512F; CI runners with AVX-512F exercise the `_mm512_*` path.
        #[cfg(target_arch = "x86_64")]
        #[test]
        fn test_norm_l2_f32_scalar_vs_avx512_parity(
            data in prop::collection::vec(arbitrary_f32(), 4..4048)
        ) {
            if !std::is_x86_feature_detected!("avx512f") {
                return Ok(());
            }
            let scalar = norm_l2_f32_scalar(&data);
            let avx512 = unsafe { x86::norm_l2_f32_avx512(&data) };
            prop_assert!(approx::relative_eq!(scalar, avx512, max_relative = 1e-3));
        }

        /// AVX + FMA-direct parity for the f32 L2-norm kernel. Covers the
        /// AMD Piledriver / Steamroller / FX-7500 tier. Early-returns on
        /// hosts without both AVX and FMA.
        #[cfg(target_arch = "x86_64")]
        #[test]
        fn test_norm_l2_f32_scalar_vs_avx_fma_parity(
            data in prop::collection::vec(arbitrary_f32(), 4..4048)
        ) {
            if !(std::is_x86_feature_detected!("avx") && std::is_x86_feature_detected!("fma")) {
                return Ok(());
            }
            let scalar = norm_l2_f32_scalar(&data);
            let avx_fma = unsafe { x86::norm_l2_f32_avx_fma(&data) };
            prop_assert!(approx::relative_eq!(scalar, avx_fma, max_relative = 1e-3));
        }

        /// AVX-only-direct parity for the f32 L2-norm kernel. Covers the
        /// Intel Sandy Bridge / Ivy Bridge tier (AVX without FMA).
        /// Early-returns on hosts without AVX.
        #[cfg(target_arch = "x86_64")]
        #[test]
        fn test_norm_l2_f32_scalar_vs_avx_parity(
            data in prop::collection::vec(arbitrary_f32(), 4..4048)
        ) {
            if !std::is_x86_feature_detected!("avx") {
                return Ok(());
            }
            let scalar = norm_l2_f32_scalar(&data);
            let avx = unsafe { x86::norm_l2_f32_avx(&data) };
            prop_assert!(approx::relative_eq!(scalar, avx, max_relative = 1e-3));
        }
    }

    #[test]
    fn test_norm_l2_fsl_f32() {
        let values = Float32Array::from(vec![3.0, 4.0, 6.0, 8.0]);
        let fsl = FixedSizeListArray::try_new_from_values(values, 2).unwrap();

        let norms = norm_l2_fsl(&fsl).unwrap();
        assert_eq!(norms.len(), 2);
        assert!(approx::relative_eq!(
            norms.value(0),
            5.0,
            max_relative = 1e-6
        ));
        assert!(approx::relative_eq!(
            norms.value(1),
            10.0,
            max_relative = 1e-6
        ));
    }

    #[test]
    fn test_norm_l2_fsl_f16_and_f64_match_norm_l2() {
        // The FSL helper must agree with the per-vector `norm_l2` kernel for
        // every supported value type, since callers cache these norms and feed
        // them to `cosine_with_norms`.
        let f16_vals: Vec<f16> = [3.0f32, 4.0, 6.0, 8.0]
            .iter()
            .map(|&v| f16::from_f32(v))
            .collect();
        let f16_fsl =
            FixedSizeListArray::try_new_from_values(Float16Array::from(f16_vals), 2).unwrap();
        let f16_norms = norm_l2_fsl(&f16_fsl).unwrap();
        assert_eq!(
            f16_norms.value(0),
            norm_l2(&[f16::from_f32(3.0), f16::from_f32(4.0)])
        );
        assert_eq!(
            f16_norms.value(1),
            norm_l2(&[f16::from_f32(6.0), f16::from_f32(8.0)])
        );

        let f64_fsl = FixedSizeListArray::try_new_from_values(
            Float64Array::from(vec![3.0, 4.0, 6.0, 8.0]),
            2,
        )
        .unwrap();
        let f64_norms = norm_l2_fsl(&f64_fsl).unwrap();
        assert_eq!(f64_norms.value(0), norm_l2(&[3.0f64, 4.0]));
        assert_eq!(f64_norms.value(1), norm_l2(&[6.0f64, 8.0]));
    }

    #[test]
    fn test_norm_l2_fsl_rejects_unsupported_value_type() {
        let fsl = FixedSizeListArray::try_new_from_values(UInt8Array::from(vec![1u8, 2, 3, 4]), 2)
            .unwrap();
        let err = norm_l2_fsl(&fsl).unwrap_err().to_string();
        assert!(
            err.contains("unsupported vector element type UInt8"),
            "got: {err}"
        );
    }

    /// `norm_squared_fsl` must accumulate in a type wider than the element type.
    /// `f16 * f16` rounds each square back to `f16`, which saturates to `inf` at
    /// `|x| >= 256` and to zero at `|x| <= 1.726e-4`.
    #[test]
    fn test_norm_squared_fsl_f16_accumulates_wide() {
        use arrow_array::Float16Array;
        use arrow_schema::Field;
        use std::sync::Arc;

        // dim 2: row 0 overflows at the square, row 1 underflows at the square.
        let raw = [256.0f32, 0.0, 1e-4, 1e-4];
        let values = Float16Array::from_iter_values(raw.map(f16::from_f32));
        let field = Arc::new(Field::new("item", DataType::Float16, true));
        let fsl = FixedSizeListArray::try_new(field, 2, Arc::new(values), None).unwrap();

        let got = try_norm_squared_fsl(&fsl).unwrap();
        // Independent reference: square and sum the same f16 inputs in f64.
        let expected = raw
            .chunks(2)
            .map(|c| {
                c.iter()
                    .map(|&x| {
                        let x = f16::from_f32(x).to_f64();
                        x * x
                    })
                    .sum::<f64>()
            })
            .collect::<Vec<_>>();

        for (row, (&got, &want)) in got.iter().zip(expected.iter()).enumerate() {
            assert!(
                approx::relative_eq!(got as f64, want, max_relative = 1e-3),
                "row {row}: got {got}, want {want}"
            );
        }
    }
    #[rstest::rstest]
    #[case::f16(DataType::Float16)]
    #[case::f32(DataType::Float32)]
    #[case::f64(DataType::Float64)]
    #[case::i8(DataType::Int8)]
    #[case::u8(DataType::UInt8)]
    #[case::i32(DataType::Int32)]
    fn test_arrow_norm_support_and_dimension(#[case] dtype: DataType) {
        let values = super::super::tests::arrow_values(&dtype, vec![99, 99, 3, 4, 5, 12]);
        let vectors = FixedSizeListArray::try_new_from_values(values, 2)
            .unwrap()
            .slice(1, 2);
        let supported = matches!(
            dtype,
            DataType::Float16 | DataType::Float32 | DataType::Float64
        );
        let norms = norm_l2_fsl(&vectors);
        let squared = try_norm_squared_fsl(&vectors);
        if supported {
            assert_eq!(norms.unwrap().values(), &[5.0, 13.0]);
            assert_eq!(squared.unwrap(), vec![25.0, 169.0]);
        } else {
            for error in [norms.unwrap_err(), squared.unwrap_err()] {
                assert!(
                    matches!(&error, ArrowError::InvalidArgumentError(message)
                    if message.contains("unsupported vector element type") && message.contains(&dtype.to_string())),
                    "{error}"
                );
            }
        }
        let zero_dim = FixedSizeListArray::try_new_with_length(
            Arc::new(arrow_schema::Field::new("item", dtype.clone(), true)),
            0,
            super::super::tests::arrow_values(&dtype, vec![]),
            None,
            1,
        )
        .unwrap();
        for error in [
            norm_l2_fsl(&zero_dim).unwrap_err(),
            try_norm_squared_fsl(&zero_dim).unwrap_err(),
        ] {
            assert!(
                matches!(&error, ArrowError::InvalidArgumentError(message)
                if message.contains(if supported {"dimension 0"} else {"unsupported vector element type"})),
                "{error}"
            );
        }
    }
}
