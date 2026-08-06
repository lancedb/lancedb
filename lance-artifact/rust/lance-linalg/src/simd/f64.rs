// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! `f64x4` and `f64x8` SIMD types for f64 distance computations.

use std::fmt::Formatter;

#[cfg(target_arch = "aarch64")]
use std::arch::aarch64::*;
#[cfg(target_arch = "loongarch64")]
use std::arch::loongarch64::*;
#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::*;
#[cfg(target_arch = "loongarch64")]
use std::mem::transmute;
use std::ops::{Add, AddAssign, Mul, Sub, SubAssign};

use super::{FloatSimd, SIMD};

/// 4 of 64-bit `f64` values. Uses 256-bit SIMD if possible.
#[allow(non_camel_case_types)]
#[cfg(target_arch = "x86_64")]
#[derive(Clone, Copy)]
pub struct f64x4(std::arch::x86_64::__m256d);

#[allow(non_camel_case_types)]
#[cfg(target_arch = "aarch64")]
#[derive(Clone, Copy)]
pub struct f64x4(float64x2x2_t);

#[allow(non_camel_case_types)]
#[cfg(target_arch = "loongarch64")]
#[derive(Clone, Copy)]
pub struct f64x4(v4f64);

impl std::fmt::Debug for f64x4 {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut arr = [0.0_f64; 4];
        unsafe {
            self.store_unaligned(arr.as_mut_ptr());
        }
        write!(f, "f64x4({:?})", arr)
    }
}

impl From<&[f64]> for f64x4 {
    fn from(value: &[f64]) -> Self {
        unsafe { Self::load_unaligned(value.as_ptr()) }
    }
}

impl<'a> From<&'a [f64; 4]> for f64x4 {
    fn from(value: &'a [f64; 4]) -> Self {
        unsafe { Self::load_unaligned(value.as_ptr()) }
    }
}

impl SIMD<f64, 4> for f64x4 {
    fn splat(val: f64) -> Self {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            Self(_mm256_set1_pd(val))
        }
        #[cfg(target_arch = "aarch64")]
        unsafe {
            Self(float64x2x2_t(vdupq_n_f64(val), vdupq_n_f64(val)))
        }
        #[cfg(target_arch = "loongarch64")]
        unsafe {
            Self(transmute(lasx_xvreplgr2vr_d(transmute(val))))
        }
    }

    fn zeros() -> Self {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            Self(_mm256_setzero_pd())
        }
        #[cfg(target_arch = "aarch64")]
        {
            Self::splat(0.0)
        }
        #[cfg(target_arch = "loongarch64")]
        {
            Self::splat(0.0)
        }
    }

    #[inline]
    unsafe fn load(ptr: *const f64) -> Self {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            Self(_mm256_load_pd(ptr))
        }
        #[cfg(target_arch = "aarch64")]
        {
            Self::load_unaligned(ptr)
        }
        #[cfg(target_arch = "loongarch64")]
        {
            Self(transmute(lasx_xvld::<0>(transmute(ptr))))
        }
    }

    #[inline]
    unsafe fn load_unaligned(ptr: *const f64) -> Self {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            Self(_mm256_loadu_pd(ptr))
        }
        #[cfg(target_arch = "aarch64")]
        {
            Self(vld1q_f64_x2(ptr))
        }
        #[cfg(target_arch = "loongarch64")]
        {
            Self(transmute(lasx_xvld::<0>(transmute(ptr))))
        }
    }

    unsafe fn store(&self, ptr: *mut f64) {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            _mm256_store_pd(ptr, self.0);
        }
        #[cfg(target_arch = "aarch64")]
        unsafe {
            vst1q_f64_x2(ptr, self.0);
        }
        #[cfg(target_arch = "loongarch64")]
        unsafe {
            lasx_xvst::<0>(transmute(self.0), transmute(ptr));
        }
    }

    unsafe fn store_unaligned(&self, ptr: *mut f64) {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            _mm256_storeu_pd(ptr, self.0);
        }
        #[cfg(target_arch = "aarch64")]
        unsafe {
            vst1q_f64_x2(ptr, self.0);
        }
        #[cfg(target_arch = "loongarch64")]
        unsafe {
            lasx_xvst::<0>(transmute(self.0), transmute(ptr));
        }
    }

    #[inline]
    fn reduce_sum(&self) -> f64 {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            // [a, b, c, d] -> hadd -> [a+b, a+b, c+d, c+d]
            let sum = _mm256_hadd_pd(self.0, self.0);
            // Extract low 128 and high 128, add them
            let lo = _mm256_castpd256_pd128(sum);
            let hi = _mm256_extractf128_pd(sum, 1);
            let r = _mm_add_pd(lo, hi);
            _mm_cvtsd_f64(r)
        }
        #[cfg(target_arch = "aarch64")]
        unsafe {
            let sum = vaddq_f64(self.0.0, self.0.1);
            vaddvq_f64(sum)
        }
        #[cfg(target_arch = "loongarch64")]
        {
            self.as_array().iter().sum()
        }
    }

    fn reduce_min(&self) -> f64 {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            // Swap high/low 128-bit lanes and min
            let hi = _mm256_permute2f128_pd(self.0, self.0, 1);
            let m = _mm256_min_pd(self.0, hi);
            // Swap within 128-bit lane and min
            let shuf = _mm256_permute_pd(m, 0b0101);
            let m = _mm256_min_pd(m, shuf);
            _mm256_cvtsd_f64(m)
        }
        #[cfg(target_arch = "aarch64")]
        unsafe {
            let m = vminq_f64(self.0.0, self.0.1);
            vminvq_f64(m)
        }
        #[cfg(target_arch = "loongarch64")]
        {
            self.as_array()
                .iter()
                .copied()
                .fold(f64::INFINITY, f64::min)
        }
    }

    fn min(&self, rhs: &Self) -> Self {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            Self(_mm256_min_pd(self.0, rhs.0))
        }
        #[cfg(target_arch = "aarch64")]
        unsafe {
            Self(float64x2x2_t(
                vminq_f64(self.0.0, rhs.0.0),
                vminq_f64(self.0.1, rhs.0.1),
            ))
        }
        #[cfg(target_arch = "loongarch64")]
        unsafe {
            Self(lasx_xvfmin_d(self.0, rhs.0))
        }
    }

    fn find(&self, val: f64) -> Option<i32> {
        unsafe {
            for i in 0..4 {
                if self.as_array().get_unchecked(i) == &val {
                    return Some(i as i32);
                }
            }
        }
        None
    }
}

impl FloatSimd<f64, 4> for f64x4 {
    fn multiply_add(&mut self, a: Self, b: Self) {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            self.0 = _mm256_fmadd_pd(a.0, b.0, self.0);
        }
        #[cfg(target_arch = "aarch64")]
        unsafe {
            self.0.0 = vfmaq_f64(self.0.0, a.0.0, b.0.0);
            self.0.1 = vfmaq_f64(self.0.1, a.0.1, b.0.1);
        }
        #[cfg(target_arch = "loongarch64")]
        unsafe {
            self.0 = lasx_xvfmadd_d(a.0, b.0, self.0);
        }
    }
}

impl Add for f64x4 {
    type Output = Self;

    #[inline]
    fn add(self, rhs: Self) -> Self::Output {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            Self(_mm256_add_pd(self.0, rhs.0))
        }
        #[cfg(target_arch = "aarch64")]
        unsafe {
            Self(float64x2x2_t(
                vaddq_f64(self.0.0, rhs.0.0),
                vaddq_f64(self.0.1, rhs.0.1),
            ))
        }
        #[cfg(target_arch = "loongarch64")]
        unsafe {
            Self(lasx_xvfadd_d(self.0, rhs.0))
        }
    }
}

impl AddAssign for f64x4 {
    #[inline]
    fn add_assign(&mut self, rhs: Self) {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            self.0 = _mm256_add_pd(self.0, rhs.0)
        }
        #[cfg(target_arch = "aarch64")]
        unsafe {
            self.0.0 = vaddq_f64(self.0.0, rhs.0.0);
            self.0.1 = vaddq_f64(self.0.1, rhs.0.1);
        }
        #[cfg(target_arch = "loongarch64")]
        unsafe {
            self.0 = lasx_xvfadd_d(self.0, rhs.0);
        }
    }
}

impl Sub for f64x4 {
    type Output = Self;

    #[inline]
    fn sub(self, rhs: Self) -> Self::Output {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            Self(_mm256_sub_pd(self.0, rhs.0))
        }
        #[cfg(target_arch = "aarch64")]
        unsafe {
            Self(float64x2x2_t(
                vsubq_f64(self.0.0, rhs.0.0),
                vsubq_f64(self.0.1, rhs.0.1),
            ))
        }
        #[cfg(target_arch = "loongarch64")]
        unsafe {
            Self(lasx_xvfsub_d(self.0, rhs.0))
        }
    }
}

impl SubAssign for f64x4 {
    #[inline]
    fn sub_assign(&mut self, rhs: Self) {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            self.0 = _mm256_sub_pd(self.0, rhs.0)
        }
        #[cfg(target_arch = "aarch64")]
        unsafe {
            self.0.0 = vsubq_f64(self.0.0, rhs.0.0);
            self.0.1 = vsubq_f64(self.0.1, rhs.0.1);
        }
        #[cfg(target_arch = "loongarch64")]
        unsafe {
            self.0 = lasx_xvfsub_d(self.0, rhs.0);
        }
    }
}

impl Mul for f64x4 {
    type Output = Self;

    #[inline]
    fn mul(self, rhs: Self) -> Self::Output {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            Self(_mm256_mul_pd(self.0, rhs.0))
        }
        #[cfg(target_arch = "aarch64")]
        unsafe {
            Self(float64x2x2_t(
                vmulq_f64(self.0.0, rhs.0.0),
                vmulq_f64(self.0.1, rhs.0.1),
            ))
        }
        #[cfg(target_arch = "loongarch64")]
        unsafe {
            Self(lasx_xvfmul_d(self.0, rhs.0))
        }
    }
}

// ---------------------------------------------------------------------------
// f64x8: 8 × f64 values (512-bit SIMD or 2 × 256-bit)
// ---------------------------------------------------------------------------

/// 8 of 64-bit `f64` values. Stored as a pair of 256-bit AVX vectors on
/// x86_64. Originally there was a sibling AVX-512 variant gated on
/// `target_feature = "avx512f"`, but no project CI configuration enables
/// `+avx512f` globally, so the variant was dead code. Removed in the
/// runtime-SIMD-dispatch retrofit; per-tier dispatch happens in the kernel
/// functions in `crate::distance::*` via `match *SIMD_SUPPORT` + per-tier
/// `#[target_feature(enable = "...")]` inner functions.
#[allow(non_camel_case_types)]
#[cfg(target_arch = "x86_64")]
#[derive(Clone, Copy)]
pub struct f64x8(__m256d, __m256d);

#[allow(non_camel_case_types)]
#[cfg(target_arch = "aarch64")]
#[derive(Clone, Copy)]
pub struct f64x8(float64x2x2_t, float64x2x2_t);

#[allow(non_camel_case_types)]
#[cfg(target_arch = "loongarch64")]
#[derive(Clone, Copy)]
pub struct f64x8(v4f64, v4f64);

impl std::fmt::Debug for f64x8 {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut arr = [0.0_f64; 8];
        unsafe {
            self.store_unaligned(arr.as_mut_ptr());
        }
        write!(f, "f64x8({:?})", arr)
    }
}

impl From<&[f64]> for f64x8 {
    fn from(value: &[f64]) -> Self {
        unsafe { Self::load_unaligned(value.as_ptr()) }
    }
}

impl<'a> From<&'a [f64; 8]> for f64x8 {
    fn from(value: &'a [f64; 8]) -> Self {
        unsafe { Self::load_unaligned(value.as_ptr()) }
    }
}

impl SIMD<f64, 8> for f64x8 {
    #[inline]
    fn splat(val: f64) -> Self {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            Self(_mm256_set1_pd(val), _mm256_set1_pd(val))
        }
        #[cfg(target_arch = "aarch64")]
        unsafe {
            let v = vdupq_n_f64(val);
            Self(float64x2x2_t(v, v), float64x2x2_t(v, v))
        }
        #[cfg(target_arch = "loongarch64")]
        unsafe {
            let v = transmute(lasx_xvreplgr2vr_d(transmute(val)));
            Self(v, v)
        }
    }

    #[inline]
    fn zeros() -> Self {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            Self(_mm256_setzero_pd(), _mm256_setzero_pd())
        }
        #[cfg(target_arch = "aarch64")]
        {
            Self::splat(0.0)
        }
        #[cfg(target_arch = "loongarch64")]
        {
            Self::splat(0.0)
        }
    }

    #[inline]
    unsafe fn load(ptr: *const f64) -> Self {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            Self(_mm256_load_pd(ptr), _mm256_load_pd(ptr.add(4)))
        }
        #[cfg(target_arch = "aarch64")]
        {
            Self::load_unaligned(ptr)
        }
        #[cfg(target_arch = "loongarch64")]
        {
            Self(
                transmute(lasx_xvld::<0>(transmute(ptr))),
                transmute(lasx_xvld::<32>(transmute(ptr))),
            )
        }
    }

    #[inline]
    unsafe fn load_unaligned(ptr: *const f64) -> Self {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            Self(_mm256_loadu_pd(ptr), _mm256_loadu_pd(ptr.add(4)))
        }
        #[cfg(target_arch = "aarch64")]
        unsafe {
            Self(vld1q_f64_x2(ptr), vld1q_f64_x2(ptr.add(4)))
        }
        #[cfg(target_arch = "loongarch64")]
        {
            Self(
                transmute(lasx_xvld::<0>(transmute(ptr))),
                transmute(lasx_xvld::<32>(transmute(ptr))),
            )
        }
    }

    #[inline]
    unsafe fn store(&self, ptr: *mut f64) {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            _mm256_store_pd(ptr, self.0);
            _mm256_store_pd(ptr.add(4), self.1);
        }
        #[cfg(target_arch = "aarch64")]
        unsafe {
            vst1q_f64_x2(ptr, self.0);
            vst1q_f64_x2(ptr.add(4), self.1);
        }
        #[cfg(target_arch = "loongarch64")]
        {
            lasx_xvst::<0>(transmute(self.0), transmute(ptr));
            lasx_xvst::<32>(transmute(self.1), transmute(ptr));
        }
    }

    #[inline]
    unsafe fn store_unaligned(&self, ptr: *mut f64) {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            _mm256_storeu_pd(ptr, self.0);
            _mm256_storeu_pd(ptr.add(4), self.1);
        }
        #[cfg(target_arch = "aarch64")]
        unsafe {
            vst1q_f64_x2(ptr, self.0);
            vst1q_f64_x2(ptr.add(4), self.1);
        }
        #[cfg(target_arch = "loongarch64")]
        {
            lasx_xvst::<0>(transmute(self.0), transmute(ptr));
            lasx_xvst::<32>(transmute(self.1), transmute(ptr));
        }
    }

    #[inline]
    fn reduce_sum(&self) -> f64 {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            let sum = _mm256_add_pd(self.0, self.1);
            let hi = _mm256_permute2f128_pd(sum, sum, 1);
            let sum = _mm256_add_pd(sum, hi);
            let sum = _mm256_hadd_pd(sum, sum);
            _mm256_cvtsd_f64(sum)
        }
        #[cfg(target_arch = "aarch64")]
        unsafe {
            let sum0 = vaddq_f64(self.0.0, self.0.1);
            let sum1 = vaddq_f64(self.1.0, self.1.1);
            let sum = vaddq_f64(sum0, sum1);
            vaddvq_f64(sum)
        }
        #[cfg(target_arch = "loongarch64")]
        {
            self.as_array().iter().sum()
        }
    }

    #[inline]
    fn reduce_min(&self) -> f64 {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            let m = _mm256_min_pd(self.0, self.1);
            let hi = _mm256_permute2f128_pd(m, m, 1);
            let m = _mm256_min_pd(m, hi);
            let shuf = _mm256_permute_pd(m, 0b0101);
            let m = _mm256_min_pd(m, shuf);
            _mm256_cvtsd_f64(m)
        }
        #[cfg(target_arch = "aarch64")]
        unsafe {
            let m0 = vminq_f64(self.0.0, self.0.1);
            let m1 = vminq_f64(self.1.0, self.1.1);
            let m = vminq_f64(m0, m1);
            vminvq_f64(m)
        }
        #[cfg(target_arch = "loongarch64")]
        {
            self.as_array()
                .iter()
                .copied()
                .fold(f64::INFINITY, f64::min)
        }
    }

    #[inline]
    fn min(&self, rhs: &Self) -> Self {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            Self(_mm256_min_pd(self.0, rhs.0), _mm256_min_pd(self.1, rhs.1))
        }
        #[cfg(target_arch = "aarch64")]
        unsafe {
            Self(
                float64x2x2_t(vminq_f64(self.0.0, rhs.0.0), vminq_f64(self.0.1, rhs.0.1)),
                float64x2x2_t(vminq_f64(self.1.0, rhs.1.0), vminq_f64(self.1.1, rhs.1.1)),
            )
        }
        #[cfg(target_arch = "loongarch64")]
        unsafe {
            Self(lasx_xvfmin_d(self.0, rhs.0), lasx_xvfmin_d(self.1, rhs.1))
        }
    }

    #[inline]
    fn find(&self, val: f64) -> Option<i32> {
        unsafe {
            for i in 0..8 {
                if self.as_array().get_unchecked(i) == &val {
                    return Some(i as i32);
                }
            }
        }
        None
    }
}

impl FloatSimd<f64, 8> for f64x8 {
    #[inline]
    fn multiply_add(&mut self, a: Self, b: Self) {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            self.0 = _mm256_fmadd_pd(a.0, b.0, self.0);
            self.1 = _mm256_fmadd_pd(a.1, b.1, self.1);
        }
        #[cfg(target_arch = "aarch64")]
        unsafe {
            self.0.0 = vfmaq_f64(self.0.0, a.0.0, b.0.0);
            self.0.1 = vfmaq_f64(self.0.1, a.0.1, b.0.1);
            self.1.0 = vfmaq_f64(self.1.0, a.1.0, b.1.0);
            self.1.1 = vfmaq_f64(self.1.1, a.1.1, b.1.1);
        }
        #[cfg(target_arch = "loongarch64")]
        unsafe {
            self.0 = lasx_xvfmadd_d(a.0, b.0, self.0);
            self.1 = lasx_xvfmadd_d(a.1, b.1, self.1);
        }
    }
}

impl Add for f64x8 {
    type Output = Self;

    #[inline]
    fn add(self, rhs: Self) -> Self::Output {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            Self(_mm256_add_pd(self.0, rhs.0), _mm256_add_pd(self.1, rhs.1))
        }
        #[cfg(target_arch = "aarch64")]
        unsafe {
            Self(
                float64x2x2_t(vaddq_f64(self.0.0, rhs.0.0), vaddq_f64(self.0.1, rhs.0.1)),
                float64x2x2_t(vaddq_f64(self.1.0, rhs.1.0), vaddq_f64(self.1.1, rhs.1.1)),
            )
        }
        #[cfg(target_arch = "loongarch64")]
        unsafe {
            Self(lasx_xvfadd_d(self.0, rhs.0), lasx_xvfadd_d(self.1, rhs.1))
        }
    }
}

impl AddAssign for f64x8 {
    #[inline]
    fn add_assign(&mut self, rhs: Self) {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            self.0 = _mm256_add_pd(self.0, rhs.0);
            self.1 = _mm256_add_pd(self.1, rhs.1);
        }
        #[cfg(target_arch = "aarch64")]
        unsafe {
            self.0.0 = vaddq_f64(self.0.0, rhs.0.0);
            self.0.1 = vaddq_f64(self.0.1, rhs.0.1);
            self.1.0 = vaddq_f64(self.1.0, rhs.1.0);
            self.1.1 = vaddq_f64(self.1.1, rhs.1.1);
        }
        #[cfg(target_arch = "loongarch64")]
        unsafe {
            self.0 = lasx_xvfadd_d(self.0, rhs.0);
            self.1 = lasx_xvfadd_d(self.1, rhs.1);
        }
    }
}

impl Mul for f64x8 {
    type Output = Self;

    #[inline]
    fn mul(self, rhs: Self) -> Self::Output {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            Self(_mm256_mul_pd(self.0, rhs.0), _mm256_mul_pd(self.1, rhs.1))
        }
        #[cfg(target_arch = "aarch64")]
        unsafe {
            Self(
                float64x2x2_t(vmulq_f64(self.0.0, rhs.0.0), vmulq_f64(self.0.1, rhs.0.1)),
                float64x2x2_t(vmulq_f64(self.1.0, rhs.1.0), vmulq_f64(self.1.1, rhs.1.1)),
            )
        }
        #[cfg(target_arch = "loongarch64")]
        unsafe {
            Self(lasx_xvfmul_d(self.0, rhs.0), lasx_xvfmul_d(self.1, rhs.1))
        }
    }
}

impl Sub for f64x8 {
    type Output = Self;

    #[inline]
    fn sub(self, rhs: Self) -> Self::Output {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            Self(_mm256_sub_pd(self.0, rhs.0), _mm256_sub_pd(self.1, rhs.1))
        }
        #[cfg(target_arch = "aarch64")]
        unsafe {
            Self(
                float64x2x2_t(vsubq_f64(self.0.0, rhs.0.0), vsubq_f64(self.0.1, rhs.0.1)),
                float64x2x2_t(vsubq_f64(self.1.0, rhs.1.0), vsubq_f64(self.1.1, rhs.1.1)),
            )
        }
        #[cfg(target_arch = "loongarch64")]
        unsafe {
            Self(lasx_xvfsub_d(self.0, rhs.0), lasx_xvfsub_d(self.1, rhs.1))
        }
    }
}

impl SubAssign for f64x8 {
    #[inline]
    fn sub_assign(&mut self, rhs: Self) {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            self.0 = _mm256_sub_pd(self.0, rhs.0);
            self.1 = _mm256_sub_pd(self.1, rhs.1);
        }
        #[cfg(target_arch = "aarch64")]
        unsafe {
            self.0.0 = vsubq_f64(self.0.0, rhs.0.0);
            self.0.1 = vsubq_f64(self.0.1, rhs.0.1);
            self.1.0 = vsubq_f64(self.1.0, rhs.1.0);
            self.1.1 = vsubq_f64(self.1.1, rhs.1.1);
        }
        #[cfg(target_arch = "loongarch64")]
        unsafe {
            self.0 = lasx_xvfsub_d(self.0, rhs.0);
            self.1 = lasx_xvfsub_d(self.1, rhs.1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_f64x4_basic_ops() {
        // The `f64x4` constructor / load / store / arithmetic paths all lower
        // to AVX intrinsics on x86_64; none of them need AVX2.
        #[cfg(target_arch = "x86_64")]
        if !std::is_x86_feature_detected!("avx") {
            return;
        }
        let a = [1.0_f64, 2.0, 3.0, 4.0];
        let b = [5.0_f64, 6.0, 7.0, 8.0];

        let simd_a: f64x4 = (&a).into();
        let simd_b: f64x4 = (&b).into();

        let sum = simd_a + simd_b;
        assert_eq!(sum.as_array(), [6.0, 8.0, 10.0, 12.0]);

        let product = simd_a * simd_b;
        assert_eq!(product.as_array(), [5.0, 12.0, 21.0, 32.0]);

        let diff = simd_b - simd_a;
        assert_eq!(diff.as_array(), [4.0, 4.0, 4.0, 4.0]);

        assert_eq!(simd_a.reduce_sum(), 10.0);
        assert_eq!(simd_a.reduce_min(), 1.0);
    }

    #[test]
    fn test_f64x4_fma() {
        // `multiply_add` lowers to `_mm256_fmadd_pd`, which needs FMA.
        #[cfg(target_arch = "x86_64")]
        if !std::is_x86_feature_detected!("avx") || !std::is_x86_feature_detected!("fma") {
            return;
        }
        let a = [1.0_f64, 2.0, 3.0, 4.0];
        let b = [2.0_f64, 3.0, 4.0, 5.0];

        let simd_a: f64x4 = (&a).into();
        let simd_b: f64x4 = (&b).into();
        let mut acc = f64x4::zeros();
        acc.multiply_add(simd_a, simd_b);
        assert_eq!(acc.as_array(), [2.0, 6.0, 12.0, 20.0]);
    }

    #[test]
    fn test_f64x4_min() {
        // `min` / `reduce_min` are AVX intrinsics.
        #[cfg(target_arch = "x86_64")]
        if !std::is_x86_feature_detected!("avx") {
            return;
        }
        let a = [1.0_f64, 5.0, 2.0, 8.0];
        let b = [3.0_f64, 2.0, 4.0, 1.0];
        let simd_a: f64x4 = (&a).into();
        let simd_b: f64x4 = (&b).into();

        let m = simd_a.min(&simd_b);
        assert_eq!(m.as_array(), [1.0, 2.0, 2.0, 1.0]);
        assert_eq!(m.reduce_min(), 1.0);
    }

    #[test]
    fn test_f64x8_basic_ops() {
        // `f64x8` is a pair of `__m256d`; add / reduce are AVX intrinsics.
        #[cfg(target_arch = "x86_64")]
        if !std::is_x86_feature_detected!("avx") {
            return;
        }
        let a: [f64; 8] = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0];
        let b: [f64; 8] = [10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0];

        let simd_a: f64x8 = (&a).into();
        let simd_b: f64x8 = (&b).into();

        let sum = simd_a + simd_b;
        assert_eq!(
            sum.as_array(),
            [11.0, 22.0, 33.0, 44.0, 55.0, 66.0, 77.0, 88.0]
        );

        assert_eq!(simd_a.reduce_sum(), 36.0);
        assert_eq!(simd_a.reduce_min(), 1.0);
    }

    #[test]
    fn test_f64x8_fma() {
        // `multiply_add` lowers to `_mm256_fmadd_pd`, which needs FMA.
        #[cfg(target_arch = "x86_64")]
        if !std::is_x86_feature_detected!("avx") || !std::is_x86_feature_detected!("fma") {
            return;
        }
        let a: [f64; 8] = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0];
        let b: [f64; 8] = [1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0];

        let simd_a: f64x8 = (&a).into();
        let simd_b: f64x8 = (&b).into();
        let mut acc = f64x8::zeros();
        acc.multiply_add(simd_a, simd_b);
        assert_eq!(acc.as_array(), a);
        assert_eq!(acc.reduce_sum(), 36.0);
    }

    #[test]
    fn test_f64x8_min() {
        // `min` / `reduce_min` are AVX intrinsics.
        #[cfg(target_arch = "x86_64")]
        if !std::is_x86_feature_detected!("avx") {
            return;
        }
        let a: [f64; 8] = [5.0, 1.0, 8.0, 3.0, 9.0, 2.0, 7.0, 4.0];
        let b: [f64; 8] = [2.0, 6.0, 3.0, 7.0, 1.0, 8.0, 4.0, 9.0];
        let simd_a: f64x8 = (&a).into();
        let simd_b: f64x8 = (&b).into();

        let m = simd_a.min(&simd_b);
        assert_eq!(m.as_array(), [2.0, 1.0, 3.0, 3.0, 1.0, 2.0, 4.0, 4.0]);
        assert_eq!(m.reduce_min(), 1.0);

        // Test with negative values
        let c: [f64; 8] = [-1.0, 5.0, 3.0, 7.0, 2.0, 4.0, 6.0, -3.0];
        let simd_c: f64x8 = (&c).into();
        assert_eq!(simd_c.reduce_min(), -3.0);
    }
}
