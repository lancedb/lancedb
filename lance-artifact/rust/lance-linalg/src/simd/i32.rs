// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::fmt::Formatter;
use std::ops::{Add, AddAssign, Mul, Sub, SubAssign};

#[cfg(target_arch = "aarch64")]
use std::arch::aarch64::*;
#[cfg(target_arch = "loongarch64")]
use std::arch::loongarch64::*;
#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::*;
#[cfg(target_arch = "loongarch64")]
use std::mem::transmute;

use super::SIMD;

#[allow(non_camel_case_types)]
#[cfg(target_arch = "x86_64")]
#[derive(Clone, Copy)]
pub struct i32x8(pub(crate) __m256i);

#[allow(non_camel_case_types)]
#[cfg(target_arch = "aarch64")]
#[derive(Clone, Copy)]
pub struct i32x8(int32x4x2_t);

#[allow(non_camel_case_types)]
#[cfg(target_arch = "loongarch64")]
#[derive(Clone, Copy)]
pub struct i32x8(v8i32);

impl std::fmt::Debug for i32x8 {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut arr = [0; 8];
        unsafe {
            self.store_unaligned(arr.as_mut_ptr());
        }
        write!(f, "i32x8({:?})", arr)
    }
}

impl From<&[i32]> for i32x8 {
    fn from(value: &[i32]) -> Self {
        unsafe { Self::load_unaligned(value.as_ptr()) }
    }
}

impl From<&[i32; 8]> for i32x8 {
    fn from(value: &[i32; 8]) -> Self {
        unsafe { Self::load_unaligned(value.as_ptr()) }
    }
}

impl SIMD<i32, 8> for i32x8 {
    #[inline]
    fn splat(val: i32) -> Self {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            Self(_mm256_set1_epi32(val))
        }
        #[cfg(target_arch = "aarch64")]
        unsafe {
            Self(int32x4x2_t(vdupq_n_s32(val), vdupq_n_s32(val)))
        }
        #[cfg(target_arch = "loongarch64")]
        unsafe {
            Self(lasx_xvreplgr2vr_w(val))
        }
    }

    #[inline]
    fn zeros() -> Self {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            Self(_mm256_setzero_si256())
        }
        #[cfg(target_arch = "aarch64")]
        {
            Self::splat(0)
        }
        #[cfg(target_arch = "loongarch64")]
        {
            Self::splat(0)
        }
    }

    #[inline]
    unsafe fn load(ptr: *const i32) -> Self {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            Self(_mm256_loadu_si256(ptr as *const __m256i))
        }
        #[cfg(target_arch = "aarch64")]
        {
            Self(vld1q_s32_x2(ptr))
        }
        #[cfg(target_arch = "loongarch64")]
        {
            Self(transmute(lasx_xvld::<0>(transmute(ptr))))
        }
    }

    #[inline]
    unsafe fn load_unaligned(ptr: *const i32) -> Self {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            Self(_mm256_loadu_si256(ptr as *const __m256i))
        }
        #[cfg(target_arch = "aarch64")]
        {
            Self(vld1q_s32_x2(ptr))
        }
        #[cfg(target_arch = "loongarch64")]
        {
            Self(transmute(lasx_xvld::<0>(transmute(ptr))))
        }
    }

    #[inline]
    unsafe fn store(&self, ptr: *mut i32) {
        self.store_unaligned(ptr)
    }

    unsafe fn store_unaligned(&self, ptr: *mut i32) {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            _mm256_storeu_si256(ptr as *mut __m256i, self.0);
        }
        #[cfg(target_arch = "aarch64")]
        unsafe {
            vst1q_s32_x2(ptr, self.0)
        }
        #[cfg(target_arch = "loongarch64")]
        unsafe {
            lasx_xvst::<0>(transmute(self.0), transmute(ptr))
        }
    }

    fn reduce_sum(&self) -> i32 {
        #[cfg(target_arch = "x86_64")]
        {
            self.as_array().iter().sum()
        }
        #[cfg(target_arch = "aarch64")]
        unsafe {
            let sum = vaddq_s32(self.0.0, self.0.1);
            vaddvq_s32(sum)
        }
        #[cfg(target_arch = "loongarch64")]
        {
            self.as_array().iter().sum()
        }
    }

    fn reduce_min(&self) -> i32 {
        todo!()
    }

    fn min(&self, rhs: &Self) -> Self {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            Self(_mm256_min_epi32(self.0, rhs.0))
        }
        #[cfg(target_arch = "aarch64")]
        unsafe {
            Self(int32x4x2_t(
                vminq_s32(self.0.0, rhs.0.0),
                vminq_s32(self.0.1, rhs.0.1),
            ))
        }
        #[cfg(target_arch = "loongarch64")]
        unsafe {
            Self(lasx_xvmin_w(self.0, rhs.0))
        }
    }

    fn find(&self, val: i32) -> Option<i32> {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            for i in 0..8 {
                if self.as_array().get_unchecked(i) == &val {
                    return Some(i as i32);
                }
            }
        }
        #[cfg(target_arch = "aarch64")]
        unsafe {
            let tgt = vdupq_n_s32(val);
            let mut arr = [0; 8];
            let mask1 = vceqq_s32(self.0.0, tgt);
            let mask2 = vceqq_s32(self.0.1, tgt);
            vst1q_u32(arr.as_mut_ptr(), mask1);
            vst1q_u32(arr.as_mut_ptr().add(4), mask2);
            for i in 0..8 {
                if arr.get_unchecked(i) != &0 {
                    return Some(i as i32);
                }
            }
        }
        #[cfg(target_arch = "loongarch64")]
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

impl Add for i32x8 {
    type Output = Self;

    #[inline]
    fn add(self, rhs: Self) -> Self::Output {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            Self(_mm256_add_epi32(self.0, rhs.0))
        }
        #[cfg(target_arch = "aarch64")]
        unsafe {
            Self(int32x4x2_t(
                vaddq_s32(self.0.0, rhs.0.0),
                vaddq_s32(self.0.1, rhs.0.1),
            ))
        }
        #[cfg(target_arch = "loongarch64")]
        unsafe {
            Self(lasx_xvadd_w(self.0, rhs.0))
        }
    }
}

impl AddAssign for i32x8 {
    #[inline]
    fn add_assign(&mut self, rhs: Self) {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            self.0 = _mm256_add_epi32(self.0, rhs.0);
        }
        #[cfg(target_arch = "aarch64")]
        unsafe {
            self.0.0 = vaddq_s32(self.0.0, rhs.0.0);
            self.0.1 = vaddq_s32(self.0.1, rhs.0.1);
        }
        #[cfg(target_arch = "loongarch64")]
        unsafe {
            self.0 = lasx_xvadd_w(self.0, rhs.0);
        }
    }
}

impl Sub for i32x8 {
    type Output = Self;

    #[inline]
    fn sub(self, rhs: Self) -> Self::Output {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            Self(_mm256_sub_epi32(self.0, rhs.0))
        }
        #[cfg(target_arch = "aarch64")]
        unsafe {
            Self(int32x4x2_t(
                vsubq_s32(self.0.0, rhs.0.0),
                vsubq_s32(self.0.1, rhs.0.1),
            ))
        }
        #[cfg(target_arch = "loongarch64")]
        unsafe {
            Self(lasx_xvsub_w(self.0, rhs.0))
        }
    }
}

impl SubAssign for i32x8 {
    #[inline]
    fn sub_assign(&mut self, rhs: Self) {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            self.0 = _mm256_sub_epi32(self.0, rhs.0);
        }
        #[cfg(target_arch = "aarch64")]
        unsafe {
            self.0.0 = vsubq_s32(self.0.0, rhs.0.0);
            self.0.1 = vsubq_s32(self.0.1, rhs.0.1);
        }
        #[cfg(target_arch = "loongarch64")]
        unsafe {
            self.0 = lasx_xvsub_w(self.0, rhs.0);
        }
    }
}

impl Mul for i32x8 {
    type Output = Self;

    #[inline]
    fn mul(self, rhs: Self) -> Self::Output {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            Self(_mm256_mul_epi32(self.0, rhs.0))
        }
        #[cfg(target_arch = "aarch64")]
        unsafe {
            Self(int32x4x2_t(
                vmulq_s32(self.0.0, rhs.0.0),
                vmulq_s32(self.0.1, rhs.0.1),
            ))
        }
        #[cfg(target_arch = "loongarch64")]
        unsafe {
            Self(lasx_xvmul_w(self.0, rhs.0))
        }
    }
}

#[cfg(test)]
mod tests {}
