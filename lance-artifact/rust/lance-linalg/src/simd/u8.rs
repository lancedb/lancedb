// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! `u8x8`, 8 of `u8` values

use std::fmt::Formatter;

#[cfg(target_arch = "aarch64")]
use std::arch::aarch64::*;
#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::*;
use std::ops::{Add, AddAssign, Mul, Sub, SubAssign};

use super::{SIMD, Shuffle};

/// 16 of 8-bit `u8` values.
#[allow(non_camel_case_types)]
#[cfg(target_arch = "x86_64")]
#[derive(Clone, Copy)]
pub struct u8x16(pub __m128i);

/// 16 of 8-bit `u8` values.
#[allow(non_camel_case_types)]
#[cfg(target_arch = "aarch64")]
#[derive(Clone, Copy)]
pub struct u8x16(pub uint8x16_t);

#[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
#[derive(Clone, Copy)]
pub struct u8x16(pub [u8; 16]);

impl u8x16 {
    #[inline]
    pub fn bit_and(self, mask: u8) -> Self {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            Self(_mm_and_si128(self.0, _mm_set1_epi8(mask as i8)))
        }
        #[cfg(target_arch = "aarch64")]
        unsafe {
            Self(vandq_u8(self.0, vdupq_n_u8(mask)))
        }
        #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
        {
            let mut result = self.0;
            for i in 0..16 {
                result[i] &= mask;
            }
            Self(result)
        }
    }

    #[inline]
    pub fn right_shift<const N: i32>(self) -> Self {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            let shifted = _mm_srli_epi16(self.0, N);
            let mask = _mm_set1_epi8((1_i8 << (8 - N)) - 1);
            Self(_mm_and_si128(shifted, mask))
        }
        #[cfg(target_arch = "aarch64")]
        unsafe {
            Self(vshrq_n_u8::<N>(self.0))
        }
        #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
        {
            let mut result = [0u8; 16];
            for i in 0..16 {
                result[i] = self.0[i] >> N;
            }
            Self(result)
        }
    }
}

impl std::fmt::Debug for u8x16 {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut arr = [0u8; 16];
        unsafe {
            self.store_unaligned(arr.as_mut_ptr());
        }
        write!(f, "u8x16({:?})", arr)
    }
}

impl From<&[u8]> for u8x16 {
    fn from(value: &[u8]) -> Self {
        unsafe { Self::load_unaligned(value.as_ptr()) }
    }
}

impl<'a> From<&'a [u8; 16]> for u8x16 {
    fn from(value: &'a [u8; 16]) -> Self {
        unsafe { Self::load_unaligned(value.as_ptr()) }
    }
}

impl SIMD<u8, 16> for u8x16 {
    #[inline]
    fn splat(val: u8) -> Self {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            Self(_mm_set1_epi8(val as i8))
        }
        #[cfg(target_arch = "aarch64")]
        unsafe {
            Self(vdupq_n_u8(val))
        }
        #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
        {
            let mut result = [0u8; 16];
            for i in 0..16 {
                result[i] = val;
            }
            Self(result)
        }
    }

    #[inline]
    fn zeros() -> Self {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            Self(_mm_setzero_si128())
        }
        #[cfg(target_arch = "aarch64")]
        {
            Self::splat(0)
        }
        #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
        {
            Self([0; 16])
        }
    }

    #[inline]
    unsafe fn load(ptr: *const u8) -> Self {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            Self(_mm_loadu_si128(ptr as *const __m128i))
        }
        #[cfg(target_arch = "aarch64")]
        {
            Self::load_unaligned(ptr)
        }
        #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
        {
            Self::load_unaligned(ptr)
        }
    }

    #[inline]
    unsafe fn load_unaligned(ptr: *const u8) -> Self {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            Self(_mm_loadu_si128(ptr as *const __m128i))
        }
        #[cfg(target_arch = "aarch64")]
        {
            Self(vld1q_u8(ptr))
        }
        #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
        {
            let mut result = [0u8; 16];
            for i in 0..16 {
                result[i] = *ptr.add(i);
            }
            Self(result)
        }
    }

    #[inline]
    unsafe fn store(&self, ptr: *mut u8) {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            _mm_storeu_si128(ptr as *mut __m128i, self.0)
        }
        #[cfg(target_arch = "aarch64")]
        unsafe {
            vst1q_u8(ptr, self.0)
        }
        #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
        {
            self.store_unaligned(ptr);
        }
    }

    #[inline]
    unsafe fn store_unaligned(&self, ptr: *mut u8) {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            _mm_storeu_si128(ptr as *mut __m128i, self.0)
        }
        #[cfg(target_arch = "aarch64")]
        unsafe {
            vst1q_u8(ptr, self.0)
        }
        #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
        {
            for i in 0..16 {
                *ptr.add(i) = self.0[i];
            }
        }
    }

    fn reduce_sum(&self) -> u8 {
        todo!("it is not implemented yet");
    }

    #[inline]
    fn reduce_min(&self) -> u8 {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            let low = _mm_and_si128(self.0, _mm_set1_epi8(0xFF_u8 as i8));
            let high = _mm_srli_si128(self.0, 8);
            let min_low = _mm_min_epu8(low, high);
            let min_low = _mm_min_epu8(min_low, _mm_srli_si128(min_low, 4));
            let min_low = _mm_min_epu8(min_low, _mm_srli_si128(min_low, 2));
            let min_low = _mm_min_epu8(min_low, _mm_srli_si128(min_low, 1));
            _mm_extract_epi8(min_low, 0) as u8
        }
        #[cfg(target_arch = "aarch64")]
        unsafe {
            vminvq_u8(self.0)
        }
        #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
        {
            let mut min = self.0[0];
            for i in 1..16 {
                min = std::cmp::min(min, self.0[i]);
            }
            min
        }
    }

    #[inline]
    fn min(&self, rhs: &Self) -> Self {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            Self(_mm_min_epu8(self.0, rhs.0))
        }
        #[cfg(target_arch = "aarch64")]
        unsafe {
            Self(vminq_u8(self.0, rhs.0))
        }
        #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
        {
            let mut result = [0u8; 16];
            for i in 0..16 {
                result[i] = std::cmp::min(self.0[i], rhs.0[i]);
            }
            Self(result)
        }
    }

    fn find(&self, _val: u8) -> Option<i32> {
        todo!()
    }
}

impl Shuffle for u8x16 {
    fn shuffle(&self, indices: u8x16) -> Self {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            Self(_mm_shuffle_epi8(self.0, indices.0))
        }
        #[cfg(target_arch = "aarch64")]
        unsafe {
            Self(vqtbl1q_u8(self.0, indices.0))
        }
        #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
        {
            let mut result = [0u8; 16];
            for i in 0..16 {
                result[i] = self.0[indices.0[i] as usize];
            }
            Self(result)
        }
    }
}

impl Add for u8x16 {
    type Output = Self;

    #[inline]
    fn add(self, rhs: Self) -> Self::Output {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            Self(_mm_adds_epu8(self.0, rhs.0))
        }
        #[cfg(target_arch = "aarch64")]
        unsafe {
            Self(vqaddq_u8(self.0, rhs.0))
        }
        #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
        {
            let mut result = [0u8; 16];
            for i in 0..16 {
                result[i] = self.0[i].saturating_add(rhs.0[i]);
            }
            Self(result)
        }
    }
}

impl AddAssign for u8x16 {
    #[inline]
    fn add_assign(&mut self, rhs: Self) {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            self.0 = _mm_adds_epu8(self.0, rhs.0)
        }
        #[cfg(target_arch = "aarch64")]
        unsafe {
            self.0 = vqaddq_u8(self.0, rhs.0)
        }
        #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
        {
            for i in 0..16 {
                self.0[i] = self.0[i].saturating_add(rhs.0[i]);
            }
        }
    }
}

impl Mul for u8x16 {
    type Output = Self;

    #[inline]
    fn mul(self, rhs: Self) -> Self::Output {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            let a_lo = _mm_unpacklo_epi8(self.0, _mm_setzero_si128());
            let a_hi = _mm_unpackhi_epi8(self.0, _mm_setzero_si128());
            let b_lo = _mm_unpacklo_epi8(rhs.0, _mm_setzero_si128());
            let b_hi = _mm_unpackhi_epi8(rhs.0, _mm_setzero_si128());

            let res_lo = _mm_mullo_epi16(a_lo, b_lo);
            let res_hi = _mm_mullo_epi16(a_hi, b_hi);

            Self(_mm_packus_epi16(res_lo, res_hi))
        }
        #[cfg(target_arch = "aarch64")]
        unsafe {
            Self(vmulq_u8(self.0, rhs.0))
        }
        #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
        {
            let mut result = [0u8; 16];
            for i in 0..16 {
                result[i] = self.0[i].wrapping_mul(rhs.0[i]);
            }
            Self(result)
        }
    }
}

impl Sub for u8x16 {
    type Output = Self;

    #[inline]
    fn sub(self, rhs: Self) -> Self::Output {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            Self(_mm_sub_epi8(self.0, rhs.0))
        }
        #[cfg(target_arch = "aarch64")]
        unsafe {
            Self(vsubq_u8(self.0, rhs.0))
        }
        #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
        {
            let mut result = [0u8; 16];
            for i in 0..16 {
                result[i] = self.0[i].wrapping_sub(rhs.0[i]);
            }
            Self(result)
        }
    }
}

impl SubAssign for u8x16 {
    #[inline]
    fn sub_assign(&mut self, rhs: Self) {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            self.0 = _mm_sub_epi8(self.0, rhs.0)
        }
        #[cfg(target_arch = "aarch64")]
        unsafe {
            self.0 = vsubq_u8(self.0, rhs.0)
        }
        #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
        {
            for i in 0..16 {
                self.0[i] = self.0[i].wrapping_sub(rhs.0[i]);
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_basic_u8x16_ops() {
        let a = (0..16).map(|f| f as u8).collect::<Vec<_>>();
        let b = (16..32).map(|f| f as u8).collect::<Vec<_>>();

        let simd_a = unsafe { u8x16::load_unaligned(a.as_ptr()) };
        let simd_b = unsafe { u8x16::load_unaligned(b.as_ptr()) };

        let simd_add = simd_a + simd_b;
        (0..16)
            .zip(simd_add.as_array().iter())
            .for_each(|(x, &y)| assert_eq!((x + x + 16) as u8, y));

        // on x86_64, the result of simd_mul is saturated
        // on aarch64, the result of simd_mul is not saturated
        let simd_mul = simd_a * simd_b;
        (0..16).zip(simd_mul.as_array().iter()).for_each(|(x, &y)| {
            #[cfg(target_arch = "x86_64")]
            assert_eq!(std::cmp::min(x * (x + 16), 255_i32) as u8, y);
            #[cfg(target_arch = "aarch64")]
            assert_eq!((x * (x + 16_i32)) as u8, y);
        });
    }

    #[test]
    fn test_saturating_add() {
        let a = u8x16::splat(200);
        let b = u8x16::splat(100);
        let mut result = a + b;

        let expected = (0..16).map(|_| 255).collect::<Vec<_>>();
        assert_eq!(result.as_array(), expected.as_slice());

        result += b;
        assert_eq!(result.as_array(), expected.as_slice());
    }
}
