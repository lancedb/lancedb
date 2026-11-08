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

/// 8 of 32-bit `i32` values. Use 256-bit SIMD if possible.
///
/// The x86_64 arm reaches AVX and AVX2 intrinsics with no `#[target_feature]`
/// gate of its own, so callers must already be inside an AVX2-checked context.
/// `x86_64-unknown-linux-gnu` is pinned to `target-cpu=x86-64-v2`
/// (`.cargo/config.toml`), which is below AVX.
#[allow(non_camel_case_types)]
#[cfg(target_arch = "x86_64")]
#[derive(Clone, Copy)]
pub struct i32x8(pub(crate) __m256i);

/// 8 of 32-bit `i32` values. Use 256-bit SIMD if possible.
#[allow(non_camel_case_types)]
#[cfg(target_arch = "aarch64")]
#[derive(Clone, Copy)]
pub struct i32x8(int32x4x2_t);

/// 8 of 32-bit `i32` values. Use 256-bit SIMD if possible.
#[allow(non_camel_case_types)]
#[cfg(target_arch = "loongarch64")]
#[derive(Clone, Copy)]
pub struct i32x8(v8i32);

#[allow(non_camel_case_types)]
#[cfg(simd_fallback)]
#[derive(Clone, Copy)]
pub struct i32x8([i32; 8]);

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
    /// Loads the first 8 values from `value`.
    ///
    /// # Panics
    ///
    /// Panics if `value` contains fewer than 8 values.
    fn from(value: &[i32]) -> Self {
        assert!(
            value.len() >= 8,
            "i32x8 requires at least 8 values, got {}",
            value.len()
        );
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
        #[cfg(simd_fallback)]
        {
            Self([val; 8])
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
        #[cfg(simd_fallback)]
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
        #[cfg(simd_fallback)]
        {
            unsafe { Self::load_unaligned(ptr) }
        }
    }

    #[inline]
    unsafe fn load_unaligned(ptr: *const i32) -> Self {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            // Treat the register as plain data so this load remains valid on
            // pre-AVX hosts. LLVM still lowers this to an AVX move when AVX is
            // enabled for the caller.
            Self(ptr.cast::<__m256i>().read_unaligned())
        }
        #[cfg(target_arch = "aarch64")]
        {
            Self(vld1q_s32_x2(ptr))
        }
        #[cfg(target_arch = "loongarch64")]
        {
            Self(transmute(lasx_xvld::<0>(transmute(ptr))))
        }
        #[cfg(simd_fallback)]
        {
            unsafe { Self(std::ptr::read_unaligned(ptr as *const [i32; 8])) }
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
        #[cfg(simd_fallback)]
        {
            unsafe { std::ptr::write_unaligned(ptr as *mut [i32; 8], self.0) }
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
        #[cfg(simd_fallback)]
        {
            self.0.iter().sum()
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
        #[cfg(simd_fallback)]
        {
            Self(std::array::from_fn(|i| self.0[i].min(rhs.0[i])))
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
        #[cfg(simd_fallback)]
        {
            if let Some(i) = self.0.iter().position(|&v| v == val) {
                return Some(i as i32);
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
        #[cfg(simd_fallback)]
        {
            Self(std::array::from_fn(|i| self.0[i].wrapping_add(rhs.0[i])))
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
        #[cfg(simd_fallback)]
        {
            for i in 0..8 {
                self.0[i] = self.0[i].wrapping_add(rhs.0[i]);
            }
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
        #[cfg(simd_fallback)]
        {
            Self(std::array::from_fn(|i| self.0[i].wrapping_sub(rhs.0[i])))
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
        #[cfg(simd_fallback)]
        {
            for i in 0..8 {
                self.0[i] = self.0[i].wrapping_sub(rhs.0[i]);
            }
        }
    }
}

impl Mul for i32x8 {
    type Output = Self;

    /// Lane-wise product, keeping the low 32 bits of each result.
    ///
    /// `mul` wraps on overflow rather than panicking the way scalar `i32 * i32`
    /// does in a debug build, and every arm agrees on that: `vpmulld`,
    /// `vmulq_s32`, `lasx_xvmul_w` and `wrapping_mul` each discard the high
    /// half. This is a statement about `mul` alone — `reduce_sum` sums in scalar
    /// `i32` outside aarch64 (so it panics on overflow in a debug build) but
    /// reduces in-register on aarch64, where it wraps.
    ///
    /// Picking a widening variant here is a silent wrong answer, not a compile
    /// error: `_mm256_mul_epi32` (`vpmuldq`) multiplies only the even 32-bit
    /// lanes and writes four 64-bit results, so `[1, 2, ..., 8]` squared came
    /// back as `[1, 0, 9, 0, 25, 0, 49, 0]`.
    #[inline]
    fn mul(self, rhs: Self) -> Self::Output {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            Self(_mm256_mullo_epi32(self.0, rhs.0))
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
        #[cfg(simd_fallback)]
        {
            Self(std::array::from_fn(|i| self.0[i].wrapping_mul(rhs.0[i])))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[test]
    fn test_slice_conversion_rejects_short_input() {
        assert!(std::panic::catch_unwind(|| i32x8::from(&[0; 7][..])).is_err());
    }

    #[test]
    fn test_slice_conversion_uses_cpu_baseline() {
        let values = [1_i32, 2, 3, 4, 5, 6, 7, 8];
        let from_slice = i32x8::from(&values[..]);
        let from_array = i32x8::from(&values);

        #[cfg(target_arch = "x86_64")]
        if !std::is_x86_feature_detected!("avx") {
            std::hint::black_box((from_slice, from_array));
            return;
        }

        assert_eq!(from_slice.as_array(), values);
        assert_eq!(from_array.as_array(), values);
    }

    /// Lane-wise, low-32-bits multiplication is what every arm promises, so
    /// this runs everywhere: only the x86 feature check is arch-gated, matching
    /// `f32.rs`'s and `f64.rs`'s test modules.
    ///
    /// Every case below has to produce a different answer under the widening
    /// `vpmuldq` this file used to call. All-zero *inputs* would not: `vpmuldq`
    /// returns zeros for those too.
    #[rstest]
    #[case::squares([1, 2, 3, 4, 5, 6, 7, 8], [1, 2, 3, 4, 5, 6, 7, 8], [1, 4, 9, 16, 25, 36, 49, 64])]
    #[case::mixed_signs([-3, 7, -3, 7, -3, 7, -3, 7], [7, -3, 7, -3, 7, -3, 7, -3], [-21; 8])]
    #[case::wraps_to_low_32_bits([65536; 8], [65536; 8], [0; 8])]
    fn mul_is_lane_wise(#[case] lhs: [i32; 8], #[case] rhs: [i32; 8], #[case] expected: [i32; 8]) {
        // `load_unaligned` / `store_unaligned` are AVX and `mul` is AVX2, and
        // none of them is `#[target_feature]`-gated, so a pre-Haswell host would
        // SIGILL. The `qemu-pre-haswell` CI job runs exactly that.
        #[cfg(target_arch = "x86_64")]
        if !std::is_x86_feature_detected!("avx2") {
            return;
        }

        let product = i32x8::from(&lhs) * i32x8::from(&rhs);

        assert_eq!(product.as_array(), expected);
    }
}
