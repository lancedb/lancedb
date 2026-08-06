// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use super::BitPacker;

use crate::bitpacker_internal::{Available, UnsafeBitPacker};

const BLOCK_LEN: usize = 32 * 8;

#[cfg(target_arch = "x86_64")]
mod avx2 {
    use super::BLOCK_LEN;
    use crate::bitpacker_internal::Available;

    use std::arch::x86_64::__m256i as DataType;
    use std::arch::x86_64::_mm256_and_si256 as op_and;
    use std::arch::x86_64::_mm256_lddqu_si256 as load_unaligned;
    use std::arch::x86_64::_mm256_or_si256 as op_or;
    use std::arch::x86_64::_mm256_set1_epi32 as set1;
    use std::arch::x86_64::_mm256_slli_epi32 as left_shift_32;
    use std::arch::x86_64::_mm256_srli_epi32 as right_shift_32;
    use std::arch::x86_64::_mm256_storeu_si256 as store_unaligned;

    use std::arch::x86_64::{
        _mm256_add_epi32, _mm256_extract_epi32, _mm256_permute2f128_si256, _mm256_shuffle_epi32,
        _mm256_slli_si256, _mm256_srli_si256, _mm256_sub_epi32,
    };

    #[allow(non_snake_case)]
    unsafe fn or_collapse_to_u32(accumulator: DataType) -> u32 {
        let a__b__c__d__e__f__g__h_ = accumulator;
        let ______a__b________e__f = _mm256_srli_si256(a__b__c__d__e__f__g__h_, 8);
        let a__b__ca_db_e__f__ge_hf = op_or(a__b__c__d__e__f__g__h_, ______a__b________e__f);
        let ___a__b__ca____e__f__ge = _mm256_srli_si256(a__b__ca_db_e__f__ge_hf, 4);
        let _________cadb______gehf = op_or(a__b__ca_db_e__f__ge_hf, ___a__b__ca____e__f__ge);
        let cadb = _mm256_extract_epi32(_________cadb______gehf, 0);
        let gehf = _mm256_extract_epi32(_________cadb______gehf, 4);
        (cadb | gehf) as u32
    }

    unsafe fn compute_delta(curr: DataType, prev: DataType) -> DataType {
        let left_shift = _mm256_slli_si256(curr, 4);
        let curr_shift = _mm256_srli_si256(curr, 12);
        let curr_right_only = _mm256_permute2f128_si256(curr_shift, curr_shift, 8);
        let prev_shift = _mm256_srli_si256(prev, 12);
        let sub_left = _mm256_permute2f128_si256(prev_shift, prev_shift, 3 | (8 << 4));
        let diff = op_or(left_shift, op_or(curr_right_only, sub_left));
        _mm256_sub_epi32(curr, diff)
    }

    #[allow(non_snake_case)]
    unsafe fn integrate_delta(prev: DataType, delta: DataType) -> DataType {
        let offset_repeat = _mm256_shuffle_epi32(prev, 0xff);
        let offset = _mm256_permute2f128_si256(offset_repeat, offset_repeat, 3 | (8 << 4));
        let a__b__c__d__e__f__g__h__ = delta;
        let ______a__b________e__f__ = _mm256_slli_si256(delta, 8);
        let a__b__ca_db_e__f__ge_fh_ =
            _mm256_add_epi32(a__b__c__d__e__f__g__h__, ______a__b________e__f__);
        let ___a__b__ca____e__f__ge_ = _mm256_slli_si256(a__b__ca_db_e__f__ge_fh_, 4);
        let halved_prefix_sum =
            _mm256_add_epi32(___a__b__ca____e__f__ge_, a__b__ca_db_e__f__ge_fh_);
        let offsetted_halved_prefix_sum = _mm256_add_epi32(halved_prefix_sum, offset);
        let select_last_low = _mm256_shuffle_epi32(offsetted_halved_prefix_sum, 0xff);
        let high_offset = _mm256_permute2f128_si256(select_last_low, select_last_low, 8);
        _mm256_add_epi32(high_offset, offsetted_halved_prefix_sum)
    }

    unsafe fn add(left: DataType, right: DataType) -> DataType {
        _mm256_add_epi32(left, right)
    }

    unsafe fn sub(left: DataType, right: DataType) -> DataType {
        _mm256_sub_epi32(left, right)
    }

    declare_bitpacker!(target_feature(enable = "avx2"));

    impl Available for UnsafeBitPackerImpl {
        fn available() -> bool {
            is_x86_feature_detected!("avx2")
        }
    }
}

#[cfg(all(target_arch = "aarch64", target_endian = "little"))]
mod neon {
    use super::BLOCK_LEN;
    use crate::bitpacker_internal::Available;
    use std::arch::aarch64::{
        uint32x4_t, vaddq_u32, vandq_u32, vdupq_n_u32, vextq_u32, vgetq_lane_u32, vld1q_u32,
        vorrq_u32, vshlq_n_u32, vshrq_n_u32, vst1q_u32, vsubq_u32,
    };

    pub(crate) type DataType = [uint32x4_t; 2];

    #[inline]
    unsafe fn set1(el: i32) -> DataType {
        let lanes = vdupq_n_u32(el as u32);
        [lanes, lanes]
    }

    #[inline]
    unsafe fn right_shift_32<const N: i32>(el: DataType) -> DataType {
        const {
            assert!(N >= 0);
            assert!(N <= 32);
        }

        match N {
            0 => el,
            1 => [vshrq_n_u32::<1>(el[0]), vshrq_n_u32::<1>(el[1])],
            2 => [vshrq_n_u32::<2>(el[0]), vshrq_n_u32::<2>(el[1])],
            3 => [vshrq_n_u32::<3>(el[0]), vshrq_n_u32::<3>(el[1])],
            4 => [vshrq_n_u32::<4>(el[0]), vshrq_n_u32::<4>(el[1])],
            5 => [vshrq_n_u32::<5>(el[0]), vshrq_n_u32::<5>(el[1])],
            6 => [vshrq_n_u32::<6>(el[0]), vshrq_n_u32::<6>(el[1])],
            7 => [vshrq_n_u32::<7>(el[0]), vshrq_n_u32::<7>(el[1])],
            8 => [vshrq_n_u32::<8>(el[0]), vshrq_n_u32::<8>(el[1])],
            9 => [vshrq_n_u32::<9>(el[0]), vshrq_n_u32::<9>(el[1])],
            10 => [vshrq_n_u32::<10>(el[0]), vshrq_n_u32::<10>(el[1])],
            11 => [vshrq_n_u32::<11>(el[0]), vshrq_n_u32::<11>(el[1])],
            12 => [vshrq_n_u32::<12>(el[0]), vshrq_n_u32::<12>(el[1])],
            13 => [vshrq_n_u32::<13>(el[0]), vshrq_n_u32::<13>(el[1])],
            14 => [vshrq_n_u32::<14>(el[0]), vshrq_n_u32::<14>(el[1])],
            15 => [vshrq_n_u32::<15>(el[0]), vshrq_n_u32::<15>(el[1])],
            16 => [vshrq_n_u32::<16>(el[0]), vshrq_n_u32::<16>(el[1])],
            17 => [vshrq_n_u32::<17>(el[0]), vshrq_n_u32::<17>(el[1])],
            18 => [vshrq_n_u32::<18>(el[0]), vshrq_n_u32::<18>(el[1])],
            19 => [vshrq_n_u32::<19>(el[0]), vshrq_n_u32::<19>(el[1])],
            20 => [vshrq_n_u32::<20>(el[0]), vshrq_n_u32::<20>(el[1])],
            21 => [vshrq_n_u32::<21>(el[0]), vshrq_n_u32::<21>(el[1])],
            22 => [vshrq_n_u32::<22>(el[0]), vshrq_n_u32::<22>(el[1])],
            23 => [vshrq_n_u32::<23>(el[0]), vshrq_n_u32::<23>(el[1])],
            24 => [vshrq_n_u32::<24>(el[0]), vshrq_n_u32::<24>(el[1])],
            25 => [vshrq_n_u32::<25>(el[0]), vshrq_n_u32::<25>(el[1])],
            26 => [vshrq_n_u32::<26>(el[0]), vshrq_n_u32::<26>(el[1])],
            27 => [vshrq_n_u32::<27>(el[0]), vshrq_n_u32::<27>(el[1])],
            28 => [vshrq_n_u32::<28>(el[0]), vshrq_n_u32::<28>(el[1])],
            29 => [vshrq_n_u32::<29>(el[0]), vshrq_n_u32::<29>(el[1])],
            30 => [vshrq_n_u32::<30>(el[0]), vshrq_n_u32::<30>(el[1])],
            31 => [vshrq_n_u32::<31>(el[0]), vshrq_n_u32::<31>(el[1])],
            32 => set1(0),
            _ => core::hint::unreachable_unchecked(),
        }
    }

    #[inline]
    unsafe fn left_shift_32<const N: i32>(el: DataType) -> DataType {
        const {
            assert!(N >= 0);
            assert!(N <= 32);
        }

        match N {
            0 => el,
            1 => [vshlq_n_u32::<1>(el[0]), vshlq_n_u32::<1>(el[1])],
            2 => [vshlq_n_u32::<2>(el[0]), vshlq_n_u32::<2>(el[1])],
            3 => [vshlq_n_u32::<3>(el[0]), vshlq_n_u32::<3>(el[1])],
            4 => [vshlq_n_u32::<4>(el[0]), vshlq_n_u32::<4>(el[1])],
            5 => [vshlq_n_u32::<5>(el[0]), vshlq_n_u32::<5>(el[1])],
            6 => [vshlq_n_u32::<6>(el[0]), vshlq_n_u32::<6>(el[1])],
            7 => [vshlq_n_u32::<7>(el[0]), vshlq_n_u32::<7>(el[1])],
            8 => [vshlq_n_u32::<8>(el[0]), vshlq_n_u32::<8>(el[1])],
            9 => [vshlq_n_u32::<9>(el[0]), vshlq_n_u32::<9>(el[1])],
            10 => [vshlq_n_u32::<10>(el[0]), vshlq_n_u32::<10>(el[1])],
            11 => [vshlq_n_u32::<11>(el[0]), vshlq_n_u32::<11>(el[1])],
            12 => [vshlq_n_u32::<12>(el[0]), vshlq_n_u32::<12>(el[1])],
            13 => [vshlq_n_u32::<13>(el[0]), vshlq_n_u32::<13>(el[1])],
            14 => [vshlq_n_u32::<14>(el[0]), vshlq_n_u32::<14>(el[1])],
            15 => [vshlq_n_u32::<15>(el[0]), vshlq_n_u32::<15>(el[1])],
            16 => [vshlq_n_u32::<16>(el[0]), vshlq_n_u32::<16>(el[1])],
            17 => [vshlq_n_u32::<17>(el[0]), vshlq_n_u32::<17>(el[1])],
            18 => [vshlq_n_u32::<18>(el[0]), vshlq_n_u32::<18>(el[1])],
            19 => [vshlq_n_u32::<19>(el[0]), vshlq_n_u32::<19>(el[1])],
            20 => [vshlq_n_u32::<20>(el[0]), vshlq_n_u32::<20>(el[1])],
            21 => [vshlq_n_u32::<21>(el[0]), vshlq_n_u32::<21>(el[1])],
            22 => [vshlq_n_u32::<22>(el[0]), vshlq_n_u32::<22>(el[1])],
            23 => [vshlq_n_u32::<23>(el[0]), vshlq_n_u32::<23>(el[1])],
            24 => [vshlq_n_u32::<24>(el[0]), vshlq_n_u32::<24>(el[1])],
            25 => [vshlq_n_u32::<25>(el[0]), vshlq_n_u32::<25>(el[1])],
            26 => [vshlq_n_u32::<26>(el[0]), vshlq_n_u32::<26>(el[1])],
            27 => [vshlq_n_u32::<27>(el[0]), vshlq_n_u32::<27>(el[1])],
            28 => [vshlq_n_u32::<28>(el[0]), vshlq_n_u32::<28>(el[1])],
            29 => [vshlq_n_u32::<29>(el[0]), vshlq_n_u32::<29>(el[1])],
            30 => [vshlq_n_u32::<30>(el[0]), vshlq_n_u32::<30>(el[1])],
            31 => [vshlq_n_u32::<31>(el[0]), vshlq_n_u32::<31>(el[1])],
            32 => set1(0),
            _ => core::hint::unreachable_unchecked(),
        }
    }

    #[inline]
    unsafe fn op_or(left: DataType, right: DataType) -> DataType {
        [vorrq_u32(left[0], right[0]), vorrq_u32(left[1], right[1])]
    }

    #[inline]
    unsafe fn op_and(left: DataType, right: DataType) -> DataType {
        [vandq_u32(left[0], right[0]), vandq_u32(left[1], right[1])]
    }

    #[inline]
    unsafe fn load_unaligned(addr: *const DataType) -> DataType {
        let ptr = addr.cast::<u32>();
        [vld1q_u32(ptr), vld1q_u32(ptr.add(4))]
    }

    #[inline]
    unsafe fn store_unaligned(addr: *mut DataType, data: DataType) {
        let ptr = addr.cast::<u32>();
        vst1q_u32(ptr, data[0]);
        vst1q_u32(ptr.add(4), data[1]);
    }

    #[inline]
    unsafe fn or_collapse_to_u32(accumulator: DataType) -> u32 {
        vgetq_lane_u32(accumulator[0], 0)
            | vgetq_lane_u32(accumulator[0], 1)
            | vgetq_lane_u32(accumulator[0], 2)
            | vgetq_lane_u32(accumulator[0], 3)
            | vgetq_lane_u32(accumulator[1], 0)
            | vgetq_lane_u32(accumulator[1], 1)
            | vgetq_lane_u32(accumulator[1], 2)
            | vgetq_lane_u32(accumulator[1], 3)
    }

    #[inline]
    unsafe fn compute_delta(curr: DataType, prev: DataType) -> DataType {
        [
            vsubq_u32(curr[0], vextq_u32(prev[1], curr[0], 3)),
            vsubq_u32(curr[1], vextq_u32(curr[0], curr[1], 3)),
        ]
    }

    #[allow(non_snake_case)]
    #[inline]
    unsafe fn integrate_half(base: u32, delta: uint32x4_t) -> uint32x4_t {
        let base = vdupq_n_u32(base);
        let zero = vdupq_n_u32(0);
        let a__b__c__d_ = delta;
        let ______a__b_ = vextq_u32(zero, a__b__c__d_, 2);
        let a__b__ca_db = vaddq_u32(______a__b_, a__b__c__d_);
        let ___a__b__ca = vextq_u32(zero, a__b__ca_db, 3);
        let a_ab_abc_abcd = vaddq_u32(___a__b__ca, a__b__ca_db);
        vaddq_u32(base, a_ab_abc_abcd)
    }

    #[inline]
    unsafe fn integrate_delta(prev: DataType, delta: DataType) -> DataType {
        let low = integrate_half(vgetq_lane_u32(prev[1], 3), delta[0]);
        let high = integrate_half(vgetq_lane_u32(low, 3), delta[1]);
        [low, high]
    }

    #[inline]
    unsafe fn add(left: DataType, right: DataType) -> DataType {
        [vaddq_u32(left[0], right[0]), vaddq_u32(left[1], right[1])]
    }

    #[inline]
    unsafe fn sub(left: DataType, right: DataType) -> DataType {
        [vsubq_u32(left[0], right[0]), vsubq_u32(left[1], right[1])]
    }

    declare_bitpacker!(target_feature(enable = "neon"));

    impl Available for UnsafeBitPackerImpl {
        fn available() -> bool {
            std::arch::is_aarch64_feature_detected!("neon")
        }
    }
}

mod scalar {
    use super::BLOCK_LEN;
    use crate::bitpacker_internal::Available;
    use std::ptr;

    pub(crate) type DataType = [u32; 8];

    pub(crate) fn set1(el: i32) -> DataType {
        [el as u32; 8]
    }

    pub(crate) fn right_shift_32<const N: i32>(el: DataType) -> DataType {
        [
            el[0] >> N,
            el[1] >> N,
            el[2] >> N,
            el[3] >> N,
            el[4] >> N,
            el[5] >> N,
            el[6] >> N,
            el[7] >> N,
        ]
    }

    pub(crate) fn left_shift_32<const N: i32>(el: DataType) -> DataType {
        [
            el[0] << N,
            el[1] << N,
            el[2] << N,
            el[3] << N,
            el[4] << N,
            el[5] << N,
            el[6] << N,
            el[7] << N,
        ]
    }

    pub(crate) fn op_or(left: DataType, right: DataType) -> DataType {
        [
            left[0] | right[0],
            left[1] | right[1],
            left[2] | right[2],
            left[3] | right[3],
            left[4] | right[4],
            left[5] | right[5],
            left[6] | right[6],
            left[7] | right[7],
        ]
    }

    pub(crate) fn op_and(left: DataType, right: DataType) -> DataType {
        [
            left[0] & right[0],
            left[1] & right[1],
            left[2] & right[2],
            left[3] & right[3],
            left[4] & right[4],
            left[5] & right[5],
            left[6] & right[6],
            left[7] & right[7],
        ]
    }

    pub(crate) unsafe fn load_unaligned(addr: *const DataType) -> DataType {
        ptr::read_unaligned(addr)
    }

    pub(crate) unsafe fn store_unaligned(addr: *mut DataType, data: DataType) {
        ptr::write_unaligned(addr, data);
    }

    pub(crate) fn or_collapse_to_u32(accumulator: DataType) -> u32 {
        ((accumulator[0] | accumulator[1]) | (accumulator[2] | accumulator[3]))
            | ((accumulator[4] | accumulator[5]) | (accumulator[6] | accumulator[7]))
    }

    fn compute_delta(curr: DataType, prev: DataType) -> DataType {
        [
            curr[0].wrapping_sub(prev[7]),
            curr[1].wrapping_sub(curr[0]),
            curr[2].wrapping_sub(curr[1]),
            curr[3].wrapping_sub(curr[2]),
            curr[4].wrapping_sub(curr[3]),
            curr[5].wrapping_sub(curr[4]),
            curr[6].wrapping_sub(curr[5]),
            curr[7].wrapping_sub(curr[6]),
        ]
    }

    fn integrate_delta(offset: DataType, delta: DataType) -> DataType {
        let el0 = offset[7].wrapping_add(delta[0]);
        let el1 = el0.wrapping_add(delta[1]);
        let el2 = el1.wrapping_add(delta[2]);
        let el3 = el2.wrapping_add(delta[3]);
        let el4 = el3.wrapping_add(delta[4]);
        let el5 = el4.wrapping_add(delta[5]);
        let el6 = el5.wrapping_add(delta[6]);
        let el7 = el6.wrapping_add(delta[7]);
        [el0, el1, el2, el3, el4, el5, el6, el7]
    }

    pub(crate) fn add(left: DataType, right: DataType) -> DataType {
        [
            left[0].wrapping_add(right[0]),
            left[1].wrapping_add(right[1]),
            left[2].wrapping_add(right[2]),
            left[3].wrapping_add(right[3]),
            left[4].wrapping_add(right[4]),
            left[5].wrapping_add(right[5]),
            left[6].wrapping_add(right[6]),
            left[7].wrapping_add(right[7]),
        ]
    }

    pub(crate) fn sub(left: DataType, right: DataType) -> DataType {
        [
            left[0].wrapping_sub(right[0]),
            left[1].wrapping_sub(right[1]),
            left[2].wrapping_sub(right[2]),
            left[3].wrapping_sub(right[3]),
            left[4].wrapping_sub(right[4]),
            left[5].wrapping_sub(right[5]),
            left[6].wrapping_sub(right[6]),
            left[7].wrapping_sub(right[7]),
        ]
    }

    // The `allow(unused)` is here to put an attribute that has no effect.
    //
    // For other bitpackers, we enable a specific CPU instruction set, but for
    // the scalar bitpacker none is required.
    declare_bitpacker!(allow(unused));

    impl Available for UnsafeBitPackerImpl {
        fn available() -> bool {
            true
        }
    }
}

#[derive(Clone, Copy)]
enum InstructionSet {
    #[cfg(target_arch = "x86_64")]
    AVX2,
    #[cfg(all(target_arch = "aarch64", target_endian = "little"))]
    NEON,
    Scalar,
}

/// 8-wide bitpacker implementation.
///
/// One block contains 256 integers.
#[derive(Clone, Copy)]
pub struct BitPacker8x(InstructionSet);

impl BitPacker8x {
    #[cfg(target_arch = "x86_64")]
    pub(crate) fn new_avx2() -> Option<Self> {
        avx2::UnsafeBitPackerImpl::available().then_some(BitPacker8x(InstructionSet::AVX2))
    }

    #[cfg(not(target_arch = "x86_64"))]
    pub(crate) fn new_avx2() -> Option<Self> {
        None
    }

    #[cfg(all(target_arch = "aarch64", target_endian = "little"))]
    pub(crate) fn new_neon() -> Option<Self> {
        neon::UnsafeBitPackerImpl::available().then_some(BitPacker8x(InstructionSet::NEON))
    }

    #[cfg(not(all(target_arch = "aarch64", target_endian = "little")))]
    pub(crate) fn new_neon() -> Option<Self> {
        None
    }

    pub(crate) fn new_scalar() -> Self {
        BitPacker8x(InstructionSet::Scalar)
    }
}

impl BitPacker for BitPacker8x {
    const BLOCK_LEN: usize = BLOCK_LEN;

    fn new() -> Self {
        Self::new_avx2()
            .or_else(Self::new_neon)
            .unwrap_or_else(Self::new_scalar)
    }

    fn compress(&self, decompressed: &[u32], compressed: &mut [u8], num_bits: u8) -> usize {
        unsafe {
            match self.0 {
                #[cfg(target_arch = "x86_64")]
                InstructionSet::AVX2 => {
                    avx2::UnsafeBitPackerImpl::compress(decompressed, compressed, num_bits)
                }
                #[cfg(all(target_arch = "aarch64", target_endian = "little"))]
                InstructionSet::NEON => {
                    neon::UnsafeBitPackerImpl::compress(decompressed, compressed, num_bits)
                }
                InstructionSet::Scalar => {
                    scalar::UnsafeBitPackerImpl::compress(decompressed, compressed, num_bits)
                }
            }
        }
    }

    fn compress_sorted(
        &self,
        initial: u32,
        decompressed: &[u32],
        compressed: &mut [u8],
        num_bits: u8,
    ) -> usize {
        unsafe {
            match self.0 {
                #[cfg(target_arch = "x86_64")]
                InstructionSet::AVX2 => avx2::UnsafeBitPackerImpl::compress_sorted(
                    initial,
                    decompressed,
                    compressed,
                    num_bits,
                ),
                #[cfg(all(target_arch = "aarch64", target_endian = "little"))]
                InstructionSet::NEON => neon::UnsafeBitPackerImpl::compress_sorted(
                    initial,
                    decompressed,
                    compressed,
                    num_bits,
                ),
                InstructionSet::Scalar => scalar::UnsafeBitPackerImpl::compress_sorted(
                    initial,
                    decompressed,
                    compressed,
                    num_bits,
                ),
            }
        }
    }

    fn compress_strictly_sorted(
        &self,
        initial: Option<u32>,
        decompressed: &[u32],
        compressed: &mut [u8],
        num_bits: u8,
    ) -> usize {
        unsafe {
            match self.0 {
                #[cfg(target_arch = "x86_64")]
                InstructionSet::AVX2 => avx2::UnsafeBitPackerImpl::compress_strictly_sorted(
                    initial,
                    decompressed,
                    compressed,
                    num_bits,
                ),
                #[cfg(all(target_arch = "aarch64", target_endian = "little"))]
                InstructionSet::NEON => neon::UnsafeBitPackerImpl::compress_strictly_sorted(
                    initial,
                    decompressed,
                    compressed,
                    num_bits,
                ),
                InstructionSet::Scalar => scalar::UnsafeBitPackerImpl::compress_strictly_sorted(
                    initial,
                    decompressed,
                    compressed,
                    num_bits,
                ),
            }
        }
    }

    fn decompress(&self, compressed: &[u8], decompressed: &mut [u32], num_bits: u8) -> usize {
        unsafe {
            match self.0 {
                #[cfg(target_arch = "x86_64")]
                InstructionSet::AVX2 => {
                    avx2::UnsafeBitPackerImpl::decompress(compressed, decompressed, num_bits)
                }
                #[cfg(all(target_arch = "aarch64", target_endian = "little"))]
                InstructionSet::NEON => {
                    neon::UnsafeBitPackerImpl::decompress(compressed, decompressed, num_bits)
                }
                InstructionSet::Scalar => {
                    scalar::UnsafeBitPackerImpl::decompress(compressed, decompressed, num_bits)
                }
            }
        }
    }

    fn decompress_sorted(
        &self,
        initial: u32,
        compressed: &[u8],
        decompressed: &mut [u32],
        num_bits: u8,
    ) -> usize {
        unsafe {
            match self.0 {
                #[cfg(target_arch = "x86_64")]
                InstructionSet::AVX2 => avx2::UnsafeBitPackerImpl::decompress_sorted(
                    initial,
                    compressed,
                    decompressed,
                    num_bits,
                ),
                #[cfg(all(target_arch = "aarch64", target_endian = "little"))]
                InstructionSet::NEON => neon::UnsafeBitPackerImpl::decompress_sorted(
                    initial,
                    compressed,
                    decompressed,
                    num_bits,
                ),
                InstructionSet::Scalar => scalar::UnsafeBitPackerImpl::decompress_sorted(
                    initial,
                    compressed,
                    decompressed,
                    num_bits,
                ),
            }
        }
    }

    fn decompress_strictly_sorted(
        &self,
        initial: Option<u32>,
        compressed: &[u8],
        decompressed: &mut [u32],
        num_bits: u8,
    ) -> usize {
        unsafe {
            match self.0 {
                #[cfg(target_arch = "x86_64")]
                InstructionSet::AVX2 => avx2::UnsafeBitPackerImpl::decompress_strictly_sorted(
                    initial,
                    compressed,
                    decompressed,
                    num_bits,
                ),
                #[cfg(all(target_arch = "aarch64", target_endian = "little"))]
                InstructionSet::NEON => neon::UnsafeBitPackerImpl::decompress_strictly_sorted(
                    initial,
                    compressed,
                    decompressed,
                    num_bits,
                ),
                InstructionSet::Scalar => scalar::UnsafeBitPackerImpl::decompress_strictly_sorted(
                    initial,
                    compressed,
                    decompressed,
                    num_bits,
                ),
            }
        }
    }

    fn num_bits(&self, decompressed: &[u32]) -> u8 {
        unsafe {
            match self.0 {
                #[cfg(target_arch = "x86_64")]
                InstructionSet::AVX2 => avx2::UnsafeBitPackerImpl::num_bits(decompressed),
                #[cfg(all(target_arch = "aarch64", target_endian = "little"))]
                InstructionSet::NEON => neon::UnsafeBitPackerImpl::num_bits(decompressed),
                InstructionSet::Scalar => scalar::UnsafeBitPackerImpl::num_bits(decompressed),
            }
        }
    }

    fn num_bits_sorted(&self, initial: u32, decompressed: &[u32]) -> u8 {
        unsafe {
            match self.0 {
                #[cfg(target_arch = "x86_64")]
                InstructionSet::AVX2 => {
                    avx2::UnsafeBitPackerImpl::num_bits_sorted(initial, decompressed)
                }
                #[cfg(all(target_arch = "aarch64", target_endian = "little"))]
                InstructionSet::NEON => {
                    neon::UnsafeBitPackerImpl::num_bits_sorted(initial, decompressed)
                }
                InstructionSet::Scalar => {
                    scalar::UnsafeBitPackerImpl::num_bits_sorted(initial, decompressed)
                }
            }
        }
    }

    fn num_bits_strictly_sorted(&self, initial: Option<u32>, decompressed: &[u32]) -> u8 {
        unsafe {
            match self.0 {
                #[cfg(target_arch = "x86_64")]
                InstructionSet::AVX2 => {
                    avx2::UnsafeBitPackerImpl::num_bits_strictly_sorted(initial, decompressed)
                }
                #[cfg(all(target_arch = "aarch64", target_endian = "little"))]
                InstructionSet::NEON => {
                    neon::UnsafeBitPackerImpl::num_bits_strictly_sorted(initial, decompressed)
                }
                InstructionSet::Scalar => {
                    scalar::UnsafeBitPackerImpl::num_bits_strictly_sorted(initial, decompressed)
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::BitPacker8x;
    use crate::bitpacker_internal::BitPacker;
    use bitpacking::{BitPacker as ExternalBitPacker, BitPacker8x as ExternalBitPacker8x};

    fn mask_for_width(width: u8) -> u32 {
        match width {
            0 => 0,
            32 => u32::MAX,
            _ => (1u32 << width) - 1,
        }
    }

    fn raw_values(width: u8, seed: u64) -> Vec<u32> {
        let mask = mask_for_width(width);
        let mut state = seed;
        (0..BitPacker8x::BLOCK_LEN)
            .map(|idx| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                match seed % 4 {
                    0 => 0,
                    1 => mask,
                    2 => idx as u32 & mask,
                    _ => state as u32 & mask,
                }
            })
            .collect()
    }

    fn sorted_values(width: u8, seed: u64) -> (u32, Vec<u32>) {
        if width == 0 {
            return (17, vec![17; BitPacker8x::BLOCK_LEN]);
        }
        if width == 32 {
            return (0, vec![u32::MAX; BitPacker8x::BLOCK_LEN]);
        }

        let mask = mask_for_width(width).min(127);
        let mut state = seed;
        let mut current = 17u32;
        let values = (0..BitPacker8x::BLOCK_LEN)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                current += state as u32 & mask;
                current
            })
            .collect();
        (17, values)
    }

    fn strictly_sorted_values(width: u8, seed: u64) -> (Option<u32>, Vec<u32>) {
        let mask = mask_for_width(width).min(127);
        let mut state = seed;
        let mut current = 0u32;
        let values = (0..BitPacker8x::BLOCK_LEN)
            .map(|idx| {
                if idx == 0 {
                    current = 0;
                } else {
                    state ^= state << 13;
                    state ^= state >> 7;
                    state ^= state << 17;
                    current += 1 + (state as u32 & mask);
                }
                current
            })
            .collect();
        (None, values)
    }

    fn assert_raw_compatible(ours: BitPacker8x, external: ExternalBitPacker8x) {
        for width in 0..=32 {
            for seed in [0, 1, 2, 123456789] {
                let values = raw_values(width, seed);
                assert_eq!(ours.num_bits(&values), external.num_bits(&values));

                let mut actual = vec![0u8; BitPacker8x::compressed_block_size(width)];
                let actual_len = ours.compress(&values, &mut actual, width);

                let mut expected = vec![0u8; ExternalBitPacker8x::compressed_block_size(width)];
                let expected_len = external.compress(&values, &mut expected, width);

                assert_eq!(actual_len, expected_len);
                assert_eq!(actual, expected, "raw width {width} seed {seed}");

                let mut decoded = vec![0u32; BitPacker8x::BLOCK_LEN];
                assert_eq!(ours.decompress(&actual, &mut decoded, width), actual_len);
                assert_eq!(decoded, values);
            }
        }
    }

    fn assert_sorted_compatible(ours: BitPacker8x, external: ExternalBitPacker8x) {
        for width in 0..=32 {
            for seed in [0, 1, 2, 123456789] {
                let (initial, values) = sorted_values(width, seed);
                assert_eq!(
                    ours.num_bits_sorted(initial, &values),
                    external.num_bits_sorted(initial, &values)
                );

                let mut actual = vec![0u8; BitPacker8x::compressed_block_size(width)];
                let actual_len = ours.compress_sorted(initial, &values, &mut actual, width);

                let mut expected = vec![0u8; ExternalBitPacker8x::compressed_block_size(width)];
                let expected_len = external.compress_sorted(initial, &values, &mut expected, width);

                assert_eq!(actual_len, expected_len);
                assert_eq!(actual, expected, "sorted width {width} seed {seed}");

                let mut decoded = vec![0u32; BitPacker8x::BLOCK_LEN];
                assert_eq!(
                    ours.decompress_sorted(initial, &actual, &mut decoded, width),
                    actual_len
                );
                assert_eq!(decoded, values);
            }
        }
    }

    fn assert_strictly_sorted_compatible(ours: BitPacker8x, external: ExternalBitPacker8x) {
        for width in 0..=16 {
            for seed in [0, 1, 2, 123456789] {
                let (initial, values) = strictly_sorted_values(width, seed);
                let num_bits = external.num_bits_strictly_sorted(initial, &values);
                assert_eq!(ours.num_bits_strictly_sorted(initial, &values), num_bits);

                let mut actual = vec![0u8; BitPacker8x::compressed_block_size(num_bits)];
                let actual_len =
                    ours.compress_strictly_sorted(initial, &values, &mut actual, num_bits);

                let mut expected = vec![0u8; ExternalBitPacker8x::compressed_block_size(num_bits)];
                let expected_len =
                    external.compress_strictly_sorted(initial, &values, &mut expected, num_bits);

                assert_eq!(actual_len, expected_len);
                assert_eq!(actual, expected, "strict width {width} seed {seed}");

                let mut decoded = vec![0u32; BitPacker8x::BLOCK_LEN];
                assert_eq!(
                    ours.decompress_strictly_sorted(initial, &actual, &mut decoded, num_bits),
                    actual_len
                );
                assert_eq!(decoded, values);
            }
        }
    }

    #[test]
    fn bitpacker8x_raw_compatible_with_external_bitpacking() {
        assert_raw_compatible(BitPacker8x::new(), ExternalBitPacker8x::new());
    }

    #[test]
    fn bitpacker8x_sorted_compatible_with_external_bitpacking() {
        assert_sorted_compatible(BitPacker8x::new(), ExternalBitPacker8x::new());
    }

    #[test]
    fn scalar_backend_matches_external_bitpacker8x() {
        let scalar = BitPacker8x::new_scalar();
        let external = ExternalBitPacker8x::new();

        assert_raw_compatible(scalar, external);
        assert_sorted_compatible(scalar, external);
    }

    #[test]
    fn scalar_backend_matches_external_strictly_sorted_bitpacker8x() {
        let scalar = BitPacker8x::new_scalar();
        let external = ExternalBitPacker8x::new();

        assert_strictly_sorted_compatible(scalar, external);
    }

    #[cfg(all(target_arch = "aarch64", target_endian = "little"))]
    #[test]
    fn neon_backend_matches_external_bitpacker8x() {
        if let Some(neon) = BitPacker8x::new_neon() {
            let external = ExternalBitPacker8x::new();

            assert_raw_compatible(neon, external);
            assert_sorted_compatible(neon, external);
            assert_strictly_sorted_compatible(neon, external);
        }
    }
}
