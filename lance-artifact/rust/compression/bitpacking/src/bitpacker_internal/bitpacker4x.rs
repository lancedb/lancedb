// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use super::BitPacker;

use crate::bitpacker_internal::{Available, UnsafeBitPacker};

const BLOCK_LEN: usize = 32 * 4;

#[cfg(target_arch = "x86_64")]
mod sse3 {

    use super::BLOCK_LEN;
    use crate::bitpacker_internal::Available;

    use std::arch::x86_64::__m128i as DataType;
    use std::arch::x86_64::_mm_and_si128 as op_and;
    use std::arch::x86_64::_mm_lddqu_si128 as load_unaligned;
    use std::arch::x86_64::_mm_or_si128 as op_or;
    use std::arch::x86_64::_mm_set1_epi32 as set1;
    use std::arch::x86_64::_mm_slli_epi32 as left_shift_32;
    use std::arch::x86_64::_mm_srli_epi32 as right_shift_32;
    use std::arch::x86_64::_mm_storeu_si128 as store_unaligned;
    use std::arch::x86_64::{
        _mm_add_epi32, _mm_cvtsi128_si32, _mm_shuffle_epi32, _mm_slli_si128, _mm_srli_si128,
        _mm_sub_epi32,
    };

    #[allow(non_snake_case)]
    #[inline]
    unsafe fn or_collapse_to_u32(accumulator: DataType) -> u32 {
        let a__b__c__d_ = accumulator;
        let ______a__b_ = _mm_srli_si128(a__b__c__d_, 8);
        let a__b__ca_db = op_or(a__b__c__d_, ______a__b_);
        let ___a__b__ca = _mm_srli_si128(a__b__ca_db, 4);
        let _______cadb = op_or(a__b__ca_db, ___a__b__ca);
        _mm_cvtsi128_si32(_______cadb) as u32
    }

    #[target_feature(enable = "sse3")]
    unsafe fn compute_delta(curr: DataType, prev: DataType) -> DataType {
        _mm_sub_epi32(
            curr,
            op_or(_mm_slli_si128(curr, 4), _mm_srli_si128(prev, 12)),
        )
    }

    #[target_feature(enable = "sse3")]
    #[allow(non_snake_case)]
    #[inline]
    unsafe fn integrate_delta(prev: DataType, delta: DataType) -> DataType {
        let offset = _mm_shuffle_epi32(prev, 0xff);
        let a__b__c__d_ = delta;
        let ______a__b_ = _mm_slli_si128(delta, 8);
        let a__b__ca_db = _mm_add_epi32(______a__b_, a__b__c__d_);
        let ___a__b__ca = _mm_slli_si128(a__b__ca_db, 4);
        let a_ab_abc_abcd: DataType = _mm_add_epi32(___a__b__ca, a__b__ca_db);
        _mm_add_epi32(offset, a_ab_abc_abcd)
    }

    #[target_feature(enable = "sse3")]
    #[inline]
    unsafe fn add(left: DataType, right: DataType) -> DataType {
        _mm_add_epi32(left, right)
    }

    unsafe fn sub(left: DataType, right: DataType) -> DataType {
        _mm_sub_epi32(left, right)
    }

    declare_bitpacker!(target_feature(enable = "sse3"));

    impl Available for UnsafeBitPackerImpl {
        fn available() -> bool {
            is_x86_feature_detected!("sse3")
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

    pub(crate) type DataType = uint32x4_t;

    #[inline]
    /// Creates a vector with all elements set to `el`.
    unsafe fn set1(el: i32) -> DataType {
        vdupq_n_u32(el as u32)
    }

    #[inline]
    unsafe fn right_shift_32<const N: i32>(el: DataType) -> DataType {
        const {
            assert!(N >= 0);
            assert!(N <= 32);
        }

        // We unroll here because vshrq_n_u32 only accepts constants from 1 to 32.
        match N {
            0 => el,
            1 => vshrq_n_u32::<1>(el),
            2 => vshrq_n_u32::<2>(el),
            3 => vshrq_n_u32::<3>(el),
            4 => vshrq_n_u32::<4>(el),
            5 => vshrq_n_u32::<5>(el),
            6 => vshrq_n_u32::<6>(el),
            7 => vshrq_n_u32::<7>(el),
            8 => vshrq_n_u32::<8>(el),
            9 => vshrq_n_u32::<9>(el),
            10 => vshrq_n_u32::<10>(el),
            11 => vshrq_n_u32::<11>(el),
            12 => vshrq_n_u32::<12>(el),
            13 => vshrq_n_u32::<13>(el),
            14 => vshrq_n_u32::<14>(el),
            15 => vshrq_n_u32::<15>(el),
            16 => vshrq_n_u32::<16>(el),
            17 => vshrq_n_u32::<17>(el),
            18 => vshrq_n_u32::<18>(el),
            19 => vshrq_n_u32::<19>(el),
            20 => vshrq_n_u32::<20>(el),
            21 => vshrq_n_u32::<21>(el),
            22 => vshrq_n_u32::<22>(el),
            23 => vshrq_n_u32::<23>(el),
            24 => vshrq_n_u32::<24>(el),
            25 => vshrq_n_u32::<25>(el),
            26 => vshrq_n_u32::<26>(el),
            27 => vshrq_n_u32::<27>(el),
            28 => vshrq_n_u32::<28>(el),
            29 => vshrq_n_u32::<29>(el),
            30 => vshrq_n_u32::<30>(el),
            31 => vshrq_n_u32::<31>(el),
            32 => vdupq_n_u32(0),
            _ => core::hint::unreachable_unchecked(),
        }
    }

    #[inline]
    unsafe fn left_shift_32<const N: i32>(el: DataType) -> DataType {
        const {
            assert!(N >= 0);
            assert!(N <= 32);
        }

        // We unroll here because vshlq_n_u32 only accepts constants from 0 to 31.
        match N {
            0 => el,
            1 => vshlq_n_u32::<1>(el),
            2 => vshlq_n_u32::<2>(el),
            3 => vshlq_n_u32::<3>(el),
            4 => vshlq_n_u32::<4>(el),
            5 => vshlq_n_u32::<5>(el),
            6 => vshlq_n_u32::<6>(el),
            7 => vshlq_n_u32::<7>(el),
            8 => vshlq_n_u32::<8>(el),
            9 => vshlq_n_u32::<9>(el),
            10 => vshlq_n_u32::<10>(el),
            11 => vshlq_n_u32::<11>(el),
            12 => vshlq_n_u32::<12>(el),
            13 => vshlq_n_u32::<13>(el),
            14 => vshlq_n_u32::<14>(el),
            15 => vshlq_n_u32::<15>(el),
            16 => vshlq_n_u32::<16>(el),
            17 => vshlq_n_u32::<17>(el),
            18 => vshlq_n_u32::<18>(el),
            19 => vshlq_n_u32::<19>(el),
            20 => vshlq_n_u32::<20>(el),
            21 => vshlq_n_u32::<21>(el),
            22 => vshlq_n_u32::<22>(el),
            23 => vshlq_n_u32::<23>(el),
            24 => vshlq_n_u32::<24>(el),
            25 => vshlq_n_u32::<25>(el),
            26 => vshlq_n_u32::<26>(el),
            27 => vshlq_n_u32::<27>(el),
            28 => vshlq_n_u32::<28>(el),
            29 => vshlq_n_u32::<29>(el),
            30 => vshlq_n_u32::<30>(el),
            31 => vshlq_n_u32::<31>(el),
            32 => vdupq_n_u32(0),
            _ => core::hint::unreachable_unchecked(),
        }
    }

    use vorrq_u32 as op_or;

    #[inline]
    unsafe fn op_and(left: DataType, right: DataType) -> DataType {
        vandq_u32(left, right)
    }

    #[inline]
    unsafe fn load_unaligned(addr: *const DataType) -> DataType {
        vld1q_u32(addr.cast::<u32>())
    }

    #[inline]
    unsafe fn store_unaligned(addr: *mut DataType, data: DataType) {
        vst1q_u32(addr.cast::<u32>(), data);
    }

    #[inline]
    /// Collapses the vector by performing a bitwise OR across all lanes
    unsafe fn or_collapse_to_u32(acc: DataType) -> u32 {
        vgetq_lane_u32(acc, 0)
            | vgetq_lane_u32(acc, 1)
            | vgetq_lane_u32(acc, 2)
            | vgetq_lane_u32(acc, 3)
    }

    unsafe fn compute_delta(curr: DataType, prev: DataType) -> DataType {
        // Build a vector with [prev[3], curr[0], curr[1], curr[2]]
        let prev_shifted = vextq_u32(prev, curr, 3);
        vsubq_u32(curr, prev_shifted)
    }

    #[allow(non_snake_case)]
    #[inline]
    unsafe fn integrate_delta(prev: DataType, delta: DataType) -> DataType {
        let base = vdupq_n_u32(vgetq_lane_u32(prev, 3));
        let zero = vdupq_n_u32(0);
        let a__b__c__d_ = delta;
        let ______a__b_ = vextq_u32(zero, a__b__c__d_, 2);
        let a__b__ca_db = vaddq_u32(______a__b_, a__b__c__d_);
        let ___a__b__ca = vextq_u32(zero, a__b__ca_db, 3);
        let a_ab_abc_abcd = vaddq_u32(___a__b__ca, a__b__ca_db);
        vaddq_u32(base, a_ab_abc_abcd)
    }

    #[inline]
    unsafe fn add(left: DataType, right: DataType) -> DataType {
        vaddq_u32(left, right)
    }

    #[inline]
    unsafe fn sub(left: DataType, right: DataType) -> DataType {
        vsubq_u32(left, right)
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

    pub(crate) type DataType = [u32; 4];

    pub(crate) fn set1(el: i32) -> DataType {
        [el as u32; 4]
    }

    pub(crate) fn right_shift_32<const N: i32>(el: DataType) -> DataType {
        [el[0] >> N, el[1] >> N, el[2] >> N, el[3] >> N]
    }

    pub(crate) fn left_shift_32<const N: i32>(el: DataType) -> DataType {
        [el[0] << N, el[1] << N, el[2] << N, el[3] << N]
    }

    pub(crate) fn op_or(left: DataType, right: DataType) -> DataType {
        [
            left[0] | right[0],
            left[1] | right[1],
            left[2] | right[2],
            left[3] | right[3],
        ]
    }

    pub(crate) fn op_and(left: DataType, right: DataType) -> DataType {
        [
            left[0] & right[0],
            left[1] & right[1],
            left[2] & right[2],
            left[3] & right[3],
        ]
    }

    pub(crate) unsafe fn load_unaligned(addr: *const DataType) -> DataType {
        ptr::read_unaligned(addr)
    }

    pub(crate) unsafe fn store_unaligned(addr: *mut DataType, data: DataType) {
        ptr::write_unaligned(addr, data);
    }

    pub(crate) fn or_collapse_to_u32(accumulator: DataType) -> u32 {
        (accumulator[0] | accumulator[1]) | (accumulator[2] | accumulator[3])
    }

    fn compute_delta(curr: DataType, prev: DataType) -> DataType {
        [
            curr[0].wrapping_sub(prev[3]),
            curr[1].wrapping_sub(curr[0]),
            curr[2].wrapping_sub(curr[1]),
            curr[3].wrapping_sub(curr[2]),
        ]
    }

    fn integrate_delta(offset: DataType, delta: DataType) -> DataType {
        let el0 = offset[3].wrapping_add(delta[0]);
        let el1 = el0.wrapping_add(delta[1]);
        let el2 = el1.wrapping_add(delta[2]);
        let el3 = el2.wrapping_add(delta[3]);
        [el0, el1, el2, el3]
    }

    pub(crate) fn add(left: DataType, right: DataType) -> DataType {
        [
            left[0].wrapping_add(right[0]),
            left[1].wrapping_add(right[1]),
            left[2].wrapping_add(right[2]),
            left[3].wrapping_add(right[3]),
        ]
    }

    pub(crate) fn sub(left: DataType, right: DataType) -> DataType {
        [
            left[0].wrapping_sub(right[0]),
            left[1].wrapping_sub(right[1]),
            left[2].wrapping_sub(right[2]),
            left[3].wrapping_sub(right[3]),
        ]
    }

    // The `allow(unused)` is here to put an attribute that has no effect.
    //
    // For other bitpacker, we enable specific CPU instruction set, but for the
    // scalar bitpacker none is required.
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
    SSE3,
    #[cfg(all(target_arch = "aarch64", target_endian = "little"))]
    NEON,
    Scalar,
}

/// `BitPacker4x` packs integers in groups of 4. This gives an opportunity
/// to leverage `SSE3` instructions to encode and decode the stream.
///
/// One block must contain `128 integers`.
#[derive(Clone, Copy)]
pub struct BitPacker4x(InstructionSet);

impl BitPacker4x {
    #[cfg(target_arch = "x86_64")]
    pub(crate) fn new_sse() -> Option<Self> {
        sse3::UnsafeBitPackerImpl::available().then_some(BitPacker4x(InstructionSet::SSE3))
    }

    #[cfg(not(target_arch = "x86_64"))]
    pub(crate) fn new_sse() -> Option<Self> {
        None
    }

    #[cfg(all(target_arch = "aarch64", target_endian = "little"))]
    pub(crate) fn new_neon() -> Option<Self> {
        neon::UnsafeBitPackerImpl::available().then_some(BitPacker4x(InstructionSet::NEON))
    }

    #[cfg(not(all(target_arch = "aarch64", target_endian = "little")))]
    pub(crate) fn new_neon() -> Option<Self> {
        None
    }

    pub(crate) fn new_scalar() -> Self {
        BitPacker4x(InstructionSet::Scalar)
    }
}

impl BitPacker for BitPacker4x {
    const BLOCK_LEN: usize = BLOCK_LEN;

    fn new() -> Self {
        Self::new_sse()
            .or_else(Self::new_neon)
            .unwrap_or_else(Self::new_scalar)
    }

    fn compress(&self, decompressed: &[u32], compressed: &mut [u8], num_bits: u8) -> usize {
        unsafe {
            match self.0 {
                #[cfg(target_arch = "x86_64")]
                InstructionSet::SSE3 => {
                    sse3::UnsafeBitPackerImpl::compress(decompressed, compressed, num_bits)
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
                InstructionSet::SSE3 => sse3::UnsafeBitPackerImpl::compress_sorted(
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
                InstructionSet::SSE3 => sse3::UnsafeBitPackerImpl::compress_strictly_sorted(
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
                InstructionSet::SSE3 => {
                    sse3::UnsafeBitPackerImpl::decompress(compressed, decompressed, num_bits)
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
                InstructionSet::SSE3 => sse3::UnsafeBitPackerImpl::decompress_strictly_sorted(
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
                InstructionSet::SSE3 => sse3::UnsafeBitPackerImpl::decompress_sorted(
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

    fn num_bits(&self, decompressed: &[u32]) -> u8 {
        unsafe {
            match self.0 {
                #[cfg(target_arch = "x86_64")]
                InstructionSet::SSE3 => sse3::UnsafeBitPackerImpl::num_bits(decompressed),
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
                InstructionSet::SSE3 => {
                    sse3::UnsafeBitPackerImpl::num_bits_sorted(initial, decompressed)
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
                InstructionSet::SSE3 => {
                    sse3::UnsafeBitPackerImpl::num_bits_strictly_sorted(initial, decompressed)
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

#[cfg(any(
    target_arch = "x86_64",
    all(target_arch = "aarch64", target_endian = "little")
))]
#[cfg(any())]
mod tests {
    use super::BLOCK_LEN;
    use super::scalar;
    use crate::bitpacker_internal::Available;
    use crate::tests::test_util_compatible;
    use crate::{BitPacker, BitPacker4x};

    #[cfg(target_arch = "x86_64")]
    #[test]
    fn test_compatible_sse3() {
        use super::sse3;
        if sse3::UnsafeBitPackerImpl::available() {
            test_util_compatible::<scalar::UnsafeBitPackerImpl, sse3::UnsafeBitPackerImpl>(
                BLOCK_LEN,
            );
        }
    }

    #[cfg(all(target_arch = "aarch64", target_endian = "little"))]
    #[test]
    fn test_compatible_neon() {
        use super::neon;
        if neon::UnsafeBitPackerImpl::available() {
            test_util_compatible::<scalar::UnsafeBitPackerImpl, neon::UnsafeBitPackerImpl>(
                BLOCK_LEN,
            );
        }
    }

    #[test]
    fn test_delta_bit_width_32() {
        let values = vec![i32::max_value() as u32 + 1; BitPacker4x::BLOCK_LEN];
        let bit_packer = BitPacker4x::new();
        let bit_width = bit_packer.num_bits_sorted(0, &values);
        assert_eq!(bit_width, 32);

        let mut block = vec![0u8; BitPacker4x::compressed_block_size(bit_width)];
        bit_packer.compress_sorted(0, &values, &mut block, bit_width);

        let mut decoded_values = vec![0x10101010; BitPacker4x::BLOCK_LEN];
        bit_packer.decompress_sorted(0, &block, &mut decoded_values, bit_width);

        assert_eq!(values, decoded_values);
    }

    #[test]
    fn test_bit_width_32() {
        let mut values = vec![i32::max_value() as u32 + 1; BitPacker4x::BLOCK_LEN];
        values[0] = 0;
        let bit_packer = BitPacker4x::new();
        let bit_width = bit_packer.num_bits(&values);
        assert_eq!(bit_width, 32);

        let mut block = vec![0u8; BitPacker4x::compressed_block_size(bit_width)];
        bit_packer.compress(&values, &mut block, bit_width);

        let mut decoded_values = vec![0x10101010; BitPacker4x::BLOCK_LEN];
        bit_packer.decompress(&block, &mut decoded_values, bit_width);

        assert_eq!(values, decoded_values);
    }
}

#[cfg(test)]
mod tests {
    use super::{BLOCK_LEN, BitPacker4x};
    use crate::bitpacker_internal::BitPacker;
    use bitpacking::{BitPacker as ExternalBitPacker, BitPacker4x as ExternalBitPacker4x};

    fn mask_for_width(width: u8) -> u32 {
        match width {
            0 => 0,
            32 => u32::MAX,
            _ => (1u32 << width) - 1,
        }
    }

    fn raw_values(width: u8) -> Vec<u32> {
        let mask = mask_for_width(width);
        (0..BLOCK_LEN)
            .map(|idx| ((idx * 17 + 3) as u32) & mask)
            .collect()
    }

    fn sorted_values(width: u8) -> (u32, Vec<u32>) {
        if width == 0 {
            return (11, vec![11; BLOCK_LEN]);
        }
        if width == 32 {
            return (0, vec![u32::MAX; BLOCK_LEN]);
        }

        let mask = mask_for_width(width).min(127);
        let initial = 11u32;
        let mut current = initial;
        let values = (0..BLOCK_LEN)
            .map(|idx| {
                current += (idx as u32 * 7 + 1) & mask;
                current
            })
            .collect();
        (initial, values)
    }

    fn strictly_sorted_values(width: u8) -> (Option<u32>, Vec<u32>) {
        let mask = mask_for_width(width).min(127);
        let mut current = 0u32;
        let values = (0..BLOCK_LEN)
            .map(|idx| {
                if idx == 0 {
                    current = 0;
                } else {
                    current += 1 + ((idx as u32 * 5) & mask);
                }
                current
            })
            .collect();
        (None, values)
    }

    #[test]
    fn scalar_backend_matches_external_bitpacker4x() {
        let scalar = BitPacker4x::new_scalar();
        let external = ExternalBitPacker4x::new();

        for width in 0..=32 {
            let values = raw_values(width);
            assert_eq!(scalar.num_bits(&values), external.num_bits(&values));

            let mut actual = vec![0u8; BitPacker4x::compressed_block_size(width)];
            let actual_len = scalar.compress(&values, &mut actual, width);

            let mut expected = vec![0u8; ExternalBitPacker4x::compressed_block_size(width)];
            let expected_len = external.compress(&values, &mut expected, width);

            assert_eq!(actual_len, expected_len);
            assert_eq!(actual, expected, "raw width {width}");

            let mut decoded = vec![0u32; BLOCK_LEN];
            assert_eq!(scalar.decompress(&actual, &mut decoded, width), actual_len);
            assert_eq!(decoded, values);

            let (initial, values) = sorted_values(width);
            assert_eq!(
                scalar.num_bits_sorted(initial, &values),
                external.num_bits_sorted(initial, &values)
            );

            let mut actual = vec![0u8; BitPacker4x::compressed_block_size(width)];
            let actual_len = scalar.compress_sorted(initial, &values, &mut actual, width);

            let mut expected = vec![0u8; ExternalBitPacker4x::compressed_block_size(width)];
            let expected_len = external.compress_sorted(initial, &values, &mut expected, width);

            assert_eq!(actual_len, expected_len);
            assert_eq!(actual, expected, "sorted width {width}");

            let mut decoded = vec![0u32; BLOCK_LEN];
            assert_eq!(
                scalar.decompress_sorted(initial, &actual, &mut decoded, width),
                actual_len
            );
            assert_eq!(decoded, values);
        }
    }

    #[test]
    fn scalar_backend_matches_external_strictly_sorted_bitpacker4x() {
        let scalar = BitPacker4x::new_scalar();
        let external = ExternalBitPacker4x::new();

        for width in 0..=16 {
            let (initial, values) = strictly_sorted_values(width);
            let num_bits = external.num_bits_strictly_sorted(initial, &values);
            assert_eq!(scalar.num_bits_strictly_sorted(initial, &values), num_bits);

            let mut actual = vec![0u8; BitPacker4x::compressed_block_size(num_bits)];
            let actual_len =
                scalar.compress_strictly_sorted(initial, &values, &mut actual, num_bits);

            let mut expected = vec![0u8; ExternalBitPacker4x::compressed_block_size(num_bits)];
            let expected_len =
                external.compress_strictly_sorted(initial, &values, &mut expected, num_bits);

            assert_eq!(actual_len, expected_len);
            assert_eq!(actual, expected, "strict width {width}");

            let mut decoded = vec![0u32; BLOCK_LEN];
            assert_eq!(
                scalar.decompress_strictly_sorted(initial, &actual, &mut decoded, num_bits),
                actual_len
            );
            assert_eq!(decoded, values);
        }
    }
}
