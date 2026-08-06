// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

// NOTICE:
// This file is a modification of the `fastlanes` crate: https://github.com/spiraldb/fastlanes
// It is modified to allow a rust stable build
//
// The original code can be accessed at
//      https://github.com/spiraldb/fastlanes/blob/8e0ff374f815d919d0c0ebdccf5ffd9e6dc7d663/src/bitpacking.rs
//      https://github.com/spiraldb/fastlanes/blob/8e0ff374f815d919d0c0ebdccf5ffd9e6dc7d663/src/lib.rs
//      https://github.com/spiraldb/fastlanes/blob/8e0ff374f815d919d0c0ebdccf5ffd9e6dc7d663/src/macros.rs
//
// The original code is licensed under the Apache Software License:
// https://github.com/spiraldb/fastlanes/blob/8e0ff374f815d919d0c0ebdccf5ffd9e6dc7d663/LICENSE

use arrayref::{array_mut_ref, array_ref};
use core::mem::size_of;

mod bitpacker_internal;

pub use bitpacker_internal::{BitPacker, BitPacker4x, BitPacker8x};

pub const FL_ORDER: [usize; 8] = [0, 4, 2, 6, 1, 5, 3, 7];

pub trait FastLanes: Sized + Copy {
    const T: usize = size_of::<Self>() * 8;
    const LANES: usize = 1024 / Self::T;
}

// Implement the trait for basic unsigned integer types
impl FastLanes for u8 {}
impl FastLanes for u16 {}
impl FastLanes for u32 {}
impl FastLanes for u64 {}

macro_rules! pack {
    ($T:ty, $W:expr, $packed:expr, $lane:expr, | $_1:tt $idx:ident | $($body:tt)*) => {
        macro_rules! __kernel__ {( $_1 $idx:ident ) => ( $($body)* )}
        {
            use paste::paste;

            // The number of bits of T.
            const T: usize = <$T>::T;

            #[inline(always)]
            fn index(row: usize, lane: usize) -> usize {
                let o = row / 8;
                let s = row % 8;
                (FL_ORDER[o] * 16) + (s * 128) + lane
            }

            if $W == 0 {
                // Nothing to do if W is 0, since the packed array is zero bytes.
            } else if $W == T {
                // Special case for W=T, we can just copy the input value directly to the packed value.
                paste!(seq_t!(row in $T {
                    let idx = index(row, $lane);
                    $packed[<$T>::LANES * row + $lane] = __kernel__!(idx);
                }));
            } else {
                // A mask of W bits.
                let mask: $T = (1 << $W) - 1;

                // First we loop over each lane in the virtual 1024 bit word.
                let mut tmp: $T = 0;

                // Loop over each of the rows of the lane.
                // Inlining this loop means all branches are known at compile time and
                // the code is auto-vectorized for SIMD execution.
                paste!(seq_t!(row in $T {
                    let idx = index(row, $lane);
                    let src = __kernel__!(idx);
                    let src = src & mask;

                    // Shift the src bits into their position in the tmp output variable.
                    if row == 0 {
                        tmp = src;
                    } else {
                        tmp |= src << (row * $W) % T;
                    }

                    // If the next packed position is after our current one, then we have filled
                    // the current output and we can write the packed value.
                    let curr_word: usize = (row * $W) / T;
                    let next_word: usize = ((row + 1) * $W) / T;

                    #[allow(unused_assignments)]
                    if next_word > curr_word {
                        $packed[<$T>::LANES * curr_word + $lane] = tmp;
                        let remaining_bits: usize = ((row + 1) * $W) % T;
                        // Keep the remaining bits for the next packed value.
                        tmp = src >> $W - remaining_bits;
                    }
                }));
            }
        }
    };
}

macro_rules! unpack {
    ($T:ty, $W:expr, $packed:expr, $lane:expr, | $_1:tt $idx:ident, $_2:tt $elem:ident | $($body:tt)*) => {
        macro_rules! __kernel__ {( $_1 $idx:ident, $_2 $elem:ident ) => ( $($body)* )}
        {
            use paste::paste;

            // The number of bits of T.
            const T: usize = <$T>::T;

            #[inline(always)]
            fn index(row: usize, lane: usize) -> usize {
                let o = row / 8;
                let s = row % 8;
                (FL_ORDER[o] * 16) + (s * 128) + lane
            }

            if $W == 0 {
                // Special case for W=0, we just need to zero the output.
                // We'll still respect the iteration order in case the kernel has side effects.
                paste!(seq_t!(row in $T {
                    let idx = index(row, $lane);
                    let zero: $T = 0;
                    __kernel__!(idx, zero);
                }));
            } else if $W == T {
                // Special case for W=T, we can just copy the packed value directly to the output.
                paste!(seq_t!(row in $T {
                    let idx = index(row, $lane);
                    let src = $packed[<$T>::LANES * row + $lane];
                    __kernel__!(idx, src);
                }));
            } else {
                #[inline]
                fn mask(width: usize) -> $T {
                    if width == T { <$T>::MAX } else { (1 << (width % T)) - 1 }
                }

                let mut src: $T = $packed[$lane];
                let mut tmp: $T;

                paste!(seq_t!(row in $T {
                    // Figure out the packed positions
                    let curr_word: usize = (row * $W) / T;
                    let next_word = ((row + 1) * $W) / T;

                    let shift = (row * $W) % T;

                    if next_word > curr_word {
                        // Consume some bits from the curr packed input, the remainder are in the next
                        // packed input value
                        let remaining_bits = ((row + 1) * $W) % T;
                        let current_bits = $W - remaining_bits;
                        tmp = (src >> shift) & mask(current_bits);

                        if next_word < $W {
                            // Load the next packed value
                            src = $packed[<$T>::LANES * next_word + $lane];
                            // Consume the remaining bits from the next input value.
                            tmp |= (src & mask(remaining_bits)) << current_bits;
                        }
                    } else {
                        // Otherwise, just grab W bits from the src value
                        tmp = (src >> shift) & mask($W);
                    }

                    // Write out the unpacked value
                    let idx = index(row, $lane);
                    __kernel__!(idx, tmp);
                }));
            }
        }
    };
}

// Macro for repeating a code block bit_size_of::<T> times.
macro_rules! seq_t {
    ($ident:ident in u8 $body:tt) => {seq_macro::seq!($ident in 0..8 $body)};
    ($ident:ident in u16 $body:tt) => {seq_macro::seq!($ident in 0..16 $body)};
    ($ident:ident in u32 $body:tt) => {seq_macro::seq!($ident in 0..32 $body)};
    ($ident:ident in u64 $body:tt) => {seq_macro::seq!($ident in 0..64 $body)};
}

/// `BitPack` into a compile-time known bit-width.
pub trait BitPacking: FastLanes {
    /// Packs 1024 elements into `W` bits each, where `W` is runtime-known instead of
    /// compile-time known.
    ///
    /// # Safety
    /// The input slice must be of exactly length 1024. The output slice must be of length
    /// `1024 * W / T`, where `T` is the bit-width of Self and `W` is the packed width.
    /// These lengths are checked only with `debug_assert` (i.e., not checked on release builds).
    unsafe fn unchecked_pack(width: usize, input: &[Self], output: &mut [Self]);

    /// Unpacks 1024 elements from `W` bits each, where `W` is runtime-known instead of
    /// compile-time known.
    ///
    /// # Safety
    /// The input slice must be of length `1024 * W / T`, where `T` is the bit-width of Self and `W`
    /// is the packed width. The output slice must be of exactly length 1024.
    /// These lengths are checked only with `debug_assert` (i.e., not checked on release builds).
    unsafe fn unchecked_unpack(width: usize, input: &[Self], output: &mut [Self]);
}

impl BitPacking for u8 {
    unsafe fn unchecked_pack(width: usize, input: &[Self], output: &mut [Self]) {
        let packed_len = 128 * width / size_of::<Self>();
        debug_assert_eq!(
            output.len(),
            packed_len,
            "Output buffer must be of size 1024 * W / T"
        );
        debug_assert_eq!(input.len(), 1024, "Input buffer must be of size 1024");
        debug_assert!(
            width <= Self::T,
            "Width must be less than or equal to {}",
            Self::T
        );

        match width {
            0 => {
                // Nothing to write when width is zero.
            }
            1 => pack_8_1(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 / 8],
            ),
            2 => pack_8_2(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 2 / 8],
            ),
            3 => pack_8_3(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 3 / 8],
            ),
            4 => pack_8_4(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 4 / 8],
            ),
            5 => pack_8_5(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 5 / 8],
            ),
            6 => pack_8_6(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 6 / 8],
            ),
            7 => pack_8_7(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 7 / 8],
            ),
            8 => pack_8_8(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 8 / 8],
            ),

            _ => unreachable!("Unsupported width: {}", width),
        }
    }

    unsafe fn unchecked_unpack(width: usize, input: &[Self], output: &mut [Self]) {
        let packed_len = 128 * width / size_of::<Self>();
        debug_assert_eq!(
            input.len(),
            packed_len,
            "Input buffer must be of size 1024 * W / T"
        );
        debug_assert_eq!(output.len(), 1024, "Output buffer must be of size 1024");
        debug_assert!(
            width <= Self::T,
            "Width must be less than or equal to {}",
            Self::T
        );

        match width {
            0 => {
                // A zero-width packed chunk implies all zeros.
                output.fill(0);
            }
            1 => unpack_8_1(
                array_ref![input, 0, 1024 / 8],
                array_mut_ref![output, 0, 1024],
            ),
            2 => unpack_8_2(
                array_ref![input, 0, 1024 * 2 / 8],
                array_mut_ref![output, 0, 1024],
            ),
            3 => unpack_8_3(
                array_ref![input, 0, 1024 * 3 / 8],
                array_mut_ref![output, 0, 1024],
            ),
            4 => unpack_8_4(
                array_ref![input, 0, 1024 * 4 / 8],
                array_mut_ref![output, 0, 1024],
            ),
            5 => unpack_8_5(
                array_ref![input, 0, 1024 * 5 / 8],
                array_mut_ref![output, 0, 1024],
            ),
            6 => unpack_8_6(
                array_ref![input, 0, 1024 * 6 / 8],
                array_mut_ref![output, 0, 1024],
            ),
            7 => unpack_8_7(
                array_ref![input, 0, 1024 * 7 / 8],
                array_mut_ref![output, 0, 1024],
            ),
            8 => unpack_8_8(
                array_ref![input, 0, 1024 * 8 / 8],
                array_mut_ref![output, 0, 1024],
            ),

            _ => unreachable!("Unsupported width: {}", width),
        }
    }
}

impl BitPacking for u16 {
    unsafe fn unchecked_pack(width: usize, input: &[Self], output: &mut [Self]) {
        let packed_len = 128 * width / size_of::<Self>();
        debug_assert_eq!(
            output.len(),
            packed_len,
            "Output buffer must be of size 1024 * W / T"
        );
        debug_assert_eq!(input.len(), 1024, "Input buffer must be of size 1024");
        debug_assert!(
            width <= Self::T,
            "Width must be less than or equal to {}",
            Self::T
        );

        match width {
            0 => {
                // Nothing to write when width is zero.
            }
            1 => pack_16_1(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 / 16],
            ),
            2 => pack_16_2(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 2 / 16],
            ),
            3 => pack_16_3(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 3 / 16],
            ),
            4 => pack_16_4(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 4 / 16],
            ),
            5 => pack_16_5(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 5 / 16],
            ),
            6 => pack_16_6(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 6 / 16],
            ),
            7 => pack_16_7(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 7 / 16],
            ),
            8 => pack_16_8(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 8 / 16],
            ),
            9 => pack_16_9(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 9 / 16],
            ),

            10 => pack_16_10(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 10 / 16],
            ),
            11 => pack_16_11(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 11 / 16],
            ),
            12 => pack_16_12(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 12 / 16],
            ),
            13 => pack_16_13(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 13 / 16],
            ),
            14 => pack_16_14(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 14 / 16],
            ),
            15 => pack_16_15(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 15 / 16],
            ),
            16 => pack_16_16(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 16 / 16],
            ),

            _ => unreachable!("Unsupported width: {}", width),
        }
    }

    unsafe fn unchecked_unpack(width: usize, input: &[Self], output: &mut [Self]) {
        let packed_len = 128 * width / size_of::<Self>();
        debug_assert_eq!(
            input.len(),
            packed_len,
            "Input buffer must be of size 1024 * W / T"
        );
        debug_assert_eq!(output.len(), 1024, "Output buffer must be of size 1024");
        debug_assert!(
            width <= Self::T,
            "Width must be less than or equal to {}",
            Self::T
        );

        match width {
            0 => {
                output.fill(0);
            }
            1 => unpack_16_1(
                array_ref![input, 0, 1024 / 16],
                array_mut_ref![output, 0, 1024],
            ),
            2 => unpack_16_2(
                array_ref![input, 0, 1024 * 2 / 16],
                array_mut_ref![output, 0, 1024],
            ),
            3 => unpack_16_3(
                array_ref![input, 0, 1024 * 3 / 16],
                array_mut_ref![output, 0, 1024],
            ),
            4 => unpack_16_4(
                array_ref![input, 0, 1024 * 4 / 16],
                array_mut_ref![output, 0, 1024],
            ),
            5 => unpack_16_5(
                array_ref![input, 0, 1024 * 5 / 16],
                array_mut_ref![output, 0, 1024],
            ),
            6 => unpack_16_6(
                array_ref![input, 0, 1024 * 6 / 16],
                array_mut_ref![output, 0, 1024],
            ),
            7 => unpack_16_7(
                array_ref![input, 0, 1024 * 7 / 16],
                array_mut_ref![output, 0, 1024],
            ),
            8 => unpack_16_8(
                array_ref![input, 0, 1024 * 8 / 16],
                array_mut_ref![output, 0, 1024],
            ),
            9 => unpack_16_9(
                array_ref![input, 0, 1024 * 9 / 16],
                array_mut_ref![output, 0, 1024],
            ),

            10 => unpack_16_10(
                array_ref![input, 0, 1024 * 10 / 16],
                array_mut_ref![output, 0, 1024],
            ),
            11 => unpack_16_11(
                array_ref![input, 0, 1024 * 11 / 16],
                array_mut_ref![output, 0, 1024],
            ),
            12 => unpack_16_12(
                array_ref![input, 0, 1024 * 12 / 16],
                array_mut_ref![output, 0, 1024],
            ),
            13 => unpack_16_13(
                array_ref![input, 0, 1024 * 13 / 16],
                array_mut_ref![output, 0, 1024],
            ),
            14 => unpack_16_14(
                array_ref![input, 0, 1024 * 14 / 16],
                array_mut_ref![output, 0, 1024],
            ),
            15 => unpack_16_15(
                array_ref![input, 0, 1024 * 15 / 16],
                array_mut_ref![output, 0, 1024],
            ),
            16 => unpack_16_16(
                array_ref![input, 0, 1024 * 16 / 16],
                array_mut_ref![output, 0, 1024],
            ),

            _ => unreachable!("Unsupported width: {}", width),
        }
    }
}

impl BitPacking for u32 {
    unsafe fn unchecked_pack(width: usize, input: &[Self], output: &mut [Self]) {
        let packed_len = 128 * width / size_of::<Self>();
        debug_assert_eq!(
            output.len(),
            packed_len,
            "Output buffer must be of size 1024 * W / T"
        );
        debug_assert_eq!(input.len(), 1024, "Input buffer must be of size 1024");
        debug_assert!(
            width <= Self::T,
            "Width must be less than or equal to {}",
            Self::T
        );

        match width {
            0 => {
                // Nothing to write when width is zero.
            }
            1 => pack_32_1(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 / 32],
            ),
            2 => pack_32_2(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 2 / 32],
            ),
            3 => pack_32_3(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 3 / 32],
            ),
            4 => pack_32_4(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 4 / 32],
            ),
            5 => pack_32_5(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 5 / 32],
            ),
            6 => pack_32_6(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 6 / 32],
            ),
            7 => pack_32_7(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 7 / 32],
            ),
            8 => pack_32_8(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 8 / 32],
            ),
            9 => pack_32_9(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 9 / 32],
            ),

            10 => pack_32_10(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 10 / 32],
            ),
            11 => pack_32_11(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 11 / 32],
            ),
            12 => pack_32_12(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 12 / 32],
            ),
            13 => pack_32_13(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 13 / 32],
            ),
            14 => pack_32_14(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 14 / 32],
            ),
            15 => pack_32_15(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 15 / 32],
            ),
            16 => pack_32_16(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 16 / 32],
            ),
            17 => pack_32_17(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 17 / 32],
            ),
            18 => pack_32_18(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 18 / 32],
            ),
            19 => pack_32_19(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 19 / 32],
            ),

            20 => pack_32_20(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 20 / 32],
            ),
            21 => pack_32_21(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 21 / 32],
            ),
            22 => pack_32_22(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 22 / 32],
            ),
            23 => pack_32_23(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 23 / 32],
            ),
            24 => pack_32_24(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 24 / 32],
            ),
            25 => pack_32_25(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 25 / 32],
            ),
            26 => pack_32_26(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 26 / 32],
            ),
            27 => pack_32_27(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 27 / 32],
            ),
            28 => pack_32_28(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 28 / 32],
            ),
            29 => pack_32_29(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 29 / 32],
            ),

            30 => pack_32_30(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 30 / 32],
            ),
            31 => pack_32_31(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 31 / 32],
            ),
            32 => pack_32_32(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 32 / 32],
            ),

            _ => unreachable!("Unsupported width: {}", width),
        }
    }

    unsafe fn unchecked_unpack(width: usize, input: &[Self], output: &mut [Self]) {
        let packed_len = 128 * width / size_of::<Self>();
        debug_assert_eq!(
            input.len(),
            packed_len,
            "Input buffer must be of size 1024 * W / T"
        );
        debug_assert_eq!(output.len(), 1024, "Output buffer must be of size 1024");
        debug_assert!(
            width <= Self::T,
            "Width must be less than or equal to {}",
            Self::T
        );

        match width {
            0 => {
                output.fill(0);
            }
            1 => unpack_32_1(
                array_ref![input, 0, 1024 / 32],
                array_mut_ref![output, 0, 1024],
            ),
            2 => unpack_32_2(
                array_ref![input, 0, 1024 * 2 / 32],
                array_mut_ref![output, 0, 1024],
            ),
            3 => unpack_32_3(
                array_ref![input, 0, 1024 * 3 / 32],
                array_mut_ref![output, 0, 1024],
            ),
            4 => unpack_32_4(
                array_ref![input, 0, 1024 * 4 / 32],
                array_mut_ref![output, 0, 1024],
            ),
            5 => unpack_32_5(
                array_ref![input, 0, 1024 * 5 / 32],
                array_mut_ref![output, 0, 1024],
            ),
            6 => unpack_32_6(
                array_ref![input, 0, 1024 * 6 / 32],
                array_mut_ref![output, 0, 1024],
            ),
            7 => unpack_32_7(
                array_ref![input, 0, 1024 * 7 / 32],
                array_mut_ref![output, 0, 1024],
            ),
            8 => unpack_32_8(
                array_ref![input, 0, 1024 * 8 / 32],
                array_mut_ref![output, 0, 1024],
            ),
            9 => unpack_32_9(
                array_ref![input, 0, 1024 * 9 / 32],
                array_mut_ref![output, 0, 1024],
            ),

            10 => unpack_32_10(
                array_ref![input, 0, 1024 * 10 / 32],
                array_mut_ref![output, 0, 1024],
            ),
            11 => unpack_32_11(
                array_ref![input, 0, 1024 * 11 / 32],
                array_mut_ref![output, 0, 1024],
            ),
            12 => unpack_32_12(
                array_ref![input, 0, 1024 * 12 / 32],
                array_mut_ref![output, 0, 1024],
            ),
            13 => unpack_32_13(
                array_ref![input, 0, 1024 * 13 / 32],
                array_mut_ref![output, 0, 1024],
            ),
            14 => unpack_32_14(
                array_ref![input, 0, 1024 * 14 / 32],
                array_mut_ref![output, 0, 1024],
            ),
            15 => unpack_32_15(
                array_ref![input, 0, 1024 * 15 / 32],
                array_mut_ref![output, 0, 1024],
            ),
            16 => unpack_32_16(
                array_ref![input, 0, 1024 * 16 / 32],
                array_mut_ref![output, 0, 1024],
            ),
            17 => unpack_32_17(
                array_ref![input, 0, 1024 * 17 / 32],
                array_mut_ref![output, 0, 1024],
            ),
            18 => unpack_32_18(
                array_ref![input, 0, 1024 * 18 / 32],
                array_mut_ref![output, 0, 1024],
            ),
            19 => unpack_32_19(
                array_ref![input, 0, 1024 * 19 / 32],
                array_mut_ref![output, 0, 1024],
            ),

            20 => unpack_32_20(
                array_ref![input, 0, 1024 * 20 / 32],
                array_mut_ref![output, 0, 1024],
            ),
            21 => unpack_32_21(
                array_ref![input, 0, 1024 * 21 / 32],
                array_mut_ref![output, 0, 1024],
            ),
            22 => unpack_32_22(
                array_ref![input, 0, 1024 * 22 / 32],
                array_mut_ref![output, 0, 1024],
            ),
            23 => unpack_32_23(
                array_ref![input, 0, 1024 * 23 / 32],
                array_mut_ref![output, 0, 1024],
            ),
            24 => unpack_32_24(
                array_ref![input, 0, 1024 * 24 / 32],
                array_mut_ref![output, 0, 1024],
            ),
            25 => unpack_32_25(
                array_ref![input, 0, 1024 * 25 / 32],
                array_mut_ref![output, 0, 1024],
            ),
            26 => unpack_32_26(
                array_ref![input, 0, 1024 * 26 / 32],
                array_mut_ref![output, 0, 1024],
            ),
            27 => unpack_32_27(
                array_ref![input, 0, 1024 * 27 / 32],
                array_mut_ref![output, 0, 1024],
            ),
            28 => unpack_32_28(
                array_ref![input, 0, 1024 * 28 / 32],
                array_mut_ref![output, 0, 1024],
            ),
            29 => unpack_32_29(
                array_ref![input, 0, 1024 * 29 / 32],
                array_mut_ref![output, 0, 1024],
            ),

            30 => unpack_32_30(
                array_ref![input, 0, 1024 * 30 / 32],
                array_mut_ref![output, 0, 1024],
            ),
            31 => unpack_32_31(
                array_ref![input, 0, 1024 * 31 / 32],
                array_mut_ref![output, 0, 1024],
            ),
            32 => unpack_32_32(
                array_ref![input, 0, 1024 * 32 / 32],
                array_mut_ref![output, 0, 1024],
            ),

            _ => unreachable!("Unsupported width: {}", width),
        }
    }
}

impl BitPacking for u64 {
    unsafe fn unchecked_pack(width: usize, input: &[Self], output: &mut [Self]) {
        let packed_len = 128 * width / size_of::<Self>();
        debug_assert_eq!(
            output.len(),
            packed_len,
            "Output buffer must be of size 1024 * W / T"
        );
        debug_assert_eq!(input.len(), 1024, "Input buffer must be of size 1024");
        debug_assert!(
            width <= Self::T,
            "Width must be less than or equal to {}",
            Self::T
        );

        match width {
            0 => {
                // Nothing to write when width is zero.
            }
            1 => pack_64_1(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 / 64],
            ),
            2 => pack_64_2(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 2 / 64],
            ),
            3 => pack_64_3(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 3 / 64],
            ),
            4 => pack_64_4(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 4 / 64],
            ),
            5 => pack_64_5(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 5 / 64],
            ),
            6 => pack_64_6(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 6 / 64],
            ),
            7 => pack_64_7(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 7 / 64],
            ),
            8 => pack_64_8(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 8 / 64],
            ),
            9 => pack_64_9(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 9 / 64],
            ),

            10 => pack_64_10(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 10 / 64],
            ),
            11 => pack_64_11(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 11 / 64],
            ),
            12 => pack_64_12(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 12 / 64],
            ),
            13 => pack_64_13(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 13 / 64],
            ),
            14 => pack_64_14(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 14 / 64],
            ),
            15 => pack_64_15(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 15 / 64],
            ),
            16 => pack_64_16(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 16 / 64],
            ),
            17 => pack_64_17(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 17 / 64],
            ),
            18 => pack_64_18(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 18 / 64],
            ),
            19 => pack_64_19(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 19 / 64],
            ),

            20 => pack_64_20(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 20 / 64],
            ),
            21 => pack_64_21(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 21 / 64],
            ),
            22 => pack_64_22(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 22 / 64],
            ),
            23 => pack_64_23(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 23 / 64],
            ),
            24 => pack_64_24(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 24 / 64],
            ),
            25 => pack_64_25(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 25 / 64],
            ),
            26 => pack_64_26(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 26 / 64],
            ),
            27 => pack_64_27(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 27 / 64],
            ),
            28 => pack_64_28(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 28 / 64],
            ),
            29 => pack_64_29(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 29 / 64],
            ),

            30 => pack_64_30(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 30 / 64],
            ),
            31 => pack_64_31(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 31 / 64],
            ),
            32 => pack_64_32(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 32 / 64],
            ),
            33 => pack_64_33(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 33 / 64],
            ),
            34 => pack_64_34(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 34 / 64],
            ),
            35 => pack_64_35(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 35 / 64],
            ),
            36 => pack_64_36(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 36 / 64],
            ),
            37 => pack_64_37(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 37 / 64],
            ),
            38 => pack_64_38(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 38 / 64],
            ),
            39 => pack_64_39(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 39 / 64],
            ),

            40 => pack_64_40(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 40 / 64],
            ),
            41 => pack_64_41(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 41 / 64],
            ),
            42 => pack_64_42(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 42 / 64],
            ),
            43 => pack_64_43(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 43 / 64],
            ),
            44 => pack_64_44(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 44 / 64],
            ),
            45 => pack_64_45(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 45 / 64],
            ),
            46 => pack_64_46(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 46 / 64],
            ),
            47 => pack_64_47(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 47 / 64],
            ),
            48 => pack_64_48(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 48 / 64],
            ),
            49 => pack_64_49(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 49 / 64],
            ),

            50 => pack_64_50(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 50 / 64],
            ),
            51 => pack_64_51(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 51 / 64],
            ),
            52 => pack_64_52(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 52 / 64],
            ),
            53 => pack_64_53(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 53 / 64],
            ),
            54 => pack_64_54(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 54 / 64],
            ),
            55 => pack_64_55(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 55 / 64],
            ),
            56 => pack_64_56(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 56 / 64],
            ),
            57 => pack_64_57(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 57 / 64],
            ),
            58 => pack_64_58(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 58 / 64],
            ),
            59 => pack_64_59(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 59 / 64],
            ),

            60 => pack_64_60(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 60 / 64],
            ),
            61 => pack_64_61(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 61 / 64],
            ),
            62 => pack_64_62(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 62 / 64],
            ),
            63 => pack_64_63(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 63 / 64],
            ),
            64 => pack_64_64(
                array_ref![input, 0, 1024],
                array_mut_ref![output, 0, 1024 * 64 / 64],
            ),

            _ => unreachable!("Unsupported width: {}", width),
        }
    }

    unsafe fn unchecked_unpack(width: usize, input: &[Self], output: &mut [Self]) {
        let packed_len = 128 * width / size_of::<Self>();
        debug_assert_eq!(
            input.len(),
            packed_len,
            "Input buffer must be of size 1024 * W / T"
        );
        debug_assert_eq!(output.len(), 1024, "Output buffer must be of size 1024");
        debug_assert!(
            width <= Self::T,
            "Width must be less than or equal to {}",
            Self::T
        );

        match width {
            0 => {
                output.fill(0);
            }
            1 => unpack_64_1(
                array_ref![input, 0, 1024 / 64],
                array_mut_ref![output, 0, 1024],
            ),
            2 => unpack_64_2(
                array_ref![input, 0, 1024 * 2 / 64],
                array_mut_ref![output, 0, 1024],
            ),
            3 => unpack_64_3(
                array_ref![input, 0, 1024 * 3 / 64],
                array_mut_ref![output, 0, 1024],
            ),
            4 => unpack_64_4(
                array_ref![input, 0, 1024 * 4 / 64],
                array_mut_ref![output, 0, 1024],
            ),
            5 => unpack_64_5(
                array_ref![input, 0, 1024 * 5 / 64],
                array_mut_ref![output, 0, 1024],
            ),
            6 => unpack_64_6(
                array_ref![input, 0, 1024 * 6 / 64],
                array_mut_ref![output, 0, 1024],
            ),
            7 => unpack_64_7(
                array_ref![input, 0, 1024 * 7 / 64],
                array_mut_ref![output, 0, 1024],
            ),
            8 => unpack_64_8(
                array_ref![input, 0, 1024 * 8 / 64],
                array_mut_ref![output, 0, 1024],
            ),
            9 => unpack_64_9(
                array_ref![input, 0, 1024 * 9 / 64],
                array_mut_ref![output, 0, 1024],
            ),

            10 => unpack_64_10(
                array_ref![input, 0, 1024 * 10 / 64],
                array_mut_ref![output, 0, 1024],
            ),
            11 => unpack_64_11(
                array_ref![input, 0, 1024 * 11 / 64],
                array_mut_ref![output, 0, 1024],
            ),
            12 => unpack_64_12(
                array_ref![input, 0, 1024 * 12 / 64],
                array_mut_ref![output, 0, 1024],
            ),
            13 => unpack_64_13(
                array_ref![input, 0, 1024 * 13 / 64],
                array_mut_ref![output, 0, 1024],
            ),
            14 => unpack_64_14(
                array_ref![input, 0, 1024 * 14 / 64],
                array_mut_ref![output, 0, 1024],
            ),
            15 => unpack_64_15(
                array_ref![input, 0, 1024 * 15 / 64],
                array_mut_ref![output, 0, 1024],
            ),
            16 => unpack_64_16(
                array_ref![input, 0, 1024 * 16 / 64],
                array_mut_ref![output, 0, 1024],
            ),
            17 => unpack_64_17(
                array_ref![input, 0, 1024 * 17 / 64],
                array_mut_ref![output, 0, 1024],
            ),
            18 => unpack_64_18(
                array_ref![input, 0, 1024 * 18 / 64],
                array_mut_ref![output, 0, 1024],
            ),
            19 => unpack_64_19(
                array_ref![input, 0, 1024 * 19 / 64],
                array_mut_ref![output, 0, 1024],
            ),

            20 => unpack_64_20(
                array_ref![input, 0, 1024 * 20 / 64],
                array_mut_ref![output, 0, 1024],
            ),
            21 => unpack_64_21(
                array_ref![input, 0, 1024 * 21 / 64],
                array_mut_ref![output, 0, 1024],
            ),
            22 => unpack_64_22(
                array_ref![input, 0, 1024 * 22 / 64],
                array_mut_ref![output, 0, 1024],
            ),
            23 => unpack_64_23(
                array_ref![input, 0, 1024 * 23 / 64],
                array_mut_ref![output, 0, 1024],
            ),
            24 => unpack_64_24(
                array_ref![input, 0, 1024 * 24 / 64],
                array_mut_ref![output, 0, 1024],
            ),
            25 => unpack_64_25(
                array_ref![input, 0, 1024 * 25 / 64],
                array_mut_ref![output, 0, 1024],
            ),
            26 => unpack_64_26(
                array_ref![input, 0, 1024 * 26 / 64],
                array_mut_ref![output, 0, 1024],
            ),
            27 => unpack_64_27(
                array_ref![input, 0, 1024 * 27 / 64],
                array_mut_ref![output, 0, 1024],
            ),
            28 => unpack_64_28(
                array_ref![input, 0, 1024 * 28 / 64],
                array_mut_ref![output, 0, 1024],
            ),
            29 => unpack_64_29(
                array_ref![input, 0, 1024 * 29 / 64],
                array_mut_ref![output, 0, 1024],
            ),

            30 => unpack_64_30(
                array_ref![input, 0, 1024 * 30 / 64],
                array_mut_ref![output, 0, 1024],
            ),
            31 => unpack_64_31(
                array_ref![input, 0, 1024 * 31 / 64],
                array_mut_ref![output, 0, 1024],
            ),
            32 => unpack_64_32(
                array_ref![input, 0, 1024 * 32 / 64],
                array_mut_ref![output, 0, 1024],
            ),
            33 => unpack_64_33(
                array_ref![input, 0, 1024 * 33 / 64],
                array_mut_ref![output, 0, 1024],
            ),
            34 => unpack_64_34(
                array_ref![input, 0, 1024 * 34 / 64],
                array_mut_ref![output, 0, 1024],
            ),
            35 => unpack_64_35(
                array_ref![input, 0, 1024 * 35 / 64],
                array_mut_ref![output, 0, 1024],
            ),
            36 => unpack_64_36(
                array_ref![input, 0, 1024 * 36 / 64],
                array_mut_ref![output, 0, 1024],
            ),
            37 => unpack_64_37(
                array_ref![input, 0, 1024 * 37 / 64],
                array_mut_ref![output, 0, 1024],
            ),
            38 => unpack_64_38(
                array_ref![input, 0, 1024 * 38 / 64],
                array_mut_ref![output, 0, 1024],
            ),
            39 => unpack_64_39(
                array_ref![input, 0, 1024 * 39 / 64],
                array_mut_ref![output, 0, 1024],
            ),

            40 => unpack_64_40(
                array_ref![input, 0, 1024 * 40 / 64],
                array_mut_ref![output, 0, 1024],
            ),
            41 => unpack_64_41(
                array_ref![input, 0, 1024 * 41 / 64],
                array_mut_ref![output, 0, 1024],
            ),
            42 => unpack_64_42(
                array_ref![input, 0, 1024 * 42 / 64],
                array_mut_ref![output, 0, 1024],
            ),
            43 => unpack_64_43(
                array_ref![input, 0, 1024 * 43 / 64],
                array_mut_ref![output, 0, 1024],
            ),
            44 => unpack_64_44(
                array_ref![input, 0, 1024 * 44 / 64],
                array_mut_ref![output, 0, 1024],
            ),
            45 => unpack_64_45(
                array_ref![input, 0, 1024 * 45 / 64],
                array_mut_ref![output, 0, 1024],
            ),
            46 => unpack_64_46(
                array_ref![input, 0, 1024 * 46 / 64],
                array_mut_ref![output, 0, 1024],
            ),
            47 => unpack_64_47(
                array_ref![input, 0, 1024 * 47 / 64],
                array_mut_ref![output, 0, 1024],
            ),
            48 => unpack_64_48(
                array_ref![input, 0, 1024 * 48 / 64],
                array_mut_ref![output, 0, 1024],
            ),
            49 => unpack_64_49(
                array_ref![input, 0, 1024 * 49 / 64],
                array_mut_ref![output, 0, 1024],
            ),

            50 => unpack_64_50(
                array_ref![input, 0, 1024 * 50 / 64],
                array_mut_ref![output, 0, 1024],
            ),
            51 => unpack_64_51(
                array_ref![input, 0, 1024 * 51 / 64],
                array_mut_ref![output, 0, 1024],
            ),
            52 => unpack_64_52(
                array_ref![input, 0, 1024 * 52 / 64],
                array_mut_ref![output, 0, 1024],
            ),
            53 => unpack_64_53(
                array_ref![input, 0, 1024 * 53 / 64],
                array_mut_ref![output, 0, 1024],
            ),
            54 => unpack_64_54(
                array_ref![input, 0, 1024 * 54 / 64],
                array_mut_ref![output, 0, 1024],
            ),
            55 => unpack_64_55(
                array_ref![input, 0, 1024 * 55 / 64],
                array_mut_ref![output, 0, 1024],
            ),
            56 => unpack_64_56(
                array_ref![input, 0, 1024 * 56 / 64],
                array_mut_ref![output, 0, 1024],
            ),
            57 => unpack_64_57(
                array_ref![input, 0, 1024 * 57 / 64],
                array_mut_ref![output, 0, 1024],
            ),
            58 => unpack_64_58(
                array_ref![input, 0, 1024 * 58 / 64],
                array_mut_ref![output, 0, 1024],
            ),
            59 => unpack_64_59(
                array_ref![input, 0, 1024 * 59 / 64],
                array_mut_ref![output, 0, 1024],
            ),

            60 => unpack_64_60(
                array_ref![input, 0, 1024 * 60 / 64],
                array_mut_ref![output, 0, 1024],
            ),
            61 => unpack_64_61(
                array_ref![input, 0, 1024 * 61 / 64],
                array_mut_ref![output, 0, 1024],
            ),
            62 => unpack_64_62(
                array_ref![input, 0, 1024 * 62 / 64],
                array_mut_ref![output, 0, 1024],
            ),
            63 => unpack_64_63(
                array_ref![input, 0, 1024 * 63 / 64],
                array_mut_ref![output, 0, 1024],
            ),
            64 => unpack_64_64(
                array_ref![input, 0, 1024 * 64 / 64],
                array_mut_ref![output, 0, 1024],
            ),

            _ => unreachable!("Unsupported width: {}", width),
        }
    }
}

macro_rules! unpack_8 {
    ($name:ident, $bits:expr) => {
        fn $name(input: &[u8; 1024 * $bits / u8::T], output: &mut [u8; 1024]) {
            for lane in 0..u8::LANES {
                unpack!(u8, $bits, input, lane, |$idx, $elem| {
                    output[$idx] = $elem;
                });
            }
        }
    };
}

unpack_8!(unpack_8_1, 1);
unpack_8!(unpack_8_2, 2);
unpack_8!(unpack_8_3, 3);
unpack_8!(unpack_8_4, 4);
unpack_8!(unpack_8_5, 5);
unpack_8!(unpack_8_6, 6);
unpack_8!(unpack_8_7, 7);
unpack_8!(unpack_8_8, 8);

macro_rules! pack_8 {
    ($name:ident, $bits:expr) => {
        fn $name(input: &[u8; 1024], output: &mut [u8; 1024 * $bits / u8::T]) {
            for lane in 0..u8::LANES {
                pack!(u8, $bits, output, lane, |$idx| { input[$idx] });
            }
        }
    };
}
pack_8!(pack_8_1, 1);
pack_8!(pack_8_2, 2);
pack_8!(pack_8_3, 3);
pack_8!(pack_8_4, 4);
pack_8!(pack_8_5, 5);
pack_8!(pack_8_6, 6);
pack_8!(pack_8_7, 7);
pack_8!(pack_8_8, 8);

macro_rules! unpack_16 {
    ($name:ident, $bits:expr) => {
        fn $name(input: &[u16; 1024 * $bits / u16::T], output: &mut [u16; 1024]) {
            for lane in 0..u16::LANES {
                unpack!(u16, $bits, input, lane, |$idx, $elem| {
                    output[$idx] = $elem;
                });
            }
        }
    };
}

unpack_16!(unpack_16_1, 1);
unpack_16!(unpack_16_2, 2);
unpack_16!(unpack_16_3, 3);
unpack_16!(unpack_16_4, 4);
unpack_16!(unpack_16_5, 5);
unpack_16!(unpack_16_6, 6);
unpack_16!(unpack_16_7, 7);
unpack_16!(unpack_16_8, 8);
unpack_16!(unpack_16_9, 9);
unpack_16!(unpack_16_10, 10);
unpack_16!(unpack_16_11, 11);
unpack_16!(unpack_16_12, 12);
unpack_16!(unpack_16_13, 13);
unpack_16!(unpack_16_14, 14);
unpack_16!(unpack_16_15, 15);
unpack_16!(unpack_16_16, 16);

macro_rules! pack_16 {
    ($name:ident, $bits:expr) => {
        fn $name(input: &[u16; 1024], output: &mut [u16; 1024 * $bits / u16::T]) {
            for lane in 0..u16::LANES {
                pack!(u16, $bits, output, lane, |$idx| { input[$idx] });
            }
        }
    };
}

pack_16!(pack_16_1, 1);
pack_16!(pack_16_2, 2);
pack_16!(pack_16_3, 3);
pack_16!(pack_16_4, 4);
pack_16!(pack_16_5, 5);
pack_16!(pack_16_6, 6);
pack_16!(pack_16_7, 7);
pack_16!(pack_16_8, 8);
pack_16!(pack_16_9, 9);
pack_16!(pack_16_10, 10);
pack_16!(pack_16_11, 11);
pack_16!(pack_16_12, 12);
pack_16!(pack_16_13, 13);
pack_16!(pack_16_14, 14);
pack_16!(pack_16_15, 15);
pack_16!(pack_16_16, 16);

macro_rules! unpack_32 {
    ($name:ident, $bit_width:expr) => {
        fn $name(input: &[u32; 1024 * $bit_width / u32::T], output: &mut [u32; 1024]) {
            for lane in 0..u32::LANES {
                unpack!(u32, $bit_width, input, lane, |$idx, $elem| {
                    output[$idx] = $elem
                });
            }
        }
    };
}

unpack_32!(unpack_32_1, 1);
unpack_32!(unpack_32_2, 2);
unpack_32!(unpack_32_3, 3);
unpack_32!(unpack_32_4, 4);
unpack_32!(unpack_32_5, 5);
unpack_32!(unpack_32_6, 6);
unpack_32!(unpack_32_7, 7);
unpack_32!(unpack_32_8, 8);
unpack_32!(unpack_32_9, 9);
unpack_32!(unpack_32_10, 10);
unpack_32!(unpack_32_11, 11);
unpack_32!(unpack_32_12, 12);
unpack_32!(unpack_32_13, 13);
unpack_32!(unpack_32_14, 14);
unpack_32!(unpack_32_15, 15);
unpack_32!(unpack_32_16, 16);
unpack_32!(unpack_32_17, 17);
unpack_32!(unpack_32_18, 18);
unpack_32!(unpack_32_19, 19);
unpack_32!(unpack_32_20, 20);
unpack_32!(unpack_32_21, 21);
unpack_32!(unpack_32_22, 22);
unpack_32!(unpack_32_23, 23);
unpack_32!(unpack_32_24, 24);
unpack_32!(unpack_32_25, 25);
unpack_32!(unpack_32_26, 26);
unpack_32!(unpack_32_27, 27);
unpack_32!(unpack_32_28, 28);
unpack_32!(unpack_32_29, 29);
unpack_32!(unpack_32_30, 30);
unpack_32!(unpack_32_31, 31);
unpack_32!(unpack_32_32, 32);

macro_rules! pack_32 {
    ($name:ident, $bits:expr) => {
        fn $name(input: &[u32; 1024], output: &mut [u32; 1024 * $bits / u32::BITS as usize]) {
            for lane in 0..u32::LANES {
                pack!(u32, $bits, output, lane, |$idx| { input[$idx] });
            }
        }
    };
}

pack_32!(pack_32_1, 1);
pack_32!(pack_32_2, 2);
pack_32!(pack_32_3, 3);
pack_32!(pack_32_4, 4);
pack_32!(pack_32_5, 5);
pack_32!(pack_32_6, 6);
pack_32!(pack_32_7, 7);
pack_32!(pack_32_8, 8);
pack_32!(pack_32_9, 9);
pack_32!(pack_32_10, 10);
pack_32!(pack_32_11, 11);
pack_32!(pack_32_12, 12);
pack_32!(pack_32_13, 13);
pack_32!(pack_32_14, 14);
pack_32!(pack_32_15, 15);
pack_32!(pack_32_16, 16);
pack_32!(pack_32_17, 17);
pack_32!(pack_32_18, 18);
pack_32!(pack_32_19, 19);
pack_32!(pack_32_20, 20);
pack_32!(pack_32_21, 21);
pack_32!(pack_32_22, 22);
pack_32!(pack_32_23, 23);
pack_32!(pack_32_24, 24);
pack_32!(pack_32_25, 25);
pack_32!(pack_32_26, 26);
pack_32!(pack_32_27, 27);
pack_32!(pack_32_28, 28);
pack_32!(pack_32_29, 29);
pack_32!(pack_32_30, 30);
pack_32!(pack_32_31, 31);
pack_32!(pack_32_32, 32);

macro_rules! unpack_64 {
    ($name:ident, $bit_width:expr) => {
        fn $name(input: &[u64; 1024 * $bit_width / u64::T], output: &mut [u64; 1024]) {
            for lane in 0..u64::LANES {
                unpack!(u64, $bit_width, input, lane, |$idx, $elem| {
                    output[$idx] = $elem
                });
            }
        }
    };
}

unpack_64!(unpack_64_1, 1);
unpack_64!(unpack_64_2, 2);
unpack_64!(unpack_64_3, 3);
unpack_64!(unpack_64_4, 4);
unpack_64!(unpack_64_5, 5);
unpack_64!(unpack_64_6, 6);
unpack_64!(unpack_64_7, 7);
unpack_64!(unpack_64_8, 8);
unpack_64!(unpack_64_9, 9);
unpack_64!(unpack_64_10, 10);
unpack_64!(unpack_64_11, 11);
unpack_64!(unpack_64_12, 12);
unpack_64!(unpack_64_13, 13);
unpack_64!(unpack_64_14, 14);
unpack_64!(unpack_64_15, 15);
unpack_64!(unpack_64_16, 16);
unpack_64!(unpack_64_17, 17);
unpack_64!(unpack_64_18, 18);
unpack_64!(unpack_64_19, 19);
unpack_64!(unpack_64_20, 20);
unpack_64!(unpack_64_21, 21);
unpack_64!(unpack_64_22, 22);
unpack_64!(unpack_64_23, 23);
unpack_64!(unpack_64_24, 24);
unpack_64!(unpack_64_25, 25);
unpack_64!(unpack_64_26, 26);
unpack_64!(unpack_64_27, 27);
unpack_64!(unpack_64_28, 28);
unpack_64!(unpack_64_29, 29);
unpack_64!(unpack_64_30, 30);
unpack_64!(unpack_64_31, 31);
unpack_64!(unpack_64_32, 32);

unpack_64!(unpack_64_33, 33);
unpack_64!(unpack_64_34, 34);
unpack_64!(unpack_64_35, 35);
unpack_64!(unpack_64_36, 36);
unpack_64!(unpack_64_37, 37);
unpack_64!(unpack_64_38, 38);
unpack_64!(unpack_64_39, 39);
unpack_64!(unpack_64_40, 40);
unpack_64!(unpack_64_41, 41);
unpack_64!(unpack_64_42, 42);
unpack_64!(unpack_64_43, 43);
unpack_64!(unpack_64_44, 44);
unpack_64!(unpack_64_45, 45);
unpack_64!(unpack_64_46, 46);
unpack_64!(unpack_64_47, 47);
unpack_64!(unpack_64_48, 48);
unpack_64!(unpack_64_49, 49);
unpack_64!(unpack_64_50, 50);
unpack_64!(unpack_64_51, 51);
unpack_64!(unpack_64_52, 52);
unpack_64!(unpack_64_53, 53);
unpack_64!(unpack_64_54, 54);
unpack_64!(unpack_64_55, 55);
unpack_64!(unpack_64_56, 56);
unpack_64!(unpack_64_57, 57);
unpack_64!(unpack_64_58, 58);
unpack_64!(unpack_64_59, 59);
unpack_64!(unpack_64_60, 60);
unpack_64!(unpack_64_61, 61);
unpack_64!(unpack_64_62, 62);
unpack_64!(unpack_64_63, 63);
unpack_64!(unpack_64_64, 64);

macro_rules! pack_64 {
    ($name:ident, $bits:expr) => {
        fn $name(input: &[u64; 1024], output: &mut [u64; 1024 * $bits / u64::BITS as usize]) {
            for lane in 0..u64::LANES {
                pack!(u64, $bits, output, lane, |$idx| { input[$idx] });
            }
        }
    };
}

pack_64!(pack_64_1, 1);
pack_64!(pack_64_2, 2);
pack_64!(pack_64_3, 3);
pack_64!(pack_64_4, 4);
pack_64!(pack_64_5, 5);
pack_64!(pack_64_6, 6);
pack_64!(pack_64_7, 7);
pack_64!(pack_64_8, 8);
pack_64!(pack_64_9, 9);
pack_64!(pack_64_10, 10);
pack_64!(pack_64_11, 11);
pack_64!(pack_64_12, 12);
pack_64!(pack_64_13, 13);
pack_64!(pack_64_14, 14);
pack_64!(pack_64_15, 15);
pack_64!(pack_64_16, 16);
pack_64!(pack_64_17, 17);
pack_64!(pack_64_18, 18);
pack_64!(pack_64_19, 19);
pack_64!(pack_64_20, 20);
pack_64!(pack_64_21, 21);
pack_64!(pack_64_22, 22);
pack_64!(pack_64_23, 23);
pack_64!(pack_64_24, 24);
pack_64!(pack_64_25, 25);
pack_64!(pack_64_26, 26);
pack_64!(pack_64_27, 27);
pack_64!(pack_64_28, 28);
pack_64!(pack_64_29, 29);
pack_64!(pack_64_30, 30);
pack_64!(pack_64_31, 31);
pack_64!(pack_64_32, 32);

pack_64!(pack_64_33, 33);
pack_64!(pack_64_34, 34);
pack_64!(pack_64_35, 35);
pack_64!(pack_64_36, 36);
pack_64!(pack_64_37, 37);
pack_64!(pack_64_38, 38);
pack_64!(pack_64_39, 39);
pack_64!(pack_64_40, 40);
pack_64!(pack_64_41, 41);
pack_64!(pack_64_42, 42);
pack_64!(pack_64_43, 43);
pack_64!(pack_64_44, 44);
pack_64!(pack_64_45, 45);
pack_64!(pack_64_46, 46);
pack_64!(pack_64_47, 47);
pack_64!(pack_64_48, 48);
pack_64!(pack_64_49, 49);
pack_64!(pack_64_50, 50);
pack_64!(pack_64_51, 51);
pack_64!(pack_64_52, 52);
pack_64!(pack_64_53, 53);
pack_64!(pack_64_54, 54);
pack_64!(pack_64_55, 55);
pack_64!(pack_64_56, 56);
pack_64!(pack_64_57, 57);
pack_64!(pack_64_58, 58);
pack_64!(pack_64_59, 59);
pack_64!(pack_64_60, 60);
pack_64!(pack_64_61, 61);
pack_64!(pack_64_62, 62);
pack_64!(pack_64_63, 63);
pack_64!(pack_64_64, 64);

#[cfg(test)]
mod test {
    use super::*;
    use bitpacking::{BitPacker as ExternalBitPacker, BitPacker4x as ExternalBitPacker4x};
    use core::array;
    // a fast random number generator
    pub struct XorShift {
        state: u64,
    }

    impl XorShift {
        pub fn new(seed: u64) -> Self {
            Self { state: seed }
        }

        pub fn next(&mut self) -> u64 {
            let mut x = self.state;
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            self.state = x;
            x
        }
    }

    fn mask_for_width(width: u8) -> u32 {
        match width {
            0 => 0,
            32 => u32::MAX,
            _ => (1u32 << width) - 1,
        }
    }

    fn raw_bitpacker4x_case(width: u8, seed: u64) -> Vec<u32> {
        let mask = mask_for_width(width);
        let mut rng = XorShift::new(seed);
        (0..BitPacker4x::BLOCK_LEN)
            .map(|idx| match seed % 4 {
                0 => 0,
                1 => mask,
                2 => idx as u32 & mask,
                _ => (rng.next() as u32) & mask,
            })
            .collect()
    }

    fn sorted_bitpacker4x_case(width: u8, seed: u64) -> (u32, Vec<u32>) {
        if width == 0 {
            return (17, vec![17; BitPacker4x::BLOCK_LEN]);
        }
        if width == 32 {
            return (0, vec![u32::MAX; BitPacker4x::BLOCK_LEN]);
        }

        let mask = mask_for_width(width).min(127);
        let mut rng = XorShift::new(seed);
        let mut current = 17u32;
        let values = (0..BitPacker4x::BLOCK_LEN)
            .map(|_| {
                current += (rng.next() as u32) & mask;
                current
            })
            .collect();
        (17, values)
    }

    #[test]
    fn test_bitpacker4x_raw_compatible_with_external_bitpacking() {
        let ours = BitPacker4x::new();
        let external = ExternalBitPacker4x::new();

        for width in 0..=32 {
            for seed in [0, 1, 2, 123456789] {
                let values = raw_bitpacker4x_case(width, seed);
                assert_eq!(ours.num_bits(&values), external.num_bits(&values));

                let mut actual = vec![0u8; BitPacker4x::compressed_block_size(width)];
                let actual_len = ours.compress(&values, &mut actual, width);

                let mut expected = vec![0u8; ExternalBitPacker4x::compressed_block_size(width)];
                let expected_len = external.compress(&values, &mut expected, width);

                assert_eq!(actual_len, expected_len);
                assert_eq!(actual, expected, "width {width} seed {seed}");

                let mut decoded = vec![0u32; BitPacker4x::BLOCK_LEN];
                let consumed = ours.decompress(&actual, &mut decoded, width);
                assert_eq!(consumed, actual_len);
                assert_eq!(decoded, values);
            }
        }
    }

    #[test]
    fn test_bitpacker4x_sorted_compatible_with_external_bitpacking() {
        let ours = BitPacker4x::new();
        let external = ExternalBitPacker4x::new();

        for width in 0..=32 {
            for seed in [0, 1, 2, 123456789] {
                let (initial, values) = sorted_bitpacker4x_case(width, seed);
                assert_eq!(
                    ours.num_bits_sorted(initial, &values),
                    external.num_bits_sorted(initial, &values)
                );

                let mut actual = vec![0u8; BitPacker4x::compressed_block_size(width)];
                let actual_len = ours.compress_sorted(initial, &values, &mut actual, width);

                let mut expected = vec![0u8; ExternalBitPacker4x::compressed_block_size(width)];
                let expected_len = external.compress_sorted(initial, &values, &mut expected, width);

                assert_eq!(actual_len, expected_len);
                assert_eq!(actual, expected, "width {width} seed {seed}");

                let mut decoded = vec![0u32; BitPacker4x::BLOCK_LEN];
                let consumed = ours.decompress_sorted(initial, &actual, &mut decoded, width);
                assert_eq!(consumed, actual_len);
                assert_eq!(decoded, values);
            }
        }
    }

    // a macro version of this function generalize u8, u16, u32, u64 takes very long time for a test build, so I
    // write it for each type separately
    fn pack_unpack_u8(bit_width: usize) {
        let mut values: [u8; 1024] = [0; 1024];
        let mut rng = XorShift::new(123456789);
        for value in &mut values {
            *value = (rng.next() % (1 << bit_width)) as u8;
        }

        let mut packed = vec![0; 1024 * bit_width / 8];
        for lane in 0..u8::LANES {
            // Always loop over lanes first. This is what the compiler vectorizes.
            pack!(u8, bit_width, packed, lane, |$pos| {
                values[$pos]
            });
        }

        let mut unpacked: [u8; 1024] = [0; 1024];
        for lane in 0..u8::LANES {
            // Always loop over lanes first. This is what the compiler vectorizes.
            unpack!(u8, bit_width, packed, lane, |$idx, $elem| {
                unpacked[$idx] = $elem;
            });
        }

        assert_eq!(values, unpacked);
    }

    fn pack_unpack_u16(bit_width: usize) {
        let mut values: [u16; 1024] = [0; 1024];
        let mut rng = XorShift::new(123456789);
        for value in &mut values {
            *value = (rng.next() % (1 << bit_width)) as u16;
        }

        let mut packed = vec![0; 1024 * bit_width / 16];
        for lane in 0..u16::LANES {
            // Always loop over lanes first. This is what the compiler vectorizes.
            pack!(u16, bit_width, packed, lane, |$pos| {
                values[$pos]
            });
        }

        let mut unpacked: [u16; 1024] = [0; 1024];
        for lane in 0..u16::LANES {
            // Always loop over lanes first. This is what the compiler vectorizes.
            unpack!(u16, bit_width, packed, lane, |$idx, $elem| {
                unpacked[$idx] = $elem;
            });
        }

        assert_eq!(values, unpacked);
    }

    fn pack_unpack_u32(bit_width: usize) {
        let mut values: [u32; 1024] = [0; 1024];
        let mut rng = XorShift::new(123456789);
        for value in &mut values {
            *value = (rng.next() % (1 << bit_width)) as u32;
        }

        let mut packed = vec![0; 1024 * bit_width / 32];
        for lane in 0..u32::LANES {
            // Always loop over lanes first. This is what the compiler vectorizes.
            pack!(u32, bit_width, packed, lane, |$pos| {
                values[$pos]
            });
        }

        let mut unpacked: [u32; 1024] = [0; 1024];
        for lane in 0..u32::LANES {
            // Always loop over lanes first. This is what the compiler vectorizes.
            unpack!(u32, bit_width, packed, lane, |$idx, $elem| {
                unpacked[$idx] = $elem;
            });
        }

        assert_eq!(values, unpacked);
    }

    fn pack_unpack_u64(bit_width: usize) {
        let mut values: [u64; 1024] = [0; 1024];
        let mut rng = XorShift::new(123456789);
        if bit_width == 64 {
            for value in &mut values {
                *value = rng.next();
            }
        } else {
            for value in &mut values {
                *value = rng.next() % (1 << bit_width);
            }
        }

        let mut packed = vec![0; 1024 * bit_width / 64];
        for lane in 0..u64::LANES {
            // Always loop over lanes first. This is what the compiler vectorizes.
            pack!(u64, bit_width, packed, lane, |$pos| {
                values[$pos]
            });
        }

        let mut unpacked: [u64; 1024] = [0; 1024];
        for lane in 0..u64::LANES {
            // Always loop over lanes first. This is what the compiler vectorizes.
            unpack!(u64, bit_width, packed, lane, |$idx, $elem| {
                unpacked[$idx] = $elem;
            });
        }

        assert_eq!(values, unpacked);
    }

    #[test]
    fn test_pack() {
        pack_unpack_u8(0);
        pack_unpack_u8(1);
        pack_unpack_u8(2);
        pack_unpack_u8(3);
        pack_unpack_u8(4);
        pack_unpack_u8(5);
        pack_unpack_u8(6);
        pack_unpack_u8(7);
        pack_unpack_u8(8);

        pack_unpack_u16(0);
        pack_unpack_u16(1);
        pack_unpack_u16(2);
        pack_unpack_u16(3);
        pack_unpack_u16(4);
        pack_unpack_u16(5);
        pack_unpack_u16(6);
        pack_unpack_u16(7);
        pack_unpack_u16(8);
        pack_unpack_u16(9);
        pack_unpack_u16(10);
        pack_unpack_u16(11);
        pack_unpack_u16(12);
        pack_unpack_u16(13);
        pack_unpack_u16(14);
        pack_unpack_u16(15);
        pack_unpack_u16(16);

        pack_unpack_u32(0);
        pack_unpack_u32(1);
        pack_unpack_u32(2);
        pack_unpack_u32(3);
        pack_unpack_u32(4);
        pack_unpack_u32(5);
        pack_unpack_u32(6);
        pack_unpack_u32(7);
        pack_unpack_u32(8);
        pack_unpack_u32(9);
        pack_unpack_u32(10);
        pack_unpack_u32(11);
        pack_unpack_u32(12);
        pack_unpack_u32(13);
        pack_unpack_u32(14);
        pack_unpack_u32(15);
        pack_unpack_u32(16);
        pack_unpack_u32(17);
        pack_unpack_u32(18);
        pack_unpack_u32(19);
        pack_unpack_u32(20);
        pack_unpack_u32(21);
        pack_unpack_u32(22);
        pack_unpack_u32(23);
        pack_unpack_u32(24);
        pack_unpack_u32(25);
        pack_unpack_u32(26);
        pack_unpack_u32(27);
        pack_unpack_u32(28);
        pack_unpack_u32(29);
        pack_unpack_u32(30);
        pack_unpack_u32(31);
        pack_unpack_u32(32);

        pack_unpack_u64(0);
        pack_unpack_u64(1);
        pack_unpack_u64(2);
        pack_unpack_u64(3);
        pack_unpack_u64(4);
        pack_unpack_u64(5);
        pack_unpack_u64(6);
        pack_unpack_u64(7);
        pack_unpack_u64(8);
        pack_unpack_u64(9);
        pack_unpack_u64(10);
        pack_unpack_u64(11);
        pack_unpack_u64(12);
        pack_unpack_u64(13);
        pack_unpack_u64(14);
        pack_unpack_u64(15);
        pack_unpack_u64(16);
        pack_unpack_u64(17);
        pack_unpack_u64(18);
        pack_unpack_u64(19);
        pack_unpack_u64(20);
        pack_unpack_u64(21);
        pack_unpack_u64(22);
        pack_unpack_u64(23);
        pack_unpack_u64(24);
        pack_unpack_u64(25);
        pack_unpack_u64(26);
        pack_unpack_u64(27);
        pack_unpack_u64(28);
        pack_unpack_u64(29);
        pack_unpack_u64(30);
        pack_unpack_u64(31);
        pack_unpack_u64(32);
        pack_unpack_u64(33);
        pack_unpack_u64(34);
        pack_unpack_u64(35);
        pack_unpack_u64(36);
        pack_unpack_u64(37);
        pack_unpack_u64(38);
        pack_unpack_u64(39);
        pack_unpack_u64(40);
        pack_unpack_u64(41);
        pack_unpack_u64(42);
        pack_unpack_u64(43);
        pack_unpack_u64(44);
        pack_unpack_u64(45);
        pack_unpack_u64(46);
        pack_unpack_u64(47);
        pack_unpack_u64(48);
        pack_unpack_u64(49);
        pack_unpack_u64(50);
        pack_unpack_u64(51);
        pack_unpack_u64(52);
        pack_unpack_u64(53);
        pack_unpack_u64(54);
        pack_unpack_u64(55);
        pack_unpack_u64(56);
        pack_unpack_u64(57);
        pack_unpack_u64(58);
        pack_unpack_u64(59);
        pack_unpack_u64(60);
        pack_unpack_u64(61);
        pack_unpack_u64(62);
        pack_unpack_u64(63);
        pack_unpack_u64(64);
    }

    fn unchecked_pack_unpack_u8(bit_width: usize) {
        let mut values = [0u8; 1024];
        let mut rng = XorShift::new(123456789);
        for value in &mut values {
            *value = (rng.next() % (1 << bit_width)) as u8;
        }
        let mut packed = vec![0; 1024 * bit_width / 8];
        unsafe {
            BitPacking::unchecked_pack(bit_width, &values, &mut packed);
        }
        let mut output = [0; 1024];
        unsafe { BitPacking::unchecked_unpack(bit_width, &packed, &mut output) };
        assert_eq!(values, output);
    }

    fn unchecked_pack_unpack_u16(bit_width: usize) {
        let mut values = [0u16; 1024];
        let mut rng = XorShift::new(123456789);
        for value in &mut values {
            *value = (rng.next() % (1 << bit_width)) as u16;
        }
        let mut packed = vec![0; 1024 * bit_width / u16::T];
        unsafe {
            BitPacking::unchecked_pack(bit_width, &values, &mut packed);
        }
        let mut output = [0; 1024];
        unsafe { BitPacking::unchecked_unpack(bit_width, &packed, &mut output) };
        assert_eq!(values, output);
    }

    fn unchecked_pack_unpack_u32(bit_width: usize) {
        let mut values = [0u32; 1024];
        let mut rng = XorShift::new(123456789);
        for value in &mut values {
            *value = (rng.next() % (1 << bit_width)) as u32;
        }
        let mut packed = vec![0; 1024 * bit_width / u32::T];
        unsafe {
            BitPacking::unchecked_pack(bit_width, &values, &mut packed);
        }
        let mut output = [0; 1024];
        unsafe { BitPacking::unchecked_unpack(bit_width, &packed, &mut output) };
        assert_eq!(values, output);
    }

    fn unchecked_pack_unpack_u64(bit_width: usize) {
        let mut values = [0u64; 1024];
        let mut rng = XorShift::new(123456789);
        if bit_width == 64 {
            for value in &mut values {
                *value = rng.next();
            }
        } else {
            for value in &mut values {
                *value = rng.next() % (1 << bit_width);
            }
        }
        let mut packed = vec![0; 1024 * bit_width / u64::T];
        unsafe {
            BitPacking::unchecked_pack(bit_width, &values, &mut packed);
        }
        let mut output = [0; 1024];
        unsafe { BitPacking::unchecked_unpack(bit_width, &packed, &mut output) };
        assert_eq!(values, output);
    }

    #[test]
    fn test_unchecked_pack() {
        let input = array::from_fn(|i| i as u32);
        let mut packed = [0; 320];
        unsafe { BitPacking::unchecked_pack(10, &input, &mut packed) };
        let mut output = [0; 1024];
        unsafe { BitPacking::unchecked_unpack(10, &packed, &mut output) };
        assert_eq!(input, output);

        unchecked_pack_unpack_u8(1);
        unchecked_pack_unpack_u8(2);
        unchecked_pack_unpack_u8(3);
        unchecked_pack_unpack_u8(4);
        unchecked_pack_unpack_u8(5);
        unchecked_pack_unpack_u8(6);
        unchecked_pack_unpack_u8(7);
        unchecked_pack_unpack_u8(8);

        unchecked_pack_unpack_u16(1);
        unchecked_pack_unpack_u16(2);
        unchecked_pack_unpack_u16(3);
        unchecked_pack_unpack_u16(4);
        unchecked_pack_unpack_u16(5);
        unchecked_pack_unpack_u16(6);
        unchecked_pack_unpack_u16(7);
        unchecked_pack_unpack_u16(8);
        unchecked_pack_unpack_u16(9);
        unchecked_pack_unpack_u16(10);
        unchecked_pack_unpack_u16(11);
        unchecked_pack_unpack_u16(12);
        unchecked_pack_unpack_u16(13);
        unchecked_pack_unpack_u16(14);
        unchecked_pack_unpack_u16(15);
        unchecked_pack_unpack_u16(16);

        unchecked_pack_unpack_u32(1);
        unchecked_pack_unpack_u32(2);
        unchecked_pack_unpack_u32(3);
        unchecked_pack_unpack_u32(4);
        unchecked_pack_unpack_u32(5);
        unchecked_pack_unpack_u32(6);
        unchecked_pack_unpack_u32(7);
        unchecked_pack_unpack_u32(8);
        unchecked_pack_unpack_u32(9);
        unchecked_pack_unpack_u32(10);
        unchecked_pack_unpack_u32(11);
        unchecked_pack_unpack_u32(12);
        unchecked_pack_unpack_u32(13);
        unchecked_pack_unpack_u32(14);
        unchecked_pack_unpack_u32(15);
        unchecked_pack_unpack_u32(16);
        unchecked_pack_unpack_u32(17);
        unchecked_pack_unpack_u32(18);
        unchecked_pack_unpack_u32(19);
        unchecked_pack_unpack_u32(20);
        unchecked_pack_unpack_u32(21);
        unchecked_pack_unpack_u32(22);
        unchecked_pack_unpack_u32(23);
        unchecked_pack_unpack_u32(24);
        unchecked_pack_unpack_u32(25);
        unchecked_pack_unpack_u32(26);
        unchecked_pack_unpack_u32(27);
        unchecked_pack_unpack_u32(28);
        unchecked_pack_unpack_u32(29);
        unchecked_pack_unpack_u32(30);
        unchecked_pack_unpack_u32(31);
        unchecked_pack_unpack_u32(32);

        unchecked_pack_unpack_u64(1);
        unchecked_pack_unpack_u64(2);
        unchecked_pack_unpack_u64(3);
        unchecked_pack_unpack_u64(4);
        unchecked_pack_unpack_u64(5);
        unchecked_pack_unpack_u64(6);
        unchecked_pack_unpack_u64(7);
        unchecked_pack_unpack_u64(8);
        unchecked_pack_unpack_u64(9);
        unchecked_pack_unpack_u64(10);
        unchecked_pack_unpack_u64(11);
        unchecked_pack_unpack_u64(12);
        unchecked_pack_unpack_u64(13);
        unchecked_pack_unpack_u64(14);
        unchecked_pack_unpack_u64(15);
        unchecked_pack_unpack_u64(16);
        unchecked_pack_unpack_u64(17);
        unchecked_pack_unpack_u64(18);
        unchecked_pack_unpack_u64(19);
        unchecked_pack_unpack_u64(20);
        unchecked_pack_unpack_u64(21);
        unchecked_pack_unpack_u64(22);
        unchecked_pack_unpack_u64(23);
        unchecked_pack_unpack_u64(24);
        unchecked_pack_unpack_u64(25);
        unchecked_pack_unpack_u64(26);
        unchecked_pack_unpack_u64(27);
        unchecked_pack_unpack_u64(28);
        unchecked_pack_unpack_u64(29);
        unchecked_pack_unpack_u64(30);
        unchecked_pack_unpack_u64(31);
        unchecked_pack_unpack_u64(32);
        unchecked_pack_unpack_u64(33);
        unchecked_pack_unpack_u64(34);
        unchecked_pack_unpack_u64(35);
        unchecked_pack_unpack_u64(36);
        unchecked_pack_unpack_u64(37);
        unchecked_pack_unpack_u64(38);
        unchecked_pack_unpack_u64(39);
        unchecked_pack_unpack_u64(40);
        unchecked_pack_unpack_u64(41);
        unchecked_pack_unpack_u64(42);
        unchecked_pack_unpack_u64(43);
        unchecked_pack_unpack_u64(44);
        unchecked_pack_unpack_u64(45);
        unchecked_pack_unpack_u64(46);
        unchecked_pack_unpack_u64(47);
        unchecked_pack_unpack_u64(48);
        unchecked_pack_unpack_u64(49);
        unchecked_pack_unpack_u64(50);
        unchecked_pack_unpack_u64(51);
        unchecked_pack_unpack_u64(52);
        unchecked_pack_unpack_u64(53);
        unchecked_pack_unpack_u64(54);
        unchecked_pack_unpack_u64(55);
        unchecked_pack_unpack_u64(56);
        unchecked_pack_unpack_u64(57);
        unchecked_pack_unpack_u64(58);
        unchecked_pack_unpack_u64(59);
        unchecked_pack_unpack_u64(60);
        unchecked_pack_unpack_u64(61);
        unchecked_pack_unpack_u64(62);
        unchecked_pack_unpack_u64(63);
        unchecked_pack_unpack_u64(64);
    }
}
