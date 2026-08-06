// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

/// Returns true if the given number is a power of two.
///
/// ```
/// use lance_core::utils::bit::is_pwr_two;
///
/// assert!(is_pwr_two(1));
/// assert!(is_pwr_two(2));
/// assert!(is_pwr_two(1024));
/// assert!(!is_pwr_two(3));
/// assert!(!is_pwr_two(1000));
/// ```
pub fn is_pwr_two(n: u64) -> bool {
    n & (n - 1) == 0
}

/// Returns the number of padding bytes needed to align `n` to `ALIGN`.
///
/// ```
/// use lance_core::utils::bit::pad_bytes;
///
/// assert_eq!(pad_bytes::<8>(0), 0);
/// assert_eq!(pad_bytes::<8>(1), 7);
/// assert_eq!(pad_bytes::<8>(8), 0);
/// assert_eq!(pad_bytes::<8>(9), 7);
/// ```
pub fn pad_bytes<const ALIGN: usize>(n: usize) -> usize {
    debug_assert!(is_pwr_two(ALIGN as u64));
    (ALIGN - (n & (ALIGN - 1))) & (ALIGN - 1)
}

/// Returns the number of padding bytes needed to align `n` to `align`.
///
/// ```
/// use lance_core::utils::bit::pad_bytes_to;
///
/// assert_eq!(pad_bytes_to(0, 8), 0);
/// assert_eq!(pad_bytes_to(1, 8), 7);
/// assert_eq!(pad_bytes_to(8, 8), 0);
/// assert_eq!(pad_bytes_to(9, 8), 7);
/// ```
pub fn pad_bytes_to(n: usize, align: usize) -> usize {
    debug_assert!(is_pwr_two(align as u64));
    (align - (n & (align - 1))) & (align - 1)
}

/// Returns the number of padding bytes needed to align `n` to `ALIGN` (u64 version).
///
/// ```
/// use lance_core::utils::bit::pad_bytes_u64;
///
/// assert_eq!(pad_bytes_u64::<8>(0), 0);
/// assert_eq!(pad_bytes_u64::<8>(1), 7);
/// assert_eq!(pad_bytes_u64::<8>(8), 0);
/// assert_eq!(pad_bytes_u64::<8>(9), 7);
/// ```
pub fn pad_bytes_u64<const ALIGN: u64>(n: u64) -> u64 {
    debug_assert!(is_pwr_two(ALIGN));
    (ALIGN - (n & (ALIGN - 1))) & (ALIGN - 1)
}

// This is a lookup table for the log2 of the first 256 numbers
const LOG_TABLE_256: [u8; 256] = [
    0, 1, 2, 2, 3, 3, 3, 3, 4, 4, 4, 4, 4, 4, 4, 4, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5,
    6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6,
    7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
    7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7,
    8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
    8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
    8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
    8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8,
];

/// Returns the number of bits needed to represent the given number.
///
/// Inspired by <https://graphics.stanford.edu/~seander/bithacks.html>
///
/// ```
/// use lance_core::utils::bit::log_2_ceil;
///
/// assert_eq!(log_2_ceil(1), 1);
/// assert_eq!(log_2_ceil(2), 2);
/// assert_eq!(log_2_ceil(255), 8);
/// assert_eq!(log_2_ceil(256), 9);
/// ```
pub fn log_2_ceil(val: u32) -> u32 {
    assert!(val > 0);
    let upper_half = val >> 16;
    if upper_half == 0 {
        let third_quarter = val >> 8;
        if third_quarter == 0 {
            // Use lowest 8 bits (upper 24 are 0)
            LOG_TABLE_256[val as usize] as u32
        } else {
            // Use bits 16..24 (0..16 are 0)
            LOG_TABLE_256[third_quarter as usize] as u32 + 8
        }
    } else {
        let first_quarter = upper_half >> 8;
        if first_quarter == 0 {
            // Use bits 8..16 (0..8 are 0)
            16 + LOG_TABLE_256[upper_half as usize] as u32
        } else {
            // Use most significant bits (it's a big number!)
            24 + LOG_TABLE_256[first_quarter as usize] as u32
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::utils::bit::{is_pwr_two, log_2_ceil, pad_bytes, pad_bytes_to, pad_bytes_u64};

    #[test]
    fn test_bit_utils() {
        // Test values not in doctests
        assert!(is_pwr_two(4));
        assert!(is_pwr_two(1024));
        assert!(!is_pwr_two(5));

        // Test different alignment (64) not shown in doctests
        assert_eq!(pad_bytes::<64>(100), 28);
        assert_eq!(pad_bytes_to(100, 64), 28);
        assert_eq!(pad_bytes_u64::<64>(100), 28);
    }

    #[test]
    fn test_log_2_ceil() {
        #[cfg_attr(coverage, coverage(off))]
        fn classic_approach(mut val: u32) -> u32 {
            let mut counter = 0;
            while val > 0 {
                val >>= 1;
                counter += 1;
            }
            counter
        }

        for i in 1..(16 * 1024) {
            assert_eq!(log_2_ceil(i), classic_approach(i));
        }
        assert_eq!(log_2_ceil(50 * 1024), classic_approach(50 * 1024));
        assert_eq!(
            log_2_ceil(1024 * 1024 * 1024),
            classic_approach(1024 * 1024 * 1024)
        );
        // Cover the branch where upper_half != 0 but first_quarter == 0
        // (value between 2^16 and 2^24)
        assert_eq!(log_2_ceil(100_000), classic_approach(100_000));
    }
}
