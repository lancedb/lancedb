// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::ops::Range;

/// A row address encodes a fragment ID (upper 32 bits) and row offset (lower 32 bits).
///
/// ```
/// use lance_core::utils::address::RowAddress;
///
/// let addr = RowAddress::new_from_parts(5, 100);
/// assert_eq!(addr.fragment_id(), 5);
/// assert_eq!(addr.row_offset(), 100);
///
/// // Convert to/from u64
/// let raw: u64 = addr.into();
/// let addr2: RowAddress = raw.into();
/// assert_eq!(addr, addr2);
///
/// // Display format
/// assert_eq!(format!("{}", addr), "(5, 100)");
/// ```
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RowAddress(u64);

impl RowAddress {
    pub const FRAGMENT_SIZE: u64 = 1 << 32;
    /// A fragment id that will never be used.
    pub const TOMBSTONE_FRAG: u32 = 0xffffffff;
    /// A row id that will never be used.
    pub const TOMBSTONE_ROW: u64 = 0xffffffffffffffff;

    pub fn new_from_u64(row_addr: u64) -> Self {
        Self(row_addr)
    }

    pub fn new_from_parts(fragment_id: u32, row_offset: u32) -> Self {
        Self(((fragment_id as u64) << 32) | row_offset as u64)
    }

    /// Returns the address for the first row of a fragment.
    pub fn first_row(fragment_id: u32) -> Self {
        Self::new_from_parts(fragment_id, 0)
    }

    /// Returns the range of u64 addresses for a given fragment.
    ///
    /// ```
    /// use lance_core::utils::address::RowAddress;
    ///
    /// let range = RowAddress::address_range(2);
    /// assert_eq!(range.start, 2 * RowAddress::FRAGMENT_SIZE);
    /// assert_eq!(range.end, 3 * RowAddress::FRAGMENT_SIZE);
    /// ```
    pub fn address_range(fragment_id: u32) -> Range<u64> {
        u64::from(Self::first_row(fragment_id))..u64::from(Self::first_row(fragment_id + 1))
    }

    pub fn fragment_id(&self) -> u32 {
        (self.0 >> 32) as u32
    }

    pub fn row_offset(&self) -> u32 {
        self.0 as u32
    }
}

impl From<RowAddress> for u64 {
    fn from(row_addr: RowAddress) -> Self {
        row_addr.0
    }
}

impl From<u64> for RowAddress {
    fn from(row_addr: u64) -> Self {
        Self(row_addr)
    }
}

impl std::fmt::Debug for RowAddress {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self) // use Display
    }
}

impl std::fmt::Display for RowAddress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "({}, {})", self.fragment_id(), self.row_offset())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_row_address() {
        // new_from_u64 (not in doctest)
        let addr = RowAddress::new_from_u64(0x0000_0001_0000_0002);
        assert_eq!(addr.fragment_id(), 1);
        assert_eq!(addr.row_offset(), 2);

        // address_range uses first_row internally (coverage)
        let range = RowAddress::address_range(3);
        assert_eq!(range.start, 3 * RowAddress::FRAGMENT_SIZE);

        // From impls with different values than doctest
        let addr2 = RowAddress::new_from_parts(7, 8);
        let raw: u64 = addr2.into();
        let addr3: RowAddress = raw.into();
        assert_eq!(addr2, addr3);

        // Debug format (doctest only tests Display)
        assert_eq!(format!("{:?}", addr), "(1, 2)");
    }
}
