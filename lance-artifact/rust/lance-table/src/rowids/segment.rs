// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::ops::{Range, RangeInclusive};

use super::{bitmap::Bitmap, encoded_array::EncodedU64Array};
use lance_core::deepsize::DeepSizeOf;

/// Convert an estimated serialized byte cost from `u128` to `usize`, saturating
/// at [`usize::MAX`] when the value does not fit (infeasible encodings).
#[inline]
fn u128_byte_cost_to_usize(v: u128) -> usize {
    usize::try_from(v).unwrap_or(usize::MAX)
}

/// Different ways to represent a sequence of distinct u64s.
///
/// This is designed to be especially efficient for sequences that are sorted,
/// but not meaningfully larger than a `Vec<u64>` in the worst case.
///
/// The representation is chosen based on the properties of the sequence:
///                                                           
///  Sorted?───►Yes ───►Contiguous?─► Yes─► Range            
///    │                ▼                                 
///    │                No                                
///    │                ▼                                 
///    │              Dense?─────► Yes─► RangeWithBitmap/RangeWithHoles
///    │                ▼                                 
///    │                No─────────────► SortedArray      
///    ▼                                                    
///    No──────────────────────────────► Array            
///
/// "Dense" is decided based on the estimated byte size of the representation.
///
/// Size of RangeWithBitMap for N values:
///     8 bytes + 8 bytes + ceil((max - min) / 8) bytes
/// Size of SortedArray for N values (assuming u16 packed):
///     8 bytes + 8 bytes + 8 bytes + 2 bytes * N
///
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum U64Segment {
    /// A contiguous sorted range of row ids.
    ///
    /// Total size: 16 bytes
    Range(Range<u64>),
    /// A sorted range of row ids, that is mostly contiguous.
    ///
    /// Total size: 24 bytes + n_holes * 4 bytes
    /// Use when: 32 * n_holes < max - min
    RangeWithHoles {
        range: Range<u64>,
        /// Bitmap of offsets from the start of the range that are holes.
        /// This is sorted, so binary search can be used. It's typically
        /// relatively small.
        holes: EncodedU64Array,
    },
    /// A sorted range of row ids, that is mostly contiguous.
    ///
    /// Bitmap is 1 when the value is present, 0 when it's missing.
    ///
    /// Total size: 24 bytes + ceil((max - min) / 8) bytes
    /// Use when: max - min > 16 * len
    RangeWithBitmap { range: Range<u64>, bitmap: Bitmap },
    /// A sorted array of row ids, that is sparse.
    ///
    /// Total size: 24 bytes + 2 * n_values bytes
    SortedArray(EncodedU64Array),
    /// An array of row ids, that is not sorted.
    Array(EncodedU64Array),
}

impl DeepSizeOf for U64Segment {
    fn deep_size_of_children(&self, context: &mut lance_core::deepsize::Context) -> usize {
        match self {
            Self::Range(_) => 0,
            Self::RangeWithHoles { holes, .. } => holes.deep_size_of_children(context),
            Self::RangeWithBitmap { bitmap, .. } => bitmap.deep_size_of_children(context),
            Self::SortedArray(array) => array.deep_size_of_children(context),
            Self::Array(array) => array.deep_size_of_children(context),
        }
    }
}

/// Statistics about a segment of u64s.
#[derive(Debug)]
struct SegmentStats {
    /// Min value in the segment.
    min: u64,
    /// Max value in the segment
    max: u64,
    /// Total number of values in the segment
    count: u64,
    /// Whether the segment is sorted
    sorted: bool,
}

impl SegmentStats {
    /// Number of missing values ("holes") in the range `[min, max]`.
    ///
    /// Returns `u128` because the total slot count `max - min + 1` can be up
    /// to `2^64` (when `min = 0, max = u64::MAX`), which exceeds `u64::MAX`.
    fn n_holes(&self) -> u128 {
        debug_assert!(self.sorted);
        if self.count == 0 {
            0
        } else {
            let total_slots = self.max as u128 - self.min as u128 + 1;
            total_slots - self.count as u128
        }
    }
}

impl U64Segment {
    /// Return the values that are missing from the slice.
    fn holes_in_slice<'a>(
        range: RangeInclusive<u64>,
        existing: impl IntoIterator<Item = u64> + 'a,
    ) -> impl Iterator<Item = u64> + 'a {
        let mut existing = existing.into_iter().peekable();
        range.filter(move |val| {
            if let Some(&existing_val) = existing.peek()
                && existing_val == *val
            {
                existing.next();
                return false;
            }
            true
        })
    }

    fn compute_stats(values: impl IntoIterator<Item = u64>) -> SegmentStats {
        let mut sorted = true;
        let mut min = u64::MAX;
        let mut max = 0;
        let mut count = 0;

        for val in values {
            count += 1;
            if val < min {
                min = val;
            }
            if val > max {
                max = val;
            }
            if sorted && count > 1 && val < max {
                sorted = false;
            }
        }

        if count == 0 {
            min = 0;
            max = 0;
        }

        SegmentStats {
            min,
            max,
            count,
            sorted,
        }
    }

    /// Estimate the serialized byte size of each sorted encoding variant.
    ///
    /// All arithmetic is performed in `u128` to avoid overflow when the range
    /// span `max - min + 1` approaches or exceeds `2^64`. Infeasible sizes
    /// saturate to `usize::MAX` so they always lose the `min()` comparison.
    fn sorted_sequence_sizes(stats: &SegmentStats) -> [usize; 3] {
        let n_holes = stats.n_holes();
        let total_slots = stats.max as u128 - stats.min as u128 + 1;

        let range_with_holes = 24u128.saturating_add(4u128.saturating_mul(n_holes));
        let range_with_bitmap = 24u128.saturating_add(total_slots.div_ceil(8));
        let sorted_array = 24u128.saturating_add(2u128.saturating_mul(stats.count as u128));

        [
            u128_byte_cost_to_usize(range_with_holes),
            u128_byte_cost_to_usize(range_with_bitmap),
            u128_byte_cost_to_usize(sorted_array),
        ]
    }

    fn from_stats_and_sequence(
        stats: SegmentStats,
        sequence: impl IntoIterator<Item = u64>,
    ) -> Self {
        if stats.sorted {
            let n_holes = stats.n_holes();
            // Range-backed encodings store an exclusive end as `Range<u64>`,
            // which cannot represent `u64::MAX + 1`. Compute the end once and
            // gate all range-backed branches on its representability.
            let exclusive_end = stats.max.checked_add(1);
            if stats.count == 0 {
                Self::Range(0..0)
            } else if n_holes == 0 && exclusive_end.is_some() {
                Self::Range(stats.min..exclusive_end.unwrap())
            } else if let Some(end) = exclusive_end {
                let sizes = Self::sorted_sequence_sizes(&stats);
                let min_size = sizes.iter().min().unwrap();
                if min_size == &sizes[0] {
                    let range = stats.min..end;
                    let mut holes =
                        Self::holes_in_slice(stats.min..=stats.max, sequence).collect::<Vec<_>>();
                    holes.sort_unstable();
                    let holes = EncodedU64Array::from(holes);
                    Self::RangeWithHoles { range, holes }
                } else if min_size == &sizes[1] {
                    let range = stats.min..end;
                    let mut bitmap = Bitmap::new_full((stats.max - stats.min) as usize + 1);
                    for hole in Self::holes_in_slice(stats.min..=stats.max, sequence) {
                        let offset = (hole - stats.min) as usize;
                        bitmap.clear(offset);
                    }
                    Self::RangeWithBitmap { range, bitmap }
                } else {
                    Self::SortedArray(EncodedU64Array::from_iter(sequence))
                }
            } else {
                // max == u64::MAX: exclusive end is unrepresentable in Range<u64>,
                // so no range-backed encoding can be used.
                Self::SortedArray(EncodedU64Array::from_iter(sequence))
            }
        } else {
            Self::Array(EncodedU64Array::from_iter(sequence))
        }
    }

    pub fn from_slice(slice: &[u64]) -> Self {
        Self::from_iter(slice.iter().copied())
    }
}

impl FromIterator<u64> for U64Segment {
    fn from_iter<T: IntoIterator<Item = u64>>(iter: T) -> Self {
        let values: Vec<u64> = iter.into_iter().collect();
        let stats = Self::compute_stats(values.iter().copied());
        Self::from_stats_and_sequence(stats, values)
    }
}

impl U64Segment {
    pub fn iter(&self) -> Box<dyn DoubleEndedIterator<Item = u64> + '_> {
        match self {
            Self::Range(range) => Box::new(range.clone()),
            Self::RangeWithHoles { range, holes } => {
                Box::new((range.start..range.end).filter(move |&val| {
                    // TODO: we could write a more optimal version of this
                    // iterator, but would need special handling to make it
                    // double ended.
                    holes.binary_search(val).is_err()
                }))
            }
            Self::RangeWithBitmap { range, bitmap } => {
                Box::new((range.start..range.end).filter(|val| {
                    let offset = (val - range.start) as usize;
                    bitmap.get(offset)
                }))
            }
            Self::SortedArray(array) => Box::new(array.iter()),
            Self::Array(array) => Box::new(array.iter()),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Self::Range(range) => (range.end - range.start) as usize,
            Self::RangeWithHoles { range, holes } => {
                let holes = holes.iter().count();
                (range.end - range.start) as usize - holes
            }
            Self::RangeWithBitmap { range, bitmap } => {
                let holes = bitmap.count_zeros();
                (range.end - range.start) as usize - holes
            }
            Self::SortedArray(array) => array.len(),
            Self::Array(array) => array.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get the min and max value of the segment, excluding tombstones.
    pub fn range(&self) -> Option<RangeInclusive<u64>> {
        match self {
            Self::Range(range) if range.is_empty() => None,
            Self::Range(range)
            | Self::RangeWithBitmap { range, .. }
            | Self::RangeWithHoles { range, .. } => Some(range.start..=(range.end - 1)),
            Self::SortedArray(array) => {
                // We can assume that the array is sorted.
                let min_value = array.first().unwrap();
                let max_value = array.last().unwrap();
                Some(min_value..=max_value)
            }
            Self::Array(array) => {
                let min_value = array.min().unwrap();
                let max_value = array.max().unwrap();
                Some(min_value..=max_value)
            }
        }
    }

    pub fn slice(&self, offset: usize, len: usize) -> Self {
        if len == 0 {
            return Self::Range(0..0);
        }

        let values: Vec<u64> = self.iter().skip(offset).take(len).collect();

        // `from_slice` will compute stats and select the best representation.
        Self::from_slice(&values)
    }

    pub fn position(&self, val: u64) -> Option<usize> {
        match self {
            Self::Range(range) => {
                if range.contains(&val) {
                    Some((val - range.start) as usize)
                } else {
                    None
                }
            }
            Self::RangeWithHoles { range, holes } => {
                if !range.contains(&val) {
                    return None;
                }
                // binary_search returns Err(idx) where idx is the count of holes
                // strictly less than val (holes are unique and sorted).
                match holes.binary_search(val) {
                    Ok(_) => None,
                    Err(num_holes_before) => {
                        let offset = (val - range.start) as usize;
                        Some(offset - num_holes_before)
                    }
                }
            }
            Self::RangeWithBitmap { range, bitmap } => {
                if range.contains(&val) && bitmap.get((val - range.start) as usize) {
                    let offset = (val - range.start) as usize;
                    let num_zeros = bitmap.slice(0, offset).count_zeros();
                    Some(offset - num_zeros)
                } else {
                    None
                }
            }
            Self::SortedArray(array) => array.binary_search(val).ok(),
            Self::Array(array) => array.iter().position(|v| v == val),
        }
    }

    pub fn get(&self, i: usize) -> Option<u64> {
        match self {
            Self::Range(range) => match range.start.checked_add(i as u64) {
                Some(val) if val < range.end => Some(val),
                _ => None,
            },
            Self::RangeWithHoles { range, holes } => {
                let len = (range.end - range.start) as usize - holes.len();
                if i >= len {
                    return None;
                }
                // The i-th surviving value v satisfies v = range.start + i + k,
                // where k = |{h ∈ holes : h < v}|. holes[k] - k is monotone
                // non-decreasing in k (holes are sorted and unique), so binary
                // search for the smallest k such that holes[k] - k > range.start + i.
                let target = range.start + i as u64;
                let mut lo = 0usize;
                let mut hi = holes.len();
                while lo < hi {
                    let mid = (lo + hi) / 2;
                    let h = holes.get(mid).unwrap();
                    if h.saturating_sub(mid as u64) > target {
                        hi = mid;
                    } else {
                        lo = mid + 1;
                    }
                }
                Some(range.start + i as u64 + lo as u64)
            }
            Self::RangeWithBitmap { range, bitmap } => {
                // Find the i-th set bit (a "select1") via byte-wise popcount.
                // Bytes past `bitmap.len()` are zero-padded by construction
                // (Bitmap::new_full), so popcount counts only valid positions.
                let mut remaining = i;
                for (byte_idx, &byte) in bitmap.data.iter().enumerate() {
                    let ones = byte.count_ones() as usize;
                    if remaining < ones {
                        let mut b = byte;
                        for _ in 0..remaining {
                            b &= b - 1; // clear lowest set bit
                        }
                        let bit = b.trailing_zeros() as usize;
                        return Some(range.start + (byte_idx * 8 + bit) as u64);
                    }
                    remaining -= ones;
                }
                None
            }
            Self::SortedArray(array) => array.get(i),
            Self::Array(array) => array.get(i),
        }
    }

    /// Check if a value is contained in the segment
    pub fn contains(&self, val: u64) -> bool {
        match self {
            Self::Range(range) => range.contains(&val),
            Self::RangeWithHoles { range, holes } => {
                if !range.contains(&val) {
                    return false;
                }
                // Check if the value is not in the holes
                !holes.iter().any(|hole| hole == val)
            }
            Self::RangeWithBitmap { range, bitmap } => {
                if !range.contains(&val) {
                    return false;
                }
                // Check if the bitmap has the value set (not cleared)
                let idx = (val - range.start) as usize;
                bitmap.get(idx)
            }
            Self::SortedArray(array) => array.binary_search(val).is_ok(),
            Self::Array(array) => array.iter().any(|v| v == val),
        }
    }

    /// Produce a new segment that has `val` as the new highest value in the segment
    pub fn with_new_high(self, val: u64) -> lance_core::Result<Self> {
        // Check that the new value is higher than the current maximum
        if let Some(range) = self.range()
            && val <= *range.end()
        {
            return Err(lance_core::Error::invalid_input(format!(
                "New value {} must be higher than current maximum {}",
                val,
                range.end()
            )));
        }

        Ok(match self {
            Self::Range(range) => {
                // Special case for empty range: create a range containing only the new value
                if range.start == range.end {
                    Self::Range(Range {
                        start: val,
                        end: val + 1,
                    })
                } else if val == range.end {
                    Self::Range(Range {
                        start: range.start,
                        end: val + 1,
                    })
                } else {
                    Self::RangeWithHoles {
                        range: Range {
                            start: range.start,
                            end: val + 1,
                        },
                        holes: EncodedU64Array::U64((range.end..val).collect()),
                    }
                }
            }
            Self::RangeWithHoles { range, holes } => {
                if val == range.end {
                    Self::RangeWithHoles {
                        range: Range {
                            start: range.start,
                            end: val + 1,
                        },
                        holes,
                    }
                } else {
                    let mut new_holes: Vec<u64> = holes.iter().collect();
                    new_holes.extend(range.end..val);
                    Self::RangeWithHoles {
                        range: Range {
                            start: range.start,
                            end: val + 1,
                        },
                        holes: EncodedU64Array::U64(new_holes),
                    }
                }
            }
            Self::RangeWithBitmap { range, bitmap } => {
                let new_range = Range {
                    start: range.start,
                    end: val + 1,
                };
                let gap_size = (val - range.end) as usize;
                let new_bitmap = bitmap
                    .iter()
                    .chain(std::iter::repeat_n(false, gap_size))
                    .chain(std::iter::once(true))
                    .collect::<Vec<bool>>();

                Self::RangeWithBitmap {
                    range: new_range,
                    bitmap: Bitmap::from(new_bitmap.as_slice()),
                }
            }
            Self::SortedArray(array) => match array {
                EncodedU64Array::U64(mut vec) => {
                    vec.push(val);
                    Self::SortedArray(EncodedU64Array::U64(vec))
                }
                EncodedU64Array::U16 { base, offsets } => {
                    if let Some(offset) = val.checked_sub(base) {
                        if offset <= u16::MAX as u64 {
                            let mut offsets = offsets;
                            offsets.push(offset as u16);
                            return Ok(Self::SortedArray(EncodedU64Array::U16 { base, offsets }));
                        } else if offset <= u32::MAX as u64 {
                            let mut u32_offsets: Vec<u32> =
                                offsets.into_iter().map(|o| o as u32).collect();
                            u32_offsets.push(offset as u32);
                            return Ok(Self::SortedArray(EncodedU64Array::U32 {
                                base,
                                offsets: u32_offsets,
                            }));
                        }
                    }
                    let mut new_array: Vec<u64> =
                        offsets.into_iter().map(|o| base + o as u64).collect();
                    new_array.push(val);
                    Self::SortedArray(EncodedU64Array::from(new_array))
                }
                EncodedU64Array::U32 { base, mut offsets } => {
                    if let Some(offset) = val.checked_sub(base)
                        && offset <= u32::MAX as u64
                    {
                        offsets.push(offset as u32);
                        return Ok(Self::SortedArray(EncodedU64Array::U32 { base, offsets }));
                    }
                    let mut new_array: Vec<u64> =
                        offsets.into_iter().map(|o| base + o as u64).collect();
                    new_array.push(val);
                    Self::SortedArray(EncodedU64Array::from(new_array))
                }
            },
            Self::Array(array) => match array {
                EncodedU64Array::U64(mut vec) => {
                    vec.push(val);
                    Self::Array(EncodedU64Array::U64(vec))
                }
                EncodedU64Array::U16 { base, offsets } => {
                    if let Some(offset) = val.checked_sub(base) {
                        if offset <= u16::MAX as u64 {
                            let mut offsets = offsets;
                            offsets.push(offset as u16);
                            return Ok(Self::Array(EncodedU64Array::U16 { base, offsets }));
                        } else if offset <= u32::MAX as u64 {
                            let mut u32_offsets: Vec<u32> =
                                offsets.into_iter().map(|o| o as u32).collect();
                            u32_offsets.push(offset as u32);
                            return Ok(Self::Array(EncodedU64Array::U32 {
                                base,
                                offsets: u32_offsets,
                            }));
                        }
                    }
                    let mut new_array: Vec<u64> =
                        offsets.into_iter().map(|o| base + o as u64).collect();
                    new_array.push(val);
                    Self::Array(EncodedU64Array::from(new_array))
                }
                EncodedU64Array::U32 { base, mut offsets } => {
                    if let Some(offset) = val.checked_sub(base)
                        && offset <= u32::MAX as u64
                    {
                        offsets.push(offset as u32);
                        return Ok(Self::Array(EncodedU64Array::U32 { base, offsets }));
                    }
                    let mut new_array: Vec<u64> =
                        offsets.into_iter().map(|o| base + o as u64).collect();
                    new_array.push(val);
                    Self::Array(EncodedU64Array::from(new_array))
                }
            },
        })
    }

    /// Delete a set of row ids from the segment.
    /// The row ids are assumed to be in the segment. (within the range, not
    /// already deleted.)
    /// They are also assumed to be ordered by appearance in the segment.
    pub fn delete(&self, vals: &[u64]) -> Self {
        // TODO: can we enforce these assumptions? or make them safer?
        debug_assert!(vals.iter().all(|&val| self.range().unwrap().contains(&val)));

        let make_new_iter = || {
            let mut vals_iter = vals.iter().copied().peekable();
            self.iter().filter(move |val| {
                if let Some(&next_val) = vals_iter.peek()
                    && next_val == *val
                {
                    vals_iter.next();
                    return false;
                }
                true
            })
        };
        let stats = Self::compute_stats(make_new_iter());
        Self::from_stats_and_sequence(stats, make_new_iter())
    }

    pub fn mask(&mut self, positions: &[u32]) {
        if positions.is_empty() {
            return;
        }
        if positions.len() == self.len() {
            *self = Self::Range(0..0);
            return;
        }
        let count = (self.len() - positions.len()) as u64;
        let sorted = match self {
            Self::Range(_) => true,
            Self::RangeWithHoles { .. } => true,
            Self::RangeWithBitmap { .. } => true,
            Self::SortedArray(_) => true,
            Self::Array(_) => false,
        };
        // To get minimum, need to find the first value that is not masked.
        let first_unmasked = (0..self.len())
            .zip(positions.iter().cycle())
            .find(|(sequential_i, i)| **i != *sequential_i as u32)
            .map(|(sequential_i, _)| sequential_i)
            .unwrap();
        let min = self.get(first_unmasked).unwrap();

        let last_unmasked = (0..self.len())
            .rev()
            .zip(positions.iter().rev().cycle())
            .filter(|(sequential_i, i)| **i != *sequential_i as u32)
            .map(|(sequential_i, _)| sequential_i)
            .next()
            .unwrap();
        let max = self.get(last_unmasked).unwrap();

        let stats = SegmentStats {
            min,
            max,
            count,
            sorted,
        };

        let mut positions = positions.iter().copied().peekable();
        let sequence = self.iter().enumerate().filter_map(move |(i, val)| {
            if let Some(next_pos) = positions.peek()
                && *next_pos == i as u32
            {
                positions.next();
                return None;
            }
            Some(val)
        });
        *self = Self::from_stats_and_sequence(stats, sequence)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_segments() {
        fn check_segment(values: &[u64], expected: &U64Segment) {
            let segment = U64Segment::from_slice(values);
            assert_eq!(segment, *expected);
            assert_eq!(values.len(), segment.len());

            let roundtripped = segment.iter().collect::<Vec<_>>();
            assert_eq!(roundtripped, values);

            let expected_min = values.iter().copied().min();
            let expected_max = values.iter().copied().max();
            match segment.range() {
                Some(range) => {
                    assert_eq!(range.start(), &expected_min.unwrap());
                    assert_eq!(range.end(), &expected_max.unwrap());
                }
                None => {
                    assert_eq!(expected_min, None);
                    assert_eq!(expected_max, None);
                }
            }

            for (i, value) in values.iter().enumerate() {
                assert_eq!(segment.get(i), Some(*value), "i = {}", i);
                assert_eq!(segment.position(*value), Some(i), "i = {}", i);
            }

            check_segment_iter(&segment);
        }

        fn check_segment_iter(segment: &U64Segment) {
            // Should be able to iterate forwards and backwards, and get the same thing.
            let forwards = segment.iter().collect::<Vec<_>>();
            let mut backwards = segment.iter().rev().collect::<Vec<_>>();
            backwards.reverse();
            assert_eq!(forwards, backwards);

            // Should be able to pull from both sides in lockstep.
            let mut expected = Vec::with_capacity(segment.len());
            let mut actual = Vec::with_capacity(segment.len());
            let mut iter = segment.iter();
            // Alternating forwards and backwards
            for i in 0..segment.len() {
                if i % 2 == 0 {
                    actual.push(iter.next().unwrap());
                    expected.push(segment.get(i / 2).unwrap());
                } else {
                    let i = segment.len() - 1 - i / 2;
                    actual.push(iter.next_back().unwrap());
                    expected.push(segment.get(i).unwrap());
                };
            }
            assert_eq!(expected, actual);
        }

        // Empty
        check_segment(&[], &U64Segment::Range(0..0));

        // Single value
        check_segment(&[42], &U64Segment::Range(42..43));

        // Contiguous range
        check_segment(
            &(100..200).collect::<Vec<_>>(),
            &U64Segment::Range(100..200),
        );

        // Range with a hole
        let values = (0..1000).filter(|&x| x != 100).collect::<Vec<_>>();
        check_segment(
            &values,
            &U64Segment::RangeWithHoles {
                range: 0..1000,
                holes: vec![100].into(),
            },
        );

        // Range with every other value missing
        let values = (0..1000).filter(|&x| x % 2 == 0).collect::<Vec<_>>();
        check_segment(
            &values,
            &U64Segment::RangeWithBitmap {
                range: 0..999,
                bitmap: Bitmap::from((0..999).map(|x| x % 2 == 0).collect::<Vec<_>>().as_slice()),
            },
        );

        // Sparse but sorted sequence
        check_segment(
            &[1, 7000, 24000],
            &U64Segment::SortedArray(vec![1, 7000, 24000].into()),
        );

        // Sparse unsorted sequence
        check_segment(
            &[7000, 1, 24000],
            &U64Segment::Array(vec![7000, 1, 24000].into()),
        );
    }

    #[test]
    fn test_segment_overflow_boundary() {
        // Sparse range spanning i64::MAX — the original overflow reproducer.
        // n_holes ≈ 2^63, which overflows `4 * n_holes as usize` without u128 arithmetic.
        let values: Vec<u64> = vec![0, 1, 2, 100, i64::MAX as u64];
        let segment = U64Segment::from_slice(&values);
        assert!(
            matches!(segment, U64Segment::SortedArray(_)),
            "sparse range spanning i64::MAX should be SortedArray, got {:?}",
            std::mem::discriminant(&segment)
        );
        assert_eq!(segment.len(), 5);
        assert_eq!(segment.iter().collect::<Vec<_>>(), values);

        // Two values at u64 extremes — triggers n_holes() total_slots overflow
        // (u64::MAX - 0 + 1 wraps to 0 without u128).
        let values: Vec<u64> = vec![0, u64::MAX];
        let segment = U64Segment::from_slice(&values);
        assert!(
            matches!(segment, U64Segment::SortedArray(_)),
            "full u64 span should be SortedArray, got {:?}",
            std::mem::discriminant(&segment)
        );
        assert_eq!(segment.len(), 2);
        assert_eq!(segment.iter().collect::<Vec<_>>(), values);

        // Small dense set near u64::MAX — cost estimation correctly prefers a
        // range-backed encoding, but Range<u64> cannot represent u64::MAX + 1
        // as the exclusive end. Must fall back to SortedArray.
        let values: Vec<u64> = vec![u64::MAX - 3, u64::MAX - 1, u64::MAX];
        let segment = U64Segment::from_slice(&values);
        assert!(
            matches!(segment, U64Segment::SortedArray(_)),
            "dense set near u64::MAX should be SortedArray (exclusive end unrepresentable), got {:?}",
            std::mem::discriminant(&segment)
        );
        assert_eq!(segment.len(), 3);
        assert_eq!(segment.iter().collect::<Vec<_>>(), values);

        // Single value at u64::MAX — contiguous range with n_holes == 0, but
        // exclusive end u64::MAX + 1 overflows.
        let values: Vec<u64> = vec![u64::MAX];
        let segment = U64Segment::from_slice(&values);
        assert!(
            matches!(segment, U64Segment::SortedArray(_)),
            "single u64::MAX should be SortedArray, got {:?}",
            std::mem::discriminant(&segment)
        );
        assert_eq!(segment.len(), 1);
        assert_eq!(segment.iter().collect::<Vec<_>>(), values);

        // Contiguous range ending just below u64::MAX — exclusive end is
        // representable, so Range encoding should still be used.
        let values: Vec<u64> = vec![u64::MAX - 3, u64::MAX - 2, u64::MAX - 1];
        let segment = U64Segment::from_slice(&values);
        assert_eq!(segment, U64Segment::Range((u64::MAX - 3)..u64::MAX));
        assert_eq!(segment.len(), 3);
        assert_eq!(segment.iter().collect::<Vec<_>>(), values);

        // Regression: normal dense range with few holes still picks RangeWithHoles.
        // Needs total_slots > 32 * n_holes for RangeWithHoles to beat RangeWithBitmap.
        let values: Vec<u64> = (100..1100).filter(|&x| x != 500).collect();
        let segment = U64Segment::from_slice(&values);
        assert_eq!(
            segment,
            U64Segment::RangeWithHoles {
                range: 100..1100,
                holes: vec![500].into(),
            }
        );
        assert_eq!(segment.len(), 999);
        assert_eq!(segment.iter().collect::<Vec<_>>(), values);

        // Regression: small dense range with hole picks RangeWithBitmap.
        let values: Vec<u64> = vec![100, 101, 102, 103, 105];
        let segment = U64Segment::from_slice(&values);
        assert!(
            matches!(segment, U64Segment::RangeWithBitmap { .. }),
            "small dense range with hole should be RangeWithBitmap, got {:?}",
            std::mem::discriminant(&segment)
        );
        assert_eq!(segment.len(), 5);
        assert_eq!(segment.iter().collect::<Vec<_>>(), values);
    }

    #[test]
    fn test_u128_byte_cost_to_usize() {
        assert_eq!(super::u128_byte_cost_to_usize(0), 0);
        assert_eq!(super::u128_byte_cost_to_usize(42), 42);
        assert_eq!(
            super::u128_byte_cost_to_usize(usize::MAX as u128),
            usize::MAX
        );
        assert_eq!(super::u128_byte_cost_to_usize(u128::MAX), usize::MAX);
    }

    #[test]
    fn test_sorted_sequence_sizes_sparse_span_saturates_range_with_holes_cost() {
        let stats = super::SegmentStats {
            min: 0,
            max: i64::MAX as u64,
            count: 5,
            sorted: true,
        };
        let sizes = U64Segment::sorted_sequence_sizes(&stats);
        assert_eq!(sizes[0], usize::MAX);
        assert!(sizes[2] < sizes[0]);
    }

    #[test]
    fn test_sorted_sequence_sizes_sorted_array_cost_saturates() {
        // Nearly full [0, u64::MAX] with one hole: count = u64::MAX, n_holes = 1.
        // SortedArray cost 24 + 2 * u64::MAX does not fit in usize on 64-bit.
        let stats = super::SegmentStats {
            min: 0,
            max: u64::MAX,
            count: u64::MAX,
            sorted: true,
        };
        let sizes = U64Segment::sorted_sequence_sizes(&stats);
        assert_eq!(sizes[2], usize::MAX);
    }

    #[test]
    fn test_sorted_sequence_sizes_full_span_bitmap_cost() {
        // Synthetic stats: full [0, u64::MAX] slot space; exercises `range_with_bitmap`
        // cost path (always fits in `usize` on 64-bit targets).
        let stats = super::SegmentStats {
            min: 0,
            max: u64::MAX,
            count: 1,
            sorted: true,
        };
        let sizes = U64Segment::sorted_sequence_sizes(&stats);
        assert!(sizes[1] < sizes[0]);
        assert!(sizes[1] < usize::MAX);
    }

    #[test]
    fn test_with_new_high() {
        // Test Range: contiguous sequence
        let segment = U64Segment::Range(10..20);

        // Test adding value that extends the range
        let result = segment.clone().with_new_high(20).unwrap();
        assert_eq!(result, U64Segment::Range(10..21));

        // Test adding value that creates holes
        let result = segment.with_new_high(25).unwrap();
        assert_eq!(
            result,
            U64Segment::RangeWithHoles {
                range: 10..26,
                holes: EncodedU64Array::U64(vec![20, 21, 22, 23, 24]),
            }
        );

        // Test RangeWithHoles: sequence with existing holes
        let segment = U64Segment::RangeWithHoles {
            range: 10..20,
            holes: EncodedU64Array::U64(vec![15, 17]),
        };

        // Test adding value that extends the range without new holes
        let result = segment.clone().with_new_high(20).unwrap();
        assert_eq!(
            result,
            U64Segment::RangeWithHoles {
                range: 10..21,
                holes: EncodedU64Array::U64(vec![15, 17]),
            }
        );

        // Test adding value that creates additional holes
        let result = segment.with_new_high(25).unwrap();
        assert_eq!(
            result,
            U64Segment::RangeWithHoles {
                range: 10..26,
                holes: EncodedU64Array::U64(vec![15, 17, 20, 21, 22, 23, 24]),
            }
        );

        // Test RangeWithBitmap: sequence with bitmap representation
        let mut bitmap = Bitmap::new_full(10);
        bitmap.clear(3); // Clear position 3 (value 13)
        bitmap.clear(7); // Clear position 7 (value 17)
        let segment = U64Segment::RangeWithBitmap {
            range: 10..20,
            bitmap,
        };

        // Test adding value that extends the range without new holes
        let result = segment.clone().with_new_high(20).unwrap();
        let expected_bitmap = {
            let mut b = Bitmap::new_full(11);
            b.clear(3); // Clear position 3 (value 13)
            b.clear(7); // Clear position 7 (value 17)
            b
        };
        assert_eq!(
            result,
            U64Segment::RangeWithBitmap {
                range: 10..21,
                bitmap: expected_bitmap,
            }
        );

        // Test adding value that creates additional holes
        let result = segment.with_new_high(25).unwrap();
        let expected_bitmap = {
            let mut b = Bitmap::new_full(16);
            b.clear(3); // Clear position 3 (value 13)
            b.clear(7); // Clear position 7 (value 17)
            // Clear positions 10-14 (values 20-24)
            for i in 10..15 {
                b.clear(i);
            }
            b
        };
        assert_eq!(
            result,
            U64Segment::RangeWithBitmap {
                range: 10..26,
                bitmap: expected_bitmap,
            }
        );

        // Test SortedArray: sparse sorted sequence
        let segment = U64Segment::SortedArray(EncodedU64Array::U64(vec![1, 5, 10]));

        let result = segment.with_new_high(15).unwrap();
        assert_eq!(
            result,
            U64Segment::SortedArray(EncodedU64Array::U64(vec![1, 5, 10, 15]))
        );

        // Test Array: unsorted sequence
        let segment = U64Segment::Array(EncodedU64Array::U64(vec![10, 5, 1]));

        let result = segment.with_new_high(15).unwrap();
        assert_eq!(
            result,
            U64Segment::Array(EncodedU64Array::U64(vec![10, 5, 1, 15]))
        );

        // Test edge cases
        // Empty segment
        let segment = U64Segment::Range(0..0);
        let result = segment.with_new_high(5).unwrap();
        assert_eq!(result, U64Segment::Range(5..6));

        // Single value segment
        let segment = U64Segment::Range(42..43);
        let result = segment.with_new_high(50).unwrap();
        assert_eq!(
            result,
            U64Segment::RangeWithHoles {
                range: 42..51,
                holes: EncodedU64Array::U64(vec![43, 44, 45, 46, 47, 48, 49]),
            }
        );
    }

    #[test]
    fn test_with_new_high_assertion() {
        let segment = U64Segment::Range(10..20);
        // This should return an error because 15 is not higher than the current maximum 19
        let result = segment.with_new_high(15);
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert!(
            error
                .to_string()
                .contains("New value 15 must be higher than current maximum 19")
        );
    }

    #[test]
    fn test_with_new_high_assertion_equal() {
        let segment = U64Segment::Range(1..6);
        // This should return an error because 5 is not higher than the current maximum 5
        let result = segment.with_new_high(5);
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert!(
            error
                .to_string()
                .contains("New value 5 must be higher than current maximum 5")
        );
    }

    #[test]
    fn test_contains() {
        // Test Range: contiguous sequence
        let segment = U64Segment::Range(10..20);
        assert!(segment.contains(10), "Should contain 10");
        assert!(segment.contains(15), "Should contain 15");
        assert!(segment.contains(19), "Should contain 19");
        assert!(!segment.contains(9), "Should not contain 9");
        assert!(!segment.contains(20), "Should not contain 20");
        assert!(!segment.contains(25), "Should not contain 25");

        // Test RangeWithHoles: sequence with holes
        let segment = U64Segment::RangeWithHoles {
            range: 10..20,
            holes: EncodedU64Array::U64(vec![15, 17]),
        };
        assert!(segment.contains(10), "Should contain 10");
        assert!(segment.contains(14), "Should contain 14");
        assert!(!segment.contains(15), "Should not contain 15 (hole)");
        assert!(segment.contains(16), "Should contain 16");
        assert!(!segment.contains(17), "Should not contain 17 (hole)");
        assert!(segment.contains(18), "Should contain 18");
        assert!(
            !segment.contains(20),
            "Should not contain 20 (out of range)"
        );

        // Test RangeWithBitmap: sequence with bitmap
        let mut bitmap = Bitmap::new_full(10);
        bitmap.clear(3); // Clear position 3 (value 13)
        bitmap.clear(7); // Clear position 7 (value 17)
        let segment = U64Segment::RangeWithBitmap {
            range: 10..20,
            bitmap,
        };
        assert!(segment.contains(10), "Should contain 10");
        assert!(segment.contains(12), "Should contain 12");
        assert!(
            !segment.contains(13),
            "Should not contain 13 (cleared in bitmap)"
        );
        assert!(segment.contains(16), "Should contain 16");
        assert!(
            !segment.contains(17),
            "Should not contain 17 (cleared in bitmap)"
        );
        assert!(segment.contains(19), "Should contain 19");
        assert!(
            !segment.contains(20),
            "Should not contain 20 (out of range)"
        );

        // Test SortedArray: sparse sorted sequence
        let segment = U64Segment::SortedArray(EncodedU64Array::U64(vec![1, 5, 10]));
        assert!(segment.contains(1), "Should contain 1");
        assert!(segment.contains(5), "Should contain 5");
        assert!(segment.contains(10), "Should contain 10");
        assert!(!segment.contains(0), "Should not contain 0");
        assert!(!segment.contains(3), "Should not contain 3");
        assert!(!segment.contains(15), "Should not contain 15");

        // Test Array: unsorted sequence
        let segment = U64Segment::Array(EncodedU64Array::U64(vec![10, 5, 1]));
        assert!(segment.contains(1), "Should contain 1");
        assert!(segment.contains(5), "Should contain 5");
        assert!(segment.contains(10), "Should contain 10");
        assert!(!segment.contains(0), "Should not contain 0");
        assert!(!segment.contains(3), "Should not contain 3");
        assert!(!segment.contains(15), "Should not contain 15");

        // Test empty segment
        let segment = U64Segment::Range(0..0);
        assert!(
            !segment.contains(0),
            "Empty segment should not contain anything"
        );
        assert!(
            !segment.contains(5),
            "Empty segment should not contain anything"
        );
    }
}
