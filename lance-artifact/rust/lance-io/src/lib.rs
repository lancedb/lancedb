// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors
#![recursion_limit = "512"]
use std::{
    ops::{Range, RangeFrom, RangeFull, RangeTo},
    sync::Arc,
};

use arrow::datatypes::UInt32Type;
use arrow_array::{PrimitiveArray, UInt32Array};

use lance_core::{Error, Result};

pub mod ffi;
pub mod local;
pub mod object_reader;
pub mod object_store;
pub mod object_writer;
pub mod scheduler;
pub mod spill;
pub mod stream;
#[cfg(test)]
pub mod testing;
pub mod traits;
#[cfg(target_os = "linux")]
pub mod uring;
pub mod utils;

pub use scheduler::{bytes_read_counter, iops_counter};

/// Defines a selection of rows to read from a file/batch
#[derive(Debug, Clone, PartialEq, Default)]
pub enum ReadBatchParams {
    /// Select a contiguous range of rows
    Range(Range<usize>),
    /// Select multiple contiguous ranges of rows
    Ranges(Arc<[Range<u64>]>),
    /// Select all rows (this is the default)
    #[default]
    RangeFull,
    /// Select all rows up to a given index
    RangeTo(RangeTo<usize>),
    /// Select all rows starting at a given index
    RangeFrom(RangeFrom<usize>),
    /// Select scattered non-contiguous rows
    Indices(UInt32Array),
}

impl std::fmt::Display for ReadBatchParams {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::Range(r) => write!(f, "Range({}..{})", r.start, r.end),
            Self::Ranges(ranges) => {
                let mut ranges_str = ranges.iter().fold(String::new(), |mut acc, r| {
                    acc.push_str(&format!("{}..{}", r.start, r.end));
                    acc.push(',');
                    acc
                });
                // Remove the trailing comma
                if !ranges_str.is_empty() {
                    ranges_str.pop();
                }
                write!(f, "Ranges({})", ranges_str)
            }
            Self::RangeFull => write!(f, "RangeFull"),
            Self::RangeTo(r) => write!(f, "RangeTo({})", r.end),
            Self::RangeFrom(r) => write!(f, "RangeFrom({})", r.start),
            Self::Indices(indices) => {
                let mut indices_str = indices.values().iter().fold(String::new(), |mut acc, v| {
                    acc.push_str(&v.to_string());
                    acc.push(',');
                    acc
                });
                if !indices_str.is_empty() {
                    indices_str.pop();
                }
                write!(f, "Indices({})", indices_str)
            }
        }
    }
}

impl From<&[u32]> for ReadBatchParams {
    fn from(value: &[u32]) -> Self {
        Self::Indices(UInt32Array::from_iter_values(value.iter().copied()))
    }
}

impl From<UInt32Array> for ReadBatchParams {
    fn from(value: UInt32Array) -> Self {
        Self::Indices(value)
    }
}

impl From<RangeFull> for ReadBatchParams {
    fn from(_: RangeFull) -> Self {
        Self::RangeFull
    }
}

impl From<Range<usize>> for ReadBatchParams {
    fn from(r: Range<usize>) -> Self {
        Self::Range(r)
    }
}

impl From<RangeTo<usize>> for ReadBatchParams {
    fn from(r: RangeTo<usize>) -> Self {
        Self::RangeTo(r)
    }
}

impl From<RangeFrom<usize>> for ReadBatchParams {
    fn from(r: RangeFrom<usize>) -> Self {
        Self::RangeFrom(r)
    }
}

impl From<&Self> for ReadBatchParams {
    fn from(params: &Self) -> Self {
        params.clone()
    }
}

impl ReadBatchParams {
    /// Validate that the selection is valid given the length of the batch
    pub fn valid_given_len(&self, len: usize) -> bool {
        match self {
            Self::Indices(indices) => indices.iter().all(|i| i.unwrap_or(0) < len as u32),
            Self::Range(r) => r.start < len && r.end <= len,
            Self::Ranges(ranges) => ranges.iter().all(|r| r.end <= len as u64),
            Self::RangeFull => true,
            Self::RangeTo(r) => r.end <= len,
            Self::RangeFrom(r) => r.start < len,
        }
    }

    /// Slice the selection
    ///
    /// For example, given ReadBatchParams::RangeFull and slice(10, 20), the output will be
    /// ReadBatchParams::Range(10..20)
    ///
    /// Given ReadBatchParams::Range(10..20) and slice(5, 3), the output will be
    /// ReadBatchParams::Range(15..18)
    ///
    /// Given ReadBatchParams::RangeTo(20) and slice(10, 5), the output will be
    /// ReadBatchParams::Range(10..15)
    ///
    /// Given ReadBatchParams::RangeFrom(20) and slice(10, 5), the output will be
    /// ReadBatchParams::Range(30..35)
    ///
    /// Given ReadBatchParams::Indices([1, 3, 5, 7, 9]) and slice(1, 3), the output will be
    /// ReadBatchParams::Indices([3, 5, 7])
    ///
    /// You cannot slice beyond the bounds of the selection and an attempt to do so will
    /// return an error.
    pub fn slice(&self, start: usize, length: usize) -> Result<Self> {
        let out_of_bounds = |size: usize| {
            Err(Error::invalid_input_source(
                format!(
                    "Cannot slice from {} with length {} given a selection of size {}",
                    start, length, size
                )
                .into(),
            ))
        };

        match self {
            Self::Indices(indices) => {
                if start + length > indices.len() {
                    return out_of_bounds(indices.len());
                }
                Ok(Self::Indices(indices.slice(start, length)))
            }
            Self::Range(r) => {
                if (r.start + start + length) > r.end {
                    return out_of_bounds(r.end - r.start);
                }
                Ok(Self::Range((r.start + start)..(r.start + start + length)))
            }
            Self::Ranges(ranges) => {
                let mut new_ranges = Vec::with_capacity(ranges.len());
                let mut to_skip = start as u64;
                let mut to_take = length as u64;
                let mut total_num_rows = 0;
                for r in ranges.as_ref() {
                    let num_rows = r.end - r.start;
                    total_num_rows += num_rows;
                    if to_skip > num_rows {
                        to_skip -= num_rows;
                        continue;
                    }
                    let new_start = r.start + to_skip;
                    let to_take_this_range = (num_rows - to_skip).min(to_take);
                    new_ranges.push(new_start..(new_start + to_take_this_range));
                    to_skip = 0;
                    to_take -= to_take_this_range;
                    if to_take == 0 {
                        break;
                    }
                }
                if to_take > 0 {
                    out_of_bounds(total_num_rows as usize)
                } else {
                    Ok(Self::Ranges(new_ranges.into()))
                }
            }
            Self::RangeFull => Ok(Self::Range(start..(start + length))),
            Self::RangeTo(range) => {
                if start + length > range.end {
                    return out_of_bounds(range.end);
                }
                Ok(Self::Range(start..(start + length)))
            }
            Self::RangeFrom(r) => {
                // No way to validate out_of_bounds, assume caller will do so
                Ok(Self::Range((r.start + start)..(r.start + start + length)))
            }
        }
    }

    /// Convert a read range into a vector of row offsets
    ///
    /// RangeFull and RangeFrom are unbounded and cannot be converted into row offsets
    /// and any attempt to do so will return an error.  Call slice first
    pub fn to_offsets(&self) -> Result<PrimitiveArray<UInt32Type>> {
        match self {
            Self::Indices(indices) => Ok(indices.clone()),
            Self::Range(r) => Ok(UInt32Array::from(Vec::from_iter(
                r.start as u32..r.end as u32,
            ))),
            Self::Ranges(ranges) => {
                let num_rows = ranges
                    .iter()
                    .map(|r| (r.end - r.start) as usize)
                    .sum::<usize>();
                let mut offsets = Vec::with_capacity(num_rows);
                for r in ranges.as_ref() {
                    offsets.extend(r.start as u32..r.end as u32);
                }
                Ok(UInt32Array::from(offsets))
            }
            Self::RangeFull => Err(Error::invalid_input("cannot materialize RangeFull")),
            Self::RangeTo(r) => Ok(UInt32Array::from(Vec::from_iter(0..r.end as u32))),
            Self::RangeFrom(_) => Err(Error::invalid_input("cannot materialize RangeFrom")),
        }
    }

    pub fn iter_offset_ranges<'a>(
        &'a self,
    ) -> Result<Box<dyn Iterator<Item = Range<u32>> + Send + 'a>> {
        match self {
            Self::Indices(indices) => Ok(Box::new(indices.values().iter().map(|i| *i..(*i + 1)))),
            Self::Range(r) => Ok(Box::new(std::iter::once(r.start as u32..r.end as u32))),
            Self::Ranges(ranges) => Ok(Box::new(
                ranges.iter().map(|r| r.start as u32..r.end as u32),
            )),
            Self::RangeFull => Err(Error::invalid_input("cannot materialize RangeFull")),
            Self::RangeTo(r) => Ok(Box::new(std::iter::once(0..r.end as u32))),
            Self::RangeFrom(_) => Err(Error::invalid_input("cannot materialize RangeFrom")),
        }
    }

    /// Convert a read range into a vector of row ranges
    pub fn to_ranges(&self) -> Result<Vec<Range<u64>>> {
        match self {
            Self::Indices(indices) => Ok(indices
                .values()
                .iter()
                .map(|i| *i as u64..(*i + 1) as u64)
                .collect()),
            Self::Range(r) => Ok(vec![r.start as u64..r.end as u64]),
            Self::Ranges(ranges) => Ok(ranges.to_vec()),
            Self::RangeFull => Err(Error::invalid_input("cannot materialize RangeFull")),
            Self::RangeTo(r) => Ok(vec![0..r.end as u64]),
            Self::RangeFrom(_) => Err(Error::invalid_input("cannot materialize RangeFrom")),
        }
    }

    /// Same thing as to_offsets but the caller knows the total number of rows in the file
    ///
    /// This makes it possible to materialize RangeFull / RangeFrom
    pub fn to_offsets_total(&self, total: u32) -> PrimitiveArray<UInt32Type> {
        match self {
            Self::Indices(indices) => indices.clone(),
            Self::Range(r) => UInt32Array::from_iter_values(r.start as u32..r.end as u32),
            Self::Ranges(ranges) => {
                let num_rows = ranges
                    .iter()
                    .map(|r| (r.end - r.start) as usize)
                    .sum::<usize>();
                let mut offsets = Vec::with_capacity(num_rows);
                for r in ranges.as_ref() {
                    offsets.extend(r.start as u32..r.end as u32);
                }
                UInt32Array::from(offsets)
            }
            Self::RangeFull => UInt32Array::from_iter_values(0_u32..total),
            Self::RangeTo(r) => UInt32Array::from_iter_values(0..r.end as u32),
            Self::RangeFrom(r) => UInt32Array::from_iter_values(r.start as u32..total),
        }
    }
}

#[cfg(test)]
mod test {
    use std::ops::{RangeFrom, RangeTo};

    use arrow_array::UInt32Array;

    use crate::ReadBatchParams;

    #[test]
    fn test_params_slice() {
        let params = ReadBatchParams::Ranges(vec![0..15, 20..40].into());
        let sliced = params.slice(10, 10).unwrap();
        assert_eq!(sliced, ReadBatchParams::Ranges(vec![10..15, 20..25].into()));
    }

    #[test]
    fn test_params_to_offsets() {
        let check = |params: ReadBatchParams, base_offset, length, expected: Vec<u32>| {
            let offsets = params
                .slice(base_offset, length)
                .unwrap()
                .to_offsets()
                .unwrap();
            let expected = UInt32Array::from(expected);
            assert_eq!(offsets, expected);
        };

        check(ReadBatchParams::RangeFull, 0, 100, (0..100).collect());
        check(ReadBatchParams::RangeFull, 50, 100, (50..150).collect());
        check(
            ReadBatchParams::RangeFrom(RangeFrom { start: 500 }),
            0,
            100,
            (500..600).collect(),
        );
        check(
            ReadBatchParams::RangeFrom(RangeFrom { start: 500 }),
            100,
            100,
            (600..700).collect(),
        );
        check(
            ReadBatchParams::RangeTo(RangeTo { end: 800 }),
            0,
            100,
            (0..100).collect(),
        );
        check(
            ReadBatchParams::RangeTo(RangeTo { end: 800 }),
            200,
            100,
            (200..300).collect(),
        );
        check(
            ReadBatchParams::Indices(UInt32Array::from(vec![1, 3, 5, 7, 9])),
            0,
            2,
            vec![1, 3],
        );
        check(
            ReadBatchParams::Indices(UInt32Array::from(vec![1, 3, 5, 7, 9])),
            2,
            2,
            vec![5, 7],
        );

        let check_error = |params: ReadBatchParams, base_offset, length| {
            assert!(params.slice(base_offset, length).is_err());
        };

        check_error(ReadBatchParams::Indices(UInt32Array::from(vec![1])), 0, 2);
        check_error(ReadBatchParams::Indices(UInt32Array::from(vec![1])), 1, 1);
        check_error(ReadBatchParams::Range(0..10), 5, 6);
        check_error(ReadBatchParams::RangeTo(RangeTo { end: 10 }), 5, 6);

        assert!(ReadBatchParams::RangeFull.to_offsets().is_err());
        assert!(
            ReadBatchParams::RangeFrom(RangeFrom { start: 10 })
                .to_offsets()
                .is_err()
        );
    }
}
