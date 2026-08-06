// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Statistics accumulator for streams of Arrow arrays.
//!
//! Tracks min, max, null_count, and optional nan_count across batches of arrays sharing
//! the same [`DataType`]. Uses [`ArrowScalar`] for extrema tracking.
//!
//! # Example
//!
//! ```
//! use std::sync::Arc;
//! use arrow_array::{ArrayRef, Int32Array};
//! use arrow_schema::DataType;
//! use lance_arrow_stats::StatisticsAccumulator;
//!
//! let mut acc = StatisticsAccumulator::new(&DataType::Int32);
//! let array: ArrayRef = Arc::new(Int32Array::from(vec![Some(1), None, Some(3)]));
//! acc.update(&array).unwrap();
//!
//! let stats = acc.finish();
//! assert_eq!(stats.null_count, 1);
//! assert!(stats.nan_count.is_none());
//! ```
//!
//! # Data Type Support
//!
//! All basic types are supported.  Every data type supports `null_count` and `buffer_memory`.
//! The `nan_count` field is `Some` only for floating-point types (including lists of floats)
//! and `None` for all other types.
//!
//! # List Types
//!
//! List types are supported.  The `item_nulls` field will be set to the number of null items within list entries.
//! This will be `Some` only for list types.
//!
//! # String Types / Binary Types
//!
//! String & binary types are supported.  Binary comparison will be used for string types to calculate the min and the
//! max.  This works for ASCII but may be surprising for special characters.  For example, "é" would sort after "z".
//!
//! In addition, string view and binary view types are supported.
//!
//! # Unsupported Types
//!
//! Special encodings (dictionary, run end encoded, view types) are not currently fully supported.  The min and max will
//! be set to `None` for these types.  The `nan_count` will also be `None` unless the underlying
//! type is a floating-point type.
//!
//! Structs are not currently supported.

mod nan;

use arrow_array::cast::AsArray;
use arrow_array::types::*;
use arrow_array::{Array, ArrayRef};
use arrow_schema::{ArrowError, DataType};
use lance_arrow_scalar::ArrowScalar;

use nan::count_nans;

type Result<T> = std::result::Result<T, ArrowError>;

/// Returns true if the data type can contain NaN values (float primitives
/// or list types whose items are floats).
fn can_have_nan(data_type: &DataType) -> bool {
    match data_type {
        DataType::Float16 | DataType::Float32 | DataType::Float64 => true,
        DataType::List(f) | DataType::LargeList(f) => can_have_nan(f.data_type()),
        DataType::FixedSizeList(f, _) => can_have_nan(f.data_type()),
        _ => false,
    }
}

/// Accumulated statistics for a stream of Arrow arrays of a single [`DataType`].
#[derive(Debug, Clone)]
pub struct StatisticsAccumulator {
    data_type: DataType,
    min: Option<ArrowScalar>,
    max: Option<ArrowScalar>,
    null_count: u64,
    /// Count of NaN values. `Some` only for floating-point types (or lists of floats).
    nan_count: Option<u64>,
    /// Number of null items within list entries. `Some` only for list types.
    item_nulls: Option<u64>,
    buffer_memory: u64,
}

/// Snapshot of accumulated statistics.
#[derive(Debug, Clone)]
pub struct Statistics {
    pub min: Option<ArrowScalar>,
    pub max: Option<ArrowScalar>,
    pub null_count: u64,
    /// Count of NaN values. `None` for non-floating-point types.
    pub nan_count: Option<u64>,
    /// Number of null items within list entries. `None` for non-list types.
    pub item_nulls: Option<u64>,
    /// Total buffer memory in bytes across all arrays seen by this accumulator.
    pub buffer_memory: u64,
}

impl StatisticsAccumulator {
    /// Create a new accumulator for arrays of the given data type.
    pub fn new(data_type: &DataType) -> Self {
        let item_nulls = match data_type {
            DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _) => Some(0),
            _ => None,
        };
        let nan_count = if can_have_nan(data_type) {
            Some(0)
        } else {
            None
        };
        Self {
            data_type: data_type.clone(),
            min: None,
            max: None,
            null_count: 0,
            nan_count,
            item_nulls,
            buffer_memory: 0,
        }
    }

    /// Returns the data type this accumulator expects.
    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }

    /// Update with a new batch of values.
    pub fn update(&mut self, array: &ArrayRef) -> Result<()> {
        if array.data_type() != &self.data_type {
            return Err(ArrowError::InvalidArgumentError(format!(
                "Type mismatch: expected {:?}, got {:?}",
                self.data_type,
                array.data_type()
            )));
        }

        if array.is_empty() {
            return Ok(());
        }

        self.buffer_memory += array.get_buffer_memory_size() as u64;
        self.null_count += array.null_count() as u64;

        match array.data_type() {
            DataType::List(_) => {
                let list = array.as_list::<i32>();
                self.update_items(
                    (0..list.len())
                        .filter(|&i| !list.is_null(i))
                        .map(|i| list.value(i)),
                )
            }
            DataType::LargeList(_) => {
                let list = array.as_list::<i64>();
                self.update_items(
                    (0..list.len())
                        .filter(|&i| !list.is_null(i))
                        .map(|i| list.value(i)),
                )
            }
            DataType::FixedSizeList(_, _) => {
                let list = array.as_fixed_size_list();
                self.update_items(
                    (0..list.len())
                        .filter(|&i| !list.is_null(i))
                        .map(|i| list.value(i)),
                )
            }
            _ => {
                if let Some(ref mut nan_count) = self.nan_count {
                    *nan_count += count_nans(array);
                }
                let (batch_min, batch_max) = find_min_max(array)?;
                self.update_min(batch_min);
                self.update_max(batch_max);
                Ok(())
            }
        }
    }

    /// Process items from list entries, updating min/max, nan_count, and item_nulls.
    fn update_items(&mut self, items: impl Iterator<Item = ArrayRef>) -> Result<()> {
        for item_array in items {
            self.update_item(&item_array)?;
        }
        Ok(())
    }

    /// Process a single item array. If it is itself a list type, recurse into
    /// its non-null entries; otherwise treat it as a leaf and compute min/max.
    fn update_item(&mut self, item_array: &ArrayRef) -> Result<()> {
        if item_array.is_empty() {
            return Ok(());
        }
        if let Some(ref mut item_nulls) = self.item_nulls {
            *item_nulls += item_array.null_count() as u64;
        }
        match item_array.data_type() {
            DataType::List(_) => {
                let list = item_array.as_list::<i32>();
                for i in 0..list.len() {
                    if !list.is_null(i) {
                        self.update_item(&list.value(i))?;
                    }
                }
            }
            DataType::LargeList(_) => {
                let list = item_array.as_list::<i64>();
                for i in 0..list.len() {
                    if !list.is_null(i) {
                        self.update_item(&list.value(i))?;
                    }
                }
            }
            DataType::FixedSizeList(_, _) => {
                let list = item_array.as_fixed_size_list();
                for i in 0..list.len() {
                    if !list.is_null(i) {
                        self.update_item(&list.value(i))?;
                    }
                }
            }
            _ => {
                if let Some(ref mut nan_count) = self.nan_count {
                    *nan_count += count_nans(item_array);
                }
                let (batch_min, batch_max) = find_min_max(item_array)?;
                self.update_min(batch_min);
                self.update_max(batch_max);
            }
        }
        Ok(())
    }

    fn update_min(&mut self, batch_min: Option<ArrowScalar>) {
        if let Some(new_min) = batch_min {
            self.min = Some(match self.min.take() {
                Some(cur) if cur <= new_min => cur,
                _ => new_min,
            });
        }
    }

    fn update_max(&mut self, batch_max: Option<ArrowScalar>) {
        if let Some(new_max) = batch_max {
            self.max = Some(match self.max.take() {
                Some(cur) if cur >= new_max => cur,
                _ => new_max,
            });
        }
    }

    /// Merge another accumulator into this one.
    pub fn merge(&mut self, other: &Self) -> Result<()> {
        if self.data_type != other.data_type {
            return Err(ArrowError::InvalidArgumentError(format!(
                "Type mismatch: expected {:?}, got {:?}",
                self.data_type, other.data_type
            )));
        }

        self.null_count += other.null_count;
        if let (Some(a), Some(b)) = (&mut self.nan_count, other.nan_count) {
            *a += b;
        }
        self.buffer_memory += other.buffer_memory;

        if let (Some(a), Some(b)) = (&mut self.item_nulls, other.item_nulls) {
            *a += b;
        }

        if let Some(ref other_min) = other.min {
            self.min = Some(match self.min.take() {
                Some(cur) if cur <= *other_min => cur,
                _ => other_min.clone(),
            });
        }

        if let Some(ref other_max) = other.max {
            self.max = Some(match self.max.take() {
                Some(cur) if cur >= *other_max => cur,
                _ => other_max.clone(),
            });
        }

        Ok(())
    }

    /// Consume the accumulator and return a statistics snapshot.
    pub fn finish(self) -> Statistics {
        Statistics {
            min: self.min,
            max: self.max,
            null_count: self.null_count,
            nan_count: self.nan_count,
            item_nulls: self.item_nulls,
            buffer_memory: self.buffer_memory,
        }
    }

    /// Return a snapshot of the current statistics without consuming the accumulator.
    pub fn statistics(&self) -> Statistics {
        Statistics {
            min: self.min.clone(),
            max: self.max.clone(),
            null_count: self.null_count,
            nan_count: self.nan_count,
            item_nulls: self.item_nulls,
            buffer_memory: self.buffer_memory,
        }
    }

    /// Reset all statistics back to initial state.
    pub fn reset(&mut self) {
        self.min = None;
        self.max = None;
        self.null_count = 0;
        if let Some(ref mut nan_count) = self.nan_count {
            *nan_count = 0;
        }
        if let Some(ref mut item_nulls) = self.item_nulls {
            *item_nulls = 0;
        }
        self.buffer_memory = 0;
    }
}

macro_rules! find_extrema_primitive {
    ($array:expr, $arrow_type:ty) => {{
        let typed = $array.as_primitive::<$arrow_type>();
        let mut min_idx: Option<usize> = None;
        let mut max_idx: Option<usize> = None;
        let mut min_val = None;
        let mut max_val = None;
        for i in 0..typed.len() {
            if typed.is_null(i) {
                continue;
            }
            let v = typed.value(i);
            if min_val.is_none() || v < *min_val.as_ref().unwrap() {
                min_val = Some(v);
                min_idx = Some(i);
            }
            if max_val.is_none() || v > *max_val.as_ref().unwrap() {
                max_val = Some(v);
                max_idx = Some(i);
            }
        }
        (min_idx, max_idx)
    }};
}

macro_rules! find_extrema_float {
    ($array:expr, $arrow_type:ty) => {{
        let typed = $array.as_primitive::<$arrow_type>();
        let mut min_idx: Option<usize> = None;
        let mut max_idx: Option<usize> = None;
        let mut min_val = None;
        let mut max_val = None;
        for i in 0..typed.len() {
            if typed.is_null(i) {
                continue;
            }
            let v = typed.value(i);
            if v.is_nan() {
                continue;
            }
            // Use total_cmp for a consistent total ordering that
            // distinguishes -0.0 from 0.0 (matching ArrowScalar's Ord).
            if min_val.is_none()
                || v.total_cmp(min_val.as_ref().unwrap()) == std::cmp::Ordering::Less
            {
                min_val = Some(v);
                min_idx = Some(i);
            }
            if max_val.is_none()
                || v.total_cmp(max_val.as_ref().unwrap()) == std::cmp::Ordering::Greater
            {
                max_val = Some(v);
                max_idx = Some(i);
            }
        }
        (min_idx, max_idx)
    }};
}

macro_rules! find_extrema_bytes {
    ($array:expr, $cast:ident :: < $offset:ty >) => {{
        let typed = $array.$cast::<$offset>();
        let mut min_idx: Option<usize> = None;
        let mut max_idx: Option<usize> = None;
        let mut min_val = None;
        let mut max_val = None;
        for i in 0..typed.len() {
            if typed.is_null(i) {
                continue;
            }
            let v = typed.value(i);
            if min_val.is_none() || v < min_val.unwrap() {
                min_val = Some(v);
                min_idx = Some(i);
            }
            if max_val.is_none() || v > max_val.unwrap() {
                max_val = Some(v);
                max_idx = Some(i);
            }
        }
        (min_idx, max_idx)
    }};
}

fn find_min_max(array: &ArrayRef) -> Result<(Option<ArrowScalar>, Option<ArrowScalar>)> {
    let (min_idx, max_idx) = find_min_max_indices(array)?;

    let min_scalar = min_idx
        .map(|i| ArrowScalar::try_new(array, i))
        .transpose()?;
    let max_scalar = max_idx
        .map(|i| ArrowScalar::try_new(array, i))
        .transpose()?;

    Ok((min_scalar, max_scalar))
}

fn find_min_max_indices(array: &ArrayRef) -> Result<(Option<usize>, Option<usize>)> {
    use DataType::*;

    let result = match array.data_type() {
        // Integer types
        Int8 => find_extrema_primitive!(array, Int8Type),
        Int16 => find_extrema_primitive!(array, Int16Type),
        Int32 => find_extrema_primitive!(array, Int32Type),
        Int64 => find_extrema_primitive!(array, Int64Type),
        UInt8 => find_extrema_primitive!(array, UInt8Type),
        UInt16 => find_extrema_primitive!(array, UInt16Type),
        UInt32 => find_extrema_primitive!(array, UInt32Type),
        UInt64 => find_extrema_primitive!(array, UInt64Type),

        // Float types (skip NaN)
        Float16 => find_extrema_float!(array, Float16Type),
        Float32 => find_extrema_float!(array, Float32Type),
        Float64 => find_extrema_float!(array, Float64Type),

        // Temporal types
        Date32 => find_extrema_primitive!(array, Date32Type),
        Date64 => find_extrema_primitive!(array, Date64Type),
        Time32(arrow_schema::TimeUnit::Second) => {
            find_extrema_primitive!(array, Time32SecondType)
        }
        Time32(arrow_schema::TimeUnit::Millisecond) => {
            find_extrema_primitive!(array, Time32MillisecondType)
        }
        Time64(arrow_schema::TimeUnit::Microsecond) => {
            find_extrema_primitive!(array, Time64MicrosecondType)
        }
        Time64(arrow_schema::TimeUnit::Nanosecond) => {
            find_extrema_primitive!(array, Time64NanosecondType)
        }
        Timestamp(arrow_schema::TimeUnit::Second, _) => {
            find_extrema_primitive!(array, TimestampSecondType)
        }
        Timestamp(arrow_schema::TimeUnit::Millisecond, _) => {
            find_extrema_primitive!(array, TimestampMillisecondType)
        }
        Timestamp(arrow_schema::TimeUnit::Microsecond, _) => {
            find_extrema_primitive!(array, TimestampMicrosecondType)
        }
        Timestamp(arrow_schema::TimeUnit::Nanosecond, _) => {
            find_extrema_primitive!(array, TimestampNanosecondType)
        }
        Duration(arrow_schema::TimeUnit::Second) => {
            find_extrema_primitive!(array, DurationSecondType)
        }
        Duration(arrow_schema::TimeUnit::Millisecond) => {
            find_extrema_primitive!(array, DurationMillisecondType)
        }
        Duration(arrow_schema::TimeUnit::Microsecond) => {
            find_extrema_primitive!(array, DurationMicrosecondType)
        }
        Duration(arrow_schema::TimeUnit::Nanosecond) => {
            find_extrema_primitive!(array, DurationNanosecondType)
        }

        // Boolean
        Boolean => {
            let typed = array.as_boolean();
            let mut min_idx: Option<usize> = None;
            let mut max_idx: Option<usize> = None;
            let mut min_val: Option<bool> = None;
            let mut max_val: Option<bool> = None;
            for i in 0..typed.len() {
                if typed.is_null(i) {
                    continue;
                }
                let v = typed.value(i);
                if min_val.is_none() || (!v && min_val.unwrap()) {
                    min_val = Some(v);
                    min_idx = Some(i);
                }
                if max_val.is_none() || (v && !max_val.unwrap()) {
                    max_val = Some(v);
                    max_idx = Some(i);
                }
            }
            (min_idx, max_idx)
        }

        // String types
        Utf8 => find_extrema_bytes!(array, as_string::<i32>),
        LargeUtf8 => find_extrema_bytes!(array, as_string::<i64>),

        // Binary types
        Binary => find_extrema_bytes!(array, as_binary::<i32>),
        LargeBinary => find_extrema_bytes!(array, as_binary::<i64>),

        // For unsupported types we skip min/max (and nan_count).
        // null_count and buffer_memory are already tracked above.
        _ => return Ok((None, None)),
    };

    Ok(result)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::*;
    use arrow_schema::DataType;
    use rstest::rstest;

    use super::*;

    #[test]
    fn test_empty_array() {
        let mut acc = StatisticsAccumulator::new(&DataType::Int32);
        let array: ArrayRef = Arc::new(Int32Array::from(Vec::<i32>::new()));
        acc.update(&array).unwrap();
        let stats = acc.finish();
        assert!(stats.min.is_none());
        assert!(stats.max.is_none());
        assert_eq!(stats.null_count, 0);
        assert_eq!(stats.nan_count, None);
    }

    #[test]
    fn test_all_nulls() {
        let mut acc = StatisticsAccumulator::new(&DataType::Int32);
        let array: ArrayRef = Arc::new(Int32Array::from(vec![None, None, None]));
        acc.update(&array).unwrap();
        let stats = acc.finish();
        assert!(stats.min.is_none());
        assert!(stats.max.is_none());
        assert_eq!(stats.null_count, 3);
    }

    #[test]
    fn test_single_value() {
        let mut acc = StatisticsAccumulator::new(&DataType::Int32);
        let array: ArrayRef = Arc::new(Int32Array::from(vec![42]));
        acc.update(&array).unwrap();
        let stats = acc.finish();
        assert_eq!(stats.min.as_ref().unwrap(), stats.max.as_ref().unwrap());
        assert_eq!(format!("{}", stats.min.unwrap()), "42");
    }

    #[test]
    fn test_basic_int_stats() {
        let mut acc = StatisticsAccumulator::new(&DataType::Int32);
        let array: ArrayRef = Arc::new(Int32Array::from(vec![1, 5, 3, 2, 4]));
        acc.update(&array).unwrap();
        let stats = acc.finish();
        assert_eq!(format!("{}", stats.min.unwrap()), "1");
        assert_eq!(format!("{}", stats.max.unwrap()), "5");
        assert_eq!(stats.null_count, 0);
        assert_eq!(stats.nan_count, None);
    }

    #[test]
    fn test_with_nulls() {
        let mut acc = StatisticsAccumulator::new(&DataType::Int32);
        let array: ArrayRef = Arc::new(Int32Array::from(vec![Some(1), None, Some(3)]));
        acc.update(&array).unwrap();
        let stats = acc.finish();
        assert_eq!(format!("{}", stats.min.unwrap()), "1");
        assert_eq!(format!("{}", stats.max.unwrap()), "3");
        assert_eq!(stats.null_count, 1);
    }

    #[test]
    fn test_float_nan_excluded() {
        let mut acc = StatisticsAccumulator::new(&DataType::Float64);
        let array: ArrayRef = Arc::new(Float64Array::from(vec![1.0, f64::NAN, 3.0]));
        acc.update(&array).unwrap();
        let stats = acc.finish();
        assert_eq!(format!("{}", stats.min.unwrap()), "1.0");
        assert_eq!(format!("{}", stats.max.unwrap()), "3.0");
        assert_eq!(stats.nan_count, Some(1));
    }

    #[test]
    fn test_all_nan() {
        let mut acc = StatisticsAccumulator::new(&DataType::Float64);
        let array: ArrayRef = Arc::new(Float64Array::from(vec![f64::NAN, f64::NAN]));
        acc.update(&array).unwrap();
        let stats = acc.finish();
        assert!(stats.min.is_none());
        assert!(stats.max.is_none());
        assert_eq!(stats.nan_count, Some(2));
    }

    #[test]
    fn test_null_and_nan() {
        let mut acc = StatisticsAccumulator::new(&DataType::Float64);
        let array: ArrayRef = Arc::new(Float64Array::from(vec![None, Some(f64::NAN)]));
        acc.update(&array).unwrap();
        let stats = acc.finish();
        assert!(stats.min.is_none());
        assert!(stats.max.is_none());
        assert_eq!(stats.null_count, 1);
        assert_eq!(stats.nan_count, Some(1));
    }

    #[test]
    fn test_multiple_updates() {
        let mut acc = StatisticsAccumulator::new(&DataType::Int32);
        let a1: ArrayRef = Arc::new(Int32Array::from(vec![5, 3]));
        let a2: ArrayRef = Arc::new(Int32Array::from(vec![Some(1), None, Some(7)]));
        acc.update(&a1).unwrap();
        acc.update(&a2).unwrap();
        let stats = acc.finish();
        assert_eq!(format!("{}", stats.min.unwrap()), "1");
        assert_eq!(format!("{}", stats.max.unwrap()), "7");
        assert_eq!(stats.null_count, 1);
    }

    #[test]
    fn test_merge() {
        let mut acc1 = StatisticsAccumulator::new(&DataType::Int32);
        let a1: ArrayRef = Arc::new(Int32Array::from(vec![1, 5]));
        acc1.update(&a1).unwrap();

        let mut acc2 = StatisticsAccumulator::new(&DataType::Int32);
        let a2: ArrayRef = Arc::new(Int32Array::from(vec![Some(3), None, Some(10)]));
        acc2.update(&a2).unwrap();

        acc1.merge(&acc2).unwrap();
        let stats = acc1.finish();
        assert_eq!(format!("{}", stats.min.unwrap()), "1");
        assert_eq!(format!("{}", stats.max.unwrap()), "10");
        assert_eq!(stats.null_count, 1);
    }

    #[test]
    fn test_merge_type_mismatch() {
        let acc1 = StatisticsAccumulator::new(&DataType::Int32);
        let acc2 = StatisticsAccumulator::new(&DataType::Float64);
        let mut acc1 = acc1;
        assert!(acc1.merge(&acc2).is_err());
    }

    #[test]
    fn test_type_mismatch_error() {
        let mut acc = StatisticsAccumulator::new(&DataType::Int32);
        let array: ArrayRef = Arc::new(Float64Array::from(vec![1.0]));
        assert!(acc.update(&array).is_err());
    }

    #[test]
    fn test_reset() {
        let mut acc = StatisticsAccumulator::new(&DataType::Int32);
        let array: ArrayRef = Arc::new(Int32Array::from(vec![Some(1), None, Some(3)]));
        acc.update(&array).unwrap();
        acc.reset();
        let stats = acc.finish();
        assert!(stats.min.is_none());
        assert!(stats.max.is_none());
        assert_eq!(stats.null_count, 0);
        assert_eq!(stats.nan_count, None);
    }

    #[test]
    fn test_string_stats() {
        let mut acc = StatisticsAccumulator::new(&DataType::Utf8);
        let array: ArrayRef = Arc::new(StringArray::from(vec!["apple", "cherry", "banana"]));
        acc.update(&array).unwrap();
        let stats = acc.finish();
        assert_eq!(format!("{}", stats.min.unwrap()), "apple");
        assert_eq!(format!("{}", stats.max.unwrap()), "cherry");
    }

    #[test]
    fn test_boolean_stats() {
        let mut acc = StatisticsAccumulator::new(&DataType::Boolean);
        let array: ArrayRef = Arc::new(BooleanArray::from(vec![true, false]));
        acc.update(&array).unwrap();
        let stats = acc.finish();
        assert_eq!(format!("{}", stats.min.unwrap()), "false");
        assert_eq!(format!("{}", stats.max.unwrap()), "true");
    }

    #[rstest]
    #[case::i32(
        DataType::Int32,
        Arc::new(Int32Array::from(vec![3, 1, 2])) as ArrayRef,
        "1", "3"
    )]
    #[case::i64(
        DataType::Int64,
        Arc::new(Int64Array::from(vec![30, 10, 20])) as ArrayRef,
        "10", "30"
    )]
    #[case::u32(
        DataType::UInt32,
        Arc::new(UInt32Array::from(vec![3, 1, 2])) as ArrayRef,
        "1", "3"
    )]
    #[case::u64(
        DataType::UInt64,
        Arc::new(UInt64Array::from(vec![30, 10, 20])) as ArrayRef,
        "10", "30"
    )]
    #[case::f32(
        DataType::Float32,
        Arc::new(Float32Array::from(vec![3.0f32, 1.0, 2.0])) as ArrayRef,
        "1.0", "3.0"
    )]
    #[case::f64(
        DataType::Float64,
        Arc::new(Float64Array::from(vec![3.0f64, 1.0, 2.0])) as ArrayRef,
        "1.0", "3.0"
    )]
    fn test_rstest_primitives(
        #[case] dt: DataType,
        #[case] array: ArrayRef,
        #[case] expected_min: &str,
        #[case] expected_max: &str,
    ) {
        let mut acc = StatisticsAccumulator::new(&dt);
        acc.update(&array).unwrap();
        let stats = acc.finish();
        assert_eq!(format!("{}", stats.min.unwrap()), expected_min);
        assert_eq!(format!("{}", stats.max.unwrap()), expected_max);
    }

    #[test]
    fn test_statistics_does_not_consume() {
        let mut acc = StatisticsAccumulator::new(&DataType::Int32);
        let array: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
        acc.update(&array).unwrap();
        let s1 = acc.statistics();
        let s2 = acc.statistics();
        assert_eq!(format!("{}", s1.min.unwrap()), "1");
        assert_eq!(format!("{}", s2.max.unwrap()), "3");
    }

    #[test]
    fn test_merge_into_empty() {
        let mut acc1 = StatisticsAccumulator::new(&DataType::Int32);
        let mut acc2 = StatisticsAccumulator::new(&DataType::Int32);
        let array: ArrayRef = Arc::new(Int32Array::from(vec![5, 10]));
        acc2.update(&array).unwrap();

        acc1.merge(&acc2).unwrap();
        let stats = acc1.finish();
        assert_eq!(format!("{}", stats.min.unwrap()), "5");
        assert_eq!(format!("{}", stats.max.unwrap()), "10");
    }

    #[test]
    fn test_buffer_memory() {
        let mut acc = StatisticsAccumulator::new(&DataType::Int32);
        let a1: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let a2: ArrayRef = Arc::new(Int32Array::from(vec![4, 5]));
        let expected = a1.get_buffer_memory_size() + a2.get_buffer_memory_size();
        acc.update(&a1).unwrap();
        acc.update(&a2).unwrap();
        let stats = acc.finish();
        assert_eq!(stats.buffer_memory, expected as u64);
        assert!(stats.buffer_memory > 0);
    }

    #[test]
    fn test_buffer_memory_reset() {
        let mut acc = StatisticsAccumulator::new(&DataType::Int32);
        let array: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
        acc.update(&array).unwrap();
        assert!(acc.statistics().buffer_memory > 0);
        acc.reset();
        assert_eq!(acc.statistics().buffer_memory, 0);
    }

    #[test]
    fn test_non_list_item_nulls_is_none() {
        let mut acc = StatisticsAccumulator::new(&DataType::Int32);
        let array: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
        acc.update(&array).unwrap();
        let stats = acc.finish();
        assert_eq!(stats.item_nulls, None);
    }

    mod list_tests {
        use super::*;
        use arrow_array::builder::{Int32Builder, LargeListBuilder, ListBuilder};
        use arrow_schema::Field;

        fn list_data_type() -> DataType {
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true)))
        }

        fn large_list_data_type() -> DataType {
            DataType::LargeList(Arc::new(Field::new("item", DataType::Int32, true)))
        }

        /// Build a ListArray from a slice of optional lists of optional i32.
        fn build_list_array(rows: &[Option<&[Option<i32>]>]) -> ArrayRef {
            let mut builder = ListBuilder::new(Int32Builder::new());
            for row in rows {
                match row {
                    Some(items) => {
                        for item in *items {
                            match item {
                                Some(v) => builder.values().append_value(*v),
                                None => builder.values().append_null(),
                            }
                        }
                        builder.append(true);
                    }
                    None => builder.append(false),
                }
            }
            Arc::new(builder.finish())
        }

        /// Build a LargeListArray from a slice of optional lists of optional i32.
        fn build_large_list_array(rows: &[Option<&[Option<i32>]>]) -> ArrayRef {
            let mut builder = LargeListBuilder::new(Int32Builder::new());
            for row in rows {
                match row {
                    Some(items) => {
                        for item in *items {
                            match item {
                                Some(v) => builder.values().append_value(*v),
                                None => builder.values().append_null(),
                            }
                        }
                        builder.append(true);
                    }
                    None => builder.append(false),
                }
            }
            Arc::new(builder.finish())
        }

        #[test]
        fn test_list_basic() {
            // [[1, 5], [3, 2, 4]]
            let array = build_list_array(&[
                Some(&[Some(1), Some(5)]),
                Some(&[Some(3), Some(2), Some(4)]),
            ]);
            let mut acc = StatisticsAccumulator::new(&list_data_type());
            acc.update(&array).unwrap();
            let stats = acc.finish();
            assert_eq!(format!("{}", stats.min.unwrap()), "1");
            assert_eq!(format!("{}", stats.max.unwrap()), "5");
            assert_eq!(stats.null_count, 0);
            assert_eq!(stats.item_nulls, Some(0));
            assert_eq!(stats.nan_count, None);
        }

        #[test]
        fn test_list_with_null_items() {
            // [[1, null, 5], [null, 3]]
            let array =
                build_list_array(&[Some(&[Some(1), None, Some(5)]), Some(&[None, Some(3)])]);
            let mut acc = StatisticsAccumulator::new(&list_data_type());
            acc.update(&array).unwrap();
            let stats = acc.finish();
            assert_eq!(format!("{}", stats.min.unwrap()), "1");
            assert_eq!(format!("{}", stats.max.unwrap()), "5");
            assert_eq!(stats.null_count, 0);
            assert_eq!(stats.item_nulls, Some(2));
        }

        #[test]
        fn test_list_with_null_lists() {
            // [[1, 2], null, [3]]
            let array = build_list_array(&[Some(&[Some(1), Some(2)]), None, Some(&[Some(3)])]);
            let mut acc = StatisticsAccumulator::new(&list_data_type());
            acc.update(&array).unwrap();
            let stats = acc.finish();
            assert_eq!(format!("{}", stats.min.unwrap()), "1");
            assert_eq!(format!("{}", stats.max.unwrap()), "3");
            assert_eq!(stats.null_count, 1);
            assert_eq!(stats.item_nulls, Some(0));
        }

        #[test]
        fn test_list_with_null_lists_and_null_items() {
            // [[1, null], null, [null, 3]]
            let array = build_list_array(&[Some(&[Some(1), None]), None, Some(&[None, Some(3)])]);
            let mut acc = StatisticsAccumulator::new(&list_data_type());
            acc.update(&array).unwrap();
            let stats = acc.finish();
            assert_eq!(format!("{}", stats.min.unwrap()), "1");
            assert_eq!(format!("{}", stats.max.unwrap()), "3");
            assert_eq!(stats.null_count, 1);
            assert_eq!(stats.item_nulls, Some(2));
        }

        #[test]
        fn test_list_all_null_lists() {
            let array = build_list_array(&[None, None]);
            let mut acc = StatisticsAccumulator::new(&list_data_type());
            acc.update(&array).unwrap();
            let stats = acc.finish();
            assert!(stats.min.is_none());
            assert!(stats.max.is_none());
            assert_eq!(stats.null_count, 2);
            assert_eq!(stats.item_nulls, Some(0));
        }

        #[test]
        fn test_list_empty_lists() {
            // [[], [1], []]
            let array = build_list_array(&[Some(&[]), Some(&[Some(1)]), Some(&[])]);
            let mut acc = StatisticsAccumulator::new(&list_data_type());
            acc.update(&array).unwrap();
            let stats = acc.finish();
            assert_eq!(format!("{}", stats.min.unwrap()), "1");
            assert_eq!(format!("{}", stats.max.unwrap()), "1");
            assert_eq!(stats.null_count, 0);
            assert_eq!(stats.item_nulls, Some(0));
        }

        #[test]
        fn test_list_all_items_null() {
            // [[null, null]]
            let array = build_list_array(&[Some(&[None, None])]);
            let mut acc = StatisticsAccumulator::new(&list_data_type());
            acc.update(&array).unwrap();
            let stats = acc.finish();
            assert!(stats.min.is_none());
            assert!(stats.max.is_none());
            assert_eq!(stats.null_count, 0);
            assert_eq!(stats.item_nulls, Some(2));
        }

        #[test]
        fn test_list_multiple_updates() {
            let a1 = build_list_array(&[Some(&[Some(5), Some(3)])]);
            let a2 = build_list_array(&[Some(&[Some(1), None]), None, Some(&[Some(7)])]);
            let mut acc = StatisticsAccumulator::new(&list_data_type());
            acc.update(&a1).unwrap();
            acc.update(&a2).unwrap();
            let stats = acc.finish();
            assert_eq!(format!("{}", stats.min.unwrap()), "1");
            assert_eq!(format!("{}", stats.max.unwrap()), "7");
            assert_eq!(stats.null_count, 1);
            assert_eq!(stats.item_nulls, Some(1));
        }

        #[test]
        fn test_list_merge() {
            let a1 = build_list_array(&[Some(&[Some(1), Some(5)])]);
            let a2 = build_list_array(&[Some(&[Some(3), None]), None, Some(&[Some(10)])]);

            let mut acc1 = StatisticsAccumulator::new(&list_data_type());
            acc1.update(&a1).unwrap();
            let mut acc2 = StatisticsAccumulator::new(&list_data_type());
            acc2.update(&a2).unwrap();

            acc1.merge(&acc2).unwrap();
            let stats = acc1.finish();
            assert_eq!(format!("{}", stats.min.unwrap()), "1");
            assert_eq!(format!("{}", stats.max.unwrap()), "10");
            assert_eq!(stats.null_count, 1);
            assert_eq!(stats.item_nulls, Some(1));
        }

        #[test]
        fn test_list_reset() {
            let array = build_list_array(&[Some(&[Some(1), None])]);
            let mut acc = StatisticsAccumulator::new(&list_data_type());
            acc.update(&array).unwrap();
            acc.reset();
            let stats = acc.finish();
            assert!(stats.min.is_none());
            assert!(stats.max.is_none());
            assert_eq!(stats.null_count, 0);
            assert_eq!(stats.item_nulls, Some(0));
        }

        #[test]
        fn test_large_list() {
            let array =
                build_large_list_array(&[Some(&[Some(10), None, Some(1)]), None, Some(&[Some(5)])]);
            let mut acc = StatisticsAccumulator::new(&large_list_data_type());
            acc.update(&array).unwrap();
            let stats = acc.finish();
            assert_eq!(format!("{}", stats.min.unwrap()), "1");
            assert_eq!(format!("{}", stats.max.unwrap()), "10");
            assert_eq!(stats.null_count, 1);
            assert_eq!(stats.item_nulls, Some(1));
        }

        /// Build a List<List<Int32>> array from nested slices.
        ///
        /// Each outer Option represents an outer list entry (None = null outer list).
        /// Each inner Option<&[Option<i32>]> represents an inner list entry
        /// (None = null inner list).
        #[allow(clippy::type_complexity)]
        fn build_nested_list_array(rows: &[Option<&[Option<&[Option<i32>]>]>]) -> ArrayRef {
            let inner_builder = ListBuilder::new(Int32Builder::new());
            let mut builder = ListBuilder::new(inner_builder);
            for row in rows {
                match row {
                    Some(inner_lists) => {
                        let inner_builder = builder.values();
                        for inner_list in *inner_lists {
                            match inner_list {
                                Some(items) => {
                                    for item in *items {
                                        match item {
                                            Some(v) => {
                                                inner_builder.values().append_value(*v);
                                            }
                                            None => {
                                                inner_builder.values().append_null();
                                            }
                                        }
                                    }
                                    inner_builder.append(true);
                                }
                                None => {
                                    inner_builder.append(false);
                                }
                            }
                        }
                        builder.append(true);
                    }
                    None => builder.append(false),
                }
            }
            Arc::new(builder.finish())
        }

        fn nested_list_data_type() -> DataType {
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
                true,
            )))
        }

        #[test]
        fn test_nested_list() {
            // [[[1, 2], [3]], null, [[null, 5], null, [6]]]
            let array = build_nested_list_array(&[
                Some(&[Some(&[Some(1), Some(2)][..]), Some(&[Some(3)])]),
                None,
                Some(&[Some(&[None, Some(5)]), None, Some(&[Some(6)])]),
            ]);

            let mut acc = StatisticsAccumulator::new(&nested_list_data_type());
            acc.update(&array).unwrap();
            let stats = acc.finish();

            // min/max should be computed across all leaf int32 values
            assert_eq!(format!("{}", stats.min.unwrap()), "1");
            assert_eq!(format!("{}", stats.max.unwrap()), "6");
            // null_count: only the one null outer list
            assert_eq!(stats.null_count, 1);
            // item_nulls: 1 null int32 + 1 null inner list = 2
            assert_eq!(stats.item_nulls, Some(2));
        }
    }

    mod proptests {
        use super::*;
        use arrow_select::take::take;
        use proptest::prelude::*;

        /// Shuffle an array by applying a random permutation via the `take` kernel.
        fn shuffle(array: &ArrayRef, permutation: &[usize]) -> ArrayRef {
            let indices =
                UInt32Array::from(permutation.iter().map(|&i| i as u32).collect::<Vec<_>>());
            take(array.as_ref(), &indices, None).unwrap()
        }

        /// Compute stats for an array, returning (min, max) as Option<ArrowScalar>.
        fn compute_stats(array: &ArrayRef) -> (Option<ArrowScalar>, Option<ArrowScalar>) {
            let mut acc = StatisticsAccumulator::new(array.data_type());
            acc.update(array).unwrap();
            let stats = acc.finish();
            (stats.min, stats.max)
        }

        macro_rules! prop_test_full {
            ($name:ident, $array_ty:ty, $elem_strategy:expr) => {
                proptest! {
                    #[test]
                    fn $name(
                        values in proptest::collection::vec($elem_strategy, 1..100usize),
                    ) {
                        let len = values.len();
                        let array: ArrayRef = Arc::new(<$array_ty>::from(values));
                        let (orig_min, orig_max) = compute_stats(&array);

                        // min <= max when both exist
                        if let (Some(mn), Some(mx)) = (&orig_min, &orig_max) {
                            prop_assert!(mn <= mx, "min {:?} > max {:?}", mn, mx);
                        }

                        // Reverse the array as a simple permutation
                        let rev_indices: Vec<usize> = (0..len).rev().collect();
                        let reversed = shuffle(&array, &rev_indices);
                        let (rev_min, rev_max) = compute_stats(&reversed);
                        prop_assert_eq!(&orig_min, &rev_min, "min changed after reverse");
                        prop_assert_eq!(&orig_max, &rev_max, "max changed after reverse");
                    }
                }
            };
        }

        macro_rules! prop_test_nullable_full {
            ($name:ident, $array_ty:ty, $elem_strategy:expr) => {
                proptest! {
                    #[test]
                    fn $name(
                        values in proptest::collection::vec(
                            proptest::option::of($elem_strategy), 1..100usize
                        ),
                    ) {
                        let len = values.len();
                        let array: ArrayRef = Arc::new(<$array_ty>::from(values));
                        let (orig_min, orig_max) = compute_stats(&array);

                        if let (Some(mn), Some(mx)) = (&orig_min, &orig_max) {
                            prop_assert!(mn <= mx, "min {:?} > max {:?}", mn, mx);
                        }

                        let rev_indices: Vec<usize> = (0..len).rev().collect();
                        let reversed = shuffle(&array, &rev_indices);
                        let (rev_min, rev_max) = compute_stats(&reversed);
                        prop_assert_eq!(&orig_min, &rev_min, "min changed after reverse");
                        prop_assert_eq!(&orig_max, &rev_max, "max changed after reverse");

                        // Also verify null_count and nan_count are invariant
                        let mut acc_orig = StatisticsAccumulator::new(array.data_type());
                        acc_orig.update(&array).unwrap();
                        let mut acc_rev = StatisticsAccumulator::new(array.data_type());
                        acc_rev.update(&reversed).unwrap();
                        prop_assert_eq!(
                            acc_orig.statistics().null_count,
                            acc_rev.statistics().null_count,
                            "null_count changed after shuffle"
                        );
                        prop_assert_eq!(
                            acc_orig.statistics().nan_count,
                            acc_rev.statistics().nan_count,
                            "nan_count changed after shuffle"
                        );
                    }
                }
            };
        }

        // --- Integer types ---
        prop_test_full!(prop_i32, Int32Array, any::<i32>());
        prop_test_full!(prop_i64, Int64Array, any::<i64>());
        prop_test_full!(prop_u32, UInt32Array, any::<u32>());
        prop_test_full!(prop_u64, UInt64Array, any::<u64>());
        prop_test_full!(prop_i8, Int8Array, any::<i8>());
        prop_test_full!(prop_i16, Int16Array, any::<i16>());
        prop_test_full!(prop_u8, UInt8Array, any::<u8>());
        prop_test_full!(prop_u16, UInt16Array, any::<u16>());

        // --- Nullable integer types ---
        prop_test_nullable_full!(prop_i32_nullable, Int32Array, any::<i32>());
        prop_test_nullable_full!(prop_i64_nullable, Int64Array, any::<i64>());
        prop_test_nullable_full!(prop_u32_nullable, UInt32Array, any::<u32>());

        // --- Float types (with NaN) ---
        prop_test_full!(prop_f32, Float32Array, any::<f32>());
        prop_test_full!(prop_f64, Float64Array, any::<f64>());
        prop_test_nullable_full!(prop_f64_nullable, Float64Array, any::<f64>());

        // --- String type ---
        prop_test_full!(prop_string, StringArray, "[a-z]{0,20}");
        prop_test_nullable_full!(prop_string_nullable, StringArray, "[a-z]{0,20}");

        // --- Boolean type ---
        prop_test_full!(prop_bool, BooleanArray, any::<bool>());
        prop_test_nullable_full!(prop_bool_nullable, BooleanArray, any::<bool>());

        // --- Random permutation shuffle test (uses prop_shuffle) ---
        proptest! {
            #[test]
            fn prop_random_permutation_i32(
                values in proptest::collection::vec(
                    proptest::option::of(any::<i32>()), 1..100usize
                ),
            ) {
                let len = values.len();
                let array: ArrayRef = Arc::new(Int32Array::from(values));
                let (orig_min, orig_max) = compute_stats(&array);

                if let (Some(mn), Some(mx)) = (&orig_min, &orig_max) {
                    prop_assert!(mn <= mx);
                }

                // Create and shuffle a permutation
                let mut perm: Vec<usize> = (0..len).collect();
                // Deterministic "shuffle" using a reversal + rotation
                perm.reverse();
                if len > 1 {
                    perm.rotate_left(len / 2);
                }

                let shuffled = shuffle(&array, &perm);
                let (shuf_min, shuf_max) = compute_stats(&shuffled);
                prop_assert_eq!(&orig_min, &shuf_min);
                prop_assert_eq!(&orig_max, &shuf_max);
            }
        }

        proptest! {
            /// Verify that splitting an array into two chunks and merging
            /// the accumulators gives the same result as processing the
            /// whole array at once.
            #[test]
            fn prop_merge_consistent_i32(
                values in proptest::collection::vec(
                    proptest::option::of(any::<i32>()), 2..100usize
                ),
            ) {
                let array: ArrayRef = Arc::new(Int32Array::from(values.clone()));
                let split = values.len() / 2;

                let mut full_acc = StatisticsAccumulator::new(&DataType::Int32);
                full_acc.update(&array).unwrap();

                let left: ArrayRef = Arc::new(Int32Array::from(values[..split].to_vec()));
                let right: ArrayRef = Arc::new(Int32Array::from(values[split..].to_vec()));
                let mut left_acc = StatisticsAccumulator::new(&DataType::Int32);
                left_acc.update(&left).unwrap();
                let mut right_acc = StatisticsAccumulator::new(&DataType::Int32);
                right_acc.update(&right).unwrap();
                left_acc.merge(&right_acc).unwrap();

                let full_stats = full_acc.finish();
                let merged_stats = left_acc.finish();

                prop_assert_eq!(&full_stats.min, &merged_stats.min);
                prop_assert_eq!(&full_stats.max, &merged_stats.max);
                prop_assert_eq!(full_stats.null_count, merged_stats.null_count);
            }
        }
    }

    #[test]
    fn test_unsupported_type_tracks_null_count_and_memory() {
        use arrow_array::builder::{Int32Builder, StructBuilder};
        use arrow_schema::Field;

        let fields = vec![Field::new("a", DataType::Int32, true)];
        let mut builder = StructBuilder::new(fields, vec![Box::new(Int32Builder::new()) as _]);
        for _ in 0..3 {
            builder
                .field_builder::<Int32Builder>(0)
                .unwrap()
                .append_null();
            builder.append_null();
        }
        let struct_array: ArrayRef = Arc::new(builder.finish());

        let dt = struct_array.data_type().clone();
        let mut acc = StatisticsAccumulator::new(&dt);
        acc.update(&struct_array).unwrap();

        let stats = acc.finish();
        assert!(stats.min.is_none());
        assert!(stats.max.is_none());
        assert_eq!(stats.null_count, 3);
        assert_eq!(stats.nan_count, None);
        assert!(stats.buffer_memory > 0);
    }
}
