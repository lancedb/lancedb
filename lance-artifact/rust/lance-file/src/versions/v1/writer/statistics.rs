// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Statistics collection utilities

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, ArrowNumericType, ArrowPrimitiveType, OffsetSizeTrait, PrimitiveArray,
    RecordBatch, StructArray,
    builder::{ArrayBuilder, BooleanBuilder, PrimitiveBuilder, make_builder},
    builder::{GenericBinaryBuilder, GenericStringBuilder},
    cast::{AsArray, as_generic_binary_array, as_primitive_array},
    types::{
        ArrowDictionaryKeyType, Date32Type, Date64Type, Decimal128Type, DurationMicrosecondType,
        DurationMillisecondType, DurationNanosecondType, DurationSecondType, Float32Type,
        Float64Type, Int8Type, Int16Type, Int32Type, Int64Type, Time32MillisecondType,
        Time32SecondType, Time64MicrosecondType, Time64NanosecondType, TimestampMicrosecondType,
        TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType, UInt8Type,
        UInt16Type, UInt32Type, UInt64Type,
    },
};
use arrow_schema::{ArrowError, DataType, Field as ArrowField, Schema as ArrowSchema, TimeUnit};
use datafusion_common::ScalarValue;
use lance_arrow::{DataTypeExt, as_fixed_size_binary_array};
use lance_core::Result;
use lance_core::datatypes::{Field, Schema};
use num_traits::{Float, Zero, bounds::Bounded};
use std::str;

/// Max number of bytes that are included in statistics for binary columns.
const BINARY_PREFIX_LENGTH: usize = 64;

/// Statistics for a single column chunk.
#[derive(Debug, PartialEq)]
pub struct StatisticsRow {
    /// Number of nulls in this column chunk.
    pub(crate) null_count: i64,
    /// Minimum value in this column chunk, if any
    pub(crate) min_value: ScalarValue,
    /// Maximum value in this column chunk, if any
    pub(crate) max_value: ScalarValue,
}

fn compute_primitive_statistics<T: ArrowNumericType>(
    arrays: &[&ArrayRef],
) -> (T::Native, T::Native, i64)
where
    T::Native: Bounded + PartialOrd,
{
    let mut min_value = T::Native::max_value();
    let mut max_value = T::Native::min_value();
    let mut null_count: i64 = 0;
    let mut all_values_null = true;
    let arrays_iterator = arrays.iter().map(|x| as_primitive_array::<T>(x));

    for array in arrays_iterator {
        null_count += array.null_count() as i64;
        if array.null_count() == array.len() {
            continue;
        }
        all_values_null = false;

        array.iter().for_each(|value| {
            if let Some(value) = value {
                if let Some(Ordering::Greater) = value.partial_cmp(&max_value) {
                    max_value = value;
                }
                if let Some(Ordering::Less) = value.partial_cmp(&min_value) {
                    min_value = value;
                }
            };
        });
    }

    if all_values_null {
        return (T::Native::min_value(), T::Native::max_value(), null_count);
    }
    (min_value, max_value, null_count)
}

fn get_statistics<T: ArrowNumericType>(arrays: &[&ArrayRef]) -> StatisticsRow
where
    T::Native: Bounded,
    datafusion_common::scalar::ScalarValue: From<<T as ArrowPrimitiveType>::Native>,
{
    debug_assert!(!arrays.is_empty());
    // Note: we take data_type off of arrays instead of using T::DATA_TYPE because
    // there might be parameters like timezone for temporal types that need to
    // be propagated.
    let (min_value, max_value, null_count) = compute_primitive_statistics::<T>(arrays);
    StatisticsRow {
        null_count,
        min_value: ScalarValue::new_primitive::<T>(Some(min_value), arrays[0].data_type()).unwrap(),
        max_value: ScalarValue::new_primitive::<T>(Some(max_value), arrays[0].data_type()).unwrap(),
    }
}

fn compute_float_statistics<T: ArrowNumericType>(
    arrays: &[&ArrayRef],
) -> (T::Native, T::Native, i64)
where
    T::Native: Float,
{
    let mut min_value = T::Native::infinity();
    let mut max_value = T::Native::neg_infinity();
    let mut null_count: i64 = 0;
    let arrays_iterator = arrays.iter().map(|x| as_primitive_array::<T>(x));

    for array in arrays_iterator {
        null_count += array.null_count() as i64;
        if array.null_count() == array.len() {
            continue;
        }

        array.iter().for_each(|value| {
            if let Some(value) = value {
                if let Some(Ordering::Greater) = value.partial_cmp(&max_value) {
                    max_value = value;
                }
                if let Some(Ordering::Less) = value.partial_cmp(&min_value) {
                    min_value = value;
                }
            };
        });
    }

    // If all values are null or NaN, these might have never changed from initial values.
    if min_value == T::Native::infinity() && max_value == T::Native::neg_infinity() {
        min_value = T::Native::neg_infinity();
        max_value = T::Native::infinity();
    }
    (min_value, max_value, null_count)
}

fn get_float_statistics<T: ArrowNumericType>(arrays: &[&ArrayRef]) -> StatisticsRow
where
    T::Native: Bounded + Float,
    datafusion_common::scalar::ScalarValue: From<<T as ArrowPrimitiveType>::Native>,
{
    let (mut min_value, mut max_value, null_count) = compute_float_statistics::<T>(arrays);

    if min_value == T::Native::zero() {
        min_value = T::Native::neg_zero();
    }

    if max_value == T::Native::neg_zero() {
        max_value = T::Native::zero();
    }

    let min_value = ScalarValue::from(min_value);
    let max_value = ScalarValue::from(max_value);

    StatisticsRow {
        null_count,
        min_value,
        max_value,
    }
}

fn get_decimal_statistics(arrays: &[&ArrayRef]) -> StatisticsRow {
    let (min_value, max_value, null_count) = compute_primitive_statistics::<Decimal128Type>(arrays);
    let array = as_primitive_array::<Decimal128Type>(arrays[0]);
    let precision = array.precision();
    let scale = array.scale();

    StatisticsRow {
        null_count,
        min_value: ScalarValue::Decimal128(Some(min_value), precision, scale),
        max_value: ScalarValue::Decimal128(Some(max_value), precision, scale),
    }
}

/// Truncate a UTF8 slice to the longest prefix that is still a valid UTF8 string, while being less than `length` bytes.
fn truncate_utf8(data: &str, length: usize) -> Option<&str> {
    // We return values like that at an earlier stage in the process.
    assert!(data.len() >= length);
    let mut char_indices = data.char_indices();

    // We know `data` is a valid UTF8 encoded string, which means it has at least one valid UTF8 byte, which will make this loop exist.
    while let Some((idx, c)) = char_indices.next_back() {
        let split_point = idx + c.len_utf8();
        if split_point <= length {
            return Some(&data[0..split_point]);
        }
    }

    None
}

/// Try and increment the string's bytes from right to left, returning when the result is a valid UTF8 string.
/// Returns `None` when it can't increment any byte.
fn increment_utf8(mut data: Vec<u8>) -> Option<Vec<u8>> {
    for idx in (0..data.len()).rev() {
        let original = data[idx];
        let (mut byte, mut overflow) = data[idx].overflowing_add(1);

        // Until overflow: 0xFF -> 0x00
        while !overflow {
            data[idx] = byte;

            if str::from_utf8(&data).is_ok() {
                return Some(data);
            }
            (byte, overflow) = data[idx].overflowing_add(1);
        }

        data[idx] = original;
    }

    None
}

/// Truncate a binary slice to make sure its length is less than `length`
fn truncate_binary(data: &[u8], length: usize) -> Option<&[u8]> {
    // We return values like that at an earlier stage in the process.
    assert!(data.len() >= length);
    // If all bytes are already maximal, no need to truncate

    Some(&data[0..length])
}

/// Try and increment the bytes from right to left.
///
/// Returns `None` if all bytes are set to `u8::MAX`.
fn increment(mut data: Vec<u8>) -> Option<Vec<u8>> {
    for byte in data.iter_mut().rev() {
        let (incremented, overflow) = byte.overflowing_add(1);
        *byte = incremented;

        if !overflow {
            return Some(data);
        }
    }

    None
}

fn get_string_statistics<T: OffsetSizeTrait>(arrays: &[&ArrayRef]) -> StatisticsRow {
    let mut min_value: Option<&str> = None;
    let mut max_value: Option<&str> = None;
    let mut null_count: i64 = 0;
    let mut bounds_truncated = false;

    let array_iterator = arrays.iter().map(|x| x.as_string::<T>());

    for array in array_iterator {
        null_count += array.null_count() as i64;
        if array.null_count() == array.len() {
            continue;
        }

        array.iter().for_each(|value| {
            if let Some(mut val) = value {
                if val.len() > BINARY_PREFIX_LENGTH {
                    val = truncate_utf8(val, BINARY_PREFIX_LENGTH).unwrap();
                    bounds_truncated = true;
                }

                if let Some(v) = min_value {
                    if let Some(Ordering::Less) = val.partial_cmp(v) {
                        min_value = Some(val);
                    }
                } else {
                    min_value = Some(val);
                }

                if let Some(v) = max_value {
                    if let Some(Ordering::Greater) = val.partial_cmp(v) {
                        max_value = Some(val);
                    }
                } else {
                    max_value = Some(val);
                }
            }
        });
    }

    let max_value_bound: Option<Vec<u8>>;
    if let Some(v) = max_value {
        // If the bounds were truncated, then we need to increment the max_value,
        // since shorter values are considered smaller than longer values if the
        // short values are a prefix of the long ones.
        if bounds_truncated {
            max_value_bound = increment_utf8(v.as_bytes().to_vec());
            // We can safely unwrap because increment_utf8 will only return
            // Some() if the bound is valid UTF-8 bytes.
            max_value = max_value_bound
                .as_ref()
                .map(|bound| str::from_utf8(bound).unwrap());
        }
    }

    let min_value = min_value.map(|x| x.to_string());
    let max_value = max_value.map(|x| x.to_string());

    match arrays[0].data_type() {
        DataType::Utf8 => StatisticsRow {
            null_count,
            min_value: ScalarValue::Utf8(min_value),
            max_value: ScalarValue::Utf8(max_value),
        },
        DataType::LargeUtf8 => StatisticsRow {
            null_count,
            min_value: ScalarValue::LargeUtf8(min_value),
            max_value: ScalarValue::LargeUtf8(max_value),
        },
        _ => {
            unreachable!()
        }
    }
}

fn get_binary_statistics<T: OffsetSizeTrait>(arrays: &[&ArrayRef]) -> StatisticsRow {
    let mut min_value: Option<&[u8]> = None;
    let mut max_value: Option<&[u8]> = None;
    let mut null_count: i64 = 0;
    let mut bounds_truncated = false;

    let array_iterator = arrays.iter().map(|x| as_generic_binary_array::<T>(x));

    for array in array_iterator {
        null_count += array.null_count() as i64;
        if array.null_count() == array.len() {
            continue;
        }

        array.iter().for_each(|value| {
            if let Some(mut val) = value {
                // Truncate binary buffer to BINARY_PREFIX_LENGTH to avoid comparing potentially
                // very long buffers.
                if val.len() > BINARY_PREFIX_LENGTH {
                    val = truncate_binary(val, BINARY_PREFIX_LENGTH).unwrap();
                    bounds_truncated = true;
                }

                if let Some(v) = min_value {
                    if let Some(Ordering::Less) = val.partial_cmp(v) {
                        min_value = Some(val);
                    }
                } else {
                    min_value = Some(val);
                }

                if let Some(v) = max_value {
                    if let Some(Ordering::Greater) = val.partial_cmp(v) {
                        max_value = Some(val);
                    }
                } else {
                    max_value = Some(val);
                }
            }
        });
    }

    let min_value = min_value.map(|x| x.to_vec());
    // If the bounds were truncated, then we need to increment the max_value,
    // since shorter values are considered smaller than longer values if the
    // short values are a prefix of the long ones.
    let max_value = if let Some(v) = max_value {
        if bounds_truncated {
            increment(v.to_vec())
        } else {
            Some(v.to_vec())
        }
    } else {
        None
    };

    match arrays[0].data_type() {
        DataType::Binary => StatisticsRow {
            null_count,
            min_value: ScalarValue::Binary(min_value),
            max_value: ScalarValue::Binary(max_value),
        },
        DataType::LargeBinary => StatisticsRow {
            null_count,
            min_value: ScalarValue::LargeBinary(min_value),
            max_value: ScalarValue::LargeBinary(max_value),
        },
        _ => {
            unreachable!()
        }
    }
}

fn get_fixed_size_binary_statistics(arrays: &[&ArrayRef]) -> StatisticsRow {
    let mut min_value: Option<&[u8]> = None;
    let mut max_value: Option<&[u8]> = None;
    let mut null_count: i64 = 0;

    let array_iterator = arrays.iter().map(|x| as_fixed_size_binary_array(x));

    let length = as_fixed_size_binary_array(arrays[0]).value_length() as usize;
    // Truncate binary buffer to BINARY_PREFIX_LENGTH to avoid comparing potentially
    // very long buffers.
    let do_truncate = length > BINARY_PREFIX_LENGTH;

    for array in array_iterator {
        null_count += array.null_count() as i64;
        if array.null_count() == array.len() {
            continue;
        }

        array.iter().for_each(|value| {
            if let Some(mut val) = value {
                if do_truncate {
                    val = truncate_binary(val, BINARY_PREFIX_LENGTH).unwrap();
                }

                if let Some(v) = min_value {
                    if let Some(Ordering::Less) = val.partial_cmp(v) {
                        min_value = Some(val);
                    }
                } else {
                    min_value = Some(val);
                }

                if let Some(v) = max_value {
                    if let Some(Ordering::Greater) = val.partial_cmp(v) {
                        max_value = Some(val);
                    }
                } else {
                    max_value = Some(val);
                }
            }
        });
    }

    let min_value = min_value.map(|x| x.to_vec());
    // If the bounds were truncated, then we need to increment the max_value,
    // since shorter values are considered smaller than longer values if the
    // short values are a prefix of the long ones.
    let max_value = if let Some(v) = max_value {
        if do_truncate {
            increment(v.to_vec())
        } else {
            Some(v.to_vec())
        }
    } else {
        None
    };

    StatisticsRow {
        null_count,
        min_value: ScalarValue::FixedSizeBinary(length as i32, min_value),
        max_value: ScalarValue::FixedSizeBinary(length as i32, max_value),
    }
}

fn get_boolean_statistics(arrays: &[&ArrayRef]) -> StatisticsRow {
    let mut true_present = false;
    let mut false_present = false;
    let mut null_count: i64 = 0;

    let array_iterator = arrays.iter().map(|x| x.as_boolean());

    for array in array_iterator {
        null_count += array.null_count() as i64;
        if array.null_count() == array.len() || (true_present && false_present) {
            continue;
        }

        array.iter().for_each(|value| {
            if let Some(value) = value {
                if value {
                    true_present = true;
                } else {
                    false_present = true;
                }
            };
        });
    }

    StatisticsRow {
        null_count,
        min_value: ScalarValue::Boolean(Some(true_present && !false_present)),
        max_value: ScalarValue::Boolean(Some(true_present || !false_present)),
    }
}

fn cast_dictionary_arrays<'a, T: ArrowDictionaryKeyType + 'static>(
    arrays: &'a [&'a ArrayRef],
) -> Vec<&'a Arc<dyn Array>> {
    arrays
        .iter()
        .map(|x| x.as_dictionary::<T>().values())
        .collect::<Vec<_>>()
}

fn get_dictionary_statistics(arrays: &[&ArrayRef]) -> StatisticsRow {
    let data_type = arrays[0].data_type();
    match data_type {
        DataType::Dictionary(key_type, _) => match key_type.as_ref() {
            DataType::Int8 => collect_statistics(&cast_dictionary_arrays::<Int8Type>(arrays)),
            DataType::Int16 => collect_statistics(&cast_dictionary_arrays::<Int16Type>(arrays)),
            DataType::Int32 => collect_statistics(&cast_dictionary_arrays::<Int32Type>(arrays)),
            DataType::Int64 => collect_statistics(&cast_dictionary_arrays::<Int64Type>(arrays)),
            DataType::UInt8 => collect_statistics(&cast_dictionary_arrays::<UInt8Type>(arrays)),
            DataType::UInt16 => collect_statistics(&cast_dictionary_arrays::<UInt16Type>(arrays)),
            DataType::UInt32 => collect_statistics(&cast_dictionary_arrays::<UInt32Type>(arrays)),
            DataType::UInt64 => collect_statistics(&cast_dictionary_arrays::<UInt64Type>(arrays)),
            _ => {
                panic!("Unsupported dictionary key type: {}", key_type);
            }
        },
        _ => {
            panic!("Unsupported data type for dictionary: {}", data_type);
        }
    }
}

fn get_temporal_statistics(arrays: &[&ArrayRef]) -> StatisticsRow {
    match arrays[0].data_type() {
        DataType::Time32(TimeUnit::Second) => get_statistics::<Time32SecondType>(arrays),
        DataType::Time32(TimeUnit::Millisecond) => get_statistics::<Time32MillisecondType>(arrays),
        DataType::Date32 => get_statistics::<Date32Type>(arrays),
        // Note: the tz is propagated through the data type in arrays within get_statistics()
        DataType::Timestamp(TimeUnit::Second, _) => get_statistics::<TimestampSecondType>(arrays),
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            get_statistics::<TimestampMillisecondType>(arrays)
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            get_statistics::<TimestampMicrosecondType>(arrays)
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            get_statistics::<TimestampNanosecondType>(arrays)
        }
        DataType::Time64(TimeUnit::Microsecond) => get_statistics::<Time64MicrosecondType>(arrays),
        DataType::Time64(TimeUnit::Nanosecond) => get_statistics::<Time64NanosecondType>(arrays),
        DataType::Date64 => get_statistics::<Date64Type>(arrays),
        DataType::Duration(TimeUnit::Second) => get_statistics::<DurationSecondType>(arrays),
        DataType::Duration(TimeUnit::Millisecond) => {
            get_statistics::<DurationMillisecondType>(arrays)
        }
        DataType::Duration(TimeUnit::Microsecond) => {
            get_statistics::<DurationMicrosecondType>(arrays)
        }
        DataType::Duration(TimeUnit::Nanosecond) => {
            get_statistics::<DurationNanosecondType>(arrays)
        }
        _ => {
            unreachable!()
        }
    }
}

pub fn collect_statistics(arrays: &[&ArrayRef]) -> StatisticsRow {
    if arrays.is_empty() {
        panic!("No arrays to collect statistics from");
    }
    match arrays[0].data_type() {
        DataType::Boolean => get_boolean_statistics(arrays),
        DataType::Int8 => get_statistics::<Int8Type>(arrays),
        DataType::UInt8 => get_statistics::<UInt8Type>(arrays),
        DataType::Int16 => get_statistics::<Int16Type>(arrays),
        DataType::UInt16 => get_statistics::<UInt16Type>(arrays),
        DataType::Int32 => get_statistics::<Int32Type>(arrays),
        DataType::UInt32 => get_statistics::<UInt32Type>(arrays),
        DataType::Int64 => get_statistics::<Int64Type>(arrays),
        DataType::UInt64 => get_statistics::<UInt64Type>(arrays),
        DataType::Float32 => get_float_statistics::<Float32Type>(arrays),
        DataType::Float64 => get_float_statistics::<Float64Type>(arrays),
        DataType::Date32
        | DataType::Time32(_)
        | DataType::Date64
        | DataType::Time64(_)
        | DataType::Timestamp(_, _)
        | DataType::Duration(_) => get_temporal_statistics(arrays),
        DataType::Decimal128(_, _) => get_decimal_statistics(arrays),
        // TODO: Decimal256
        DataType::Binary => get_binary_statistics::<i32>(arrays),
        DataType::LargeBinary => get_binary_statistics::<i64>(arrays),
        DataType::FixedSizeBinary(_) => get_fixed_size_binary_statistics(arrays),
        DataType::Utf8 => get_string_statistics::<i32>(arrays),
        DataType::LargeUtf8 => get_string_statistics::<i64>(arrays),
        DataType::Dictionary(_, _) => get_dictionary_statistics(arrays),
        // DataType::List(_) => get_list_statistics(arrays),
        // DataType::LargeList(_) => get_list_statistics(arrays),
        _ => unreachable!(),
    }
}

pub fn supports_stats_collection(datatype: &DataType) -> bool {
    matches!(
        datatype,
        DataType::Boolean
            | DataType::Int8
            | DataType::UInt8
            | DataType::Int16
            | DataType::UInt16
            | DataType::Int32
            | DataType::UInt32
            | DataType::Int64
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64
            | DataType::Date32
            | DataType::Date64
            | DataType::Utf8
            | DataType::Binary
            | DataType::LargeUtf8
            | DataType::LargeBinary
            // | DataType::FixedSizeBinary(_)
            | DataType::Decimal128(_, _)
    )
}

#[derive(Debug)]
pub struct StatisticsCollector {
    builders: BTreeMap<i32, (Field, StatisticsBuilder)>,
}

impl StatisticsCollector {
    /// Try to create a new statistics collection. If no fields in the schema
    /// support statistics, will just return None.
    pub fn try_new(schema: &Schema) -> Option<Self> {
        let builders: BTreeMap<i32, (Field, StatisticsBuilder)> = visit_fields(schema)
            .filter(|f| supports_stats_collection(&f.data_type()))
            .map(|f| (f.id, (f.clone(), StatisticsBuilder::new(&f.data_type()))))
            .collect();
        if builders.is_empty() {
            None
        } else {
            Some(Self { builders })
        }
    }

    pub fn get_builder(&mut self, field_id: i32) -> Option<&mut StatisticsBuilder> {
        self.builders.get_mut(&field_id).map(|(_, b)| b)
    }

    pub fn finish(&mut self) -> Result<RecordBatch> {
        let mut arrays: Vec<ArrayRef> = vec![];
        let mut fields: Vec<ArrowField> = vec![];

        self.builders.iter_mut().for_each(|(_, (field, builder))| {
            let null_count = Arc::new(builder.null_count.finish());
            let min_value = Arc::new(builder.min_value.finish());
            let max_value = Arc::new(builder.max_value.finish());
            let struct_fields = vec![
                ArrowField::new("null_count", DataType::Int64, false),
                ArrowField::new("min_value", field.data_type(), field.nullable),
                ArrowField::new("max_value", field.data_type(), field.nullable),
            ];

            let stats = StructArray::new(
                struct_fields.clone().into(),
                vec![null_count.clone(), min_value, max_value],
                null_count.nulls().cloned(),
            );
            let field = ArrowField::new_struct(field.id.to_string(), struct_fields, false);
            fields.push(field);
            arrays.push(Arc::new(stats));
        });

        let schema = Arc::new(ArrowSchema::new(fields));
        let batch = RecordBatch::try_new(schema, arrays);
        match batch {
            Ok(batch) => Ok(batch),
            _ => Err(ArrowError::SchemaError(
                "all columns in a record batch must have the same length".to_string(),
            )
            .into()),
        }
    }
}

/// Visit all fields, only visiting children of structs.
fn visit_fields(schema: &Schema) -> impl Iterator<Item = &Field> {
    let mut fields: Vec<&Field> = schema.fields.iter().rev().collect();
    std::iter::from_fn(move || {
        if let Some(field) = fields.pop() {
            if field.data_type().is_struct() {
                fields.extend(field.children.iter().rev());
            }
            Some(field)
        } else {
            None
        }
    })
}

pub struct StatisticsBuilder {
    null_count: PrimitiveBuilder<Int64Type>,
    min_value: Box<dyn ArrayBuilder>,
    max_value: Box<dyn ArrayBuilder>,
    dt: DataType,
}

impl std::fmt::Debug for StatisticsBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StatisticsBuilder")
            .field("null_count", &self.null_count)
            .field("dt", &self.dt)
            .finish()
    }
}

impl StatisticsBuilder {
    fn new(data_type: &DataType) -> Self {
        let null_count = PrimitiveBuilder::<Int64Type>::new();
        let min_value = make_builder(data_type, 1);
        let max_value = make_builder(data_type, 1);
        let dt = data_type.clone();
        Self {
            null_count,
            min_value,
            max_value,
            dt,
        }
    }

    fn string_statistics_appender<T: OffsetSizeTrait>(&mut self, row: StatisticsRow) {
        let min_value_builder = self
            .min_value
            .as_any_mut()
            .downcast_mut::<GenericStringBuilder<T>>()
            .unwrap();
        let max_value_builder = self
            .max_value
            .as_any_mut()
            .downcast_mut::<GenericStringBuilder<T>>()
            .unwrap();

        self.null_count.append_value(row.null_count);

        match self.dt {
            DataType::Utf8 => {
                if let ScalarValue::Utf8(Some(min_value)) = row.min_value {
                    min_value_builder.append_value(min_value);
                } else {
                    min_value_builder.append_null();
                }

                if let ScalarValue::Utf8(Some(max_value)) = row.max_value {
                    max_value_builder.append_value(max_value);
                } else {
                    max_value_builder.append_null();
                }
            }
            DataType::LargeUtf8 => {
                if let ScalarValue::LargeUtf8(Some(min_value)) = row.min_value {
                    min_value_builder.append_value(min_value);
                } else {
                    min_value_builder.append_null();
                }

                if let ScalarValue::LargeUtf8(Some(max_value)) = row.max_value {
                    max_value_builder.append_value(max_value);
                } else {
                    max_value_builder.append_null();
                }
            }
            _ => unreachable!(),
        }
    }

    fn binary_statistics_appender<T: OffsetSizeTrait>(&mut self, row: StatisticsRow) {
        let min_value_builder = self
            .min_value
            .as_any_mut()
            .downcast_mut::<GenericBinaryBuilder<T>>()
            .unwrap();
        let max_value_builder = self
            .max_value
            .as_any_mut()
            .downcast_mut::<GenericBinaryBuilder<T>>()
            .unwrap();

        self.null_count.append_value(row.null_count);

        match self.dt {
            DataType::Binary => {
                if let ScalarValue::Binary(Some(min_value)) = row.min_value {
                    min_value_builder.append_value(min_value);
                } else {
                    min_value_builder.append_null();
                }

                if let ScalarValue::Binary(Some(max_value)) = row.max_value {
                    max_value_builder.append_value(max_value);
                } else {
                    max_value_builder.append_null();
                }
            }
            DataType::LargeBinary => {
                if let ScalarValue::LargeBinary(Some(min_value)) = row.min_value {
                    min_value_builder.append_value(min_value);
                } else {
                    min_value_builder.append_null();
                }

                if let ScalarValue::LargeBinary(Some(max_value)) = row.max_value {
                    max_value_builder.append_value(max_value);
                } else {
                    max_value_builder.append_null();
                }
            }
            _ => unreachable!(),
        }
    }

    fn statistics_appender<T: arrow_array::ArrowPrimitiveType>(&mut self, row: StatisticsRow) {
        let min_value_builder = self
            .min_value
            .as_any_mut()
            .downcast_mut::<PrimitiveBuilder<T>>()
            .unwrap();
        let max_value_builder = self
            .max_value
            .as_any_mut()
            .downcast_mut::<PrimitiveBuilder<T>>()
            .unwrap();

        let min_value = row
            .min_value
            .to_array()
            .unwrap()
            .as_any()
            .downcast_ref::<PrimitiveArray<T>>()
            .unwrap()
            .value(0);
        let max_value = row
            .max_value
            .to_array()
            .unwrap()
            .as_any()
            .downcast_ref::<PrimitiveArray<T>>()
            .unwrap()
            .value(0);

        self.null_count.append_value(row.null_count);
        min_value_builder.append_value(min_value);
        max_value_builder.append_value(max_value);
    }

    fn boolean_appender(&mut self, row: StatisticsRow) {
        let min_value_builder = self
            .min_value
            .as_any_mut()
            .downcast_mut::<BooleanBuilder>()
            .unwrap();
        let max_value_builder = self
            .max_value
            .as_any_mut()
            .downcast_mut::<BooleanBuilder>()
            .unwrap();

        self.null_count.append_value(row.null_count);

        if let ScalarValue::Boolean(Some(min_value)) = row.min_value {
            min_value_builder.append_value(min_value);
        } else {
            min_value_builder.append_value(false);
        };

        if let ScalarValue::Boolean(Some(max_value)) = row.max_value {
            max_value_builder.append_value(max_value);
        } else {
            max_value_builder.append_value(true);
        };
    }

    pub fn append(&mut self, row: StatisticsRow) {
        match self.dt {
            DataType::Boolean => self.boolean_appender(row),
            DataType::Int8 => self.statistics_appender::<Int8Type>(row),
            DataType::UInt8 => self.statistics_appender::<UInt8Type>(row),
            DataType::Int16 => self.statistics_appender::<Int16Type>(row),
            DataType::UInt16 => self.statistics_appender::<UInt16Type>(row),
            DataType::Int32 => self.statistics_appender::<Int32Type>(row),
            DataType::UInt32 => self.statistics_appender::<UInt32Type>(row),
            DataType::Int64 => self.statistics_appender::<Int64Type>(row),
            DataType::UInt64 => self.statistics_appender::<UInt64Type>(row),
            DataType::Float32 => self.statistics_appender::<Float32Type>(row),
            DataType::Float64 => self.statistics_appender::<Float64Type>(row),
            DataType::Date32 => self.statistics_appender::<Date32Type>(row),
            DataType::Date64 => self.statistics_appender::<Date64Type>(row),
            DataType::Time32(TimeUnit::Second) => self.statistics_appender::<Time32SecondType>(row),
            DataType::Time32(TimeUnit::Millisecond) => {
                self.statistics_appender::<Time32MillisecondType>(row)
            }
            DataType::Time64(TimeUnit::Microsecond) => {
                self.statistics_appender::<Time64MicrosecondType>(row)
            }
            DataType::Time64(TimeUnit::Nanosecond) => {
                self.statistics_appender::<Time64NanosecondType>(row)
            }
            DataType::Timestamp(TimeUnit::Second, _) => {
                self.statistics_appender::<TimestampSecondType>(row)
            }
            DataType::Timestamp(TimeUnit::Millisecond, _) => {
                self.statistics_appender::<TimestampMillisecondType>(row)
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                self.statistics_appender::<TimestampMicrosecondType>(row)
            }
            DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                self.statistics_appender::<TimestampNanosecondType>(row)
            }
            DataType::Duration(TimeUnit::Second) => {
                self.statistics_appender::<DurationSecondType>(row)
            }
            DataType::Duration(TimeUnit::Millisecond) => {
                self.statistics_appender::<DurationMillisecondType>(row)
            }
            DataType::Duration(TimeUnit::Microsecond) => {
                self.statistics_appender::<DurationMicrosecondType>(row)
            }
            DataType::Duration(TimeUnit::Nanosecond) => {
                self.statistics_appender::<DurationNanosecondType>(row)
            }
            DataType::Decimal128(_, _) => self.statistics_appender::<Decimal128Type>(row),
            // TODO: Decimal256
            DataType::Binary => self.binary_statistics_appender::<i32>(row),
            DataType::LargeBinary => self.binary_statistics_appender::<i64>(row),
            DataType::Utf8 => self.string_statistics_appender::<i32>(row),
            DataType::LargeUtf8 => self.string_statistics_appender::<i64>(row),
            // Dictionary type is not needed here. We collected stats for values.
            _ => {
                todo!("Stats collection for {} is not supported yet", self.dt);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::{
        BinaryArray, BooleanArray, Date32Array, Date64Array, Datum, Decimal128Array,
        DictionaryArray, DurationMicrosecondArray, DurationMillisecondArray,
        DurationNanosecondArray, DurationSecondArray, FixedSizeBinaryArray, Float32Array,
        Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, LargeBinaryArray,
        LargeStringArray, StringArray, Time32MillisecondArray, Time32SecondArray,
        Time64MicrosecondArray, Time64NanosecondArray, TimestampMicrosecondArray,
        TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt8Array,
        UInt16Array, UInt32Array, UInt64Array, builder::StringDictionaryBuilder, make_array,
        new_empty_array, new_null_array,
    };
    use arrow_select::interleave::interleave;
    use num_traits::One;
    use proptest::{prop_assert, prop_assert_eq, strategy::Strategy, test_runner::TestCaseError};

    use super::*;
    use arrow_schema::{Field as ArrowField, Fields as ArrowFields, Schema as ArrowSchema};

    #[test]
    #[should_panic(expected = "No arrays to collect statistics from")]
    fn test_no_arrays() {
        let arrays: Vec<ArrayRef> = vec![];
        let array_refs = arrays.iter().collect::<Vec<_>>();
        collect_statistics(array_refs.as_slice());
    }

    #[test]
    fn test_edge_cases() {
        // Empty arrays, default min/max values
        let arrays: Vec<ArrayRef> = vec![Arc::new(UInt32Array::from_iter_values(vec![]))];
        let array_refs = arrays.iter().collect::<Vec<_>>();
        assert_eq!(
            collect_statistics(array_refs.as_slice()),
            StatisticsRow {
                null_count: 0,
                min_value: ScalarValue::from(u32::MIN),
                max_value: ScalarValue::from(u32::MAX),
            }
        );

        let empty_string_vec: Vec<Option<&str>> = vec![];
        let arrays: Vec<ArrayRef> = vec![Arc::new(StringArray::from(empty_string_vec))];
        let array_refs = arrays.iter().collect::<Vec<_>>();
        assert_eq!(
            collect_statistics(array_refs.as_slice()),
            StatisticsRow {
                null_count: 0,
                min_value: ScalarValue::Utf8(None),
                max_value: ScalarValue::Utf8(None),
            }
        );
        let empty_string_vec: Vec<Option<&str>> = vec![];
        let arrays: Vec<ArrayRef> = vec![Arc::new(LargeStringArray::from(empty_string_vec))];
        let array_refs = arrays.iter().collect::<Vec<_>>();
        assert_eq!(
            collect_statistics(array_refs.as_slice()),
            StatisticsRow {
                null_count: 0,
                min_value: ScalarValue::LargeUtf8(None),
                max_value: ScalarValue::LargeUtf8(None),
            }
        );

        let empty_binary_vec: Vec<Option<&[u8]>> = vec![];
        let arrays: Vec<ArrayRef> = vec![Arc::new(BinaryArray::from(empty_binary_vec))];
        let array_refs = arrays.iter().collect::<Vec<_>>();
        assert_eq!(
            collect_statistics(array_refs.as_slice()),
            StatisticsRow {
                null_count: 0,
                min_value: ScalarValue::Binary(None),
                max_value: ScalarValue::Binary(None),
            }
        );
        let empty_binary_vec: Vec<Option<&[u8]>> = vec![];
        let arrays: Vec<ArrayRef> = vec![Arc::new(LargeBinaryArray::from(empty_binary_vec))];
        let array_refs = arrays.iter().collect::<Vec<_>>();
        assert_eq!(
            collect_statistics(array_refs.as_slice()),
            StatisticsRow {
                null_count: 0,
                min_value: ScalarValue::LargeBinary(None),
                max_value: ScalarValue::LargeBinary(None),
            }
        );

        let empty_boolean_vec: Vec<Option<bool>> = vec![];
        let arrays: Vec<ArrayRef> = vec![Arc::new(BooleanArray::from(empty_boolean_vec))];
        let array_refs = arrays.iter().collect::<Vec<_>>();
        assert_eq!(
            collect_statistics(array_refs.as_slice()),
            StatisticsRow {
                null_count: 0,
                min_value: ScalarValue::from(false),
                max_value: ScalarValue::from(true),
            }
        );
    }

    #[test]
    fn test_collect_primitive_stats() {
        struct TestCase {
            source_arrays: Vec<ArrayRef>,
            expected_min: ScalarValue,
            expected_max: ScalarValue,
            expected_null_count: i64,
        }

        let cases: [TestCase; 24] = [
            // Int8
            TestCase {
                source_arrays: vec![
                    Arc::new(Int8Array::from(vec![4, 3, 7, 2])),
                    Arc::new(Int8Array::from(vec![-10, 3, 5])),
                ],
                expected_min: ScalarValue::from(-10_i8),
                expected_max: ScalarValue::from(7_i8),
                expected_null_count: 0,
            },
            // UInt8
            TestCase {
                source_arrays: vec![
                    Arc::new(UInt8Array::from(vec![4, 3, 7, 2])),
                    Arc::new(UInt8Array::from(vec![10, 3, 5])),
                ],
                expected_min: ScalarValue::from(2_u8),
                expected_max: ScalarValue::from(10_u8),
                expected_null_count: 0,
            },
            // Int16
            TestCase {
                source_arrays: vec![
                    Arc::new(Int16Array::from(vec![4, 3, 7, 2])),
                    Arc::new(Int16Array::from(vec![-10, 3, 5])),
                ],
                expected_min: ScalarValue::from(-10_i16),
                expected_max: ScalarValue::from(7_i16),
                expected_null_count: 0,
            },
            // UInt16
            TestCase {
                source_arrays: vec![
                    Arc::new(UInt16Array::from(vec![4, 3, 7, 2])),
                    Arc::new(UInt16Array::from(vec![10, 3, 5])),
                ],
                expected_min: ScalarValue::from(2_u16),
                expected_max: ScalarValue::from(10_u16),
                expected_null_count: 0,
            },
            // Int32
            TestCase {
                source_arrays: vec![
                    Arc::new(Int32Array::from(vec![4, 3, 7, 2])),
                    Arc::new(Int32Array::from(vec![-10, 3, 5])),
                ],
                expected_min: ScalarValue::from(-10_i32),
                expected_max: ScalarValue::from(7_i32),
                expected_null_count: 0,
            },
            // UInt32
            TestCase {
                source_arrays: vec![
                    Arc::new(UInt32Array::from(vec![4, 3, 7, 2])),
                    Arc::new(UInt32Array::from(vec![10, 3, 5])),
                ],
                expected_min: ScalarValue::from(2_u32),
                expected_max: ScalarValue::from(10_u32),
                expected_null_count: 0,
            },
            // Int64
            TestCase {
                source_arrays: vec![
                    Arc::new(Int64Array::from(vec![4, 3, 7, 2])),
                    Arc::new(Int64Array::from(vec![-10, 3, 5])),
                ],
                expected_min: ScalarValue::from(-10_i64),
                expected_max: ScalarValue::from(7_i64),
                expected_null_count: 0,
            },
            // UInt64
            TestCase {
                source_arrays: vec![
                    Arc::new(UInt64Array::from(vec![4, 3, 7, 2])),
                    Arc::new(UInt64Array::from(vec![10, 3, 5])),
                ],
                expected_min: ScalarValue::from(2_u64),
                expected_max: ScalarValue::from(10_u64),
                expected_null_count: 0,
            },
            // Boolean
            TestCase {
                source_arrays: vec![Arc::new(BooleanArray::from(vec![true, false]))],
                expected_min: ScalarValue::from(false),
                expected_max: ScalarValue::from(true),
                expected_null_count: 0,
            },
            // Date
            TestCase {
                source_arrays: vec![
                    Arc::new(Date32Array::from(vec![53, 42])),
                    Arc::new(Date32Array::from(vec![68, 32])),
                ],
                expected_min: ScalarValue::Date32(Some(32)),
                expected_max: ScalarValue::Date32(Some(68)),
                expected_null_count: 0,
            },
            TestCase {
                source_arrays: vec![
                    Arc::new(Date64Array::from(vec![53, 42])),
                    Arc::new(Date64Array::from(vec![68, 32])),
                ],
                expected_min: ScalarValue::Date64(Some(32)),
                expected_max: ScalarValue::Date64(Some(68)),
                expected_null_count: 0,
            },
            // Time
            TestCase {
                source_arrays: vec![
                    Arc::new(Time32SecondArray::from(vec![53, 42])),
                    Arc::new(Time32SecondArray::from(vec![68, 32])),
                ],
                expected_min: ScalarValue::Time32Second(Some(32)),
                expected_max: ScalarValue::Time32Second(Some(68)),
                expected_null_count: 0,
            },
            TestCase {
                source_arrays: vec![
                    Arc::new(Time32MillisecondArray::from(vec![53, 42])),
                    Arc::new(Time32MillisecondArray::from(vec![68, 32])),
                ],
                expected_min: ScalarValue::Time32Millisecond(Some(32)),
                expected_max: ScalarValue::Time32Millisecond(Some(68)),
                expected_null_count: 0,
            },
            TestCase {
                source_arrays: vec![
                    Arc::new(Time64MicrosecondArray::from(vec![53, 42])),
                    Arc::new(Time64MicrosecondArray::from(vec![68, 32])),
                ],
                expected_min: ScalarValue::Time64Microsecond(Some(32)),
                expected_max: ScalarValue::Time64Microsecond(Some(68)),
                expected_null_count: 0,
            },
            TestCase {
                source_arrays: vec![
                    Arc::new(Time64NanosecondArray::from(vec![53, 42])),
                    Arc::new(Time64NanosecondArray::from(vec![68, 32])),
                ],
                expected_min: ScalarValue::Time64Nanosecond(Some(32)),
                expected_max: ScalarValue::Time64Nanosecond(Some(68)),
                expected_null_count: 0,
            },
            // Timestamp
            TestCase {
                source_arrays: vec![
                    Arc::new(TimestampSecondArray::with_timezone_opt(
                        vec![53, 42].into(),
                        Some("UTC"),
                    )),
                    Arc::new(TimestampSecondArray::with_timezone_opt(
                        vec![68, 32].into(),
                        Some("UTC"),
                    )),
                ],
                expected_min: ScalarValue::TimestampSecond(Some(32), Some("UTC".into())),
                expected_max: ScalarValue::TimestampSecond(Some(68), Some("UTC".into())),
                expected_null_count: 0,
            },
            TestCase {
                source_arrays: vec![
                    Arc::new(TimestampMillisecondArray::with_timezone_opt(
                        vec![53, 42].into(),
                        Some("UTC"),
                    )),
                    Arc::new(TimestampMillisecondArray::with_timezone_opt(
                        vec![68, 32].into(),
                        Some("UTC"),
                    )),
                ],
                expected_min: ScalarValue::TimestampMillisecond(Some(32), Some("UTC".into())),
                expected_max: ScalarValue::TimestampMillisecond(Some(68), Some("UTC".into())),
                expected_null_count: 0,
            },
            TestCase {
                source_arrays: vec![
                    Arc::new(TimestampMicrosecondArray::with_timezone_opt(
                        vec![53, 42].into(),
                        Some("UTC"),
                    )),
                    Arc::new(TimestampMicrosecondArray::with_timezone_opt(
                        vec![68, 32].into(),
                        Some("UTC"),
                    )),
                ],
                expected_min: ScalarValue::TimestampMicrosecond(Some(32), Some("UTC".into())),
                expected_max: ScalarValue::TimestampMicrosecond(Some(68), Some("UTC".into())),
                expected_null_count: 0,
            },
            TestCase {
                source_arrays: vec![
                    Arc::new(TimestampNanosecondArray::with_timezone_opt(
                        vec![53, 42].into(),
                        Some("UTC"),
                    )),
                    Arc::new(TimestampNanosecondArray::with_timezone_opt(
                        vec![68, 32].into(),
                        Some("UTC"),
                    )),
                ],
                expected_min: ScalarValue::TimestampNanosecond(Some(32), Some("UTC".into())),
                expected_max: ScalarValue::TimestampNanosecond(Some(68), Some("UTC".into())),
                expected_null_count: 0,
            },
            // Duration
            TestCase {
                source_arrays: vec![
                    Arc::new(DurationSecondArray::from(vec![53, 42])),
                    Arc::new(DurationSecondArray::from(vec![68, 32])),
                ],
                expected_min: ScalarValue::DurationSecond(Some(32)),
                expected_max: ScalarValue::DurationSecond(Some(68)),
                expected_null_count: 0,
            },
            TestCase {
                source_arrays: vec![
                    Arc::new(DurationMillisecondArray::from(vec![53, 42])),
                    Arc::new(DurationMillisecondArray::from(vec![68, 32])),
                ],
                expected_min: ScalarValue::DurationMillisecond(Some(32)),
                expected_max: ScalarValue::DurationMillisecond(Some(68)),
                expected_null_count: 0,
            },
            TestCase {
                source_arrays: vec![
                    Arc::new(DurationMicrosecondArray::from(vec![53, 42])),
                    Arc::new(DurationMicrosecondArray::from(vec![68, 32])),
                ],
                expected_min: ScalarValue::DurationMicrosecond(Some(32)),
                expected_max: ScalarValue::DurationMicrosecond(Some(68)),
                expected_null_count: 0,
            },
            TestCase {
                source_arrays: vec![
                    Arc::new(DurationNanosecondArray::from(vec![53, 42])),
                    Arc::new(DurationNanosecondArray::from(vec![68, 32])),
                ],
                expected_min: ScalarValue::DurationNanosecond(Some(32)),
                expected_max: ScalarValue::DurationNanosecond(Some(68)),
                expected_null_count: 0,
            },
            // Decimal
            TestCase {
                source_arrays: vec![
                    Arc::new(Decimal128Array::from(vec![53, 42])),
                    Arc::new(Decimal128Array::from(vec![68, 32])),
                ],
                expected_min: ScalarValue::try_new_decimal128(32, 38, 10).unwrap(),
                expected_max: ScalarValue::try_new_decimal128(68, 38, 10).unwrap(),
                expected_null_count: 0,
            },
        ];

        for case in cases {
            let array_refs = case.source_arrays.iter().collect::<Vec<_>>();
            let stats = collect_statistics(&array_refs);
            assert_eq!(
                stats,
                StatisticsRow {
                    min_value: case.expected_min,
                    max_value: case.expected_max,
                    null_count: case.expected_null_count,
                },
                "Statistics are wrong for input data: {:?}",
                case.source_arrays
            );
        }
    }

    #[test]
    fn test_collect_float_stats() {
        // NaN values are ignored in statistics
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(Float32Array::from(vec![4.0f32, 3.0, f32::NAN, 2.0])),
            Arc::new(Float32Array::from(vec![-10.0f32, 3.0, 5.0, f32::NAN])),
        ];
        let array_refs = arrays.iter().collect::<Vec<_>>();
        let stats = collect_statistics(&array_refs);
        assert_eq!(
            stats,
            StatisticsRow {
                null_count: 0,
                min_value: ScalarValue::from(-10.0_f32),
                max_value: ScalarValue::from(5.0_f32),
            }
        );

        // (Negative) Infinity can be min or max.
        let arrays: Vec<ArrayRef> = vec![Arc::new(Float64Array::from(vec![
            4.0f64,
            f64::neg_infinity(),
            f64::infinity(),
        ]))];
        let array_refs = arrays.iter().collect::<Vec<_>>();
        let stats = collect_statistics(&array_refs);
        assert_eq!(
            stats,
            StatisticsRow {
                null_count: 0,
                min_value: ScalarValue::from(f64::neg_infinity()),
                max_value: ScalarValue::from(f64::infinity()),
            }
        );

        // Max value for zero is always positive, min value for zero is always negative
        let arrays: Vec<ArrayRef> = vec![Arc::new(Float32Array::from(vec![-0.0, 0.0]))];
        let array_refs = arrays.iter().collect::<Vec<_>>();
        let stats = collect_statistics(&array_refs);
        assert_eq!(
            stats,
            StatisticsRow {
                null_count: 0,
                min_value: ScalarValue::from(-0.0_f32),
                max_value: ScalarValue::from(0.0_f32),
            }
        );

        // If all values are NaN, min and max are -Inf and Inf respectively,
        // NaN values don't count towards null count.
        let arrays: Vec<ArrayRef> = vec![Arc::new(Float64Array::from(vec![
            f64::NAN,
            f64::NAN,
            f64::NAN,
        ]))];
        let array_refs = arrays.iter().collect::<Vec<_>>();
        let stats = collect_statistics(&array_refs);
        assert_eq!(
            stats,
            StatisticsRow {
                null_count: 0,
                min_value: ScalarValue::from(f64::neg_infinity()),
                max_value: ScalarValue::from(f64::infinity()),
            }
        );
    }

    #[test]
    fn test_collect_binary_stats() {
        // Test string, binary with truncation and null values.

        let filler = "48 chars of filler                              ";
        let min_binary_value = vec![0xFFu8; BINARY_PREFIX_LENGTH];

        struct TestCase {
            source_arrays: Vec<ArrayRef>,
            stats: StatisticsRow,
        }

        let cases: [TestCase; 13] = [
            // StringArray
            // Whole strings are used if short enough
            TestCase {
                source_arrays: vec![
                    Arc::new(StringArray::from(vec![Some("foo"), None, Some("bar")])),
                    Arc::new(StringArray::from(vec!["yee", "haw"])),
                ],
                stats: StatisticsRow {
                    null_count: 1,
                    min_value: ScalarValue::from("bar"),
                    max_value: ScalarValue::from("yee"),
                },
            },
            // Prefixes are used if strings are too long. Multi-byte characters are
            // not split.
            TestCase {
                source_arrays: vec![Arc::new(StringArray::from(vec![
                    format!("{}{}", filler, "bacteriologists🧑‍🔬"),
                    format!("{}{}", filler, "terrestial planet"),
                ]))],
                stats: StatisticsRow {
                    null_count: 0,
                    // Bacteriologists is just 15 bytes, but the next character is multi-byte
                    // so we truncate before.
                    min_value: ScalarValue::from(
                        format!("{}{}", filler, "bacteriologists").as_str(),
                    ),
                    // Increment the last character to make sure it's greater than max value
                    max_value: ScalarValue::from(
                        format!("{}{}", filler, "terrestial planf").as_str(),
                    ),
                },
            },
            // Sting is not incremented if it's exact length of the limit
            TestCase {
                source_arrays: vec![Arc::new(StringArray::from(vec![format!(
                    "{}{}",
                    filler, "terrestial planf"
                )]))],
                stats: StatisticsRow {
                    null_count: 0,
                    min_value: ScalarValue::from(
                        format!("{}{}", filler, "terrestial planf").as_str(),
                    ),
                    max_value: ScalarValue::from(
                        format!("{}{}", filler, "terrestial planf").as_str(),
                    ),
                },
            },
            // LargeStringArray
            TestCase {
                source_arrays: vec![
                    Arc::new(LargeStringArray::from(vec![Some("foo"), None, Some("bar")])),
                    Arc::new(LargeStringArray::from(vec!["yee", "haw"])),
                ],
                stats: StatisticsRow {
                    null_count: 1,
                    min_value: ScalarValue::LargeUtf8(Some("bar".to_string())),
                    max_value: ScalarValue::LargeUtf8(Some("yee".to_string())),
                },
            },
            TestCase {
                source_arrays: vec![Arc::new(LargeStringArray::from(vec![
                    format!("{}{}", filler, "bacteriologists🧑‍🔬"),
                    format!("{}{}", filler, "terrestial planet"),
                ]))],
                stats: StatisticsRow {
                    null_count: 0,
                    // Bacteriologists is just 15 bytes, but the next character is multi-byte
                    // so we truncate before.
                    min_value: ScalarValue::LargeUtf8(Some(format!(
                        "{}{}",
                        filler, "bacteriologists"
                    ))),
                    // Increment the last character to make sure it's greater than max value
                    max_value: ScalarValue::LargeUtf8(Some(format!(
                        "{}{}",
                        filler, "terrestial planf"
                    ))),
                },
            },
            // Sting is not incremented if it's exact length of the limit
            TestCase {
                source_arrays: vec![Arc::new(LargeStringArray::from(vec![format!(
                    "{}{}",
                    filler, "terrestial planf"
                )]))],
                stats: StatisticsRow {
                    null_count: 0,
                    min_value: ScalarValue::LargeUtf8(Some(format!(
                        "{}{}",
                        filler, "terrestial planf"
                    ))),
                    max_value: ScalarValue::LargeUtf8(Some(format!(
                        "{}{}",
                        filler, "terrestial planf"
                    ))),
                },
            },
            // BinaryArray
            // If not truncated max value exists (in the edge case where the value is
            // 0xFF up until the limit), just return null as max.)
            TestCase {
                source_arrays: vec![Arc::new(BinaryArray::from(vec![
                    vec![0xFFu8; BINARY_PREFIX_LENGTH + 5].as_ref(),
                ]))],
                stats: StatisticsRow {
                    null_count: 0,
                    // We can truncate the minimum value, since the prefix is less than the full value
                    min_value: ScalarValue::Binary(Some(min_binary_value.clone())),
                    // We can't truncate the max value, so we return None
                    max_value: ScalarValue::Binary(None),
                },
            },
            TestCase {
                source_arrays: vec![Arc::new(BinaryArray::from(vec![
                    vec![0xFFu8; BINARY_PREFIX_LENGTH].as_ref(),
                ]))],
                stats: StatisticsRow {
                    null_count: 0,
                    min_value: ScalarValue::Binary(Some(min_binary_value.clone())),
                    max_value: ScalarValue::Binary(Some(min_binary_value.clone())),
                },
            },
            // LargeBinaryArray
            // If not truncated max value exists (in the edge case where the value is
            // 0xFF up until the limit), just return null as max.)
            TestCase {
                source_arrays: vec![Arc::new(LargeBinaryArray::from(vec![
                    vec![0xFFu8; BINARY_PREFIX_LENGTH + 5].as_ref(),
                ]))],
                stats: StatisticsRow {
                    null_count: 0,
                    // We can truncate the minimum value, since the prefix is less than the full value
                    min_value: ScalarValue::LargeBinary(Some(min_binary_value.clone())),
                    // We can't truncate the max value, so we return None
                    max_value: ScalarValue::LargeBinary(None),
                },
            },
            TestCase {
                source_arrays: vec![Arc::new(LargeBinaryArray::from(vec![
                    vec![0xFFu8; BINARY_PREFIX_LENGTH].as_ref(),
                ]))],
                stats: StatisticsRow {
                    null_count: 0,
                    // We can truncate the minimum value, since the prefix is less than the full value
                    min_value: ScalarValue::LargeBinary(Some(min_binary_value.clone())),
                    max_value: ScalarValue::LargeBinary(Some(min_binary_value.clone())),
                },
            },
            // FixedSizeBinaryArray
            TestCase {
                source_arrays: vec![Arc::new(FixedSizeBinaryArray::from(vec![
                    Some(vec![0, 1].as_slice()),
                    Some(vec![2, 3].as_slice()),
                    Some(vec![4, 5].as_slice()),
                    Some(vec![6, 7].as_slice()),
                    Some(vec![8, 9].as_slice()),
                ]))],
                stats: StatisticsRow {
                    null_count: 0,
                    min_value: ScalarValue::FixedSizeBinary(2, Some(vec![0, 1])),
                    max_value: ScalarValue::FixedSizeBinary(2, Some(vec![8, 9])),
                },
            },
            TestCase {
                source_arrays: vec![Arc::new(FixedSizeBinaryArray::from(vec![
                    min_binary_value.as_slice(),
                ]))],
                stats: StatisticsRow {
                    null_count: 0,
                    min_value: ScalarValue::FixedSizeBinary(
                        BINARY_PREFIX_LENGTH.try_into().unwrap(),
                        Some(min_binary_value.clone()),
                    ),
                    max_value: ScalarValue::FixedSizeBinary(
                        BINARY_PREFIX_LENGTH.try_into().unwrap(),
                        Some(min_binary_value),
                    ),
                },
            },
            TestCase {
                source_arrays: vec![Arc::new(FixedSizeBinaryArray::from(vec![
                    &[0xFFu8; BINARY_PREFIX_LENGTH + 7],
                ]))],
                stats: StatisticsRow {
                    null_count: 0,
                    min_value: ScalarValue::FixedSizeBinary(
                        (BINARY_PREFIX_LENGTH + 7).try_into().unwrap(),
                        Some(vec![0xFFu8; BINARY_PREFIX_LENGTH]),
                    ),
                    // We can't truncate the max value, so we return None
                    max_value: ScalarValue::FixedSizeBinary(
                        (BINARY_PREFIX_LENGTH).try_into().unwrap(),
                        None,
                    ),
                },
            },
        ];

        for case in cases {
            let array_refs = case.source_arrays.iter().collect::<Vec<_>>();
            assert_eq!(
                collect_statistics(&array_refs),
                case.stats,
                "Statistics are wrong for input data: {:?}",
                case.source_arrays
            );
        }
    }

    #[test]
    fn test_collect_dictionary_stats() {
        // Dictionary stats are collected from the underlying values
        let dictionary_values = StringArray::from(vec![None, Some("abc"), Some("def")]);

        let mut builder =
            StringDictionaryBuilder::<Int32Type>::new_with_dictionary(3, &dictionary_values)
                .unwrap();
        builder.append("def").unwrap();
        builder.append_null();
        builder.append("abc").unwrap();

        let arr = builder.finish();
        let arrays: Vec<ArrayRef> = vec![Arc::new(arr)];
        let array_refs = arrays.iter().collect::<Vec<_>>();
        let stats = collect_statistics(&array_refs);
        assert_eq!(
            stats,
            StatisticsRow {
                null_count: 1,
                min_value: ScalarValue::from("abc"),
                max_value: ScalarValue::from("def"),
            }
        );

        let dictionary = Arc::new(StringArray::from(vec!["A", "C", "G", "T"]));
        let indices_1 = UInt32Array::from(vec![1, 0, 2, 1]);
        let indices_2 = UInt32Array::from(vec![0, 1, 3, 0]);
        let dictionary_array_1 =
            Arc::new(DictionaryArray::try_new(indices_1, dictionary.clone()).unwrap()) as ArrayRef;
        let dictionary_array_2 =
            Arc::new(DictionaryArray::try_new(indices_2, dictionary).unwrap()) as ArrayRef;
        let array_refs = vec![&dictionary_array_1, &dictionary_array_2];
        let stats = collect_statistics(&array_refs);
        assert_eq!(
            stats,
            StatisticsRow {
                null_count: 0,
                min_value: ScalarValue::from("A"),
                max_value: ScalarValue::from("T"),
            }
        );
    }

    #[test]
    fn test_stats_collector() {
        use lance_core::datatypes::Schema;

        // Check the output schema is correct
        let arrow_schema = ArrowSchema::new(vec![
            ArrowField::new("a", DataType::Int32, true),
            ArrowField::new("b", DataType::Utf8, true),
        ]);
        let schema = Schema::try_from(&arrow_schema).unwrap();
        let mut collector = StatisticsCollector::try_new(&schema).unwrap();

        // Collect stats for a
        let id = schema.field("a").unwrap().id;
        let builder = collector.get_builder(id).unwrap();
        builder.append(StatisticsRow {
            null_count: 2,
            min_value: ScalarValue::from(1_i32),
            max_value: ScalarValue::from(3_i32),
        });
        builder.append(StatisticsRow {
            null_count: 0,
            min_value: ScalarValue::Int32(Some(i32::MIN)),
            max_value: ScalarValue::Int32(Some(i32::MAX)),
        });

        // If we try to finish at this point, it will error since we don't have
        // stats for b yet.
        assert!(collector.finish().is_err());

        // We cannot reuse old collector as it's builders were finished.
        let mut collector = StatisticsCollector::try_new(&schema).unwrap();

        let id = schema.field("a").unwrap().id;
        let builder = collector.get_builder(id).unwrap();
        builder.append(StatisticsRow {
            null_count: 2,
            min_value: ScalarValue::from(1_i32),
            max_value: ScalarValue::from(3_i32),
        });
        builder.append(StatisticsRow {
            null_count: 0,
            min_value: ScalarValue::Int32(Some(i32::MIN)),
            max_value: ScalarValue::Int32(Some(i32::MAX)),
        });

        // Collect stats for b
        let id = schema.field("b").unwrap().id;
        let builder = collector.get_builder(id).unwrap();
        builder.append(StatisticsRow {
            null_count: 6,
            min_value: ScalarValue::from("aaa"),
            max_value: ScalarValue::from("bbb"),
        });
        builder.append(StatisticsRow {
            null_count: 0,
            min_value: ScalarValue::Utf8(None),
            max_value: ScalarValue::Utf8(None),
        });

        // Now we can finish
        let batch = collector.finish().unwrap();

        let expected_schema = ArrowSchema::new(vec![
            ArrowField::new(
                "0",
                DataType::Struct(ArrowFields::from(vec![
                    ArrowField::new("null_count", DataType::Int64, false),
                    ArrowField::new("min_value", DataType::Int32, true),
                    ArrowField::new("max_value", DataType::Int32, true),
                ])),
                false,
            ),
            ArrowField::new(
                "1",
                DataType::Struct(ArrowFields::from(vec![
                    ArrowField::new("null_count", DataType::Int64, false),
                    ArrowField::new("min_value", DataType::Utf8, true),
                    ArrowField::new("max_value", DataType::Utf8, true),
                ])),
                false,
            ),
        ]);

        assert_eq!(batch.schema().as_ref(), &expected_schema);

        let expected_batch = RecordBatch::try_new(
            Arc::new(expected_schema),
            vec![
                Arc::new(StructArray::from(vec![
                    (
                        Arc::new(ArrowField::new("null_count", DataType::Int64, false)),
                        Arc::new(Int64Array::from(vec![2, 0])) as ArrayRef,
                    ),
                    (
                        Arc::new(ArrowField::new("min_value", DataType::Int32, true)),
                        Arc::new(Int32Array::from(vec![1, i32::MIN])) as ArrayRef,
                    ),
                    (
                        Arc::new(ArrowField::new("max_value", DataType::Int32, true)),
                        Arc::new(Int32Array::from(vec![3, i32::MAX])) as ArrayRef,
                    ),
                ])),
                Arc::new(StructArray::from(vec![
                    (
                        Arc::new(ArrowField::new("null_count", DataType::Int64, false)),
                        Arc::new(Int64Array::from(vec![6, 0])) as ArrayRef,
                    ),
                    (
                        Arc::new(ArrowField::new("min_value", DataType::Utf8, true)),
                        Arc::new(StringArray::from(vec![Some("aaa"), None])) as ArrayRef,
                    ),
                    (
                        Arc::new(ArrowField::new("max_value", DataType::Utf8, true)),
                        Arc::new(StringArray::from(vec![Some("bbb"), None])) as ArrayRef,
                    ),
                ])),
            ],
        )
        .unwrap();

        assert_eq!(batch, expected_batch);

        // Check binary and string collectors return null for min/max if no
        // values are supplied
        let arrow_schema = ArrowSchema::new(vec![
            ArrowField::new("boolean", DataType::Boolean, true),
            ArrowField::new("binary", DataType::Binary, true),
            ArrowField::new("large_binary", DataType::LargeBinary, true),
            ArrowField::new("string", DataType::Utf8, true),
            ArrowField::new("large_string", DataType::LargeUtf8, true),
        ]);
        let schema = Schema::try_from(&arrow_schema).unwrap();
        let mut collector = StatisticsCollector::try_new(&schema).unwrap();

        // Collect stats
        let id = schema.field("boolean").unwrap().id;
        let builder = collector.get_builder(id).unwrap();
        builder.append(StatisticsRow {
            null_count: 0,
            min_value: ScalarValue::Boolean(Some(false)),
            max_value: ScalarValue::Boolean(Some(true)),
        });
        let id = schema.field("binary").unwrap().id;
        let builder = collector.get_builder(id).unwrap();
        builder.append(StatisticsRow {
            null_count: 0,
            min_value: ScalarValue::Binary(None),
            max_value: ScalarValue::Binary(None),
        });
        let id = schema.field("large_binary").unwrap().id;
        let builder = collector.get_builder(id).unwrap();
        builder.append(StatisticsRow {
            null_count: 0,
            min_value: ScalarValue::LargeBinary(None),
            max_value: ScalarValue::LargeBinary(None),
        });
        let id = schema.field("string").unwrap().id;
        let builder = collector.get_builder(id).unwrap();
        builder.append(StatisticsRow {
            null_count: 0,
            min_value: ScalarValue::Utf8(None),
            max_value: ScalarValue::Utf8(None),
        });
        let id = schema.field("large_string").unwrap().id;
        let builder = collector.get_builder(id).unwrap();
        builder.append(StatisticsRow {
            null_count: 0,
            min_value: ScalarValue::LargeUtf8(None),
            max_value: ScalarValue::LargeUtf8(None),
        });

        let expected_schema = ArrowSchema::new(vec![
            ArrowField::new(
                "0",
                DataType::Struct(ArrowFields::from(vec![
                    ArrowField::new("null_count", DataType::Int64, false),
                    ArrowField::new("min_value", DataType::Boolean, true),
                    ArrowField::new("max_value", DataType::Boolean, true),
                ])),
                false,
            ),
            ArrowField::new(
                "1",
                DataType::Struct(ArrowFields::from(vec![
                    ArrowField::new("null_count", DataType::Int64, false),
                    ArrowField::new("min_value", DataType::Binary, true),
                    ArrowField::new("max_value", DataType::Binary, true),
                ])),
                false,
            ),
            ArrowField::new(
                "2",
                DataType::Struct(ArrowFields::from(vec![
                    ArrowField::new("null_count", DataType::Int64, false),
                    ArrowField::new("min_value", DataType::LargeBinary, true),
                    ArrowField::new("max_value", DataType::LargeBinary, true),
                ])),
                false,
            ),
            ArrowField::new(
                "3",
                DataType::Struct(ArrowFields::from(vec![
                    ArrowField::new("null_count", DataType::Int64, false),
                    ArrowField::new("min_value", DataType::Utf8, true),
                    ArrowField::new("max_value", DataType::Utf8, true),
                ])),
                false,
            ),
            ArrowField::new(
                "4",
                DataType::Struct(ArrowFields::from(vec![
                    ArrowField::new("null_count", DataType::Int64, false),
                    ArrowField::new("min_value", DataType::LargeUtf8, true),
                    ArrowField::new("max_value", DataType::LargeUtf8, true),
                ])),
                false,
            ),
        ]);

        let none_str_vec: Vec<Option<&str>> = vec![None];
        let expected_batch = RecordBatch::try_new(
            Arc::new(expected_schema.clone()),
            vec![
                Arc::new(StructArray::from(vec![
                    (
                        Arc::new(ArrowField::new("null_count", DataType::Int64, false)),
                        Arc::new(Int64Array::from(vec![0])) as ArrayRef,
                    ),
                    (
                        Arc::new(ArrowField::new("min_value", DataType::Boolean, true)),
                        Arc::new(BooleanArray::from(vec![false])) as ArrayRef,
                    ),
                    (
                        Arc::new(ArrowField::new("max_value", DataType::Boolean, true)),
                        Arc::new(BooleanArray::from(vec![true])) as ArrayRef,
                    ),
                ])),
                Arc::new(StructArray::from(vec![
                    (
                        Arc::new(ArrowField::new("null_count", DataType::Int64, false)),
                        Arc::new(Int64Array::from(vec![0])) as ArrayRef,
                    ),
                    (
                        Arc::new(ArrowField::new("min_value", DataType::Binary, true)),
                        Arc::new(BinaryArray::from(vec![None])) as ArrayRef,
                    ),
                    (
                        Arc::new(ArrowField::new("max_value", DataType::Binary, true)),
                        Arc::new(BinaryArray::from(vec![None])) as ArrayRef,
                    ),
                ])),
                Arc::new(StructArray::from(vec![
                    (
                        Arc::new(ArrowField::new("null_count", DataType::Int64, false)),
                        Arc::new(Int64Array::from(vec![0])) as ArrayRef,
                    ),
                    (
                        Arc::new(ArrowField::new("min_value", DataType::LargeBinary, true)),
                        Arc::new(LargeBinaryArray::from(vec![None])) as ArrayRef,
                    ),
                    (
                        Arc::new(ArrowField::new("max_value", DataType::LargeBinary, true)),
                        Arc::new(LargeBinaryArray::from(vec![None])) as ArrayRef,
                    ),
                ])),
                Arc::new(StructArray::from(vec![
                    (
                        Arc::new(ArrowField::new("null_count", DataType::Int64, false)),
                        Arc::new(Int64Array::from(vec![0])) as ArrayRef,
                    ),
                    (
                        Arc::new(ArrowField::new("min_value", DataType::Utf8, true)),
                        Arc::new(StringArray::from(none_str_vec.clone())) as ArrayRef,
                    ),
                    (
                        Arc::new(ArrowField::new("max_value", DataType::Utf8, true)),
                        Arc::new(StringArray::from(none_str_vec.clone())) as ArrayRef,
                    ),
                ])),
                Arc::new(StructArray::from(vec![
                    (
                        Arc::new(ArrowField::new("null_count", DataType::Int64, false)),
                        Arc::new(Int64Array::from(vec![0])) as ArrayRef,
                    ),
                    (
                        Arc::new(ArrowField::new("min_value", DataType::LargeUtf8, true)),
                        Arc::new(LargeStringArray::from(none_str_vec.clone())) as ArrayRef,
                    ),
                    (
                        Arc::new(ArrowField::new("max_value", DataType::LargeUtf8, true)),
                        Arc::new(LargeStringArray::from(none_str_vec.clone())) as ArrayRef,
                    ),
                ])),
            ],
        )
        .unwrap();

        let batch = collector.finish().unwrap();
        assert_eq!(batch.schema().as_ref(), &expected_schema);
        assert_eq!(batch, expected_batch);
    }

    // Property 1: for all values, if an array is entirely that value then it
    // should become both the min and max. There are some exceptions:
    // * NaN, null are ignored for min and max
    // * string / binary more than 64 bytes are truncated.
    fn assert_min_max_constant_property(
        value: ScalarValue,
        with_nulls: bool,
    ) -> std::result::Result<(), TestCaseError> {
        let array_scalar = value.to_scalar().unwrap();
        let (array, _) = array_scalar.get();
        let array = make_array(array.to_data());

        let array = if with_nulls {
            let nulls = new_null_array(array.data_type(), 1);
            interleave(&[&array, &nulls], &[(1, 0), (0, 0)]).unwrap()
        } else {
            array
        };

        let stats = collect_statistics(&[&array]);
        prop_assert_eq!(
            stats,
            StatisticsRow {
                null_count: if with_nulls { 1 } else { 0 },
                min_value: value.clone(),
                max_value: value,
            },
            "Statistics are wrong for input data: {:?}",
            array
        );
        Ok(())
    }

    #[test]
    fn test_min_max_constant_property() {
        let values = [
            ScalarValue::Boolean(Some(true)),
            ScalarValue::Boolean(Some(false)),
            ScalarValue::Int8(Some(1)),
            ScalarValue::Int16(Some(1)),
            ScalarValue::Int32(Some(1)),
            ScalarValue::Int64(Some(1)),
            ScalarValue::UInt8(Some(1)),
            ScalarValue::UInt16(Some(1)),
            ScalarValue::UInt32(Some(1)),
            ScalarValue::UInt64(Some(1)),
            ScalarValue::Float32(Some(1.0)),
            ScalarValue::Float32(Some(f32::INFINITY)),
            ScalarValue::Float32(Some(f32::NEG_INFINITY)),
            ScalarValue::Float64(Some(1.0)),
            ScalarValue::Float64(Some(f64::INFINITY)),
            ScalarValue::Float64(Some(f64::NEG_INFINITY)),
            ScalarValue::Utf8(Some("foo".to_string())),
            ScalarValue::Utf8(Some("a".repeat(BINARY_PREFIX_LENGTH))),
            ScalarValue::Binary(Some(vec![0_u8; BINARY_PREFIX_LENGTH])),
        ];

        for value in values {
            assert_min_max_constant_property(value.clone(), false).unwrap();
            assert_min_max_constant_property(value.clone(), true).unwrap();
        }
    }

    proptest::proptest! {
        #[test]
        fn test_min_max_constant_property_timestamp(
            timezone_index in 0..3_usize,
            timeunit_index in 0..4_usize,
        ) {
            let value = Some(42);
            // Check different timezones to validate we propagate them.
            let timezones = [None, Some("UTC".into()), Some("America/New_York".into())];
            let timeunits = [TimeUnit::Second, TimeUnit::Millisecond, TimeUnit::Microsecond, TimeUnit::Nanosecond];

            let timezone = timezones[timezone_index].clone();
            let timeunit = timeunits[timeunit_index];
            let value = match timeunit {
                TimeUnit::Second => ScalarValue::TimestampSecond(value, timezone),
                TimeUnit::Millisecond => ScalarValue::TimestampMillisecond(value, timezone),
                TimeUnit::Microsecond => ScalarValue::TimestampMicrosecond(value, timezone),
                TimeUnit::Nanosecond => ScalarValue::TimestampNanosecond(value, timezone),
            };

            assert_min_max_constant_property(value.clone(), false)?;
            assert_min_max_constant_property(value, true)?;
        }
    }

    // Property 2: The min and max should always be less than / greater than
    // all values in the array respectively.
    fn assert_min_max_ordering_float<F: ArrowPrimitiveType>()
    -> std::result::Result<(), TestCaseError>
    where
        F::Native: Float,
    {
        let values = vec![
            F::Native::neg_infinity(),
            F::Native::infinity(),
            F::Native::zero(),
            F::Native::neg_zero(),
            F::Native::one(),
            F::Native::nan(),
            F::Native::min_value(),
            F::Native::max_value(),
        ];

        let mut runner = proptest::test_runner::TestRunner::default();

        // Choose random subset of these values
        let all_options = values.len();
        let subset = proptest::sample::subsequence(values, (0, all_options));

        // Turn into an array and compute statistics
        let results = subset.prop_map(|subset| {
            let array = Arc::new(PrimitiveArray::<F>::from_iter_values(subset.clone())) as ArrayRef;
            let statistics = collect_statistics(&[&array]);
            (subset, statistics)
        });

        runner.run(&results, |(subset, stats)| {
            // Assert min is <= all values
            prop_assert!(subset.iter().all(|val| val.is_nan()
                || stats.min_value
                    <= ScalarValue::new_primitive::<F>(Some(*val), &F::DATA_TYPE).unwrap()));

            // Assert max is >= all values
            prop_assert!(subset.iter().all(|val| val.is_nan()
                || stats.max_value
                    >= ScalarValue::new_primitive::<F>(Some(*val), &F::DATA_TYPE).unwrap()));

            // If array is empty, assert min and max are -inf, +inf, respectively
            if subset.is_empty() {
                prop_assert_eq!(
                    stats.min_value,
                    ScalarValue::new_primitive::<F>(Some(F::Native::neg_infinity()), &F::DATA_TYPE)
                        .unwrap()
                );
                prop_assert_eq!(
                    stats.max_value,
                    ScalarValue::new_primitive::<F>(Some(F::Native::infinity()), &F::DATA_TYPE)
                        .unwrap()
                );
            }

            Ok(())
        })?;

        Ok(())
    }

    #[test]
    fn test_min_max_ordering_float() {
        assert_min_max_ordering_float::<Float32Type>().unwrap();
        assert_min_max_ordering_float::<Float64Type>().unwrap();
    }

    proptest::proptest! {
        #[test]
        fn test_min_max_ordering_string(values in proptest::collection::vec(".{0, 100}", 0..10)) {
            let array = Arc::new(StringArray::from(values.clone())) as ArrayRef;
            let statistics = collect_statistics(&[&array]);

            // If array is empty, assert min and max are null
            if array.is_empty() {
                prop_assert_eq!(&statistics.min_value, &ScalarValue::Utf8(None));
                prop_assert_eq!(&statistics.max_value, &ScalarValue::Utf8(None));
            }

            // Assert min is <= all values
            prop_assert!(values.iter().all(|val| statistics.min_value <= ScalarValue::Utf8(Some(val.clone()))));

            // Assert max is >= all values, or is null
            prop_assert!(statistics.max_value.is_null() || values.iter().all(|val| statistics.max_value >= ScalarValue::Utf8(Some(val.clone()))));

            // Assert min and max are less than BINARY_PREFIX_LENGTH
            match &statistics.min_value {
                ScalarValue::Utf8(Some(min)) => prop_assert!(min.len() <= BINARY_PREFIX_LENGTH),
                ScalarValue::Utf8(None) => (),
                _ => unreachable!(),
            }
            match &statistics.max_value {
                ScalarValue::Utf8(Some(max)) => prop_assert!(max.len() <= BINARY_PREFIX_LENGTH),
                ScalarValue::Utf8(None) => (),
                _ => unreachable!(),
            }
        }
    }

    proptest::proptest! {
        #[test]
        fn test_min_max_ordering_binary(values in proptest::collection::vec(proptest::collection::vec(0..u8::MAX, 0..100), 0..10)) {
            let array = Arc::new(BinaryArray::from_iter_values(values.clone())) as ArrayRef;
            let statistics = collect_statistics(&[&array]);

            // If array is empty, assert min and max are null
            if array.is_empty() {
                prop_assert_eq!(&statistics.min_value, &ScalarValue::Binary(None));
                prop_assert_eq!(&statistics.max_value, &ScalarValue::Binary(None));
            }

            // Assert min is <= all values
            prop_assert!(values.iter().all(|val| statistics.min_value <= ScalarValue::Binary(Some(val.clone()))));

            // Assert max is >= all values
            prop_assert!(statistics.max_value.is_null() || values.iter().all(|val| statistics.max_value >= ScalarValue::Binary(Some(val.clone()))));

            // Assert min and max are less than BINARY_PREFIX_LENGTH
            match &statistics.min_value {
                ScalarValue::Binary(Some(min)) => prop_assert!(min.len() <= BINARY_PREFIX_LENGTH),
                ScalarValue::Binary(None) => (),
                _ => unreachable!(),
            }
            match &statistics.max_value {
                ScalarValue::Binary(Some(max)) => prop_assert!(max.len() <= BINARY_PREFIX_LENGTH),
                ScalarValue::Binary(None) => (),
                _ => unreachable!(),
            }
        }
    }

    proptest::proptest! {
        #[test]
        fn test_min_max_ordering_fsb(values in proptest::collection::vec(proptest::collection::vec(0..u8::MAX, 100), 0..10)) {
            let array = if values.is_empty() {
                // FixedSizeBinary constructors doesn't handle empty inputs.
                new_empty_array(&DataType::FixedSizeBinary(100))
            } else {
                Arc::new(FixedSizeBinaryArray::try_from_iter(values.iter()).unwrap()) as ArrayRef
            };

            let statistics = collect_statistics(&[&array]);

            // If array is empty, assert min and max are null
            if array.is_empty() {
                prop_assert_eq!(&statistics.min_value, &ScalarValue::FixedSizeBinary(100, None));
                prop_assert_eq!(&statistics.max_value, &ScalarValue::FixedSizeBinary(100, None));
            }

            // Assert min is <= all values
            prop_assert!(values.iter().all(|val| statistics.min_value <= ScalarValue::FixedSizeBinary(100, Some(val.clone()))));

            // Assert max is >= all values
            prop_assert!(statistics.max_value.is_null() || values.iter().all(|val| statistics.max_value >= ScalarValue::FixedSizeBinary(100, Some(val.clone()))));

            // Assert min and max are less than BINARY_PREFIX_LENGTH
            match &statistics.min_value {
                ScalarValue::FixedSizeBinary(100, Some(min)) => prop_assert!(min.len() <= BINARY_PREFIX_LENGTH),
                ScalarValue::FixedSizeBinary(100, None) => (),
                _ => unreachable!(),
            }
            match &statistics.max_value {
                ScalarValue::FixedSizeBinary(100, Some(max)) => prop_assert!(max.len() <= BINARY_PREFIX_LENGTH),
                ScalarValue::FixedSizeBinary(100, None) => (),
                _ => unreachable!(),
            }
        }
    }

    #[test]
    fn test_boolean_statistics_multi_array() {
        use arrow_array::BooleanArray;
        use std::sync::Arc;

        // Array 1: [True, False, True, None, None] - 2 nulls
        let bool_array1 = BooleanArray::from(vec![Some(true), Some(false), Some(true), None, None]);
        let array1_ref: ArrayRef = Arc::new(bool_array1);

        // Array 2: [False, True, False, None, None] - 2 nulls
        let bool_array2 =
            BooleanArray::from(vec![Some(false), Some(true), Some(false), None, None]);
        let array2_ref: ArrayRef = Arc::new(bool_array2);

        // Test individual arrays first
        let stats1 = collect_statistics(&[&array1_ref]);
        let stats2 = collect_statistics(&[&array2_ref]);

        assert_eq!(stats1.null_count, 2, "First array should have 2 nulls");
        assert_eq!(stats2.null_count, 2, "Second array should have 2 nulls");

        let array_refs: Vec<&ArrayRef> = vec![&array1_ref, &array2_ref];
        let combined_stats = collect_statistics(&array_refs);

        assert_eq!(
            combined_stats.null_count, 4,
            "Combined statistics should have null_count=4 (2+2), got {}",
            combined_stats.null_count
        );

        assert_eq!(
            combined_stats.min_value,
            ScalarValue::Boolean(Some(false)),
            "Min value should be false"
        );
        assert_eq!(
            combined_stats.max_value,
            ScalarValue::Boolean(Some(true)),
            "Max value should be true"
        );
    }
}
