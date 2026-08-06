// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::{collections::HashMap, sync::Arc};

/// Bits per value for FixedWidth dictionary values (legacy default for 128-bit values)
pub const DICT_FIXED_WIDTH_BITS_PER_VALUE: u64 = 128;
/// Bits per index for dictionary indices (always i32)
pub const DICT_INDICES_BITS_PER_VALUE: u64 = 32;

use arrow_array::{
    Array, DictionaryArray, PrimitiveArray, UInt64Array,
    cast::AsArray,
    types::{
        ArrowDictionaryKeyType, Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type, UInt16Type,
        UInt32Type, UInt64Type,
    },
};
use arrow_buffer::ArrowNativeType;
use arrow_schema::DataType;
use arrow_select::take::TakeOptions;
use lance_core::{Error, Result, error::LanceOptionExt, utils::hash::U8SliceKey};

use crate::{
    buffer::LanceBuffer,
    data::{BlockInfo, DataBlock, FixedWidthDataBlock, VariableWidthBlock},
    statistics::{ComputeStat, GetStat, Stat},
};

// Helper function for normalize_dict_nulls
fn normalize_dict_nulls_impl<K: ArrowDictionaryKeyType>(
    array: Arc<dyn Array>,
) -> Result<Arc<dyn Array>> {
    // TODO: Fast path when there is only one null index? (common case)

    let dict_array = array.as_dictionary_opt::<K>().expect_ok()?;

    if dict_array.values().null_count() == 0 {
        return Ok(array);
    }

    let mut mapping = vec![None; dict_array.values().len()];
    let mut skipped = 0;
    let mut valid_indices = Vec::with_capacity(dict_array.values().len());
    for (old_idx, is_valid) in dict_array.values().nulls().expect_ok()?.iter().enumerate() {
        if is_valid {
            // Should be safe since we are only decreasing K values (e.g. won't overflow u8 keys into u16)
            mapping[old_idx] = Some(K::Native::from_usize(old_idx - skipped).expect_ok()?);
            valid_indices.push(old_idx as u64);
        } else {
            skipped += 1;
            mapping[old_idx] = None;
        }
    }

    let mut keys_builder = PrimitiveArray::<K>::builder(dict_array.keys().len());
    for key in dict_array.keys().iter() {
        if let Some(key) = key {
            if let Some(mapped) = mapping[key.to_usize().expect_ok()?] {
                // Valid item
                keys_builder.append_value(mapped);
            } else {
                // Null via values
                keys_builder.append_null();
            }
        } else {
            // Null via keys
            keys_builder.append_null();
        }
    }
    let keys = keys_builder.finish();

    let valid_indices = UInt64Array::from(valid_indices);
    let values = arrow_select::take::take(
        dict_array.values(),
        &valid_indices,
        Some(TakeOptions {
            check_bounds: false,
        }),
    )?;

    Ok(Arc::new(DictionaryArray::new(keys, values)) as Arc<dyn Array>)
}

/// In Arrow a dictionary array can have nulls in two different places:
/// 1. The keys can be null
/// 2. The values can be null
///
/// We want to normalize this so that all nulls are in the keys.  This way we can store
/// the nulls with the keys as rep-def values the same as any other array.
pub fn normalize_dict_nulls(array: Arc<dyn Array>) -> Result<Arc<dyn Array>> {
    match array.data_type() {
        DataType::Dictionary(key_type, _) => match key_type.as_ref() {
            DataType::UInt8 => normalize_dict_nulls_impl::<UInt8Type>(array),
            DataType::UInt16 => normalize_dict_nulls_impl::<UInt16Type>(array),
            DataType::UInt32 => normalize_dict_nulls_impl::<UInt32Type>(array),
            DataType::UInt64 => normalize_dict_nulls_impl::<UInt64Type>(array),
            DataType::Int8 => normalize_dict_nulls_impl::<Int8Type>(array),
            DataType::Int16 => normalize_dict_nulls_impl::<Int16Type>(array),
            DataType::Int32 => normalize_dict_nulls_impl::<Int32Type>(array),
            DataType::Int64 => normalize_dict_nulls_impl::<Int64Type>(array),
            _ => Err(Error::not_supported_source(
                format!("Unsupported dictionary key type: {}", key_type).into(),
            )),
        },
        _ => Err(Error::internal(format!(
            "Data type is not a dictionary: {}",
            array.data_type()
        ))),
    }
}

fn dict_encode_variable_width<T>(
    variable_width_data_block: &VariableWidthBlock,
    bits_per_offset: u8,
    max_dict_entries: u32,
    max_encoded_size: usize,
) -> Option<(DataBlock, DataBlock)>
where
    T: ArrowNativeType,
    usize: TryFrom<T>,
{
    use std::collections::hash_map::Entry;
    let mut map = HashMap::new();
    let offsets = variable_width_data_block
        .offsets
        .borrow_to_typed_slice::<T>();
    let offsets = offsets.as_ref();

    let max_len = variable_width_data_block
        .get_stat(Stat::MaxLength)
        .expect("VariableWidth DataBlock should have valid `Stat::MaxLength` statistics");
    let max_len = max_len.as_primitive::<UInt64Type>().value(0);

    let max_dict_data_len = variable_width_data_block.data.len();
    let max_len: usize = max_len.try_into().unwrap_or(usize::MAX);
    let dict_data_capacity = max_len
        .saturating_mul(32)
        .max(1024)
        .min(max_dict_data_len)
        .min(max_encoded_size);

    let mut dictionary_buffer: Vec<u8> = Vec::with_capacity(dict_data_capacity);
    let mut dictionary_offsets_buffer = vec![T::default()];
    let mut curr_idx = 0;
    let mut indices_buffer = Vec::with_capacity(variable_width_data_block.num_values as usize);
    let bytes_per_offset = (bits_per_offset / 8) as usize;

    for window in offsets.windows(2) {
        let start = usize::try_from(window[0]).ok()?;
        let end = usize::try_from(window[1]).ok()?;
        if start > end || end > variable_width_data_block.data.len() {
            return None;
        }

        let key = &variable_width_data_block.data[start..end];

        let idx = match map.entry(U8SliceKey(key)) {
            Entry::Occupied(entry) => *entry.get(),
            Entry::Vacant(entry) => {
                if max_dict_entries == 0 || curr_idx as u32 >= max_dict_entries {
                    return None;
                }
                if curr_idx == i32::MAX {
                    return None;
                }
                dictionary_buffer.extend_from_slice(key);
                let dict_offset = T::from_usize(dictionary_buffer.len())?;
                dictionary_offsets_buffer.push(dict_offset);
                let idx = curr_idx;
                entry.insert(idx);
                curr_idx += 1;
                idx
            }
        };

        indices_buffer.push(idx);

        let indices_bytes = indices_buffer
            .len()
            .saturating_mul(DICT_INDICES_BITS_PER_VALUE as usize / 8);
        let offsets_bytes = dictionary_offsets_buffer
            .len()
            .saturating_mul(bytes_per_offset);
        let encoded_size = dictionary_buffer
            .len()
            .saturating_add(indices_bytes)
            .saturating_add(offsets_bytes);
        if encoded_size > max_encoded_size {
            return None;
        }
    }

    let mut dictionary_data_block = DataBlock::VariableWidth(VariableWidthBlock {
        data: LanceBuffer::reinterpret_vec(dictionary_buffer),
        offsets: LanceBuffer::reinterpret_vec(dictionary_offsets_buffer),
        bits_per_offset,
        num_values: curr_idx as u64,
        block_info: BlockInfo::default(),
    });
    dictionary_data_block.compute_stat();

    let mut indices_data_block = DataBlock::FixedWidth(FixedWidthDataBlock {
        data: LanceBuffer::reinterpret_vec(indices_buffer),
        bits_per_value: DICT_INDICES_BITS_PER_VALUE,
        num_values: variable_width_data_block.num_values,
        block_info: BlockInfo::default(),
    });
    indices_data_block.compute_stat();

    Some((indices_data_block, dictionary_data_block))
}

/// Dictionary encodes a data block
///
/// Currently only supported for some common cases (string / binary / 64-bit / 128-bit)
///
/// Returns a block of indices (will always be a fixed width data block) and a block of dictionary
pub fn dictionary_encode(
    data_block: &DataBlock,
    max_dict_entries: u32,
    max_encoded_size: usize,
) -> Option<(DataBlock, DataBlock)> {
    match data_block {
        DataBlock::FixedWidth(fixed_width_data_block) => {
            use std::collections::hash_map::Entry;

            let bytes_per_value = match fixed_width_data_block.bits_per_value {
                64 => 8usize,
                128 => 16usize,
                _ => return None,
            };

            match fixed_width_data_block.bits_per_value {
                64 => {
                    let mut map = HashMap::new();
                    let u64_slice = fixed_width_data_block.data.borrow_to_typed_slice::<u64>();
                    let u64_slice = u64_slice.as_ref();
                    let mut dictionary_buffer =
                        Vec::with_capacity((fixed_width_data_block.num_values as usize).min(1024));
                    let mut indices_buffer =
                        Vec::with_capacity(fixed_width_data_block.num_values as usize);
                    let mut curr_idx: i32 = 0;

                    for &value in u64_slice.iter() {
                        let idx = match map.entry(value) {
                            Entry::Occupied(entry) => *entry.get(),
                            Entry::Vacant(entry) => {
                                if max_dict_entries == 0 || curr_idx as u32 >= max_dict_entries {
                                    return None;
                                }
                                if curr_idx == i32::MAX {
                                    return None;
                                }
                                dictionary_buffer.push(value);
                                let idx = curr_idx;
                                entry.insert(idx);
                                curr_idx += 1;
                                idx
                            }
                        };
                        indices_buffer.push(idx);
                        let dict_bytes = dictionary_buffer.len().saturating_mul(bytes_per_value);
                        let indices_bytes = indices_buffer
                            .len()
                            .saturating_mul(DICT_INDICES_BITS_PER_VALUE as usize / 8);
                        let encoded_size = dict_bytes.saturating_add(indices_bytes);
                        if encoded_size > max_encoded_size {
                            return None;
                        }
                    }

                    let mut dictionary_data_block = DataBlock::FixedWidth(FixedWidthDataBlock {
                        data: LanceBuffer::reinterpret_vec(dictionary_buffer),
                        bits_per_value: 64,
                        num_values: curr_idx as u64,
                        block_info: BlockInfo::default(),
                    });
                    dictionary_data_block.compute_stat();
                    let mut indices_data_block = DataBlock::FixedWidth(FixedWidthDataBlock {
                        data: LanceBuffer::reinterpret_vec(indices_buffer),
                        bits_per_value: DICT_INDICES_BITS_PER_VALUE,
                        num_values: fixed_width_data_block.num_values,
                        block_info: BlockInfo::default(),
                    });
                    indices_data_block.compute_stat();

                    Some((indices_data_block, dictionary_data_block))
                }
                128 => {
                    // TODO: a follow up PR to support `FixedWidth DataBlock with bits_per_value == 256`.
                    let mut map = HashMap::new();
                    let u128_slice = fixed_width_data_block.data.borrow_to_typed_slice::<u128>();
                    let u128_slice = u128_slice.as_ref();
                    let mut dictionary_buffer =
                        Vec::with_capacity((fixed_width_data_block.num_values as usize).min(1024));
                    let mut indices_buffer =
                        Vec::with_capacity(fixed_width_data_block.num_values as usize);
                    let mut curr_idx: i32 = 0;

                    for &value in u128_slice.iter() {
                        let idx = match map.entry(value) {
                            Entry::Occupied(entry) => *entry.get(),
                            Entry::Vacant(entry) => {
                                if max_dict_entries == 0 || curr_idx as u32 >= max_dict_entries {
                                    return None;
                                }
                                if curr_idx == i32::MAX {
                                    return None;
                                }
                                dictionary_buffer.push(value);
                                let idx = curr_idx;
                                entry.insert(idx);
                                curr_idx += 1;
                                idx
                            }
                        };
                        indices_buffer.push(idx);
                        let dict_bytes = dictionary_buffer.len().saturating_mul(bytes_per_value);
                        let indices_bytes = indices_buffer
                            .len()
                            .saturating_mul(DICT_INDICES_BITS_PER_VALUE as usize / 8);
                        let encoded_size = dict_bytes.saturating_add(indices_bytes);
                        if encoded_size > max_encoded_size {
                            return None;
                        }
                    }

                    let mut dictionary_data_block = DataBlock::FixedWidth(FixedWidthDataBlock {
                        data: LanceBuffer::reinterpret_vec(dictionary_buffer),
                        bits_per_value: DICT_FIXED_WIDTH_BITS_PER_VALUE,
                        num_values: curr_idx as u64,
                        block_info: BlockInfo::default(),
                    });
                    dictionary_data_block.compute_stat();
                    let mut indices_data_block = DataBlock::FixedWidth(FixedWidthDataBlock {
                        data: LanceBuffer::reinterpret_vec(indices_buffer),
                        bits_per_value: DICT_INDICES_BITS_PER_VALUE,
                        num_values: fixed_width_data_block.num_values,
                        block_info: BlockInfo::default(),
                    });
                    indices_data_block.compute_stat();

                    Some((indices_data_block, dictionary_data_block))
                }
                _ => None,
            }
        }
        DataBlock::VariableWidth(variable_width_data_block) => {
            match variable_width_data_block.bits_per_offset {
                32 => dict_encode_variable_width::<u32>(
                    variable_width_data_block,
                    32,
                    max_dict_entries,
                    max_encoded_size,
                ),
                64 => dict_encode_variable_width::<u64>(
                    variable_width_data_block,
                    64,
                    max_dict_entries,
                    max_encoded_size,
                ),
                _ => None,
            }
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        buffer::LanceBuffer,
        data::{BlockInfo, FixedWidthDataBlock},
    };
    use arrow_array::{Array, StringArray};
    use std::sync::Arc;

    #[test]
    fn test_dictionary_encode_abort_fixed_width() {
        // Create a u128 block with very high cardinality where dict encoding
        // would result in larger data (dictionary overhead + indices > original)
        let num_values = 120u64;

        // Create actual data: each value is unique u128 so dictionary encode will not be helpful
        let mut data = Vec::with_capacity(num_values as usize);
        for i in 0..num_values {
            data.push(i as u128);
        }

        let mut data_block = DataBlock::FixedWidth(FixedWidthDataBlock {
            bits_per_value: DICT_FIXED_WIDTH_BITS_PER_VALUE,
            data: LanceBuffer::reinterpret_vec(data),
            num_values,
            block_info: BlockInfo::default(),
        });

        // Compute stats naturally
        data_block.compute_stat();

        // Dictionary encoding should abort and return None
        let max_encoded_size = usize::try_from(data_block.data_size()).unwrap_or(usize::MAX);
        let result = dictionary_encode(&data_block, 1000, max_encoded_size);
        assert!(
            result.is_none(),
            "Dictionary encoding should abort for high cardinality u128 data"
        );
    }

    #[test]
    fn test_dictionary_encode_success_fixed_width() {
        // Create a u128 block with low cardinality where dict encoding helps
        let num_values = 120u64;
        let cardinality = 3u64;

        // Create data with few unique u128 values
        let mut data = Vec::with_capacity(num_values as usize);
        for i in 0..num_values {
            data.push((i % cardinality) as u128);
        }

        let mut data_block = DataBlock::FixedWidth(FixedWidthDataBlock {
            bits_per_value: DICT_FIXED_WIDTH_BITS_PER_VALUE,
            data: LanceBuffer::reinterpret_vec(data),
            num_values,
            block_info: BlockInfo::default(),
        });

        // Compute stats naturally
        data_block.compute_stat();

        // Dictionary encoding should succeed and return Some
        let max_encoded_size = usize::try_from(data_block.data_size()).unwrap_or(usize::MAX);
        let result = dictionary_encode(&data_block, 1000, max_encoded_size);
        assert!(
            result.is_some(),
            "Dictionary encoding should succeed for low cardinality u128 data"
        );

        if let Some((indices, dictionary)) = result {
            // Verify indices block
            if let DataBlock::FixedWidth(indices_block) = indices {
                assert_eq!(indices_block.num_values, num_values);
                assert_eq!(indices_block.bits_per_value, DICT_INDICES_BITS_PER_VALUE);
            } else {
                panic!("Expected FixedWidth indices block");
            }

            // Verify dictionary block
            if let DataBlock::FixedWidth(dict_block) = dictionary {
                assert_eq!(dict_block.num_values, cardinality);
                assert_eq!(dict_block.bits_per_value, DICT_FIXED_WIDTH_BITS_PER_VALUE);
            } else {
                panic!("Expected FixedWidth dictionary block");
            }
        }
    }

    #[test]
    fn test_dictionary_encode_abort_variable_width() {
        // Create a variable-width block with high cardinality where dict encoding
        // won't provide sufficient benefit
        let num_values = 120u64;
        let mut values = Vec::with_capacity(num_values as usize);
        for i in 0..num_values {
            values.push(format!("unique_value_{:04}", i));
        }
        let array = StringArray::from(values);
        // from_array already computes stats
        let data_block = DataBlock::from_array(Arc::new(array) as Arc<dyn Array>);

        // Dictionary encoding should abort and return None
        let max_encoded_size = usize::try_from(data_block.data_size()).unwrap_or(usize::MAX);
        let result = dictionary_encode(&data_block, 10, max_encoded_size);
        assert!(
            result.is_none(),
            "Dictionary encoding should abort for high cardinality string data"
        );
    }

    #[test]
    fn test_dictionary_encode_success_low_cardinality() {
        // Create a variable-width block with low cardinality where dict encoding helps
        let num_values = 120u64;
        let cardinality = 3u64;

        let mut values = Vec::with_capacity(num_values as usize);
        for i in 0..num_values {
            values.push(format!("value_{}", i % cardinality));
        }

        let array = StringArray::from(values);
        let data_block = DataBlock::from_array(Arc::new(array) as Arc<dyn Array>);

        // Dictionary encoding should succeed and return Some
        let max_encoded_size = usize::try_from(data_block.data_size()).unwrap_or(usize::MAX);
        let result = dictionary_encode(&data_block, 100, max_encoded_size);
        assert!(
            result.is_some(),
            "Dictionary encoding should succeed for low cardinality data"
        );

        if let Some((indices, dictionary)) = result {
            // Verify indices block
            if let DataBlock::FixedWidth(indices_block) = indices {
                assert_eq!(indices_block.num_values, num_values);
                assert_eq!(indices_block.bits_per_value, DICT_INDICES_BITS_PER_VALUE);
            } else {
                panic!("Expected FixedWidth indices block");
            }

            // Verify dictionary block
            if let DataBlock::VariableWidth(dict_block) = dictionary {
                assert_eq!(dict_block.num_values, cardinality);
            } else {
                panic!("Expected VariableWidth dictionary block");
            }
        }
    }

    #[test]
    fn test_dictionary_encode_invalid_offset_width_returns_none() {
        let array = StringArray::from(vec!["a", "b", "c", "a"]);
        let data_block = DataBlock::from_array(Arc::new(array) as Arc<dyn Array>);
        let invalid_block = match data_block {
            DataBlock::VariableWidth(mut var) => {
                var.bits_per_offset = 16;
                DataBlock::VariableWidth(var)
            }
            other => panic!("Expected VariableWidth data block, got {:?}", other),
        };
        let max_encoded_size = usize::try_from(invalid_block.data_size()).unwrap_or(usize::MAX);
        assert!(dictionary_encode(&invalid_block, 100, max_encoded_size).is_none());
    }

    #[test]
    fn test_dictionary_encode_respects_size_limit() {
        let num_values = 10_000u64;
        let cardinality = 50u64;

        let mut values = Vec::with_capacity(num_values as usize);
        for i in 0..num_values {
            values.push(format!("value_{:08}", i % cardinality));
        }

        let array = StringArray::from(values);
        let data_block = DataBlock::from_array(Arc::new(array) as Arc<dyn Array>);

        let full_size = usize::try_from(data_block.data_size()).unwrap_or(usize::MAX);
        let too_small_limit = full_size / 10;
        assert!(dictionary_encode(&data_block, 1000, too_small_limit).is_none());
        assert!(dictionary_encode(&data_block, 1000, full_size).is_some());
    }

    #[test]
    fn test_dictionary_encode_respects_entry_limit() {
        let num_values = 10_000u64;
        let cardinality = 200u64;

        let mut values = Vec::with_capacity(num_values as usize);
        for i in 0..num_values {
            values.push(format!("value_{:08}", i % cardinality));
        }

        let array = StringArray::from(values);
        let data_block = DataBlock::from_array(Arc::new(array) as Arc<dyn Array>);

        let max_encoded_size = usize::try_from(data_block.data_size()).unwrap_or(usize::MAX);
        assert!(dictionary_encode(&data_block, 10, max_encoded_size).is_none());
        assert!(dictionary_encode(&data_block, 500, max_encoded_size).is_some());
    }
}
