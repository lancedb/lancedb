// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::{collections::HashMap, env, hash::RandomState, sync::Arc};

#[cfg(test)]
use arrow_array::cast::AsArray;
use arrow_array::{ArrayRef, UInt8Array};
use arrow_schema::DataType;
use hyperloglogplus::{HyperLogLog, HyperLogLogPlus};

use crate::{
    array_encoding::{
        logical::{
            blob::BlobFieldEncoder, list::ListFieldEncoder, primitive::PrimitiveFieldEncoder,
        },
        physical::{
            basic::BasicEncoder,
            binary::BinaryEncoder,
            dictionary::{AlreadyDictionaryEncoder, DictionaryEncoder},
            fixed_size_list::FslEncoder,
            fsst::FsstArrayEncoder,
            packed_struct::PackedStructEncoder,
        },
    },
    constants::{
        COMPRESSION_LEVEL_META_KEY, COMPRESSION_META_KEY, PACKED_STRUCT_LEGACY_META_KEY,
        PACKED_STRUCT_META_KEY,
    },
    encoder::{
        ArrayEncoder, ArrayEncodingStrategy, ColumnIndexSequence, FieldEncoder,
        FieldEncodingContext, FieldEncodingStrategy,
    },
    encodings::{
        logical::r#struct::StructFieldEncoder,
        physical::{
            block::{CompressionConfig, CompressionScheme},
            value::ValueEncoder,
        },
    },
};

use lance_arrow::BLOB_META_KEY;
use lance_core::datatypes::{BLOB_DESC_FIELD, Field};
use lance_core::{Error, Result};

/// Field-to-column composition for the `pb::ArrayEncoding` grammar.
#[derive(Debug)]
pub struct ArrayFieldEncodingStrategy {
    array_encoding_strategy: Arc<dyn ArrayEncodingStrategy>,
}

impl ArrayFieldEncodingStrategy {
    /// Create the field strategy for the `pb::ArrayEncoding` grammar.
    ///
    /// ```
    /// use lance_encoding::encoder::ArrayFieldEncodingStrategy;
    ///
    /// let strategy = ArrayFieldEncodingStrategy::new();
    /// ```
    pub fn new() -> Self {
        Self {
            array_encoding_strategy: Arc::new(ArrayStrategy),
        }
    }

    fn is_primitive_type(data_type: &DataType) -> bool {
        matches!(
            data_type,
            DataType::Boolean
                | DataType::Date32
                | DataType::Date64
                | DataType::Decimal128(_, _)
                | DataType::Decimal256(_, _)
                | DataType::Duration(_)
                | DataType::Float16
                | DataType::Float32
                | DataType::Float64
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::Int8
                | DataType::Interval(_)
                | DataType::Null
                | DataType::Time32(_)
                | DataType::Time64(_)
                | DataType::Timestamp(_, _)
                | DataType::UInt16
                | DataType::UInt32
                | DataType::UInt64
                | DataType::UInt8
                | DataType::FixedSizeBinary(_)
                | DataType::FixedSizeList(_, _)
                | DataType::Binary
                | DataType::LargeBinary
                | DataType::Utf8
                | DataType::LargeUtf8,
        )
    }
}

impl Default for ArrayFieldEncodingStrategy {
    fn default() -> Self {
        Self::new()
    }
}

impl FieldEncodingStrategy for ArrayFieldEncodingStrategy {
    fn create_field_encoder(
        &self,
        field: &Field,
        column_index: &mut ColumnIndexSequence,
        context: &FieldEncodingContext<'_>,
    ) -> Result<Box<dyn FieldEncoder>> {
        let options = context.options;
        let data_type = field.data_type();
        if Self::is_primitive_type(&data_type) {
            let column_index = column_index.next_column_index(field.id as u32);
            if field.metadata.contains_key(BLOB_META_KEY) {
                let mut packed_meta = HashMap::new();
                packed_meta.insert(PACKED_STRUCT_META_KEY.to_string(), "true".to_string());
                let desc_field =
                    Field::try_from(BLOB_DESC_FIELD.clone().with_metadata(packed_meta)).unwrap();
                let desc_encoder = Box::new(PrimitiveFieldEncoder::try_new(
                    options,
                    self.array_encoding_strategy.clone(),
                    column_index,
                    desc_field,
                )?);
                Ok(Box::new(BlobFieldEncoder::new(desc_encoder)))
            } else {
                Ok(Box::new(PrimitiveFieldEncoder::try_new(
                    options,
                    self.array_encoding_strategy.clone(),
                    column_index,
                    field.clone(),
                )?))
            }
        } else {
            match data_type {
                DataType::List(_child) | DataType::LargeList(_child) => {
                    let list_idx = column_index.next_column_index(field.id as u32);
                    let inner_encoding = context.strategy.create_field_encoder(
                        &field.children[0],
                        column_index,
                        context,
                    )?;
                    let offsets_encoder =
                        Arc::new(BasicEncoder::new(Box::new(ValueEncoder::default())));
                    Ok(Box::new(ListFieldEncoder::new(
                        inner_encoding,
                        offsets_encoder,
                        options.cache_bytes_per_column,
                        options.keep_original_array,
                        list_idx,
                    )))
                }
                DataType::Struct(_) => {
                    let field_metadata = &field.metadata;
                    if field_metadata
                        .get(PACKED_STRUCT_LEGACY_META_KEY)
                        .map(|v| v == "true")
                        .unwrap_or(field_metadata.contains_key(PACKED_STRUCT_META_KEY))
                    {
                        Ok(Box::new(PrimitiveFieldEncoder::try_new(
                            options,
                            self.array_encoding_strategy.clone(),
                            column_index.next_column_index(field.id as u32),
                            field.clone(),
                        )?))
                    } else {
                        let header_idx = column_index.next_column_index(field.id as u32);
                        let children_encoders = field
                            .children
                            .iter()
                            .map(|field| {
                                context
                                    .strategy
                                    .create_field_encoder(field, column_index, context)
                            })
                            .collect::<Result<Vec<_>>>()?;
                        Ok(Box::new(StructFieldEncoder::new(
                            children_encoders,
                            header_idx,
                        )))
                    }
                }
                DataType::Dictionary(_, value_type) => {
                    // A dictionary of primitive is, itself, primitive
                    if Self::is_primitive_type(&value_type) {
                        Ok(Box::new(PrimitiveFieldEncoder::try_new(
                            options,
                            self.array_encoding_strategy.clone(),
                            column_index.next_column_index(field.id as u32),
                            field.clone(),
                        )?))
                    } else {
                        // A dictionary of logical is, itself, logical and we don't support that today
                        // It could be possible (e.g. store indices in one column and values in remaining columns)
                        // but would be a significant amount of work
                        //
                        // An easier fallback implementation would be to decode-on-write and encode-on-read
                        Err(Error::not_supported_source(format!("cannot encode a dictionary column whose value type is a logical type ({})", value_type).into()))
                    }
                }
                _ => todo!("Implement encoding for field {}", field),
            }
        }
    }
}

/// Page-encoding selection for the `pb::ArrayEncoding` grammar.
#[derive(Debug)]
struct ArrayStrategy;

impl ArrayStrategy {
    fn get_field_compression(field_meta: &HashMap<String, String>) -> Option<CompressionConfig> {
        let compression = field_meta.get(COMPRESSION_META_KEY)?;
        let compression_scheme = compression.parse::<CompressionScheme>();
        match compression_scheme {
            Ok(compression_scheme) => Some(CompressionConfig::new(
                compression_scheme,
                field_meta
                    .get(COMPRESSION_LEVEL_META_KEY)
                    .and_then(|level| level.parse().ok()),
            )),
            Err(_) => None,
        }
    }

    fn default_binary_encoder(
        arrays: &[ArrayRef],
        field_meta: Option<&HashMap<String, String>>,
        data_size: u64,
    ) -> Result<Box<dyn ArrayEncoder>> {
        let bin_indices_encoder =
            Self::choose_array_encoder(arrays, &DataType::UInt64, data_size, false, None)?;

        if let Some(compression) = field_meta.and_then(Self::get_field_compression) {
            if compression.scheme() == CompressionScheme::Fsst {
                // User requested FSST
                let raw_encoder = Box::new(BinaryEncoder::try_new(bin_indices_encoder, None)?);
                Ok(Box::new(FsstArrayEncoder::new(raw_encoder)))
            } else {
                // Generic compression
                Ok(Box::new(BinaryEncoder::try_new(
                    bin_indices_encoder,
                    Some(compression),
                )?))
            }
        } else {
            Ok(Box::new(BinaryEncoder::try_new(bin_indices_encoder, None)?))
        }
    }

    fn choose_array_encoder(
        arrays: &[ArrayRef],
        data_type: &DataType,
        data_size: u64,
        use_dict_encoding: bool,
        field_meta: Option<&HashMap<String, String>>,
    ) -> Result<Box<dyn ArrayEncoder>> {
        match data_type {
            DataType::FixedSizeList(inner, dimension) => {
                Ok(Box::new(BasicEncoder::new(Box::new(FslEncoder::new(
                    Self::choose_array_encoder(
                        arrays,
                        inner.data_type(),
                        data_size,
                        use_dict_encoding,
                        None,
                    )?,
                    *dimension as u32,
                )))))
            }
            DataType::Dictionary(key_type, value_type) => {
                let key_encoder =
                    Self::choose_array_encoder(arrays, key_type, data_size, false, None)?;
                let value_encoder =
                    Self::choose_array_encoder(arrays, value_type, data_size, false, None)?;

                Ok(Box::new(AlreadyDictionaryEncoder::new(
                    key_encoder,
                    value_encoder,
                )))
            }
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Binary | DataType::LargeBinary => {
                if use_dict_encoding {
                    let dict_indices_encoder = Self::choose_array_encoder(
                        // We need to pass arrays to this method to figure out what kind of compression to
                        // use but we haven't actually calculated the indices yet.  For now, we just assume
                        // worst case and use the full range.  In the future maybe we can pass in statistics
                        // instead of the actual data
                        &[Arc::new(UInt8Array::from_iter_values(0_u8..255_u8))],
                        &DataType::UInt8,
                        data_size,
                        false,
                        None,
                    )?;
                    let dict_items_encoder = Self::choose_array_encoder(
                        arrays,
                        &DataType::Utf8,
                        data_size,
                        false,
                        None,
                    )?;

                    Ok(Box::new(DictionaryEncoder::new(
                        dict_indices_encoder,
                        dict_items_encoder,
                    )))
                } else {
                    Self::default_binary_encoder(arrays, field_meta, data_size)
                }
            }
            DataType::Struct(fields) => {
                let num_fields = fields.len();
                let mut inner_encoders = Vec::new();

                for i in 0..num_fields {
                    let inner_datatype = fields[i].data_type();
                    let inner_encoder = Self::choose_array_encoder(
                        arrays,
                        inner_datatype,
                        data_size,
                        use_dict_encoding,
                        None,
                    )?;
                    inner_encoders.push(inner_encoder);
                }

                Ok(Box::new(PackedStructEncoder::new(inner_encoders)))
            }
            DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => Ok(
                Box::new(BasicEncoder::new(Box::new(ValueEncoder::default()))),
            ),

            // TODO: for signed integers, I intend to make it a cascaded encoding, a sparse array for the negative values and very wide(bit-width) values,
            // then a bitpacked array for the narrow(bit-width) values, I need `BitpackedForNeg` to be merged first, I am
            // thinking about putting this sparse array in the metadata so bitpacking remain using one page buffer only.
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => Ok(Box::new(
                BasicEncoder::new(Box::new(ValueEncoder::default())),
            )),
            _ => Ok(Box::new(BasicEncoder::new(Box::new(
                ValueEncoder::default(),
            )))),
        }
    }
}

fn get_dict_encoding_threshold() -> u64 {
    env::var("LANCE_DICT_ENCODING_THRESHOLD")
        .ok()
        .and_then(|val| val.parse().ok())
        .unwrap_or(100)
}

// check whether we want to use dictionary encoding or not
// by applying a threshold on cardinality
// returns true if cardinality < threshold but false if the total number of rows is less than the threshold
// The choice to use 100 is just a heuristic for now
// hyperloglog is used for cardinality estimation
// error rate = 1.04 / sqrt(2^p), where p is the precision
// and error rate is 1.04 / sqrt(2^12) = 1.56%
fn check_dict_encoding(arrays: &[ArrayRef], threshold: u64) -> bool {
    let num_total_rows = arrays.iter().map(|arr| arr.len()).sum::<usize>();
    if num_total_rows < threshold as usize {
        return false;
    }
    const PRECISION: u8 = 12;

    let mut hll: HyperLogLogPlus<String, RandomState> =
        HyperLogLogPlus::new(PRECISION, RandomState::new()).unwrap();

    for arr in arrays {
        let string_array = arrow_array::cast::as_string_array(arr);
        for value in string_array.iter().flatten() {
            hll.insert(value);
            let estimated_cardinality = hll.count() as u64;
            if estimated_cardinality >= threshold {
                return false;
            }
        }
    }

    true
}

#[cfg(test)]
fn check_fixed_size_encoding(arrays: &[ArrayRef]) -> Option<u64> {
    if arrays.is_empty() {
        return None;
    }

    // make sure no array has an empty string
    if !arrays.iter().all(|arr| {
        if let Some(arr) = arr.as_string_opt::<i32>() {
            arr.iter().flatten().all(|s| !s.is_empty())
        } else if let Some(arr) = arr.as_binary_opt::<i32>() {
            arr.iter().flatten().all(|s| !s.is_empty())
        } else if let Some(arr) = arr.as_string_opt::<i64>() {
            arr.iter().flatten().all(|s| !s.is_empty())
        } else if let Some(arr) = arr.as_binary_opt::<i64>() {
            arr.iter().flatten().all(|s| !s.is_empty())
        } else {
            panic!("wrong dtype");
        }
    }) {
        return None;
    }

    let lengths = arrays
        .iter()
        .flat_map(|arr| {
            if let Some(arr) = arr.as_string_opt::<i32>() {
                let offsets = arr.offsets().inner();
                offsets
                    .windows(2)
                    .map(|w| (w[1] - w[0]) as u64)
                    .collect::<Vec<_>>()
            } else if let Some(arr) = arr.as_binary_opt::<i32>() {
                let offsets = arr.offsets().inner();
                offsets
                    .windows(2)
                    .map(|w| (w[1] - w[0]) as u64)
                    .collect::<Vec<_>>()
            } else if let Some(arr) = arr.as_string_opt::<i64>() {
                let offsets = arr.offsets().inner();
                offsets
                    .windows(2)
                    .map(|w| (w[1] - w[0]) as u64)
                    .collect::<Vec<_>>()
            } else if let Some(arr) = arr.as_binary_opt::<i64>() {
                let offsets = arr.offsets().inner();
                offsets
                    .windows(2)
                    .map(|w| (w[1] - w[0]) as u64)
                    .collect::<Vec<_>>()
            } else {
                panic!("wrong dtype");
            }
        })
        .collect::<Vec<_>>();

    // find first non-zero value in lengths
    let first_non_zero = lengths.iter().position(|&x| x != 0);
    if let Some(first_non_zero) = first_non_zero {
        // make sure all lengths are equal to first_non_zero length or zero
        if !lengths
            .iter()
            .all(|&x| x == 0 || x == lengths[first_non_zero])
        {
            return None;
        }

        // set the byte width
        Some(lengths[first_non_zero])
    } else {
        None
    }
}

impl ArrayEncodingStrategy for ArrayStrategy {
    fn create_array_encoder(
        &self,
        arrays: &[ArrayRef],
        field: &Field,
    ) -> Result<Box<dyn ArrayEncoder>> {
        let data_size = arrays
            .iter()
            .map(|arr| arr.get_buffer_memory_size() as u64)
            .sum::<u64>();
        let data_type = arrays[0].data_type();

        let use_dict_encoding = data_type == &DataType::Utf8
            && check_dict_encoding(arrays, get_dict_encoding_threshold());

        Self::choose_array_encoder(
            arrays,
            data_type,
            data_size,
            use_dict_encoding,
            Some(&field.metadata),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ArrayEncodingStrategy, ArrayStrategy, check_dict_encoding, check_fixed_size_encoding,
    };
    use crate::constants::{COMPRESSION_LEVEL_META_KEY, COMPRESSION_META_KEY};
    use arrow_array::{ArrayRef, StringArray};
    use arrow_schema::Field;
    use std::collections::HashMap;
    use std::sync::Arc;

    fn is_dict_encoding_applicable(arr: Vec<Option<&str>>, threshold: u64) -> bool {
        let arr = StringArray::from(arr);
        let arr = Arc::new(arr) as ArrayRef;
        check_dict_encoding(&[arr], threshold)
    }

    #[test]
    fn test_dict_encoding_should_be_applied_if_cardinality_less_than_threshold() {
        assert!(is_dict_encoding_applicable(
            vec![Some("a"), Some("b"), Some("a"), Some("b")],
            3,
        ));
    }

    #[test]
    fn test_dict_encoding_should_not_be_applied_if_cardinality_larger_than_threshold() {
        assert!(!is_dict_encoding_applicable(
            vec![Some("a"), Some("b"), Some("c"), Some("d")],
            3,
        ));
    }

    #[test]
    fn test_dict_encoding_should_not_be_applied_if_cardinality_equal_to_threshold() {
        assert!(!is_dict_encoding_applicable(
            vec![Some("a"), Some("b"), Some("c"), Some("a")],
            3,
        ));
    }

    #[test]
    fn test_dict_encoding_should_not_be_applied_for_empty_arrays() {
        assert!(!is_dict_encoding_applicable(vec![], 3));
    }

    #[test]
    fn test_dict_encoding_should_not_be_applied_for_smaller_than_threshold_arrays() {
        assert!(!is_dict_encoding_applicable(vec![Some("a"), Some("a")], 3));
    }

    fn is_fixed_size_encoding_applicable(arrays: Vec<Vec<Option<&str>>>) -> bool {
        let mut final_arrays = Vec::new();
        for arr in arrays {
            let arr = StringArray::from(arr);
            let arr = Arc::new(arr) as ArrayRef;
            final_arrays.push(arr);
        }

        check_fixed_size_encoding(&final_arrays).is_some()
    }

    #[test]
    fn test_fixed_size_binary_encoding_applicable() {
        assert!(!is_fixed_size_encoding_applicable(vec![vec![]]));

        assert!(is_fixed_size_encoding_applicable(vec![vec![
            Some("a"),
            Some("b")
        ]]));

        assert!(!is_fixed_size_encoding_applicable(vec![vec![
            Some("abc"),
            Some("de")
        ]]));

        assert!(is_fixed_size_encoding_applicable(vec![vec![
            Some("pqr"),
            None
        ]]));

        assert!(!is_fixed_size_encoding_applicable(vec![vec![
            Some("pqr"),
            Some("")
        ]]));

        assert!(!is_fixed_size_encoding_applicable(vec![vec![
            Some(""),
            Some("")
        ]]));
    }

    #[test]
    fn test_fixed_size_binary_encoding_applicable_multiple_arrays() {
        assert!(is_fixed_size_encoding_applicable(vec![
            vec![Some("a"), Some("b")],
            vec![Some("c"), Some("d")]
        ]));

        assert!(!is_fixed_size_encoding_applicable(vec![
            vec![Some("ab"), Some("bc")],
            vec![Some("c"), Some("d")]
        ]));

        assert!(!is_fixed_size_encoding_applicable(vec![
            vec![Some("ab"), None],
            vec![None, Some("d")]
        ]));

        assert!(is_fixed_size_encoding_applicable(vec![
            vec![Some("a"), None],
            vec![None, Some("d")]
        ]));

        assert!(!is_fixed_size_encoding_applicable(vec![
            vec![Some(""), None],
            vec![None, Some("")]
        ]));

        assert!(!is_fixed_size_encoding_applicable(vec![
            vec![None, None],
            vec![None, None]
        ]));
    }

    fn verify_array_encoder(
        array: ArrayRef,
        field_meta: Option<HashMap<String, String>>,
        expected_encoder: &str,
    ) {
        let encoding_strategy = ArrayStrategy;
        let mut field = Field::new("test_field", array.data_type().clone(), true);
        if let Some(field_meta) = field_meta {
            field.set_metadata(field_meta);
        }
        let lance_field = lance_core::datatypes::Field::try_from(field).unwrap();
        let encoder_result = encoding_strategy.create_array_encoder(&[array], &lance_field);
        assert!(encoder_result.is_ok());
        let encoder = encoder_result.unwrap();
        assert_eq!(format!("{:?}", encoder).as_str(), expected_encoder);
    }

    #[test]
    fn test_choose_encoder_for_zstd_compressed_string_field() {
        verify_array_encoder(
            Arc::new(StringArray::from(vec!["a", "bb", "ccc"])),
            Some(HashMap::from([(
                COMPRESSION_META_KEY.to_string(),
                "zstd".to_string(),
            )])),
            "BinaryEncoder { indices_encoder: BasicEncoder { values_encoder: ValueEncoder }, compression_config: Some(CompressionConfig { scheme: Zstd, level: None }), buffer_compressor: Some(ZstdBufferCompressor { compression_level: 0 }) }",
        );
    }

    #[test]
    fn test_choose_encoder_for_zstd_compression_level() {
        verify_array_encoder(
            Arc::new(StringArray::from(vec!["a", "bb", "ccc"])),
            Some(HashMap::from([
                (COMPRESSION_META_KEY.to_string(), "zstd".to_string()),
                (COMPRESSION_LEVEL_META_KEY.to_string(), "22".to_string()),
            ])),
            "BinaryEncoder { indices_encoder: BasicEncoder { values_encoder: ValueEncoder }, compression_config: Some(CompressionConfig { scheme: Zstd, level: Some(22) }), buffer_compressor: Some(ZstdBufferCompressor { compression_level: 22 }) }",
        );
    }
}
