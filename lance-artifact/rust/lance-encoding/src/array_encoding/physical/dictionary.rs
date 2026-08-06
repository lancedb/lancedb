// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::sync::Arc;
use std::vec;

use arrow_array::builder::{ArrayBuilder, StringBuilder};
use arrow_array::cast::AsArray;
use arrow_array::types::UInt8Type;
use arrow_array::{
    Array, ArrayRef, DictionaryArray, StringArray, UInt8Array, make_array, new_null_array,
};
use arrow_schema::DataType;
use futures::{FutureExt, future::BoxFuture};
use lance_arrow::DataTypeExt;
use lance_core::{Error, Result};
use std::collections::HashMap;

use crate::array_encoding::logical::primitive::PrimitiveFieldDecoder;
use crate::buffer::LanceBuffer;
use crate::data::{
    BlockInfo, DataBlock, DictionaryDataBlock, FixedWidthDataBlock, NullableDataBlock,
    VariableWidthBlock,
};
use crate::decoder::LogicalPageDecoder;
use crate::format::ProtobufUtils;
use crate::{
    EncodingsIo,
    decoder::{PageScheduler, PrimitivePageDecoder},
    encoder::{ArrayEncoder, EncodedArray},
};

#[derive(Debug)]
pub struct DictionaryPageScheduler {
    indices_scheduler: Arc<dyn PageScheduler>,
    items_scheduler: Arc<dyn PageScheduler>,
    // The number of items in the dictionary
    num_dictionary_items: u32,
    // If true, decode the dictionary items.  If false, leave them dictionary encoded (e.g. the
    // output type is probably a dictionary type)
    should_decode_dict: bool,
}

impl DictionaryPageScheduler {
    pub fn new(
        indices_scheduler: Arc<dyn PageScheduler>,
        items_scheduler: Arc<dyn PageScheduler>,
        num_dictionary_items: u32,
        should_decode_dict: bool,
    ) -> Self {
        Self {
            indices_scheduler,
            items_scheduler,
            num_dictionary_items,
            should_decode_dict,
        }
    }
}

impl PageScheduler for DictionaryPageScheduler {
    fn schedule_ranges(
        &self,
        ranges: &[std::ops::Range<u64>],
        scheduler: &Arc<dyn EncodingsIo>,
        top_level_row: u64,
    ) -> BoxFuture<'static, Result<Box<dyn PrimitivePageDecoder>>> {
        // We want to decode indices and items
        // e.g. indices [0, 1, 2, 0, 1, 0]
        // items (dictionary) ["abcd", "hello", "apple"]
        // This will map to ["abcd", "hello", "apple", "abcd", "hello", "abcd"]
        // We decode all the items during scheduling itself
        // These are used to rebuild the string later

        // Schedule indices for decoding
        let indices_page_decoder =
            self.indices_scheduler
                .schedule_ranges(ranges, scheduler, top_level_row);

        // Schedule items for decoding
        let items_range = 0..(self.num_dictionary_items as u64);
        let items_page_decoder = self.items_scheduler.schedule_ranges(
            std::slice::from_ref(&items_range),
            scheduler,
            top_level_row,
        );

        let copy_size = self.num_dictionary_items as u64;

        if self.should_decode_dict {
            tokio::spawn(async move {
                let items_decoder: Arc<dyn PrimitivePageDecoder> =
                    Arc::from(items_page_decoder.await?);

                let mut primitive_wrapper = PrimitiveFieldDecoder::new_from_data(
                    items_decoder.clone(),
                    DataType::Utf8,
                    copy_size,
                    false,
                );

                // Decode all items
                let drained_task = primitive_wrapper.drain(copy_size)?;
                let items_decode_task = drained_task.task;
                let (decoded_dict, _) = items_decode_task.decode()?;

                let indices_decoder: Box<dyn PrimitivePageDecoder> = indices_page_decoder.await?;

                Ok(Box::new(DictionaryPageDecoder {
                    decoded_dict,
                    indices_decoder,
                }) as Box<dyn PrimitivePageDecoder>)
            })
            .map(|join_handle| join_handle.unwrap())
            .boxed()
        } else {
            let num_dictionary_items = self.num_dictionary_items;
            tokio::spawn(async move {
                let items_decoder: Arc<dyn PrimitivePageDecoder> =
                    Arc::from(items_page_decoder.await?);

                let decoded_dict = items_decoder
                    .decode(0, num_dictionary_items as u64)?
                    .clone();

                let indices_decoder = indices_page_decoder.await?;

                Ok(Box::new(DirectDictionaryPageDecoder {
                    decoded_dict,
                    indices_decoder,
                }) as Box<dyn PrimitivePageDecoder>)
            })
            .map(|join_handle| join_handle.unwrap())
            .boxed()
        }
    }
}

struct DirectDictionaryPageDecoder {
    decoded_dict: DataBlock,
    indices_decoder: Box<dyn PrimitivePageDecoder>,
}

impl PrimitivePageDecoder for DirectDictionaryPageDecoder {
    fn decode(&self, rows_to_skip: u64, num_rows: u64) -> Result<DataBlock> {
        let indices = self
            .indices_decoder
            .decode(rows_to_skip, num_rows)?
            .as_fixed_width()
            .unwrap();
        let dict = self.decoded_dict.clone();
        Ok(DataBlock::Dictionary(DictionaryDataBlock {
            indices,
            dictionary: Box::new(dict),
        }))
    }
}

struct DictionaryPageDecoder {
    decoded_dict: Arc<dyn Array>,
    indices_decoder: Box<dyn PrimitivePageDecoder>,
}

impl PrimitivePageDecoder for DictionaryPageDecoder {
    fn decode(&self, rows_to_skip: u64, num_rows: u64) -> Result<DataBlock> {
        // Decode the indices
        let indices_data = self.indices_decoder.decode(rows_to_skip, num_rows)?;

        let indices_array = make_array(indices_data.into_arrow(DataType::UInt8, false)?);
        let indices_array = indices_array.as_primitive::<UInt8Type>();

        let dictionary = self.decoded_dict.clone();

        let adjusted_indices: UInt8Array = indices_array
            .iter()
            .map(|x| match x {
                Some(0) => None,
                Some(x) => Some(x - 1),
                None => None,
            })
            .collect();

        // Build dictionary array using indices and items
        let dict_array =
            DictionaryArray::<UInt8Type>::try_new(adjusted_indices, dictionary).unwrap();
        let string_array = arrow_cast::cast(&dict_array, &DataType::Utf8).unwrap();
        let string_array = string_array.as_any().downcast_ref::<StringArray>().unwrap();

        let null_buffer = string_array.nulls().map(|n| n.buffer().clone());
        let offsets_buffer = string_array.offsets().inner().inner().clone();
        let bytes_buffer = string_array.values().clone();

        let string_data = DataBlock::VariableWidth(VariableWidthBlock {
            bits_per_offset: 32,
            data: LanceBuffer::from(bytes_buffer),
            offsets: LanceBuffer::from(offsets_buffer),
            num_values: num_rows,
            block_info: BlockInfo::new(),
        });
        if let Some(nulls) = null_buffer {
            Ok(DataBlock::Nullable(NullableDataBlock {
                data: Box::new(string_data),
                nulls: LanceBuffer::from(nulls),
                block_info: BlockInfo::new(),
            }))
        } else {
            Ok(string_data)
        }
    }
}

/// An encoder for data that is already dictionary encoded.  Stores the
/// data as a dictionary encoding.
#[derive(Debug)]
pub struct AlreadyDictionaryEncoder {
    indices_encoder: Box<dyn ArrayEncoder>,
    items_encoder: Box<dyn ArrayEncoder>,
}

impl AlreadyDictionaryEncoder {
    pub fn new(
        indices_encoder: Box<dyn ArrayEncoder>,
        items_encoder: Box<dyn ArrayEncoder>,
    ) -> Self {
        Self {
            indices_encoder,
            items_encoder,
        }
    }
}

impl ArrayEncoder for AlreadyDictionaryEncoder {
    fn encode(
        &self,
        data: DataBlock,
        data_type: &DataType,
        buffer_index: &mut u32,
    ) -> Result<EncodedArray> {
        let DataType::Dictionary(key_type, value_type) = data_type else {
            panic!("Expected dictionary type");
        };

        let dict_data = match data {
            DataBlock::Dictionary(dict_data) => dict_data,
            DataBlock::AllNull(all_null) => {
                // In 2.1 this won't happen, kind of annoying to materialize a bunch of nulls
                let indices = UInt8Array::from(vec![0; all_null.num_values as usize]);
                let indices = arrow_cast::cast(&indices, key_type.as_ref()).unwrap();
                let indices = indices.into_data();
                let values = new_null_array(value_type, 1);
                DictionaryDataBlock {
                    indices: FixedWidthDataBlock {
                        bits_per_value: key_type.byte_width() as u64 * 8,
                        data: LanceBuffer::from(indices.buffers()[0].clone()),
                        num_values: all_null.num_values,
                        block_info: BlockInfo::new(),
                    },
                    dictionary: Box::new(DataBlock::from_array(values)),
                }
            }
            _ => panic!("Expected dictionary data"),
        };
        let num_dictionary_items = dict_data.dictionary.num_values() as u32;

        let encoded_indices = self.indices_encoder.encode(
            DataBlock::FixedWidth(dict_data.indices),
            key_type,
            buffer_index,
        )?;
        let encoded_items =
            self.items_encoder
                .encode(*dict_data.dictionary, value_type, buffer_index)?;

        let encoded = DataBlock::Dictionary(DictionaryDataBlock {
            dictionary: Box::new(encoded_items.data),
            indices: encoded_indices.data.as_fixed_width().unwrap(),
        });

        let encoding = ProtobufUtils::dict_encoding(
            encoded_indices.encoding,
            encoded_items.encoding,
            num_dictionary_items,
        );

        Ok(EncodedArray {
            data: encoded,
            encoding,
        })
    }
}

#[derive(Debug)]
pub struct DictionaryEncoder {
    indices_encoder: Box<dyn ArrayEncoder>,
    items_encoder: Box<dyn ArrayEncoder>,
}

impl DictionaryEncoder {
    pub fn new(
        indices_encoder: Box<dyn ArrayEncoder>,
        items_encoder: Box<dyn ArrayEncoder>,
    ) -> Self {
        Self {
            indices_encoder,
            items_encoder,
        }
    }
}

fn encode_dict_indices_and_items(string_array: &StringArray) -> (ArrayRef, ArrayRef) {
    let mut arr_hashmap: HashMap<&str, u8> = HashMap::new();
    // We start with a dict index of 1 because the value 0 is reserved for nulls
    // The dict indices are adjusted by subtracting 1 later during decode
    let mut curr_dict_index = 1;
    let total_capacity = string_array.len();

    let mut dict_indices = Vec::with_capacity(total_capacity);
    let mut dict_builder = StringBuilder::new();

    for i in 0..string_array.len() {
        if !string_array.is_valid(i) {
            // null value
            dict_indices.push(0);
            continue;
        }

        let st = string_array.value(i);

        let hashmap_entry = *arr_hashmap.entry(st).or_insert(curr_dict_index);
        dict_indices.push(hashmap_entry);

        // if item didn't exist in the hashmap, add it to the dictionary
        // and increment the dictionary index
        if hashmap_entry == curr_dict_index {
            dict_builder.append_value(st);
            curr_dict_index += 1;
        }
    }

    let array_dict_indices = Arc::new(UInt8Array::from(dict_indices)) as ArrayRef;

    // If there is an empty dictionary:
    // Either there is an array of nulls or an empty array altogether
    // In this case create the dictionary with a single null element
    // Because decoding [] is not currently supported by the binary decoder
    if dict_builder.is_empty() {
        dict_builder.append_option(Option::<&str>::None);
    }

    let dict_elements = dict_builder.finish();
    let array_dict_elements = arrow_cast::cast(&dict_elements, &DataType::Utf8).unwrap();

    (array_dict_indices, array_dict_elements)
}

impl ArrayEncoder for DictionaryEncoder {
    fn encode(
        &self,
        data: DataBlock,
        data_type: &DataType,
        buffer_index: &mut u32,
    ) -> Result<EncodedArray> {
        if !matches!(data_type, DataType::Utf8) {
            return Err(Error::invalid_input_source(
                format!(
                    "DictionaryEncoder only supports string arrays but got {}",
                    data_type
                )
                .into(),
            ));
        }
        // We only support string arrays for now
        let str_data = make_array(data.into_arrow(DataType::Utf8, false)?);

        let (index_array, items_array) = encode_dict_indices_and_items(str_data.as_string());
        let dict_size = items_array.len() as u32;
        let index_data = DataBlock::from(index_array);
        let items_data = DataBlock::from(items_array);

        let encoded_indices =
            self.indices_encoder
                .encode(index_data, &DataType::UInt8, buffer_index)?;

        let encoded_items = self
            .items_encoder
            .encode(items_data, &DataType::Utf8, buffer_index)?;

        let encoded_data = DataBlock::Dictionary(DictionaryDataBlock {
            indices: encoded_indices.data.as_fixed_width().unwrap(),
            dictionary: Box::new(encoded_items.data),
        });

        let encoding = ProtobufUtils::dict_encoding(
            encoded_indices.encoding,
            encoded_items.encoding,
            dict_size,
        );

        Ok(EncodedArray {
            data: encoded_data,
            encoding,
        })
    }
}

#[cfg(test)]
mod tests {

    use arrow_array::{
        ArrayRef, DictionaryArray, StringArray, UInt8Array,
        builder::{LargeStringBuilder, StringBuilder},
    };
    use arrow_schema::{DataType, Field};
    use std::{collections::HashMap, sync::Arc, vec};

    use crate::testing::{TestCases, check_basic_random, check_round_trip_encoding_of_data};

    use super::encode_dict_indices_and_items;

    // These tests cover the case where we opportunistically convert some (or all) pages of
    // a string column into dictionaries (and decode on read)

    #[test]
    fn test_encode_dict_nulls() {
        // Null entries in string arrays should be adjusted
        let string_array = Arc::new(StringArray::from(vec![
            None,
            Some("foo"),
            Some("bar"),
            Some("bar"),
            None,
            Some("foo"),
            None,
            None,
        ]));
        let (dict_indices, dict_items) = encode_dict_indices_and_items(&string_array);

        let expected_indices = Arc::new(UInt8Array::from(vec![0, 1, 2, 2, 0, 1, 0, 0])) as ArrayRef;
        let expected_items = Arc::new(StringArray::from(vec!["foo", "bar"])) as ArrayRef;
        assert_eq!(&dict_indices, &expected_indices);
        assert_eq!(&dict_items, &expected_items);
    }

    #[test_log::test(tokio::test)]
    async fn test_utf8() {
        let field = Field::new("", DataType::Utf8, false);
        check_basic_random(field).await;
    }

    #[test_log::test(tokio::test)]
    async fn test_binary() {
        let field = Field::new("", DataType::Binary, false);
        check_basic_random(field).await;
    }

    #[test_log::test(tokio::test)]
    async fn test_large_binary() {
        let field = Field::new("", DataType::LargeBinary, true);
        check_basic_random(field).await;
    }

    #[test_log::test(tokio::test)]
    async fn test_large_utf8() {
        let field = Field::new("", DataType::LargeUtf8, true);
        check_basic_random(field).await;
    }

    #[test_log::test(tokio::test)]
    async fn test_simple_utf8() {
        let string_array = StringArray::from(vec![Some("abc"), Some("de"), None, Some("fgh")]);

        let test_cases = TestCases::default()
            .with_range(0..2)
            .with_range(0..3)
            .with_range(1..3)
            .with_indices(vec![1, 3]);
        check_round_trip_encoding_of_data(
            vec![Arc::new(string_array)],
            &test_cases,
            HashMap::new(),
        )
        .await;
    }

    #[test_log::test(tokio::test)]
    async fn test_sliced_utf8() {
        let string_array = StringArray::from(vec![Some("abc"), Some("de"), None, Some("fgh")]);
        let string_array = string_array.slice(1, 3);

        let test_cases = TestCases::default()
            .with_range(0..1)
            .with_range(0..2)
            .with_range(1..2);
        check_round_trip_encoding_of_data(
            vec![Arc::new(string_array)],
            &test_cases,
            HashMap::new(),
        )
        .await;
    }

    #[test_log::test(tokio::test)]
    async fn test_empty_strings() {
        // Scenario 1: Some strings are empty

        let values = [Some("abc"), Some(""), None];
        // Test empty list at beginning, middle, and end
        for order in [[0, 1, 2], [1, 0, 2], [2, 0, 1]] {
            let mut string_builder = StringBuilder::new();
            for idx in order {
                string_builder.append_option(values[idx]);
            }
            let string_array = Arc::new(string_builder.finish());
            let test_cases = TestCases::default()
                .with_indices(vec![1])
                .with_indices(vec![0])
                .with_indices(vec![2]);
            check_round_trip_encoding_of_data(
                vec![string_array.clone()],
                &test_cases,
                HashMap::new(),
            )
            .await;
            let test_cases = test_cases.with_batch_size(1);
            check_round_trip_encoding_of_data(vec![string_array], &test_cases, HashMap::new())
                .await;
        }

        // Scenario 2: All strings are empty

        // When encoding an array of empty strings there are no bytes to encode
        // which is strange and we want to ensure we handle it
        let string_array = Arc::new(StringArray::from(vec![Some(""), None, Some("")]));

        let test_cases = TestCases::default().with_range(0..2).with_indices(vec![1]);
        check_round_trip_encoding_of_data(vec![string_array.clone()], &test_cases, HashMap::new())
            .await;
        let test_cases = test_cases.with_batch_size(1);
        check_round_trip_encoding_of_data(vec![string_array], &test_cases, HashMap::new()).await;
    }

    #[test_log::test(tokio::test)]
    #[ignore] // This test is quite slow in debug mode
    async fn test_jumbo_string() {
        // This is an overflow test.  We have a list of lists where each list
        // has 1Mi items.  We encode 5000 of these lists and so we have over 4Gi in the
        // offsets range
        let mut string_builder = LargeStringBuilder::new();
        // a 1 MiB string
        let giant_string = String::from_iter((0..(1024 * 1024)).map(|_| '0'));
        for _ in 0..5000 {
            string_builder.append_option(Some(&giant_string));
        }
        let giant_array = Arc::new(string_builder.finish()) as ArrayRef;
        let arrs = vec![giant_array];

        // // We can't validate because our validation relies on concatenating all input arrays
        let test_cases = TestCases::default().without_validation();
        check_round_trip_encoding_of_data(arrs, &test_cases, HashMap::new()).await;
    }

    // These tests cover the case where the input is already dictionary encoded

    #[test_log::test(tokio::test)]
    async fn test_random_dictionary_input() {
        let dict_field = Field::new(
            "",
            DataType::Dictionary(Box::new(DataType::UInt16), Box::new(DataType::Utf8)),
            false,
        );
        check_basic_random(dict_field).await;
    }

    #[test_log::test(tokio::test)]
    async fn test_simple_already_dictionary() {
        let values = StringArray::from_iter_values(["a", "bb", "ccc"]);
        let indices = UInt8Array::from(vec![0, 1, 2, 0, 1, 2, 0, 1, 2]);
        let dict_array = DictionaryArray::new(indices, Arc::new(values));

        let test_cases = TestCases::default()
            .with_range(0..2)
            .with_range(1..3)
            .with_range(2..4)
            .with_indices(vec![1])
            .with_indices(vec![2]);
        check_round_trip_encoding_of_data(vec![Arc::new(dict_array)], &test_cases, HashMap::new())
            .await;
    }
}
