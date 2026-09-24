// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::sync::Arc;

use arrow_schema::{DataType, Fields};
use bytes::Bytes;
use bytes::BytesMut;
use futures::{FutureExt, future::BoxFuture};
use lance_arrow::DataTypeExt;
use lance_core::{Error, Result};

use crate::data::BlockInfo;
use crate::data::FixedSizeListBlock;
use crate::format::ProtobufUtils;
use crate::{
    EncodingsIo,
    buffer::LanceBuffer,
    data::{DataBlock, FixedWidthDataBlock, StructDataBlock},
    decoder::{PageScheduler, PrimitivePageDecoder},
    encoder::{ArrayEncoder, EncodedArray},
};

#[derive(Debug)]
pub struct PackedStructPageScheduler {
    // We don't actually need these schedulers right now since we decode all the field bytes directly
    // But they can be useful if we actually need to use the decoders for the inner fields later
    // e.g. once bitpacking is added
    _inner_schedulers: Vec<Box<dyn PageScheduler>>,
    fields: Fields,
    buffer_offset: u64,
}

impl PackedStructPageScheduler {
    pub fn new(
        _inner_schedulers: Vec<Box<dyn PageScheduler>>,
        struct_datatype: DataType,
        buffer_offset: u64,
    ) -> Self {
        let DataType::Struct(fields) = struct_datatype else {
            panic!("Struct datatype expected");
        };
        Self {
            _inner_schedulers,
            fields,
            buffer_offset,
        }
    }
}

impl PageScheduler for PackedStructPageScheduler {
    fn schedule_ranges(
        &self,
        ranges: &[std::ops::Range<u64>],
        scheduler: &Arc<dyn EncodingsIo>,
        top_level_row: u64,
    ) -> BoxFuture<'static, Result<Box<dyn PrimitivePageDecoder>>> {
        let mut total_bytes_per_row: u64 = 0;

        for field in &self.fields {
            let bytes_per_field = field.data_type().byte_width() as u64;
            total_bytes_per_row += bytes_per_field;
        }

        // Parts of the arrays in a page may be encoded in different encoding tasks
        // In that case decoding two different sets of rows can result in the same ranges parameter being passed in
        // e.g. we may get ranges[0..2] and ranges[0..2] to decode 4 rows through 2 tasks
        // So to get the correct byte ranges we need to know the position of the buffer in the page (i.e. the buffer offset)
        // This is computed directly from the buffer stored in the protobuf
        let byte_ranges = ranges
            .iter()
            .map(|range| {
                let start = self.buffer_offset + (range.start * total_bytes_per_row);
                let end = self.buffer_offset + (range.end * total_bytes_per_row);
                start..end
            })
            .collect::<Vec<_>>();

        // Directly creates a future to decode the bytes
        let bytes = scheduler.submit_request(byte_ranges, top_level_row);

        let copy_struct_fields = self.fields.clone();

        tokio::spawn(async move {
            let bytes = bytes.await?;

            let mut combined_bytes = BytesMut::default();
            for byte_slice in bytes {
                combined_bytes.extend_from_slice(&byte_slice);
            }

            Ok(Box::new(PackedStructPageDecoder {
                data: combined_bytes.freeze(),
                fields: copy_struct_fields,
                total_bytes_per_row: total_bytes_per_row as usize,
            }) as Box<dyn PrimitivePageDecoder>)
        })
        .map(|join_handle| join_handle.unwrap())
        .boxed()
    }
}

struct PackedStructPageDecoder {
    data: Bytes,
    fields: Fields,
    total_bytes_per_row: usize,
}

impl PrimitivePageDecoder for PackedStructPageDecoder {
    fn decode(&self, rows_to_skip: u64, num_rows: u64) -> Result<DataBlock> {
        // Decoding workflow:
        // rows 0-2: {x: [1, 2, 3], y: [4, 5, 6], z: [7, 8, 9]}
        // rows 3-5: {x: [10, 11, 12], y: [13, 14, 15], z: [16, 17, 18]}
        // packed encoding: [
        // [1, 4, 7, 2, 5, 8, 3, 6, 9],
        // [10, 13, 16, 11, 14, 17, 12, 15, 18]
        // ]
        // suppose bytes_per_field=1, 4, 8 for fields x, y, and z, respectively.
        // Then total_bytes_per_row = 13
        // Suppose rows_to_skip=1 and num_rows=2. Then we will slice bytes 13 to 39.
        // Now we have [2, 5, 8, 3, 6, 9]
        // We rearrange this to get [BytesMut(2, 3), BytesMut(5, 6), BytesMut(8, 9)] as a Vec<BytesMut>
        // This is used to reconstruct the struct array later

        let bytes_to_skip = (rows_to_skip as usize) * self.total_bytes_per_row;

        let mut children = Vec::with_capacity(self.fields.len());

        let mut start_index = 0;

        for field in &self.fields {
            let bytes_per_field = field.data_type().byte_width();
            let mut field_bytes = Vec::with_capacity(bytes_per_field * num_rows as usize);

            let mut byte_index = start_index;

            for _ in 0..num_rows {
                let start = bytes_to_skip + byte_index;
                field_bytes.extend_from_slice(&self.data[start..(start + bytes_per_field)]);
                byte_index += self.total_bytes_per_row;
            }

            start_index += bytes_per_field;
            let child_block = FixedWidthDataBlock {
                data: LanceBuffer::from(field_bytes),
                bits_per_value: bytes_per_field as u64 * 8,
                num_values: num_rows,
                block_info: BlockInfo::new(),
            };
            let child_block = FixedSizeListBlock::from_flat(child_block, field.data_type());
            children.push(child_block);
        }
        Ok(DataBlock::Struct(StructDataBlock {
            children,
            block_info: BlockInfo::default(),
            validity: None,
        }))
    }
}

#[derive(Debug)]
pub struct PackedStructEncoder {
    inner_encoders: Vec<Box<dyn ArrayEncoder>>,
}

impl PackedStructEncoder {
    pub fn new(inner_encoders: Vec<Box<dyn ArrayEncoder>>) -> Self {
        Self { inner_encoders }
    }
}

impl ArrayEncoder for PackedStructEncoder {
    fn encode(
        &self,
        data: DataBlock,
        data_type: &DataType,
        buffer_index: &mut u32,
    ) -> Result<EncodedArray> {
        let struct_data = data.as_struct().unwrap();

        let DataType::Struct(child_types) = data_type else {
            panic!("Struct datatype expected");
        };

        // Encode individual fields
        let mut encoded_fields = Vec::with_capacity(struct_data.children.len());
        for ((child, encoder), child_type) in struct_data
            .children
            .into_iter()
            .zip(&self.inner_encoders)
            .zip(child_types)
        {
            encoded_fields.push(encoder.encode(child, child_type.data_type(), &mut 0)?);
        }

        let (encoded_data_vec, child_encodings): (Vec<_>, Vec<_>) = encoded_fields
            .into_iter()
            .map(|field| (field.data, field.encoding))
            .unzip();

        // Zip together encoded data
        //
        // We can currently encode both FixedWidth and FixedSizeList.  In order
        // to encode the latter we "flatten" it converting a FixedSizeList into
        // a FixedWidth with very wide items.
        let fixed_fields = encoded_data_vec
            .into_iter()
            .map(|child| match child {
                DataBlock::FixedWidth(fixed) => Ok(fixed),
                DataBlock::FixedSizeList(fixed_size_list) => {
                    let flattened = fixed_size_list.try_into_flat().ok_or_else(|| {
                        Error::invalid_input(
                            "Packed struct encoder cannot pack nullable fixed-width data blocks",
                        )
                    })?;
                    Ok(flattened)
                }
                _ => Err(Error::invalid_input(
                    "Packed struct encoder currently only implemented for fixed-width data blocks",
                )),
            })
            .collect::<Result<Vec<_>>>()?;
        let total_bits_per_value = fixed_fields.iter().map(|f| f.bits_per_value).sum::<u64>();

        let num_values = fixed_fields[0].num_values;
        debug_assert!(
            fixed_fields
                .iter()
                .all(|field| field.num_values == num_values)
        );

        let zipped_input = fixed_fields
            .into_iter()
            .map(|field| (field.data, field.bits_per_value))
            .collect::<Vec<_>>();
        let zipped = LanceBuffer::zip_into_one(zipped_input, num_values)?;

        // Create encoding protobuf
        let index = *buffer_index;
        *buffer_index += 1;

        let packed_data = DataBlock::FixedWidth(FixedWidthDataBlock {
            data: zipped,
            bits_per_value: total_bits_per_value,
            num_values,
            block_info: BlockInfo::new(),
        });

        let encoding = ProtobufUtils::packed_struct(child_encodings, index);

        Ok(EncodedArray {
            data: packed_data,
            encoding,
        })
    }
}

#[cfg(test)]
mod tests {

    use arrow_array::{ArrayRef, Int32Array, StructArray, UInt8Array, UInt64Array};
    use arrow_schema::{DataType, Field, Fields};
    use std::{collections::HashMap, sync::Arc, vec};

    use crate::testing::{
        TestCases, TestEncoding, check_basic_random_case, check_round_trip_encoding_of_data,
    };

    #[rstest::rstest]
    #[test_log::test(tokio::test)]
    async fn test_random_packed_struct(
        #[values(
            TestEncoding::Array,
            TestEncoding::StructuralU16,
            TestEncoding::StructuralU32,
            TestEncoding::StructuralSparse
        )]
        encoding: TestEncoding,
        #[values(4096, 1024 * 1024)] page_size: u64,
        #[values(false, true)] use_slicing: bool,
    ) {
        let data_type = DataType::Struct(Fields::from(vec![
            Field::new("a", DataType::UInt64, false),
            Field::new("b", DataType::UInt32, false),
        ]));
        let mut metadata = HashMap::new();
        metadata.insert("packed".to_string(), "true".to_string());

        let field = Field::new("", data_type, false).with_metadata(metadata);

        check_basic_random_case(field, encoding, page_size, use_slicing).await;
    }

    #[test_log::test(tokio::test)]
    async fn test_specific_packed_struct() {
        let array1 = Arc::new(UInt64Array::from(vec![1, 2, 3, 4]));
        let array2 = Arc::new(Int32Array::from(vec![5, 6, 7, 8]));
        let array3 = Arc::new(UInt8Array::from(vec![9, 10, 11, 12]));

        let struct_array1 = Arc::new(StructArray::from(vec![
            (
                Arc::new(Field::new("x", DataType::UInt64, false)),
                array1.clone() as ArrayRef,
            ),
            (
                Arc::new(Field::new("y", DataType::Int32, false)),
                array2.clone() as ArrayRef,
            ),
            (
                Arc::new(Field::new("z", DataType::UInt8, false)),
                array3.clone() as ArrayRef,
            ),
        ]));

        let array4 = Arc::new(UInt64Array::from(vec![13, 14, 15, 16]));
        let array5 = Arc::new(Int32Array::from(vec![17, 18, 19, 20]));
        let array6 = Arc::new(UInt8Array::from(vec![21, 22, 23, 24]));

        let struct_array2 = Arc::new(StructArray::from(vec![
            (
                Arc::new(Field::new("x", DataType::UInt64, false)),
                array4.clone() as ArrayRef,
            ),
            (
                Arc::new(Field::new("y", DataType::Int32, false)),
                array5.clone() as ArrayRef,
            ),
            (
                Arc::new(Field::new("z", DataType::UInt8, false)),
                array6.clone() as ArrayRef,
            ),
        ]));

        let test_cases = TestCases::default()
            .with_range(0..2)
            .with_range(0..6)
            .with_range(1..4)
            .with_indices(vec![1, 3, 7]);

        let mut metadata = HashMap::new();
        metadata.insert("packed".to_string(), "true".to_string());

        check_round_trip_encoding_of_data(
            vec![struct_array1, struct_array2],
            &test_cases,
            metadata,
        )
        .await;
    }
}
