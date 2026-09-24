// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use core::panic;
use std::sync::Arc;

use arrow_array::ArrayRef;
use arrow_array::cast::AsArray;
use arrow_array::types::UInt64Type;
use arrow_buffer::{BooleanBuffer, BooleanBufferBuilder, NullBuffer, ScalarBuffer, bit_util};
use futures::TryFutureExt;

use futures::{FutureExt, future::BoxFuture};

use crate::array_encoding::logical::primitive::PrimitiveFieldDecoder;
use crate::buffer::LanceBuffer;
use crate::data::{
    BlockInfo, DataBlock, FixedWidthDataBlock, NullableDataBlock, VariableWidthBlock,
};
use crate::decoder::LogicalPageDecoder;
use crate::encoder::{ArrayEncoder, EncodedArray};
use crate::encodings::physical::block::{
    BufferCompressor, CompressionConfig, GeneralBufferCompressor,
};
use crate::format::ProtobufUtils;
use crate::{
    EncodingsIo,
    decoder::{PageScheduler, PrimitivePageDecoder},
};

use arrow_array::{PrimitiveArray, UInt64Array};
use arrow_schema::DataType;
use lance_core::{Error, Result};

fn oversized_binary_batch_error(num_rows: u64, num_bytes: u64) -> Error {
    Error::not_supported(format!(
        "Could not create array with more than 2GiB of string/binary data in a single batch \
         ({} rows would require {} bytes). Please reduce the batch_size, set \
         LANCE_DEFAULT_BATCH_SIZE to a smaller value, or convert the column to \
         large_string/large_binary.",
        num_rows, num_bytes
    ))
}

fn oversized_large_binary_batch_error(num_rows: u64, num_bytes: u64) -> Error {
    Error::not_supported(format!(
        "Could not create large_string/large_binary array in a single batch because {} rows \
         would require {} bytes, which exceeds i64::MAX. Please reduce the batch_size or set \
         LANCE_DEFAULT_BATCH_SIZE to a smaller value.",
        num_rows, num_bytes
    ))
}

struct IndicesNormalizer {
    indices: Vec<u64>,
    validity: BooleanBufferBuilder,
    null_adjustment: u64,
}

impl IndicesNormalizer {
    fn new(num_rows: u64, null_adjustment: u64) -> Self {
        let mut indices = Vec::with_capacity(num_rows as usize);
        indices.push(0);
        Self {
            indices,
            validity: BooleanBufferBuilder::new(num_rows as usize),
            null_adjustment,
        }
    }

    fn normalize(&self, val: u64) -> (bool, u64) {
        if val >= self.null_adjustment {
            (false, val - self.null_adjustment)
        } else {
            (true, val)
        }
    }

    fn extend(&mut self, new_indices: &PrimitiveArray<UInt64Type>, is_start: bool) -> Result<()> {
        let mut last = *self.indices.last().unwrap();
        if is_start {
            let (is_valid, val) = self.normalize(new_indices.value(0));
            self.indices.push(val);
            self.validity.append(is_valid);
            last += val;
        }
        let mut prev = self.normalize(*new_indices.values().first().unwrap()).1;
        for (i, w) in new_indices.values().windows(2).enumerate() {
            let (is_valid, val) = self.normalize(w[1]);
            let next = match val.checked_sub(prev) {
                Some(delta) => delta + last,
                None => {
                    return Err(lance_core::Error::invalid_input(format!(
                        "corrupt binary page: normalized offset {} is less than previous offset {} \
                             at index {}, null_adjustment={}, raw values were [{}, {}]. \
                             This usually indicates the file data has been corrupted.",
                        val, prev, i, self.null_adjustment, w[0], w[1]
                    )));
                }
            };
            self.indices.push(next);
            self.validity.append(is_valid);
            prev = val;
            last = next;
        }
        Ok(())
    }

    fn into_parts(mut self) -> (Vec<u64>, BooleanBuffer) {
        (self.indices, self.validity.finish())
    }
}

#[derive(Debug)]
pub struct BinaryPageScheduler {
    indices_scheduler: Arc<dyn PageScheduler>,
    bytes_scheduler: Arc<dyn PageScheduler>,
    offsets_type: DataType,
    null_adjustment: u64,
}

impl BinaryPageScheduler {
    pub fn new(
        indices_scheduler: Arc<dyn PageScheduler>,
        bytes_scheduler: Arc<dyn PageScheduler>,
        offsets_type: DataType,
        null_adjustment: u64,
    ) -> Self {
        Self {
            indices_scheduler,
            bytes_scheduler,
            offsets_type,
            null_adjustment,
        }
    }

    fn decode_indices(decoder: Arc<dyn PrimitivePageDecoder>, num_rows: u64) -> Result<ArrayRef> {
        let mut primitive_wrapper =
            PrimitiveFieldDecoder::new_from_data(decoder, DataType::UInt64, num_rows, false);
        let drained_task = primitive_wrapper.drain(num_rows)?;
        let indices_decode_task = drained_task.task;
        indices_decode_task.decode().map(|(arr, _)| arr)
    }
}

struct IndirectData {
    decoded_indices: UInt64Array,
    offsets_type: DataType,
    validity: BooleanBuffer,
    bytes_decoder_fut: BoxFuture<'static, Result<Box<dyn PrimitivePageDecoder>>>,
}

impl PageScheduler for BinaryPageScheduler {
    fn schedule_ranges(
        &self,
        ranges: &[std::ops::Range<u64>],
        scheduler: &Arc<dyn EncodingsIo>,
        top_level_row: u64,
    ) -> BoxFuture<'static, Result<Box<dyn PrimitivePageDecoder>>> {
        // ranges corresponds to row ranges that the user wants to fetch.
        // if user wants row range a..b
        // Case 1: if a != 0, we need indices a-1..b to decode
        // Case 2: if a = 0, we need indices 0..b to decode
        let indices_ranges = ranges
            .iter()
            .map(|range| {
                if range.start != 0 {
                    (range.start - 1)..range.end
                } else {
                    0..range.end
                }
            })
            .collect::<Vec<std::ops::Range<u64>>>();

        // We schedule all the indices for decoding together
        // This is more efficient compared to scheduling them one by one (reduces speed significantly for random access)
        let indices_page_decoder =
            self.indices_scheduler
                .schedule_ranges(&indices_ranges, scheduler, top_level_row);

        let num_rows = ranges.iter().map(|r| r.end - r.start).sum::<u64>();
        let indices_num_rows = indices_ranges.iter().map(|r| r.end - r.start).sum::<u64>();

        let ranges = ranges.to_vec();
        let copy_scheduler = scheduler.clone();
        let copy_bytes_scheduler = self.bytes_scheduler.clone();
        let null_adjustment = self.null_adjustment;
        let offsets_type = self.offsets_type.clone();

        tokio::spawn(async move {
            // For the following data:
            // "abcd", "hello", "abcd", "apple", "hello", "abcd"
            //   4,        9,     13,      18,      23,     27
            // e.g. want to scan rows 0, 2, 4
            // i.e. offsets are 4 | 9, 13 | 18, 23
            // Normalization is required for decoding later on
            // Normalize each part: 0, 4 | 0, 4 | 0, 5
            // Remove leading zeros except first one: 0, 4 | 4 | 5
            // Cumulative sum: 0, 4 | 8 | 13
            // These are the normalized offsets stored in decoded_indices
            // Rest of the workflow is continued later in BinaryPageDecoder
            let indices_decoder = Arc::from(indices_page_decoder.await?);
            let indices = Self::decode_indices(indices_decoder, indices_num_rows)?;
            let decoded_indices = indices.as_primitive::<UInt64Type>();

            let mut indices_builder = IndicesNormalizer::new(num_rows, null_adjustment);
            let mut bytes_ranges = Vec::new();
            let mut curr_offset_index = 0;

            for curr_row_range in ranges.iter() {
                let row_start = curr_row_range.start;
                let curr_range_len = (curr_row_range.end - row_start) as usize;

                let curr_indices;

                if row_start == 0 {
                    curr_indices = decoded_indices.slice(0, curr_range_len);
                    curr_offset_index = curr_range_len;
                } else {
                    curr_indices = decoded_indices.slice(curr_offset_index, curr_range_len + 1);
                    curr_offset_index += curr_range_len + 1;
                }

                let first = if row_start == 0 {
                    0
                } else {
                    indices_builder
                        .normalize(*curr_indices.values().first().unwrap())
                        .1
                };
                let last = indices_builder
                    .normalize(*curr_indices.values().last().unwrap())
                    .1;

                if first != last {
                    bytes_ranges.push(first..last);
                }

                indices_builder.extend(&curr_indices, row_start == 0)?;
            }

            let (indices, validity) = indices_builder.into_parts();
            let decoded_indices = UInt64Array::from(indices);

            // In the indirect task we schedule the bytes, but we do not await them.  We don't want to
            // await the bytes until the decoder is ready for them so that we don't release the backpressure
            // too early
            let bytes_decoder_fut =
                copy_bytes_scheduler.schedule_ranges(&bytes_ranges, &copy_scheduler, top_level_row);

            Ok(IndirectData {
                decoded_indices,
                validity,
                offsets_type,
                bytes_decoder_fut,
            })
        })
        // Propagate join panic
        .map(|join_handle| join_handle.unwrap())
        .and_then(|indirect_data| {
            async move {
                // Later, this will be called once the decoder actually starts polling.  At that point
                // we await the bytes (releasing the backpressure)
                let bytes_decoder = indirect_data.bytes_decoder_fut.await?;
                Ok(Box::new(BinaryPageDecoder {
                    decoded_indices: indirect_data.decoded_indices,
                    offsets_type: indirect_data.offsets_type,
                    validity: indirect_data.validity,
                    bytes_decoder,
                }) as Box<dyn PrimitivePageDecoder>)
            }
        })
        .boxed()
    }
}

struct BinaryPageDecoder {
    decoded_indices: UInt64Array,
    offsets_type: DataType,
    validity: BooleanBuffer,
    bytes_decoder: Box<dyn PrimitivePageDecoder>,
}

impl PrimitivePageDecoder for BinaryPageDecoder {
    fn variable_width_bytes(&self, rows_to_skip: u64, num_rows: u64) -> Result<Option<u64>> {
        if num_rows == 0 {
            return Ok(Some(0));
        }
        // `decoded_indices` holds one cumulative byte offset per row plus a final
        // sentinel, so the value bytes for the requested rows are the difference
        // between the bounding entries.  Only value bytes count: Arrow's i32
        // limit constrains the final offset value, not the offset buffer size.
        let value_bytes = self
            .decoded_indices
            .value((rows_to_skip + num_rows) as usize)
            - self.decoded_indices.value(rows_to_skip as usize);
        Ok(Some(value_bytes))
    }

    // Continuing the example from BinaryPageScheduler
    // Suppose batch_size = 2. Then first, rows_to_skip=0, num_rows=2
    // Need to scan 2 rows
    // First row will be 4-0=4 bytes, second also 8-4=4 bytes.
    // Allocate 8 bytes capacity.
    // Next rows_to_skip=2, num_rows=1
    // Skip 8 bytes. Allocate 5 bytes capacity.
    //
    // The normalized offsets are [0, 4, 8, 13]
    // We only need [8, 13] to decode in this case.
    // These need to be normalized in order to build the string later
    // So return [0, 5]
    fn decode(&self, rows_to_skip: u64, num_rows: u64) -> Result<DataBlock> {
        // STEP 1: validity buffer
        let target_validity = self
            .validity
            .slice(rows_to_skip as usize, num_rows as usize);
        let has_nulls = target_validity.count_set_bits() < target_validity.len();

        let validity_buffer = if has_nulls {
            let num_validity_bits = arrow_buffer::bit_util::ceil(num_rows as usize, 8);
            let mut validity_buffer = Vec::with_capacity(num_validity_bits);

            if rows_to_skip == 0 {
                validity_buffer.extend_from_slice(target_validity.inner().as_slice());
            } else {
                // Need to copy the buffer because there may be a bit offset in first byte
                let target_validity = BooleanBuffer::from_iter(target_validity.iter());
                validity_buffer.extend_from_slice(target_validity.inner().as_slice());
            }
            Some(validity_buffer)
        } else {
            None
        };

        // STEP 2: offsets buffer
        // Currently we always do a copy here, we need to cast to the appropriate type
        // and we go ahead and normalize so the starting offset is 0 (though we could skip
        // this)
        let bytes_per_offset = match self.offsets_type {
            DataType::Int32 => 4,
            DataType::Int64 => 8,
            _ => panic!("Unsupported offsets type"),
        };

        let target_offsets = self
            .decoded_indices
            .slice(rows_to_skip as usize, (num_rows + 1) as usize);

        // Normalize and cast (TODO: could fuse these into one pass for micro-optimization)
        let target_vec = target_offsets.values();
        let start = target_vec[0];
        let end = *target_vec.last().unwrap();
        let num_bytes = end - start;
        let offsets_buffer = match bytes_per_offset {
            4 => {
                if num_bytes > i32::MAX as u64 {
                    return Err(oversized_binary_batch_error(num_rows, num_bytes));
                }
                ScalarBuffer::from_iter(target_vec.iter().map(|&offset| (offset - start) as i32))
                    .into_inner()
            }
            8 => {
                if num_bytes > i64::MAX as u64 {
                    return Err(oversized_large_binary_batch_error(num_rows, num_bytes));
                }
                ScalarBuffer::from_iter(target_vec.iter().map(|&offset| (offset - start) as i64))
                    .into_inner()
            }
            _ => panic!("Unsupported offsets type"),
        };

        let bytes_to_skip = self.decoded_indices.value(rows_to_skip as usize);
        let num_bytes = self
            .decoded_indices
            .value((rows_to_skip + num_rows) as usize)
            - bytes_to_skip;

        let bytes = self.bytes_decoder.decode(bytes_to_skip, num_bytes)?;
        let bytes = bytes.as_fixed_width().unwrap();
        debug_assert_eq!(bytes.bits_per_value, 8);

        let string_data = DataBlock::VariableWidth(VariableWidthBlock {
            bits_per_offset: bytes_per_offset * 8,
            data: bytes.data,
            num_values: num_rows,
            offsets: LanceBuffer::from(offsets_buffer),
            block_info: BlockInfo::new(),
        });
        if let Some(validity) = validity_buffer {
            Ok(DataBlock::Nullable(NullableDataBlock {
                data: Box::new(string_data),
                nulls: LanceBuffer::from(validity),
                block_info: BlockInfo::new(),
            }))
        } else {
            Ok(string_data)
        }
    }
}

#[derive(Debug)]
pub struct BinaryEncoder {
    indices_encoder: Box<dyn ArrayEncoder>,
    compression_config: Option<CompressionConfig>,
    buffer_compressor: Option<Box<dyn BufferCompressor>>,
}

impl BinaryEncoder {
    pub fn try_new(
        indices_encoder: Box<dyn ArrayEncoder>,
        compression_config: Option<CompressionConfig>,
    ) -> Result<Self> {
        let buffer_compressor = compression_config
            .map(GeneralBufferCompressor::get_compressor)
            .transpose()?;
        Ok(Self {
            indices_encoder,
            compression_config,
            buffer_compressor,
        })
    }

    // In 2.1 we will materialize nulls higher up (in the primitive encoder).  Unfortunately,
    // in 2.0 we actually need to write the offsets.
    fn all_null_variable_width(data_type: &DataType, num_values: u64) -> VariableWidthBlock {
        if matches!(data_type, DataType::Binary | DataType::Utf8) {
            VariableWidthBlock {
                bits_per_offset: 32,
                data: LanceBuffer::empty(),
                num_values,
                offsets: LanceBuffer::reinterpret_vec(vec![0_u32; num_values as usize + 1]),
                block_info: BlockInfo::new(),
            }
        } else {
            VariableWidthBlock {
                bits_per_offset: 64,
                data: LanceBuffer::empty(),
                num_values,
                offsets: LanceBuffer::reinterpret_vec(vec![0_u64; num_values as usize + 1]),
                block_info: BlockInfo::new(),
            }
        }
    }
}

// Creates indices arrays from string arrays
// Strings are a vector of arrays corresponding to each record batch
// Zero offset is removed from the start of the offsets array
// The indices array is computed across all arrays in the vector
fn get_indices_from_string_arrays(
    offsets: LanceBuffer,
    bits_per_offset: u8,
    nulls: Option<LanceBuffer>,
    num_rows: usize,
) -> (DataBlock, u64) {
    let mut indices = Vec::with_capacity(num_rows);
    let mut last_offset = 0_u64;
    if bits_per_offset == 32 {
        let offsets = offsets.borrow_to_typed_slice::<i32>();
        indices.extend(offsets.as_ref().windows(2).map(|w| {
            let strlen = (w[1] - w[0]) as u64;
            last_offset += strlen;
            last_offset
        }));
    } else if bits_per_offset == 64 {
        let offsets = offsets.borrow_to_typed_slice::<i64>();
        indices.extend(offsets.as_ref().windows(2).map(|w| {
            let strlen = (w[1] - w[0]) as u64;
            last_offset += strlen;
            last_offset
        }));
    }

    if indices.is_empty() {
        return (
            DataBlock::FixedWidth(FixedWidthDataBlock {
                bits_per_value: 64,
                data: LanceBuffer::empty(),
                num_values: 0,
                block_info: BlockInfo::new(),
            }),
            0,
        );
    }

    let last_offset = *indices.last().expect("Indices array is empty");
    // 8 exabytes in a single array seems unlikely but...just in case
    assert!(
        last_offset < u64::MAX / 2,
        "Indices array with strings up to 2^63 is too large for this encoding"
    );
    let null_adjustment: u64 = *indices.last().expect("Indices array is empty") + 1;

    if let Some(nulls) = nulls {
        let nulls = NullBuffer::new(BooleanBuffer::new(nulls.into_buffer(), 0, num_rows));
        indices
            .iter_mut()
            .zip(nulls.iter())
            .for_each(|(index, is_valid)| {
                if !is_valid {
                    *index += null_adjustment;
                }
            });
    }
    let indices = DataBlock::FixedWidth(FixedWidthDataBlock {
        bits_per_value: 64,
        data: LanceBuffer::reinterpret_vec(indices),
        num_values: num_rows as u64,
        block_info: BlockInfo::new(),
    });
    (indices, null_adjustment)
}

impl ArrayEncoder for BinaryEncoder {
    fn encode(
        &self,
        data: DataBlock,
        data_type: &DataType,
        buffer_index: &mut u32,
    ) -> Result<EncodedArray> {
        let (mut data, nulls) = match data {
            DataBlock::Nullable(nullable) => {
                let data = nullable.data.as_variable_width().unwrap();
                (data, Some(nullable.nulls))
            }
            DataBlock::VariableWidth(variable) => (variable, None),
            DataBlock::AllNull(all_null) => {
                let data = Self::all_null_variable_width(data_type, all_null.num_values);
                let validity =
                    LanceBuffer::all_unset(bit_util::ceil(all_null.num_values as usize, 8));
                (data, Some(validity))
            }
            _ => panic!("Expected variable width data block but got {}", data.name()),
        };

        let (indices, null_adjustment) = get_indices_from_string_arrays(
            data.offsets,
            data.bits_per_offset,
            nulls,
            data.num_values as usize,
        );
        let encoded_indices =
            self.indices_encoder
                .encode(indices, &DataType::UInt64, buffer_index)?;

        let encoded_indices_data = encoded_indices.data.as_fixed_width().unwrap();

        assert!(encoded_indices_data.bits_per_value <= 64);

        if let Some(buffer_compressor) = &self.buffer_compressor {
            let mut compressed_data = Vec::with_capacity(data.data.len());
            buffer_compressor.compress(&data.data, &mut compressed_data)?;
            data.data = LanceBuffer::from(compressed_data);
        }

        let data = DataBlock::VariableWidth(VariableWidthBlock {
            bits_per_offset: encoded_indices_data.bits_per_value as u8,
            offsets: encoded_indices_data.data,
            data: data.data,
            num_values: data.num_values,
            block_info: BlockInfo::new(),
        });

        let bytes_buffer_index = *buffer_index;
        *buffer_index += 1;

        let bytes_encoding = ProtobufUtils::flat_encoding(
            /*bits_per_value=*/ 8,
            bytes_buffer_index,
            self.compression_config,
        );

        let encoding =
            ProtobufUtils::binary(encoded_indices.encoding, bytes_encoding, null_adjustment);

        Ok(EncodedArray { data, encoding })
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::StringArray;

    use super::*;

    #[derive(Debug)]
    struct EmptyBytesDecoder;

    impl PrimitivePageDecoder for EmptyBytesDecoder {
        fn decode(&self, _rows_to_skip: u64, _num_rows: u64) -> Result<DataBlock> {
            Ok(DataBlock::FixedWidth(FixedWidthDataBlock {
                bits_per_value: 8,
                data: LanceBuffer::empty(),
                num_values: 0,
                block_info: BlockInfo::new(),
            }))
        }
    }

    #[derive(Debug)]
    struct BytesDecoder {
        bytes: Vec<u8>,
    }

    impl PrimitivePageDecoder for BytesDecoder {
        fn decode(&self, rows_to_skip: u64, num_rows: u64) -> Result<DataBlock> {
            let start = rows_to_skip as usize;
            let end = start + num_rows as usize;
            Ok(DataBlock::FixedWidth(FixedWidthDataBlock {
                bits_per_value: 8,
                data: LanceBuffer::from(self.bytes[start..end].to_vec()),
                num_values: num_rows,
                block_info: BlockInfo::new(),
            }))
        }
    }

    #[test]
    fn test_encode_indices_adjusts_nulls() {
        // Null entries in string arrays should be adjusted
        let string_array = Arc::new(StringArray::from(vec![
            None,
            Some("foo"),
            Some("foo"),
            None,
            None,
            None,
        ])) as ArrayRef;
        let string_data = DataBlock::from(string_array).as_nullable().unwrap();
        let nulls = string_data.nulls;
        let string_data = string_data.data.as_variable_width().unwrap();

        let (indices, null_adjustment) = get_indices_from_string_arrays(
            string_data.offsets,
            string_data.bits_per_offset,
            Some(nulls),
            string_data.num_values as usize,
        );

        let indices = indices.as_fixed_width().unwrap();
        assert_eq!(indices.bits_per_value, 64);
        assert_eq!(
            indices.data,
            LanceBuffer::reinterpret_vec(vec![7_u64, 3, 6, 13, 13, 13])
        );
        assert_eq!(null_adjustment, 7);
    }

    #[test]
    fn test_binary_overflow_error_is_actionable() {
        let num_rows = 1;
        let start = 100_u64;
        let end = start + i32::MAX as u64 + 1;
        let decoded_indices = UInt64Array::from(vec![start, end]);
        let decoder = BinaryPageDecoder {
            decoded_indices,
            validity: BooleanBuffer::from_iter([true]),
            offsets_type: DataType::Int32,
            bytes_decoder: Box::new(EmptyBytesDecoder),
        };

        let error = decoder.decode(0, num_rows).unwrap_err();
        assert!(matches!(error, Error::NotSupported { .. }));
        let message = error.to_string();
        assert!(message.contains("more than 2GiB of string/binary data"));
        assert!(message.contains("batch_size"));
        assert!(message.contains("LANCE_DEFAULT_BATCH_SIZE"));
        assert!(message.contains("large_string/large_binary"));
    }

    #[test]
    fn test_large_binary_overflow_error_is_actionable() {
        let num_rows = 1;
        let start = 100_u64;
        let end = start + i64::MAX as u64 + 1;
        let decoded_indices = UInt64Array::from(vec![start, end]);
        let decoder = BinaryPageDecoder {
            decoded_indices,
            validity: BooleanBuffer::from_iter([true]),
            offsets_type: DataType::Int64,
            bytes_decoder: Box::new(EmptyBytesDecoder),
        };

        let error = decoder.decode(0, num_rows).unwrap_err();
        assert!(matches!(error, Error::NotSupported { .. }));
        let message = error.to_string();
        assert!(message.contains("large_string/large_binary"));
        assert!(message.contains("exceeds i64::MAX"));
        assert!(message.contains("batch_size"));
        assert!(message.contains("LANCE_DEFAULT_BATCH_SIZE"));
    }

    #[test]
    fn test_large_binary_decode_success_path() {
        let decoded_indices = UInt64Array::from(vec![100_u64, 102, 105]);
        let mut bytes = vec![0_u8; 100];
        bytes.extend_from_slice(b"abcde");
        let decoder = BinaryPageDecoder {
            decoded_indices,
            validity: BooleanBuffer::from_iter([true, true]),
            offsets_type: DataType::Int64,
            bytes_decoder: Box::new(BytesDecoder { bytes }),
        };

        let data = decoder.decode(0, 2).unwrap();
        let variable = data.as_variable_width().unwrap();
        assert_eq!(variable.bits_per_offset, 64);
        assert_eq!(variable.data.as_ref(), b"abcde");
        assert_eq!(
            variable.offsets.borrow_to_typed_slice::<i64>().as_ref(),
            &[0_i64, 2, 5]
        );
    }

    #[derive(Debug)]
    struct NeverDecodedStub;

    impl PrimitivePageDecoder for NeverDecodedStub {
        fn decode(&self, _rows_to_skip: u64, _num_rows: u64) -> Result<DataBlock> {
            unreachable!("byte accounting must not decode any values")
        }
    }

    /// A real physical binary page over strings of the given byte lengths.
    fn binary_page(value_lens: &[u64]) -> BinaryPageDecoder {
        let mut indices = vec![0u64];
        for len in value_lens {
            indices.push(indices.last().unwrap() + len);
        }
        BinaryPageDecoder {
            decoded_indices: UInt64Array::from(indices),
            offsets_type: DataType::Int32,
            validity: BooleanBuffer::new_set(value_lens.len()),
            bytes_decoder: Box::new(NeverDecodedStub),
        }
    }

    /// Variable-width value bytes of `n` rows starting at `skip` (what the
    /// output array's i32 offsets index into).
    fn expected_bytes(value_lens: &[u64], skip: usize, n: usize) -> u64 {
        value_lens[skip..skip + n].iter().sum::<u64>()
    }

    #[test]
    fn test_unknown_size_pages_split_at_page_boundaries() {
        use crate::array_encoding::logical::primitive::PrimitiveFieldDecoder;
        use crate::array_encoding::logical::r#struct::SimpleStructDecoder;
        use crate::decoder::{DecoderReady, I32_OFFSET_BYTE_BUDGET, LogicalPageDecoder};
        use arrow_schema::{Field as ArrowField, Fields};
        use std::collections::VecDeque;

        /// A variable-width page that cannot report sizes (default
        /// `variable_width_bytes` returns `None`).
        #[derive(Debug)]
        struct UnknownSizeStub;

        impl PrimitivePageDecoder for UnknownSizeStub {
            fn decode(&self, _rows_to_skip: u64, num_rows: u64) -> Result<DataBlock> {
                Ok(DataBlock::VariableWidth(VariableWidthBlock {
                    bits_per_offset: 32,
                    data: LanceBuffer::empty(),
                    offsets: LanceBuffer::reinterpret_vec(vec![0_i32; num_rows as usize + 1]),
                    num_values: num_rows,
                    block_info: BlockInfo::new(),
                }))
            }
        }

        let fields = Fields::from(vec![ArrowField::new("value", DataType::Utf8, false)]);
        let mut root = SimpleStructDecoder::new(fields, 6);
        for _ in 0..2 {
            root.accept_child(DecoderReady {
                decoder: Box::new(PrimitiveFieldDecoder::new_from_data(
                    Arc::new(UnknownSizeStub),
                    DataType::Utf8,
                    3,
                    false,
                )),
                path: VecDeque::from([0]),
            })
            .unwrap();
        }

        // Each page's size is unknown, so pages must not stack in one batch:
        // the batch takes the first page alone and stops at its boundary.
        let limit = root.max_rows_to_drain(6, I32_OFFSET_BYTE_BUDGET).unwrap();
        assert_eq!(limit.rows, 3);
    }

    #[test]
    fn test_physical_binary_page_reports_variable_width_bytes() {
        let page = binary_page(&[4, 4, 5]);
        assert_eq!(
            page.variable_width_bytes(0, 3).unwrap(),
            Some(expected_bytes(&[4, 4, 5], 0, 3))
        );
        assert_eq!(
            page.variable_width_bytes(1, 2).unwrap(),
            Some(expected_bytes(&[4, 4, 5], 1, 2))
        );
        assert_eq!(page.variable_width_bytes(0, 0).unwrap(), Some(0));
    }

    #[test]
    fn test_primitive_field_decoder_truncates_to_byte_budget() {
        use crate::array_encoding::logical::primitive::PrimitiveFieldDecoder;
        use crate::decoder::LogicalPageDecoder;

        let lens = [4u64, 4, 5];
        let decoder = PrimitiveFieldDecoder::new_from_data(
            Arc::new(binary_page(&lens)),
            DataType::Utf8,
            3,
            false,
        );
        let full = expected_bytes(&lens, 0, 3);

        let limit = decoder.max_rows_to_drain(3, full).unwrap();
        assert_eq!((limit.rows, limit.bytes), (3, full));

        // One byte short of the full request: only two rows fit.
        let limit = decoder.max_rows_to_drain(3, full - 1).unwrap();
        assert_eq!((limit.rows, limit.bytes), (2, expected_bytes(&lens, 0, 2)));

        // Nothing fits: zero rows, so the stream root can raise an error.
        let limit = decoder.max_rows_to_drain(3, 0).unwrap();
        assert_eq!(limit.rows, 0);
    }

    #[test]
    fn test_physical_binary_pages_accumulate_budget_across_pages() {
        use crate::array_encoding::logical::primitive::PrimitiveFieldDecoder;
        use crate::array_encoding::logical::r#struct::SimpleStructDecoder;
        use crate::decoder::{DecoderReady, LogicalPageDecoder};
        use arrow_schema::{Field as ArrowField, Fields};
        use std::collections::VecDeque;

        let lens = [4u64, 4, 5];
        let page_bytes = expected_bytes(&lens, 0, 3);
        let fields = Fields::from(vec![ArrowField::new("value", DataType::Utf8, false)]);
        let mut root = SimpleStructDecoder::new(fields, 6);
        for _ in 0..2 {
            root.accept_child(DecoderReady {
                decoder: Box::new(PrimitiveFieldDecoder::new_from_data(
                    Arc::new(binary_page(&lens)),
                    DataType::Utf8,
                    3,
                    false,
                )),
                path: VecDeque::from([0]),
            })
            .unwrap();
        }

        // Both pages fit: the batch spans the page boundary.
        let limit = root.max_rows_to_drain(6, page_bytes * 2).unwrap();
        assert_eq!((limit.rows, limit.bytes), (6, page_bytes * 2));

        // Page 1 plus the first two rows of page 2 fit.
        let second_page_prefix = expected_bytes(&lens, 0, 2);
        let limit = root
            .max_rows_to_drain(6, page_bytes + second_page_prefix)
            .unwrap();
        assert_eq!(
            (limit.rows, limit.bytes),
            (5, page_bytes + second_page_prefix)
        );

        // Budget for page 1 only: the batch stops at the page boundary.
        let limit = root.max_rows_to_drain(6, page_bytes).unwrap();
        assert_eq!((limit.rows, limit.bytes), (3, page_bytes));
    }
}
