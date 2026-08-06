// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Plain encoding
//!
//! Plain encoding works with fixed stride types, i.e., `boolean`, `i8...i64`, `f16...f64`,
//! it stores the array directly in the file. It offers O(1) read access.

use std::ops::Range;
use std::slice::from_raw_parts;
use std::sync::Arc;

use arrow_arith::numeric::sub;
use arrow_array::{
    Array, ArrayRef, BooleanArray, FixedSizeBinaryArray, FixedSizeListArray, UInt8Array,
    UInt32Array, builder::BooleanBuilder, cast::AsArray, make_array, new_empty_array,
};
use arrow_buffer::{Buffer, bit_util};
use arrow_data::{ArrayDataBuilder, BufferSpec, layout};
use arrow_schema::{DataType, Field};
use arrow_select::{concat::concat, take::take};
use async_recursion::async_recursion;
use futures::stream::{self, StreamExt, TryStreamExt};
use lance_arrow::*;
use lance_core::{Error, Result};
use lance_io::{
    ReadBatchParams,
    traits::{Reader, Writer},
};
use tokio::io::AsyncWriteExt;

/// Encoder for plain encoding.
///
pub struct PlainEncoder<'a> {
    writer: &'a mut dyn Writer,
    data_type: &'a DataType,
}

impl<'a> PlainEncoder<'a> {
    pub fn new(writer: &'a mut dyn Writer, data_type: &'a DataType) -> Self {
        PlainEncoder { writer, data_type }
    }

    /// Write an continuous plain-encoded array to the writer.
    pub async fn write(writer: &'a mut dyn Writer, arrays: &[&'a dyn Array]) -> Result<usize> {
        let pos = writer.tell().await?;
        if !arrays.is_empty() {
            let mut encoder = Self::new(writer, arrays[0].data_type());
            encoder.encode(arrays).await?;
        }
        Ok(pos)
    }

    /// Encode an slice of an Array of a batch.
    /// Returns the offset of the metadata
    pub async fn encode(&mut self, arrays: &[&dyn Array]) -> Result<usize> {
        self.encode_internal(arrays, self.data_type).await
    }

    #[async_recursion]
    async fn encode_internal(
        &mut self,
        array: &[&dyn Array],
        data_type: &DataType,
    ) -> Result<usize> {
        if let DataType::FixedSizeList(items, _) = data_type {
            self.encode_fixed_size_list(array, items).await
        } else {
            self.encode_primitive(array).await
        }
    }

    async fn encode_boolean(&mut self, arrays: &[&BooleanArray]) -> Result<()> {
        let capacity: usize = arrays.iter().map(|a| a.len()).sum();
        let mut builder = BooleanBuilder::with_capacity(capacity);

        for array in arrays {
            for val in array.iter() {
                builder.append_value(val.unwrap_or_default());
            }
        }

        let boolean_array = builder.finish();
        self.writer
            .write_all(boolean_array.into_data().buffers()[0].as_slice())
            .await?;
        Ok(())
    }

    /// Encode array of primitive values.
    async fn encode_primitive(&mut self, arrays: &[&dyn Array]) -> Result<usize> {
        assert!(!arrays.is_empty());
        let data_type = arrays[0].data_type();
        let offset = self.writer.tell().await?;

        if matches!(data_type, DataType::Boolean) {
            let boolean_arr = arrays
                .iter()
                .map(|a| a.as_boolean())
                .collect::<Vec<&BooleanArray>>();
            self.encode_boolean(boolean_arr.as_slice()).await?;
        } else {
            let byte_width = data_type.byte_width();
            for a in arrays.iter() {
                let data = a.to_data();
                let slice = unsafe {
                    from_raw_parts(
                        data.buffers()[0].as_ptr().add(a.offset() * byte_width),
                        a.len() * byte_width,
                    )
                };
                self.writer.write_all(slice).await?;
            }
        }
        Ok(offset)
    }

    /// Encode fixed size list.
    async fn encode_fixed_size_list(
        &mut self,
        arrays: &[&dyn Array],
        items: &Field,
    ) -> Result<usize> {
        let mut value_arrs: Vec<ArrayRef> = Vec::new();

        for array in arrays {
            let list_array = array
                .as_any()
                .downcast_ref::<FixedSizeListArray>()
                .ok_or_else(|| {
                    Error::schema(format!(
                        "Needed a FixedSizeListArray but got {}",
                        array.data_type()
                    ))
                })?;
            let offset = list_array.value_offset(0) as usize;
            let length = list_array.len();
            let value_length = list_array.value_length() as usize;
            let value_array = list_array.values().slice(offset, length * value_length);
            value_arrs.push(value_array);
        }

        self.encode_internal(
            value_arrs
                .iter()
                .map(|a| a.as_ref())
                .collect::<Vec<_>>()
                .as_slice(),
            items.data_type(),
        )
        .await
    }
}

/// Decoder for plain encoding.
pub struct PlainDecoder<'a> {
    reader: &'a dyn Reader,
    data_type: &'a DataType,
    /// The start position of the batch in the file.
    position: usize,
    /// Number of the rows in this batch.
    length: usize,
}

/// Get byte range from the row offset range.
#[inline]
fn get_byte_range(data_type: &DataType, row_range: Range<usize>) -> Range<usize> {
    match data_type {
        DataType::Boolean => row_range.start / 8..bit_util::ceil(row_range.end, 8),
        _ => row_range.start * data_type.byte_width()..row_range.end * data_type.byte_width(),
    }
}

fn bytes_to_array(
    data_type: &DataType,
    bytes: bytes::Bytes,
    len: usize,
    offset: usize,
) -> Result<ArrayRef> {
    let layout = layout(data_type);
    if layout.buffers.len() != 1 {
        return Err(Error::internal(format!(
            "v1 plain encoding requires one value buffer, found {data_type}"
        )));
    }

    let buffer = if let BufferSpec::FixedWidth {
        byte_width,
        alignment,
    } = &layout.buffers[0]
    {
        let min_buffer_size = (len + offset).saturating_mul(*byte_width);
        if bytes.len() < min_buffer_size {
            Buffer::copy_bytes_bytes(bytes, min_buffer_size)
        } else {
            Buffer::from_bytes_bytes(bytes, *alignment as u64)
        }
    } else {
        Buffer::from_slice_ref(bytes)
    };

    let data = ArrayDataBuilder::new(data_type.clone())
        .len(len)
        .offset(offset)
        .null_count(0)
        .add_buffer(buffer)
        .build()?;
    Ok(make_array(data))
}

impl<'a> PlainDecoder<'a> {
    pub fn new(
        reader: &'a dyn Reader,
        data_type: &'a DataType,
        position: usize,
        length: usize,
    ) -> Result<Self> {
        Ok(PlainDecoder {
            reader,
            data_type,
            position,
            length,
        })
    }

    /// Decode primitive values, from "offset" to "offset + length".
    ///
    async fn decode_primitive(&self, start: usize, end: usize) -> Result<ArrayRef> {
        if end > self.length {
            return Err(Error::invalid_input(format!(
                "PlainDecoder: request([{}..{}]) out of range: [0..{}]",
                start, end, self.length
            )));
        }
        let byte_range = get_byte_range(self.data_type, start..end);
        let range = Range {
            start: self.position + byte_range.start,
            end: self.position + byte_range.end,
        };

        let data = self.reader.get_range(range).await?;
        // booleans are bitpacked, so we need an offset to provide the exact
        // requested range.
        let offset = if self.data_type == &DataType::Boolean {
            start % 8
        } else {
            0
        };
        bytes_to_array(self.data_type, data, end - start, offset)
    }

    async fn decode_fixed_size_list(
        &self,
        items: &Field,
        list_size: i32,
        start: usize,
        end: usize,
    ) -> Result<ArrayRef> {
        if !items.data_type().is_fixed_stride() {
            return Err(Error::schema(format!(
                "Items for fixed size list should be primitives but found {}",
                items.data_type()
            )));
        };
        let item_decoder = PlainDecoder::new(
            self.reader,
            items.data_type(),
            self.position,
            self.length * list_size as usize,
        )?;
        let item_array = item_decoder
            .get(start * list_size as usize..end * list_size as usize)
            .await?;
        Ok(Arc::new(FixedSizeListArray::new(
            Arc::new(items.clone()),
            list_size,
            item_array,
            None,
        )) as ArrayRef)
    }

    async fn decode_fixed_size_binary(
        &self,
        stride: i32,
        start: usize,
        end: usize,
    ) -> Result<ArrayRef> {
        let bytes_decoder = PlainDecoder::new(
            self.reader,
            &DataType::UInt8,
            self.position,
            self.length * stride as usize,
        )?;
        let bytes_array = bytes_decoder
            .get(start * stride as usize..end * stride as usize)
            .await?;
        let values = bytes_array
            .as_any()
            .downcast_ref::<UInt8Array>()
            .ok_or_else(|| {
                Error::schema("Could not cast to UInt8Array for FixedSizeBinary".to_string())
            })?;
        Ok(Arc::new(FixedSizeBinaryArray::try_new_from_values(values, stride)?) as ArrayRef)
    }

    async fn take_boolean(&self, indices: &UInt32Array) -> Result<ArrayRef> {
        let block_size = self.reader.block_size() as u32;
        let boolean_block_size = block_size * 8;

        let mut chunk_ranges = vec![];
        let mut start: u32 = 0;
        for j in 0..(indices.len() - 1) as u32 {
            if (indices.value(j as usize + 1) / boolean_block_size)
                > (indices.value(start as usize) / boolean_block_size)
            {
                let next_start = j + 1;
                chunk_ranges.push(start..next_start);
                start = next_start;
            }
        }
        // Remaining
        chunk_ranges.push(start..indices.len() as u32);

        let arrays = stream::iter(chunk_ranges)
            .map(|cr| async move {
                let request = indices.slice(cr.start as usize, cr.len());
                // request contains the array indices we are retrieving in this chunk.

                // Get the starting index
                let start = request.value(0);
                // Final index is the last value
                let end = request.value(request.len() - 1);
                let array = self.get(start as usize..end as usize + 1).await?;

                let shifted_indices = sub(&request, &UInt32Array::new_scalar(start))?;
                Ok::<ArrayRef, Error>(take(&array, &shifted_indices, None)?)
            })
            .buffered(self.reader.io_parallelism())
            .try_collect::<Vec<_>>()
            .await?;
        let references = arrays.iter().map(|a| a.as_ref()).collect::<Vec<_>>();
        Ok(concat(&references)?)
    }
}

fn make_chunked_requests(
    indices: &[u32],
    byte_width: usize,
    block_size: usize,
) -> Vec<Range<usize>> {
    let mut chunked_ranges = vec![];
    let mut start: usize = 0;
    // Note: limit the I/O size to the block size.
    //
    // Another option could be checking whether `indices[i]` and `indices[i+1]` are not
    // farther way than the block size:
    //    indices[i] * byte_width + block_size < indices[i+1] * byte_width
    // It might allow slightly larger sequential reads.
    for i in 0..indices.len() - 1 {
        // If contiguous, continue
        if indices[i + 1] == indices[i] + 1 {
            continue;
        }
        if indices[i + 1] as usize * byte_width > indices[start] as usize * byte_width + block_size
        {
            chunked_ranges.push(start..i + 1);
            start = i + 1;
        }
    }
    chunked_ranges.push(start..indices.len());
    chunked_ranges
}

impl PlainDecoder<'_> {
    pub async fn decode(&self) -> Result<ArrayRef> {
        self.get(0..self.length).await
    }

    pub async fn take(&self, indices: &UInt32Array) -> Result<ArrayRef> {
        if indices.is_empty() {
            return Ok(new_empty_array(self.data_type));
        }

        if matches!(self.data_type, DataType::Boolean) {
            return self.take_boolean(indices).await;
        }

        let block_size = self.reader.block_size();
        let byte_width = self.data_type.byte_width();

        let chunked_ranges = make_chunked_requests(indices.values(), byte_width, block_size);

        let arrays = stream::iter(chunked_ranges)
            .map(|cr| async move {
                let request = indices.slice(cr.start, cr.len());

                let start = request.value(0);
                let end = request.value(request.len() - 1);
                let array = self.get(start as usize..end as usize + 1).await?;
                let adjusted_offsets = sub(&request, &UInt32Array::new_scalar(start))?;
                Ok::<ArrayRef, Error>(take(&array, &adjusted_offsets, None)?)
            })
            .buffered(self.reader.io_parallelism())
            .try_collect::<Vec<_>>()
            .await?;
        let references = arrays.iter().map(|a| a.as_ref()).collect::<Vec<_>>();
        Ok(concat(&references)?)
    }

    #[async_recursion]
    async fn decode_range(&self, index: Range<usize>) -> Result<ArrayRef> {
        if index.is_empty() {
            return Ok(new_empty_array(self.data_type));
        }
        match self.data_type {
            DataType::FixedSizeList(items, list_size) => {
                self.decode_fixed_size_list(items, *list_size, index.start, index.end)
                    .await
            }
            DataType::FixedSizeBinary(stride) => {
                self.decode_fixed_size_binary(*stride, index.start, index.end)
                    .await
            }
            _ => self.decode_primitive(index.start, index.end).await,
        }
    }

    pub async fn get(&self, params: impl Into<ReadBatchParams>) -> Result<ArrayRef> {
        match params.into() {
            ReadBatchParams::Range(range) => self.decode_range(range).await,
            ReadBatchParams::Ranges(_) => Err(Error::invalid_input(
                "multiple ranges are not supported by v1 plain encoding",
            )),
            ReadBatchParams::RangeFull => self.decode_range(0..self.length).await,
            ReadBatchParams::RangeTo(range) => self.decode_range(0..range.end).await,
            ReadBatchParams::RangeFrom(range) => self.decode_range(range.start..self.length).await,
            ReadBatchParams::Indices(indices) => self.take(&indices).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Deref;

    use arrow_array::*;
    use arrow_buffer::Buffer;
    use arrow_data::ArrayDataBuilder;
    use bytes::Bytes;
    use lance_core::utils::tempfile::TempStdFile;
    use lance_io::local::LocalObjectReader;
    use rand::prelude::*;

    use super::*;

    #[tokio::test]
    async fn test_encode_decode_primitive_array() {
        let int_types = vec![
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::UInt8,
            DataType::UInt16,
            DataType::UInt32,
            DataType::UInt64,
        ];
        let input: Vec<i64> = Vec::from_iter(1..127_i64);
        for t in int_types {
            let buffer = Buffer::from_slice_ref(input.as_slice());
            let mut arrs: Vec<ArrayRef> = Vec::new();
            for _ in 0..10 {
                arrs.push(Arc::new(make_array_(&t, &buffer).await));
            }
            test_round_trip(arrs.as_slice(), t).await;
        }

        let float_types = vec![DataType::Float16, DataType::Float32, DataType::Float64];
        let mut rng = rand::rng();
        let input: Vec<f64> = (1..127).map(|_| rng.random()).collect();
        for t in float_types {
            let buffer = Buffer::from_slice_ref(input.as_slice());
            let mut arrs: Vec<ArrayRef> = Vec::new();

            for _ in 0..10 {
                arrs.push(Arc::new(make_array_(&t, &buffer).await));
            }
            test_round_trip(arrs.as_slice(), t).await;
        }
    }

    async fn test_round_trip(expected: &[ArrayRef], data_type: DataType) {
        let path = TempStdFile::default();

        let expected_as_array = expected
            .iter()
            .map(|e| e.as_ref())
            .collect::<Vec<&dyn Array>>();
        {
            let mut writer = tokio::fs::File::create(&path).await.unwrap();
            let mut encoder = PlainEncoder::new(&mut writer, &data_type);
            assert_eq!(
                encoder.encode(expected_as_array.as_slice()).await.unwrap(),
                0
            );
            writer.flush().await.unwrap();
        }

        let reader = LocalObjectReader::open_local_path(&path, 1024, None)
            .await
            .unwrap();
        assert!(reader.size().await.unwrap() > 0);
        // Expected size is the total of all arrays
        let expected_size = expected.iter().map(|e| e.len()).sum();
        let decoder = PlainDecoder::new(reader.as_ref(), &data_type, 0, expected_size).unwrap();
        let arr = decoder.decode().await.unwrap();
        let actual = arr.as_ref();
        let expected_merged = concat(expected_as_array.as_slice()).unwrap();
        assert_eq!(expected_merged.deref(), actual);
        assert_eq!(expected_size, actual.len());
    }

    #[tokio::test]
    async fn test_encode_decode_bool_array() {
        let mut arrs: Vec<ArrayRef> = Vec::new();

        for _ in 0..10 {
            // It is important that the boolean array length is < 8 so we can test if the Arrays are merged correctly
            arrs.push(Arc::new(BooleanArray::from(vec![true, true, true])) as ArrayRef);
        }
        test_round_trip(arrs.as_slice(), DataType::Boolean).await;
    }

    #[tokio::test]
    async fn test_encode_decode_fixed_size_list_array() {
        let int_types = vec![
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::UInt8,
            DataType::UInt16,
            DataType::UInt32,
            DataType::UInt64,
        ];
        let input = Vec::from_iter(1..127_i64);
        for t in int_types {
            let buffer = Buffer::from_slice_ref(input.as_slice());
            let list_type =
                DataType::FixedSizeList(Arc::new(Field::new("item", t.clone(), true)), 3);
            let mut arrs: Vec<ArrayRef> = Vec::new();

            for _ in 0..10 {
                let items = make_array_(&t.clone(), &buffer).await;
                let arr = FixedSizeListArray::try_new_from_values(items, 3).unwrap();
                arrs.push(Arc::new(arr) as ArrayRef);
            }
            test_round_trip(arrs.as_slice(), list_type).await;
        }
    }

    #[tokio::test]
    async fn test_encode_decode_fixed_size_binary_array() {
        let t = DataType::FixedSizeBinary(3);
        let mut arrs: Vec<ArrayRef> = Vec::new();

        for _ in 0..10 {
            let values = UInt8Array::from(Vec::from_iter(1..127_u8));
            let arr = FixedSizeBinaryArray::try_new_from_values(&values, 3).unwrap();
            arrs.push(Arc::new(arr) as ArrayRef);
        }
        test_round_trip(arrs.as_slice(), t).await;
    }

    #[tokio::test]
    async fn test_bytes_to_array_padding() {
        let bytes = Bytes::from_static(&[0x01, 0x00, 0x02, 0x00, 0x03]);
        let arr = bytes_to_array(&DataType::UInt16, bytes, 3, 0).unwrap();

        let expected = UInt16Array::from(vec![1, 2, 3]);
        assert_eq!(arr.as_ref(), &expected);

        // Underlying data is padded to the nearest multiple of two bytes (for u16).
        let data = arr.to_data();
        let buf = &data.buffers()[0];
        let repr = format!("{:?}", buf);
        assert!(
            repr.contains("[1, 0, 2, 0, 3, 0]"),
            "Underlying buffer contains unexpected data: {}",
            repr
        );
    }

    #[tokio::test]
    async fn test_encode_decode_nested_fixed_size_list() {
        // FixedSizeList of FixedSizeList
        let inner = DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Int64, true)), 2);
        let t = DataType::FixedSizeList(Arc::new(Field::new("item", inner, true)), 2);
        let mut arrs: Vec<ArrayRef> = Vec::new();

        for _ in 0..10 {
            let values = Int64Array::from_iter_values(1..=120_i64);
            let arr = FixedSizeListArray::try_new_from_values(
                FixedSizeListArray::try_new_from_values(values, 2).unwrap(),
                2,
            )
            .unwrap();
            arrs.push(Arc::new(arr) as ArrayRef);
        }
        test_round_trip(arrs.as_slice(), t).await;

        // FixedSizeList of FixedSizeBinary
        let inner = DataType::FixedSizeBinary(2);
        let t = DataType::FixedSizeList(Arc::new(Field::new("item", inner, true)), 2);
        let mut arrs: Vec<ArrayRef> = Vec::new();

        for _ in 0..10 {
            let values = UInt8Array::from_iter_values(1..=120_u8);
            let arr = FixedSizeListArray::try_new_from_values(
                FixedSizeBinaryArray::try_new_from_values(&values, 2).unwrap(),
                2,
            )
            .unwrap();
            arrs.push(Arc::new(arr) as ArrayRef);
        }
        test_round_trip(arrs.as_slice(), t).await;
    }

    async fn make_array_(data_type: &DataType, buffer: &Buffer) -> ArrayRef {
        arrow_array::make_array(
            ArrayDataBuilder::new(data_type.clone())
                .len(126)
                .add_buffer(buffer.clone())
                .build()
                .unwrap(),
        )
    }

    #[tokio::test]
    async fn test_decode_by_range() {
        let path = TempStdFile::default();

        let array = Int32Array::from_iter_values([0, 1, 2, 3, 4, 5]);
        {
            let mut writer = tokio::fs::File::create(&path).await.unwrap();
            let mut encoder = PlainEncoder::new(&mut writer, array.data_type());
            assert_eq!(encoder.encode(&[&array]).await.unwrap(), 0);
            writer.flush().await.unwrap();
        }

        let reader = LocalObjectReader::open_local_path(&path, 2048, None)
            .await
            .unwrap();
        assert!(reader.size().await.unwrap() > 0);
        let decoder =
            PlainDecoder::new(reader.as_ref(), array.data_type(), 0, array.len()).unwrap();
        assert_eq!(
            decoder.get(2..4).await.unwrap().as_ref(),
            &Int32Array::from_iter_values([2, 3])
        );

        assert_eq!(
            decoder.get(..4).await.unwrap().as_ref(),
            &Int32Array::from_iter_values([0, 1, 2, 3])
        );

        assert_eq!(
            decoder.get(2..).await.unwrap().as_ref(),
            &Int32Array::from_iter_values([2, 3, 4, 5])
        );

        assert_eq!(
            &decoder.get(2..2).await.unwrap(),
            &new_empty_array(&DataType::Int32)
        );

        assert_eq!(
            &decoder.get(5..5).await.unwrap(),
            &new_empty_array(&DataType::Int32)
        );

        assert!(decoder.get(3..1000).await.is_err());
    }

    #[tokio::test]
    async fn test_take() {
        let path = TempStdFile::default();

        let array = Int32Array::from_iter_values(0..100);

        {
            let mut writer = tokio::fs::File::create(&path).await.unwrap();
            let mut encoder = PlainEncoder::new(&mut writer, array.data_type());
            assert_eq!(encoder.encode(&[&array]).await.unwrap(), 0);
            AsyncWriteExt::shutdown(&mut writer).await.unwrap();
        }

        let reader = LocalObjectReader::open_local_path(&path, 2048, None)
            .await
            .unwrap();
        assert!(reader.size().await.unwrap() > 0);
        let decoder =
            PlainDecoder::new(reader.as_ref(), array.data_type(), 0, array.len()).unwrap();

        let results = decoder
            .take(&UInt32Array::from_iter(
                [2, 4, 5, 20, 30, 55, 60].iter().map(|i| *i as u32),
            ))
            .await
            .unwrap();
        assert_eq!(
            results.as_ref(),
            &Int32Array::from_iter_values([2, 4, 5, 20, 30, 55, 60])
        );
    }

    // Re-enable the following tests once the Lance FileReader / FileWrite is migrated.

    // #[tokio::test]
    // async fn test_boolean_slice() {
    //     let store = ObjectStore::memory();
    //     let path = Path::from("/bool_slice");
    //
    //     let arrow_schema = Arc::new(ArrowSchema::new(vec![Field::new(
    //         "b",
    //         DataType::Boolean,
    //         true,
    //     )]));
    //     let schema = Schema::try_from(arrow_schema.as_ref()).unwrap();
    //     let mut file_writer =
    //         FileWriter::try_new(&store, &path, schema.clone(), &Default::default())
    //             .await
    //             .unwrap();
    //
    //     let array = BooleanArray::from((0..120).map(|v| v % 5 == 0).collect::<Vec<_>>());
    //     for i in 0..10 {
    //         let data = array.slice(i * 12, 12); // one and half byte
    //         file_writer
    //             .write(&[RecordBatch::try_new(arrow_schema.clone(), vec![Arc::new(data)]).unwrap()])
    //             .await
    //             .unwrap();
    //     }
    //     file_writer.finish().await.unwrap();
    //
    //     let batch = read_file_as_one_batch(&store, &path).await;
    //     assert_eq!(batch.column_by_name("b").unwrap().as_ref(), &array);
    //
    //     let array = BooleanArray::from(vec![Some(true), Some(false), None]);
    //     let mut file_writer = FileWriter::try_new(&store, &path, schema, &Default::default())
    //         .await
    //         .unwrap();
    //     file_writer
    //         .write(&[RecordBatch::try_new(arrow_schema.clone(), vec![Arc::new(array)]).unwrap()])
    //         .await
    //         .unwrap();
    //     file_writer.finish().await.unwrap();
    //     let batch = read_file_as_one_batch(&store, &path).await;
    //
    //     // None default to Some(false), since we don't support null values yet.
    //     let expected = BooleanArray::from(vec![Some(true), Some(false), Some(false)]);
    //     assert_eq!(batch.column_by_name("b").unwrap().as_ref(), &expected);
    // }
    //
    // #[tokio::test]
    // async fn test_encode_fixed_size_list_slice() {
    //     let store = ObjectStore::memory();
    //     let path = Path::from("/shared_slice");
    //
    //     let array = Int32Array::from_iter_values(0..1600);
    //     let fixed_size_list = FixedSizeListArray::try_new_from_values(array, 16).unwrap();
    //     let arrow_schema = Arc::new(ArrowSchema::new(vec![Field::new(
    //         "fl",
    //         fixed_size_list.data_type().clone(),
    //         false,
    //     )]));
    //     let schema = Schema::try_from(arrow_schema.as_ref()).unwrap();
    //     let mut file_writer = FileWriter::try_new(&store, &path, schema, &Default::default())
    //         .await
    //         .unwrap();
    //
    //     for i in (0..100).step_by(4) {
    //         let slice: FixedSizeListArray = fixed_size_list.slice(i, 4);
    //         file_writer
    //             .write(&[
    //                 RecordBatch::try_new(arrow_schema.clone(), vec![Arc::new(slice)]).unwrap(),
    //             ])
    //             .await
    //             .unwrap();
    //     }
    //     file_writer.finish().await.unwrap();
    //
    //     let batch = read_file_as_one_batch(&store, &path).await;
    //     assert_eq!(
    //         batch.column_by_name("fl").unwrap().as_ref(),
    //         &fixed_size_list
    //     );
    // }
    //
    // #[tokio::test]
    // async fn test_take_boolean() {
    //     let temp_dir = tempfile::tempdir().unwrap();
    //     let path = temp_dir.join("/bool_take");
    //
    //     let arrow_schema = Arc::new(ArrowSchema::new(vec![Field::new(
    //         "b",
    //         DataType::Boolean,
    //         false,
    //     )]));
    //     let schema = Schema::try_from(arrow_schema.as_ref()).unwrap();
    //     let mut file_writer =
    //         FileWriter::try_new(&store, &path, schema.clone(), &Default::default())
    //             .await
    //             .unwrap();
    //
    //     let array = BooleanArray::from((0..120).map(|v| v % 5 == 0).collect::<Vec<_>>());
    //     let batch =
    //         RecordBatch::try_new(arrow_schema.clone(), vec![Arc::new(array.clone())]).unwrap();
    //     file_writer.write(&[batch]).await.unwrap();
    //     file_writer.finish().await.unwrap();
    //
    //     let reader = FileReader::try_new(&store, &path).await.unwrap();
    //     let actual = reader
    //         .take(&[2, 4, 5, 8, 63, 64, 65], &schema)
    //         .await
    //         .unwrap();
    //
    //     assert_eq!(
    //         actual.column_by_name("b").unwrap().as_ref(),
    //         &BooleanArray::from(vec![false, false, true, false, false, false, true])
    //     );
    // }

    #[test]
    fn test_make_chunked_request() {
        let byte_width: usize = 4096; // 4K
        let prefetch_size: usize = 64 * 1024; // 64KB.
        let u32_overflow: usize = u32::MAX as usize + 10;

        let indices: Vec<u32> = vec![
            1,
            10,
            20,
            100,
            120,
            (u32_overflow / byte_width) as u32, // Two overflow offsets
            (u32_overflow / byte_width) as u32 + 100,
        ];
        let chunks = make_chunked_requests(&indices, byte_width, prefetch_size);
        assert_eq!(chunks.len(), 6, "got chunks: {:?}", chunks);
        assert_eq!(chunks, vec![(0..2), (2..3), (3..4), (4..5), (5..6), (6..7)])
    }
}
