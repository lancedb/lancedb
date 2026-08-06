// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Var-length binary encoding.
//!

use std::marker::PhantomData;
use std::ops::Range;
use std::sync::Arc;

use arrow_arith::numeric::sub;
use arrow_array::{
    Array, ArrayRef, GenericByteArray, Int64Array, OffsetSizeTrait, UInt32Array,
    builder::{ArrayBuilder, PrimitiveBuilder},
    cast::AsArray,
    cast::as_primitive_array,
    new_empty_array,
    types::{
        BinaryType, ByteArrayType, Int64Type, LargeBinaryType, LargeUtf8Type, UInt32Type, Utf8Type,
    },
};
use arrow_buffer::{ArrowNativeType, Buffer, MutableBuffer, ScalarBuffer, bit_util};
use arrow_cast::cast::cast;
use arrow_data::ArrayDataBuilder;
use arrow_schema::DataType;
use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use lance_arrow::BufferExt;
use tokio::io::AsyncWriteExt;

use super::plain::PlainDecoder;
use lance_core::Result;
use lance_io::{
    ReadBatchParams,
    traits::{Reader, Writer},
};

/// Encoder for Var-binary encoding.
pub struct BinaryEncoder<'a> {
    writer: &'a mut dyn Writer,
}

impl<'a> BinaryEncoder<'a> {
    pub fn new(writer: &'a mut dyn Writer) -> Self {
        Self { writer }
    }

    async fn encode_typed_arr<T: ByteArrayType>(&mut self, arrs: &[&dyn Array]) -> Result<usize> {
        let capacity: usize = arrs.iter().map(|a| a.len()).sum();
        let mut pos_builder: PrimitiveBuilder<Int64Type> =
            PrimitiveBuilder::with_capacity(capacity + 1);

        let mut last_offset: usize = self.writer.tell().await?;
        pos_builder.append_value(last_offset as i64);
        for array in arrs.iter() {
            let arr = array
                .as_any()
                .downcast_ref::<GenericByteArray<T>>()
                .unwrap();

            let offsets = arr.value_offsets();

            let start = offsets[0].as_usize();
            let end = offsets[offsets.len() - 1].as_usize();
            let b = unsafe {
                std::slice::from_raw_parts(
                    arr.to_data().buffers()[1].as_ptr().add(start),
                    end - start,
                )
            };
            self.writer.write_all(b).await?;

            let start_offset = offsets[0].as_usize();
            offsets
                .iter()
                .skip(1)
                .map(|b| b.as_usize() - start_offset + last_offset)
                .for_each(|o| pos_builder.append_value(o as i64));
            last_offset = pos_builder.values_slice()[pos_builder.len() - 1] as usize;
        }

        let positions_offset = self.writer.tell().await?;
        let pos_array = pos_builder.finish();
        self.writer
            .write_all(pos_array.to_data().buffers()[0].as_slice())
            .await?;
        Ok(positions_offset)
    }
}

impl BinaryEncoder<'_> {
    pub async fn encode(&mut self, arrs: &[&dyn Array]) -> Result<usize> {
        assert!(!arrs.is_empty());
        let data_type = arrs[0].data_type();
        match data_type {
            DataType::Utf8 => self.encode_typed_arr::<Utf8Type>(arrs).await,
            DataType::Binary => self.encode_typed_arr::<BinaryType>(arrs).await,
            DataType::LargeUtf8 => self.encode_typed_arr::<LargeUtf8Type>(arrs).await,
            DataType::LargeBinary => self.encode_typed_arr::<LargeBinaryType>(arrs).await,
            _ => Err(lance_core::Error::invalid_input(format!(
                "Unsupported data type for binary encoding: {}",
                data_type
            ))),
        }
    }
}

/// Var-binary encoding decoder.
pub struct BinaryDecoder<'a, T: ByteArrayType> {
    reader: &'a dyn Reader,

    position: usize,

    length: usize,

    nullable: bool,

    phantom: PhantomData<T>,
}

/// Var-length Binary Decoder
///
impl<'a, T: ByteArrayType> BinaryDecoder<'a, T> {
    /// Create a [BinaryEncoder] to decode one batch.
    ///
    ///  - `position`, file position where this batch starts.
    ///  - `length`, the number of records in this batch.
    ///  - `nullable`, whether this batch contains nullable value.
    ///
    /// ## Example
    ///
    /// ```rust
    /// use arrow_array::types::Utf8Type;
    /// use object_store::path::Path;
    /// use lance_file::versions::v1::encoding::binary::BinaryDecoder;
    /// use lance_io::{local::LocalObjectReader, traits::Reader};
    ///
    /// async {
    ///     let reader = LocalObjectReader::open_local_path("/tmp/foo.lance", 2048, None).await.unwrap();
    ///     let string_decoder = BinaryDecoder::<Utf8Type>::new(reader.as_ref(), 100, 1024, true);
    /// };
    /// ```
    pub fn new(reader: &'a dyn Reader, position: usize, length: usize, nullable: bool) -> Self {
        Self {
            reader,
            position,
            length,
            nullable,
            phantom: PhantomData,
        }
    }

    /// Get the position array for the batch.
    async fn get_positions(&self, index: Range<usize>) -> Result<Arc<Int64Array>> {
        let position_decoder = PlainDecoder::new(
            self.reader,
            &DataType::Int64,
            self.position,
            self.length + 1,
        )?;
        let values = position_decoder.get(index.start..index.end + 1).await?;
        Ok(Arc::new(as_primitive_array(&values).clone()))
    }

    fn count_nulls<O: OffsetSizeTrait>(offsets: &ScalarBuffer<O>) -> (usize, Option<Buffer>) {
        let mut null_count = 0;
        let mut null_buf = MutableBuffer::new_null(offsets.len() - 1);
        offsets.windows(2).enumerate().for_each(|(idx, w)| {
            if w[0] == w[1] {
                bit_util::unset_bit(null_buf.as_mut(), idx);
                null_count += 1;
            } else {
                bit_util::set_bit(null_buf.as_mut(), idx);
            }
        });
        let null_buf = if null_count > 0 {
            Some(null_buf.into())
        } else {
            None
        };
        (null_count, null_buf)
    }

    /// Read the array with batch positions and range.
    ///
    /// Parameters
    ///
    ///  - *positions*: position array for the batch.
    ///  - *range*: range of rows to read.
    async fn get_range(&self, positions: &Int64Array, range: Range<usize>) -> Result<ArrayRef> {
        assert!(positions.len() >= range.end);
        let start = positions.value(range.start);
        let end = positions.value(range.end);

        let start_scalar = Int64Array::new_scalar(start);

        let slice = positions.slice(range.start, range.len() + 1);
        let offset_data = if T::Offset::IS_LARGE {
            sub(&slice, &start_scalar)?.into_data()
        } else {
            cast(
                &(Arc::new(sub(&slice, &start_scalar)?) as ArrayRef),
                &DataType::Int32,
            )?
            .into_data()
        };

        let bytes: Bytes = if start >= end {
            Bytes::new()
        } else {
            self.reader.get_range(start as usize..end as usize).await?
        };

        let mut data_builder = ArrayDataBuilder::new(T::DATA_TYPE)
            .len(range.len())
            .null_count(0);

        // Count nulls
        if self.nullable {
            let (null_count, null_buf) = Self::count_nulls(slice.values());
            data_builder = data_builder
                .null_count(null_count)
                .null_bit_buffer(null_buf);
        }

        let buf = Buffer::from_bytes_bytes(bytes, /*bytes_per_value=*/ 1);
        let array_data = data_builder
            .add_buffer(offset_data.buffers()[0].clone())
            .add_buffer(buf)
            .build()?;

        Ok(Arc::new(GenericByteArray::<T>::from(array_data)))
    }
}

#[derive(Debug)]
struct TakeChunksPlan {
    indices: UInt32Array,
    is_contiguous: bool,
}

/// Group the indices into chunks, such that either:
/// 1. the indices are contiguous (and non-repeating)
/// 2. the values are within `min_io_size` of each other (and thus are worth
///    grabbing in a single request)
fn plan_take_chunks(
    positions: &Int64Array,
    indices: &UInt32Array,
    min_io_size: i64,
) -> Result<Vec<TakeChunksPlan>> {
    let start = indices.value(0);
    let indices = sub(indices, &UInt32Array::new_scalar(start))?;
    let indices_ref = indices.as_primitive::<UInt32Type>();

    let mut chunks: Vec<TakeChunksPlan> = vec![];
    let mut start_idx = 0;
    let mut last_idx: i64 = -1;
    let mut is_contiguous = true;
    for i in 0..indices.len() {
        let current = indices_ref.value(i) as usize;
        let curr_contiguous = current == start_idx || current as i64 - last_idx == 1;

        if !curr_contiguous
            && positions.value(current) - positions.value(indices_ref.value(start_idx) as usize)
                > min_io_size
        {
            chunks.push(TakeChunksPlan {
                indices: as_primitive_array(&indices.slice(start_idx, i - start_idx)).clone(),
                is_contiguous,
            });
            start_idx = i;
            is_contiguous = true;
        } else {
            is_contiguous &= curr_contiguous;
        }

        last_idx = current as i64;
    }
    chunks.push(TakeChunksPlan {
        indices: as_primitive_array(&indices.slice(start_idx, indices.len() - start_idx)).clone(),
        is_contiguous,
    });

    Ok(chunks)
}

impl<T: ByteArrayType> BinaryDecoder<'_, T> {
    pub async fn decode(&self) -> Result<ArrayRef> {
        self.get(..).await
    }

    /// Take the values at the given indices.
    ///
    /// This function assumes indices are sorted.
    pub async fn take(&self, indices: &UInt32Array) -> Result<ArrayRef> {
        if indices.is_empty() {
            return Ok(new_empty_array(&T::DATA_TYPE));
        }

        let start = indices.value(0);
        let end = indices.value(indices.len() - 1);

        // TODO: make min batch size configurable.
        // TODO: make reading positions in chunks too.
        const MIN_IO_SIZE: i64 = 64 * 1024; // 64KB
        let positions = self
            .get_positions(start as usize..(end + 1) as usize)
            .await?;
        // Use indices and positions to pre-allocate an exact-size buffer
        let capacity = indices
            .iter()
            .map(|i| {
                let relative_index = (i.unwrap() - start) as usize;
                let start = positions.value(relative_index) as usize;
                let end = positions.value(relative_index + 1) as usize;
                end - start
            })
            .sum();
        let mut buffer = MutableBuffer::with_capacity(capacity);

        let offsets_capacity = std::mem::size_of::<T::Offset>() * (indices.len() + 1);
        let mut offsets = MutableBuffer::with_capacity(offsets_capacity);
        let mut offset = T::Offset::from_usize(0).unwrap();
        // Safety: We allocated appropriate capacity just above.
        unsafe {
            offsets.push_unchecked(offset);
        }

        let chunks = plan_take_chunks(&positions, indices, MIN_IO_SIZE)?;

        let positions_ref = positions.as_ref();
        futures::stream::iter(chunks)
            .map(|chunk| async move {
                let chunk_offset = chunk.indices.value(0);
                let chunk_end = chunk.indices.value(chunk.indices.len() - 1);
                let array = self
                    .get_range(positions_ref, chunk_offset as usize..chunk_end as usize + 1)
                    .await?;
                Result::Ok((chunk, chunk_offset, array))
            })
            .buffered(self.reader.io_parallelism())
            .try_for_each(|(chunk, chunk_offset, array)| {
                let array: &GenericByteArray<T> = array.as_bytes();

                // Faster to do one large memcpy than O(n) small ones.
                if chunk.is_contiguous {
                    buffer.extend_from_slice(array.value_data());
                }

                // Append each value to the buffer in the correct order
                for index in chunk.indices.values() {
                    if !chunk.is_contiguous {
                        let value = array.value((index - chunk_offset) as usize);
                        let value_ref: &[u8] = value.as_ref();
                        buffer.extend_from_slice(value_ref);
                    }

                    offset += array.value_length((index - chunk_offset) as usize);
                    // Append next offset
                    // Safety: We allocated appropriate capacity on initialization
                    unsafe {
                        offsets.push_unchecked(offset);
                    }
                }
                futures::future::ready(Ok(()))
            })
            .await?;

        let mut data_builder = ArrayDataBuilder::new(T::DATA_TYPE)
            .len(indices.len())
            .null_count(0);

        let offsets: ScalarBuffer<T::Offset> = ScalarBuffer::from(Buffer::from(offsets));

        // We should have pre-sized perfectly.
        debug_assert_eq!(buffer.len(), capacity);

        if self.nullable {
            let (null_count, null_buf) = Self::count_nulls(&offsets);
            data_builder = data_builder
                .null_count(null_count)
                .null_bit_buffer(null_buf);
        }

        let array_data = data_builder
            .add_buffer(offsets.into_inner())
            .add_buffer(buffer.into())
            .build()?;

        Ok(Arc::new(GenericByteArray::<T>::from(array_data)))
    }
}

impl<T: ByteArrayType> BinaryDecoder<'_, T> {
    async fn decode_range(&self, index: Range<usize>) -> Result<ArrayRef> {
        if index.end > self.length {
            return Err(lance_core::Error::invalid_input(format!(
                "v1 binary row range {}..{} exceeds length {}",
                index.start, index.end, self.length
            )));
        }
        if index.is_empty() {
            return Ok(new_empty_array(&T::DATA_TYPE));
        }
        let position_decoder = PlainDecoder::new(
            self.reader,
            &DataType::Int64,
            self.position,
            self.length + 1,
        )?;
        let positions = position_decoder.get(index.start..index.end + 1).await?;
        let int64_positions: &Int64Array = as_primitive_array(&positions);

        self.get_range(int64_positions, 0..index.len()).await
    }

    pub async fn get(&self, params: impl Into<ReadBatchParams>) -> Result<ArrayRef> {
        match params.into() {
            ReadBatchParams::Range(range) => self.decode_range(range).await,
            ReadBatchParams::Ranges(_) => Err(lance_core::Error::invalid_input(
                "multiple ranges are not supported by v1 binary encoding",
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
    use super::*;

    use arrow_array::{
        BinaryArray, GenericStringArray, LargeStringArray, StringArray, types::GenericStringType,
    };
    use arrow_select::concat::concat;
    use lance_core::utils::tempfile::TempStdFile;

    use lance_io::local::LocalObjectReader;

    async fn write_test_data<O: OffsetSizeTrait>(
        path: impl AsRef<std::path::Path>,
        arr: &[&GenericStringArray<O>],
    ) -> Result<usize> {
        let mut writer = tokio::fs::File::create(path).await?;
        // Write some garbage to reset "tell()".
        writer.write_all(b"1234").await.unwrap();
        let mut encoder = BinaryEncoder::new(&mut writer);

        let arrs = arr.iter().map(|a| a as &dyn Array).collect::<Vec<_>>();
        let pos = encoder.encode(arrs.as_slice()).await.unwrap();
        AsyncWriteExt::shutdown(&mut writer).await.unwrap();
        Ok(pos)
    }

    async fn test_round_trips<O: OffsetSizeTrait>(arrs: &[&GenericStringArray<O>]) {
        let path = TempStdFile::default();

        let pos = write_test_data(&path, arrs).await.unwrap();

        let reader = LocalObjectReader::open_local_path(&path, 1024, None)
            .await
            .unwrap();
        let read_len = arrs.iter().map(|a| a.len()).sum();
        let decoder =
            BinaryDecoder::<GenericStringType<O>>::new(reader.as_ref(), pos, read_len, true);
        let actual_arr = decoder.decode().await.unwrap();

        let arrs_ref = arrs.iter().map(|a| a as &dyn Array).collect::<Vec<_>>();
        let expected = concat(arrs_ref.as_slice()).unwrap();
        assert_eq!(
            actual_arr
                .as_any()
                .downcast_ref::<GenericStringArray<O>>()
                .unwrap(),
            expected
                .as_any()
                .downcast_ref::<GenericStringArray<O>>()
                .unwrap(),
        );
    }

    #[tokio::test]
    async fn test_write_binary_data() {
        test_round_trips(&[&StringArray::from(vec!["a", "b", "cd", "efg"])]).await;
        test_round_trips(&[&StringArray::from(vec![Some("a"), None, Some("cd"), None])]).await;
        test_round_trips(&[
            &StringArray::from(vec![Some("a"), None, Some("cd"), None]),
            &StringArray::from(vec![Some("f"), None, Some("gh"), None]),
            &StringArray::from(vec![Some("t"), None, Some("uv"), None]),
        ])
        .await;
        test_round_trips(&[&LargeStringArray::from(vec!["a", "b", "cd", "efg"])]).await;
        test_round_trips(&[&LargeStringArray::from(vec![
            Some("a"),
            None,
            Some("cd"),
            None,
        ])])
        .await;
        test_round_trips(&[
            &LargeStringArray::from(vec![Some("a"), Some("b")]),
            &LargeStringArray::from(vec![Some("c")]),
            &LargeStringArray::from(vec![Some("d"), Some("e")]),
        ])
        .await;
    }

    #[tokio::test]
    async fn test_write_binary_data_with_offset() {
        let array: StringArray = StringArray::from(vec![Some("d"), Some("e")]).slice(1, 1);
        test_round_trips(&[&array]).await;
    }

    #[tokio::test]
    async fn test_range_query() {
        let data = StringArray::from_iter_values(["a", "b", "c", "d", "e", "f", "g"]);

        let path = TempStdFile::default();
        let mut object_writer = tokio::fs::File::create(&path).await.unwrap();

        // Write some garbage to reset "tell()".
        object_writer.write_all(b"1234").await.unwrap();
        let mut encoder = BinaryEncoder::new(&mut object_writer);
        let pos = encoder.encode(&[&data]).await.unwrap();
        AsyncWriteExt::shutdown(&mut object_writer).await.unwrap();

        let reader = LocalObjectReader::open_local_path(&path, 1024, None)
            .await
            .unwrap();
        let decoder = BinaryDecoder::<Utf8Type>::new(reader.as_ref(), pos, data.len(), false);
        assert_eq!(
            decoder.decode().await.unwrap().as_ref(),
            &StringArray::from_iter_values(["a", "b", "c", "d", "e", "f", "g"])
        );

        assert_eq!(
            decoder.get(..).await.unwrap().as_ref(),
            &StringArray::from_iter_values(["a", "b", "c", "d", "e", "f", "g"])
        );

        assert_eq!(
            decoder.get(2..5).await.unwrap().as_ref(),
            &StringArray::from_iter_values(["c", "d", "e"])
        );

        assert_eq!(
            decoder.get(..5).await.unwrap().as_ref(),
            &StringArray::from_iter_values(["a", "b", "c", "d", "e"])
        );

        assert_eq!(
            decoder.get(4..).await.unwrap().as_ref(),
            &StringArray::from_iter_values(["e", "f", "g"])
        );
        assert_eq!(
            decoder.get(2..2).await.unwrap().as_ref(),
            &new_empty_array(&DataType::Utf8)
        );
        assert!(decoder.get(100..100).await.is_err());
    }

    #[tokio::test]
    async fn test_take() {
        let data = StringArray::from_iter_values(["a", "b", "c", "d", "e", "f", "g"]);

        let path = TempStdFile::default();

        let pos = write_test_data(&path, &[&data]).await.unwrap();
        let reader = LocalObjectReader::open_local_path(&path, 1024, None)
            .await
            .unwrap();
        let decoder = BinaryDecoder::<Utf8Type>::new(reader.as_ref(), pos, data.len(), false);

        let actual = decoder
            .take(&UInt32Array::from_iter_values([1, 2, 5]))
            .await
            .unwrap();
        assert_eq!(
            actual.as_ref(),
            &StringArray::from_iter_values(["b", "c", "f"])
        );
    }

    #[tokio::test]
    async fn test_take_sparse_indices() {
        let data = StringArray::from_iter_values((0..1000000).map(|v| format!("string-{v}")));

        let path = TempStdFile::default();
        let pos = write_test_data(&path, &[&data]).await.unwrap();
        let reader = LocalObjectReader::open_local_path(&path, 1024, None)
            .await
            .unwrap();
        let decoder = BinaryDecoder::<Utf8Type>::new(reader.as_ref(), pos, data.len(), false);

        let positions = decoder.get_positions(1..999998).await.unwrap();
        let indices = UInt32Array::from_iter_values([1, 999998]);
        let chunks = plan_take_chunks(positions.as_ref(), &indices, 64 * 1024).unwrap();
        // Relative offset within the positions.
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].indices, UInt32Array::from_iter_values([0]),);
        assert_eq!(chunks[1].indices, UInt32Array::from_iter_values([999997]),);

        let actual = decoder
            .take(&UInt32Array::from_iter_values([1, 999998]))
            .await
            .unwrap();
        assert_eq!(
            actual.as_ref(),
            &StringArray::from_iter_values(["string-1", "string-999998"])
        );
    }

    #[tokio::test]
    async fn test_take_dense_indices() {
        let data = StringArray::from_iter_values((0..1000000).map(|v| format!("string-{v}")));

        let path = TempStdFile::default();
        let pos = write_test_data(&path, &[&data]).await.unwrap();

        let reader = LocalObjectReader::open_local_path(&path, 1024, None)
            .await
            .unwrap();
        let decoder = BinaryDecoder::<Utf8Type>::new(reader.as_ref(), pos, data.len(), false);

        let positions = decoder.get_positions(1..999998).await.unwrap();
        let indices = UInt32Array::from_iter_values([
            2, 3, 4, 1001, 1001, 1002, 2001, 2002, 2004, 3004, 3005,
        ]);

        let chunks = plan_take_chunks(positions.as_ref(), &indices, 1024).unwrap();
        assert_eq!(chunks.len(), 4);
        // A contiguous range.
        assert_eq!(chunks[0].indices, UInt32Array::from_iter_values(0..3));
        assert!(chunks[0].is_contiguous);
        // Not contiguous because of repeats
        assert_eq!(
            chunks[1].indices,
            UInt32Array::from_iter_values([999, 999, 1000])
        );
        assert!(!chunks[1].is_contiguous);
        // Not contiguous because of gaps
        assert_eq!(
            chunks[2].indices,
            UInt32Array::from_iter_values([1999, 2000, 2002])
        );
        assert!(!chunks[2].is_contiguous);
        // Another contiguous range, this time after non-contiguous ones
        assert_eq!(
            chunks[3].indices,
            UInt32Array::from_iter_values([3002, 3003])
        );
        assert!(chunks[3].is_contiguous);

        let actual = decoder.take(&indices).await.unwrap();
        assert_eq!(
            actual.as_ref(),
            &StringArray::from_iter_values(indices.values().iter().map(|v| format!("string-{v}")))
        );
    }

    #[tokio::test]
    async fn test_write_slice() {
        let path = TempStdFile::default();
        let data = StringArray::from_iter_values((0..100).map(|v| format!("abcdef-{v:#03}")));

        let mut object_writer = tokio::fs::File::create(&path).await.unwrap();
        let mut encoder = BinaryEncoder::new(&mut object_writer);
        for i in 0..10 {
            let pos = encoder.encode(&[&data.slice(i * 10, 10)]).await.unwrap();
            assert_eq!(pos, (i * (8 * 11) /* offset array */ + (i + 1) * (10 * 10)));
        }
    }

    #[tokio::test]
    async fn test_write_binary_with_nulls() {
        let data = BinaryArray::from_iter((0..60000).map(|v| {
            if v % 4 != 0 {
                Some::<&[u8]>(b"abcdefgh")
            } else {
                None
            }
        }));
        let path = TempStdFile::default();

        let pos = {
            let mut object_writer = tokio::fs::File::create(&path).await.unwrap();

            // Write some garbage to reset "tell()".
            object_writer.write_all(b"1234").await.unwrap();
            let mut encoder = BinaryEncoder::new(&mut object_writer);

            // let arrs = arr.iter().map(|a| a as &dyn Array).collect::<Vec<_>>();
            let pos = encoder.encode(&[&data]).await.unwrap();
            AsyncWriteExt::shutdown(&mut object_writer).await.unwrap();
            pos
        };

        let reader = LocalObjectReader::open_local_path(&path, 1024, None)
            .await
            .unwrap();
        let decoder = BinaryDecoder::<BinaryType>::new(reader.as_ref(), pos, data.len(), true);
        let idx = UInt32Array::from(vec![0_u32, 5_u32, 59996_u32]);
        let actual = decoder.take(&idx).await.unwrap();
        let values: Vec<Option<&[u8]>> = vec![None, Some(b"abcdefgh"), None];
        assert_eq!(actual.as_binary::<i32>(), &BinaryArray::from(values));
    }
}
