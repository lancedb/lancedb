// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::{ops::Range, sync::Arc};

use arrow_buffer::BooleanBufferBuilder;
use bytes::Bytes;

use futures::{FutureExt, future::BoxFuture};
use lance_core::Result;
use log::trace;

use crate::{
    EncodingsIo,
    buffer::LanceBuffer,
    data::{BlockInfo, DataBlock, FixedWidthDataBlock},
    decoder::{PageScheduler, PrimitivePageDecoder},
};

/// A physical scheduler for bitmap buffers encoded densely as 1 bit per value
/// with bit-endianness(e.g. what Arrow uses for validity bitmaps and boolean arrays)
///
/// This decoder decodes from one buffer of disk data into one buffer of memory data
#[derive(Debug, Clone, Copy)]
pub struct DenseBitmapScheduler {
    buffer_offset: u64,
}

impl DenseBitmapScheduler {
    pub fn new(buffer_offset: u64) -> Self {
        Self { buffer_offset }
    }
}

impl PageScheduler for DenseBitmapScheduler {
    fn schedule_ranges(
        &self,
        ranges: &[Range<u64>],
        scheduler: &Arc<dyn EncodingsIo>,
        top_level_row: u64,
    ) -> BoxFuture<'static, Result<Box<dyn PrimitivePageDecoder>>> {
        let mut min = u64::MAX;
        let mut max = 0;
        let chunk_reqs = ranges
            .iter()
            .map(|range| {
                debug_assert_ne!(range.start, range.end);
                let start = self.buffer_offset + range.start / 8;
                let bit_offset = range.start % 8;
                let end = self.buffer_offset + range.end.div_ceil(8);
                let byte_range = start..end;
                min = min.min(start);
                max = max.max(end);
                (byte_range, bit_offset, range.end - range.start)
            })
            .collect::<Vec<_>>();

        let byte_ranges = chunk_reqs
            .iter()
            .map(|(range, _, _)| range.clone())
            .collect::<Vec<_>>();
        trace!(
            "Scheduling I/O for {} ranges across byte range {}..{}",
            byte_ranges.len(),
            min,
            max
        );
        let bytes = scheduler.submit_request(byte_ranges, top_level_row);

        async move {
            let bytes = bytes.await?;
            let chunks = bytes
                .into_iter()
                .zip(chunk_reqs)
                .map(|(bytes, (_, bit_offset, length))| BitmapData {
                    data: bytes,
                    bit_offset,
                    length,
                })
                .collect::<Vec<_>>();
            Ok(Box::new(BitmapDecoder { chunks }) as Box<dyn PrimitivePageDecoder>)
        }
        .boxed()
    }
}

struct BitmapData {
    data: Bytes,
    bit_offset: u64,
    length: u64,
}

struct BitmapDecoder {
    chunks: Vec<BitmapData>,
}

impl PrimitivePageDecoder for BitmapDecoder {
    fn decode(&self, rows_to_skip: u64, num_rows: u64) -> Result<DataBlock> {
        let mut rows_to_skip = rows_to_skip;
        let mut dest_builder = BooleanBufferBuilder::new(num_rows as usize);

        let mut rows_remaining = num_rows;
        for chunk in &self.chunks {
            if chunk.length <= rows_to_skip {
                rows_to_skip -= chunk.length;
            } else {
                let start = rows_to_skip + chunk.bit_offset;
                let num_vals_to_take = rows_remaining.min(chunk.length - rows_to_skip);
                let end = start + num_vals_to_take;
                dest_builder.append_packed_range(start as usize..end as usize, &chunk.data);
                rows_to_skip = 0;
                rows_remaining -= num_vals_to_take;
            }
        }

        let bool_buffer = dest_builder.finish().into_inner();
        Ok(DataBlock::FixedWidth(FixedWidthDataBlock {
            data: LanceBuffer::from(bool_buffer),
            bits_per_value: 1,
            num_values: num_rows,
            block_info: BlockInfo::new(),
        }))
    }
}

#[cfg(test)]
mod tests {

    use arrow_array::BooleanArray;
    use arrow_schema::{DataType, Field};
    use bytes::Bytes;
    use std::{collections::HashMap, sync::Arc};

    use crate::array_encoding::physical::bitmap::BitmapData;
    use crate::data::{DataBlock, FixedWidthDataBlock};
    use crate::decoder::PrimitivePageDecoder;
    use crate::testing::{TestCases, check_basic_random, check_round_trip_encoding_of_data};

    use super::BitmapDecoder;

    #[test_log::test(tokio::test)]
    async fn test_bitmap_boolean() {
        let field = Field::new("", DataType::Boolean, false);
        check_basic_random(field).await;
    }

    #[test_log::test(tokio::test)]
    async fn test_fsl_bitmap_boolean() {
        let field = Field::new("", DataType::Boolean, true);
        let field = Field::new("", DataType::FixedSizeList(Arc::new(field), 3), true);
        check_basic_random(field).await;
    }

    #[test_log::test(tokio::test)]
    async fn test_simple_boolean() {
        let array = BooleanArray::from(vec![
            Some(false),
            Some(true),
            None,
            Some(false),
            Some(true),
            None,
            Some(false),
            None,
            None,
        ]);

        let test_cases = TestCases::default()
            .with_range(0..2)
            .with_range(0..3)
            .with_range(1..9)
            .with_indices(vec![0, 1, 3, 4]);
        check_round_trip_encoding_of_data(vec![Arc::new(array)], &test_cases, HashMap::default())
            .await;
    }

    #[test_log::test(tokio::test)]
    async fn test_tiny_boolean() {
        // Test case for a tiny boolean array that is technically smaller than 1 byte
        let array = BooleanArray::from(vec![Some(false), Some(true), None]);

        let test_cases = TestCases::default()
            .with_range(0..1)
            .with_range(1..3)
            .with_indices(vec![0, 2]);
        check_round_trip_encoding_of_data(vec![Arc::new(array)], &test_cases, HashMap::default())
            .await;
    }

    #[test]
    fn test_bitmap_decoder_edge_cases() {
        // Regression for a case where the row skip and the bit offset
        // require us to read from the second Bytes instead of the first
        let decoder = BitmapDecoder {
            chunks: vec![
                BitmapData {
                    data: Bytes::from_static(&[0b11111111]),
                    bit_offset: 4,
                    length: 4,
                },
                BitmapData {
                    data: Bytes::from_static(&[0b00000000]),
                    bit_offset: 4,
                    length: 4,
                },
            ],
        };

        // Read from first and second chunk
        let result = decoder.decode(2, 4).unwrap();
        let DataBlock::FixedWidth(FixedWidthDataBlock { data, .. }) = result else {
            panic!("expected fixed width data block");
        };
        assert_eq!(data.as_ref(), &[0b00000011]);

        // Read from second chunk
        let result = decoder.decode(5, 1).unwrap();
        let DataBlock::FixedWidth(FixedWidthDataBlock { data, .. }) = result else {
            panic!("expected fixed width data block");
        };
        assert_eq!(data.as_ref(), &[0b00000000]);
    }
}
