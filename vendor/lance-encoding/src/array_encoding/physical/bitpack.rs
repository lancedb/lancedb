// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::sync::Arc;

use arrow_buffer::bit_util::ceil;
use bytes::Bytes;
use futures::future::{BoxFuture, FutureExt};
use log::trace;

use lance_bitpacking::BitPacking;
use lance_core::{Error, Result};

use crate::buffer::LanceBuffer;
use crate::data::BlockInfo;
use crate::data::{DataBlock, FixedWidthDataBlock};
use crate::decoder::{PageScheduler, PrimitivePageDecoder};
use bytemuck::cast_slice;

const LOG_ELEMS_PER_CHUNK: u8 = 10;
const ELEMS_PER_CHUNK: u64 = 1 << LOG_ELEMS_PER_CHUNK;

#[derive(Debug)]
pub struct BitpackedForNonNegScheduler {
    compressed_bit_width: u64,
    uncompressed_bits_per_value: u64,
    buffer_offset: u64,
}

impl BitpackedForNonNegScheduler {
    pub fn new(
        compressed_bit_width: u64,
        uncompressed_bits_per_value: u64,
        buffer_offset: u64,
    ) -> Self {
        Self {
            compressed_bit_width,
            uncompressed_bits_per_value,
            buffer_offset,
        }
    }

    fn locate_chunk_start(&self, relative_row_num: u64) -> u64 {
        let chunk_size = ELEMS_PER_CHUNK * self.compressed_bit_width / 8;
        self.buffer_offset + (relative_row_num / ELEMS_PER_CHUNK * chunk_size)
    }

    fn locate_chunk_end(&self, relative_row_num: u64) -> u64 {
        let chunk_size = ELEMS_PER_CHUNK * self.compressed_bit_width / 8;
        self.buffer_offset + (relative_row_num / ELEMS_PER_CHUNK * chunk_size) + chunk_size
    }
}

impl PageScheduler for BitpackedForNonNegScheduler {
    fn schedule_ranges(
        &self,
        ranges: &[std::ops::Range<u64>],
        scheduler: &Arc<dyn crate::EncodingsIo>,
        top_level_row: u64,
    ) -> BoxFuture<'static, Result<Box<dyn PrimitivePageDecoder>>> {
        assert!(!ranges.is_empty());

        let mut byte_ranges = vec![];

        // map one bytes to multiple ranges, one bytes has at least one range corresponding to it
        let mut bytes_idx_to_range_indices = vec![];
        let first_byte_range = std::ops::Range {
            start: self.locate_chunk_start(ranges[0].start),
            end: self.locate_chunk_end(ranges[0].end - 1),
        }; // the ranges are half-open
        byte_ranges.push(first_byte_range);
        bytes_idx_to_range_indices.push(vec![ranges[0].clone()]);

        for (i, range) in ranges.iter().enumerate().skip(1) {
            let this_start = self.locate_chunk_start(range.start);
            let this_end = self.locate_chunk_end(range.end - 1);

            // when the current range start is in the same chunk as the previous range's end, we colaesce this two bytes ranges
            // when the current range start is not in the same chunk as the previous range's end, we create a new bytes range
            if this_start == self.locate_chunk_start(ranges[i - 1].end - 1) {
                byte_ranges.last_mut().unwrap().end = this_end;
                bytes_idx_to_range_indices
                    .last_mut()
                    .unwrap()
                    .push(range.clone());
            } else {
                byte_ranges.push(this_start..this_end);
                bytes_idx_to_range_indices.push(vec![range.clone()]);
            }
        }

        trace!(
            "Scheduling I/O for {} ranges spread across byte range {}..{}",
            byte_ranges.len(),
            byte_ranges[0].start,
            byte_ranges.last().unwrap().end
        );

        let bytes = scheduler.submit_request(byte_ranges.clone(), top_level_row);

        // copy the necessary data from `self` to move into the async block
        let compressed_bit_width = self.compressed_bit_width;
        let uncompressed_bits_per_value = self.uncompressed_bits_per_value;
        let num_rows = ranges.iter().map(|range| range.end - range.start).sum();

        async move {
            let bytes = bytes.await?;
            let decompressed_output = bitpacked_for_non_neg_decode(
                compressed_bit_width,
                uncompressed_bits_per_value,
                &bytes,
                &bytes_idx_to_range_indices,
                num_rows,
            );
            Ok(Box::new(BitpackedForNonNegPageDecoder {
                uncompressed_bits_per_value,
                decompressed_buf: decompressed_output,
            }) as Box<dyn PrimitivePageDecoder>)
        }
        .boxed()
    }
}

#[derive(Debug)]
struct BitpackedForNonNegPageDecoder {
    // number of bits in the uncompressed value. E.g. this will be 32 for DataType::UInt32
    uncompressed_bits_per_value: u64,

    decompressed_buf: LanceBuffer,
}

impl PrimitivePageDecoder for BitpackedForNonNegPageDecoder {
    fn decode(&self, rows_to_skip: u64, num_rows: u64) -> Result<DataBlock> {
        if ![8, 16, 32, 64].contains(&self.uncompressed_bits_per_value) {
            return Err(Error::invalid_input_source("BitpackedForNonNegPageDecoder should only has uncompressed_bits_per_value of 8, 16, 32, or 64".into()));
        }

        let elem_size_in_bytes = self.uncompressed_bits_per_value / 8;

        Ok(DataBlock::FixedWidth(FixedWidthDataBlock {
            data: self.decompressed_buf.slice_with_length(
                (rows_to_skip * elem_size_in_bytes) as usize,
                (num_rows * elem_size_in_bytes) as usize,
            ),
            bits_per_value: self.uncompressed_bits_per_value,
            num_values: num_rows,
            block_info: BlockInfo::new(),
        }))
    }
}

macro_rules! bitpacked_decode {
    ($uncompressed_type:ty, $compressed_bit_width:expr, $data:expr, $bytes_idx_to_range_indices:expr, $num_rows:expr) => {{
        let mut decompressed: Vec<$uncompressed_type> = Vec::with_capacity($num_rows as usize);
        let packed_chunk_size_in_byte: usize = (ELEMS_PER_CHUNK * $compressed_bit_width) as usize / 8;
        let mut decompress_chunk_buf = vec![0 as $uncompressed_type; ELEMS_PER_CHUNK as usize];

        for (i, bytes) in $data.iter().enumerate() {
            let mut ranges_idx = 0;
            let mut curr_range_start = $bytes_idx_to_range_indices[i][0].start;
            let mut chunk_num = 0;

            while chunk_num * packed_chunk_size_in_byte < bytes.len() {
                // Copy for memory alignment
                // TODO: This copy should not be needed
                let chunk_in_u8: Vec<u8> = bytes[chunk_num * packed_chunk_size_in_byte..]
                    [..packed_chunk_size_in_byte]
                    .to_vec();
                chunk_num += 1;
                let chunk = cast_slice(&chunk_in_u8);
                unsafe {
                    BitPacking::unchecked_unpack(
                        $compressed_bit_width as usize,
                        chunk,
                        &mut decompress_chunk_buf,
                    );
                }

                loop {
                    // Case 1: All the elements after (curr_range_start % ELEMS_PER_CHUNK) inside this chunk are needed.
                    let elems_after_curr_range_start_in_this_chunk =
                        ELEMS_PER_CHUNK - curr_range_start % ELEMS_PER_CHUNK;
                    if curr_range_start + elems_after_curr_range_start_in_this_chunk
                        <= $bytes_idx_to_range_indices[i][ranges_idx].end
                    {
                        decompressed.extend_from_slice(
                            &decompress_chunk_buf[(curr_range_start % ELEMS_PER_CHUNK) as usize..],
                        );
                        curr_range_start += elems_after_curr_range_start_in_this_chunk;
                        break;
                    } else {
                        // Case 2: Only part of the elements after (curr_range_start % ELEMS_PER_CHUNK) inside this chunk are needed.
                        let elems_this_range_needed_in_this_chunk =
                            ($bytes_idx_to_range_indices[i][ranges_idx].end - curr_range_start)
                                .min(ELEMS_PER_CHUNK - curr_range_start % ELEMS_PER_CHUNK);
                        decompressed.extend_from_slice(
                            &decompress_chunk_buf[(curr_range_start % ELEMS_PER_CHUNK) as usize..]
                                [..elems_this_range_needed_in_this_chunk as usize],
                        );
                        if curr_range_start + elems_this_range_needed_in_this_chunk
                            == $bytes_idx_to_range_indices[i][ranges_idx].end
                        {
                            ranges_idx += 1;
                            if ranges_idx == $bytes_idx_to_range_indices[i].len() {
                                break;
                            }
                            curr_range_start = $bytes_idx_to_range_indices[i][ranges_idx].start;
                        } else {
                            curr_range_start += elems_this_range_needed_in_this_chunk;
                        }
                    }
                }
            }
        }

        LanceBuffer::reinterpret_vec(decompressed)
    }};
}

fn bitpacked_for_non_neg_decode(
    compressed_bit_width: u64,
    uncompressed_bits_per_value: u64,
    data: &[Bytes],
    bytes_idx_to_range_indices: &[Vec<std::ops::Range<u64>>],
    num_rows: u64,
) -> LanceBuffer {
    match uncompressed_bits_per_value {
        8 => bitpacked_decode!(
            u8,
            compressed_bit_width,
            data,
            bytes_idx_to_range_indices,
            num_rows
        ),
        16 => bitpacked_decode!(
            u16,
            compressed_bit_width,
            data,
            bytes_idx_to_range_indices,
            num_rows
        ),
        32 => bitpacked_decode!(
            u32,
            compressed_bit_width,
            data,
            bytes_idx_to_range_indices,
            num_rows
        ),
        64 => bitpacked_decode!(
            u64,
            compressed_bit_width,
            data,
            bytes_idx_to_range_indices,
            num_rows
        ),
        _ => unreachable!(
            "bitpacked_for_non_neg_decode only supports 8, 16, 32, 64 uncompressed_bits_per_value"
        ),
    }
}

// A physical scheduler for bitpacked buffers
#[derive(Debug, Clone, Copy)]
pub struct BitpackedScheduler {
    bits_per_value: u64,
    uncompressed_bits_per_value: u64,
    buffer_offset: u64,
    signed: bool,
}

impl BitpackedScheduler {
    pub fn new(
        bits_per_value: u64,
        uncompressed_bits_per_value: u64,
        buffer_offset: u64,
        signed: bool,
    ) -> Self {
        Self {
            bits_per_value,
            uncompressed_bits_per_value,
            buffer_offset,
            signed,
        }
    }
}

impl PageScheduler for BitpackedScheduler {
    fn schedule_ranges(
        &self,
        ranges: &[std::ops::Range<u64>],
        scheduler: &Arc<dyn crate::EncodingsIo>,
        top_level_row: u64,
    ) -> BoxFuture<'static, Result<Box<dyn PrimitivePageDecoder>>> {
        let mut min = u64::MAX;
        let mut max = 0;

        let mut buffer_bit_start_offsets: Vec<u8> = vec![];
        let mut buffer_bit_end_offsets: Vec<Option<u8>> = vec![];
        let byte_ranges = ranges
            .iter()
            .map(|range| {
                let start_byte_offset = range.start * self.bits_per_value / 8;
                let mut end_byte_offset = range.end * self.bits_per_value / 8;
                if !(range.end * self.bits_per_value).is_multiple_of(8) {
                    // If the end of the range is not byte-aligned, we need to read one more byte
                    end_byte_offset += 1;

                    let end_bit_offset = range.end * self.bits_per_value % 8;
                    buffer_bit_end_offsets.push(Some(end_bit_offset as u8));
                } else {
                    buffer_bit_end_offsets.push(None);
                }

                let start_bit_offset = range.start * self.bits_per_value % 8;
                buffer_bit_start_offsets.push(start_bit_offset as u8);

                let start = self.buffer_offset + start_byte_offset;
                let end = self.buffer_offset + end_byte_offset;
                min = min.min(start);
                max = max.max(end);

                start..end
            })
            .collect::<Vec<_>>();

        trace!(
            "Scheduling I/O for {} ranges spread across byte range {}..{}",
            byte_ranges.len(),
            min,
            max
        );

        let bytes = scheduler.submit_request(byte_ranges, top_level_row);

        let bits_per_value = self.bits_per_value;
        let uncompressed_bits_per_value = self.uncompressed_bits_per_value;
        let signed = self.signed;
        async move {
            let bytes = bytes.await?;
            Ok(Box::new(BitpackedPageDecoder {
                buffer_bit_start_offsets,
                buffer_bit_end_offsets,
                bits_per_value,
                uncompressed_bits_per_value,
                signed,
                data: bytes,
            }) as Box<dyn PrimitivePageDecoder>)
        }
        .boxed()
    }
}

#[derive(Debug)]
struct BitpackedPageDecoder {
    // bit offsets of the first value within each buffer
    buffer_bit_start_offsets: Vec<u8>,

    // bit offsets of the last value within each buffer. e.g. if there was a buffer
    // with 2 values, packed into 5 bits, this would be [Some(3)], indicating that
    // the bits from the 3rd->8th bit in the last byte shouldn't be decoded.
    buffer_bit_end_offsets: Vec<Option<u8>>,

    // the number of bits used to represent a compressed value. E.g. if the max value
    // in the page was 7 (0b111), then this will be 3
    bits_per_value: u64,

    // number of bits in the uncompressed value. E.g. this will be 32 for u32
    uncompressed_bits_per_value: u64,

    // whether or not to use the msb as a sign bit during decoding
    signed: bool,

    data: Vec<Bytes>,
}

impl PrimitivePageDecoder for BitpackedPageDecoder {
    fn decode(&self, rows_to_skip: u64, num_rows: u64) -> Result<DataBlock> {
        let num_bytes = self.uncompressed_bits_per_value / 8 * num_rows;
        let mut dest = vec![0; num_bytes as usize];

        // current maximum supported bits per value = 64
        debug_assert!(self.bits_per_value <= 64);

        let mut rows_to_skip = rows_to_skip;
        let mut rows_taken = 0;
        let byte_len = self.uncompressed_bits_per_value / 8;
        let mut dst_idx = 0; // index for current byte being written to destination buffer

        // create bit mask for source bits
        let mask = u64::MAX >> (64 - self.bits_per_value);

        for i in 0..self.data.len() {
            let src = &self.data[i];
            let (mut src_idx, mut src_offset) = match compute_start_offset(
                rows_to_skip,
                src.len(),
                self.bits_per_value,
                self.buffer_bit_start_offsets[i],
                self.buffer_bit_end_offsets[i],
            ) {
                StartOffset::SkipFull(rows_to_skip_here) => {
                    rows_to_skip -= rows_to_skip_here;
                    continue;
                }
                StartOffset::SkipSome(buffer_start_offset) => (
                    buffer_start_offset.index,
                    buffer_start_offset.bit_offset as u64,
                ),
            };

            while src_idx < src.len() && rows_taken < num_rows {
                rows_taken += 1;
                let mut curr_mask = mask; // copy mask

                // current source byte being written to destination
                let mut curr_src = src[src_idx] & (curr_mask << src_offset) as u8;

                // how many bits from the current source value have been written to destination
                let mut src_bits_written = 0;

                // the offset within the current destination byte to write to
                let mut dst_offset = 0;

                let is_negative = is_encoded_item_negative(
                    src,
                    src_idx,
                    src_offset,
                    self.bits_per_value as usize,
                );

                while src_bits_written < self.bits_per_value {
                    // write bits from current source byte into destination
                    dest[dst_idx] += (curr_src >> src_offset) << dst_offset;
                    let bits_written = (self.bits_per_value - src_bits_written)
                        .min(8 - src_offset)
                        .min(8 - dst_offset);
                    src_bits_written += bits_written;
                    dst_offset += bits_written;
                    src_offset += bits_written;
                    curr_mask >>= bits_written;

                    if dst_offset == 8 {
                        dst_idx += 1;
                        dst_offset = 0;
                    }

                    if src_offset == 8 {
                        src_idx += 1;
                        src_offset = 0;
                        if src_idx == src.len() {
                            break;
                        }
                        curr_src = src[src_idx] & curr_mask as u8;
                    }
                }

                // if the type is signed, need to pad out the rest of the byte with 1s
                let mut negative_padded_current_byte = false;
                if self.signed && is_negative && dst_offset > 0 {
                    negative_padded_current_byte = true;
                    while dst_offset < 8 {
                        dest[dst_idx] |= 1 << dst_offset;
                        dst_offset += 1;
                    }
                }

                // advance destination offset to the next location
                // note that we don't need to do this if we wrote the full number of bits
                // because source index would have been advanced by the inner loop above
                if self.uncompressed_bits_per_value != self.bits_per_value {
                    let partial_bytes_written = ceil(self.bits_per_value as usize, 8);

                    // we also want to move one location to the next location in destination,
                    // unless we wrote something byte-aligned in which case the logic above
                    // would have already advanced dst_idx
                    let mut to_next_byte = 1;
                    if self.bits_per_value.is_multiple_of(8) {
                        to_next_byte = 0;
                    }
                    let next_dst_idx =
                        dst_idx + byte_len as usize - partial_bytes_written + to_next_byte;

                    // pad remaining bytes with 1 for negative signed numbers
                    if self.signed && is_negative {
                        if !negative_padded_current_byte {
                            dest[dst_idx] = 0xFF;
                        }
                        for i in dest.iter_mut().take(next_dst_idx).skip(dst_idx + 1) {
                            *i = 0xFF;
                        }
                    }

                    dst_idx = next_dst_idx;
                }

                // If we've reached the last byte, there may be some extra bits from the
                // next value outside the range. We don't want to be taking those.
                if let Some(buffer_bit_end_offset) = self.buffer_bit_end_offsets[i]
                    && src_idx == src.len() - 1
                    && src_offset >= buffer_bit_end_offset as u64
                {
                    break;
                }
            }
        }

        Ok(DataBlock::FixedWidth(FixedWidthDataBlock {
            data: LanceBuffer::from(dest),
            bits_per_value: self.uncompressed_bits_per_value,
            num_values: num_rows,
            block_info: BlockInfo::new(),
        }))
    }
}

fn is_encoded_item_negative(src: &Bytes, src_idx: usize, src_offset: u64, num_bits: usize) -> bool {
    let mut last_byte_idx = src_idx + ((src_offset as usize + num_bits) / 8);
    let shift_amount = (src_offset as usize + num_bits) % 8;
    let shift_amount = if shift_amount == 0 {
        last_byte_idx -= 1;
        7
    } else {
        shift_amount - 1
    };
    let last_byte = src[last_byte_idx];
    let sign_bit_mask = 1 << shift_amount;
    let sign_bit = last_byte & sign_bit_mask;

    sign_bit > 0
}

#[derive(Debug, PartialEq)]
struct BufferStartOffset {
    index: usize,
    bit_offset: u8,
}

#[derive(Debug, PartialEq)]
enum StartOffset {
    // skip the full buffer. The value is how many rows are skipped
    // by skipping the full buffer (e.g., # rows in buffer)
    SkipFull(u64),

    // skip to some start offset in the buffer
    SkipSome(BufferStartOffset),
}

/// compute how far ahead in this buffer should we skip ahead and start reading
///
/// * `rows_to_skip` - how many rows to skip
/// * `buffer_len` - length buf buffer (in bytes)
/// * `bits_per_value` - number of bits used to represent a single bitpacked value
/// * `buffer_start_bit_offset` - offset of the start of the first value within the
///   buffer's  first byte
/// * `buffer_end_bit_offset` - end bit of the last value within the buffer. Can be
///   `None` if the end of the last value is byte aligned with end of buffer.
fn compute_start_offset(
    rows_to_skip: u64,
    buffer_len: usize,
    bits_per_value: u64,
    buffer_start_bit_offset: u8,
    buffer_end_bit_offset: Option<u8>,
) -> StartOffset {
    let rows_in_buffer = rows_in_buffer(
        buffer_len,
        bits_per_value,
        buffer_start_bit_offset,
        buffer_end_bit_offset,
    );
    if rows_to_skip >= rows_in_buffer {
        return StartOffset::SkipFull(rows_in_buffer);
    }

    let start_bit = rows_to_skip * bits_per_value + buffer_start_bit_offset as u64;
    let start_byte = start_bit / 8;

    StartOffset::SkipSome(BufferStartOffset {
        index: start_byte as usize,
        bit_offset: (start_bit % 8) as u8,
    })
}

/// calculates the number of rows in a buffer
fn rows_in_buffer(
    buffer_len: usize,
    bits_per_value: u64,
    buffer_start_bit_offset: u8,
    buffer_end_bit_offset: Option<u8>,
) -> u64 {
    let mut bits_in_buffer = (buffer_len * 8) as u64 - buffer_start_bit_offset as u64;

    // if the end of the last value of the buffer isn't byte aligned, subtract the
    // end offset from the total number of bits in buffer
    if let Some(buffer_end_bit_offset) = buffer_end_bit_offset {
        bits_in_buffer -= (8 - buffer_end_bit_offset) as u64;
    }

    bits_in_buffer / bits_per_value
}

#[cfg(test)]
pub mod test {
    use super::*;
    use crate::BufferScheduler;

    use arrow_buffer::ArrowNativeType;

    fn fixed_width_values<T: ArrowNativeType>(block: DataBlock) -> Vec<T> {
        let DataBlock::FixedWidth(FixedWidthDataBlock { data, .. }) = block else {
            panic!("expected fixed-width data");
        };
        data.borrow_to_typed_slice::<T>().to_vec()
    }

    #[test]
    fn test_rows_in_buffer() {
        let test_cases = vec![
            (5usize, 5u64, 0u8, None, 8u64),
            (2, 3, 0, Some(5), 4),
            (2, 3, 7, Some(6), 2),
        ];

        for (
            buffer_len,
            bits_per_value,
            buffer_start_bit_offset,
            buffer_end_bit_offset,
            expected,
        ) in test_cases
        {
            let result = rows_in_buffer(
                buffer_len,
                bits_per_value,
                buffer_start_bit_offset,
                buffer_end_bit_offset,
            );
            assert_eq!(expected, result);
        }
    }

    #[test]
    fn test_compute_start_offset() {
        let result = compute_start_offset(0, 5, 5, 0, None);
        assert_eq!(
            StartOffset::SkipSome(BufferStartOffset {
                index: 0,
                bit_offset: 0
            }),
            result
        );

        let result = compute_start_offset(10, 5, 5, 0, None);
        assert_eq!(StartOffset::SkipFull(8), result);
    }

    #[test_log::test(tokio::test)]
    async fn test_bitpacked_scheduler_non_byte_aligned_ranges() {
        // Legacy 5-bit LSB-first encoding of values 1..=10, with a two-byte prefix.
        let data = Bytes::from_static(&[0xFA, 0xCE, 0x41, 0x0C, 0x52, 0xCC, 0x41, 0x49, 0x01]);
        let io: Arc<dyn crate::EncodingsIo> = Arc::new(BufferScheduler::new(data));
        let scheduler = BitpackedScheduler::new(5, 16, 2, false);

        let decoder = scheduler
            .schedule_ranges(&[1..3, 5..9], &io, 0)
            .await
            .unwrap();
        let decoded = decoder.decode(3, 3).unwrap();

        // The scheduled rows are [2, 3, 6, 7, 8, 9]. Skipping across the first
        // Bytes response must begin in the middle of the second response.
        assert_eq!(fixed_width_values::<u16>(decoded), vec![7, 8, 9]);
    }

    #[test_log::test(tokio::test)]
    async fn test_bitpacked_scheduler_signed_byte_aligned() {
        // Legacy 16-bit little-endian encoding of [513, -1, 4660, -32768].
        let data = Bytes::from_static(&[0x01, 0x02, 0xFF, 0xFF, 0x34, 0x12, 0x00, 0x80]);
        let io: Arc<dyn crate::EncodingsIo> = Arc::new(BufferScheduler::new(data));
        let scheduler = BitpackedScheduler::new(16, 32, 0, true);

        let decoder = scheduler.schedule_ranges(&[1..4], &io, 0).await.unwrap();
        let decoded = decoder.decode(0, 3).unwrap();

        assert_eq!(fixed_width_values::<i32>(decoded), vec![-1, 4660, -32768]);
    }

    #[test_log::test(tokio::test)]
    async fn test_bitpacked_for_non_negative_scheduler_chunk_boundary() {
        const BIT_WIDTH: usize = 7;
        const NUM_CHUNKS: usize = 2;
        const WORDS_PER_CHUNK: usize = ELEMS_PER_CHUNK as usize * BIT_WIDTH / u32::BITS as usize;

        let values = (0..ELEMS_PER_CHUNK as usize * NUM_CHUNKS)
            .map(|index| ((index * 13 + 7) % 127) as u32)
            .collect::<Vec<_>>();
        let mut packed = vec![0_u32; WORDS_PER_CHUNK * NUM_CHUNKS];
        for chunk_index in 0..NUM_CHUNKS {
            let value_start = chunk_index * ELEMS_PER_CHUNK as usize;
            let word_start = chunk_index * WORDS_PER_CHUNK;
            // SAFETY: Both slices have the exact input and output lengths required
            // for one 1,024-value chunk at this bit width.
            unsafe {
                BitPacking::unchecked_pack(
                    BIT_WIDTH,
                    &values[value_start..value_start + ELEMS_PER_CHUNK as usize],
                    &mut packed[word_start..word_start + WORDS_PER_CHUNK],
                );
            }
        }

        let mut data = vec![0xFA, 0xCE, 0x01];
        data.extend_from_slice(cast_slice(&packed));
        let io: Arc<dyn crate::EncodingsIo> = Arc::new(BufferScheduler::new(Bytes::from(data)));
        let scheduler = BitpackedForNonNegScheduler::new(BIT_WIDTH as u64, 32, 3);

        let decoder = scheduler
            .schedule_ranges(&[1020..1028, 1530..1533], &io, 0)
            .await
            .unwrap();
        let decoded = decoder.decode(2, 7).unwrap();
        let expected = (1022..1028)
            .chain(1530..1531)
            .map(|index| values[index])
            .collect::<Vec<_>>();

        assert_eq!(fixed_width_values::<u32>(decoded), expected);
    }
}
