// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Basic encodings for variable width data
//!
//! These are not compression but represent the "leaf" encodings for variable length data
//! where we simply match the data with the rules of the structural encoding.
//!
//! These encodings are transparent since we aren't actually doing any compression.  No information
//! is needed in the encoding description.

use arrow_array::OffsetSizeTrait;
use byteorder::{ByteOrder, LittleEndian};
use core::panic;

use crate::compression::{
    BlockCompressor, BlockDecompressor, MiniBlockDecompressor, VariablePerValueDecompressor,
};

use crate::buffer::LanceBuffer;
use crate::data::{BlockInfo, DataBlock, VariableWidthBlock};
use crate::encodings::logical::primitive::fullzip::{PerValueCompressor, PerValueDataBlock};
use crate::encodings::logical::primitive::miniblock::{
    MAX_MINIBLOCK_VALUES, MiniBlockChunk, MiniBlockCompressed, MiniBlockCompressionContext,
    MiniBlockCompressor,
};
use crate::format::pb21::CompressiveEncoding;
use crate::format::pb21::compressive_encoding::Compression;
use crate::format::{ProtobufUtils21, pb21};

use lance_core::utils::bit::pad_bytes_to;
use lance_core::{Error, Result};

#[derive(Debug)]
pub struct BinaryMiniBlockEncoder {
    minichunk_size: i64,
}

impl Default for BinaryMiniBlockEncoder {
    fn default() -> Self {
        Self {
            minichunk_size: *AIM_MINICHUNK_SIZE,
        }
    }
}

const DEFAULT_AIM_MINICHUNK_SIZE: i64 = 4 * 1024;

pub static AIM_MINICHUNK_SIZE: std::sync::LazyLock<i64> = std::sync::LazyLock::new(|| {
    std::env::var("LANCE_BINARY_MINIBLOCK_CHUNK_SIZE")
        .unwrap_or_else(|_| DEFAULT_AIM_MINICHUNK_SIZE.to_string())
        .parse::<i64>()
        .unwrap_or(DEFAULT_AIM_MINICHUNK_SIZE)
});

// Make it to support both u32 and u64
fn chunk_offsets<N: OffsetSizeTrait>(
    offsets: &[N],
    data: &[u8],
    alignment: usize,
    minichunk_size: i64,
) -> (Vec<LanceBuffer>, Vec<MiniBlockChunk>) {
    #[derive(Debug)]
    struct ChunkInfo {
        chunk_start_offset_in_orig_idx: usize,
        chunk_last_offset_in_orig_idx: usize,
        // the bytes in every chunk starts at `chunk.bytes_start_offset`
        bytes_start_offset: usize,
        // every chunk is padded to 8 bytes.
        // we need to interpret every chunk as &[u32] so we need it to padded at least to 4 bytes,
        // this field can actually be eliminated and I can use `num_bytes` in `MiniBlockChunk` to compute
        // the `output_total_bytes`.
        padded_chunk_size: usize,
    }

    let byte_width: usize = N::get_byte_width();
    let mut chunks_info = vec![];
    let mut chunks = vec![];
    let mut last_offset_in_orig_idx = 0;
    loop {
        let this_last_offset_in_orig_idx =
            search_next_offset_idx(offsets, last_offset_in_orig_idx, minichunk_size);

        let num_values_in_this_chunk = this_last_offset_in_orig_idx - last_offset_in_orig_idx;
        let chunk_bytes = offsets[this_last_offset_in_orig_idx] - offsets[last_offset_in_orig_idx];
        let this_chunk_size =
            (num_values_in_this_chunk + 1) * byte_width + chunk_bytes.to_usize().unwrap();

        let padded_chunk_size = this_chunk_size.next_multiple_of(alignment);
        debug_assert!(padded_chunk_size > 0);

        let this_chunk_bytes_start_offset = (num_values_in_this_chunk + 1) * byte_width;
        chunks_info.push(ChunkInfo {
            chunk_start_offset_in_orig_idx: last_offset_in_orig_idx,
            chunk_last_offset_in_orig_idx: this_last_offset_in_orig_idx,
            bytes_start_offset: this_chunk_bytes_start_offset,
            padded_chunk_size,
        });
        chunks.push(MiniBlockChunk {
            log_num_values: if this_last_offset_in_orig_idx == offsets.len() - 1 {
                0
            } else {
                num_values_in_this_chunk.trailing_zeros() as u8
            },
            buffer_sizes: vec![padded_chunk_size as u32],
        });
        if this_last_offset_in_orig_idx == offsets.len() - 1 {
            break;
        }
        last_offset_in_orig_idx = this_last_offset_in_orig_idx;
    }

    let output_total_bytes = chunks_info
        .iter()
        .map(|chunk_info| chunk_info.padded_chunk_size)
        .sum::<usize>();

    let mut output: Vec<u8> = Vec::with_capacity(output_total_bytes);

    for chunk in chunks_info {
        let this_chunk_offsets: Vec<N> = offsets
            [chunk.chunk_start_offset_in_orig_idx..=chunk.chunk_last_offset_in_orig_idx]
            .iter()
            .map(|offset| {
                *offset - offsets[chunk.chunk_start_offset_in_orig_idx]
                    + N::from_usize(chunk.bytes_start_offset).unwrap()
            })
            .collect();

        let this_chunk_offsets = LanceBuffer::reinterpret_vec(this_chunk_offsets);
        output.extend_from_slice(&this_chunk_offsets);

        let start_in_orig = offsets[chunk.chunk_start_offset_in_orig_idx]
            .to_usize()
            .unwrap();
        let end_in_orig = offsets[chunk.chunk_last_offset_in_orig_idx]
            .to_usize()
            .unwrap();
        output.extend_from_slice(&data[start_in_orig..end_in_orig]);

        // pad this chunk to make it align to desired bytes.
        const PAD_BYTE: u8 = 72;
        let pad_len = pad_bytes_to(output.len(), alignment);

        // Compare with usize literal to avoid type mismatch with N
        if pad_len > 0_usize {
            output.extend(std::iter::repeat_n(PAD_BYTE, pad_len));
        }
    }
    (vec![LanceBuffer::reinterpret_vec(output)], chunks)
}

// search for the next offset index to cut the values into a chunk.
// this function incrementally peek the number of values in a chunk,
// each time multiplies the number of values by 2.
// It returns the offset_idx in `offsets` that belongs to this chunk.
fn search_next_offset_idx<N: OffsetSizeTrait>(
    offsets: &[N],
    last_offset_idx: usize,
    minichunk_size: i64,
) -> usize {
    // MiniBlockChunk uses `log_num_values == 0` as a sentinel for the final chunk. This means we
    // must avoid creating 1-value chunks except for the final chunk, even if the configured
    // `minichunk_size` is too small to fit more than one value.
    let remaining_values = offsets.len().saturating_sub(last_offset_idx + 1);
    if remaining_values <= 1 {
        return offsets.len() - 1;
    }

    let mut num_values = 2;
    let mut new_num_values = num_values * 2;
    loop {
        if last_offset_idx + new_num_values >= offsets.len() {
            let existing_bytes = offsets[offsets.len() - 1] - offsets[last_offset_idx];
            // existing bytes plus the new offset size
            let new_size = existing_bytes
                + N::from_usize((offsets.len() - last_offset_idx) * N::get_byte_width()).unwrap();
            if new_size.to_i64().unwrap() <= minichunk_size {
                // case 1: can fit the rest of all data into a miniblock
                return offsets.len() - 1;
            } else {
                // case 2: can only fit the last tried `num_values` into a miniblock
                return last_offset_idx + num_values;
            }
        }
        let existing_bytes = offsets[last_offset_idx + new_num_values] - offsets[last_offset_idx];
        let new_size =
            existing_bytes + N::from_usize((new_num_values + 1) * N::get_byte_width()).unwrap();
        if new_size.to_i64().unwrap() <= minichunk_size {
            if new_num_values * 2 > *MAX_MINIBLOCK_VALUES as usize {
                // hit the max number of values limit
                break;
            }
            num_values = new_num_values;
            new_num_values *= 2;
        } else {
            break;
        }
    }
    last_offset_idx + num_values
}

impl BinaryMiniBlockEncoder {
    pub fn new(minichunk_size: Option<i64>) -> Self {
        Self {
            minichunk_size: minichunk_size.unwrap_or(*AIM_MINICHUNK_SIZE),
        }
    }

    // put binary data into chunks, every chunk is less than or equal to `minichunk_size`.
    // In each chunk, offsets are put first then followed by binary bytes data, each chunk is padded to 8 bytes.
    // the offsets in the chunk points to the bytes offset in this chunk.
    fn chunk_data(&self, data: VariableWidthBlock) -> (MiniBlockCompressed, CompressiveEncoding) {
        // TODO: Support compression of offsets
        // TODO: Support general compression of data
        match data.bits_per_offset {
            32 => {
                let offsets = data.offsets.borrow_to_typed_slice::<i32>();
                let (buffers, chunks) =
                    chunk_offsets(offsets.as_ref(), &data.data, 4, self.minichunk_size);
                (
                    MiniBlockCompressed {
                        data: buffers,
                        chunks,
                        num_values: data.num_values,
                    },
                    ProtobufUtils21::variable(ProtobufUtils21::flat(32, None), None),
                )
            }
            64 => {
                let offsets = data.offsets.borrow_to_typed_slice::<i64>();
                let (buffers, chunks) =
                    chunk_offsets(offsets.as_ref(), &data.data, 8, self.minichunk_size);
                (
                    MiniBlockCompressed {
                        data: buffers,
                        chunks,
                        num_values: data.num_values,
                    },
                    ProtobufUtils21::variable(ProtobufUtils21::flat(64, None), None),
                )
            }
            _ => panic!("Unsupported bits_per_offset={}", data.bits_per_offset),
        }
    }
}

impl MiniBlockCompressor for BinaryMiniBlockEncoder {
    fn compress(
        &self,
        _context: MiniBlockCompressionContext,
        data: DataBlock,
    ) -> Result<(MiniBlockCompressed, CompressiveEncoding)> {
        match data {
            DataBlock::VariableWidth(variable_width) => Ok(self.chunk_data(variable_width)),
            _ => Err(Error::invalid_input_source(
                format!(
                    "Cannot compress a data block of type {} with BinaryMiniBlockEncoder",
                    data.name()
                )
                .into(),
            )),
        }
    }
}

#[derive(Debug)]
pub struct BinaryMiniBlockDecompressor {
    bits_per_offset: u8,
}

impl BinaryMiniBlockDecompressor {
    pub fn new(bits_per_offset: u8) -> Self {
        assert!(bits_per_offset == 32 || bits_per_offset == 64);
        Self { bits_per_offset }
    }

    pub fn from_variable(variable: &pb21::Variable) -> Self {
        if let Compression::Flat(flat) = variable
            .offsets
            .as_ref()
            .unwrap()
            .compression
            .as_ref()
            .unwrap()
        {
            Self {
                bits_per_offset: flat.bits_per_value as u8,
            }
        } else {
            panic!("Unsupported offsets compression: {:?}", variable.offsets);
        }
    }
}

/// Cold path: pinpoint why the chunk-relative offsets of a binary mini-block
/// chunk failed validation.
fn chunk_offset_violation_error<T: Copy + Into<u64>>(offsets: &[T], chunk_len: usize) -> Error {
    let mut previous: u64 = offsets[0].into();
    for (position, &offset) in offsets.iter().enumerate().skip(1) {
        let offset: u64 = offset.into();
        if offset < previous {
            return Error::corrupt_file_named(
                "binary mini-block",
                format!(
                    "value offset at position {position} decreases: {offset} < {previous} \
                     (chunk is {chunk_len} bytes)"
                ),
            );
        }
        previous = offset;
    }
    Error::corrupt_file_named(
        "binary mini-block",
        format!("value offset {previous} is out of bounds for a chunk of {chunk_len} bytes"),
    )
}

impl MiniBlockDecompressor for BinaryMiniBlockDecompressor {
    // decompress a MiniBlock of binary data, the num_values must be less than or equal
    // to the number of values this MiniBlock has, BinaryMiniBlock doesn't store `the number of values`
    // it has so assertion can not be done here and the caller of `decompress` must ensure
    // `num_values` <= number of values in the chunk.
    //
    // The chunk-relative value offsets at the front of the chunk come straight
    // from the file and are used to slice the chunk buffer, so corrupt values
    // must surface as a typed error instead of a panic or an out-of-bounds
    // read.  The monotonicity check rides along the existing rebase loop (the
    // `&=` accumulation keeps it branchless) so validation adds no extra pass.
    fn decompress(&self, data: Vec<LanceBuffer>, num_values: u64) -> Result<DataBlock> {
        assert_eq!(data.len(), 1);
        let data = data.into_iter().next().unwrap();

        let bytes_per_offset = self.bits_per_offset as usize / 8;
        if !data.len().is_multiple_of(bytes_per_offset) {
            return Err(Error::corrupt_file_named(
                "binary mini-block",
                format!(
                    "chunk size {} is not a multiple of the {}-byte offset width",
                    data.len(),
                    bytes_per_offset
                ),
            ));
        }
        let num_offsets = (num_values as usize).checked_add(1).ok_or_else(|| {
            Error::corrupt_file_named(
                "binary mini-block",
                format!("cannot decode {num_values} values from a single chunk"),
            )
        })?;
        if data.len() / bytes_per_offset < num_offsets {
            return Err(Error::corrupt_file_named(
                "binary mini-block",
                format!(
                    "chunk of {} bytes holds {} offsets but decoding {} values requires {}",
                    data.len(),
                    data.len() / bytes_per_offset,
                    num_values,
                    num_offsets
                ),
            ));
        }

        // The value region must start past the offsets being decoded, otherwise
        // the offset table itself aliases into the value bytes.  A lower bound
        // (not equality) because a prefix read of the chunk legitimately leaves
        // unrequested offsets between the requested prefix and the values.
        let min_value_region_start = num_offsets * bytes_per_offset;
        let value_region_overlap_error = |first: u64| {
            Error::corrupt_file_named(
                "binary mini-block",
                format!(
                    "value region starts at offset {first} which overlaps the {num_offsets} \
                     requested offsets ({min_value_region_start} bytes)"
                ),
            )
        };

        if self.bits_per_offset == 64 {
            let offsets_buffer = data.borrow_to_typed_slice::<u64>();
            let offsets = &offsets_buffer.as_ref()[..num_offsets];

            let first = offsets[0];
            if first < min_value_region_start as u64 {
                return Err(value_region_overlap_error(first));
            }
            let mut previous = first;
            let mut is_monotonic = true;
            let result_offsets = offsets
                .iter()
                .map(|&offset| {
                    is_monotonic &= previous <= offset;
                    previous = offset;
                    offset.wrapping_sub(first)
                })
                .collect::<Vec<u64>>();
            let last = offsets[num_offsets - 1];
            if !is_monotonic || last as usize > data.len() {
                return Err(chunk_offset_violation_error(offsets, data.len()));
            }

            Ok(DataBlock::VariableWidth(VariableWidthBlock {
                data: LanceBuffer::from(data[first as usize..last as usize].to_vec()),
                offsets: LanceBuffer::reinterpret_vec(result_offsets),
                bits_per_offset: 64,
                num_values,
                block_info: BlockInfo::new(),
            }))
        } else {
            let offsets_buffer = data.borrow_to_typed_slice::<u32>();
            let offsets = &offsets_buffer.as_ref()[..num_offsets];

            let first = offsets[0];
            if (first as u64) < min_value_region_start as u64 {
                return Err(value_region_overlap_error(first as u64));
            }
            let mut previous = first;
            let mut is_monotonic = true;
            let result_offsets = offsets
                .iter()
                .map(|&offset| {
                    is_monotonic &= previous <= offset;
                    previous = offset;
                    offset.wrapping_sub(first)
                })
                .collect::<Vec<u32>>();
            let last = offsets[num_offsets - 1];
            if !is_monotonic || last as usize > data.len() {
                return Err(chunk_offset_violation_error(offsets, data.len()));
            }

            Ok(DataBlock::VariableWidth(VariableWidthBlock {
                data: LanceBuffer::from(data[first as usize..last as usize].to_vec()),
                offsets: LanceBuffer::reinterpret_vec(result_offsets),
                bits_per_offset: 32,
                num_values,
                block_info: BlockInfo::new(),
            }))
        }
    }
}

/// Most basic encoding for variable-width data which does no compression at all
/// The DataBlock memory layout looks like below:
///
/// | bits_per_offset           | bytes_start_offset        | offsets data | bytes data |
/// | ------------------------- | ------------------------- | ------------ | ---------- |
/// | <bits_per_offset>/8 bytes | <bits_per_offset>/8 bytes | offsets_len  | data_len   |
///
/// It's used in VariableEncoder and BinaryBlockDecompressor
///
#[derive(Debug, Default)]
pub struct VariableEncoder {}

impl BlockCompressor for VariableEncoder {
    fn compress(&self, mut data: DataBlock) -> Result<LanceBuffer> {
        match data {
            DataBlock::VariableWidth(ref mut variable_width_data) => {
                match variable_width_data.bits_per_offset {
                    32 => {
                        let offsets = variable_width_data.offsets.borrow_to_typed_slice::<u32>();
                        let offsets = offsets.as_ref();
                        // The first 4 bytes store the bits per offset, the next 4 bytes store the start
                        // offset of the bytes data, then offsets data, then bytes data.
                        let bytes_start_offset = 4 + 4 + std::mem::size_of_val(offsets) as u32;

                        let output_total_bytes =
                            bytes_start_offset as usize + variable_width_data.data.len();
                        let mut output: Vec<u8> = Vec::with_capacity(output_total_bytes);

                        // Store bit_per_offset info
                        output.extend_from_slice(&(32_u32).to_le_bytes());

                        // store `bytes_start_offset` in the next 4 bytes of output buffer
                        output.extend_from_slice(&(bytes_start_offset).to_le_bytes());

                        // store offsets
                        output.extend_from_slice(&variable_width_data.offsets);

                        // store bytes
                        output.extend_from_slice(&variable_width_data.data);
                        Ok(LanceBuffer::from(output))
                    }
                    64 => {
                        let offsets = variable_width_data.offsets.borrow_to_typed_slice::<u64>();
                        let offsets = offsets.as_ref();
                        // The first 8 bytes store the bits per offset, the next 8 bytes store the start
                        // offset of the bytes data, then offsets data, then bytes data.
                        let bytes_start_offset = 8 + 8 + std::mem::size_of_val(offsets) as u64;

                        let output_total_bytes =
                            bytes_start_offset as usize + variable_width_data.data.len();
                        let mut output: Vec<u8> = Vec::with_capacity(output_total_bytes);

                        // Store bit_per_offset info
                        output.extend_from_slice(&(64_u64).to_le_bytes());

                        // store `bytes_start_offset` in the next 8 bytes of output buffer
                        output.extend_from_slice(&(bytes_start_offset).to_le_bytes());

                        // store offsets
                        output.extend_from_slice(&variable_width_data.offsets);

                        // store bytes
                        output.extend_from_slice(&variable_width_data.data);
                        Ok(LanceBuffer::from(output))
                    }
                    _ => {
                        panic!(
                            "BinaryBlockEncoder does not work with {} bits per offset VariableWidth DataBlock.",
                            variable_width_data.bits_per_offset
                        );
                    }
                }
            }
            _ => {
                panic!("BinaryBlockEncoder can only work with Variable Width DataBlock.");
            }
        }
    }
}

impl PerValueCompressor for VariableEncoder {
    fn compress(&self, data: DataBlock) -> Result<(PerValueDataBlock, CompressiveEncoding)> {
        let DataBlock::VariableWidth(variable) = data else {
            panic!("BinaryPerValueCompressor can only work with Variable Width DataBlock.");
        };

        let encoding = ProtobufUtils21::variable(
            ProtobufUtils21::flat(variable.bits_per_offset as u64, None),
            None,
        );
        Ok((PerValueDataBlock::Variable(variable), encoding))
    }
}

#[derive(Debug, Default)]
pub struct VariableDecoder {}

impl VariablePerValueDecompressor for VariableDecoder {
    fn decompress(&self, data: VariableWidthBlock) -> Result<DataBlock> {
        Ok(DataBlock::VariableWidth(data))
    }
}

#[derive(Debug, Default)]
pub struct BinaryBlockDecompressor {}

impl BlockDecompressor for BinaryBlockDecompressor {
    fn decompress(&self, data: LanceBuffer, num_values: u64) -> Result<DataBlock> {
        // In older (not quite stable) versions we stored the bits per offset as a single byte and then the num_values
        // as four bytes.  However, this led to alignment problems and was wasteful since we already store the num_values
        // in higher layers.
        //
        // In the standard scheme we use 4 bytes for the bits per offset and 4 bytes for the bytes_start_offset and we
        // rely on the passed in num_values to be correct.

        // This isn't perfect but it's probably good enough and the best I think we can do.  The bits per offset will
        // never be more than 255 and it's little endian so the last 3 bytes will always be 0.  These will be the least
        // significant 3 bytes of the number of values in the old scheme.  It's pretty unlikely these are all 0 (that would
        // mean there are at least 16M values in a single page) so we'll use this to determine if the old scheme is used.
        //
        // The header fields and the offsets themselves come straight from the file.
        // The structural checks below (all O(1)) reject blocks whose regions do not
        // line up; the offset *values* are validated later, by the mandatory layout
        // validation in `VariableWidthBlock::into_arrow`, so they are not rescanned
        // here.
        if data.len() < 4 {
            return Err(Error::corrupt_file_named(
                "variable-width block",
                format!(
                    "block of {} bytes is too small to hold a header",
                    data.len()
                ),
            ));
        }
        let is_old_scheme = data[1] != 0 || data[2] != 0 || data[3] != 0;

        let ensure_header = |header_len: usize| {
            if data.len() < header_len {
                return Err(Error::corrupt_file_named(
                    "variable-width block",
                    format!(
                        "block of {} bytes is too small for a {} byte header",
                        data.len(),
                        header_len
                    ),
                ));
            }
            Ok(())
        };
        let (bits_per_offset, bytes_start_offset, offset_start) = if is_old_scheme {
            // Old scheme
            let bits_per_offset = data[0];
            match bits_per_offset {
                32 => {
                    ensure_header(9)?;
                    debug_assert_eq!(LittleEndian::read_u32(&data[1..5]), num_values as u32);
                    let bytes_start_offset = LittleEndian::read_u32(&data[5..9]);
                    (bits_per_offset, bytes_start_offset as u64, 9_u64)
                }
                64 => {
                    ensure_header(17)?;
                    debug_assert_eq!(LittleEndian::read_u64(&data[1..9]), num_values);
                    let bytes_start_offset = LittleEndian::read_u64(&data[9..17]);
                    (bits_per_offset, bytes_start_offset, 17)
                }
                _ => {
                    return Err(Error::invalid_input_source(
                        format!("Unsupported bits_per_offset={}", bits_per_offset).into(),
                    ));
                }
            }
        } else {
            // Standard scheme
            let bits_per_offset = LittleEndian::read_u32(&data[0..4]) as u8;
            match bits_per_offset {
                32 => {
                    ensure_header(8)?;
                    let bytes_start_offset = LittleEndian::read_u32(&data[4..8]);
                    (bits_per_offset, bytes_start_offset as u64, 8)
                }
                64 => {
                    ensure_header(16)?;
                    let bytes_start_offset = LittleEndian::read_u64(&data[8..16]);
                    (bits_per_offset, bytes_start_offset, 16)
                }
                _ => {
                    return Err(Error::invalid_input_source(
                        format!("Unsupported bits_per_offset={}", bits_per_offset).into(),
                    ));
                }
            }
        };

        // The offsets region sits between the header and `bytes_start_offset`
        // and must hold exactly `num_values + 1` offsets starting at zero.
        let expected_offsets_bytes = num_values
            .checked_add(1)
            .and_then(|num_offsets| num_offsets.checked_mul(bits_per_offset as u64 / 8))
            .ok_or_else(|| {
                Error::corrupt_file_named(
                    "variable-width block",
                    format!("offsets region size overflows for {num_values} values"),
                )
            })?;
        if bytes_start_offset < offset_start || bytes_start_offset > data.len() as u64 {
            return Err(Error::corrupt_file_named(
                "variable-width block",
                format!(
                    "bytes start offset {} is outside the block (header: {} bytes, block: {} bytes)",
                    bytes_start_offset,
                    offset_start,
                    data.len()
                ),
            ));
        }
        if bytes_start_offset - offset_start != expected_offsets_bytes {
            return Err(Error::corrupt_file_named(
                "variable-width block",
                format!(
                    "expected {} offset bytes for {} values but found {}",
                    expected_offsets_bytes,
                    num_values,
                    bytes_start_offset - offset_start
                ),
            ));
        }

        // the next `bytes_start_offset - offset_start` stores the offsets.
        let offsets = data.slice_with_length(
            offset_start as usize,
            (bytes_start_offset - offset_start) as usize,
        );
        let first_offset = match bits_per_offset {
            32 => LittleEndian::read_u32(&offsets[0..4]) as u64,
            _ => LittleEndian::read_u64(&offsets[0..8]),
        };
        if first_offset != 0 {
            return Err(Error::corrupt_file_named(
                "variable-width block",
                format!("first offset must be 0 but found {first_offset}"),
            ));
        }

        // the rest are the binary bytes.
        let data = data.slice_with_length(
            bytes_start_offset as usize,
            data.len() - bytes_start_offset as usize,
        );

        Ok(DataBlock::VariableWidth(VariableWidthBlock {
            data,
            offsets,
            bits_per_offset,
            num_values,
            block_info: BlockInfo::new(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::{
        ArrayRef, StringArray,
        builder::{LargeStringBuilder, StringBuilder},
    };
    use arrow_schema::{DataType, Field};

    use crate::{
        buffer::LanceBuffer,
        constants::{
            COMPRESSION_META_KEY, STRUCTURAL_ENCODING_FULLZIP, STRUCTURAL_ENCODING_META_KEY,
            STRUCTURAL_ENCODING_MINIBLOCK,
        },
        data::{BlockInfo, DataBlock, VariableWidthBlock},
        testing::check_specific_random,
    };
    use rstest::rstest;
    use std::{collections::HashMap, sync::Arc, vec};

    use crate::testing::{
        FnArrayGeneratorProvider, TestCases, check_basic_random, check_round_trip_encoding_of_data,
    };

    #[test_log::test(tokio::test)]
    async fn test_utf8_binary() {
        let field = Field::new("", DataType::Utf8, false);
        check_specific_random(field, TestCases::basic().with_structural_encodings()).await;
    }

    #[rstest]
    #[test_log::test(tokio::test)]
    async fn test_binary(
        #[values(STRUCTURAL_ENCODING_MINIBLOCK, STRUCTURAL_ENCODING_FULLZIP)]
        structural_encoding: &str,
        #[values(DataType::Utf8, DataType::Binary)] data_type: DataType,
    ) {
        let mut field_metadata = HashMap::new();
        field_metadata.insert(
            STRUCTURAL_ENCODING_META_KEY.to_string(),
            structural_encoding.into(),
        );

        let field = Field::new("", data_type, false).with_metadata(field_metadata);
        check_basic_random(field).await;
    }

    #[rstest]
    #[test_log::test(tokio::test)]
    async fn test_binary_fsst(
        #[values(STRUCTURAL_ENCODING_MINIBLOCK, STRUCTURAL_ENCODING_FULLZIP)]
        structural_encoding: &str,
        #[values(DataType::Binary, DataType::Utf8)] data_type: DataType,
    ) {
        let mut field_metadata = HashMap::new();
        field_metadata.insert(
            STRUCTURAL_ENCODING_META_KEY.to_string(),
            structural_encoding.into(),
        );
        field_metadata.insert(COMPRESSION_META_KEY.to_string(), "fsst".into());
        let field = Field::new("", data_type, true).with_metadata(field_metadata);
        // TODO (https://github.com/lance-format/lance/issues/4783)
        let test_cases = TestCases::default().with_structural_encodings();
        check_specific_random(field, test_cases).await;
    }

    #[rstest]
    #[test_log::test(tokio::test)]
    async fn test_fsst_large_binary(
        #[values(STRUCTURAL_ENCODING_MINIBLOCK, STRUCTURAL_ENCODING_FULLZIP)]
        structural_encoding: &str,
        #[values(DataType::LargeBinary, DataType::LargeUtf8)] data_type: DataType,
    ) {
        let mut field_metadata = HashMap::new();
        field_metadata.insert(
            STRUCTURAL_ENCODING_META_KEY.to_string(),
            structural_encoding.into(),
        );
        field_metadata.insert(COMPRESSION_META_KEY.to_string(), "fsst".into());
        let field = Field::new("", data_type, true).with_metadata(field_metadata);
        check_specific_random(field, TestCases::basic().with_structural_encodings()).await;
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

    #[rstest]
    #[test_log::test(tokio::test)]
    async fn test_small_strings(
        #[values(STRUCTURAL_ENCODING_MINIBLOCK, STRUCTURAL_ENCODING_FULLZIP)]
        structural_encoding: &str,
    ) {
        use crate::testing::check_basic_generated;

        let mut field_metadata = HashMap::new();
        field_metadata.insert(
            STRUCTURAL_ENCODING_META_KEY.to_string(),
            structural_encoding.into(),
        );
        let field = Field::new("", DataType::Utf8, true).with_metadata(field_metadata);
        check_basic_generated(
            field,
            Box::new(FnArrayGeneratorProvider::new(move || {
                lance_datagen::array::utf8_prefix_plus_counter("user_", /*is_large=*/ false)
            })),
        )
        .await;
    }

    #[rstest]
    #[test_log::test(tokio::test)]
    async fn test_simple_binary(
        #[values(STRUCTURAL_ENCODING_MINIBLOCK, STRUCTURAL_ENCODING_FULLZIP)]
        structural_encoding: &str,
        #[values(DataType::Utf8, DataType::Binary)] data_type: DataType,
    ) {
        let string_array = StringArray::from(vec![Some("abc"), None, Some("pqr"), None, Some("m")]);
        let string_array = arrow_cast::cast(&string_array, &data_type).unwrap();

        let mut field_metadata = HashMap::new();
        field_metadata.insert(
            STRUCTURAL_ENCODING_META_KEY.to_string(),
            structural_encoding.into(),
        );

        let test_cases = TestCases::default()
            .with_range(0..2)
            .with_range(0..3)
            .with_range(1..3)
            .with_indices(vec![0, 1, 3, 4]);
        check_round_trip_encoding_of_data(
            vec![Arc::new(string_array)],
            &test_cases,
            field_metadata,
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
    async fn test_bigger_than_max_page_size() {
        // Create an array with one single 32MiB string
        let big_string = String::from_iter((0..(32 * 1024 * 1024)).map(|_| '0'));
        let string_array = StringArray::from(vec![
            Some(big_string),
            Some("abc".to_string()),
            None,
            None,
            Some("xyz".to_string()),
        ]);

        // Drop the max page size to 1MiB
        let test_cases = TestCases::default().with_max_page_size(1024 * 1024);

        check_round_trip_encoding_of_data(
            vec![Arc::new(string_array)],
            &test_cases,
            HashMap::new(),
        )
        .await;

        // This is a regression testing the case where a page with X rows is split into Y parts
        // where the number of parts is not evenly divisible by the number of rows.  In this
        // case we are splitting 90 rows into 4 parts.
        let big_string = String::from_iter((0..(1000 * 1000)).map(|_| '0'));
        let string_array = StringArray::from_iter_values((0..90).map(|_| big_string.clone()));

        check_round_trip_encoding_of_data(
            vec![Arc::new(string_array)],
            &TestCases::default(),
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
                .with_indices(vec![2])
                .with_indices(vec![0, 1]);
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

    #[rstest]
    #[test_log::test(tokio::test)]
    async fn test_binary_dictionary_encoding(
        #[values(true, false)] with_nulls: bool,
        #[values(100, 500, 35000)] dict_size: u32,
    ) {
        let test_cases = TestCases::default().with_structural_encodings();
        let strings = (0..dict_size)
            .map(|i| i.to_string())
            .collect::<Vec<String>>();

        let repeated_strings: Vec<_> = strings
            .iter()
            .cycle()
            .take(70000)
            .enumerate()
            .map(|(i, s)| {
                if with_nulls && i % 7 == 0 {
                    None
                } else {
                    Some(s.clone())
                }
            })
            .collect();
        let string_array = Arc::new(StringArray::from(repeated_strings)) as ArrayRef;
        check_round_trip_encoding_of_data(vec![string_array], &test_cases, HashMap::new()).await;
    }

    #[test_log::test(tokio::test)]
    async fn test_binary_encoding_verification() {
        use lance_datagen::{ByteCount, RowCount};

        let test_cases = TestCases::default()
            .with_expected_encoding("variable")
            .with_structural_encodings();

        // Test both automatic selection and explicit configuration
        // 1. Test automatic binary encoding selection (small strings that won't trigger FSST)
        let arr_small = lance_datagen::gen_batch()
            .anon_col(lance_datagen::array::rand_utf8(ByteCount::from(10), false))
            .into_batch_rows(RowCount::from(1000))
            .unwrap()
            .column(0)
            .clone();
        check_round_trip_encoding_of_data(vec![arr_small], &test_cases, HashMap::new()).await;

        // 2. Test explicit "none" compression to force binary encoding
        let metadata_explicit =
            HashMap::from([("lance-encoding:compression".to_string(), "none".to_string())]);
        let arr_large = lance_datagen::gen_batch()
            .anon_col(lance_datagen::array::rand_utf8(ByteCount::from(50), false))
            .into_batch_rows(RowCount::from(2000))
            .unwrap()
            .column(0)
            .clone();
        check_round_trip_encoding_of_data(vec![arr_large], &test_cases, metadata_explicit).await;
    }

    #[test]
    fn test_binary_miniblock_with_misaligned_buffer() {
        use super::BinaryMiniBlockDecompressor;
        use crate::buffer::LanceBuffer;
        use crate::compression::MiniBlockDecompressor;
        use crate::data::DataBlock;

        // Test case 1: u32 offsets
        {
            let decompressor = BinaryMiniBlockDecompressor {
                bits_per_offset: 32,
            };

            // Create test data with u32 offsets
            // BinaryMiniBlock format: all offsets followed by all string data
            // Need to ensure total size is divisible by 4 for u32
            let mut test_data = Vec::new();

            // Offsets section (3 offsets for 2 values + 1 end offset)
            test_data.extend_from_slice(&12u32.to_le_bytes()); // offset to start of strings (after offsets)
            test_data.extend_from_slice(&15u32.to_le_bytes()); // offset to second string
            test_data.extend_from_slice(&20u32.to_le_bytes()); // offset to end

            // String data section
            test_data.extend_from_slice(b"ABCXYZ"); // 6 bytes of string data
            test_data.extend_from_slice(&[0, 0]); // 2 bytes padding to make total 20 bytes (divisible by 4)

            // Create a misaligned buffer by adding padding and slicing
            let mut padded = Vec::with_capacity(test_data.len() + 1);
            padded.push(0xFF); // Padding byte to misalign
            padded.extend_from_slice(&test_data);

            let bytes = bytes::Bytes::from(padded);
            let misaligned = bytes.slice(1..); // Skip first byte to create misalignment

            // Create LanceBuffer with bytes_per_value=1 to bypass alignment check
            let buffer = LanceBuffer::from_bytes(misaligned, 1);

            // Verify the buffer is actually misaligned
            let ptr = buffer.as_ref().as_ptr();
            assert_ne!(
                ptr.align_offset(4),
                0,
                "Test setup: buffer should be misaligned for u32"
            );

            // Decompress with misaligned buffer - should work with borrow_to_typed_slice
            let result = decompressor.decompress(vec![buffer], 2);
            assert!(
                result.is_ok(),
                "Decompression should succeed with misaligned buffer"
            );

            // Verify the data is correct
            if let Ok(DataBlock::VariableWidth(block)) = result {
                assert_eq!(block.num_values, 2);
                // Data should be the strings (including padding from the original buffer)
                assert_eq!(&block.data.as_ref()[..6], b"ABCXYZ");
            } else {
                panic!("Expected VariableWidth block");
            }
        }

        // Test case 2: u64 offsets
        {
            let decompressor = BinaryMiniBlockDecompressor {
                bits_per_offset: 64,
            };

            // Create test data with u64 offsets
            let mut test_data = Vec::new();

            // Offsets section (3 offsets for 2 values + 1 end offset)
            test_data.extend_from_slice(&24u64.to_le_bytes()); // offset to start of strings (after offsets)
            test_data.extend_from_slice(&29u64.to_le_bytes()); // offset to second string
            test_data.extend_from_slice(&40u64.to_le_bytes()); // offset to end (divisible by 8)

            // String data section
            test_data.extend_from_slice(b"HelloWorld"); // 10 bytes of string data
            test_data.extend_from_slice(&[0, 0, 0, 0, 0, 0]); // 6 bytes padding to make total 40 bytes (divisible by 8)

            // Create misaligned buffer
            let mut padded = Vec::with_capacity(test_data.len() + 3);
            padded.extend_from_slice(&[0xFF, 0xFF, 0xFF]); // 3 bytes padding for misalignment
            padded.extend_from_slice(&test_data);

            let bytes = bytes::Bytes::from(padded);
            let misaligned = bytes.slice(3..); // Skip 3 bytes

            let buffer = LanceBuffer::from_bytes(misaligned, 1);

            // Verify misalignment for u64
            let ptr = buffer.as_ref().as_ptr();
            assert_ne!(
                ptr.align_offset(8),
                0,
                "Test setup: buffer should be misaligned for u64"
            );

            // Decompress should succeed
            let result = decompressor.decompress(vec![buffer], 2);
            assert!(
                result.is_ok(),
                "Decompression should succeed with misaligned u64 buffer"
            );

            if let Ok(DataBlock::VariableWidth(block)) = result {
                assert_eq!(block.num_values, 2);
                // Data should be the strings (including padding from the original buffer)
                assert_eq!(&block.data.as_ref()[..10], b"HelloWorld");
            } else {
                panic!("Expected VariableWidth block");
            }
        }
    }

    #[test]
    fn test_binary_miniblock_rejects_corrupt_offsets() {
        use super::BinaryMiniBlockDecompressor;
        use crate::compression::MiniBlockDecompressor;
        use lance_core::Error;

        // Chunk layout mirrors the on-disk format for ["alpha", "beta", "gamma"]:
        // LE u32 offsets [16, 21, 25, 30] followed by the value bytes, padded to
        // a multiple of 8 bytes.
        fn chunk_u32(offsets: &[u32], values: &[u8]) -> LanceBuffer {
            let mut chunk = offsets
                .iter()
                .flat_map(|offset| offset.to_le_bytes())
                .collect::<Vec<u8>>();
            chunk.extend_from_slice(values);
            chunk.resize(chunk.len().next_multiple_of(8), 0);
            LanceBuffer::from(chunk)
        }

        let decompressor = BinaryMiniBlockDecompressor::new(32);

        // The tail offset points past the end of the 32-byte chunk.
        let err = decompressor
            .decompress(
                vec![chunk_u32(&[16, 21, 25, 100_000], b"alphabetagamma")],
                3,
            )
            .unwrap_err();
        assert!(matches!(err, Error::CorruptFile { .. }), "{err:?}");
        assert!(err.to_string().contains("out of bounds"), "{err}");

        // Offsets go backwards, which would underflow the rebase subtraction.
        let err = decompressor
            .decompress(vec![chunk_u32(&[16, 25, 21, 30], b"alphabetagamma")], 3)
            .unwrap_err();
        assert!(matches!(err, Error::CorruptFile { .. }), "{err:?}");
        assert!(err.to_string().contains("decreases"), "{err}");

        // The first offset points inside the offset table, which would alias
        // the serialized offsets into the value bytes.
        let err = decompressor
            .decompress(vec![chunk_u32(&[0, 21, 25, 30], b"alphabetagamma")], 3)
            .unwrap_err();
        assert!(matches!(err, Error::CorruptFile { .. }), "{err:?}");
        assert!(err.to_string().contains("overlaps"), "{err}");

        // The chunk stores fewer offsets than the requested value count needs.
        let err = decompressor
            .decompress(vec![chunk_u32(&[8, 8], &[])], 3)
            .unwrap_err();
        assert!(matches!(err, Error::CorruptFile { .. }), "{err:?}");
        assert!(err.to_string().contains("requires 4"), "{err}");

        // The chunk size is not a multiple of the offset width.
        let err = decompressor
            .decompress(vec![LanceBuffer::from(vec![0u8; 10])], 1)
            .unwrap_err();
        assert!(matches!(err, Error::CorruptFile { .. }), "{err:?}");
        assert!(err.to_string().contains("multiple"), "{err}");

        // 64-bit offsets take the same validation path.
        fn chunk_u64(offsets: &[u64], values: &[u8]) -> LanceBuffer {
            let mut chunk = offsets
                .iter()
                .flat_map(|offset| offset.to_le_bytes())
                .collect::<Vec<u8>>();
            chunk.extend_from_slice(values);
            chunk.resize(chunk.len().next_multiple_of(8), 0);
            LanceBuffer::from(chunk)
        }
        let decompressor = BinaryMiniBlockDecompressor::new(64);
        let err = decompressor
            .decompress(
                vec![chunk_u64(&[32, 37, 41, 100_000], b"alphabetagamma")],
                3,
            )
            .unwrap_err();
        assert!(matches!(err, Error::CorruptFile { .. }), "{err:?}");
        assert!(err.to_string().contains("out of bounds"), "{err}");
        let err = decompressor
            .decompress(vec![chunk_u64(&[0, 37, 41, 46], b"alphabetagamma")], 3)
            .unwrap_err();
        assert!(matches!(err, Error::CorruptFile { .. }), "{err:?}");
        assert!(err.to_string().contains("overlaps"), "{err}");

        // A valid chunk still decodes: offsets rebase to [0, 5, 9, 14].
        let decompressor = BinaryMiniBlockDecompressor::new(32);
        let block = decompressor
            .decompress(vec![chunk_u32(&[16, 21, 25, 30], b"alphabetagamma")], 3)
            .unwrap();
        let DataBlock::VariableWidth(block) = block else {
            panic!("expected a variable-width block");
        };
        assert_eq!(block.data.as_ref(), b"alphabetagamma");
        assert_eq!(
            block.offsets,
            LanceBuffer::reinterpret_vec(vec![0_u32, 5, 9, 14])
        );
    }

    fn encoded_binary_block(bits_per_offset: u8) -> Vec<u8> {
        use crate::compression::BlockCompressor;

        let offsets = match bits_per_offset {
            32 => LanceBuffer::reinterpret_vec(vec![0_i32, 5, 9, 14]),
            64 => LanceBuffer::reinterpret_vec(vec![0_i64, 5, 9, 14]),
            _ => unreachable!(),
        };
        let block = DataBlock::VariableWidth(VariableWidthBlock {
            data: LanceBuffer::copy_slice(b"alphabetagamma"),
            offsets,
            bits_per_offset,
            num_values: 3,
            block_info: BlockInfo::new(),
        });
        BlockCompressor::compress(&super::VariableEncoder::default(), block)
            .unwrap()
            .as_ref()
            .to_vec()
    }

    /// The block decompressor only checks the block structure (all O(1)); bad
    /// offset values inside a structurally-sound block are rejected by the
    /// mandatory layout validation when the block is converted to Arrow.
    #[rstest]
    #[case::i32_tail_out_of_bounds(32, 3, 100_000, "out of bounds")]
    #[case::i64_tail_out_of_bounds(64, 3, 15, "out of bounds")]
    #[case::i32_non_monotonic(32, 2, 4, "non-monotonic")]
    #[case::i64_non_monotonic(64, 2, 4, "non-monotonic")]
    fn test_binary_block_bad_offsets_rejected_at_arrow_conversion(
        #[case] bits_per_offset: u8,
        #[case] mutated_offset_index: usize,
        #[case] mutated_offset_value: u64,
        #[case] expected_message: &str,
    ) {
        use crate::compression::BlockDecompressor;
        use lance_core::Error;

        let mut encoded = encoded_binary_block(bits_per_offset);
        let bytes_per_offset = (bits_per_offset / 8) as usize;
        // The standard scheme header is two offset-width fields.
        let mutated_offset_start = bytes_per_offset * (2 + mutated_offset_index);
        encoded[mutated_offset_start..mutated_offset_start + bytes_per_offset]
            .copy_from_slice(&mutated_offset_value.to_le_bytes()[..bytes_per_offset]);

        let block = super::BinaryBlockDecompressor::default()
            .decompress(LanceBuffer::from(encoded), 3)
            .unwrap();
        let data_type = match bits_per_offset {
            32 => DataType::Binary,
            _ => DataType::LargeBinary,
        };
        let err = block.into_arrow(data_type, false).unwrap_err();
        assert!(matches!(err, Error::CorruptFile { .. }), "{err:?}");
        assert!(err.to_string().contains(expected_message), "{err}");
    }

    #[test]
    fn test_binary_block_rejects_corrupt_structure() {
        use crate::compression::BlockDecompressor;
        use lance_core::Error;

        let decompressor = super::BinaryBlockDecompressor::default();

        // The first offset must be zero.
        let mut encoded = encoded_binary_block(32);
        encoded[8..12].copy_from_slice(&5_u32.to_le_bytes());
        let err = decompressor
            .decompress(LanceBuffer::from(encoded), 3)
            .unwrap_err();
        assert!(matches!(err, Error::CorruptFile { .. }), "{err:?}");
        assert!(err.to_string().contains("first offset"), "{err}");

        // The offsets region must hold exactly num_values + 1 offsets.
        let encoded = encoded_binary_block(32);
        let err = decompressor
            .decompress(LanceBuffer::from(encoded), 4)
            .unwrap_err();
        assert!(matches!(err, Error::CorruptFile { .. }), "{err:?}");
        assert!(err.to_string().contains("offset bytes"), "{err}");

        // A block too small to hold its header is rejected, not a panic.
        let err = decompressor
            .decompress(LanceBuffer::from(vec![0_u8; 2]), 1)
            .unwrap_err();
        assert!(matches!(err, Error::CorruptFile { .. }), "{err:?}");
        assert!(err.to_string().contains("too small"), "{err}");
    }
}
