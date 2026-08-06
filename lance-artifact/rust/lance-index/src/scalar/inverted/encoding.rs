// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::io::Write;

use super::builder::BLOCK_SIZE;
use super::index::{PositionStreamCodec, PostingTailCodec};
#[cfg(test)]
use super::tokenizer::LEGACY_BLOCK_SIZE;
use super::tokenizer::validate_block_size;
use arrow::array::LargeBinaryBuilder;
use lance_bitpacking::{BitPacker, BitPacker4x, BitPacker8x};
use lance_core::{Error, Result};

pub const MAX_POSTING_BLOCK_SIZE: usize = BitPacker8x::BLOCK_LEN;

// we compress the posting list to multiple blocks of fixed number of elements (BLOCK_SIZE),
// returns a LargeBinaryArray, where each binary is a compressed block (128 row ids + 128 frequencies)
// each block is:
// - 4 bytes for the max block score
// - 4 bytes for the first doc id
// - 1 byte for the number of bits used to pack the doc ids
// - n bytes for the packed doc ids
// - 1 byte for the number of bits used to pack the frequencies
// - n bytes for the packed frequencies
// if the block is not full (the last block), we encode the remainder separately
// using the configured remainder codec.

// compress the posting list to multiple blocks of fixed number of elements (BLOCK_SIZE),
// returns a LargeBinaryArray, where each binary is a compressed block (128 row ids + 128 frequencies)
#[cfg(test)]
pub fn compress_posting_list<'a>(
    length: usize,
    doc_ids: impl Iterator<Item = &'a u32>,
    frequencies: impl Iterator<Item = &'a u32>,
    block_max_scores: impl Iterator<Item = f32>,
) -> Result<arrow::array::LargeBinaryArray> {
    compress_posting_list_with_tail_codec(
        length,
        doc_ids,
        frequencies,
        block_max_scores,
        PostingTailCodec::VarintDelta,
    )
}

#[cfg(test)]
pub fn compress_posting_list_with_tail_codec<'a>(
    length: usize,
    doc_ids: impl Iterator<Item = &'a u32>,
    frequencies: impl Iterator<Item = &'a u32>,
    block_max_scores: impl Iterator<Item = f32>,
    tail_codec: PostingTailCodec,
) -> Result<arrow::array::LargeBinaryArray> {
    compress_posting_list_with_tail_codec_and_block_size(
        length,
        doc_ids,
        frequencies,
        block_max_scores,
        tail_codec,
        LEGACY_BLOCK_SIZE,
    )
}

#[cfg(test)]
pub fn compress_posting_list_with_tail_codec_and_block_size<'a>(
    length: usize,
    doc_ids: impl Iterator<Item = &'a u32>,
    frequencies: impl Iterator<Item = &'a u32>,
    mut block_max_scores: impl Iterator<Item = f32>,
    tail_codec: PostingTailCodec,
    block_size: usize,
) -> Result<arrow::array::LargeBinaryArray> {
    let block_size = validate_block_size(block_size)?;
    if length < block_size {
        // directly do remainder compression to avoid overhead of creating buffer
        let mut builder = LargeBinaryBuilder::with_capacity(1, length * 4 * 2 + 1);
        // write the max score of the block (128-doc blocks only)
        if posting_block_score_prefix_len(block_size) > 0 {
            let max_score = block_max_scores.next().unwrap();
            let _ = builder.write(max_score.to_le_bytes().as_ref())?;
        }
        compress_posting_remainder(
            doc_ids.copied().collect::<Vec<_>>().as_slice(),
            frequencies.copied().collect::<Vec<_>>().as_slice(),
            tail_codec,
            &mut builder,
        )?;
        builder.append_value("");
        return Ok(builder.finish());
    }

    let mut builder = LargeBinaryBuilder::with_capacity(length.div_ceil(block_size), length * 3);
    let mut doc_id_buffer = Vec::with_capacity(block_size);
    let mut freq_buffer = Vec::with_capacity(block_size);
    for (doc_id, freq) in std::iter::zip(doc_ids, frequencies) {
        doc_id_buffer.push(*doc_id);
        freq_buffer.push(*freq);

        if doc_id_buffer.len() < block_size {
            continue;
        }

        assert_eq!(doc_id_buffer.len(), block_size);

        // write the max score of the block (128-doc blocks only)
        if posting_block_score_prefix_len(block_size) > 0 {
            let max_score = block_max_scores.next().unwrap();
            let _ = builder.write(max_score.to_le_bytes().as_ref())?;
        }
        encode_posting_block_payload(&doc_id_buffer, &freq_buffer, &mut builder)?;
        builder.append_value("");
        doc_id_buffer.clear();
        freq_buffer.clear();
    }

    // we don't compress the last block if it is not full
    if !doc_id_buffer.is_empty() {
        // write the max score of the block (128-doc blocks only)
        if posting_block_score_prefix_len(block_size) > 0 {
            let max_score = block_max_scores.next().unwrap();
            let _ = builder.write(max_score.to_le_bytes().as_ref())?;
        }
        compress_posting_remainder(&doc_id_buffer, &freq_buffer, tail_codec, &mut builder)?;
        builder.append_value("");
    }
    Ok(builder.finish())
}

/// Byte length of the block-max-score prefix on posting blocks. 128-doc
/// blocks store a per-block max score, patched in at build time; 256-document
/// blocks always carry impact skip data, which supersedes it, so they
/// store none.
#[inline]
pub fn posting_block_score_prefix_len(block_size: usize) -> usize {
    if block_size == MAX_POSTING_BLOCK_SIZE {
        0
    } else {
        4
    }
}

pub fn encode_full_posting_block_into(
    doc_ids: &[u32],
    frequencies: &[u32],
    block: &mut Vec<u8>,
) -> Result<()> {
    validate_block_size(doc_ids.len())?;
    debug_assert_eq!(doc_ids.len(), frequencies.len());
    if posting_block_score_prefix_len(doc_ids.len()) > 0 {
        block.extend_from_slice(&0f32.to_le_bytes());
    }
    encode_posting_block_payload(doc_ids, frequencies, block)?;
    Ok(())
}

fn encode_posting_block_payload(
    doc_ids: &[u32],
    frequencies: &[u32],
    block: &mut impl Write,
) -> Result<()> {
    debug_assert_eq!(doc_ids.len(), frequencies.len());
    validate_block_size(doc_ids.len())?;
    let mut buffer = [0u8; MAX_POSTING_BLOCK_SIZE * 4 + 5];
    match doc_ids.len() {
        BitPacker4x::BLOCK_LEN => {
            compress_sorted_block_with::<BitPacker4x>(doc_ids, &mut buffer, block)?;
            compress_block_with::<BitPacker4x>(frequencies, &mut buffer, block)?;
        }
        // 256-document blocks store frequencies with patched FOR:
        // outliers no longer widen the whole block, which matters because one
        // large tf per block otherwise doubles the frequency payload.
        BitPacker8x::BLOCK_LEN => {
            compress_sorted_block_with::<BitPacker8x>(doc_ids, &mut buffer, block)?;
            compress_pfor_block_with::<BitPacker8x>(frequencies, &mut buffer, block)?;
        }
        _ => unreachable!("validated posting block size should be supported"),
    }
    Ok(())
}

/// Patched FOR (Lucene PForUtil style): pick the body bit width that
/// minimizes total bytes, pack all values masked to that width, and append
/// up to [`PFOR_MAX_EXCEPTIONS`] exceptions as (index u8, high-bits varint).
const PFOR_MAX_EXCEPTIONS: usize = 31;

#[inline]
fn u32_bits(value: u32) -> usize {
    (32 - value.leading_zeros()) as usize
}

#[inline]
fn varint_u32_len(value: u32) -> usize {
    u32_bits(value).max(1).div_ceil(7)
}

fn compress_pfor_block_with<P: BitPacker>(
    data: &[u32],
    buffer: &mut [u8],
    builder: &mut impl Write,
) -> Result<()> {
    debug_assert_eq!(data.len(), P::BLOCK_LEN);
    let max_bits = data.iter().map(|&v| u32_bits(v)).max().unwrap_or(0);
    let mut best_width = max_bits;
    let mut best_cost = P::BLOCK_LEN * max_bits / 8;
    for width in (0..max_bits).rev() {
        let mut exceptions = 0usize;
        let mut exception_bytes = 0usize;
        for &value in data {
            if u32_bits(value) > width {
                exceptions += 1;
                exception_bytes += 1 + varint_u32_len(value >> width);
            }
        }
        if exceptions > PFOR_MAX_EXCEPTIONS {
            break;
        }
        let cost = P::BLOCK_LEN * width / 8 + exception_bytes;
        if cost < best_cost {
            best_cost = cost;
            best_width = width;
        }
    }

    let mask = if best_width >= 32 {
        u32::MAX
    } else {
        (1u32 << best_width) - 1
    };
    let mut body = [0u32; MAX_POSTING_BLOCK_SIZE];
    let mut exception_buf = Vec::new();
    let mut exception_count = 0u8;
    for (index, &value) in data.iter().enumerate() {
        body[index] = value & mask;
        if u32_bits(value) > best_width {
            exception_buf.push(index as u8);
            encode_varint_u32(&mut exception_buf, value >> best_width);
            exception_count += 1;
        }
    }
    let compressor = P::new();
    let num_bytes = compressor.compress(&body[..P::BLOCK_LEN], buffer, best_width as u8);
    let _ = builder.write(&[best_width as u8, exception_count])?;
    let _ = builder.write(&buffer[..num_bytes])?;
    let _ = builder.write(&exception_buf)?;
    Ok(())
}

fn decompress_pfor_block_with<P: BitPacker>(
    block: &[u8],
    buffer: &mut [u32],
    res: &mut Vec<u32>,
) -> usize {
    debug_assert!(buffer.len() >= P::BLOCK_LEN);
    let buffer = &mut buffer[..P::BLOCK_LEN];
    let width = block[0];
    let exception_count = block[1] as usize;
    let compressor = P::new();
    let num_bytes = compressor.decompress(&block[2..], buffer, width);
    let mut offset = 2 + num_bytes;
    for _ in 0..exception_count {
        let index = block[offset] as usize;
        offset += 1;
        let high = decode_varint_u32(block, &mut offset)
            .expect("pfor exception high bits should be a valid varint");
        buffer[index] |= high << width;
    }
    res.extend_from_slice(buffer);
    offset
}

pub fn encode_remainder_posting_block_into(
    doc_ids: &[u32],
    frequencies: &[u32],
    codec: PostingTailCodec,
    block_size: usize,
    block: &mut Vec<u8>,
) -> Result<()> {
    debug_assert_eq!(doc_ids.len(), frequencies.len());
    if posting_block_score_prefix_len(block_size) > 0 {
        block.extend_from_slice(&0f32.to_le_bytes());
    }
    compress_posting_remainder(doc_ids, frequencies, codec, block)?;
    Ok(())
}

#[inline]
fn compress_sorted_block(data: &[u32], buffer: &mut [u8], builder: &mut impl Write) -> Result<()> {
    compress_sorted_block_with::<BitPacker4x>(data, buffer, builder)
}

#[inline]
fn compress_sorted_block_with<P: BitPacker>(
    data: &[u32],
    buffer: &mut [u8],
    builder: &mut impl Write,
) -> Result<()> {
    debug_assert_eq!(data.len(), P::BLOCK_LEN);
    let compressor = P::new();
    let num_bits = compressor.num_bits_sorted(data[0], data);
    let num_bytes = compressor.compress_sorted(data[0], data, buffer, num_bits);
    let _ = builder.write(data[0].to_le_bytes().as_ref())?;
    let _ = builder.write(&[num_bits])?;
    let _ = builder.write(&buffer[..num_bytes])?;
    Ok(())
}

#[inline]
fn compress_block(data: &[u32], buffer: &mut [u8], builder: &mut impl Write) -> Result<()> {
    compress_block_with::<BitPacker4x>(data, buffer, builder)
}

#[inline]
fn compress_block_with<P: BitPacker>(
    data: &[u32],
    buffer: &mut [u8],
    builder: &mut impl Write,
) -> Result<()> {
    debug_assert_eq!(data.len(), P::BLOCK_LEN);
    let compressor = P::new();
    let num_bits = compressor.num_bits(data);
    let num_bytes = compressor.compress(data, buffer, num_bits);
    let _ = builder.write(&[num_bits])?;
    let _ = builder.write(&buffer[..num_bytes])?;
    Ok(())
}

#[inline]
fn compress_raw_remainder(data: &[u32], builder: &mut impl Write) -> Result<()> {
    for value in data.iter() {
        let _ = builder.write(value.to_le_bytes().as_ref())?;
    }
    Ok(())
}

#[inline]
fn write_varint_u32(builder: &mut impl Write, mut value: u32) -> Result<()> {
    let mut bytes = [0u8; 5];
    let mut len = 0usize;
    while value >= 0x80 {
        bytes[len] = (value as u8) | 0x80;
        value >>= 7;
        len += 1;
    }
    bytes[len] = value as u8;
    len += 1;
    let _ = builder.write(&bytes[..len])?;
    Ok(())
}

#[inline]
fn compress_posting_remainder(
    doc_ids: &[u32],
    frequencies: &[u32],
    codec: PostingTailCodec,
    builder: &mut impl Write,
) -> Result<()> {
    debug_assert_eq!(doc_ids.len(), frequencies.len());
    match codec {
        PostingTailCodec::Fixed32 => {
            compress_raw_remainder(doc_ids, builder)?;
            compress_raw_remainder(frequencies, builder)?;
        }
        PostingTailCodec::VarintDelta => {
            let mut previous = 0u32;
            for (index, &doc_id) in doc_ids.iter().enumerate() {
                let delta = if index == 0 {
                    doc_id
                } else {
                    doc_id.checked_sub(previous).ok_or_else(|| {
                        Error::index(format!(
                            "doc ids must be sorted within a posting tail block, got {} after {}",
                            doc_id, previous
                        ))
                    })?
                };
                write_varint_u32(builder, delta)?;
                previous = doc_id;
            }
            for &frequency in frequencies {
                write_varint_u32(builder, frequency)?;
            }
        }
    }
    Ok(())
}

pub fn compress_positions(positions: &[u32]) -> Result<arrow::array::LargeBinaryArray> {
    let mut builder = LargeBinaryBuilder::with_capacity(
        positions.len().div_ceil(BLOCK_SIZE),
        positions.len() * 4,
    );
    // record the number of positions in the first binary
    let num_positions = positions.len() as u32;
    builder.append_value(num_positions.to_le_bytes().as_ref());

    let position_chunks = positions.chunks_exact(BLOCK_SIZE);
    let mut buffer = [0u8; BLOCK_SIZE * 4 + 5];
    for position_chunk in position_chunks {
        // delta encoding + bitpacking for positions
        compress_sorted_block(position_chunk, &mut buffer, &mut builder)?;
        builder.append_value("");
    }

    // we don't compress the last block if it is not full
    let length = positions.len();
    let remainder = length % BLOCK_SIZE;
    if remainder > 0 {
        compress_raw_remainder(&positions[length - remainder..], &mut builder)?;
        builder.append_value("");
    }

    Ok(builder.finish())
}

#[inline]
pub fn encode_varint_u32(dst: &mut Vec<u8>, mut value: u32) {
    while value >= 0x80 {
        dst.push((value as u8) | 0x80);
        value >>= 7;
    }
    dst.push(value as u8);
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct PositionBlockBuilder {
    codec: PositionStreamCodec,
    encoded_bytes: Vec<u8>,
    pending_deltas: Vec<u32>,
}

impl Default for PositionBlockBuilder {
    fn default() -> Self {
        Self::new(PositionStreamCodec::PackedDelta)
    }
}

impl PositionBlockBuilder {
    pub(super) fn new(codec: PositionStreamCodec) -> Self {
        Self {
            codec,
            encoded_bytes: Vec::new(),
            pending_deltas: Vec::new(),
        }
    }

    pub(super) fn size(&self) -> usize {
        self.encoded_bytes.capacity() + self.pending_deltas.capacity() * std::mem::size_of::<u32>()
    }

    pub(super) fn append_doc_positions(&mut self, positions: &[u32]) -> Result<()> {
        let mut previous = 0u32;
        for (index, &position) in positions.iter().enumerate() {
            let delta = if index == 0 {
                position
            } else {
                position.checked_sub(previous).ok_or_else(|| {
                    Error::index(format!(
                        "positions must be sorted within a document, got {} after {}",
                        position, previous
                    ))
                })?
            };
            self.push_delta(delta)?;
            previous = position;
        }
        Ok(())
    }

    pub(super) fn append_position(
        &mut self,
        position: u32,
        previous_in_doc: Option<u32>,
    ) -> Result<()> {
        let delta = match previous_in_doc {
            Some(previous) => position.checked_sub(previous).ok_or_else(|| {
                Error::index(format!(
                    "positions must be sorted within a document, got {} after {}",
                    position, previous
                ))
            })?,
            None => position,
        };
        self.push_delta(delta)
    }

    pub(super) fn finish(self) -> Vec<u8> {
        let mut bytes = self.encoded_bytes;
        match self.codec {
            PositionStreamCodec::VarintDocDelta | PositionStreamCodec::PackedDelta => {
                for delta in self.pending_deltas {
                    encode_varint_u32(&mut bytes, delta);
                }
            }
        }
        bytes
    }

    pub(super) fn decode_into(&self, frequencies: &[u32], dst: &mut Vec<u32>) -> Result<()> {
        let bytes = self.clone().finish();
        decode_position_stream_block(bytes.as_slice(), frequencies, self.codec, dst)
    }

    fn push_delta(&mut self, delta: u32) -> Result<()> {
        match self.codec {
            PositionStreamCodec::VarintDocDelta => {
                encode_varint_u32(&mut self.encoded_bytes, delta);
            }
            PositionStreamCodec::PackedDelta => {
                self.pending_deltas.push(delta);
                if self.pending_deltas.len() == BLOCK_SIZE {
                    let mut packed_buffer = [0u8; BLOCK_SIZE * 4 + 1];
                    compress_block(
                        self.pending_deltas.as_slice(),
                        &mut packed_buffer,
                        &mut self.encoded_bytes,
                    )?;
                    self.pending_deltas.clear();
                }
            }
        }
        Ok(())
    }
}

#[inline]
pub fn decode_varint_u32(src: &[u8], offset: &mut usize) -> Result<u32> {
    let mut value = 0u32;
    let mut shift = 0u32;
    while *offset < src.len() {
        let byte = src[*offset];
        *offset += 1;
        value |= u32::from(byte & 0x7F) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
        shift += 7;
        if shift >= 35 {
            return Err(Error::index(
                "invalid u32 varint in position stream".to_owned(),
            ));
        }
    }
    Err(Error::index(
        "unexpected EOF while decoding position stream".to_owned(),
    ))
}

#[cfg(test)]
fn encode_position_stream_varint_block_into(
    positions: &[u32],
    frequencies: &[u32],
    dst: &mut Vec<u8>,
) -> Result<()> {
    let mut offset = 0usize;
    for &frequency in frequencies {
        let frequency = frequency as usize;
        let end = offset
            .checked_add(frequency)
            .ok_or_else(|| Error::index("position block length overflow".to_owned()))?;
        if end > positions.len() {
            return Err(Error::index(format!(
                "position block has {} positions but frequencies require at least {}",
                positions.len(),
                end
            )));
        }
        let mut previous = 0u32;
        for (index, &position) in positions[offset..end].iter().enumerate() {
            let delta = if index == 0 {
                position
            } else {
                position.checked_sub(previous).ok_or_else(|| {
                    Error::index(format!(
                        "positions must be sorted within a document, got {} after {}",
                        position, previous
                    ))
                })?
            };
            encode_varint_u32(dst, delta);
            previous = position;
        }
        offset = end;
    }
    if offset != positions.len() {
        return Err(Error::index(format!(
            "position block has {} trailing positions after consuming {} frequencies",
            positions.len() - offset,
            frequencies.len()
        )));
    }
    Ok(())
}

fn decode_position_stream_varint_block(
    src: &[u8],
    frequencies: &[u32],
    dst: &mut Vec<u32>,
) -> Result<()> {
    let mut offset = 0usize;
    for &frequency in frequencies {
        let mut previous = 0u32;
        for index in 0..frequency as usize {
            let delta = decode_varint_u32(src, &mut offset)?;
            let position = if index == 0 {
                delta
            } else {
                previous.checked_add(delta).ok_or_else(|| {
                    Error::index("position stream overflow while decoding".to_owned())
                })?
            };
            dst.push(position);
            previous = position;
        }
    }
    if offset != src.len() {
        return Err(Error::index(format!(
            "position stream has {} trailing bytes after decoding block",
            src.len() - offset
        )));
    }
    Ok(())
}

#[cfg(test)]
fn encode_position_stream_packed_block_into(
    positions: &[u32],
    frequencies: &[u32],
    dst: &mut Vec<u8>,
) -> Result<()> {
    let mut delta_buffer = [0u32; BLOCK_SIZE];
    let mut delta_count = 0usize;
    let mut packed_buffer = [0u8; BLOCK_SIZE * 4 + 1];
    let mut offset = 0usize;

    for &frequency in frequencies {
        let frequency = frequency as usize;
        let end = offset
            .checked_add(frequency)
            .ok_or_else(|| Error::index("position block length overflow".to_owned()))?;
        if end > positions.len() {
            return Err(Error::index(format!(
                "position block has {} positions but frequencies require at least {}",
                positions.len(),
                end
            )));
        }
        let mut previous = 0u32;
        for (index, &position) in positions[offset..end].iter().enumerate() {
            let delta = if index == 0 {
                position
            } else {
                position.checked_sub(previous).ok_or_else(|| {
                    Error::index(format!(
                        "positions must be sorted within a document, got {} after {}",
                        position, previous
                    ))
                })?
            };
            delta_buffer[delta_count] = delta;
            delta_count += 1;
            if delta_count == BLOCK_SIZE {
                compress_block(&delta_buffer, &mut packed_buffer, dst)?;
                delta_count = 0;
            }
            previous = position;
        }
        offset = end;
    }

    if offset != positions.len() {
        return Err(Error::index(format!(
            "position block has {} trailing positions after consuming {} frequencies",
            positions.len() - offset,
            frequencies.len()
        )));
    }

    for delta in &delta_buffer[..delta_count] {
        encode_varint_u32(dst, *delta);
    }
    Ok(())
}

fn decode_position_stream_packed_block(
    src: &[u8],
    frequencies: &[u32],
    dst: &mut Vec<u32>,
) -> Result<()> {
    let total_positions = frequencies.iter().try_fold(0usize, |total, &frequency| {
        total.checked_add(frequency as usize).ok_or_else(|| {
            Error::index("position stream length overflow while decoding".to_owned())
        })
    })?;

    let full_delta_blocks = total_positions / BLOCK_SIZE;
    let tail_len = total_positions % BLOCK_SIZE;

    let compressor = BitPacker4x::new();
    let mut packed_offset = 0usize;
    let mut packed_values = [0u32; BLOCK_SIZE];
    let mut deltas = Vec::with_capacity(total_positions);

    for _ in 0..full_delta_blocks {
        let (num_bits, payload, next_offset) = packed_position_group(src, packed_offset)?;
        let consumed = compressor.decompress(payload, &mut packed_values, num_bits);
        debug_assert_eq!(consumed, payload.len());
        packed_offset = next_offset;
        deltas.extend_from_slice(&packed_values);
    }

    for _ in 0..tail_len {
        deltas.push(decode_varint_u32(src, &mut packed_offset)?);
    }

    if packed_offset != src.len() {
        return Err(Error::index(format!(
            "position stream has {} trailing bytes after decoding block",
            src.len() - packed_offset
        )));
    }

    let mut delta_offset = 0usize;
    for &frequency in frequencies {
        let mut previous = 0u32;
        for index in 0..frequency as usize {
            let delta = deltas[delta_offset];
            delta_offset += 1;
            let position = if index == 0 {
                delta
            } else {
                previous.checked_add(delta).ok_or_else(|| {
                    Error::index("position stream overflow while decoding".to_owned())
                })?
            };
            dst.push(position);
            previous = position;
        }
    }
    debug_assert_eq!(delta_offset, deltas.len());
    Ok(())
}

fn packed_position_group(src: &[u8], offset: usize) -> Result<(u8, &[u8], usize)> {
    let num_bits = *src.get(offset).ok_or_else(|| {
        Error::index(format!(
            "unexpected EOF reading packed position group header at byte offset {offset}; \
             stream length is {}",
            src.len()
        ))
    })?;
    if num_bits > u32::BITS as u8 {
        return Err(Error::index(format!(
            "invalid packed position group bit width {num_bits} at byte offset {offset}; \
             expected at most {}",
            u32::BITS
        )));
    }

    let payload_start = offset
        .checked_add(1)
        .ok_or_else(|| Error::index("packed position group offset overflow".to_owned()))?;
    let payload_len = usize::from(num_bits) * BLOCK_SIZE / 8;
    let payload_end = payload_start
        .checked_add(payload_len)
        .ok_or_else(|| Error::index("packed position group length overflow".to_owned()))?;
    let payload = src.get(payload_start..payload_end).ok_or_else(|| {
        Error::index(format!(
            "unexpected EOF reading packed position group payload at byte offset {offset}; \
             need {payload_len} bytes after the header but stream length is {}",
            src.len()
        ))
    })?;
    Ok((num_bits, payload, payload_end))
}

/// Decode one document's positions out of a PackedDelta position block
/// without decoding the rest of the block. Full 128-delta groups are
/// self-describing (`[num_bits u8][16 * num_bits packed bytes]`), so group
/// byte offsets are recovered by hopping headers — no format change is
/// involved. `delta_range` is the doc's range in the block-wide delta stream
/// (from the frequency prefix sums); per-doc deltas reset at document
/// boundaries, so decoding starts cleanly at `delta_range.start`.
///
/// The caller passes per-block scratch state that this function maintains:
/// `group_offsets` (lazily extended header index, seeded with `[0]`),
/// `unpacked_group`/`unpacked_group_idx` (the last unpacked group), and
/// `tail_cache` (the varint tail, decoded in full on first touch). All are
/// reset by the caller when the block cursor moves.
#[allow(clippy::too_many_arguments)]
pub(super) fn seek_packed_doc_positions(
    src: &[u8],
    total_deltas: usize,
    delta_range: std::ops::Range<usize>,
    group_offsets: &mut Vec<usize>,
    unpacked_group: &mut [u32; BLOCK_SIZE],
    unpacked_group_idx: &mut Option<usize>,
    tail_cache: &mut Vec<u32>,
    dst: &mut Vec<u32>,
) -> Result<()> {
    dst.clear();
    if delta_range.start > delta_range.end || delta_range.end > total_deltas {
        return Err(Error::index(format!(
            "invalid packed position delta range {}..{} for {total_deltas} total deltas",
            delta_range.start, delta_range.end
        )));
    }
    if delta_range.is_empty() {
        return Ok(());
    }
    let num_full_groups = total_deltas / BLOCK_SIZE;
    let packed_deltas_end = num_full_groups * BLOCK_SIZE;

    // Extend the header index far enough for this range (tail needs the
    // offset one past the last full group).
    let last_needed_group = if delta_range.end > packed_deltas_end {
        num_full_groups
    } else {
        (delta_range.end - 1) / BLOCK_SIZE
    };
    while group_offsets.len() <= last_needed_group {
        let last = *group_offsets
            .last()
            .ok_or_else(|| Error::index("packed position group offsets are empty".to_owned()))?;
        let (_, _, next_offset) = packed_position_group(src, last)?;
        group_offsets.push(next_offset);
    }

    let mut previous = 0u32;
    let mut first = true;
    let mut push_delta = |delta: u32, dst: &mut Vec<u32>| -> Result<()> {
        let position = if first {
            first = false;
            delta
        } else {
            previous
                .checked_add(delta)
                .ok_or_else(|| Error::index("position stream overflow while decoding".to_owned()))?
        };
        dst.push(position);
        previous = position;
        Ok(())
    };

    for index in delta_range.start..delta_range.end.min(packed_deltas_end) {
        let group = index / BLOCK_SIZE;
        if *unpacked_group_idx != Some(group) {
            let offset = *group_offsets.get(group).ok_or_else(|| {
                Error::index(format!(
                    "missing packed position group offset for group {group}; have {} offsets",
                    group_offsets.len()
                ))
            })?;
            let (num_bits, payload, _) = packed_position_group(src, offset)?;
            BitPacker4x::new().decompress(payload, unpacked_group, num_bits);
            *unpacked_group_idx = Some(group);
        }
        push_delta(unpacked_group[index % BLOCK_SIZE], dst)?;
    }

    if delta_range.end > packed_deltas_end {
        let tail_len = total_deltas - packed_deltas_end;
        if tail_cache.len() != tail_len {
            tail_cache.clear();
            tail_cache.reserve(tail_len);
            let mut offset = *group_offsets.get(num_full_groups).ok_or_else(|| {
                Error::index(format!(
                    "missing packed position tail offset after {num_full_groups} full groups"
                ))
            })?;
            for _ in 0..tail_len {
                tail_cache.push(decode_varint_u32(src, &mut offset)?);
            }
        }
        for index in delta_range.start.max(packed_deltas_end)..delta_range.end {
            push_delta(tail_cache[index - packed_deltas_end], dst)?;
        }
    }
    Ok(())
}

#[cfg(test)]
pub fn encode_position_stream_block_into(
    positions: &[u32],
    frequencies: &[u32],
    codec: PositionStreamCodec,
    dst: &mut Vec<u8>,
) -> Result<()> {
    match codec {
        PositionStreamCodec::VarintDocDelta => {
            encode_position_stream_varint_block_into(positions, frequencies, dst)
        }
        PositionStreamCodec::PackedDelta => {
            encode_position_stream_packed_block_into(positions, frequencies, dst)
        }
    }
}

pub fn decode_position_stream_block(
    src: &[u8],
    frequencies: &[u32],
    codec: PositionStreamCodec,
    dst: &mut Vec<u32>,
) -> Result<()> {
    match codec {
        PositionStreamCodec::VarintDocDelta => {
            decode_position_stream_varint_block(src, frequencies, dst)
        }
        PositionStreamCodec::PackedDelta => {
            decode_position_stream_packed_block(src, frequencies, dst)
        }
    }
}

/// decompress the posting list from a LargeBinaryArray
/// returns a vector of (row_id, frequency) tuples
#[cfg(test)]
pub fn decompress_posting_list(
    num_docs: u32,
    posting_list: &arrow::array::LargeBinaryArray,
) -> Result<(Vec<u32>, Vec<u32>)> {
    decompress_posting_list_with_tail_codec(num_docs, posting_list, PostingTailCodec::VarintDelta)
}

#[cfg(test)]
pub fn decompress_posting_list_with_tail_codec(
    num_docs: u32,
    posting_list: &arrow::array::LargeBinaryArray,
    tail_codec: PostingTailCodec,
) -> Result<(Vec<u32>, Vec<u32>)> {
    decompress_posting_list_with_tail_codec_and_block_size(
        num_docs,
        posting_list,
        tail_codec,
        LEGACY_BLOCK_SIZE,
    )
}

#[cfg(test)]
pub fn decompress_posting_list_with_tail_codec_and_block_size(
    num_docs: u32,
    posting_list: &arrow::array::LargeBinaryArray,
    tail_codec: PostingTailCodec,
    block_size: usize,
) -> Result<(Vec<u32>, Vec<u32>)> {
    let block_size = validate_block_size(block_size)?;
    let mut doc_ids: Vec<u32> = Vec::with_capacity(num_docs as usize);
    let mut frequencies: Vec<u32> = Vec::with_capacity(num_docs as usize);

    let mut buffer = [0u32; MAX_POSTING_BLOCK_SIZE];
    let bitpacking_blocks = num_docs as usize / block_size;
    for compressed in posting_list.iter().take(bitpacking_blocks) {
        let compressed = compressed.unwrap();
        decompress_posting_block(
            compressed,
            &mut buffer,
            &mut doc_ids,
            &mut frequencies,
            block_size,
        );
    }

    let remainder = num_docs as usize % block_size;
    if remainder > 0 {
        let compressed = posting_list.value(bitpacking_blocks);
        decompress_posting_remainder(
            compressed,
            remainder,
            tail_codec,
            block_size,
            &mut doc_ids,
            &mut frequencies,
        );
    }

    Ok((doc_ids, frequencies))
}

pub fn decompress_positions(compressed: &arrow::array::LargeBinaryArray) -> Vec<u32> {
    let num_positions = read_num_positions(compressed);
    let mut positions: Vec<u32> = Vec::with_capacity(num_positions as usize);

    let mut buffer = [0u32; BLOCK_SIZE];
    let num_blocks = num_positions as usize / BLOCK_SIZE;
    for compressed in compressed.iter().skip(1).take(num_blocks) {
        let compressed = compressed.unwrap();
        decompress_sorted_block(compressed, &mut buffer, &mut positions);
    }

    let remainder = num_positions as usize % BLOCK_SIZE;
    if remainder > 0 {
        let compressed_block = compressed.value(num_blocks + 1);
        decompress_raw_remainder(compressed_block, remainder, &mut positions);
    }

    positions
}

pub fn read_num_positions(compressed: &arrow::array::LargeBinaryArray) -> u32 {
    u32::from_le_bytes(compressed.value(0).try_into().unwrap())
}

pub fn decompress_posting_block(
    block: &[u8],
    buffer: &mut [u32],
    doc_ids: &mut Vec<u32>,
    frequencies: &mut Vec<u32>,
    block_size: usize,
) {
    debug_assert!(validate_block_size(block_size).is_ok());
    debug_assert!(buffer.len() >= block_size);
    // skip the block max score prefix (128-doc blocks only)
    let mut block = &block[posting_block_score_prefix_len(block_size)..];
    match block_size {
        BitPacker4x::BLOCK_LEN => {
            let num_bytes = decompress_sorted_block_with::<BitPacker4x>(block, buffer, doc_ids);
            block = &block[num_bytes..];
            let num_bytes = decompress_block_with::<BitPacker4x>(block, buffer, frequencies);
            block = &block[num_bytes..];
        }
        BitPacker8x::BLOCK_LEN => {
            let num_bytes = decompress_sorted_block_with::<BitPacker8x>(block, buffer, doc_ids);
            block = &block[num_bytes..];
            let num_bytes = decompress_pfor_block_with::<BitPacker8x>(block, buffer, frequencies);
            block = &block[num_bytes..];
        }
        _ => unreachable!("validated posting block size should be supported"),
    }
    debug_assert!(
        block.is_empty(),
        "posting block has {} trailing bytes after decoding",
        block.len()
    );
}

pub fn decompress_posting_remainder(
    block: &[u8],
    n: usize,
    codec: PostingTailCodec,
    block_size: usize,
    doc_ids: &mut Vec<u32>,
    frequencies: &mut Vec<u32>,
) {
    let block = &block[posting_block_score_prefix_len(block_size)..];
    match codec {
        PostingTailCodec::Fixed32 => {
            decompress_raw_remainder(block, n, doc_ids);
            decompress_raw_remainder(&block[n * 4..], n, frequencies);
        }
        PostingTailCodec::VarintDelta => {
            let mut offset = 0usize;
            let mut previous = 0u32;
            for index in 0..n {
                let delta = decode_varint_u32(block, &mut offset)
                    .expect("posting tail doc ids should contain valid varints");
                let doc_id = if index == 0 {
                    delta
                } else {
                    previous
                        .checked_add(delta)
                        .expect("posting tail doc id delta should not overflow")
                };
                doc_ids.push(doc_id);
                previous = doc_id;
            }
            for _ in 0..n {
                let frequency = decode_varint_u32(block, &mut offset)
                    .expect("posting tail frequencies should contain valid varints");
                frequencies.push(frequency);
            }
            assert_eq!(
                offset,
                block.len(),
                "posting tail block has {} trailing bytes after decoding",
                block.len() - offset
            );
        }
    }
}

pub fn decode_full_posting_block(
    block: &[u8],
    doc_ids: &mut Vec<u32>,
    frequencies: &mut Vec<u32>,
    block_size: usize,
) {
    let mut buffer = [0u32; MAX_POSTING_BLOCK_SIZE];
    decompress_posting_block(block, &mut buffer, doc_ids, frequencies, block_size);
}

pub fn decompress_sorted_block(
    block: &[u8],
    buffer: &mut [u32; BLOCK_SIZE],
    res: &mut Vec<u32>,
) -> usize {
    decompress_sorted_block_with::<BitPacker4x>(block, buffer, res)
}

fn decompress_sorted_block_with<P: BitPacker>(
    block: &[u8],
    buffer: &mut [u32],
    res: &mut Vec<u32>,
) -> usize {
    debug_assert!(buffer.len() >= P::BLOCK_LEN);
    let buffer = &mut buffer[..P::BLOCK_LEN];
    let compressor = P::new();
    let initial = u32::from_le_bytes(block[0..4].try_into().unwrap());
    let num_bits = block[4];
    let num_bytes = compressor.decompress_sorted(initial, &block[5..], buffer, num_bits);
    res.extend_from_slice(&buffer[..]);
    5 + num_bytes
}

fn decompress_block_with<P: BitPacker>(
    block: &[u8],
    buffer: &mut [u32],
    res: &mut Vec<u32>,
) -> usize {
    debug_assert!(buffer.len() >= P::BLOCK_LEN);
    let buffer = &mut buffer[..P::BLOCK_LEN];
    let compressor = P::new();
    let num_bits = block[0];
    let num_bytes = compressor.decompress(&block[1..], buffer, num_bits);
    res.extend_from_slice(&buffer[..]);
    1 + num_bytes
}

pub fn decompress_raw_remainder(compressed: &[u8], n: usize, dest: &mut Vec<u32>) {
    for bytes in compressed.chunks_exact(4).take(n) {
        let data = u32::from_le_bytes(bytes.try_into().unwrap());
        dest.push(data);
    }
}

pub fn read_posting_tail_first_doc(
    block: &[u8],
    codec: PostingTailCodec,
    block_size: usize,
) -> u32 {
    let prefix = posting_block_score_prefix_len(block_size);
    match codec {
        PostingTailCodec::Fixed32 => {
            u32::from_le_bytes(block[prefix..prefix + 4].try_into().unwrap())
        }
        PostingTailCodec::VarintDelta => {
            let mut offset = prefix;
            decode_varint_u32(block, &mut offset)
                .expect("posting tail block should contain a valid first doc id")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Array;
    use itertools::Itertools;
    use rand::Rng;

    #[test]
    fn test_compress_posting_list() -> Result<()> {
        let num_rows: usize = BLOCK_SIZE * 1024 - 7;
        let mut rng = rand::rng();
        let doc_ids: Vec<u32> = (0..num_rows)
            .map(|_| rng.random())
            .sorted_unstable()
            .collect();
        let frequencies: Vec<u32> = (0..num_rows)
            .map(|_| rng.random_range(1..=u32::MAX))
            .collect();
        let block_max_scores =
            (0..num_rows.div_ceil(BLOCK_SIZE)).map(|_| rng.random_range(0.0..1.0));
        let posting_list = compress_posting_list(
            doc_ids.len(),
            doc_ids.iter(),
            frequencies.iter(),
            block_max_scores,
        )?;
        assert_eq!(posting_list.len(), num_rows.div_ceil(BLOCK_SIZE));
        let compressed_size =
            posting_list.value_data().len() + posting_list.value_offsets().len() * 8;
        let original_size = 2 * num_rows * 4;
        assert!(
            compressed_size < original_size,
            "compressed size {} should be less than original size {}",
            compressed_size,
            original_size
        );

        let (decompressed_doc_ids, decompressed_frequencies) =
            decompress_posting_list(num_rows as u32, &posting_list)?;
        assert_eq!(doc_ids, decompressed_doc_ids);
        assert_eq!(frequencies, decompressed_frequencies);
        Ok(())
    }

    #[test]
    fn test_compress_posting_list_supported_block_sizes() -> Result<()> {
        for block_size in [128, 256] {
            let num_rows: usize = block_size * 2 + 7;
            let doc_ids = (0..num_rows as u32).collect::<Vec<_>>();
            let frequencies = (0..num_rows as u32)
                .map(|value| value % 7 + 1)
                .collect::<Vec<_>>();
            let block_max_scores =
                (0..num_rows.div_ceil(block_size)).map(|value| value as f32 + 1.0);

            let posting_list = compress_posting_list_with_tail_codec_and_block_size(
                doc_ids.len(),
                doc_ids.iter(),
                frequencies.iter(),
                block_max_scores,
                PostingTailCodec::VarintDelta,
                block_size,
            )?;
            assert_eq!(posting_list.len(), num_rows.div_ceil(block_size));

            let (decoded_doc_ids, decoded_frequencies) =
                decompress_posting_list_with_tail_codec_and_block_size(
                    num_rows as u32,
                    &posting_list,
                    PostingTailCodec::VarintDelta,
                    block_size,
                )?;
            assert_eq!(decoded_doc_ids, doc_ids);
            assert_eq!(decoded_frequencies, frequencies);
        }
        Ok(())
    }

    #[test]
    fn test_256_posting_block_uses_single_physical_bitpack_chunk() -> Result<()> {
        let block_size = BitPacker8x::BLOCK_LEN;
        let doc_ids = (0..block_size as u32).collect::<Vec<_>>();
        let frequencies = (0..block_size as u32)
            .map(|value| value % 13 + 1)
            .collect::<Vec<_>>();

        let posting_list = compress_posting_list_with_tail_codec_and_block_size(
            doc_ids.len(),
            doc_ids.iter(),
            frequencies.iter(),
            std::iter::once(1.0),
            PostingTailCodec::VarintDelta,
            block_size,
        )?;
        assert_eq!(posting_list.len(), 1);

        let block = posting_list.value(0);
        // 256-doc blocks carry no block-max-score prefix (impacts supply the
        // per-block bound): [first_doc u32][doc num_bits u8][doc payload]...
        let doc_num_bits = block[4];
        let doc_bytes = BitPacker8x::compressed_block_size(doc_num_bits);
        let freq_header_offset = 5 + doc_bytes;
        // 256-doc blocks use patched FOR for frequencies:
        // [width u8][exception_count u8][body][exceptions...]
        let freq_num_bits = block[freq_header_offset];
        let exception_count = block[freq_header_offset + 1] as usize;
        assert_eq!(exception_count, 0, "uniform freqs need no exceptions");
        let freq_bytes = BitPacker8x::compressed_block_size(freq_num_bits);
        assert_eq!(block.len(), freq_header_offset + 2 + freq_bytes);

        let (decoded_doc_ids, decoded_frequencies) =
            decompress_posting_list_with_tail_codec_and_block_size(
                doc_ids.len() as u32,
                &posting_list,
                PostingTailCodec::VarintDelta,
                block_size,
            )?;
        assert_eq!(decoded_doc_ids, doc_ids);
        assert_eq!(decoded_frequencies, frequencies);
        Ok(())
    }

    #[test]
    fn test_compress_posting_list_fixed32_tail_still_roundtrips() -> Result<()> {
        let doc_ids = vec![3_u32, 10_u32, 24_u32];
        let frequencies = vec![1_u32, 7_u32, 2_u32];
        let posting_list = compress_posting_list_with_tail_codec(
            doc_ids.len(),
            doc_ids.iter(),
            frequencies.iter(),
            std::iter::once(1.0_f32),
            PostingTailCodec::Fixed32,
        )?;
        let (decoded_doc_ids, decoded_frequencies) = decompress_posting_list_with_tail_codec(
            doc_ids.len() as u32,
            &posting_list,
            PostingTailCodec::Fixed32,
        )?;
        assert_eq!(decoded_doc_ids, doc_ids);
        assert_eq!(decoded_frequencies, frequencies);
        Ok(())
    }

    #[test]
    fn test_compress_positions() -> Result<()> {
        let num_positions: usize = BLOCK_SIZE * 2 - 7;
        let mut rng = rand::rng();
        let positions: Vec<u32> = (0..num_positions)
            .map(|_| rng.random())
            .sorted_unstable()
            .collect();
        let compressed = compress_positions(&positions)?;
        assert_eq!(compressed.len(), num_positions.div_ceil(BLOCK_SIZE) + 1);
        let compressed_size = compressed.value_data().len() + compressed.value_offsets().len() * 8;
        let original_size = 2 * num_positions * 4;
        assert!(
            compressed_size < original_size,
            "compressed size {} should be less than original size {}",
            compressed_size,
            original_size
        );

        let decompressed_positions = decompress_positions(&compressed);
        assert_eq!(positions, decompressed_positions);
        assert_eq!(positions.len(), num_positions);
        Ok(())
    }

    /// Per-doc seek decoding of a PackedDelta position block must return
    /// exactly the same positions as decoding the whole block, for every doc,
    /// across group-boundary-straddling docs and varint tails.
    #[test]
    fn test_packed_position_doc_seek_matches_block_decode() -> Result<()> {
        let mut rng = rand::rng();
        // Frequency shapes: tiny blocks (tail only), exactly one group, a doc
        // straddling group boundaries, and a large multi-group block.
        let freq_shapes: Vec<Vec<u32>> = vec![
            vec![1],
            vec![3, 1, 5],
            vec![64, 64],
            vec![100, 60, 40],
            (0..256u32).map(|i| (i % 7) + 1).collect(),
            vec![300, 2, 129, 1, 77],
        ];
        for frequencies in freq_shapes {
            let total: usize = frequencies.iter().map(|&f| f as usize).sum();
            // Positions ascend within each doc; docs are independent.
            let mut positions = Vec::with_capacity(total);
            for &freq in &frequencies {
                let mut current = rng.random_range(0..1000u32);
                for _ in 0..freq {
                    positions.push(current);
                    current += rng.random_range(1..50u32);
                }
            }

            let mut encoded = Vec::new();
            encode_position_stream_block_into(
                &positions,
                &frequencies,
                PositionStreamCodec::PackedDelta,
                &mut encoded,
            )?;

            let mut whole = Vec::new();
            decode_position_stream_block(
                &encoded,
                &frequencies,
                PositionStreamCodec::PackedDelta,
                &mut whole,
            )?;
            assert_eq!(whole, positions);

            let mut group_offsets = vec![0usize];
            let mut unpacked_group = Box::new([0u32; BLOCK_SIZE]);
            let mut unpacked_group_idx = None;
            let mut tail_cache = Vec::new();
            let mut scratch = Vec::new();
            let mut delta_start = 0usize;
            for &freq in &frequencies {
                let delta_end = delta_start + freq as usize;
                seek_packed_doc_positions(
                    &encoded,
                    total,
                    delta_start..delta_end,
                    &mut group_offsets,
                    &mut unpacked_group,
                    &mut unpacked_group_idx,
                    &mut tail_cache,
                    &mut scratch,
                )?;
                assert_eq!(
                    scratch,
                    whole[delta_start..delta_end],
                    "doc positions mismatch for range {delta_start}..{delta_end} freqs={frequencies:?}"
                );
                delta_start = delta_end;
            }
        }
        Ok(())
    }

    #[test]
    fn test_packed_position_decoders_reject_malformed_groups() {
        let frequencies = [BLOCK_SIZE as u32];
        for encoded in [&[1_u8][..], &[33_u8][..]] {
            let mut decoded = Vec::new();
            assert!(
                decode_position_stream_block(
                    encoded,
                    &frequencies,
                    PositionStreamCodec::PackedDelta,
                    &mut decoded,
                )
                .is_err()
            );

            let mut group_offsets = vec![0usize];
            let mut unpacked_group = Box::new([0u32; BLOCK_SIZE]);
            let mut unpacked_group_idx = None;
            let mut tail_cache = Vec::new();
            let mut scratch = Vec::new();
            assert!(
                seek_packed_doc_positions(
                    encoded,
                    BLOCK_SIZE,
                    0..BLOCK_SIZE,
                    &mut group_offsets,
                    &mut unpacked_group,
                    &mut unpacked_group_idx,
                    &mut tail_cache,
                    &mut scratch,
                )
                .is_err()
            );
        }
    }

    #[test]
    fn test_encode_position_stream_block_roundtrip() -> Result<()> {
        let frequencies = vec![1, 3, 2, 4];
        let positions = vec![7, 1, 3, 8, 2, 100, 0, 4, 9, 25];
        for codec in [
            PositionStreamCodec::VarintDocDelta,
            PositionStreamCodec::PackedDelta,
        ] {
            let mut encoded = Vec::new();
            encode_position_stream_block_into(&positions, &frequencies, codec, &mut encoded)?;
            let mut decoded = Vec::new();
            decode_position_stream_block(&encoded, &frequencies, codec, &mut decoded)?;
            assert_eq!(decoded, positions);
            assert!(encoded.len() < positions.len() * std::mem::size_of::<u32>());
        }
        Ok(())
    }

    #[test]
    fn test_incremental_position_block_builder_matches_batch_encoder() -> Result<()> {
        let frequencies = vec![1, 3, 2, 4, 1, 5];
        let positions = vec![7, 1, 3, 8, 2, 100, 0, 4, 9, 25, 11, 2, 6, 7, 10, 15];

        let mut builder = PositionBlockBuilder::new(PositionStreamCodec::PackedDelta);
        let mut offset = 0usize;
        for &frequency in &frequencies {
            let end = offset + frequency as usize;
            builder.append_doc_positions(&positions[offset..end])?;
            offset = end;
        }

        let incremental = builder.finish();
        let mut batch = Vec::new();
        encode_position_stream_block_into(
            &positions,
            &frequencies,
            PositionStreamCodec::PackedDelta,
            &mut batch,
        )?;
        assert_eq!(incremental, batch);
        Ok(())
    }
}
