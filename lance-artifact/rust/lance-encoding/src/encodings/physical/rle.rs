// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! # RLE (Run-Length Encoding)
//!
//! RLE compression for Lance, optimized for data with repeated values.
//!
//! ## Encoding Format
//!
//! RLE uses a dual-buffer format to store compressed data:
//!
//! - **Values Buffer**: Stores unique values in their original data type
//! - **Lengths Buffer**: Stores the repeat count for each value as u8, u16, or u32
//!
//! ### Example
//!
//! Input data: `[1, 1, 1, 2, 2, 3, 3, 3, 3]`
//!
//! Encoded as:
//! - Values buffer: `[1, 2, 3]` (3 × 4 bytes for i32)
//! - Lengths buffer: `[3, 2, 4]` (3 × 1 byte for u8 in compatibility mode)
//!
//! ### Long Run Handling
//!
//! In compatibility mode, when a run exceeds 255 values, it is split into multiple
//! runs of 255 followed by a final run with the remainder. RLE v2 can use u16 or
//! u32 run lengths to reduce this splitting.
//!
//! ## Supported Types
//!
//! RLE supports all fixed-width primitive types:
//! - 8-bit: u8, i8
//! - 16-bit: u16, i16
//! - 32-bit: u32, i32, f32
//! - 64-bit: u64, i64, f64
//!
//! ## Compression Strategy
//!
//! RLE is automatically selected when:
//! - The run count (number of value transitions) < 50% of total values
//! - This indicates sufficient repetition for RLE to be effective
//!
//! ## MiniBlock Chunk Handling
//!
//! When used in the miniblock path, all chunks share two global buffers (values and lengths).
//! Each chunk's `buffer_sizes` identifies its slice within those global buffers. Non-last chunks
//! contain a power-of-2 number of values.
//!
//! NOTE: The current encoder uses a 2048-value cap per chunk as a workaround for
//! <https://github.com/lancedb/lance/issues/4429>.
//!
//! ## Block Format
//!
//! When used in the block compression path, the encoded output is a single buffer:
//! `[8-byte header: values buffer size][values buffer][run_lengths buffer]`.

use arrow_buffer::{ArrowNativeType, ScalarBuffer};
use log::trace;

use crate::buffer::LanceBuffer;
use crate::compression::{BlockCompressor, BlockDecompressor, MiniBlockDecompressor};
use crate::data::DataBlock;
use crate::data::{BlockInfo, FixedWidthDataBlock};
use crate::encodings::logical::primitive::miniblock::{
    MAX_MINIBLOCK_BYTES, MAX_MINIBLOCK_VALUES, MiniBlockChunk, MiniBlockCompressed,
    MiniBlockCompressionContext, MiniBlockCompressor,
};
use crate::encodings::physical::block::{CompressionConfig, GeneralBufferCompressor};
use crate::format::ProtobufUtils21;
use crate::format::pb21::CompressiveEncoding;

use lance_core::{Error, Result};

/// Width used to encode RLE run lengths.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RunLengthWidth {
    /// Compatibility mode. Runs longer than 255 values are split.
    U8,
    /// RLE v2 mode for runs up to 65,535 values per entry.
    U16,
    /// RLE v2 mode for runs up to 4,294,967,295 values per entry.
    U32,
}

impl RunLengthWidth {
    pub(crate) fn from_bits(bits_per_value: u64) -> Option<Self> {
        match bits_per_value {
            8 => Some(Self::U8),
            16 => Some(Self::U16),
            32 => Some(Self::U32),
            _ => None,
        }
    }

    pub(crate) fn bits_per_value(self) -> u64 {
        match self {
            Self::U8 => 8,
            Self::U16 => 16,
            Self::U32 => 32,
        }
    }

    fn bytes_per_value(self) -> usize {
        match self {
            Self::U8 => 1,
            Self::U16 => 2,
            Self::U32 => 4,
        }
    }

    fn max_run_length(self) -> u64 {
        match self {
            Self::U8 => u8::MAX as u64,
            Self::U16 => u16::MAX as u64,
            Self::U32 => u32::MAX as u64,
        }
    }

    fn write_length(self, length: u64, dst: &mut Vec<u8>) {
        match self {
            Self::U8 => dst.push(length as u8),
            Self::U16 => dst.extend_from_slice(&(length as u16).to_le_bytes()),
            Self::U32 => dst.extend_from_slice(&(length as u32).to_le_bytes()),
        }
    }

    fn read_length(self, bytes: &[u8]) -> u64 {
        match self {
            Self::U8 => bytes[0] as u64,
            Self::U16 => u16::from_le_bytes([bytes[0], bytes[1]]) as u64,
            Self::U32 => u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as u64,
        }
    }
}

const RUN_LENGTH_WIDTHS: [RunLengthWidth; 3] =
    [RunLengthWidth::U8, RunLengthWidth::U16, RunLengthWidth::U32];

/// Select the lowest-cost run length width from precomputed entry counts.
pub(crate) fn select_run_length_width_from_entries(
    entries: &[u64],
    bits_per_value: u64,
) -> Result<(RunLengthWidth, u128)> {
    if entries.len() != RUN_LENGTH_WIDTHS.len() {
        return Err(Error::invalid_input_source(
            format!(
                "RLE run length entry statistics must have {} values, got {}",
                RUN_LENGTH_WIDTHS.len(),
                entries.len()
            )
            .into(),
        ));
    }

    if !matches!(bits_per_value, 8 | 16 | 32 | 64) {
        return Err(Error::invalid_input_source(
            format!("RLE encoding bits_per_value must be 8, 16, 32, or 64, got {bits_per_value}")
                .into(),
        ));
    }

    let mut best_width = RUN_LENGTH_WIDTHS[0];
    let mut best_cost = rle_encoded_size_from_entries(entries[0], bits_per_value, best_width);
    for (&width, &entry_count) in RUN_LENGTH_WIDTHS.iter().zip(entries.iter()).skip(1) {
        let cost = rle_encoded_size_from_entries(entry_count, bits_per_value, width);
        if cost < best_cost {
            best_width = width;
            best_cost = cost;
        }
    }

    Ok((best_width, best_cost))
}

pub(crate) fn rle_encoded_size_from_entries(
    entry_count: u64,
    bits_per_value: u64,
    run_length_width: RunLengthWidth,
) -> u128 {
    let bytes_per_value = (bits_per_value / 8) as u128;
    let bytes_per_length = run_length_width.bytes_per_value() as u128;
    (entry_count as u128) * (bytes_per_value + bytes_per_length)
}

pub(crate) fn run_length_width_index(run_length_width: RunLengthWidth) -> usize {
    match run_length_width {
        RunLengthWidth::U8 => 0,
        RunLengthWidth::U16 => 1,
        RunLengthWidth::U32 => 2,
    }
}

pub(crate) fn select_run_length_width(
    data: &LanceBuffer,
    num_values: u64,
    bits_per_value: u64,
    max_segment_values: Option<u64>,
) -> Result<(RunLengthWidth, u128)> {
    let entries = collect_run_length_entries(data, num_values, bits_per_value, max_segment_values)?;
    select_run_length_width_from_entries(&entries, bits_per_value)
}

pub(crate) fn rle_encoded_size(
    data: &LanceBuffer,
    num_values: u64,
    bits_per_value: u64,
    max_segment_values: Option<u64>,
    run_length_width: RunLengthWidth,
) -> Result<u128> {
    let entries = collect_run_length_entries(data, num_values, bits_per_value, max_segment_values)?;
    let width_idx = run_length_width_index(run_length_width);
    Ok(rle_encoded_size_from_entries(
        entries[width_idx],
        bits_per_value,
        run_length_width,
    ))
}

fn collect_run_length_entries(
    data: &LanceBuffer,
    num_values: u64,
    bits_per_value: u64,
    max_segment_values: Option<u64>,
) -> Result<[u64; 3]> {
    let num_values = usize::try_from(num_values).map_err(|_| {
        Error::invalid_input_source(
            format!("RLE num_values does not fit in usize: {num_values}").into(),
        )
    })?;

    macro_rules! collect_entries {
        ($ty:ty) => {{
            let type_size = std::mem::size_of::<$ty>();
            let expected_bytes = num_values.checked_mul(type_size).ok_or_else(|| {
                Error::invalid_input_source(
                    format!(
                        "RLE input byte length overflow: {num_values} values of {type_size} bytes"
                    )
                    .into(),
                )
            })?;
            if data.len() != expected_bytes {
                return Err(Error::invalid_input_source(
                    format!(
                        "RLE input data size mismatch: {} bytes for {} values of {} bytes",
                        data.len(),
                        num_values,
                        type_size
                    )
                    .into(),
                ));
            }
            let values = data.borrow_to_typed_slice::<$ty>();
            let values = values.get(..num_values).ok_or_else(|| {
                Error::invalid_input_source(
                    format!(
                        "RLE data has {} values but {} were expected",
                        values.len(),
                        num_values
                    )
                    .into(),
                )
            })?;
            Ok(collect_run_length_entries_from_slice(
                values,
                max_segment_values,
            ))
        }};
    }

    match bits_per_value {
        8 => collect_entries!(u8),
        16 => collect_entries!(u16),
        32 => collect_entries!(u32),
        64 => collect_entries!(u64),
        _ => Err(Error::invalid_input_source(
            format!("RLE encoding bits_per_value must be 8, 16, 32, or 64, got {bits_per_value}")
                .into(),
        )),
    }
}

fn collect_run_length_entries_from_slice<T: PartialEq + Copy>(
    values: &[T],
    max_segment_values: Option<u64>,
) -> [u64; 3] {
    if values.is_empty() {
        return [0; 3];
    }

    let mut entries = [0u64; 3];
    let mut prev = values[0];
    let mut current_length = 1u64;

    for &value in &values[1..] {
        if value != prev {
            accumulate_run_length_entries(current_length, max_segment_values, &mut entries);
            prev = value;
            current_length = 1;
        } else {
            current_length += 1;
        }
    }
    accumulate_run_length_entries(current_length, max_segment_values, &mut entries);

    entries
}

pub(crate) fn accumulate_run_length_entries(
    run_length: u64,
    max_segment_values: Option<u64>,
    entries: &mut [u64; 3],
) {
    let max_segment_values = max_segment_values.unwrap_or(run_length).max(1);
    let mut remaining = run_length;
    while remaining > 0 {
        let segment = remaining.min(max_segment_values);
        for (idx, width) in RUN_LENGTH_WIDTHS.iter().enumerate() {
            let entry_count = segment.div_ceil(width.max_run_length());
            entries[idx] = entries[idx].saturating_add(entry_count);
        }
        remaining -= segment;
    }
}

/// RLE encoder for miniblock format
#[derive(Debug)]
pub struct RleEncoder {
    run_length_width: RunLengthWidth,
    values_compression: Option<CompressionConfig>,
    run_lengths_compression: Option<CompressionConfig>,
    use_child_bitpacking: bool,
}

#[derive(Clone)]
struct RleChildCandidate {
    encoding: CompressiveEncoding,
    data: LanceBuffer,
    chunk_sizes: Vec<u32>,
    size: usize,
    requires_num_values: bool,
}

impl Default for RleEncoder {
    fn default() -> Self {
        Self::new()
    }
}

impl RleEncoder {
    pub fn new() -> Self {
        Self {
            run_length_width: RunLengthWidth::U8,
            values_compression: None,
            run_lengths_compression: None,
            use_child_bitpacking: false,
        }
    }

    pub(crate) fn with_run_length_width(run_length_width: RunLengthWidth) -> Self {
        Self {
            run_length_width,
            values_compression: None,
            run_lengths_compression: None,
            use_child_bitpacking: false,
        }
    }

    pub(crate) fn with_child_encoding(
        run_length_width: RunLengthWidth,
        values_compression: Option<CompressionConfig>,
        run_lengths_compression: Option<CompressionConfig>,
        use_child_bitpacking: bool,
    ) -> Self {
        Self {
            run_length_width,
            values_compression,
            run_lengths_compression,
            use_child_bitpacking,
        }
    }

    fn encode_data(
        &self,
        data: &LanceBuffer,
        num_values: u64,
        bits_per_value: u64,
    ) -> Result<(Vec<LanceBuffer>, Vec<MiniBlockChunk>)> {
        if num_values == 0 {
            return Ok((Vec::new(), Vec::new()));
        }

        let num_values = usize::try_from(num_values).map_err(|_| {
            Error::invalid_input_source(
                format!("RLE num_values does not fit in usize: {num_values}").into(),
            )
        })?;
        let bytes_per_value = (bits_per_value / 8) as usize;
        let bytes_per_length = self.run_length_width.bytes_per_value();

        // Pre-allocate global buffers with estimated capacity
        // Assume average compression ratio of ~10:1 (10 values per run)
        let estimated_runs = num_values / 10;
        let mut all_values = Vec::with_capacity(estimated_runs * bytes_per_value);
        let mut all_lengths = Vec::with_capacity(estimated_runs * bytes_per_length);
        let mut chunks = Vec::new();

        let mut offset = 0usize;
        let mut values_remaining = num_values;

        while values_remaining > 0 {
            let values_start = all_values.len();
            let lengths_start = all_lengths.len();

            let (_num_runs, values_processed, is_last_chunk) = match bits_per_value {
                8 => self.encode_chunk_rolling::<u8>(
                    data,
                    offset,
                    values_remaining,
                    &mut all_values,
                    &mut all_lengths,
                ),
                16 => self.encode_chunk_rolling::<u16>(
                    data,
                    offset,
                    values_remaining,
                    &mut all_values,
                    &mut all_lengths,
                ),
                32 => self.encode_chunk_rolling::<u32>(
                    data,
                    offset,
                    values_remaining,
                    &mut all_values,
                    &mut all_lengths,
                ),
                64 => self.encode_chunk_rolling::<u64>(
                    data,
                    offset,
                    values_remaining,
                    &mut all_values,
                    &mut all_lengths,
                ),
                _ => {
                    return Err(Error::invalid_input_source(
                        format!(
                            "RLE encoding bits_per_value must be 8, 16, 32, or 64, got {bits_per_value}"
                        )
                        .into(),
                    ));
                }
            };

            if values_processed == 0 {
                // A non-final chunk needs at least two values because log_num_values == 0
                // identifies the final chunk. Report an error instead of returning partial data.
                return Err(Error::internal(format!(
                    "RLE encoder made no progress: values_remaining={values_remaining}, \
                     offset={offset}, data_len={}, bits_per_value={bits_per_value}, \
                     max_miniblock_values={}",
                    data.len(),
                    *MAX_MINIBLOCK_VALUES
                )));
            }

            let log_num_values = if is_last_chunk {
                0
            } else {
                assert!(
                    values_processed.is_power_of_two(),
                    "Non-last chunk must have power-of-2 values"
                );
                values_processed.ilog2() as u8
            };

            let values_size = all_values.len() - values_start;
            let lengths_size = all_lengths.len() - lengths_start;

            let chunk = MiniBlockChunk {
                buffer_sizes: vec![values_size as u32, lengths_size as u32],
                log_num_values,
            };

            chunks.push(chunk);

            offset += values_processed;
            values_remaining -= values_processed;
        }

        // Return exactly two buffers: values and lengths
        Ok((
            vec![
                LanceBuffer::from(all_values),
                LanceBuffer::from(all_lengths),
            ],
            chunks,
        ))
    }

    fn encode_block_data(
        &self,
        data: &LanceBuffer,
        num_values: u64,
        bits_per_value: u64,
    ) -> Result<Vec<LanceBuffer>> {
        match bits_per_value {
            8 => self.encode_block_data_generic::<u8>(data, num_values),
            16 => self.encode_block_data_generic::<u16>(data, num_values),
            32 => self.encode_block_data_generic::<u32>(data, num_values),
            64 => self.encode_block_data_generic::<u64>(data, num_values),
            _ => Err(Error::invalid_input_source(
                format!(
                    "RLE encoding bits_per_value must be 8, 16, 32, or 64, got {bits_per_value}"
                )
                .into(),
            )),
        }
    }

    fn encode_block_data_generic<T>(
        &self,
        data: &LanceBuffer,
        num_values: u64,
    ) -> Result<Vec<LanceBuffer>>
    where
        T: bytemuck::Pod + PartialEq + Copy + ArrowNativeType,
    {
        let num_values = usize::try_from(num_values).map_err(|_| {
            Error::invalid_input_source(
                format!("RLE num_values does not fit in usize: {num_values}").into(),
            )
        })?;
        let type_size = std::mem::size_of::<T>();
        let expected_bytes = num_values.checked_mul(type_size).ok_or_else(|| {
            Error::invalid_input_source(
                format!("RLE input byte length overflow: {num_values} values of {type_size} bytes")
                    .into(),
            )
        })?;
        if data.len() != expected_bytes {
            return Err(Error::invalid_input_source(
                format!(
                    "RLE input data size mismatch: {} bytes for {} values of {} bytes",
                    data.len(),
                    num_values,
                    type_size
                )
                .into(),
            ));
        }
        if num_values == 0 {
            return Ok(vec![LanceBuffer::empty(), LanceBuffer::empty()]);
        }

        let values_ref = data.borrow_to_typed_slice::<T>();
        let values = values_ref.as_ref();
        let estimated_runs = num_values / 10;
        let mut all_values = Vec::with_capacity(estimated_runs * type_size);
        let mut all_lengths =
            Vec::with_capacity(estimated_runs * self.run_length_width.bytes_per_value());
        self.encode_values(values, &mut all_values, &mut all_lengths);
        Ok(vec![
            LanceBuffer::from(all_values),
            LanceBuffer::from(all_lengths),
        ])
    }

    /// Encodes the largest valid mini-block prefix from `offset`.
    fn encode_chunk_rolling<T>(
        &self,
        data: &LanceBuffer,
        offset: usize,
        values_remaining: usize,
        all_values: &mut Vec<u8>,
        all_lengths: &mut Vec<u8>,
    ) -> (usize, usize, bool)
    where
        T: bytemuck::Pod + PartialEq + Copy + std::fmt::Debug + ArrowNativeType,
    {
        let type_size = std::mem::size_of::<T>();
        let chunk_start = offset * type_size;
        let max_by_count = *MAX_MINIBLOCK_VALUES as usize;
        let max_values = values_remaining.min(max_by_count);
        let chunk_end = chunk_start + max_values * type_size;

        if chunk_start >= data.len() {
            return (0, 0, false);
        }

        let chunk_len = chunk_end.min(data.len()) - chunk_start;
        let chunk_buffer = data.slice_with_length(chunk_start, chunk_len);
        let typed_data_ref = chunk_buffer.borrow_to_typed_slice::<T>();
        let typed_data: &[T] = typed_data_ref.as_ref();
        let max_values = max_values.min(typed_data.len());

        if typed_data.is_empty() {
            return (0, 0, false);
        }

        let values_start = all_values.len();
        let all_remaining_values_fit = values_remaining <= max_by_count;
        let encoded_size = self.encoded_size(&typed_data[..max_values]);
        let (values_to_encode, is_last_chunk) = if all_remaining_values_fit
            && encoded_size <= MAX_MINIBLOCK_BYTES as usize
        {
            (max_values, true)
        } else if let Some(values_to_encode) = self.largest_power_of_two_prefix::<T>(typed_data) {
            (values_to_encode, false)
        } else {
            return (0, 0, false);
        };

        self.encode_values(&typed_data[..values_to_encode], all_values, all_lengths);

        let num_runs = (all_values.len() - values_start) / type_size;
        (num_runs, values_to_encode, is_last_chunk)
    }

    fn largest_power_of_two_prefix<T>(&self, values: &[T]) -> Option<usize>
    where
        T: bytemuck::Pod + PartialEq + Copy,
    {
        let max_prefix = values.len().min(*MAX_MINIBLOCK_VALUES as usize);
        let mut prefix = 1usize << max_prefix.ilog2();
        while prefix > 1 {
            if self.encoded_size(&values[..prefix]) <= MAX_MINIBLOCK_BYTES as usize {
                return Some(prefix);
            }
            prefix >>= 1;
        }
        None
    }

    fn encoded_size<T>(&self, values: &[T]) -> usize
    where
        T: bytemuck::Pod + PartialEq + Copy,
    {
        if values.is_empty() {
            return 0;
        }

        let mut current_value = values[0];
        let mut current_length = 1u64;
        let mut encoded_size = 0usize;

        for &value in values.iter().skip(1) {
            if value == current_value {
                current_length += 1;
            } else {
                encoded_size += self.run_size::<T>(current_length);
                current_value = value;
                current_length = 1;
            }
        }
        encoded_size += self.run_size::<T>(current_length);
        encoded_size
    }

    fn run_size<T>(&self, length: u64) -> usize
    where
        T: bytemuck::Pod,
    {
        let type_size = std::mem::size_of::<T>();
        let run_chunks = length.div_ceil(self.run_length_width.max_run_length()) as usize;
        run_chunks * (type_size + self.run_length_width.bytes_per_value())
    }

    fn encode_values<T>(&self, values: &[T], all_values: &mut Vec<u8>, all_lengths: &mut Vec<u8>)
    where
        T: bytemuck::Pod + PartialEq + Copy,
    {
        if values.is_empty() {
            return;
        }

        let mut current_value = values[0];
        let mut current_length = 1u64;

        for &value in values.iter().skip(1) {
            if value == current_value {
                current_length += 1;
            } else {
                self.add_run(&current_value, current_length, all_values, all_lengths);
                current_value = value;
                current_length = 1;
            }
        }
        self.add_run(&current_value, current_length, all_values, all_lengths);
    }

    fn add_run<T>(
        &self,
        value: &T,
        length: u64,
        all_values: &mut Vec<u8>,
        all_lengths: &mut Vec<u8>,
    ) -> usize
    where
        T: bytemuck::Pod,
    {
        let value_bytes = bytemuck::bytes_of(value);
        let type_size = std::mem::size_of::<T>();
        let max_run_length = self.run_length_width.max_run_length();
        let num_full_chunks = (length / max_run_length) as usize;
        let remainder = length % max_run_length;

        let total_chunks = num_full_chunks + if remainder > 0 { 1 } else { 0 };
        all_values.reserve(total_chunks * type_size);
        all_lengths.reserve(total_chunks * self.run_length_width.bytes_per_value());

        for _ in 0..num_full_chunks {
            all_values.extend_from_slice(value_bytes);
            self.run_length_width
                .write_length(max_run_length, all_lengths);
        }

        if remainder > 0 {
            all_values.extend_from_slice(value_bytes);
            self.run_length_width.write_length(remainder, all_lengths);
        }

        total_chunks * (type_size + self.run_length_width.bytes_per_value())
    }

    fn flat_child_candidate(
        buffers: &[LanceBuffer],
        chunks: &[MiniBlockChunk],
        buffer_index: usize,
        bits_per_value: u64,
    ) -> RleChildCandidate {
        RleChildCandidate {
            encoding: ProtobufUtils21::flat(bits_per_value, None),
            data: buffers[buffer_index].clone(),
            chunk_sizes: chunks
                .iter()
                .map(|chunk| chunk.buffer_sizes[buffer_index])
                .collect(),
            size: buffers[buffer_index].len(),
            requires_num_values: false,
        }
    }

    fn general_child_candidate(
        buffers: &[LanceBuffer],
        chunks: &[MiniBlockChunk],
        buffer_index: usize,
        bits_per_value: u64,
        compression: CompressionConfig,
    ) -> Result<Option<RleChildCandidate>> {
        if buffers.is_empty() || buffers[buffer_index].is_empty() {
            return Ok(None);
        };

        let compressor = GeneralBufferCompressor::get_compressor(compression)?;
        let original = &buffers[buffer_index];
        let mut compressed = Vec::new();
        let mut offset = 0usize;
        let mut total_original_size = 0usize;
        let mut compressed_sizes = Vec::with_capacity(chunks.len());

        for chunk in chunks.iter() {
            let chunk_size = chunk.buffer_sizes[buffer_index] as usize;
            let end = offset.checked_add(chunk_size).ok_or_else(|| {
                Error::invalid_input_source("RLE child buffer offset overflow".into())
            })?;
            if end > original.len() {
                return Err(Error::invalid_input_source(
                    format!(
                        "RLE child buffer {} chunk size exceeds buffer length: end {}, len {}",
                        buffer_index,
                        end,
                        original.len()
                    )
                    .into(),
                ));
            }

            let start = compressed.len();
            compressor.compress(&original.as_ref()[offset..end], &mut compressed)?;
            let compressed_size = compressed.len() - start;
            let compressed_size = u32::try_from(compressed_size).map_err(|_| {
                Error::invalid_input_source(
                    format!(
                        "RLE child buffer {} compressed chunk is too large: {} bytes",
                        buffer_index, compressed_size
                    )
                    .into(),
                )
            })?;
            compressed_sizes.push(compressed_size);
            total_original_size += chunk_size;
            offset = end;
        }

        if compressed.len() >= total_original_size {
            return Ok(None);
        }

        let encoding =
            ProtobufUtils21::wrapped(compression, ProtobufUtils21::flat(bits_per_value, None))?;
        Ok(Some(
            RleChildCandidate {
                encoding,
                data: LanceBuffer::from(compressed),
                chunk_sizes: compressed_sizes,
                size: 0,
                requires_num_values: false,
            }
            .with_size_from_data(),
        ))
    }

    #[cfg(feature = "bitpacking")]
    fn bitpacked_child_candidate(
        buffers: &[LanceBuffer],
        chunks: &[MiniBlockChunk],
        buffer_index: usize,
        bits_per_value: u64,
    ) -> Result<Option<RleChildCandidate>> {
        let original = &buffers[buffer_index];
        if original.is_empty() {
            return Ok(None);
        }
        let packed_bits = Self::required_bits(original, bits_per_value)?;
        if packed_bits >= bits_per_value {
            return Ok(None);
        }

        let compressor = crate::encodings::physical::bitpacking::OutOfLineBitpacking::new(
            packed_bits,
            bits_per_value,
        );
        let mut packed = Vec::new();
        let mut offset = 0usize;
        let mut packed_sizes = Vec::with_capacity(chunks.len());
        let bytes_per_value = usize::try_from(bits_per_value / 8).map_err(|_| {
            Error::invalid_input_source(
                format!("RLE child bit width is too large: {bits_per_value}").into(),
            )
        })?;

        for chunk in chunks {
            let chunk_size = chunk.buffer_sizes[buffer_index] as usize;
            let end = offset.checked_add(chunk_size).ok_or_else(|| {
                Error::invalid_input_source("RLE child buffer offset overflow".into())
            })?;
            if end > original.len() {
                return Err(Error::invalid_input_source(
                    format!(
                        "RLE child buffer {} chunk size exceeds buffer length: end {}, len {}",
                        buffer_index,
                        end,
                        original.len()
                    )
                    .into(),
                ));
            }
            if bytes_per_value == 0 || !chunk_size.is_multiple_of(bytes_per_value) {
                return Err(Error::invalid_input_source(
                    format!(
                        "RLE child buffer {} chunk has invalid size {} for {} bits per value",
                        buffer_index, chunk_size, bits_per_value
                    )
                    .into(),
                ));
            }

            let child_values = (chunk_size / bytes_per_value) as u64;
            let block = DataBlock::FixedWidth(FixedWidthDataBlock {
                bits_per_value,
                data: original.slice_with_length(offset, chunk_size),
                num_values: child_values,
                block_info: BlockInfo::default(),
            });
            let chunk_packed = BlockCompressor::compress(&compressor, block)?;
            let packed_size = u32::try_from(chunk_packed.len()).map_err(|_| {
                Error::invalid_input_source(
                    format!(
                        "RLE child buffer {} bitpacked chunk is too large: {} bytes",
                        buffer_index,
                        chunk_packed.len()
                    )
                    .into(),
                )
            })?;
            packed_sizes.push(packed_size);
            packed.extend_from_slice(chunk_packed.as_ref());
            offset = end;
        }

        if packed.len() >= original.len() {
            return Ok(None);
        }

        Ok(Some(
            RleChildCandidate {
                encoding: ProtobufUtils21::out_of_line_bitpacking(
                    bits_per_value,
                    ProtobufUtils21::flat(packed_bits, None),
                ),
                data: LanceBuffer::from(packed),
                chunk_sizes: packed_sizes,
                size: 0,
                requires_num_values: true,
            }
            .with_size_from_data(),
        ))
    }

    #[cfg(feature = "bitpacking")]
    fn required_bits(buffer: &LanceBuffer, bits_per_value: u64) -> Result<u64> {
        let max_value = match bits_per_value {
            8 => buffer.as_ref().iter().map(|value| *value as u64).max(),
            16 => buffer
                .as_ref()
                .chunks_exact(2)
                .map(|value| u16::from_le_bytes(value.try_into().unwrap()) as u64)
                .max(),
            32 => buffer
                .as_ref()
                .chunks_exact(4)
                .map(|value| u32::from_le_bytes(value.try_into().unwrap()) as u64)
                .max(),
            64 => buffer
                .as_ref()
                .chunks_exact(8)
                .map(|value| u64::from_le_bytes(value.try_into().unwrap()))
                .max(),
            _ => {
                return Err(Error::invalid_input_source(
                    format!(
                        "RLE child bitpacking only supports 8, 16, 32, or 64-bit values, got {bits_per_value}"
                    )
                    .into(),
                ));
            }
        }
        .unwrap_or(0);
        Ok((u64::BITS - max_value.leading_zeros()).max(1) as u64)
    }

    fn child_candidates(
        buffers: &[LanceBuffer],
        chunks: &[MiniBlockChunk],
        buffer_index: usize,
        bits_per_value: u64,
        compression: Option<CompressionConfig>,
        use_child_bitpacking: bool,
    ) -> Result<Vec<RleChildCandidate>> {
        #[cfg(not(feature = "bitpacking"))]
        let _ = use_child_bitpacking;
        let mut candidates = vec![Self::flat_child_candidate(
            buffers,
            chunks,
            buffer_index,
            bits_per_value,
        )];
        if let Some(compression) = compression
            && let Some(candidate) = Self::general_child_candidate(
                buffers,
                chunks,
                buffer_index,
                bits_per_value,
                compression,
            )?
        {
            candidates.push(candidate);
        }
        #[cfg(feature = "bitpacking")]
        {
            if use_child_bitpacking
                && let Some(candidate) =
                    Self::bitpacked_child_candidate(buffers, chunks, buffer_index, bits_per_value)?
            {
                candidates.push(candidate);
            }
        }
        Ok(candidates)
    }

    fn select_child_candidates(
        values: Vec<RleChildCandidate>,
        run_lengths: Vec<RleChildCandidate>,
    ) -> (RleChildCandidate, RleChildCandidate) {
        let mut best: Option<(usize, usize, usize)> = None;
        for (value_idx, value) in values.iter().enumerate() {
            for (length_idx, length) in run_lengths.iter().enumerate() {
                if value.requires_num_values && length.requires_num_values {
                    continue;
                }
                let size = value.size + length.size;
                if best.is_none_or(|(_, _, best_size)| size < best_size) {
                    best = Some((value_idx, length_idx, size));
                }
            }
        }
        let (value_idx, length_idx, _) =
            best.expect("flat RLE child candidates should always be selectable");
        (values[value_idx].clone(), run_lengths[length_idx].clone())
    }

    pub(crate) fn selected_payload_size(&self, data: &FixedWidthDataBlock) -> Result<u128> {
        let (all_buffers, chunks) =
            self.encode_data(&data.data, data.num_values, data.bits_per_value)?;
        if all_buffers.is_empty() {
            return Ok(0);
        }

        let values_candidates = Self::child_candidates(
            &all_buffers,
            &chunks,
            0,
            data.bits_per_value,
            self.values_compression,
            self.use_child_bitpacking,
        )?;
        let run_lengths_candidates = Self::child_candidates(
            &all_buffers,
            &chunks,
            1,
            self.run_length_width.bits_per_value(),
            self.run_lengths_compression,
            self.use_child_bitpacking,
        )?;
        let (values, run_lengths) =
            Self::select_child_candidates(values_candidates, run_lengths_candidates);
        Ok((values.size as u128).saturating_add(run_lengths.size as u128))
    }
}

impl RleChildCandidate {
    fn with_size_from_data(mut self) -> Self {
        self.size = self.data.len();
        self
    }
}

impl MiniBlockCompressor for RleEncoder {
    fn compress(
        &self,
        _context: MiniBlockCompressionContext,
        data: DataBlock,
    ) -> Result<(MiniBlockCompressed, CompressiveEncoding)> {
        match data {
            DataBlock::FixedWidth(fixed_width) => {
                let num_values = fixed_width.num_values;
                let bits_per_value = fixed_width.bits_per_value;

                let (all_buffers, chunks) =
                    self.encode_data(&fixed_width.data, num_values, bits_per_value)?;
                if all_buffers.is_empty() {
                    let compressed = MiniBlockCompressed {
                        data: all_buffers,
                        chunks,
                        num_values,
                    };
                    let encoding = ProtobufUtils21::rle(
                        ProtobufUtils21::flat(bits_per_value, None),
                        ProtobufUtils21::flat(self.run_length_width.bits_per_value(), None),
                    );
                    return Ok((compressed, encoding));
                }

                let values_candidates = Self::child_candidates(
                    &all_buffers,
                    &chunks,
                    0,
                    bits_per_value,
                    self.values_compression,
                    self.use_child_bitpacking,
                )?;
                let run_lengths_candidates = Self::child_candidates(
                    &all_buffers,
                    &chunks,
                    1,
                    self.run_length_width.bits_per_value(),
                    self.run_lengths_compression,
                    self.use_child_bitpacking,
                )?;
                let (values, run_lengths) =
                    Self::select_child_candidates(values_candidates, run_lengths_candidates);
                let chunks = chunks
                    .into_iter()
                    .enumerate()
                    .map(|(idx, chunk)| MiniBlockChunk {
                        buffer_sizes: vec![values.chunk_sizes[idx], run_lengths.chunk_sizes[idx]],
                        log_num_values: chunk.log_num_values,
                    })
                    .collect();

                let compressed = MiniBlockCompressed {
                    data: vec![values.data, run_lengths.data],
                    chunks,
                    num_values,
                };

                let encoding = ProtobufUtils21::rle(values.encoding, run_lengths.encoding);

                Ok((compressed, encoding))
            }
            _ => Err(Error::invalid_input_source(
                "RLE encoding only supports FixedWidth data blocks".into(),
            )),
        }
    }
}

impl BlockCompressor for RleEncoder {
    // Block format: [8-byte header: values buffer size][values buffer][run_lengths buffer]
    fn compress(&self, data: DataBlock) -> Result<LanceBuffer> {
        match data {
            DataBlock::FixedWidth(fixed_width) => {
                let num_values = fixed_width.num_values;
                let bits_per_value = fixed_width.bits_per_value;

                let all_buffers =
                    self.encode_block_data(&fixed_width.data, num_values, bits_per_value)?;

                let values_size = all_buffers[0].len() as u64;

                let mut combined = Vec::new();
                combined.extend_from_slice(&values_size.to_le_bytes());
                combined.extend_from_slice(&all_buffers[0]);
                combined.extend_from_slice(&all_buffers[1]);
                Ok(LanceBuffer::from(combined))
            }
            _ => Err(Error::invalid_input_source(
                "RLE encoding only supports FixedWidth data blocks".into(),
            )),
        }
    }
}

/// RLE decompressor for miniblock format
#[derive(Debug)]
pub struct RleDecompressor {
    bits_per_value: u64,
    run_length_width: RunLengthWidth,
    values: RleChildDecompressor,
    run_lengths: RleChildDecompressor,
}

#[derive(Debug)]
pub(crate) struct RleChildDecompressor {
    bits_per_value: u64,
    inner: RleChildDecompressorInner,
}

#[derive(Debug)]
enum RleChildDecompressorInner {
    Flat,
    Block {
        decompressor: Box<dyn BlockDecompressor>,
        requires_num_values: bool,
    },
}

impl RleChildDecompressor {
    pub(crate) fn flat(bits_per_value: u64) -> Self {
        Self {
            bits_per_value,
            inner: RleChildDecompressorInner::Flat,
        }
    }

    pub(crate) fn block(
        bits_per_value: u64,
        decompressor: Box<dyn BlockDecompressor>,
        requires_num_values: bool,
    ) -> Self {
        Self {
            bits_per_value,
            inner: RleChildDecompressorInner::Block {
                decompressor,
                requires_num_values,
            },
        }
    }

    pub(crate) fn bits_per_value(&self) -> u64 {
        self.bits_per_value
    }

    pub(crate) fn requires_num_values(&self) -> bool {
        match &self.inner {
            RleChildDecompressorInner::Flat => false,
            RleChildDecompressorInner::Block {
                requires_num_values,
                ..
            } => *requires_num_values,
        }
    }

    pub(crate) fn is_identity(&self) -> bool {
        matches!(self.inner, RleChildDecompressorInner::Flat)
    }

    fn decode(
        &self,
        data: LanceBuffer,
        num_values: Option<u64>,
        label: &str,
    ) -> Result<LanceBuffer> {
        match &self.inner {
            RleChildDecompressorInner::Flat => Ok(data),
            RleChildDecompressorInner::Block {
                decompressor,
                requires_num_values,
            } => {
                let num_values = if *requires_num_values {
                    num_values.ok_or_else(|| {
                        Error::invalid_input_source(
                            format!("RLE {label} child compression requires the run count").into(),
                        )
                    })?
                } else {
                    num_values.unwrap_or(0)
                };
                let decoded = decompressor.decompress(data, num_values)?;
                self.extract_fixed_width(decoded, num_values, label)
            }
        }
    }

    fn extract_fixed_width(
        &self,
        data: DataBlock,
        expected_num_values: u64,
        label: &str,
    ) -> Result<LanceBuffer> {
        match data {
            DataBlock::FixedWidth(block) => {
                if block.bits_per_value != self.bits_per_value {
                    return Err(Error::invalid_input_source(
                        format!(
                            "RLE {label} child decoded {}-bit values, expected {}",
                            block.bits_per_value, self.bits_per_value
                        )
                        .into(),
                    ));
                }
                if expected_num_values != 0 && block.num_values != expected_num_values {
                    return Err(Error::invalid_input_source(
                        format!(
                            "RLE {label} child decoded {} values, expected {}",
                            block.num_values, expected_num_values
                        )
                        .into(),
                    ));
                }
                Ok(block.data)
            }
            _ => Err(Error::invalid_input_source(
                format!("RLE {label} child decoded to a non fixed-width block").into(),
            )),
        }
    }
}

impl RleDecompressor {
    pub fn new(bits_per_value: u64) -> Self {
        Self {
            bits_per_value,
            run_length_width: RunLengthWidth::U8,
            values: RleChildDecompressor::flat(bits_per_value),
            run_lengths: RleChildDecompressor::flat(RunLengthWidth::U8.bits_per_value()),
        }
    }

    pub(crate) fn with_run_length_width(
        bits_per_value: u64,
        run_length_width: RunLengthWidth,
    ) -> Self {
        Self {
            bits_per_value,
            run_length_width,
            values: RleChildDecompressor::flat(bits_per_value),
            run_lengths: RleChildDecompressor::flat(run_length_width.bits_per_value()),
        }
    }

    pub(crate) fn with_child_decompressors(
        bits_per_value: u64,
        run_length_width: RunLengthWidth,
        values: RleChildDecompressor,
        run_lengths: RleChildDecompressor,
    ) -> Self {
        Self {
            bits_per_value,
            run_length_width,
            values,
            run_lengths,
        }
    }

    fn decode_data(
        &self,
        data: Vec<LanceBuffer>,
        num_values: u64,
        clamp_overflow: bool,
    ) -> Result<DataBlock> {
        if num_values == 0 {
            return Ok(DataBlock::FixedWidth(FixedWidthDataBlock {
                bits_per_value: self.bits_per_value,
                data: LanceBuffer::from(vec![]),
                num_values: 0,
                block_info: BlockInfo::default(),
            }));
        }

        if data.len() != 2 {
            return Err(Error::invalid_input_source(
                format!(
                    "RLE decompressor expects exactly 2 buffers, got {}",
                    data.len()
                )
                .into(),
            ));
        }

        let mut data_iter = data.into_iter();
        let values_buffer = data_iter.next().unwrap();
        let lengths_buffer = data_iter.next().unwrap();
        let (values_buffer, lengths_buffer) =
            self.decode_child_buffers(values_buffer, lengths_buffer)?;

        let decoded_data = match self.bits_per_value {
            8 => self.decode_generic::<u8>(
                &values_buffer,
                &lengths_buffer,
                num_values,
                clamp_overflow,
            )?,
            16 => self.decode_generic::<u16>(
                &values_buffer,
                &lengths_buffer,
                num_values,
                clamp_overflow,
            )?,
            32 => self.decode_generic::<u32>(
                &values_buffer,
                &lengths_buffer,
                num_values,
                clamp_overflow,
            )?,
            64 => self.decode_generic::<u64>(
                &values_buffer,
                &lengths_buffer,
                num_values,
                clamp_overflow,
            )?,
            _ => {
                return Err(Error::invalid_input_source(
                    format!(
                        "RLE decoding bits_per_value must be 8, 16, 32, or 64, got {}",
                        self.bits_per_value
                    )
                    .into(),
                ));
            }
        };

        Ok(DataBlock::FixedWidth(FixedWidthDataBlock {
            bits_per_value: self.bits_per_value,
            data: decoded_data,
            num_values,
            block_info: BlockInfo::default(),
        }))
    }

    fn decode_child_buffers(
        &self,
        values_buffer: LanceBuffer,
        lengths_buffer: LanceBuffer,
    ) -> Result<(LanceBuffer, LanceBuffer)> {
        let values_requires_num_runs = self.values.requires_num_values();
        let lengths_requires_num_runs = self.run_lengths.requires_num_values();
        if values_requires_num_runs && lengths_requires_num_runs {
            return Err(Error::invalid_input_source(
                "RLE values and run lengths child compression both require the run count".into(),
            ));
        }

        if values_requires_num_runs {
            let lengths_buffer = self
                .run_lengths
                .decode(lengths_buffer, None, "run lengths")?;
            let num_runs = Self::num_child_values(
                &lengths_buffer,
                self.run_lengths.bits_per_value(),
                "run lengths",
            )?;
            let values_buffer = self
                .values
                .decode(values_buffer, Some(num_runs), "values")?;
            Ok((values_buffer, lengths_buffer))
        } else if lengths_requires_num_runs {
            let values_buffer = self.values.decode(values_buffer, None, "values")?;
            let num_runs =
                Self::num_child_values(&values_buffer, self.values.bits_per_value(), "values")?;
            let lengths_buffer =
                self.run_lengths
                    .decode(lengths_buffer, Some(num_runs), "run lengths")?;
            Ok((values_buffer, lengths_buffer))
        } else {
            let values_buffer = self.values.decode(values_buffer, None, "values")?;
            let lengths_buffer = self
                .run_lengths
                .decode(lengths_buffer, None, "run lengths")?;
            Ok((values_buffer, lengths_buffer))
        }
    }

    fn num_child_values(buffer: &LanceBuffer, bits_per_value: u64, label: &str) -> Result<u64> {
        let bytes_per_value = usize::try_from(bits_per_value / 8).map_err(|_| {
            Error::invalid_input_source(
                format!("RLE {label} child bit width is too large: {bits_per_value}").into(),
            )
        })?;
        if bytes_per_value == 0 || !buffer.len().is_multiple_of(bytes_per_value) {
            return Err(Error::invalid_input_source(
                format!(
                    "RLE {label} child decoded to {} bytes, not divisible by {}",
                    buffer.len(),
                    bytes_per_value
                )
                .into(),
            ));
        }
        Ok((buffer.len() / bytes_per_value) as u64)
    }

    fn decode_generic<T>(
        &self,
        values_buffer: &LanceBuffer,
        lengths_buffer: &LanceBuffer,
        num_values: u64,
        clamp_overflow: bool,
    ) -> Result<LanceBuffer>
    where
        T: bytemuck::Pod + Copy + std::fmt::Debug + ArrowNativeType,
    {
        let type_size = std::mem::size_of::<T>();
        let length_size = self.run_length_width.bytes_per_value();

        if values_buffer.is_empty() || lengths_buffer.is_empty() {
            if num_values == 0 {
                return Ok(LanceBuffer::empty());
            } else {
                return Err(Error::invalid_input_source(
                    format!("Empty buffers but expected {} values", num_values).into(),
                ));
            }
        }

        if !values_buffer.len().is_multiple_of(type_size)
            || !lengths_buffer.len().is_multiple_of(length_size)
        {
            return Err(Error::invalid_input_source(format!(
                "Invalid buffer sizes for RLE {} decoding: values {} bytes (not divisible by {}), lengths {} bytes (not divisible by {})",
                std::any::type_name::<T>(),
                values_buffer.len(),
                type_size,
                lengths_buffer.len(),
                length_size
            )
            .into()));
        }

        let num_runs = values_buffer.len() / type_size;
        let num_length_entries = lengths_buffer.len() / length_size;
        if num_runs != num_length_entries {
            return Err(Error::invalid_input_source(
                format!(
                    "Inconsistent RLE buffers: {} runs but {} length entries",
                    num_runs, num_length_entries
                )
                .into(),
            ));
        }

        let values_ref = values_buffer.borrow_to_typed_slice::<T>();
        let values: &[T] = values_ref.as_ref();
        let lengths = lengths_buffer.as_ref();

        let expected_value_count = usize::try_from(num_values).map_err(|_| {
            Error::invalid_input_source(
                format!("RLE num_values does not fit in usize: {num_values}").into(),
            )
        })?;
        // Legacy miniblock encoders rolled back to a power-of-2 checkpoint after a run
        // had already crossed it, so a chunk's run lengths can sum past its declared
        // value count (the excess values are re-encoded at the start of the next chunk).
        // The pre-run-length-width decoder truncated the excess, so miniblock decoding
        // clamps rather than rejects to keep those files readable. Block payloads never
        // legitimately overflow, so they decode strictly.
        let mut decoded: Vec<T> = Vec::new();
        decoded
            .try_reserve_exact(expected_value_count)
            .map_err(|_| {
                Error::invalid_input_source(
                    format!("RLE decoding cannot allocate {expected_value_count} values").into(),
                )
            })?;
        for (value, length_bytes) in values.iter().zip(lengths.chunks_exact(length_size)) {
            let length = self.run_length_width.read_length(length_bytes);
            if length == 0 {
                return Err(Error::invalid_input_source(
                    "RLE decoding encountered a zero run length".into(),
                ));
            }
            let length = usize::try_from(length).map_err(|_| {
                Error::invalid_input_source(
                    format!("RLE run length does not fit in usize: {length}").into(),
                )
            })?;
            let remaining = expected_value_count - decoded.len();
            if length > remaining {
                if !clamp_overflow {
                    return Err(Error::invalid_input_source(
                        format!(
                            "RLE decoding overflowed expected value count: produced at least {}, expected {}",
                            decoded.len() + length,
                            expected_value_count
                        )
                        .into(),
                    ));
                }
                decoded.resize(expected_value_count, *value);
                break;
            }
            decoded.resize(decoded.len() + length, *value);
        }

        if decoded.len() != expected_value_count {
            return Err(Error::invalid_input_source(
                format!(
                    "RLE decoding produced {} values, expected {}",
                    decoded.len(),
                    expected_value_count
                )
                .into(),
            ));
        }

        trace!(
            "RLE decoded {} {} values",
            num_values,
            std::any::type_name::<T>()
        );
        Ok(LanceBuffer::reinterpret_vec(decoded))
    }
}

impl MiniBlockDecompressor for RleDecompressor {
    fn decompress(&self, data: Vec<LanceBuffer>, num_values: u64) -> Result<DataBlock> {
        self.decode_data(data, num_values, true)
    }

    fn decoded_size_bytes(&self, num_values: u64) -> Option<u64> {
        num_values
            .checked_mul(self.bits_per_value)
            .map(|bits| bits.div_ceil(8))
    }
}

impl BlockDecompressor for RleDecompressor {
    fn decompress(&self, data: LanceBuffer, num_values: u64) -> Result<DataBlock> {
        let (values_buffer, lengths_buffer) = parse_rle_block_frame(&data)?;
        self.decode_data(vec![values_buffer, lengths_buffer], num_values, false)
    }
}

/// Split an RLE block-format buffer into its `(values, lengths)` sub-buffers.
/// Frame: `[values_size: u64-le][values bytes][run-length bytes]`.
fn parse_rle_block_frame(data: &LanceBuffer) -> Result<(LanceBuffer, LanceBuffer)> {
    // fetch the values_size
    if data.len() < 8 {
        return Err(Error::invalid_input_source(
            format!("Insufficient data size: {}", data.len()).into(),
        ));
    }

    let values_size_bytes: [u8; 8] = data[..8].try_into().expect("slice length already checked");
    let values_size: usize = u64::from_le_bytes(values_size_bytes)
        .try_into()
        .map_err(|_| {
            Error::invalid_input_source(
                format!(
                    "Invalid values buffer size: {}",
                    u64::from_le_bytes(values_size_bytes)
                )
                .into(),
            )
        })?;

    // parse values
    let values_start: usize = 8;
    let lengths_start = values_start
        .checked_add(values_size)
        .ok_or_else(|| Error::invalid_input_source("Invalid RLE values buffer size".into()))?;

    if data.len() < lengths_start {
        return Err(Error::invalid_input_source(
            format!("Insufficient data size: {}", data.len()).into(),
        ));
    }

    let values_buffer = data.slice_with_length(values_start, values_size);
    let lengths_buffer = data.slice_with_length(lengths_start, data.len() - lengths_start);
    Ok((values_buffer, lengths_buffer))
}

#[derive(Clone, Debug)]
enum RleRunLengths {
    U8(ScalarBuffer<u8>),
    U16(ScalarBuffer<u16>),
    U32(ScalarBuffer<u32>),
}

impl RleRunLengths {
    fn try_new(buffer: LanceBuffer, width: RunLengthWidth) -> Result<Self> {
        let width_bytes = width.bytes_per_value();
        if !buffer.len().is_multiple_of(width_bytes) {
            return Err(Error::invalid_input_source(
                format!(
                    "Invalid RLE run lengths buffer: {} bytes (not divisible by {})",
                    buffer.len(),
                    width_bytes
                )
                .into(),
            ));
        }
        Ok(match width {
            RunLengthWidth::U8 => Self::U8(buffer.borrow_to_typed_slice()),
            RunLengthWidth::U16 => Self::U16(buffer.borrow_to_typed_slice()),
            RunLengthWidth::U32 => Self::U32(buffer.borrow_to_typed_slice()),
        })
    }

    fn len(&self) -> usize {
        match self {
            Self::U8(lengths) => lengths.len(),
            Self::U16(lengths) => lengths.len(),
            Self::U32(lengths) => lengths.len(),
        }
    }

    fn get(&self, index: usize) -> usize {
        match self {
            Self::U8(lengths) => lengths[index] as usize,
            Self::U16(lengths) => lengths[index] as usize,
            Self::U32(lengths) => lengths[index] as usize,
        }
    }

    fn owned_size(&self) -> usize {
        match self {
            Self::U8(lengths) => std::mem::size_of_val(lengths.as_ref()),
            Self::U16(lengths) => std::mem::size_of_val(lengths.as_ref()),
            Self::U32(lengths) => std::mem::size_of_val(lengths.as_ref()),
        }
    }

    fn into_owned(self) -> Self {
        match self {
            Self::U8(lengths) => Self::U8(ScalarBuffer::from(lengths.as_ref().to_vec())),
            Self::U16(lengths) => Self::U16(ScalarBuffer::from(lengths.as_ref().to_vec())),
            Self::U32(lengths) => Self::U32(ScalarBuffer::from(lengths.as_ref().to_vec())),
        }
    }

    fn deep_size(&self) -> usize {
        match self {
            Self::U8(lengths) => lengths.inner().capacity(),
            Self::U16(lengths) => lengths.inner().capacity(),
            Self::U32(lengths) => lengths.inner().capacity(),
        }
    }
}

/// Validated physical RLE runs for `u16` values.
///
/// The values and original-width lengths remain unexpanded. The constructor
/// verifies that every length is non-zero and that the runs cover exactly
/// `num_values` logical values.
#[derive(Clone, Debug)]
pub(crate) struct RleRuns {
    values: ScalarBuffer<u16>,
    lengths: RleRunLengths,
    num_values: usize,
    coalesced_runs: usize,
}

impl RleRuns {
    fn try_new(
        values_buffer: LanceBuffer,
        lengths_buffer: LanceBuffer,
        run_length_width: RunLengthWidth,
        num_values: u64,
    ) -> Result<Self> {
        let num_values = usize::try_from(num_values).map_err(|_| {
            Error::invalid_input_source(
                format!("RLE num_values does not fit in usize: {num_values}").into(),
            )
        })?;
        let type_size = std::mem::size_of::<u16>();
        if !values_buffer.len().is_multiple_of(type_size) {
            return Err(Error::invalid_input_source(
                format!(
                    "Invalid RLE u16 values buffer: {} bytes (not divisible by {})",
                    values_buffer.len(),
                    type_size
                )
                .into(),
            ));
        }

        let values = values_buffer.borrow_to_typed_slice::<u16>();
        let lengths = RleRunLengths::try_new(lengths_buffer, run_length_width)?;
        if values.len() != lengths.len() {
            return Err(Error::invalid_input_source(
                format!(
                    "Inconsistent RLE buffers: {} runs but {} length entries",
                    values.len(),
                    lengths.len()
                )
                .into(),
            ));
        }
        if values.is_empty() && num_values != 0 {
            return Err(Error::invalid_input_source(
                format!("Empty RLE buffers but expected {num_values} values").into(),
            ));
        }

        let mut decoded_values = 0usize;
        let mut coalesced_runs = 0usize;
        let mut previous_value = None;
        for run in 0..values.len() {
            let length = lengths.get(run);
            if length == 0 {
                return Err(Error::invalid_input_source(
                    "RLE decoding encountered a zero run length".into(),
                ));
            }
            decoded_values = decoded_values.checked_add(length).ok_or_else(|| {
                Error::invalid_input_source("RLE run length sum overflowed usize".into())
            })?;
            if decoded_values > num_values {
                return Err(Error::invalid_input_source(
                    format!(
                        "RLE decoding overflowed expected value count: produced at least {}, expected {}",
                        decoded_values, num_values
                    )
                    .into(),
                ));
            }
            if previous_value != Some(values[run]) {
                coalesced_runs += 1;
                previous_value = Some(values[run]);
            }
        }
        if decoded_values != num_values {
            return Err(Error::invalid_input_source(
                format!(
                    "RLE decoding produced {} values, expected {}",
                    decoded_values, num_values
                )
                .into(),
            ));
        }

        Ok(Self {
            values,
            lengths,
            num_values,
            coalesced_runs,
        })
    }

    pub(crate) fn num_values(&self) -> usize {
        self.num_values
    }

    pub(crate) fn num_runs(&self) -> usize {
        self.values.len()
    }

    pub(crate) fn coalesced_runs(&self) -> usize {
        self.coalesced_runs
    }

    pub(crate) fn owned_size(&self) -> usize {
        std::mem::size_of_val(self.values.as_ref()) + self.lengths.owned_size()
    }

    pub(crate) fn into_owned(self) -> Self {
        Self {
            values: ScalarBuffer::from(self.values.as_ref().to_vec()),
            lengths: self.lengths.into_owned(),
            num_values: self.num_values,
            coalesced_runs: self.coalesced_runs,
        }
    }

    pub(crate) fn deep_size(&self) -> usize {
        self.values.inner().capacity() + self.lengths.deep_size()
    }

    pub(crate) fn value(&self, run: usize) -> u16 {
        self.values[run]
    }

    pub(crate) fn length(&self, run: usize) -> usize {
        self.lengths.get(run)
    }

    pub(crate) fn iter(&self) -> impl ExactSizeIterator<Item = (u16, usize)> + '_ {
        (0..self.num_runs()).map(|run| (self.value(run), self.length(run)))
    }
}

impl RleDecompressor {
    /// Decode and validate a block frame while preserving its physical runs.
    pub(crate) fn decode_u16_runs(&self, data: LanceBuffer, num_values: u64) -> Result<RleRuns> {
        if self.bits_per_value != 16 {
            return Err(Error::invalid_input_source(
                format!(
                    "RLE level values must be 16 bits, got {}",
                    self.bits_per_value
                )
                .into(),
            ));
        }
        let (values_buffer, lengths_buffer) = parse_rle_block_frame(&data)?;
        let (values_buffer, lengths_buffer) =
            self.decode_child_buffers(values_buffer, lengths_buffer)?;
        RleRuns::try_new(
            values_buffer,
            lengths_buffer,
            self.run_length_width,
            num_values,
        )
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::compression::{
        DecompressionStrategy, DefaultDecompressionStrategy, create_rle_decompressor,
    };
    use crate::data::DataBlock;
    use crate::encodings::logical::primitive::miniblock::MAX_MINIBLOCK_VALUES;
    use crate::encodings::physical::block::{CompressionConfig, CompressionScheme};
    use crate::{
        buffer::LanceBuffer,
        compression::{BlockCompressor, BlockDecompressor},
    };
    use arrow_array::Int32Array;
    use rstest::rstest;

    fn compress_miniblock(
        compressor: &dyn MiniBlockCompressor,
        data: DataBlock,
    ) -> Result<(MiniBlockCompressed, CompressiveEncoding)> {
        compressor.compress(MiniBlockCompressionContext::new(0, true, true), data)
    }

    fn expand_u16_runs(runs: &RleRuns) -> Vec<u16> {
        let mut expanded = Vec::with_capacity(runs.num_values());
        for (value, length) in runs.iter() {
            expanded.extend(std::iter::repeat_n(value, length));
        }
        expanded
    }

    #[test]
    fn decode_u16_runs_matches_eager() {
        // Near-constant u16 levels (the all-null shape): a few long runs.
        let mut levels: Vec<u16> = vec![1u16; 1000];
        levels.extend(std::iter::repeat_n(0u16, 500));
        levels.extend(std::iter::repeat_n(2u16, 300));
        let num_values = levels.len() as u64;

        let block = DataBlock::FixedWidth(FixedWidthDataBlock {
            data: LanceBuffer::reinterpret_slice(Arc::from(levels.clone())),
            bits_per_value: 16,
            num_values,
            block_info: BlockInfo::new(),
        });
        let frame = BlockCompressor::compress(&RleEncoder::new(), block).unwrap();

        let eager =
            BlockDecompressor::decompress(&RleDecompressor::new(16), frame.clone(), num_values)
                .unwrap();
        let DataBlock::FixedWidth(eager) = eager else {
            panic!("expected fixed-width block");
        };
        assert_eq!(
            eager.data.borrow_to_typed_slice::<u16>().as_ref(),
            levels.as_slice()
        );

        // Lazy run form preserves boundaries and expands identically.
        let runs = RleDecompressor::new(16)
            .decode_u16_runs(frame, num_values)
            .unwrap();
        assert_eq!(runs.num_values(), num_values as usize);
        // The encoder splits each value into <=255-length runs (4 + 2 + 2 = 8
        // on-disk runs here); the scan identifies 3 coalesced logical runs.
        assert_eq!(runs.num_runs(), 8);
        assert_eq!(runs.coalesced_runs(), 3);
        assert_eq!(expand_u16_runs(&runs), levels);
    }

    #[test]
    fn decode_u16_runs_empty() {
        let mut empty_frame = Vec::new();
        empty_frame.extend_from_slice(&0u64.to_le_bytes());
        let runs = RleDecompressor::new(16)
            .decode_u16_runs(LanceBuffer::from(empty_frame), 0)
            .unwrap();
        assert_eq!(runs.num_values(), 0);
        assert_eq!(runs.num_runs(), 0);
        assert_eq!(runs.coalesced_runs(), 0);

        let error = RleDecompressor::new(16)
            .decode_u16_runs(LanceBuffer::empty(), 0)
            .unwrap_err();
        assert!(error.to_string().contains("Insufficient data size: 0"));
    }

    #[rstest]
    #[case::zero(0, 1, "zero run length")]
    #[case::underflow(1, 2, "produced 1 values, expected 2")]
    #[case::overflow(2, 1, "overflowed expected value count")]
    #[case::nonempty_for_empty(1, 0, "overflowed expected value count")]
    fn decode_u16_runs_rejects_invalid_coverage(
        #[case] run_length: u8,
        #[case] num_values: u64,
        #[case] expected_message: &str,
    ) {
        let mut frame = Vec::new();
        frame.extend_from_slice(&2u64.to_le_bytes());
        frame.extend_from_slice(&7u16.to_le_bytes());
        frame.push(run_length);

        let error = RleDecompressor::new(16)
            .decode_u16_runs(LanceBuffer::from(frame), num_values)
            .unwrap_err();
        assert!(matches!(error, lance_core::Error::InvalidInput { .. }));
        assert!(error.to_string().contains(expected_message));
    }

    #[test]
    #[cfg(any(feature = "lz4", feature = "zstd"))]
    fn decode_u16_runs_supports_compressed_values_child() {
        let levels: Vec<u16> = (0..1024)
            .flat_map(|run| std::iter::repeat_n((run % 8) as u16, 4))
            .collect();
        let num_values = levels.len() as u64;
        let block = DataBlock::FixedWidth(FixedWidthDataBlock {
            data: LanceBuffer::reinterpret_slice(Arc::from(levels.clone())),
            bits_per_value: 16,
            num_values,
            block_info: BlockInfo::new(),
        });
        let frame = BlockCompressor::compress(&RleEncoder::new(), block).unwrap();
        let (values, lengths) = parse_rle_block_frame(&frame).unwrap();

        let compression = test_general_compression();
        let compressor = GeneralBufferCompressor::get_compressor(compression).unwrap();
        let mut compressed_values = Vec::new();
        compressor
            .compress(values.as_ref(), &mut compressed_values)
            .unwrap();
        let mut compressed_frame = Vec::new();
        compressed_frame.extend_from_slice(&(compressed_values.len() as u64).to_le_bytes());
        compressed_frame.extend_from_slice(&compressed_values);
        compressed_frame.extend_from_slice(lengths.as_ref());

        let encoding = ProtobufUtils21::rle(
            ProtobufUtils21::wrapped(compression, ProtobufUtils21::flat(16, None)).unwrap(),
            ProtobufUtils21::flat(8, None),
        );
        let decompressor = create_rle_decompressor(
            expect_rle(&encoding),
            &DefaultDecompressionStrategy::default(),
        )
        .unwrap();
        let runs = decompressor
            .decode_u16_runs(LanceBuffer::from(compressed_frame), num_values)
            .unwrap();
        assert_eq!(expand_u16_runs(&runs), levels);
    }

    #[test]
    fn decode_u16_runs_counts_coalesced_runs() {
        // A logically constant page is emitted as ceil(N / 255) equal-valued
        // runs (the encoder caps run lengths at 255); the validated view records
        // that they can collapse to a single logical run.
        let num_values = 5000u64;
        let constant: Vec<u16> = vec![7u16; num_values as usize];
        let block = DataBlock::FixedWidth(FixedWidthDataBlock {
            data: LanceBuffer::reinterpret_slice(Arc::from(constant)),
            bits_per_value: 16,
            num_values,
            block_info: BlockInfo::new(),
        });
        let frame = BlockCompressor::compress(&RleEncoder::new(), block).unwrap();
        let runs = RleDecompressor::new(16)
            .decode_u16_runs(frame, num_values)
            .unwrap();
        assert_eq!(
            runs.num_runs(),
            num_values.div_ceil(u8::MAX as u64) as usize
        );
        assert_eq!(runs.coalesced_runs(), 1);
        assert_eq!(expand_u16_runs(&runs), vec![7u16; num_values as usize]);

        // Distinct adjacent values must not be merged: alternating single-value
        // runs stay separate and still expand to the original.
        let alternating: Vec<u16> = (0..200u16).map(|i| i % 2).collect();
        let n = alternating.len() as u64;
        let block = DataBlock::FixedWidth(FixedWidthDataBlock {
            data: LanceBuffer::reinterpret_slice(Arc::from(alternating.clone())),
            bits_per_value: 16,
            num_values: n,
            block_info: BlockInfo::new(),
        });
        let frame = BlockCompressor::compress(&RleEncoder::new(), block).unwrap();
        let runs = RleDecompressor::new(16).decode_u16_runs(frame, n).unwrap();
        assert_eq!(
            runs.coalesced_runs() as u64,
            n,
            "no two adjacent values are equal"
        );
        assert_eq!(expand_u16_runs(&runs), alternating);
    }

    // ========== Core Functionality Tests ==========

    #[test]
    fn test_basic_miniblock_rle_encoding() {
        let encoder = RleEncoder::new();

        // Test basic RLE pattern: [1, 1, 1, 2, 2, 3, 3, 3, 3]
        let array = Int32Array::from(vec![1, 1, 1, 2, 2, 3, 3, 3, 3]);
        let data_block = DataBlock::from_array(array);

        let (compressed, _) = compress_miniblock(&encoder, data_block).unwrap();

        assert_eq!(compressed.num_values, 9);
        assert_eq!(compressed.chunks.len(), 1);

        // Verify compression happened (3 runs instead of 9 values)
        let values_buffer = &compressed.data[0];
        let lengths_buffer = &compressed.data[1];
        assert_eq!(values_buffer.len(), 12); // 3 i32 values
        assert_eq!(lengths_buffer.len(), 3); // 3 u8 lengths
    }

    #[test]
    fn test_long_run_splitting() {
        let encoder = RleEncoder::new();

        // Create a run longer than 255 to test splitting
        let mut data = vec![42i32; 1000]; // Will be split into 255+255+255+235
        data.extend(&[100i32; 300]); // Will be split into 255+45

        let array = Int32Array::from(data);
        let (compressed, _) = compress_miniblock(&encoder, DataBlock::from_array(array)).unwrap();

        // Should have 6 runs total (4 for first value, 2 for second)
        let lengths_buffer = &compressed.data[1];
        assert_eq!(lengths_buffer.len(), 6);
    }

    #[test]
    fn test_rle_v2_u16_miniblock_encoding() {
        let encoder = RleEncoder::with_run_length_width(RunLengthWidth::U16);

        let data = vec![42i32; 1000];
        let array = Int32Array::from(data);
        let (compressed, encoding) =
            compress_miniblock(&encoder, DataBlock::from_array(array)).unwrap();

        assert_eq!(compressed.data[0].len(), 4);
        assert_eq!(compressed.data[1].len(), 2);
        assert_eq!(compressed.data[1].as_ref(), &1000u16.to_le_bytes());

        let rle = match encoding.compression.as_ref().unwrap() {
            crate::format::pb21::compressive_encoding::Compression::Rle(rle) => rle,
            other => panic!("expected RLE encoding, got {other:?}"),
        };
        let run_lengths = rle.run_lengths.as_ref().unwrap();
        let flat = match run_lengths.compression.as_ref().unwrap() {
            crate::format::pb21::compressive_encoding::Compression::Flat(flat) => flat,
            other => panic!("expected flat run lengths, got {other:?}"),
        };
        assert_eq!(flat.bits_per_value, 16);

        let decompressor = RleDecompressor::with_run_length_width(32, RunLengthWidth::U16);
        let decompressed = MiniBlockDecompressor::decompress(
            &decompressor,
            compressed.data,
            compressed.num_values,
        )
        .unwrap();
        match decompressed {
            DataBlock::FixedWidth(block) => {
                let values = block.data.borrow_to_typed_slice::<i32>();
                assert_eq!(values.as_ref(), vec![42i32; 1000]);
            }
            _ => panic!("Expected FixedWidth block"),
        }
    }

    #[test]
    #[cfg(any(feature = "lz4", feature = "zstd"))]
    fn test_rle_miniblock_compressed_values_child() {
        let compression = test_general_compression();
        let encoder =
            RleEncoder::with_child_encoding(RunLengthWidth::U8, Some(compression), None, false);
        let array = Int32Array::from(repeating_runs(1024, 4));
        let (compressed, encoding) =
            compress_miniblock(&encoder, DataBlock::from_array(array)).unwrap();

        let rle = expect_rle(&encoding);
        assert!(matches!(
            rle.values.as_ref().unwrap().compression.as_ref().unwrap(),
            crate::format::pb21::compressive_encoding::Compression::General(_)
        ));
        assert!(matches!(
            rle.run_lengths
                .as_ref()
                .unwrap()
                .compression
                .as_ref()
                .unwrap(),
            crate::format::pb21::compressive_encoding::Compression::Flat(_)
        ));

        let decompressor = DefaultDecompressionStrategy::default()
            .create_miniblock_decompressor(&encoding, &DefaultDecompressionStrategy::default())
            .unwrap();
        let decoded =
            MiniBlockDecompressor::decompress(decompressor.as_ref(), compressed.data, 1024 * 4)
                .unwrap();
        assert_decoded_i32_eq(decoded, &repeating_runs(1024, 4));
    }

    #[test]
    #[cfg(any(feature = "lz4", feature = "zstd"))]
    fn test_rle_miniblock_compressed_run_lengths_child() {
        let compression = test_general_compression();
        let encoder =
            RleEncoder::with_child_encoding(RunLengthWidth::U8, None, Some(compression), false);
        let expected = repeating_runs(1024, 4);
        let (compressed, encoding) = compress_miniblock(
            &encoder,
            DataBlock::from_array(Int32Array::from(expected.clone())),
        )
        .unwrap();

        let rle = expect_rle(&encoding);
        assert!(matches!(
            rle.values.as_ref().unwrap().compression.as_ref().unwrap(),
            crate::format::pb21::compressive_encoding::Compression::Flat(_)
        ));
        assert!(matches!(
            rle.run_lengths
                .as_ref()
                .unwrap()
                .compression
                .as_ref()
                .unwrap(),
            crate::format::pb21::compressive_encoding::Compression::General(_)
        ));

        let decompressor = DefaultDecompressionStrategy::default()
            .create_miniblock_decompressor(&encoding, &DefaultDecompressionStrategy::default())
            .unwrap();
        let decoded =
            MiniBlockDecompressor::decompress(decompressor.as_ref(), compressed.data, 1024 * 4)
                .unwrap();
        assert_decoded_i32_eq(decoded, &expected);
    }

    #[test]
    #[cfg(feature = "bitpacking")]
    fn test_rle_miniblock_bitpacked_run_lengths_child() {
        use crate::encodings::physical::bitpacking::OutOfLineBitpacking;

        let expected = repeating_runs(1024, 4);
        let (compressed, _) = compress_miniblock(
            &RleEncoder::new(),
            DataBlock::from_array(Int32Array::from(expected.clone())),
        )
        .unwrap();
        let run_lengths = compressed.data[1].clone();
        let num_runs = run_lengths.len() as u64;
        let run_lengths_block = DataBlock::FixedWidth(FixedWidthDataBlock {
            bits_per_value: 8,
            data: run_lengths,
            num_values: num_runs,
            block_info: BlockInfo::default(),
        });
        let bitpacked_run_lengths =
            BlockCompressor::compress(&OutOfLineBitpacking::new(3, 8), run_lengths_block).unwrap();
        let encoding = ProtobufUtils21::rle(
            ProtobufUtils21::flat(32, None),
            ProtobufUtils21::out_of_line_bitpacking(8, ProtobufUtils21::flat(3, None)),
        );

        let decompressor = DefaultDecompressionStrategy::default()
            .create_miniblock_decompressor(&encoding, &DefaultDecompressionStrategy::default())
            .unwrap();
        let decoded = MiniBlockDecompressor::decompress(
            decompressor.as_ref(),
            vec![compressed.data[0].clone(), bitpacked_run_lengths],
            expected.len() as u64,
        )
        .unwrap();
        assert_decoded_i32_eq(decoded, &expected);
    }

    #[test]
    #[cfg(feature = "bitpacking")]
    fn test_rle_rejects_two_count_dependent_child_encodings() {
        let encoding = ProtobufUtils21::rle(
            ProtobufUtils21::out_of_line_bitpacking(32, ProtobufUtils21::flat(3, None)),
            ProtobufUtils21::out_of_line_bitpacking(8, ProtobufUtils21::flat(3, None)),
        );

        let err = DefaultDecompressionStrategy::default()
            .create_miniblock_decompressor(&encoding, &DefaultDecompressionStrategy::default())
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("cannot both require the run count")
        );
    }

    #[cfg(any(feature = "lz4", feature = "zstd"))]
    fn test_general_compression() -> CompressionConfig {
        if cfg!(feature = "zstd") {
            CompressionConfig::new(CompressionScheme::Zstd, Some(3))
        } else {
            CompressionConfig::new(CompressionScheme::Lz4, None)
        }
    }

    fn repeating_runs(num_runs: usize, run_length: usize) -> Vec<i32> {
        let mut values = Vec::with_capacity(num_runs * run_length);
        for run in 0..num_runs {
            values.extend(std::iter::repeat_n((run % 8) as i32, run_length));
        }
        values
    }

    fn expect_rle(encoding: &CompressiveEncoding) -> &crate::format::pb21::Rle {
        match encoding.compression.as_ref().unwrap() {
            crate::format::pb21::compressive_encoding::Compression::Rle(rle) => rle,
            other => panic!("expected RLE encoding, got {other:?}"),
        }
    }

    fn assert_decoded_i32_eq(decoded: DataBlock, expected: &[i32]) {
        match decoded {
            DataBlock::FixedWidth(block) => {
                let values = block.data.borrow_to_typed_slice::<i32>();
                assert_eq!(values.as_ref(), expected);
            }
            _ => panic!("Expected FixedWidth block"),
        }
    }

    #[test]
    #[cfg(any(feature = "lz4", feature = "zstd"))]
    fn test_rle_miniblock_compressed_children_multiple_chunks() {
        let compression = test_general_compression();
        let encoder = RleEncoder::with_child_encoding(
            RunLengthWidth::U8,
            Some(compression),
            Some(compression),
            false,
        );
        let expected = repeating_runs(8192, 4);
        let (compressed, encoding) = compress_miniblock(
            &encoder,
            DataBlock::from_array(Int32Array::from(expected.clone())),
        )
        .unwrap();

        assert!(compressed.chunks.len() > 1);
        let rle = expect_rle(&encoding);
        assert!(matches!(
            rle.values.as_ref().unwrap().compression.as_ref().unwrap(),
            crate::format::pb21::compressive_encoding::Compression::General(_)
        ));
        assert!(matches!(
            rle.run_lengths
                .as_ref()
                .unwrap()
                .compression
                .as_ref()
                .unwrap(),
            crate::format::pb21::compressive_encoding::Compression::General(_)
        ));

        let decoded = decompress_i32_chunks(&compressed, &encoding);
        assert_eq!(decoded, expected);
    }

    #[test]
    #[cfg(feature = "bitpacking")]
    fn test_rle_miniblock_bitpacks_values_child_when_smaller() {
        let encoder = RleEncoder::with_child_encoding(RunLengthWidth::U8, None, None, true);
        let expected = monotonic_runs(2048, 4);
        let (compressed, encoding) = compress_miniblock(
            &encoder,
            DataBlock::from_array(Int32Array::from(expected.clone())),
        )
        .unwrap();

        let rle = expect_rle(&encoding);
        assert!(matches!(
            rle.values.as_ref().unwrap().compression.as_ref().unwrap(),
            crate::format::pb21::compressive_encoding::Compression::OutOfLineBitpacking(_)
        ));
        assert!(matches!(
            rle.run_lengths
                .as_ref()
                .unwrap()
                .compression
                .as_ref()
                .unwrap(),
            crate::format::pb21::compressive_encoding::Compression::Flat(_)
        ));

        let decoded = decompress_i32_chunks(&compressed, &encoding);
        assert_eq!(decoded, expected);
    }

    #[test]
    #[cfg(feature = "bitpacking")]
    fn test_rle_miniblock_bitpacks_run_lengths_when_values_do_not_shrink() {
        let encoder = RleEncoder::with_child_encoding(RunLengthWidth::U8, None, None, true);
        let expected = high_entropy_runs(2048, 4);
        let (compressed, encoding) = compress_miniblock(
            &encoder,
            DataBlock::from_array(Int32Array::from(expected.clone())),
        )
        .unwrap();

        let rle = expect_rle(&encoding);
        assert!(matches!(
            rle.values.as_ref().unwrap().compression.as_ref().unwrap(),
            crate::format::pb21::compressive_encoding::Compression::Flat(_)
        ));
        assert!(matches!(
            rle.run_lengths
                .as_ref()
                .unwrap()
                .compression
                .as_ref()
                .unwrap(),
            crate::format::pb21::compressive_encoding::Compression::OutOfLineBitpacking(_)
        ));

        let decoded = decompress_i32_chunks(&compressed, &encoding);
        assert_eq!(decoded, expected);
    }

    fn decompress_i32_chunks(
        compressed: &MiniBlockCompressed,
        encoding: &CompressiveEncoding,
    ) -> Vec<i32> {
        let strategy = DefaultDecompressionStrategy::default();
        let decompressor = strategy
            .create_miniblock_decompressor(encoding, &strategy)
            .unwrap();
        let mut offsets = vec![0usize; compressed.data.len()];
        let mut values_processed = 0u64;
        let mut decoded_values = Vec::new();

        for chunk in &compressed.chunks {
            let chunk_values = chunk.num_values(values_processed, compressed.num_values);
            let mut chunk_buffers = Vec::with_capacity(chunk.buffer_sizes.len());
            for (idx, size) in chunk.buffer_sizes.iter().enumerate() {
                let size = *size as usize;
                chunk_buffers.push(compressed.data[idx].slice_with_length(offsets[idx], size));
                offsets[idx] += size;
            }

            let decoded = decompressor
                .decompress(chunk_buffers, chunk_values)
                .unwrap();
            match decoded {
                DataBlock::FixedWidth(block) => {
                    let values = block.data.borrow_to_typed_slice::<i32>();
                    decoded_values.extend_from_slice(values.as_ref());
                }
                _ => panic!("Expected FixedWidth block"),
            }
            values_processed += chunk_values;
        }

        assert_eq!(values_processed, compressed.num_values);
        decoded_values
    }

    #[cfg(feature = "bitpacking")]
    fn monotonic_runs(num_runs: usize, run_length: usize) -> Vec<i32> {
        let mut values = Vec::with_capacity(num_runs * run_length);
        for run in 0..num_runs {
            values.extend(std::iter::repeat_n(run as i32, run_length));
        }
        values
    }

    #[cfg(feature = "bitpacking")]
    fn high_entropy_runs(num_runs: usize, run_length: usize) -> Vec<i32> {
        let mut values = Vec::with_capacity(num_runs * run_length);
        let mut state = 7u64;
        for _ in 0..num_runs {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            values.extend(std::iter::repeat_n((state >> 32) as i32, run_length));
        }
        values
    }

    #[test]
    fn test_select_run_length_width_prefers_u16_for_long_runs() {
        let mut entries = [0u64; 3];
        accumulate_run_length_entries(300, Some(*MAX_MINIBLOCK_VALUES), &mut entries);
        let (width, _) = select_run_length_width_from_entries(&entries, 32).unwrap();
        assert_eq!(width, RunLengthWidth::U16);
    }

    // ========== Round-trip Tests for Different Types ==========

    #[test]
    fn test_round_trip_all_types() {
        // Test u8
        test_round_trip_helper(vec![42u8, 42, 42, 100, 100, 255, 255, 255, 255], 8);

        // Test u16
        test_round_trip_helper(vec![1000u16, 1000, 2000, 2000, 2000, 3000], 16);

        // Test i32
        test_round_trip_helper(vec![100i32, 100, 100, -200, -200, 300, 300, 300, 300], 32);

        // Test u64
        test_round_trip_helper(vec![1_000_000_000u64; 5], 64);
    }

    fn test_round_trip_helper<T>(data: Vec<T>, bits_per_value: u64)
    where
        T: bytemuck::Pod + PartialEq + std::fmt::Debug,
    {
        let encoder = RleEncoder::new();
        let bytes: Vec<u8> = data
            .iter()
            .flat_map(|v| bytemuck::bytes_of(v))
            .copied()
            .collect();

        let block = DataBlock::FixedWidth(FixedWidthDataBlock {
            bits_per_value,
            data: LanceBuffer::from(bytes),
            num_values: data.len() as u64,
            block_info: BlockInfo::default(),
        });

        let (compressed, _) = compress_miniblock(&encoder, block).unwrap();
        let decompressor = RleDecompressor::new(bits_per_value);
        let decompressed = MiniBlockDecompressor::decompress(
            &decompressor,
            compressed.data,
            compressed.num_values,
        )
        .unwrap();

        match decompressed {
            DataBlock::FixedWidth(ref block) => {
                // Verify the decompressed data length matches expected
                assert_eq!(block.data.len(), data.len() * std::mem::size_of::<T>());
            }
            _ => panic!("Expected FixedWidth block"),
        }
    }

    // ========== Chunk Boundary Tests ==========

    #[test]
    fn test_power_of_two_chunking() {
        let encoder = RleEncoder::new();

        // Create data that will require multiple chunks
        let test_sizes = vec![1000, 2500, 5000, 10000];

        for size in test_sizes {
            let data: Vec<i32> = (0..size)
                .map(|i| i / 50) // Create runs of 50
                .collect();

            let array = Int32Array::from(data);
            let (compressed, _) =
                compress_miniblock(&encoder, DataBlock::from_array(array)).unwrap();

            // Verify all non-last chunks have power-of-2 values
            for (i, chunk) in compressed.chunks.iter().enumerate() {
                if i < compressed.chunks.len() - 1 {
                    assert!(chunk.log_num_values > 0);
                    let chunk_values = 1u64 << chunk.log_num_values;
                    assert!(chunk_values.is_power_of_two());
                    assert!(chunk_values <= *MAX_MINIBLOCK_VALUES);
                } else {
                    assert_eq!(chunk.log_num_values, 0);
                }
            }
        }
    }

    #[rstest]
    #[case::u8_lengths(RunLengthWidth::U8)]
    #[case::u16_lengths(RunLengthWidth::U16)]
    #[case::u32_lengths(RunLengthWidth::U32)]
    fn test_miniblock_chunk_counts_match_encoded_runs(#[case] run_length_width: RunLengthWidth) {
        // This pattern crosses the 2,048-value boundary in the middle of a two-value run.
        let levels = (0..4098)
            .map(|index| if index % 3 == 0 { 1u16 } else { 0u16 })
            .collect::<Vec<_>>();
        let num_values = levels.len() as u64;
        let encoder = RleEncoder::with_run_length_width(run_length_width);
        let (buffers, chunks) = encoder
            .encode_data(
                &LanceBuffer::reinterpret_vec(levels),
                num_values,
                u16::BITS as u64,
            )
            .unwrap();

        assert_eq!(buffers.len(), 2);
        let bytes_per_length = run_length_width.bytes_per_value();
        let mut values_offset = 0usize;
        let mut lengths_offset = 0usize;
        let mut values_processed = 0u64;

        for chunk in &chunks {
            let values_size = chunk.buffer_sizes[0] as usize;
            let lengths_size = chunk.buffer_sizes[1] as usize;
            let lengths_end = lengths_offset + lengths_size;
            let chunk_lengths = &buffers[1].as_ref()[lengths_offset..lengths_end];
            let length_chunks = chunk_lengths.chunks_exact(bytes_per_length);
            assert!(length_chunks.remainder().is_empty());
            let num_runs = length_chunks.len();
            let encoded_values = length_chunks
                .map(|bytes| run_length_width.read_length(bytes))
                .sum::<u64>();
            let declared_values = chunk.num_values(values_processed, num_values);

            assert_eq!(values_size, num_runs * size_of::<u16>());
            assert_eq!(encoded_values, declared_values);

            values_offset += values_size;
            lengths_offset = lengths_end;
            values_processed += declared_values;
        }

        assert_eq!(values_processed, num_values);
        assert_eq!(values_offset, buffers[0].len());
        assert_eq!(lengths_offset, buffers[1].len());
    }

    // ========== Error Handling Tests ==========

    #[test]
    fn test_encoder_rejects_zero_progress() {
        let error = RleEncoder::new()
            .encode_data(&LanceBuffer::empty(), 1, u16::BITS as u64)
            .unwrap_err();

        assert!(
            matches!(&error, Error::Internal { .. }),
            "expected internal error, got: {error:?}"
        );
        assert!(error.to_string().contains("made no progress"));
        assert!(error.to_string().contains("values_remaining=1"));
    }

    #[test]
    fn test_invalid_buffer_count() {
        let decompressor = RleDecompressor::new(32);
        let result = MiniBlockDecompressor::decompress(
            &decompressor,
            vec![LanceBuffer::from(vec![1, 2, 3, 4])],
            10,
        );
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("expects exactly 2 buffers")
        );
    }

    #[test]
    fn test_buffer_consistency() {
        let decompressor = RleDecompressor::new(32);
        let values = LanceBuffer::from(vec![1, 0, 0, 0]); // 1 i32 value
        let lengths = LanceBuffer::from(vec![5, 10]); // 2 lengths - mismatch!
        let result = MiniBlockDecompressor::decompress(&decompressor, vec![values, lengths], 15);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Inconsistent RLE buffers")
        );
    }

    #[test]
    fn test_u16_length_buffer_must_be_aligned() {
        let decompressor = RleDecompressor::with_run_length_width(32, RunLengthWidth::U16);
        let values = LanceBuffer::from(vec![1, 0, 0, 0]);
        let lengths = LanceBuffer::from(vec![5]);
        let result = MiniBlockDecompressor::decompress(&decompressor, vec![values, lengths], 5);
        assert!(matches!(&result, Err(Error::InvalidInput { .. })));
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("not divisible by 2")
        );
    }

    #[test]
    fn test_rle_rejects_underflow_and_zero_lengths_and_clamps_overflow() {
        let decompressor = RleDecompressor::with_run_length_width(32, RunLengthWidth::U16);
        let value = LanceBuffer::from(1i32.to_le_bytes().to_vec());

        let underflow = MiniBlockDecompressor::decompress(
            &decompressor,
            vec![
                value.clone(),
                LanceBuffer::from(4u16.to_le_bytes().to_vec()),
            ],
            5,
        )
        .unwrap_err();
        assert!(underflow.to_string().contains("produced 4 values"));

        let overflow = MiniBlockDecompressor::decompress(
            &decompressor,
            vec![
                value.clone(),
                LanceBuffer::from(6u16.to_le_bytes().to_vec()),
            ],
            5,
        )
        .unwrap();
        match overflow {
            DataBlock::FixedWidth(block) => {
                assert_eq!(block.num_values, 5);
                let decoded = block.data.borrow_to_typed_slice::<i32>();
                assert_eq!(decoded.as_ref(), &[1i32; 5]);
            }
            _ => panic!("Expected FixedWidth block"),
        }

        let zero = MiniBlockDecompressor::decompress(
            &decompressor,
            vec![value, LanceBuffer::from(0u16.to_le_bytes().to_vec())],
            5,
        )
        .unwrap_err();
        assert!(zero.to_string().contains("zero run length"));
    }

    #[test]
    fn test_block_rle_rejects_overflow() {
        // Block payloads have no chunk boundaries, so run lengths summing past
        // num_values can only be corruption and must stay a hard error.
        let decompressor = RleDecompressor::with_run_length_width(32, RunLengthWidth::U16);
        let values = 1i32.to_le_bytes();
        let lengths = 6u16.to_le_bytes();
        let mut payload = Vec::new();
        payload.extend_from_slice(&(values.len() as u64).to_le_bytes());
        payload.extend_from_slice(&values);
        payload.extend_from_slice(&lengths);

        let error = BlockDecompressor::decompress(&decompressor, LanceBuffer::from(payload), 5)
            .unwrap_err();
        assert!(matches!(&error, Error::InvalidInput { .. }));
        assert!(
            error
                .to_string()
                .contains("overflowed expected value count")
        );
    }

    #[test]
    fn test_rle_truncates_legacy_chunk_boundary_overflow() {
        // Legacy encoders emitted chunks declaring 2048 values whose final run crossed
        // the checkpoint boundary (e.g. run lengths summing to 2080); the excess values
        // are duplicated at the start of the next chunk and must be ignored here.
        let decompressor = RleDecompressor::with_run_length_width(32, RunLengthWidth::U16);
        let mut values = Vec::new();
        values.extend_from_slice(&7i32.to_le_bytes());
        values.extend_from_slice(&8i32.to_le_bytes());
        let mut lengths = Vec::new();
        lengths.extend_from_slice(&2000u16.to_le_bytes());
        lengths.extend_from_slice(&80u16.to_le_bytes());

        let decoded = MiniBlockDecompressor::decompress(
            &decompressor,
            vec![LanceBuffer::from(values), LanceBuffer::from(lengths)],
            2048,
        )
        .unwrap();
        match decoded {
            DataBlock::FixedWidth(block) => {
                assert_eq!(block.num_values, 2048);
                let decoded = block.data.borrow_to_typed_slice::<i32>();
                let decoded = decoded.as_ref();
                assert_eq!(decoded.len(), 2048);
                assert!(decoded[..2000].iter().all(|&v| v == 7));
                assert!(decoded[2000..].iter().all(|&v| v == 8));
            }
            _ => panic!("Expected FixedWidth block"),
        }
    }

    #[test]
    fn test_empty_data_handling() {
        let encoder = RleEncoder::new();

        // Test empty block
        let empty_block = DataBlock::FixedWidth(FixedWidthDataBlock {
            bits_per_value: 32,
            data: LanceBuffer::from(vec![]),
            num_values: 0,
            block_info: BlockInfo::default(),
        });

        let (compressed, _) = compress_miniblock(&encoder, empty_block).unwrap();
        assert_eq!(compressed.num_values, 0);
        assert!(compressed.data.is_empty());

        // Test decompression of empty data
        let decompressor = RleDecompressor::new(32);
        let decompressed = MiniBlockDecompressor::decompress(&decompressor, vec![], 0).unwrap();

        match decompressed {
            DataBlock::FixedWidth(ref block) => {
                assert_eq!(block.num_values, 0);
                assert_eq!(block.data.len(), 0);
            }
            _ => panic!("Expected FixedWidth block"),
        }
    }

    // ========== Integration Test ==========

    #[test]
    fn test_multi_chunk_round_trip() {
        let encoder = RleEncoder::new();

        // Create data that spans multiple chunks with mixed patterns
        let mut data = Vec::new();

        // High compression section
        data.extend(vec![999i32; 2000]);
        // Low compression section
        data.extend(0..1000);
        // Another high compression section
        data.extend(vec![777i32; 2000]);

        let array = Int32Array::from(data.clone());
        let (compressed, _) = compress_miniblock(&encoder, DataBlock::from_array(array)).unwrap();

        // Manually decompress all chunks
        let mut reconstructed = Vec::new();
        let mut values_offset = 0usize;
        let mut lengths_offset = 0usize;
        let mut values_processed = 0u64;

        // We now have exactly 2 global buffers
        assert_eq!(compressed.data.len(), 2);
        let global_values = &compressed.data[0];
        let global_lengths = &compressed.data[1];

        for chunk in &compressed.chunks {
            let chunk_values = if chunk.log_num_values > 0 {
                1u64 << chunk.log_num_values
            } else {
                compressed.num_values - values_processed
            };

            // Extract chunk buffers from global buffers using buffer_sizes
            let values_size = chunk.buffer_sizes[0] as usize;
            let lengths_size = chunk.buffer_sizes[1] as usize;

            let chunk_values_buffer = global_values.slice_with_length(values_offset, values_size);
            let chunk_lengths_buffer =
                global_lengths.slice_with_length(lengths_offset, lengths_size);

            let decompressor = RleDecompressor::new(32);
            let chunk_data = MiniBlockDecompressor::decompress(
                &decompressor,
                vec![chunk_values_buffer, chunk_lengths_buffer],
                chunk_values,
            )
            .unwrap();

            values_offset += values_size;
            lengths_offset += lengths_size;
            values_processed += chunk_values;

            match chunk_data {
                DataBlock::FixedWidth(ref block) => {
                    let values: &[i32] = bytemuck::cast_slice(block.data.as_ref());
                    reconstructed.extend_from_slice(values);
                }
                _ => panic!("Expected FixedWidth block"),
            }
        }

        assert_eq!(reconstructed, data);
    }

    #[test]
    fn test_1024_boundary_conditions() {
        // Comprehensive test for various boundary conditions at 1024 values
        // This consolidates multiple bug tests that were previously separate
        let encoder = RleEncoder::new();
        let decompressor = RleDecompressor::new(32);

        let test_cases = [
            ("runs_of_2", {
                let mut data = Vec::new();
                for i in 0..512 {
                    data.push(i);
                    data.push(i);
                }
                data
            }),
            ("single_run_1024", vec![42i32; 1024]),
            ("alternating_values", {
                let mut data = Vec::new();
                for i in 0..1024 {
                    data.push(i % 2);
                }
                data
            }),
            ("run_boundary_255s", {
                let mut data = Vec::new();
                data.extend(vec![1i32; 255]);
                data.extend(vec![2i32; 255]);
                data.extend(vec![3i32; 255]);
                data.extend(vec![4i32; 255]);
                data.extend(vec![5i32; 4]);
                data
            }),
            ("unique_values_1024", (0..1024).collect::<Vec<_>>()),
            ("unique_plus_duplicate", {
                // 1023 unique values followed by one duplicate (regression test)
                let mut data = Vec::new();
                for i in 0..1023 {
                    data.push(i);
                }
                data.push(1022i32); // Last value same as second-to-last
                data
            }),
            ("bug_4092_pattern", {
                // Test exact scenario that produces 4092 bytes instead of 4096
                let mut data = Vec::new();
                for i in 0..1022 {
                    data.push(i);
                }
                data.push(999999i32);
                data.push(999999i32);
                data
            }),
        ];

        for (test_name, data) in test_cases.iter() {
            assert_eq!(data.len(), 1024, "Test case {} has wrong length", test_name);

            // Compress the data
            let array = Int32Array::from(data.clone());
            let (compressed, _) =
                compress_miniblock(&encoder, DataBlock::from_array(array)).unwrap();

            // Decompress and verify
            match MiniBlockDecompressor::decompress(
                &decompressor,
                compressed.data,
                compressed.num_values,
            ) {
                Ok(decompressed) => match decompressed {
                    DataBlock::FixedWidth(ref block) => {
                        let values: &[i32] = bytemuck::cast_slice(block.data.as_ref());
                        assert_eq!(
                            values.len(),
                            1024,
                            "Test case {} got {} values, expected 1024",
                            test_name,
                            values.len()
                        );
                        assert_eq!(
                            block.data.len(),
                            4096,
                            "Test case {} got {} bytes, expected 4096",
                            test_name,
                            block.data.len()
                        );
                        assert_eq!(values, &data[..], "Test case {} data mismatch", test_name);
                    }
                    _ => panic!("Test case {} expected FixedWidth block", test_name),
                },
                Err(e) => {
                    if e.to_string().contains("4092") {
                        panic!("Test case {} found bug 4092: {}", test_name, e);
                    }
                    panic!("Test case {} failed with error: {}", test_name, e);
                }
            }
        }
    }

    #[test]
    fn test_low_repetition_50pct_bug() {
        // Test case that reproduces the 4092 bytes bug with low repetition (50%)
        // This simulates the 1M benchmark case
        let encoder = RleEncoder::new();

        // Create 1M values with low repetition (50% chance of change)
        let num_values = 1_048_576; // 1M values
        let mut data = Vec::with_capacity(num_values);
        let mut value = 0i32;
        let mut rng = 12345u64; // Simple deterministic RNG

        for _ in 0..num_values {
            data.push(value);
            // Simple LCG for deterministic randomness
            rng = rng.wrapping_mul(1664525).wrapping_add(1013904223);
            // 50% chance to increment value
            if (rng >> 16) & 1 == 1 {
                value += 1;
            }
        }

        let bytes: Vec<u8> = data.iter().flat_map(|v| v.to_le_bytes()).collect();

        let block = DataBlock::FixedWidth(FixedWidthDataBlock {
            bits_per_value: 32,
            data: LanceBuffer::from(bytes),
            num_values: num_values as u64,
            block_info: BlockInfo::default(),
        });

        let (compressed, _) = compress_miniblock(&encoder, block).unwrap();

        // Debug first few chunks
        for (i, chunk) in compressed.chunks.iter().take(5).enumerate() {
            let _chunk_values = if chunk.log_num_values > 0 {
                1 << chunk.log_num_values
            } else {
                // Last chunk - calculate remaining
                let prev_total: usize = compressed.chunks[..i]
                    .iter()
                    .map(|c| 1usize << c.log_num_values)
                    .sum();
                num_values - prev_total
            };
        }

        // Try to decompress
        let decompressor = RleDecompressor::new(32);
        match MiniBlockDecompressor::decompress(
            &decompressor,
            compressed.data,
            compressed.num_values,
        ) {
            Ok(decompressed) => match decompressed {
                DataBlock::FixedWidth(ref block) => {
                    assert_eq!(
                        block.data.len(),
                        num_values * 4,
                        "Expected {} bytes but got {}",
                        num_values * 4,
                        block.data.len()
                    );
                }
                _ => panic!("Expected FixedWidth block"),
            },
            Err(e) => {
                if e.to_string().contains("4092") {
                    panic!("Bug reproduced! {}", e);
                } else {
                    panic!("Unexpected error: {}", e);
                }
            }
        }
    }

    // ========== Encoding Verification Tests ==========

    #[test_log::test(tokio::test)]
    async fn test_rle_encoding_verification() {
        use crate::testing::{TestCases, check_round_trip_encoding_of_data};
        use arrow_array::{Array, Int32Array};
        use lance_datagen::{ArrayGenerator, RowCount};
        use std::collections::HashMap;
        use std::sync::Arc;

        let test_cases = TestCases::default()
            .with_expected_encoding("rle")
            .with_structural_encodings();

        // Test both explicit metadata and automatic selection
        // 1. Test with explicit RLE threshold metadata (also disable BSS)
        let mut metadata_explicit = HashMap::new();
        metadata_explicit.insert(
            "lance-encoding:rle-threshold".to_string(),
            "0.8".to_string(),
        );
        metadata_explicit.insert("lance-encoding:bss".to_string(), "off".to_string());

        let mut generator = RleDataGenerator::new(vec![
            i32::MIN,
            i32::MIN,
            i32::MIN,
            i32::MIN,
            i32::MIN + 1,
            i32::MIN + 1,
            i32::MIN + 1,
            i32::MIN + 1,
            i32::MIN + 2,
            i32::MIN + 2,
            i32::MIN + 2,
            i32::MIN + 2,
        ]);
        let data_explicit = generator.generate_default(RowCount::from(10000)).unwrap();
        check_round_trip_encoding_of_data(vec![data_explicit], &test_cases, metadata_explicit)
            .await;

        // 2. Test automatic RLE selection based on data characteristics
        // 80% repetition should trigger RLE (> default 50% threshold).
        //
        // Use values with the high bit set so bitpacking can't shrink the values.
        // Explicitly disable BSS to ensure RLE is tested
        let mut metadata = HashMap::new();
        metadata.insert("lance-encoding:bss".to_string(), "off".to_string());

        let mut values = vec![i32::MIN; 8000]; // 80% repetition
        values.extend(
            [
                i32::MIN + 1,
                i32::MIN + 2,
                i32::MIN + 3,
                i32::MIN + 4,
                i32::MIN + 5,
            ]
            .repeat(400),
        ); // 20% variety
        let arr = Arc::new(Int32Array::from(values)) as Arc<dyn Array>;
        check_round_trip_encoding_of_data(vec![arr], &test_cases, metadata).await;

        #[cfg(any(feature = "lz4", feature = "zstd"))]
        {
            let mut metadata = HashMap::new();
            metadata.insert(
                "lance-encoding:rle-threshold".to_string(),
                "0.8".to_string(),
            );
            metadata.insert("lance-encoding:bss".to_string(), "off".to_string());
            metadata.insert(
                "lance-encoding:compression".to_string(),
                if cfg!(feature = "zstd") {
                    "zstd".to_string()
                } else {
                    "lz4".to_string()
                },
            );
            let mut values = Vec::with_capacity(2048 * 4);
            for run in 0..2048 {
                values.extend(std::iter::repeat_n(i32::MIN + (run % 8), 4));
            }
            let arr = Arc::new(Int32Array::from(values)) as Arc<dyn Array>;
            check_round_trip_encoding_of_data(vec![arr], &test_cases, metadata).await;
        }
    }

    /// Generator that produces repetitive patterns suitable for RLE
    #[derive(Debug)]
    struct RleDataGenerator {
        pattern: Vec<i32>,
        idx: usize,
    }

    impl RleDataGenerator {
        fn new(pattern: Vec<i32>) -> Self {
            Self { pattern, idx: 0 }
        }
    }

    impl lance_datagen::ArrayGenerator for RleDataGenerator {
        fn generate(
            &mut self,
            _length: lance_datagen::RowCount,
            _rng: &mut rand_xoshiro::Xoshiro256PlusPlus,
        ) -> std::result::Result<std::sync::Arc<dyn arrow_array::Array>, arrow_schema::ArrowError>
        {
            use arrow_array::Int32Array;
            use std::sync::Arc;

            // Generate enough repetitive data to trigger RLE
            let mut values = Vec::new();
            for _ in 0..10000 {
                values.push(self.pattern[self.idx]);
                self.idx = (self.idx + 1) % self.pattern.len();
            }
            Ok(Arc::new(Int32Array::from(values)))
        }

        fn data_type(&self) -> &arrow_schema::DataType {
            &arrow_schema::DataType::Int32
        }

        fn element_size_bytes(&self) -> Option<lance_datagen::ByteCount> {
            Some(lance_datagen::ByteCount::from(4))
        }
    }

    // ========== Block Related tests ==========
    #[test]
    fn test_block_decompressor_rejects_overflowing_values_size() {
        let decompressor = RleDecompressor::new(32);

        let mut data = Vec::new();
        data.extend_from_slice(&u64::MAX.to_le_bytes());
        let result = BlockDecompressor::decompress(&decompressor, LanceBuffer::from(data), 1);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Invalid RLE values buffer size")
        );
    }

    #[test]
    fn test_block_decompressor_too_small() {
        let decompressor = RleDecompressor::new(32);
        let result =
            BlockDecompressor::decompress(&decompressor, LanceBuffer::from(vec![1, 2, 3]), 10);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Insufficient data size: 3")
        );
    }

    #[test]
    fn test_block_compressor_header_format() {
        let encoder = RleEncoder::new();

        let data = vec![1i32, 1, 1];
        let array = Int32Array::from(data);
        let compressed = BlockCompressor::compress(&encoder, DataBlock::from_array(array)).unwrap();

        // Verify header format: first 8 bytes should be values_size as u64
        assert!(compressed.len() >= 8);
        let values_size_bytes: [u8; 8] = compressed.as_ref()[..8].try_into().unwrap();
        let values_size = u64::from_le_bytes(values_size_bytes);

        // Values buffer should contain 1 i32 value (4 bytes)
        assert_eq!(values_size, 4);

        // Total size should be: 8 (header) + 4 (values) + 1 (lengths)
        assert_eq!(compressed.len(), 13);
    }

    #[test]
    fn test_block_compressor_round_trip() {
        let encoder = RleEncoder::new();
        let decompressor = RleDecompressor::new(32);

        // Test basic pattern
        let data = vec![1i32, 1, 1, 2, 2, 3, 3, 3, 3];
        let array = Int32Array::from(data.clone());
        let data_block = DataBlock::from_array(array);

        let compressed = BlockCompressor::compress(&encoder, data_block).unwrap();
        let decompressed =
            BlockDecompressor::decompress(&decompressor, compressed, data.len() as u64).unwrap();

        match decompressed {
            DataBlock::FixedWidth(block) => {
                let values: &[i32] = bytemuck::cast_slice(block.data.as_ref());
                assert_eq!(values, &data[..]);
            }
            _ => panic!("Expected FixedWidth block"),
        }
    }

    #[test]
    fn test_block_compressor_large_data() {
        let encoder = RleEncoder::new();
        let decompressor = RleDecompressor::new(32);

        // Create data that will span multiple chunks
        // Each chunks can handle ~2048 values, so use 10K values
        let mut data = Vec::new();
        data.extend(vec![999i32; 3000]); // First ~2 chunks
        data.extend(vec![777i32; 3000]); // Next ~2 chunks
        data.extend(vec![555i32; 4000]); // Final ~2 chunks

        let total_values = data.len();
        assert_eq!(total_values, 10000);

        let array = Int32Array::from(data.clone());
        let compressed = BlockCompressor::compress(&encoder, DataBlock::from_array(array)).unwrap();
        let decompressed =
            BlockDecompressor::decompress(&decompressor, compressed, total_values as u64).unwrap();

        match decompressed {
            DataBlock::FixedWidth(block) => {
                let values: &[i32] = bytemuck::cast_slice(block.data.as_ref());
                assert_eq!(values.len(), total_values);
                assert_eq!(values, &data[..]);
            }
            _ => panic!("Expected FixedWidth block"),
        }
    }
}
