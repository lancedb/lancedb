// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Compression traits and definitions for Lance 2.1
//!
//! In 2.1 the first step of encoding is structural encoding, where we shred inputs into
//! leaf arrays and take care of the validity / offsets structure.  Then we pick a structural
//! encoding (mini-block or full-zip) and then we compress the data.
//!
//! This module defines the traits for the compression step.  Each structural encoding has its
//! own compression strategy.
//!
//! Miniblock compression is a block based approach for small data.  Since we introduce some read
//! amplification and decompress entire blocks we are able to use opaque compression.
//!
//! Fullzip compression is a per-value approach where we require that values are transparently
//! compressed so that we can locate them later.

#[cfg(feature = "bitpacking")]
use crate::encodings::physical::bitpacking::{InlineBitpacking, OutOfLineBitpacking};
use crate::{
    buffer::LanceBuffer,
    compression_config::{BssMode, CompressionFieldParams},
    constants::{
        BSS_META_KEY, COMPRESSION_LEVEL_META_KEY, COMPRESSION_META_KEY, RLE_THRESHOLD_META_KEY,
    },
    data::{DataBlock, FixedWidthDataBlock, VariableWidthBlock},
    encodings::{
        logical::primitive::{
            fullzip::PerValueCompressor,
            miniblock::{MAX_MINIBLOCK_VALUES, MiniBlockCompressor},
        },
        physical::{
            binary::{
                BinaryBlockDecompressor, BinaryMiniBlockDecompressor, BinaryMiniBlockEncoder,
                VariableDecoder, VariableEncoder,
            },
            block::{
                CompressedBufferEncoder, CompressionConfig, CompressionScheme,
                GeneralBlockDecompressor,
            },
            byte_stream_split::{
                ByteStreamSplitDecompressor, ByteStreamSplitEncoder, should_use_bss,
            },
            constant::ConstantDecompressor,
            fsst::{
                FsstMiniBlockDecompressor, FsstMiniBlockEncoder, FsstPerValueDecompressor,
                FsstPerValueEncoder,
            },
            general::{GeneralMiniBlockCompressor, GeneralMiniBlockDecompressor},
            packed::{
                PackedStructFixedWidthMiniBlockDecompressor,
                PackedStructFixedWidthMiniBlockEncoder, PackedStructVariablePerValueDecompressor,
                PackedStructVariablePerValueEncoder, VariablePackedStructFieldDecoder,
                VariablePackedStructFieldKind,
            },
            rle::{
                RleChildDecompressor, RleDecompressor, RleEncoder, RunLengthWidth,
                rle_encoded_size, select_run_length_width,
            },
            value::{ValueDecompressor, ValueEncoder},
        },
    },
    format::{
        ProtobufUtils21,
        pb21::{CompressiveEncoding, compressive_encoding::Compression},
    },
    statistics::{GetStat, Stat},
};

use arrow_array::{cast::AsArray, types::UInt64Type};
use arrow_schema::DataType;
use fsst::fsst::{FSST_LEAST_INPUT_MAX_LENGTH, FSST_LEAST_INPUT_SIZE};
use lance_core::{Error, Result, datatypes::Field, error::LanceOptionExt};
use std::{str::FromStr, sync::Arc};

/// Default threshold for RLE compression selection when the user explicitly provides a threshold.
///
/// If no threshold is provided, we use a size model instead of a fixed run ratio.
/// This preserves existing behavior for users relying on the default, while making
/// the default selection more type-aware.
const DEFAULT_RLE_COMPRESSION_THRESHOLD: f64 = 0.5;

// Minimum block size (32kb) to trigger general block compression
const MIN_BLOCK_SIZE_FOR_GENERAL_COMPRESSION: u64 = 32 * 1024;
const RLE_BLOCK_HEADER_BYTES: u128 = std::mem::size_of::<u64>() as u128;

/// Trait for compression algorithms that compress an entire block of data into one opaque
/// and self-described chunk.
///
/// This is actually a _third_ compression strategy used in a few corner cases today (TODO: remove?)
///
/// This is the most general type of compression.  There are no constraints on the method
/// of compression it is assumed that the entire block of data will be present at decompression.
///
/// This is the least appropriate strategy for random access because we must load the entire
/// block to access any single value.  This should only be used for cases where random access is never
/// required (e.g. when encoding metadata buffers like a dictionary or for encoding rep/def
/// mini-block chunks)
pub trait BlockCompressor: std::fmt::Debug + Send + Sync {
    /// Compress the data into a single buffer
    ///
    /// Also returns a description of the compression that can be used to decompress
    /// when reading the data back
    fn compress(&self, data: DataBlock) -> Result<LanceBuffer>;
}

/// A trait to pick which compression to use for given data
///
/// There are several different kinds of compression.
///
/// - Block compression is the most generic, but most difficult to use efficiently
/// - Per-value compression results in either a fixed width data block or a variable
///   width data block.  In other words, there is some number of bits per value.
///   In addition, each value should be independently decompressible.
/// - Mini-block compression results in a small block of opaque data for chunks
///   of rows.  Each block is somewhere between 0 and 16KiB in size.  This is
///   used for narrow data types (both fixed and variable length) where we can
///   fit many values into an 16KiB block.
pub trait CompressionStrategy: Send + Sync + std::fmt::Debug {
    /// Create a block compressor for the given data
    fn create_block_compressor(
        &self,
        field: &Field,
        data: &DataBlock,
    ) -> Result<(Box<dyn BlockCompressor>, CompressiveEncoding)>;

    /// Create a per-value compressor for the given data
    fn create_per_value(
        &self,
        field: &Field,
        data: &DataBlock,
    ) -> Result<Box<dyn PerValueCompressor>>;

    /// Create a mini-block compressor for the given data
    fn create_miniblock_compressor(
        &self,
        field: &Field,
        data: &DataBlock,
    ) -> Result<Box<dyn MiniBlockCompressor>>;
}

fn try_bss_for_mini_block(
    data: &FixedWidthDataBlock,
    params: &CompressionFieldParams,
) -> Option<Box<dyn MiniBlockCompressor>> {
    // BSS requires general compression to be effective
    // If compression is not set or explicitly disabled, skip BSS
    if params.compression.is_none() || params.compression.as_deref() == Some("none") {
        return None;
    }

    let mode = params.bss.unwrap_or(BssMode::Auto);
    // should_use_bss already checks for supported bit widths (32/64)
    if should_use_bss(data, mode) {
        return Some(Box::new(ByteStreamSplitEncoder::new(
            data.bits_per_value as usize,
        )));
    }
    None
}

fn rle_is_applicable(data: &FixedWidthDataBlock, params: &CompressionFieldParams) -> Option<u128> {
    let bits = data.bits_per_value;
    if !matches!(bits, 8 | 16 | 32 | 64) {
        return None;
    }

    let type_size = bits / 8;
    let run_count = data.expect_single_stat::<UInt64Type>(Stat::RunCount);
    let threshold = params
        .rle_threshold
        .unwrap_or(DEFAULT_RLE_COMPRESSION_THRESHOLD);

    // If the user explicitly provided a threshold then honor it as an additional guard.
    // A lower threshold makes RLE harder to trigger and can be used to avoid CPU overhead.
    let passes_threshold = match params.rle_threshold {
        Some(_) => (run_count as f64) < (data.num_values as f64) * threshold,
        None => true,
    };

    if !passes_threshold {
        return None;
    }

    Some((data.num_values as u128) * (type_size as u128))
}

fn rle_beats_raw_and_bitpacking(
    data: &FixedWidthDataBlock,
    encoded_bytes: u128,
    raw_bytes: u128,
) -> bool {
    if encoded_bytes >= raw_bytes {
        return false;
    }

    #[cfg(feature = "bitpacking")]
    {
        if let Some(bitpack_bytes) = estimate_inline_bitpacking_bytes(data).map(u128::from)
            && bitpack_bytes < encoded_bytes
        {
            return false;
        }
    }
    true
}

fn try_fixed_u8_rle_for_mini_block(
    data: &FixedWidthDataBlock,
    params: &CompressionFieldParams,
) -> Option<Box<dyn MiniBlockCompressor>> {
    let raw_bytes = rle_is_applicable(data, params)?;
    let rle_bytes = estimate_rle_size_for_width_from_data(
        data,
        Some(*MAX_MINIBLOCK_VALUES),
        RunLengthWidth::U8,
    )
    .ok()?;
    rle_beats_raw_and_bitpacking(data, rle_bytes, raw_bytes)
        .then(|| Box::new(RleEncoder::with_run_length_width(RunLengthWidth::U8)) as _)
}

fn try_child_rle_for_mini_block(
    data: &FixedWidthDataBlock,
    params: &CompressionFieldParams,
) -> Option<Box<dyn MiniBlockCompressor>> {
    let raw_bytes = rle_is_applicable(data, params)?;
    let (run_length_width, estimated_bytes) =
        estimate_rle_width_and_size_from_data(data, Some(*MAX_MINIBLOCK_VALUES)).ok()?;
    let child_compression = rle_child_compression_config(params);
    let encoder = || {
        RleEncoder::with_child_encoding(
            run_length_width,
            child_compression,
            child_compression,
            true,
        )
    };

    #[cfg(feature = "bitpacking")]
    let bitpack_bytes = estimate_inline_bitpacking_bytes(data).map(u128::from);
    #[cfg(not(feature = "bitpacking"))]
    let bitpack_bytes = None::<u128>;

    let should_measure_children = (child_compression.is_some() || cfg!(feature = "bitpacking"))
        && (estimated_bytes >= raw_bytes
            || bitpack_bytes.is_some_and(|bytes| bytes < estimated_bytes));
    let selected_bytes = if should_measure_children {
        encoder().selected_payload_size(data).ok()?
    } else {
        estimated_bytes
    };

    rle_beats_raw_and_bitpacking(data, selected_bytes, raw_bytes).then(|| Box::new(encoder()) as _)
}

fn rle_child_compression_config(params: &CompressionFieldParams) -> Option<CompressionConfig> {
    let raw = params.compression.as_deref()?;
    if matches!(raw, "none" | "fsst") {
        return None;
    }
    let scheme = CompressionScheme::from_str(raw).ok()?;
    Some(CompressionConfig::new(scheme, params.compression_level))
}

fn try_rle_for_block_with_width(
    data: &FixedWidthDataBlock,
    params: &CompressionFieldParams,
    run_length_width: RunLengthWidth,
    rle_payload_bytes: u128,
) -> Result<Option<(Box<dyn BlockCompressor>, CompressiveEncoding)>> {
    let bits = data.bits_per_value;
    if !matches!(bits, 8 | 16 | 32 | 64) {
        return Ok(None);
    }

    let run_count = data.expect_single_stat::<UInt64Type>(Stat::RunCount);
    let threshold = params
        .rle_threshold
        .unwrap_or(DEFAULT_RLE_COMPRESSION_THRESHOLD);

    let passes_threshold = match params.rle_threshold {
        Some(_) => (run_count as f64) < (data.num_values as f64) * threshold,
        None => true,
    };

    if !passes_threshold {
        return Ok(None);
    }

    let raw_bytes = (data.num_values as u128) * ((bits / 8) as u128);
    let rle_bytes = rle_payload_bytes.saturating_add(RLE_BLOCK_HEADER_BYTES);

    if rle_bytes >= raw_bytes {
        return Ok(None);
    }

    #[cfg(feature = "bitpacking")]
    {
        if let Some(bitpack_bytes) = estimate_block_bitpacking_bytes(data)
            && bitpack_bytes < rle_bytes
        {
            return Ok(None);
        }
    }

    let compressor = Box::new(RleEncoder::with_run_length_width(run_length_width));
    let encoding = ProtobufUtils21::rle(
        ProtobufUtils21::flat(bits, None),
        ProtobufUtils21::flat(run_length_width.bits_per_value(), None),
    );
    Ok(Some((compressor, encoding)))
}

fn try_fixed_u8_rle_for_block(
    data: &FixedWidthDataBlock,
    params: &CompressionFieldParams,
) -> Result<Option<(Box<dyn BlockCompressor>, CompressiveEncoding)>> {
    if !matches!(data.bits_per_value, 8 | 16 | 32 | 64) {
        return Ok(None);
    }
    let encoded_bytes = estimate_rle_size_for_width_from_data(data, None, RunLengthWidth::U8)?;
    try_rle_for_block_with_width(data, params, RunLengthWidth::U8, encoded_bytes)
}

fn try_variable_rle_for_block(
    data: &FixedWidthDataBlock,
    params: &CompressionFieldParams,
) -> Result<Option<(Box<dyn BlockCompressor>, CompressiveEncoding)>> {
    if !matches!(data.bits_per_value, 8 | 16 | 32 | 64) {
        return Ok(None);
    }
    let (width, encoded_bytes) = estimate_rle_width_and_size_from_data(data, None)?;
    try_rle_for_block_with_width(data, params, width, encoded_bytes)
}

fn estimate_rle_width_and_size_from_data(
    data: &FixedWidthDataBlock,
    max_segment_values: Option<u64>,
) -> Result<(RunLengthWidth, u128)> {
    select_run_length_width(
        &data.data,
        data.num_values,
        data.bits_per_value,
        max_segment_values,
    )
}

fn estimate_rle_size_for_width_from_data(
    data: &FixedWidthDataBlock,
    max_segment_values: Option<u64>,
    run_length_width: RunLengthWidth,
) -> Result<u128> {
    rle_encoded_size(
        &data.data,
        data.num_values,
        data.bits_per_value,
        max_segment_values,
        run_length_width,
    )
}

fn try_bitpack_for_mini_block(_data: &FixedWidthDataBlock) -> Option<Box<dyn MiniBlockCompressor>> {
    #[cfg(feature = "bitpacking")]
    {
        let bits = _data.bits_per_value;
        if estimate_inline_bitpacking_bytes(_data).is_some() {
            return Some(Box::new(InlineBitpacking::new(bits)));
        }
        None
    }
    #[cfg(not(feature = "bitpacking"))]
    {
        None
    }
}

#[cfg(feature = "bitpacking")]
fn estimate_inline_bitpacking_bytes(data: &FixedWidthDataBlock) -> Option<u64> {
    use arrow_array::cast::AsArray;

    let bits = data.bits_per_value;
    if !matches!(bits, 8 | 16 | 32 | 64) {
        return None;
    }
    if data.num_values == 0 {
        return None;
    }

    let bit_widths = data.expect_stat(Stat::BitWidth);
    let widths = bit_widths.as_primitive::<UInt64Type>();

    let words_per_chunk: u128 = 1;
    let word_bytes: u128 = (bits / 8) as u128;
    let mut total_words: u128 = 0;
    for i in 0..widths.len() {
        let bit_width = widths.value(i) as u128;
        let packed_words = (1024u128 * bit_width) / (bits as u128);
        total_words = total_words.saturating_add(words_per_chunk.saturating_add(packed_words));
    }

    let estimated_bytes = total_words.saturating_mul(word_bytes);
    let raw_bytes = data.data_size() as u128;

    if estimated_bytes >= raw_bytes {
        return None;
    }

    u64::try_from(estimated_bytes).ok()
}

fn try_bitpack_for_block(
    data: &FixedWidthDataBlock,
) -> Option<(Box<dyn BlockCompressor>, CompressiveEncoding)> {
    let bits = data.bits_per_value;
    if !matches!(bits, 8 | 16 | 32 | 64) {
        return None;
    }

    let bit_widths = data.expect_stat(Stat::BitWidth);
    let widths = bit_widths.as_primitive::<UInt64Type>();
    let max_bit_width = *widths.values().iter().max().unwrap();

    let too_small =
        widths.len() == 1 && InlineBitpacking::min_size_bytes(widths.value(0)) >= data.data_size();

    if too_small {
        return None;
    }

    if data.num_values <= 1024 {
        let compressor = Box::new(InlineBitpacking::new(bits));
        let encoding = ProtobufUtils21::inline_bitpacking(bits, None);
        Some((compressor, encoding))
    } else {
        let compressor = Box::new(OutOfLineBitpacking::new(max_bit_width, bits));
        let encoding = ProtobufUtils21::out_of_line_bitpacking(
            bits,
            ProtobufUtils21::flat(max_bit_width, None),
        );
        Some((compressor, encoding))
    }
}

#[cfg(feature = "bitpacking")]
fn estimate_block_bitpacking_bytes(data: &FixedWidthDataBlock) -> Option<u128> {
    let bits = data.bits_per_value;
    if !matches!(bits, 8 | 16 | 32 | 64) || data.num_values == 0 {
        return None;
    }

    let bit_widths = data.expect_stat(Stat::BitWidth);
    let widths = bit_widths.as_primitive::<UInt64Type>();
    let max_bit_width = *widths.values().iter().max()?;
    let word_bytes = (bits / 8) as u128;

    let bitpacked_words = if data.num_values <= 1024 {
        1 + (1024u128 * (max_bit_width as u128)) / (bits as u128)
    } else {
        estimate_out_of_line_bitpacking_words(data.num_values, max_bit_width, bits)?
    };
    let bitpacked_bytes = bitpacked_words.saturating_mul(word_bytes);
    if bitpacked_bytes >= data.data_size() as u128 {
        return None;
    }

    Some(bitpacked_bytes)
}

#[cfg(feature = "bitpacking")]
fn estimate_out_of_line_bitpacking_words(
    num_values: u64,
    compressed_bits_per_value: u64,
    bits_per_value: u64,
) -> Option<u128> {
    let num_values = usize::try_from(num_values).ok()?;
    let compressed_bits_per_value = usize::try_from(compressed_bits_per_value).ok()?;
    let bits_per_value = usize::try_from(bits_per_value).ok()?;
    if compressed_bits_per_value >= bits_per_value {
        return None;
    }

    let elems_per_chunk = 1024usize;
    let num_chunks = num_values.div_ceil(elems_per_chunk);
    let words_per_chunk = (elems_per_chunk * compressed_bits_per_value).div_ceil(bits_per_value);
    let last_chunk_is_runt = !num_values.is_multiple_of(elems_per_chunk);

    if !last_chunk_is_runt {
        return Some((num_chunks * words_per_chunk) as u128);
    }

    let num_whole_chunks = num_chunks - 1;
    let remaining_items = num_values - num_whole_chunks * elems_per_chunk;
    let tail_bit_savings = bits_per_value - compressed_bits_per_value;
    let padding_cost = compressed_bits_per_value * (elems_per_chunk - remaining_items);
    let tail_pack_savings = tail_bit_savings * remaining_items;
    let tail_words = if padding_cost < tail_pack_savings {
        words_per_chunk
    } else {
        remaining_items
    };

    Some((num_whole_chunks * words_per_chunk + tail_words) as u128)
}

fn maybe_wrap_general_for_mini_block(
    inner: Box<dyn MiniBlockCompressor>,
    params: &CompressionFieldParams,
) -> Result<Box<dyn MiniBlockCompressor>> {
    match params.compression.as_deref() {
        None | Some("none") | Some("fsst") => Ok(inner),
        Some(raw) => {
            let scheme = CompressionScheme::from_str(raw)
                .map_err(|_| Error::invalid_input(format!("Unknown compression scheme: {raw}")))?;
            let cfg = CompressionConfig::new(scheme, params.compression_level);
            Ok(Box::new(GeneralMiniBlockCompressor::new(inner, cfg)))
        }
    }
}

fn try_general_compression(
    field_params: &CompressionFieldParams,
    data: &DataBlock,
) -> Result<Option<(Box<dyn BlockCompressor>, CompressionConfig)>> {
    // Explicitly disable general compression.
    if field_params.compression.as_deref() == Some("none") {
        return Ok(None);
    }

    // User-requested compression (unused today but perhaps still used
    // in the future someday)
    if let Some(compression_scheme) = &field_params.compression {
        let scheme: CompressionScheme = compression_scheme.parse()?;
        let config = CompressionConfig::new(scheme, field_params.compression_level);
        let compressor = Box::new(CompressedBufferEncoder::try_new(config)?);
        return Ok(Some((compressor, config)));
    }

    // Automatic compression for large blocks
    if data.data_size() > MIN_BLOCK_SIZE_FOR_GENERAL_COMPRESSION {
        let compressor = Box::new(CompressedBufferEncoder::default());
        let config = compressor.compressor.config();
        return Ok(Some((compressor, config)));
    }

    Ok(None)
}

/// Parse field-level compression metadata without applying format-specific constraints.
pub fn field_metadata_params(field: &Field) -> CompressionFieldParams {
    let mut params = CompressionFieldParams::default();

    if let Some(compression) = field.metadata.get(COMPRESSION_META_KEY) {
        params.compression = Some(compression.clone());
    }
    if let Some(level) = field.metadata.get(COMPRESSION_LEVEL_META_KEY) {
        params.compression_level = level.parse().ok();
    }
    if let Some(threshold) = field.metadata.get(RLE_THRESHOLD_META_KEY) {
        params.rle_threshold = threshold.parse().ok();
    }
    if let Some(bss_str) = field.metadata.get(BSS_META_KEY) {
        match BssMode::parse(bss_str) {
            Some(mode) => params.bss = Some(mode),
            None => log::warn!("Invalid BSS mode '{}', using default", bss_str),
        }
    }
    if let Some(minichunk_size_str) = field
        .metadata
        .get(super::constants::MINICHUNK_SIZE_META_KEY)
    {
        if let Ok(minichunk_size) = minichunk_size_str.parse::<i64>() {
            params.minichunk_size = Some(minichunk_size);
        } else {
            log::warn!("Invalid minichunk_size '{}', skipping", minichunk_size_str);
        }
    }

    params
}

/// Apply general-purpose compression requested for a fixed-width miniblock.
pub fn finalize_miniblock_compressor(
    data: &DataBlock,
    compressor: Box<dyn MiniBlockCompressor>,
    params: &CompressionFieldParams,
) -> Result<Box<dyn MiniBlockCompressor>> {
    if matches!(data, DataBlock::FixedWidth(_)) {
        maybe_wrap_general_for_mini_block(compressor, params)
    } else {
        Ok(compressor)
    }
}

/// Honor an explicit `compression = none` request for fixed-width miniblocks.
pub fn try_uncompressed_fixed_width_miniblock(
    data: &DataBlock,
    params: &CompressionFieldParams,
) -> Option<Box<dyn MiniBlockCompressor>> {
    (matches!(data, DataBlock::FixedWidth(_)) && params.compression.as_deref() == Some("none"))
        .then(|| Box::new(ValueEncoder::default()) as _)
}

/// Select byte-stream-split compression for an applicable fixed-width miniblock.
pub fn try_byte_stream_split_miniblock(
    data: &DataBlock,
    params: &CompressionFieldParams,
) -> Option<Box<dyn MiniBlockCompressor>> {
    let DataBlock::FixedWidth(data) = data else {
        return None;
    };
    try_bss_for_mini_block(data, params)
}

/// Select the original fixed-u8 RLE miniblock grammar.
pub fn try_fixed_u8_rle_miniblock(
    data: &DataBlock,
    params: &CompressionFieldParams,
) -> Option<Box<dyn MiniBlockCompressor>> {
    let DataBlock::FixedWidth(data) = data else {
        return None;
    };
    try_fixed_u8_rle_for_mini_block(data, params)
}

/// Select variable-width RLE with independently encoded children.
pub fn try_child_rle_miniblock(
    data: &DataBlock,
    params: &CompressionFieldParams,
) -> Option<Box<dyn MiniBlockCompressor>> {
    let DataBlock::FixedWidth(data) = data else {
        return None;
    };
    try_child_rle_for_mini_block(data, params)
}

/// Select inline bitpacking for applicable fixed-width miniblocks.
pub fn try_bitpacking_miniblock(data: &DataBlock) -> Option<Box<dyn MiniBlockCompressor>> {
    let DataBlock::FixedWidth(data) = data else {
        return None;
    };
    try_bitpack_for_mini_block(data)
}

/// Store fixed-width miniblock values without a value codec.
pub fn try_raw_fixed_width_miniblock(data: &DataBlock) -> Option<Box<dyn MiniBlockCompressor>> {
    matches!(data, DataBlock::FixedWidth(_)).then(|| Box::new(ValueEncoder::default()) as _)
}

/// Encode variable-width miniblocks with binary or FSST encoding.
pub fn try_variable_width_miniblock(
    field: &Field,
    data: &DataBlock,
    params: &CompressionFieldParams,
) -> Result<Option<Box<dyn MiniBlockCompressor>>> {
    let DataBlock::VariableWidth(data) = data else {
        return Ok(None);
    };
    if data.bits_per_offset != 32 && data.bits_per_offset != 64 {
        return Err(Error::invalid_input(format!(
            "Variable width compression not supported for {} bit offsets",
            data.bits_per_offset
        )));
    }

    let compression = params.compression.as_deref();
    let data_size = data.expect_single_stat::<UInt64Type>(Stat::DataSize);
    let max_len = data.expect_single_stat::<UInt64Type>(Stat::MaxLength);
    if compression == Some("none") {
        return Ok(Some(Box::new(BinaryMiniBlockEncoder::new(
            params.minichunk_size,
        ))));
    }

    let use_fsst = compression == Some("fsst")
        || (compression.is_none()
            && !matches!(field.data_type(), DataType::Binary | DataType::LargeBinary)
            && max_len >= FSST_LEAST_INPUT_MAX_LENGTH
            && data_size >= FSST_LEAST_INPUT_SIZE as u64);
    let mut encoder: Box<dyn MiniBlockCompressor> = if use_fsst {
        Box::new(FsstMiniBlockEncoder::new(params.minichunk_size))
    } else {
        Box::new(BinaryMiniBlockEncoder::new(params.minichunk_size))
    };
    if let Some(compression_scheme) = compression.filter(|scheme| *scheme != "fsst") {
        let scheme: CompressionScheme = compression_scheme.parse()?;
        let config = CompressionConfig::new(scheme, params.compression_level);
        encoder = Box::new(GeneralMiniBlockCompressor::new(encoder, config));
    }
    Ok(Some(encoder))
}

/// Encode fixed-width packed structs as miniblocks.
pub fn try_fixed_packed_struct_miniblock(
    data: &DataBlock,
) -> Result<Option<Box<dyn MiniBlockCompressor>>> {
    let DataBlock::Struct(data) = data else {
        return Ok(None);
    };
    if data.has_variable_width_child() {
        return Err(Error::invalid_input(
            "Packed struct mini-block encoding supports only fixed-width children",
        ));
    }
    Ok(Some(Box::new(
        PackedStructFixedWidthMiniBlockEncoder::default(),
    )))
}

/// Store fixed-size-list miniblocks without a value codec.
pub fn try_raw_fixed_size_list_miniblock(data: &DataBlock) -> Option<Box<dyn MiniBlockCompressor>> {
    matches!(data, DataBlock::FixedSizeList(_)).then(|| Box::new(ValueEncoder::default()) as _)
}

/// Store fixed-width and fixed-size-list values directly in full-zip pages.
pub fn try_raw_per_value(data: &DataBlock) -> Option<Box<dyn PerValueCompressor>> {
    matches!(data, DataBlock::FixedWidth(_) | DataBlock::FixedSizeList(_))
        .then(|| Box::new(ValueEncoder::default()) as _)
}

fn validate_packed_struct(field: &Field, data: &DataBlock) -> Result<Option<bool>> {
    let DataBlock::Struct(data) = data else {
        return Ok(None);
    };
    if field.children.len() != data.children.len() {
        return Err(Error::invalid_input(
            "Struct field metadata does not match data block children",
        ));
    }
    Ok(Some(data.has_variable_width_child()))
}

/// Reject variable-width packed structs while preserving the fixed-width error.
pub fn reject_packed_struct_per_value(
    field: &Field,
    data: &DataBlock,
) -> Result<Option<Box<dyn PerValueCompressor>>> {
    let Some(has_variable_child) = validate_packed_struct(field, data)? else {
        return Ok(None);
    };
    if has_variable_child {
        return Err(Error::not_supported_source(
            "Variable packed struct encoding is not enabled by the selected file format".into(),
        ));
    }
    Err(Error::invalid_input(
        "Packed struct per-value compression should not be used for fixed-width-only structs",
    ))
}

/// Encode variable-width packed structs with the exact strategy recursively.
pub fn try_variable_packed_struct_per_value(
    strategy: Arc<dyn CompressionStrategy>,
    field: &Field,
    data: &DataBlock,
) -> Result<Option<Box<dyn PerValueCompressor>>> {
    let Some(has_variable_child) = validate_packed_struct(field, data)? else {
        return Ok(None);
    };
    if !has_variable_child {
        return Err(Error::invalid_input(
            "Packed struct per-value compression should not be used for fixed-width-only structs",
        ));
    }
    Ok(Some(Box::new(PackedStructVariablePerValueEncoder::new(
        strategy,
        field.children.clone(),
    ))))
}

/// Encode variable-width values directly, with FSST or per-value compression
/// when applicable.
pub fn try_variable_width_per_value(
    field: &Field,
    data: &DataBlock,
    params: &CompressionFieldParams,
) -> Result<Option<Box<dyn PerValueCompressor>>> {
    let DataBlock::VariableWidth(data) = data else {
        return Ok(None);
    };
    let compression = params.compression.as_deref();
    if compression == Some("none") {
        return Ok(Some(Box::new(VariableEncoder::default())));
    }

    let max_len = data.expect_single_stat::<UInt64Type>(Stat::MaxLength);
    let data_size = data.expect_single_stat::<UInt64Type>(Stat::DataSize);
    let per_value_requested = compression.is_some_and(|compression| compression != "fsst");
    if (max_len > 32 * 1024 || per_value_requested) && data_size >= FSST_LEAST_INPUT_SIZE as u64 {
        if compression == Some("zstd") {
            let config = CompressionConfig::new(CompressionScheme::Zstd, params.compression_level);
            return Ok(Some(Box::new(CompressedBufferEncoder::try_new(config)?)));
        }
        return Ok(Some(Box::new(CompressedBufferEncoder::default())));
    }

    if data.bits_per_offset != 32 && data.bits_per_offset != 64 {
        return Err(Error::invalid_input(format!(
            "Per-value compression does not support variable-width data with {}-bit offsets",
            data.bits_per_offset
        )));
    }
    let encoder = Box::new(VariableEncoder::default());
    let use_fsst = compression == Some("fsst")
        || (compression.is_none()
            && !matches!(field.data_type(), DataType::Binary | DataType::LargeBinary)
            && max_len >= FSST_LEAST_INPUT_MAX_LENGTH
            && data_size >= FSST_LEAST_INPUT_SIZE as u64);
    Ok(Some(if use_fsst {
        Box::new(FsstPerValueEncoder::new(encoder))
    } else {
        encoder
    }))
}

/// Select fixed-u8 RLE for block compression.
pub fn try_fixed_u8_rle_block(
    data: &DataBlock,
    params: &CompressionFieldParams,
) -> Result<Option<(Box<dyn BlockCompressor>, CompressiveEncoding)>> {
    let DataBlock::FixedWidth(data) = data else {
        return Ok(None);
    };
    try_fixed_u8_rle_for_block(data, params)
}

/// Select variable-width RLE for block compression.
pub fn try_variable_rle_block(
    data: &DataBlock,
    params: &CompressionFieldParams,
) -> Result<Option<(Box<dyn BlockCompressor>, CompressiveEncoding)>> {
    let DataBlock::FixedWidth(data) = data else {
        return Ok(None);
    };
    try_variable_rle_for_block(data, params)
}

/// Select block bitpacking for applicable fixed-width values.
pub fn try_bitpacking_block(
    data: &DataBlock,
) -> Option<(Box<dyn BlockCompressor>, CompressiveEncoding)> {
    let DataBlock::FixedWidth(data) = data else {
        return None;
    };
    try_bitpack_for_block(data)
}

/// Select explicitly requested or automatic general-purpose block compression.
pub fn try_general_block(
    data: &DataBlock,
    params: &CompressionFieldParams,
) -> Result<Option<(Box<dyn BlockCompressor>, CompressiveEncoding)>> {
    let Some((compressor, config)) = try_general_compression(params, data)? else {
        return Ok(None);
    };
    let inner = match data {
        DataBlock::FixedWidth(data) => ProtobufUtils21::flat(data.bits_per_value, None),
        DataBlock::VariableWidth(data) => ProtobufUtils21::variable(
            ProtobufUtils21::flat(data.bits_per_offset as u64, None),
            None,
        ),
        _ => return Ok(None),
    };
    Ok(Some((compressor, ProtobufUtils21::wrapped(config, inner)?)))
}

/// Store fixed- and variable-width block values without block compression.
pub fn try_raw_block(data: &DataBlock) -> Option<(Box<dyn BlockCompressor>, CompressiveEncoding)> {
    match data {
        DataBlock::FixedWidth(data) => Some((
            Box::new(ValueEncoder::default()) as Box<dyn BlockCompressor>,
            ProtobufUtils21::flat(data.bits_per_value, None),
        )),
        DataBlock::VariableWidth(data) => Some((
            Box::new(VariableEncoder::default()) as Box<dyn BlockCompressor>,
            ProtobufUtils21::variable(
                ProtobufUtils21::flat(data.bits_per_offset as u64, None),
                None,
            ),
        )),
        _ => None,
    }
}

pub trait MiniBlockDecompressor: std::fmt::Debug + Send + Sync {
    fn decompress(&self, data: Vec<LanceBuffer>, num_values: u64) -> Result<DataBlock>;

    /// Returns the exact aggregate decoded size when it is determined solely by the value count.
    ///
    /// Implementations should only return `Some` when this aggregate estimate can be used by
    /// [`DataBlockBuilder`](crate::data::DataBlockBuilder) to preallocate the decoded output
    /// exactly. Outputs with multiple buffers or whose layout-dependent allocation cannot be
    /// represented by one aggregate estimate should return `None`.
    fn decoded_size_bytes(&self, _num_values: u64) -> Option<u64> {
        None
    }
}

pub trait FixedPerValueDecompressor: std::fmt::Debug + Send + Sync {
    /// Decompress one or more values
    fn decompress(&self, data: FixedWidthDataBlock, num_values: u64) -> Result<DataBlock>;
    /// The number of bits in each value
    ///
    /// Currently (and probably long term) this must be a multiple of 8
    fn bits_per_value(&self) -> u64;

    /// Returns the exact aggregate decoded size when it is determined solely by the value count.
    ///
    /// Implementations should only return `Some` when this aggregate estimate can be used by
    /// [`DataBlockBuilder`](crate::data::DataBlockBuilder) to preallocate the decoded output
    /// exactly. Outputs with multiple buffers or whose layout-dependent allocation cannot be
    /// represented by one aggregate estimate should return `None`.
    fn decoded_size_bytes(&self, _num_values: u64) -> Option<u64> {
        None
    }
}

pub trait VariablePerValueDecompressor: std::fmt::Debug + Send + Sync {
    /// Decompress one or more values
    fn decompress(&self, data: VariableWidthBlock) -> Result<DataBlock>;
}

pub trait BlockDecompressor: std::fmt::Debug + Send + Sync {
    fn decompress(&self, data: LanceBuffer, num_values: u64) -> Result<DataBlock>;
}

pub trait DecompressionStrategy: std::fmt::Debug + Send + Sync {
    fn create_miniblock_decompressor(
        &self,
        description: &CompressiveEncoding,
        decompression_strategy: &dyn DecompressionStrategy,
    ) -> Result<Box<dyn MiniBlockDecompressor>>;

    fn create_fixed_per_value_decompressor(
        &self,
        description: &CompressiveEncoding,
    ) -> Result<Box<dyn FixedPerValueDecompressor>>;

    fn create_variable_per_value_decompressor(
        &self,
        description: &CompressiveEncoding,
    ) -> Result<Box<dyn VariablePerValueDecompressor>>;

    fn create_block_decompressor(
        &self,
        description: &CompressiveEncoding,
    ) -> Result<Box<dyn BlockDecompressor>>;
}

#[derive(Debug, Default)]
pub struct DefaultDecompressionStrategy {}

impl DecompressionStrategy for DefaultDecompressionStrategy {
    fn create_miniblock_decompressor(
        &self,
        description: &CompressiveEncoding,
        decompression_strategy: &dyn DecompressionStrategy,
    ) -> Result<Box<dyn MiniBlockDecompressor>> {
        match description.compression.as_ref().unwrap() {
            Compression::Flat(flat) => Ok(Box::new(ValueDecompressor::from_flat(flat))),
            #[cfg(feature = "bitpacking")]
            Compression::InlineBitpacking(description) => {
                Ok(Box::new(InlineBitpacking::from_description(description)))
            }
            #[cfg(not(feature = "bitpacking"))]
            Compression::InlineBitpacking(_) => Err(Error::not_supported_source(
                "this runtime was not built with bitpacking support".into(),
            )),
            Compression::Variable(variable) => {
                let Compression::Flat(offsets) = variable
                    .offsets
                    .as_ref()
                    .unwrap()
                    .compression
                    .as_ref()
                    .unwrap()
                else {
                    panic!("Variable compression only supports flat offsets")
                };
                Ok(Box::new(BinaryMiniBlockDecompressor::new(
                    offsets.bits_per_value as u8,
                )))
            }
            Compression::Fsst(description) => {
                let inner_decompressor = decompression_strategy.create_miniblock_decompressor(
                    description.values.as_ref().unwrap(),
                    decompression_strategy,
                )?;
                Ok(Box::new(FsstMiniBlockDecompressor::new(
                    description,
                    inner_decompressor,
                )))
            }
            Compression::PackedStruct(description) => Ok(Box::new(
                PackedStructFixedWidthMiniBlockDecompressor::new(description),
            )),
            Compression::VariablePackedStruct(_) => Err(Error::not_supported_source(
                "variable packed struct decoding is not yet implemented".into(),
            )),
            Compression::FixedSizeList(fsl) => {
                // In the future, we might need to do something more complex here if FSL supports
                // compression.
                Ok(Box::new(ValueDecompressor::from_fsl(fsl)))
            }
            Compression::Rle(rle) => Ok(Box::new(create_rle_decompressor(
                rle,
                decompression_strategy,
            )?)),
            Compression::ByteStreamSplit(bss) => {
                let Compression::Flat(values) =
                    bss.values.as_ref().unwrap().compression.as_ref().unwrap()
                else {
                    panic!("ByteStreamSplit compression only supports flat values")
                };
                Ok(Box::new(ByteStreamSplitDecompressor::new(
                    values.bits_per_value as usize,
                )))
            }
            Compression::General(general) => {
                // Create inner decompressor
                let inner_decompressor = self.create_miniblock_decompressor(
                    general.values.as_ref().ok_or_else(|| {
                        Error::invalid_input("GeneralMiniBlock missing inner encoding")
                    })?,
                    decompression_strategy,
                )?;

                // Parse compression config
                let compression = general.compression.as_ref().ok_or_else(|| {
                    Error::invalid_input("GeneralMiniBlock missing compression config")
                })?;

                let scheme = compression.scheme().try_into()?;

                let compression_config = CompressionConfig::new(scheme, compression.level);

                Ok(Box::new(GeneralMiniBlockDecompressor::new(
                    inner_decompressor,
                    compression_config,
                )))
            }
            _ => todo!(),
        }
    }

    fn create_fixed_per_value_decompressor(
        &self,
        description: &CompressiveEncoding,
    ) -> Result<Box<dyn FixedPerValueDecompressor>> {
        match description.compression.as_ref().unwrap() {
            Compression::Constant(constant) => Ok(Box::new(ConstantDecompressor::new(
                constant
                    .value
                    .as_ref()
                    .map(|v| LanceBuffer::from_bytes(v.clone(), 1)),
            ))),
            Compression::Flat(flat) => Ok(Box::new(ValueDecompressor::from_flat(flat))),
            Compression::FixedSizeList(fsl) => Ok(Box::new(ValueDecompressor::from_fsl(fsl))),
            _ => todo!("fixed-per-value decompressor for {:?}", description),
        }
    }

    fn create_variable_per_value_decompressor(
        &self,
        description: &CompressiveEncoding,
    ) -> Result<Box<dyn VariablePerValueDecompressor>> {
        match description.compression.as_ref().unwrap() {
            Compression::Variable(variable) => {
                let Compression::Flat(offsets) = variable
                    .offsets
                    .as_ref()
                    .unwrap()
                    .compression
                    .as_ref()
                    .unwrap()
                else {
                    panic!("Variable compression only supports flat offsets")
                };
                assert!(offsets.bits_per_value < u8::MAX as u64);
                Ok(Box::new(VariableDecoder::default()))
            }
            Compression::Fsst(fsst) => Ok(Box::new(FsstPerValueDecompressor::new(
                LanceBuffer::from_bytes(fsst.symbol_table.clone(), 1),
                Box::new(VariableDecoder::default()),
            ))),
            Compression::General(general) => Ok(Box::new(CompressedBufferEncoder::from_scheme(
                general.compression.as_ref().expect_ok()?.scheme(),
            )?)),
            Compression::VariablePackedStruct(description) => {
                let mut fields = Vec::with_capacity(description.fields.len());
                for field in &description.fields {
                    let value_encoding = field.value.as_ref().ok_or_else(|| {
                        Error::invalid_input("VariablePackedStruct field is missing value encoding")
                    })?;
                    let decoder = match field.layout.as_ref().ok_or_else(|| {
                        Error::invalid_input("VariablePackedStruct field is missing layout details")
                    })? {
                        crate::format::pb21::variable_packed_struct::field_encoding::Layout::BitsPerValue(
                            bits_per_value,
                        ) => {
                            let decompressor =
                                self.create_fixed_per_value_decompressor(value_encoding)?;
                            VariablePackedStructFieldDecoder {
                                kind: VariablePackedStructFieldKind::Fixed {
                                    bits_per_value: *bits_per_value,
                                    decompressor: Arc::from(decompressor),
                                },
                            }
                        }
                        crate::format::pb21::variable_packed_struct::field_encoding::Layout::BitsPerLength(
                            bits_per_length,
                        ) => {
                            let decompressor =
                                self.create_variable_per_value_decompressor(value_encoding)?;
                            VariablePackedStructFieldDecoder {
                                kind: VariablePackedStructFieldKind::Variable {
                                    bits_per_length: *bits_per_length,
                                    decompressor: Arc::from(decompressor),
                                },
                            }
                        }
                    };
                    fields.push(decoder);
                }
                Ok(Box::new(PackedStructVariablePerValueDecompressor::new(
                    fields,
                )))
            }
            _ => todo!("variable-per-value decompressor for {:?}", description),
        }
    }

    fn create_block_decompressor(
        &self,
        description: &CompressiveEncoding,
    ) -> Result<Box<dyn BlockDecompressor>> {
        match description.compression.as_ref().unwrap() {
            Compression::InlineBitpacking(inline_bitpacking) => Ok(Box::new(
                InlineBitpacking::from_description(inline_bitpacking),
            )),
            Compression::Flat(flat) => Ok(Box::new(ValueDecompressor::from_flat(flat))),
            Compression::Constant(constant) => {
                let scalar = constant
                    .value
                    .as_ref()
                    .map(|v| LanceBuffer::from_bytes(v.clone(), 1));
                Ok(Box::new(ConstantDecompressor::new(scalar)))
            }
            Compression::Variable(_) => Ok(Box::new(BinaryBlockDecompressor::default())),
            Compression::FixedSizeList(fsl) => {
                Ok(Box::new(ValueDecompressor::from_fsl(fsl.as_ref())))
            }
            Compression::OutOfLineBitpacking(out_of_line) => {
                // Extract the compressed bit width from the values encoding
                let compressed_bit_width = match out_of_line
                    .values
                    .as_ref()
                    .unwrap()
                    .compression
                    .as_ref()
                    .unwrap()
                {
                    Compression::Flat(flat) => flat.bits_per_value,
                    _ => {
                        return Err(Error::invalid_input_source(
                            "OutOfLineBitpacking values must use Flat encoding".into(),
                        ));
                    }
                };
                Ok(Box::new(OutOfLineBitpacking::new(
                    compressed_bit_width,
                    out_of_line.uncompressed_bits_per_value,
                )))
            }
            Compression::General(general) => {
                let inner_desc = general
                    .values
                    .as_ref()
                    .ok_or_else(|| {
                        Error::invalid_input("General compression missing inner encoding")
                    })?
                    .as_ref();
                let inner_decompressor = self.create_block_decompressor(inner_desc)?;

                let compression = general.compression.as_ref().ok_or_else(|| {
                    Error::invalid_input("General compression missing compression config")
                })?;
                let scheme = compression.scheme().try_into()?;
                let config = CompressionConfig::new(scheme, compression.level);
                let general_decompressor =
                    GeneralBlockDecompressor::try_new(inner_decompressor, config)?;

                Ok(Box::new(general_decompressor))
            }
            Compression::Rle(rle) => Ok(Box::new(create_rle_decompressor(rle, self)?)),
            _ => todo!(),
        }
    }
}
pub(crate) fn create_rle_decompressor(
    rle: &crate::format::pb21::Rle,
    decompression_strategy: &dyn DecompressionStrategy,
) -> Result<RleDecompressor> {
    let values = rle
        .values
        .as_ref()
        .ok_or_else(|| Error::invalid_input("RLE compression missing values encoding"))?;
    let run_lengths = rle
        .run_lengths
        .as_ref()
        .ok_or_else(|| Error::invalid_input("RLE compression missing run lengths encoding"))?;

    let values = create_rle_child_decompressor(values, "values", decompression_strategy)?;
    let run_lengths =
        create_rle_child_decompressor(run_lengths, "run lengths", decompression_strategy)?;

    if !matches!(values.bits_per_value(), 8 | 16 | 32 | 64) {
        return Err(Error::invalid_input(format!(
            "RLE compression only supports 8, 16, 32, or 64-bit values, got {}",
            values.bits_per_value()
        )));
    }

    let run_length_width =
        RunLengthWidth::from_bits(run_lengths.bits_per_value()).ok_or_else(|| {
            Error::invalid_input(format!(
                "RLE compression only supports 8, 16, or 32-bit run lengths, got {}",
                run_lengths.bits_per_value()
            ))
        })?;

    if values.requires_num_values() && run_lengths.requires_num_values() {
        return Err(Error::invalid_input(
            "RLE values and run lengths child encodings cannot both require the run count",
        ));
    }

    if values.is_identity() && run_lengths.is_identity() {
        return Ok(RleDecompressor::with_run_length_width(
            values.bits_per_value(),
            run_length_width,
        ));
    }

    Ok(RleDecompressor::with_child_decompressors(
        values.bits_per_value(),
        run_length_width,
        values,
        run_lengths,
    ))
}

fn create_rle_child_decompressor(
    encoding: &CompressiveEncoding,
    role: &str,
    decompression_strategy: &dyn DecompressionStrategy,
) -> Result<RleChildDecompressor> {
    let compression = encoding
        .compression
        .as_ref()
        .ok_or_else(|| Error::invalid_input(format!("RLE {role} missing child compression")))?;
    let (bits_per_value, requires_num_values, needs_decompressor) =
        validate_rle_child_compression(compression, role)?;

    if needs_decompressor {
        Ok(RleChildDecompressor::block(
            bits_per_value,
            decompression_strategy.create_block_decompressor(encoding)?,
            requires_num_values,
        ))
    } else {
        Ok(RleChildDecompressor::flat(bits_per_value))
    }
}

fn validate_rle_child_compression(
    compression: &Compression,
    role: &str,
) -> Result<(u64, bool, bool)> {
    match compression {
        Compression::Flat(flat) => Ok((flat.bits_per_value, false, false)),
        Compression::General(general) => {
            general.compression.as_ref().ok_or_else(|| {
                Error::invalid_input(format!(
                    "RLE {role} general child missing compression config"
                ))
            })?;
            let values = general.values.as_ref().ok_or_else(|| {
                Error::invalid_input(format!("RLE {role} general child missing inner encoding"))
            })?;
            let inner = values.compression.as_ref().ok_or_else(|| {
                Error::invalid_input(format!(
                    "RLE {role} general child missing inner compression"
                ))
            })?;
            let (bits_per_value, requires_num_values) =
                validate_rle_block_child_inner(inner, role)?;
            Ok((bits_per_value, requires_num_values, true))
        }
        Compression::OutOfLineBitpacking(out_of_line) => {
            let values = out_of_line.values.as_ref().ok_or_else(|| {
                Error::invalid_input(format!(
                    "RLE {role} bitpacking child missing values encoding"
                ))
            })?;
            let Compression::Flat(_) = values.compression.as_ref().ok_or_else(|| {
                Error::invalid_input(format!(
                    "RLE {role} bitpacking child missing values compression"
                ))
            })?
            else {
                return Err(Error::invalid_input(format!(
                    "RLE {role} bitpacking child only supports flat values"
                )));
            };
            Ok((out_of_line.uncompressed_bits_per_value, true, true))
        }
        other => Err(Error::invalid_input(format!(
            "RLE {role} only supports flat, general, or out-of-line bitpacking child encodings, got {}",
            compression_name(other)
        ))),
    }
}

fn validate_rle_block_child_inner(compression: &Compression, role: &str) -> Result<(u64, bool)> {
    match compression {
        Compression::Flat(flat) => Ok((flat.bits_per_value, false)),
        Compression::OutOfLineBitpacking(out_of_line) => {
            let values = out_of_line.values.as_ref().ok_or_else(|| {
                Error::invalid_input(format!(
                    "RLE {role} bitpacking child missing values encoding"
                ))
            })?;
            let Compression::Flat(_) = values.compression.as_ref().ok_or_else(|| {
                Error::invalid_input(format!(
                    "RLE {role} bitpacking child missing values compression"
                ))
            })?
            else {
                return Err(Error::invalid_input(format!(
                    "RLE {role} bitpacking child only supports flat values"
                )));
            };
            Ok((out_of_line.uncompressed_bits_per_value, true))
        }
        other => Err(Error::invalid_input(format!(
            "RLE {role} general child only supports flat or out-of-line bitpacking inner encodings, got {}",
            compression_name(other)
        ))),
    }
}

fn compression_name(compression: &Compression) -> &'static str {
    match compression {
        Compression::Flat(_) => "flat",
        Compression::Variable(_) => "variable",
        Compression::Fsst(_) => "fsst",
        Compression::OutOfLineBitpacking(_) => "out-of-line bitpacking",
        Compression::InlineBitpacking(_) => "inline bitpacking",
        Compression::General(_) => "general",
        Compression::Constant(_) => "constant",
        Compression::Dictionary(_) => "dictionary",
        Compression::ByteStreamSplit(_) => "byte stream split",
        Compression::PackedStruct(_) => "packed struct",
        Compression::FixedSizeList(_) => "fixed-size list",
        Compression::VariablePackedStruct(_) => "variable packed struct",
        Compression::Rle(_) => "rle",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::LanceBuffer;
    use crate::compression_config::CompressionParams;
    use crate::data::{BlockInfo, DataBlock, FixedWidthDataBlock};
    use crate::encodings::logical::primitive::miniblock::MiniBlockCompressionContext;
    use crate::statistics::ComputeStat;
    use crate::testing::{TestEncoding, extract_array_encoding_chain, test_compression_strategy};
    use arrow_schema::{DataType, Field as ArrowField};
    use std::collections::HashMap;

    fn strategy(encoding: TestEncoding, params: CompressionParams) -> Arc<dyn CompressionStrategy> {
        test_compression_strategy(encoding, params)
    }

    fn baseline_strategy(params: CompressionParams) -> Arc<dyn CompressionStrategy> {
        strategy(TestEncoding::StructuralU16, params)
    }

    fn miniblock_context() -> MiniBlockCompressionContext {
        MiniBlockCompressionContext::new(0, true, true)
    }

    fn create_test_field(name: &str, data_type: DataType) -> Field {
        let arrow_field = ArrowField::new(name, data_type, true);
        let mut field = Field::try_from(&arrow_field).unwrap();
        field.id = -1;
        field
    }

    fn create_fixed_width_block_with_stats(
        bits_per_value: u64,
        num_values: u64,
        run_count: u64,
    ) -> DataBlock {
        // Create varied data to avoid low entropy
        let bytes_per_value = (bits_per_value / 8) as usize;
        let total_bytes = bytes_per_value * num_values as usize;
        let mut data = vec![0u8; total_bytes];

        // Create data with specified run count
        let values_per_run = (num_values / run_count).max(1);
        let mut run_value = 0u8;

        for i in 0..num_values as usize {
            if i % values_per_run as usize == 0 {
                run_value = run_value.wrapping_add(17); // Use prime to get varied values
            }
            // Fill all bytes of the value to create high entropy
            for j in 0..bytes_per_value {
                let byte_offset = i * bytes_per_value + j;
                if byte_offset < data.len() {
                    data[byte_offset] = run_value.wrapping_add(j as u8);
                }
            }
        }

        let mut block = FixedWidthDataBlock {
            bits_per_value,
            data: LanceBuffer::reinterpret_vec(data),
            num_values,
            block_info: BlockInfo::default(),
        };

        // Compute all statistics including BytePositionEntropy
        use crate::statistics::ComputeStat;
        block.compute_stat();

        DataBlock::FixedWidth(block)
    }

    fn create_fixed_width_block(bits_per_value: u64, num_values: u64) -> DataBlock {
        // Create data with some variety to avoid always triggering BSS
        let bytes_per_value = (bits_per_value / 8) as usize;
        let total_bytes = bytes_per_value * num_values as usize;
        let mut data = vec![0u8; total_bytes];

        // Add some variation to the data to make it more realistic
        for i in 0..num_values as usize {
            let byte_offset = i * bytes_per_value;
            if byte_offset < data.len() {
                data[byte_offset] = (i % 256) as u8;
            }
        }

        let mut block = FixedWidthDataBlock {
            bits_per_value,
            data: LanceBuffer::reinterpret_vec(data),
            num_values,
            block_info: BlockInfo::default(),
        };

        // Compute all statistics including BytePositionEntropy
        use crate::statistics::ComputeStat;
        block.compute_stat();

        DataBlock::FixedWidth(block)
    }

    fn rle_run_length_bits(encoding: &CompressiveEncoding) -> u64 {
        let Compression::Rle(rle) = encoding.compression.as_ref().unwrap() else {
            panic!("expected RLE encoding");
        };
        let Compression::Flat(run_lengths) = rle
            .run_lengths
            .as_ref()
            .unwrap()
            .compression
            .as_ref()
            .unwrap()
        else {
            panic!("expected flat run lengths");
        };
        run_lengths.bits_per_value
    }

    fn expect_rle_encoding(encoding: &CompressiveEncoding) -> &crate::format::pb21::Rle {
        match encoding.compression.as_ref().unwrap() {
            Compression::Rle(rle) => rle,
            Compression::General(general) => {
                let inner = general.values.as_ref().unwrap();
                let Compression::Rle(rle) = inner.compression.as_ref().unwrap() else {
                    panic!("expected wrapped RLE encoding");
                };
                rle
            }
            other => panic!("expected RLE encoding, got {}", compression_name(other)),
        }
    }

    fn create_variable_width_block(
        bits_per_offset: u8,
        num_values: u64,
        avg_value_size: usize,
    ) -> DataBlock {
        use crate::statistics::ComputeStat;

        // Create offsets buffer (num_values + 1 offsets)
        let mut offsets = Vec::with_capacity((num_values + 1) as usize);
        let mut current_offset = 0i64;
        offsets.push(current_offset);

        // Generate offsets with varying value sizes
        for i in 0..num_values {
            let value_size = if avg_value_size == 0 {
                1
            } else {
                ((avg_value_size as i64 + (i as i64 % 8) - 4).max(1) as usize)
                    .min(avg_value_size * 2)
            };
            current_offset += value_size as i64;
            offsets.push(current_offset);
        }

        // Create data buffer with realistic content
        let total_data_size = current_offset as usize;
        let mut data = vec![0u8; total_data_size];

        // Fill data with varied content
        for i in 0..num_values {
            let start_offset = offsets[i as usize] as usize;
            let end_offset = offsets[(i + 1) as usize] as usize;

            let content = (i % 256) as u8;
            for j in 0..end_offset - start_offset {
                data[start_offset + j] = content.wrapping_add(j as u8);
            }
        }

        // Convert offsets to appropriate lance buffer
        let offsets_buffer = match bits_per_offset {
            32 => {
                let offsets_32: Vec<i32> = offsets.iter().map(|&o| o as i32).collect();
                LanceBuffer::reinterpret_vec(offsets_32)
            }
            64 => LanceBuffer::reinterpret_vec(offsets),
            _ => panic!("Unsupported bits_per_offset: {}", bits_per_offset),
        };

        let mut block = VariableWidthBlock {
            data: LanceBuffer::from(data),
            offsets: offsets_buffer,
            bits_per_offset,
            num_values,
            block_info: BlockInfo::default(),
        };

        block.compute_stat();
        DataBlock::VariableWidth(block)
    }

    fn create_fsst_candidate_variable_width_block() -> DataBlock {
        create_variable_width_block(32, 4096, FSST_LEAST_INPUT_MAX_LENGTH as usize + 16)
    }

    #[test]
    fn test_parameter_based_compression() {
        let mut params = CompressionParams::new();

        // Configure RLE for ID columns with BSS explicitly disabled
        params.columns.insert(
            "*_id".to_string(),
            CompressionFieldParams {
                rle_threshold: Some(0.3),
                compression: Some("lz4".to_string()),
                compression_level: None,
                bss: Some(BssMode::Off), // Explicitly disable BSS to test RLE
                minichunk_size: None,
            },
        );

        let strategy = baseline_strategy(params);
        let field = create_test_field("user_id", DataType::Int32);

        // Create data with low run count for RLE
        // Use create_fixed_width_block_with_stats which properly sets run count
        let data = create_fixed_width_block_with_stats(32, 1000, 100); // 100 runs out of 1000 values

        let compressor = strategy.create_miniblock_compressor(&field, &data).unwrap();
        // Should use RLE due to low threshold (0.3) and low run count (100/1000 = 0.1)
        let debug_str = format!("{:?}", compressor);

        // The compressor should be RLE wrapped in general compression
        assert!(debug_str.contains("GeneralMiniBlockCompressor"));
        assert!(debug_str.contains("RleEncoder"));
    }

    #[test]
    fn test_type_level_parameters() {
        let mut params = CompressionParams::new();

        // Configure all Int32 to use specific settings
        params.types.insert(
            "Int32".to_string(),
            CompressionFieldParams {
                rle_threshold: Some(0.1), // Very low threshold
                compression: Some("zstd".to_string()),
                compression_level: Some(3),
                bss: Some(BssMode::Off), // Disable BSS to test RLE
                minichunk_size: None,
            },
        );

        let strategy = baseline_strategy(params);
        let field = create_test_field("some_column", DataType::Int32);
        // Create data with very low run count (50 runs for 1000 values = 0.05 ratio)
        let data = create_fixed_width_block_with_stats(32, 1000, 50);

        let compressor = strategy.create_miniblock_compressor(&field, &data).unwrap();
        // Should use RLE due to very low threshold
        assert!(format!("{:?}", compressor).contains("RleEncoder"));
    }

    // Regression for #6626: an all-zero stat segment (e.g. rep/def for a long
    // run of empty lists) used to disable block bitpacking entirely.
    #[test]
    #[cfg(feature = "bitpacking")]
    fn test_block_bitpacks_with_zero_segment() {
        let strategy = baseline_strategy(CompressionParams::default());
        let field = create_test_field("levels", DataType::UInt16);

        // First 1024 zeros, then 1024 ones; max bit width is 1.
        let mut values: Vec<u16> = vec![0; 1024];
        values.extend(std::iter::repeat_n(1u16, 1024));
        let mut block = FixedWidthDataBlock {
            bits_per_value: 16,
            data: LanceBuffer::reinterpret_vec(values),
            num_values: 2048,
            block_info: BlockInfo::default(),
        };
        block.compute_stat();
        let data = DataBlock::FixedWidth(block);

        let (compressor, _encoding) = strategy.create_block_compressor(&field, &data).unwrap();
        let debug_str = format!("{:?}", compressor);
        assert!(
            debug_str.contains("OutOfLineBitpacking"),
            "expected OutOfLineBitpacking, got: {debug_str}"
        );
    }

    #[test]
    fn test_rle_block_accounts_for_header_before_selecting() {
        let strategy = strategy(TestEncoding::StructuralSparse, CompressionParams::default());
        let field = create_test_field("small_constant", DataType::Int32);
        let values = vec![42i32; 2];
        let mut block = FixedWidthDataBlock {
            bits_per_value: 32,
            data: LanceBuffer::reinterpret_vec(values),
            num_values: 2,
            block_info: BlockInfo::default(),
        };
        block.compute_stat();
        let data = DataBlock::FixedWidth(block);

        let (compressor, encoding) = strategy.create_block_compressor(&field, &data).unwrap();

        assert!(format!("{compressor:?}").contains("ValueEncoder"));
        assert!(matches!(
            encoding.compression.as_ref(),
            Some(Compression::Flat(_))
        ));
    }

    #[test]
    #[cfg(feature = "bitpacking")]
    fn test_rle_block_prefers_bitpacking_when_smaller() {
        let strategy = strategy(TestEncoding::StructuralSparse, CompressionParams::default());
        let field = create_test_field("levels", DataType::UInt16);

        let mut values = Vec::with_capacity(2048);
        for run_idx in 0..1024 {
            values.extend(std::iter::repeat_n((run_idx % 2) as u16, 2));
        }
        let mut block = FixedWidthDataBlock {
            bits_per_value: 16,
            data: LanceBuffer::reinterpret_vec(values),
            num_values: 2048,
            block_info: BlockInfo::default(),
        };
        block.compute_stat();
        let data = DataBlock::FixedWidth(block);

        let (compressor, encoding) = strategy.create_block_compressor(&field, &data).unwrap();
        let debug_str = format!("{compressor:?}");
        assert!(
            debug_str.contains("OutOfLineBitpacking"),
            "expected OutOfLineBitpacking, got: {debug_str}"
        );
        assert!(matches!(
            encoding.compression.as_ref(),
            Some(Compression::OutOfLineBitpacking(_))
        ));
    }

    #[test]
    #[cfg(feature = "bitpacking")]
    fn test_low_cardinality_prefers_bitpacking_over_rle() {
        let strategy = baseline_strategy(CompressionParams::default());
        let field = create_test_field("int_score", DataType::Int64);

        // Low cardinality values (3/4/5) but with moderate run count:
        // RLE compresses vs raw, yet bitpacking should be smaller.
        let mut values: Vec<u64> = Vec::with_capacity(256);
        for run_idx in 0..64 {
            let value = match run_idx % 3 {
                0 => 3u64,
                1 => 4u64,
                _ => 5u64,
            };
            values.extend(std::iter::repeat_n(value, 4));
        }

        let mut block = FixedWidthDataBlock {
            bits_per_value: 64,
            data: LanceBuffer::reinterpret_vec(values),
            num_values: 256,
            block_info: BlockInfo::default(),
        };

        use crate::statistics::ComputeStat;
        block.compute_stat();

        let data = DataBlock::FixedWidth(block);
        let compressor = strategy.create_miniblock_compressor(&field, &data).unwrap();
        let debug_str = format!("{:?}", compressor);
        assert!(
            debug_str.contains("InlineBitpacking"),
            "expected InlineBitpacking, got: {debug_str}"
        );
        assert!(
            !debug_str.contains("RleEncoder"),
            "expected RLE to be skipped when bitpacking is smaller, got: {debug_str}"
        );
    }

    fn check_uncompressed_encoding(encoding: &CompressiveEncoding, variable: bool) {
        let chain = extract_array_encoding_chain(encoding);
        if variable {
            assert_eq!(chain.len(), 2);
            assert_eq!(chain.first().unwrap().as_str(), "variable");
            assert_eq!(chain.get(1).unwrap().as_str(), "flat");
        } else {
            assert_eq!(chain.len(), 1);
            assert_eq!(chain.first().unwrap().as_str(), "flat");
        }
    }

    #[test]
    fn test_none_compression() {
        let mut params = CompressionParams::new();

        // Disable compression for embeddings
        params.columns.insert(
            "embeddings".to_string(),
            CompressionFieldParams {
                compression: Some("none".to_string()),
                ..Default::default()
            },
        );

        let strategy = baseline_strategy(params);
        let field = create_test_field("embeddings", DataType::Float32);
        let fixed_data = create_fixed_width_block(32, 1000);
        let variable_data = create_variable_width_block(32, 10, 32 * 1024);

        // Test miniblock
        let compressor = strategy
            .create_miniblock_compressor(&field, &fixed_data)
            .unwrap();
        let (_block, encoding) = compressor
            .compress(miniblock_context(), fixed_data.clone())
            .unwrap();
        check_uncompressed_encoding(&encoding, false);
        let compressor = strategy
            .create_miniblock_compressor(&field, &variable_data)
            .unwrap();
        let (_block, encoding) = compressor
            .compress(miniblock_context(), variable_data.clone())
            .unwrap();
        check_uncompressed_encoding(&encoding, true);

        // Test pervalue
        let compressor = strategy.create_per_value(&field, &fixed_data).unwrap();
        let (_block, encoding) = compressor.compress(fixed_data).unwrap();
        check_uncompressed_encoding(&encoding, false);
        let compressor = strategy.create_per_value(&field, &variable_data).unwrap();
        let (_block, encoding) = compressor.compress(variable_data).unwrap();
        check_uncompressed_encoding(&encoding, true);
    }

    #[test]
    fn test_field_metadata_none_compression() {
        // Prepare field with metadata for none compression
        let mut arrow_field = ArrowField::new("simple_col", DataType::Binary, true);
        let mut metadata = HashMap::new();
        metadata.insert(COMPRESSION_META_KEY.to_string(), "none".to_string());
        arrow_field = arrow_field.with_metadata(metadata);
        let field = Field::try_from(&arrow_field).unwrap();

        let strategy = baseline_strategy(CompressionParams::new());

        // Test miniblock
        let fixed_data = create_fixed_width_block(32, 1000);
        let variable_data = create_variable_width_block(32, 10, 32 * 1024);

        let compressor = strategy
            .create_miniblock_compressor(&field, &fixed_data)
            .unwrap();
        let (_block, encoding) = compressor
            .compress(miniblock_context(), fixed_data.clone())
            .unwrap();
        check_uncompressed_encoding(&encoding, false);

        let compressor = strategy
            .create_miniblock_compressor(&field, &variable_data)
            .unwrap();
        let (_block, encoding) = compressor
            .compress(miniblock_context(), variable_data.clone())
            .unwrap();
        check_uncompressed_encoding(&encoding, true);

        // Test pervalue
        let compressor = strategy.create_per_value(&field, &fixed_data).unwrap();
        let (_block, encoding) = compressor.compress(fixed_data).unwrap();
        check_uncompressed_encoding(&encoding, false);

        let compressor = strategy.create_per_value(&field, &variable_data).unwrap();
        let (_block, encoding) = compressor.compress(variable_data).unwrap();
        check_uncompressed_encoding(&encoding, true);
    }

    #[test]
    fn test_auto_fsst_disabled_for_binary_fields() {
        let strategy = baseline_strategy(CompressionParams::default());
        let field = create_test_field("bytes", DataType::Binary);
        let variable_data = create_fsst_candidate_variable_width_block();

        let miniblock = strategy
            .create_miniblock_compressor(&field, &variable_data)
            .unwrap();
        let miniblock_debug = format!("{:?}", miniblock);
        assert!(
            miniblock_debug.contains("BinaryMiniBlockEncoder"),
            "expected BinaryMiniBlockEncoder, got: {miniblock_debug}"
        );
        assert!(
            !miniblock_debug.contains("FsstMiniBlockEncoder"),
            "did not expect FsstMiniBlockEncoder, got: {miniblock_debug}"
        );

        let per_value = strategy.create_per_value(&field, &variable_data).unwrap();
        let per_value_debug = format!("{:?}", per_value);
        assert!(
            per_value_debug.contains("VariableEncoder"),
            "expected VariableEncoder, got: {per_value_debug}"
        );
        assert!(
            !per_value_debug.contains("FsstPerValueEncoder"),
            "did not expect FsstPerValueEncoder, got: {per_value_debug}"
        );
    }

    #[test]
    fn test_auto_fsst_still_enabled_for_utf8_fields() {
        let strategy = baseline_strategy(CompressionParams::default());
        let field = create_test_field("text", DataType::Utf8);
        let variable_data = create_fsst_candidate_variable_width_block();

        let miniblock = strategy
            .create_miniblock_compressor(&field, &variable_data)
            .unwrap();
        let miniblock_debug = format!("{:?}", miniblock);
        assert!(
            miniblock_debug.contains("FsstMiniBlockEncoder"),
            "expected FsstMiniBlockEncoder, got: {miniblock_debug}"
        );

        let per_value = strategy.create_per_value(&field, &variable_data).unwrap();
        let per_value_debug = format!("{:?}", per_value);
        assert!(
            per_value_debug.contains("FsstPerValueEncoder"),
            "expected FsstPerValueEncoder, got: {per_value_debug}"
        );
    }

    #[test]
    fn test_explicit_fsst_still_supported_for_binary_fields() {
        let mut params = CompressionParams::new();
        params.columns.insert(
            "bytes".to_string(),
            CompressionFieldParams {
                compression: Some("fsst".to_string()),
                ..Default::default()
            },
        );

        let strategy = baseline_strategy(params);
        let field = create_test_field("bytes", DataType::Binary);
        let variable_data = create_fsst_candidate_variable_width_block();

        let miniblock = strategy
            .create_miniblock_compressor(&field, &variable_data)
            .unwrap();
        let miniblock_debug = format!("{:?}", miniblock);
        assert!(
            miniblock_debug.contains("FsstMiniBlockEncoder"),
            "expected FsstMiniBlockEncoder, got: {miniblock_debug}"
        );

        let per_value = strategy.create_per_value(&field, &variable_data).unwrap();
        let per_value_debug = format!("{:?}", per_value);
        assert!(
            per_value_debug.contains("FsstPerValueEncoder"),
            "expected FsstPerValueEncoder, got: {per_value_debug}"
        );
    }

    #[test]
    #[cfg(feature = "zstd")]
    fn test_compression_level_honored_for_large_per_value() {
        let mut params = CompressionParams::new();
        params.columns.insert(
            "html".to_string(),
            CompressionFieldParams {
                compression: Some("zstd".to_string()),
                compression_level: Some(19),
                ..Default::default()
            },
        );
        let strategy = baseline_strategy(params);
        let field = create_test_field("html", DataType::Utf8);
        let large = create_variable_width_block(32, 64, 40 * 1024);

        let per_value = strategy.create_per_value(&field, &large).unwrap();
        let debug = format!("{per_value:?}");
        assert!(
            debug.contains("ZstdBufferCompressor") && debug.contains("compression_level: 19"),
            "expected zstd level 19 to reach the per-value compressor, got: {debug}"
        );
    }

    #[test]
    fn test_parameter_merge_priority() {
        let mut params = CompressionParams::new();

        // Set type-level
        params.types.insert(
            "Int32".to_string(),
            CompressionFieldParams {
                rle_threshold: Some(0.5),
                compression: Some("lz4".to_string()),
                ..Default::default()
            },
        );

        // Set column-level (highest priority)
        params.columns.insert(
            "user_id".to_string(),
            CompressionFieldParams {
                rle_threshold: Some(0.2),
                compression: Some("zstd".to_string()),
                compression_level: Some(6),
                bss: None,
                minichunk_size: None,
            },
        );

        // Get merged params
        let merged = params.get_field_params("user_id", &DataType::Int32);

        // Column params should override type params
        assert_eq!(merged.rle_threshold, Some(0.2));
        assert_eq!(merged.compression, Some("zstd".to_string()));
        assert_eq!(merged.compression_level, Some(6));

        // Test field with only type params
        let merged = params.get_field_params("other_field", &DataType::Int32);
        assert_eq!(merged.rle_threshold, Some(0.5));
        assert_eq!(merged.compression, Some("lz4".to_string()));
        assert_eq!(merged.compression_level, None);
    }

    #[test]
    fn test_pattern_matching() {
        let mut params = CompressionParams::new();

        // Configure pattern for log files
        params.columns.insert(
            "log_*".to_string(),
            CompressionFieldParams {
                compression: Some("zstd".to_string()),
                compression_level: Some(6),
                ..Default::default()
            },
        );

        // Should match pattern
        let merged = params.get_field_params("log_messages", &DataType::Utf8);
        assert_eq!(merged.compression, Some("zstd".to_string()));
        assert_eq!(merged.compression_level, Some(6));

        // Should not match
        let merged = params.get_field_params("messages_log", &DataType::Utf8);
        assert_eq!(merged.compression, None);
    }

    #[test]
    fn test_legacy_metadata_support() {
        let params = CompressionParams::new();
        let strategy = baseline_strategy(params);

        // Test field with "none" compression metadata
        let mut metadata = HashMap::new();
        metadata.insert(COMPRESSION_META_KEY.to_string(), "none".to_string());
        let mut field = create_test_field("some_column", DataType::Int32);
        field.metadata = metadata;

        let data = create_fixed_width_block(32, 1000);
        let compressor = strategy.create_miniblock_compressor(&field, &data).unwrap();

        // Should respect metadata and use ValueEncoder
        assert!(format!("{:?}", compressor).contains("ValueEncoder"));
    }

    #[test]
    fn test_default_behavior() {
        // Empty params should fall back to default behavior
        let params = CompressionParams::new();
        let strategy = baseline_strategy(params);

        let field = create_test_field("random_column", DataType::Int32);
        // Create data with high run count that won't trigger RLE (600 runs for 1000 values = 0.6 ratio)
        let data = create_fixed_width_block_with_stats(32, 1000, 600);

        let compressor = strategy.create_miniblock_compressor(&field, &data).unwrap();
        // Should use default strategy's decision
        let debug_str = format!("{:?}", compressor);
        assert!(debug_str.contains("ValueEncoder") || debug_str.contains("InlineBitpacking"));
    }

    #[test]
    fn test_field_metadata_compression() {
        let params = CompressionParams::new();
        let strategy = baseline_strategy(params);

        // Test field with compression metadata
        let mut metadata = HashMap::new();
        metadata.insert(COMPRESSION_META_KEY.to_string(), "zstd".to_string());
        metadata.insert(COMPRESSION_LEVEL_META_KEY.to_string(), "6".to_string());
        let mut field = create_test_field("test_column", DataType::Int32);
        field.metadata = metadata;

        let data = create_fixed_width_block(32, 1000);
        let compressor = strategy.create_miniblock_compressor(&field, &data).unwrap();

        // Should use zstd with level 6
        let debug_str = format!("{:?}", compressor);
        assert!(debug_str.contains("GeneralMiniBlockCompressor"));
    }

    #[test]
    fn test_field_metadata_rle_threshold() {
        let params = CompressionParams::new();
        let strategy = baseline_strategy(params);

        // Test field with RLE threshold metadata
        let mut metadata = HashMap::new();
        metadata.insert(RLE_THRESHOLD_META_KEY.to_string(), "0.8".to_string());
        metadata.insert(BSS_META_KEY.to_string(), "off".to_string()); // Disable BSS to test RLE
        let mut field = create_test_field("test_column", DataType::Int32);
        field.metadata = metadata;

        // Create data with low run count (e.g., 100 runs for 1000 values = 0.1 ratio)
        // This ensures run_count (100) < num_values * threshold (1000 * 0.8 = 800)
        let data = create_fixed_width_block_with_stats(32, 1000, 100);

        let compressor = strategy.create_miniblock_compressor(&field, &data).unwrap();

        // Should use RLE because run_count (100) < num_values * threshold (800)
        let debug_str = format!("{:?}", compressor);
        assert!(debug_str.contains("RleEncoder"));
    }

    #[test]
    fn test_rle_v2_miniblock_selects_u16_run_lengths() {
        let mut metadata = HashMap::new();
        metadata.insert(RLE_THRESHOLD_META_KEY.to_string(), "1.0".to_string());
        metadata.insert(BSS_META_KEY.to_string(), "off".to_string());
        let mut field = create_test_field("test_column", DataType::Int32);
        field.metadata = metadata;

        let values = vec![7i32; 1000];
        let mut data = FixedWidthDataBlock {
            bits_per_value: 32,
            data: LanceBuffer::reinterpret_vec(values),
            num_values: 1000,
            block_info: BlockInfo::default(),
        };
        data.compute_stat();
        let data = DataBlock::FixedWidth(data);

        let strategy = strategy(TestEncoding::StructuralSparse, CompressionParams::default());
        let compressor = strategy.create_miniblock_compressor(&field, &data).unwrap();
        let (_compressed, encoding) = compressor.compress(miniblock_context(), data).unwrap();
        assert_eq!(rle_run_length_bits(&encoding), 16);
    }

    #[test]
    fn test_rle_v2_miniblock_keeps_u8_run_lengths_before_v2_3() {
        for version in [TestEncoding::StructuralU16, TestEncoding::StructuralU32] {
            let mut metadata = HashMap::new();
            metadata.insert(RLE_THRESHOLD_META_KEY.to_string(), "1.0".to_string());
            metadata.insert(BSS_META_KEY.to_string(), "off".to_string());
            let mut field = create_test_field("test_column", DataType::Int32);
            field.metadata = metadata;

            let values = vec![7i32; 1000];
            let mut data = FixedWidthDataBlock {
                bits_per_value: 32,
                data: LanceBuffer::reinterpret_vec(values),
                num_values: 1000,
                block_info: BlockInfo::default(),
            };
            data.compute_stat();
            let data = DataBlock::FixedWidth(data);

            let strategy = strategy(version, CompressionParams::default());
            let compressor = strategy.create_miniblock_compressor(&field, &data).unwrap();
            let (_compressed, encoding) = compressor.compress(miniblock_context(), data).unwrap();
            assert_eq!(rle_run_length_bits(&encoding), 8, "version={version}");
        }
    }

    #[test]
    fn test_rle_v2_uses_selected_width_cost_before_bitpacking() {
        let mut metadata = HashMap::new();
        metadata.insert(RLE_THRESHOLD_META_KEY.to_string(), "1.0".to_string());
        metadata.insert(BSS_META_KEY.to_string(), "off".to_string());
        let mut field = create_test_field("test_column", DataType::Int32);
        field.metadata = metadata;

        let values = vec![0i32; 4096];
        let mut data = FixedWidthDataBlock {
            bits_per_value: 32,
            data: LanceBuffer::reinterpret_vec(values),
            num_values: 4096,
            block_info: BlockInfo::default(),
        };
        data.compute_stat();
        let data = DataBlock::FixedWidth(data);

        let strategy = strategy(TestEncoding::StructuralSparse, CompressionParams::default());
        let compressor = strategy.create_miniblock_compressor(&field, &data).unwrap();
        let debug_str = format!("{compressor:?}");
        assert!(debug_str.contains("RleEncoder"));

        let (_compressed, encoding) = compressor.compress(miniblock_context(), data).unwrap();
        assert_eq!(rle_run_length_bits(&encoding), 16);
    }

    #[test]
    fn test_rle_v2_sorted_dictionary_indices_select_u16_run_lengths() {
        let field = create_test_field("dict_indices", DataType::Int32);

        let mut values = Vec::with_capacity(1_200);
        for value in 0..4 {
            values.extend(std::iter::repeat_n(value, 300));
        }
        let mut data = FixedWidthDataBlock {
            bits_per_value: 32,
            data: LanceBuffer::reinterpret_vec(values),
            num_values: 1_200,
            block_info: BlockInfo::default(),
        };
        data.compute_stat();
        let data = DataBlock::FixedWidth(data);

        let strategy = strategy(TestEncoding::StructuralSparse, CompressionParams::default());
        let compressor = strategy.create_miniblock_compressor(&field, &data).unwrap();
        let (_compressed, encoding) = compressor.compress(miniblock_context(), data).unwrap();
        assert_eq!(rle_run_length_bits(&encoding), 16);
    }

    #[test]
    fn test_rle_v2_short_runs_keep_u8_run_lengths() {
        let field = create_test_field("dict_indices", DataType::Int32);

        let mut values = Vec::with_capacity(1_280);
        for value in 0..10 {
            values.extend(std::iter::repeat_n(value, 128));
        }
        let mut data = FixedWidthDataBlock {
            bits_per_value: 32,
            data: LanceBuffer::reinterpret_vec(values),
            num_values: 1_280,
            block_info: BlockInfo::default(),
        };
        data.compute_stat();
        let data = DataBlock::FixedWidth(data);

        let strategy = strategy(TestEncoding::StructuralSparse, CompressionParams::default());
        let compressor = strategy.create_miniblock_compressor(&field, &data).unwrap();
        let (_compressed, encoding) = compressor.compress(miniblock_context(), data).unwrap();
        assert_eq!(rle_run_length_bits(&encoding), 8);
    }

    #[test]
    #[cfg(any(feature = "lz4", feature = "zstd"))]
    fn test_rle_miniblock_released_versions_keep_flat_children_when_compression_requested() {
        for version in [TestEncoding::StructuralU16, TestEncoding::StructuralU32] {
            let mut params = CompressionParams::new();
            params.columns.insert(
                "dict_indices".to_string(),
                CompressionFieldParams {
                    compression: Some(
                        if cfg!(feature = "lz4") { "lz4" } else { "zstd" }.to_string(),
                    ),
                    rle_threshold: Some(1.0),
                    bss: Some(BssMode::Off),
                    ..Default::default()
                },
            );
            let strategy = strategy(version, params);
            let field = create_test_field("dict_indices", DataType::UInt32);

            let mut values = Vec::with_capacity(8192 * 4);
            for value in 0..8192u32 {
                values.extend(std::iter::repeat_n(value, 4));
            }
            let mut data = FixedWidthDataBlock {
                bits_per_value: 32,
                data: LanceBuffer::reinterpret_vec(values),
                num_values: 8192 * 4,
                block_info: BlockInfo::default(),
            };
            data.compute_stat();
            let data = DataBlock::FixedWidth(data);

            let compressor = strategy.create_miniblock_compressor(&field, &data).unwrap();
            let (_compressed, encoding) = compressor.compress(miniblock_context(), data).unwrap();
            let rle = expect_rle_encoding(&encoding);

            assert!(
                matches!(
                    rle.values.as_ref().unwrap().compression.as_ref().unwrap(),
                    Compression::Flat(_)
                ),
                "version={version}"
            );
            assert!(
                matches!(
                    rle.run_lengths
                        .as_ref()
                        .unwrap()
                        .compression
                        .as_ref()
                        .unwrap(),
                    Compression::Flat(_)
                ),
                "version={version}"
            );
        }
    }

    #[test]
    #[cfg(feature = "bitpacking")]
    fn test_rle_miniblock_strategy_bitpacks_child_values_when_smaller() {
        let field = create_test_field("dict_indices", DataType::Int32);

        let mut values = Vec::with_capacity(8192 * 4);
        for value in 0..8192 {
            values.extend(std::iter::repeat_n(value, 4));
        }
        let mut data = FixedWidthDataBlock {
            bits_per_value: 32,
            data: LanceBuffer::reinterpret_vec(values),
            num_values: 8192 * 4,
            block_info: BlockInfo::default(),
        };
        data.compute_stat();
        let data = DataBlock::FixedWidth(data);

        let strategy = strategy(TestEncoding::StructuralSparse, CompressionParams::default());
        let compressor = strategy.create_miniblock_compressor(&field, &data).unwrap();
        let debug_str = format!("{compressor:?}");
        assert!(debug_str.contains("RleEncoder"));

        let (_compressed, encoding) = compressor.compress(miniblock_context(), data).unwrap();
        let Compression::Rle(rle) = encoding.compression.as_ref().unwrap() else {
            panic!("expected RLE encoding");
        };
        assert!(matches!(
            rle.values.as_ref().unwrap().compression.as_ref().unwrap(),
            Compression::OutOfLineBitpacking(_)
        ));
        assert!(matches!(
            rle.run_lengths
                .as_ref()
                .unwrap()
                .compression
                .as_ref()
                .unwrap(),
            Compression::Flat(_)
        ));
    }

    #[test]
    #[cfg(feature = "bitpacking")]
    fn test_rle_miniblock_keeps_child_bitpacked_rle_when_smaller_than_inline_bitpacking() {
        let field = create_test_field("int_score", DataType::UInt64);

        let mut values = Vec::with_capacity(8192 * 8);
        for run_idx in 0..8192 {
            let value = match run_idx % 3 {
                0 => 3u64,
                1 => 4u64,
                _ => 5u64,
            };
            values.extend(std::iter::repeat_n(value, 8));
        }
        let mut data = FixedWidthDataBlock {
            bits_per_value: 64,
            data: LanceBuffer::reinterpret_vec(values),
            num_values: 8192 * 8,
            block_info: BlockInfo::default(),
        };
        data.compute_stat();
        let data = DataBlock::FixedWidth(data);

        let strategy = strategy(TestEncoding::StructuralSparse, CompressionParams::default());
        let compressor = strategy.create_miniblock_compressor(&field, &data).unwrap();
        let debug_str = format!("{compressor:?}");
        assert!(
            debug_str.contains("RleEncoder"),
            "expected RLE to beat inline bitpacking after child selection, got: {debug_str}"
        );

        let (_compressed, encoding) = compressor.compress(miniblock_context(), data).unwrap();
        let rle = expect_rle_encoding(&encoding);
        assert!(matches!(
            rle.values.as_ref().unwrap().compression.as_ref().unwrap(),
            Compression::OutOfLineBitpacking(_)
        ));
        assert!(matches!(
            rle.run_lengths
                .as_ref()
                .unwrap()
                .compression
                .as_ref()
                .unwrap(),
            Compression::Flat(_)
        ));
    }

    #[test]
    fn test_field_metadata_override_params() {
        // Set up params with one configuration
        let mut params = CompressionParams::new();
        params.columns.insert(
            "test_column".to_string(),
            CompressionFieldParams {
                rle_threshold: Some(0.3),
                compression: Some("lz4".to_string()),
                compression_level: None,
                bss: None,
                minichunk_size: None,
            },
        );

        let strategy = baseline_strategy(params);

        // Field metadata should override params
        let mut metadata = HashMap::new();
        metadata.insert(COMPRESSION_META_KEY.to_string(), "none".to_string());
        let mut field = create_test_field("test_column", DataType::Int32);
        field.metadata = metadata;

        let data = create_fixed_width_block(32, 1000);
        let compressor = strategy.create_miniblock_compressor(&field, &data).unwrap();

        // Should use none compression (from metadata) instead of lz4 (from params)
        assert!(format!("{:?}", compressor).contains("ValueEncoder"));
    }

    #[test]
    fn test_field_metadata_mixed_configuration() {
        // Configure type-level params
        let mut params = CompressionParams::new();
        params.types.insert(
            "Int32".to_string(),
            CompressionFieldParams {
                rle_threshold: Some(0.5),
                compression: Some("lz4".to_string()),
                ..Default::default()
            },
        );

        let strategy = baseline_strategy(params);

        // Field metadata provides partial override
        let mut metadata = HashMap::new();
        metadata.insert(COMPRESSION_LEVEL_META_KEY.to_string(), "3".to_string());
        let mut field = create_test_field("test_column", DataType::Int32);
        field.metadata = metadata;

        let data = create_fixed_width_block(32, 1000);
        let compressor = strategy.create_miniblock_compressor(&field, &data).unwrap();

        // Should use lz4 (from type params) with level 3 (from metadata)
        let debug_str = format!("{:?}", compressor);
        assert!(debug_str.contains("GeneralMiniBlockCompressor"));
    }

    #[test]
    fn test_bss_field_metadata() {
        let params = CompressionParams::new();
        let strategy = baseline_strategy(params);

        // Test BSS "on" mode with compression enabled (BSS requires compression to be effective)
        let mut metadata = HashMap::new();
        metadata.insert(BSS_META_KEY.to_string(), "on".to_string());
        metadata.insert(COMPRESSION_META_KEY.to_string(), "lz4".to_string());
        let arrow_field =
            ArrowField::new("temperature", DataType::Float32, false).with_metadata(metadata);
        let field = Field::try_from(&arrow_field).unwrap();

        // Create float data
        let data = create_fixed_width_block(32, 100);

        let compressor = strategy.create_miniblock_compressor(&field, &data).unwrap();
        let debug_str = format!("{:?}", compressor);
        assert!(debug_str.contains("ByteStreamSplitEncoder"));
    }

    #[test]
    fn test_bss_with_compression() {
        let params = CompressionParams::new();
        let strategy = baseline_strategy(params);

        // Test BSS with LZ4 compression
        let mut metadata = HashMap::new();
        metadata.insert(BSS_META_KEY.to_string(), "on".to_string());
        metadata.insert(COMPRESSION_META_KEY.to_string(), "lz4".to_string());
        let arrow_field =
            ArrowField::new("sensor_data", DataType::Float64, false).with_metadata(metadata);
        let field = Field::try_from(&arrow_field).unwrap();

        // Create double data
        let data = create_fixed_width_block(64, 100);

        let compressor = strategy.create_miniblock_compressor(&field, &data).unwrap();
        let debug_str = format!("{:?}", compressor);
        // Should have BSS wrapped in general compression
        assert!(debug_str.contains("GeneralMiniBlockCompressor"));
        assert!(debug_str.contains("ByteStreamSplitEncoder"));
    }

    #[test]
    #[cfg(any(feature = "lz4", feature = "zstd"))]
    fn test_general_block_decompression_fixed_width_v2_2() {
        // Request general compression via the write path (2.2 requirement) and ensure the read path mirrors it.
        let mut params = CompressionParams::new();
        params.columns.insert(
            "dict_values".to_string(),
            CompressionFieldParams {
                compression: Some(if cfg!(feature = "lz4") { "lz4" } else { "zstd" }.to_string()),
                ..Default::default()
            },
        );

        let strategy = strategy(TestEncoding::StructuralU32, params);

        let field = create_test_field("dict_values", DataType::FixedSizeBinary(3));
        let data = create_fixed_width_block(24, 1024);
        let DataBlock::FixedWidth(expected_block) = &data else {
            panic!("expected fixed width block");
        };
        let expected_bits = expected_block.bits_per_value;
        let expected_num_values = expected_block.num_values;
        let num_values = expected_num_values;

        let (compressor, encoding) = strategy
            .create_block_compressor(&field, &data)
            .expect("general compression should be selected");
        match encoding.compression.as_ref() {
            Some(Compression::General(_)) => {}
            other => panic!("expected general compression, got {:?}", other),
        }

        let compressed_buffer = compressor
            .compress(data.clone())
            .expect("write path general compression should succeed");

        let decompressor = DefaultDecompressionStrategy::default()
            .create_block_decompressor(&encoding)
            .expect("general block decompressor should be created");

        let decoded = decompressor
            .decompress(compressed_buffer, num_values)
            .expect("decompression should succeed");

        match decoded {
            DataBlock::FixedWidth(block) => {
                assert_eq!(block.bits_per_value, expected_bits);
                assert_eq!(block.num_values, expected_num_values);
                assert_eq!(block.data.as_ref(), expected_block.data.as_ref());
            }
            _ => panic!("expected fixed width block"),
        }
    }

    #[test]
    #[cfg(any(feature = "lz4", feature = "zstd"))]
    fn test_general_compression_not_selected_for_v2_1_even_if_requested() {
        let mut params = CompressionParams::new();
        params.columns.insert(
            "dict_values".to_string(),
            CompressionFieldParams {
                compression: Some(if cfg!(feature = "lz4") { "lz4" } else { "zstd" }.to_string()),
                ..Default::default()
            },
        );

        let strategy = strategy(TestEncoding::StructuralU16, params);
        let field = create_test_field("dict_values", DataType::FixedSizeBinary(3));
        let data = create_fixed_width_block(24, 1024);

        let (_compressor, encoding) = strategy
            .create_block_compressor(&field, &data)
            .expect("block compressor selection should succeed");

        assert!(
            !matches!(encoding.compression.as_ref(), Some(Compression::General(_))),
            "general compression should not be selected for V2.1"
        );
    }

    #[test]
    fn test_none_compression_disables_auto_general_block_compression() {
        let mut params = CompressionParams::new();
        params.columns.insert(
            "dict_values".to_string(),
            CompressionFieldParams {
                compression: Some("none".to_string()),
                ..Default::default()
            },
        );

        let strategy = strategy(TestEncoding::StructuralU32, params);
        let field = create_test_field("dict_values", DataType::FixedSizeBinary(3));
        let data = create_fixed_width_block(24, 20_000);

        assert!(
            data.data_size() > MIN_BLOCK_SIZE_FOR_GENERAL_COMPRESSION,
            "test requires block size above automatic general compression threshold"
        );

        let (_compressor, encoding) = strategy
            .create_block_compressor(&field, &data)
            .expect("block compressor selection should succeed");

        assert!(
            !matches!(encoding.compression.as_ref(), Some(Compression::General(_))),
            "compression=none should disable automatic block general compression"
        );
    }

    #[test]
    fn test_rle_v2_block_selects_u32_run_lengths() {
        let field = create_test_field("dict_indices", DataType::Int32);
        let expected_values = vec![42i32; 70_000];
        let mut block = FixedWidthDataBlock {
            bits_per_value: 32,
            data: LanceBuffer::reinterpret_vec(expected_values.clone()),
            num_values: expected_values.len() as u64,
            block_info: BlockInfo::default(),
        };
        block.compute_stat();
        let data = DataBlock::FixedWidth(block);

        let strategy = strategy(TestEncoding::StructuralSparse, CompressionParams::new());
        let (compressor, encoding) = strategy.create_block_compressor(&field, &data).unwrap();
        assert_eq!(rle_run_length_bits(&encoding), 32);

        let compressed = compressor.compress(data).unwrap();
        let decompressor = DefaultDecompressionStrategy::default()
            .create_block_decompressor(&encoding)
            .unwrap();
        let decoded = decompressor
            .decompress(compressed, expected_values.len() as u64)
            .unwrap();

        match decoded {
            DataBlock::FixedWidth(block) => {
                let values = block.data.borrow_to_typed_slice::<i32>();
                assert_eq!(values.as_ref(), expected_values);
            }
            _ => panic!("expected fixed-width block"),
        }
    }

    #[test]
    fn test_rle_v2_block_keeps_u8_run_lengths_for_v2_2() {
        let field = create_test_field("dict_indices", DataType::Int32);
        let values = vec![42i32; 70_000];
        let mut block = FixedWidthDataBlock {
            bits_per_value: 32,
            data: LanceBuffer::reinterpret_vec(values),
            num_values: 70_000,
            block_info: BlockInfo::default(),
        };
        block.compute_stat();
        let data = DataBlock::FixedWidth(block);

        let strategy = strategy(TestEncoding::StructuralU32, CompressionParams::new());
        let (_compressor, encoding) = strategy.create_block_compressor(&field, &data).unwrap();
        assert_eq!(rle_run_length_bits(&encoding), 8);
    }

    #[test]
    fn test_rle_block_used_for_version_v2_2() {
        let field = create_test_field("test_repdef", DataType::UInt16);

        // Create highly repetitive data
        let num_values = 1000u64;
        let mut data = Vec::with_capacity(num_values as usize);
        for i in 0..10 {
            for _ in 0..100 {
                data.push(i as u16);
            }
        }

        let mut block = FixedWidthDataBlock {
            bits_per_value: 16,
            data: LanceBuffer::reinterpret_vec(data),
            num_values,
            block_info: BlockInfo::default(),
        };

        block.compute_stat();

        let data_block = DataBlock::FixedWidth(block);

        let strategy = strategy(TestEncoding::StructuralU32, CompressionParams::new());

        let (compressor, _) = strategy
            .create_block_compressor(&field, &data_block)
            .unwrap();

        let debug_str = format!("{:?}", compressor);
        assert!(debug_str.contains("RleEncoder"));
    }

    #[test]
    fn test_rle_block_not_used_for_version_v2_1() {
        let field = create_test_field("test_repdef", DataType::UInt16);

        // Create highly repetitive data
        let num_values = 1000u64;
        let mut data = Vec::with_capacity(num_values as usize);
        for i in 0..10 {
            for _ in 0..100 {
                data.push(i as u16);
            }
        }

        let mut block = FixedWidthDataBlock {
            bits_per_value: 16,
            data: LanceBuffer::reinterpret_vec(data),
            num_values,
            block_info: BlockInfo::default(),
        };

        block.compute_stat();

        let data_block = DataBlock::FixedWidth(block);

        let strategy = strategy(TestEncoding::StructuralU16, CompressionParams::new());

        let (compressor, _) = strategy
            .create_block_compressor(&field, &data_block)
            .unwrap();

        let debug_str = format!("{:?}", compressor);
        assert!(
            !debug_str.contains("RleEncoder"),
            "RLE should not be used for V2.1"
        );
    }
}
