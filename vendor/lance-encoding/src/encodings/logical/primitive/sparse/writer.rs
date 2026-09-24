// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Sparse structural planning and serialization.

use std::iter;

use arrow_buffer::BooleanBuffer;
use lance_core::{Error, Result, datatypes::Field, utils::bit::pad_bytes};

use crate::{
    buffer::LanceBuffer,
    compression::{CompressionStrategy, compress_required_block},
    data::{BlockInfo, DataBlock, FixedWidthDataBlock},
    decoder::PageEncoding,
    encoder::EncodedPage,
    format::pb21::{self, CompressiveEncoding},
    repdef::{NormalizedStructuralLayer, NormalizedStructuralPlan},
    statistics::ComputeStat,
};

use super::super::MiniblockChunkSize;

use super::{
    SparseCountSet, SparsePositionSet, SparseStructuralLayerPlan, SparseStructuralPlan,
    SparseValidityMeaning, SparseValiditySet,
};
use crate::encodings::logical::primitive::{
    FILL_BYTE, MINIBLOCK_ALIGNMENT,
    miniblock::{MiniBlockCompressed, MiniBlockCompressionContext},
};

#[derive(Clone, Copy, Default)]
struct PositionSetStats {
    count: u64,
    first: u64,
    last: u64,
    is_contiguous: bool,
}

impl PositionSetStats {
    fn observe(&mut self, position: u64) {
        if self.count == 0 {
            self.first = position;
            self.is_contiguous = true;
        } else if self.last.checked_add(1) != Some(position) {
            self.is_contiguous = false;
        }
        self.last = position;
        self.count += 1;
    }

    fn encoded_cost(&self, domain_len: u64) -> u64 {
        if self.count == 0 || self.count == domain_len || self.is_contiguous {
            0
        } else {
            self.count
        }
    }

    fn to_set(
        self,
        validity: &BooleanBuffer,
        want_valid: bool,
        domain_len: u64,
        label: &str,
    ) -> Result<SparsePositionSet> {
        if self.count == 0 {
            return Ok(SparsePositionSet::empty());
        }
        if self.count == domain_len {
            return Ok(SparsePositionSet::all(domain_len));
        }
        if self.is_contiguous {
            return Ok(SparsePositionSet::range(self.first, self.count));
        }

        let capacity = usize::try_from(self.count).map_err(|_| {
            Error::invalid_input_source(
                format!("Sparse structural {label} positions exceed usize::MAX").into(),
            )
        })?;
        let mut positions = Vec::with_capacity(capacity);
        for (index, is_valid) in validity.iter().enumerate() {
            if is_valid == want_valid {
                positions.push(u64::try_from(index).map_err(|_| {
                    Error::invalid_input_source(
                        format!("Sparse structural {label} position exceeds u64::MAX").into(),
                    )
                })?);
            }
        }
        SparsePositionSet::from_positions(positions, domain_len, label)
    }
}

fn usize_to_u64(value: usize, label: &str) -> Result<u64> {
    u64::try_from(value).map_err(|_| {
        Error::invalid_input_source(
            format!("Sparse structural {label} {value} exceeds u64::MAX").into(),
        )
    })
}

fn validity_set(
    validity: Option<&BooleanBuffer>,
    num_slots: usize,
    label: &str,
) -> Result<SparseValiditySet> {
    let Some(validity) = validity else {
        return Ok(SparseValiditySet {
            meaning: SparseValidityMeaning::NullPositions,
            positions: SparsePositionSet::empty(),
        });
    };
    if validity.len() != num_slots {
        return Err(Error::invalid_input_source(
            format!(
                "Sparse structural {label} validity length {} does not match {} slots",
                validity.len(),
                num_slots
            )
            .into(),
        ));
    }

    let domain_len = usize_to_u64(num_slots, "validity domain")?;
    let mut valid_stats = PositionSetStats::default();
    let mut null_stats = PositionSetStats::default();
    for (index, is_valid) in validity.iter().enumerate() {
        let index = usize_to_u64(index, "validity position")?;
        if is_valid {
            valid_stats.observe(index);
        } else {
            null_stats.observe(index);
        }
    }

    if null_stats.count == 0 {
        return Ok(SparseValiditySet {
            meaning: SparseValidityMeaning::NullPositions,
            positions: SparsePositionSet::empty(),
        });
    }
    if valid_stats.count == 0 {
        return Ok(SparseValiditySet {
            meaning: SparseValidityMeaning::ValidPositions,
            positions: SparsePositionSet::empty(),
        });
    }

    let valid_cost = valid_stats.encoded_cost(domain_len);
    let null_cost = null_stats.encoded_cost(domain_len);
    if valid_cost < null_cost {
        Ok(SparseValiditySet {
            meaning: SparseValidityMeaning::ValidPositions,
            positions: valid_stats.to_set(validity, true, domain_len, label)?,
        })
    } else {
        Ok(SparseValiditySet {
            meaning: SparseValidityMeaning::NullPositions,
            positions: null_stats.to_set(validity, false, domain_len, label)?,
        })
    }
}

/// Builds the semantic sparse plan directly from the once-normalized Arrow layers.
pub(in crate::encodings::logical::primitive) fn plan(
    normalized: &NormalizedStructuralPlan,
    num_visible_items: u64,
) -> Result<SparseStructuralPlan> {
    let mut layers = Vec::with_capacity(normalized.layers().len());
    let mut num_items = num_visible_items;

    for layer in normalized.layers() {
        match layer {
            NormalizedStructuralLayer::Validity {
                validity,
                num_slots,
            } => {
                layers.push(SparseStructuralLayerPlan::Validity {
                    num_slots: usize_to_u64(num_slots, "validity slot count")?,
                    validity: validity_set(validity, num_slots, "validity")?,
                });
            }
            NormalizedStructuralLayer::FixedSizeList {
                validity,
                dimension,
                num_slots,
            } => {
                if dimension == 0 {
                    return Err(Error::invalid_input_source(
                        "Sparse structural fixed-size-list dimension is zero".into(),
                    ));
                }
                layers.push(SparseStructuralLayerPlan::FixedSizeList {
                    num_slots: usize_to_u64(num_slots, "fixed-size-list slot count")?,
                    dimension: usize_to_u64(dimension, "fixed-size-list dimension")?,
                    validity: validity_set(validity, num_slots, "fixed-size-list validity")?,
                });
            }
            NormalizedStructuralLayer::List {
                offsets,
                validity,
                num_slots,
            } => {
                let expected_offsets = num_slots.checked_add(1).ok_or_else(|| {
                    Error::invalid_input_source(
                        "Sparse structural list offset count overflows".into(),
                    )
                })?;
                if offsets.len() != expected_offsets {
                    return Err(Error::invalid_input_source(
                        format!(
                            "Sparse structural list has {} offsets for {} slots",
                            offsets.len(),
                            num_slots
                        )
                        .into(),
                    ));
                }
                if offsets.first().copied() != Some(0) {
                    return Err(Error::invalid_input_source(
                        "Sparse structural list offsets must start at zero".into(),
                    ));
                }

                let mut non_empty_positions = Vec::new();
                let mut counts = Vec::new();
                for slot in 0..num_slots {
                    let start = offsets[slot];
                    let end = offsets[slot + 1];
                    let count = end.checked_sub(start).ok_or_else(|| {
                        Error::invalid_input_source(
                            format!(
                                "Sparse structural list offsets decrease at slot {slot}: {start}..{end}"
                            )
                            .into(),
                        )
                    })?;
                    let is_valid = validity.is_none_or(|validity| validity.value(slot));
                    if !is_valid && count != 0 {
                        return Err(Error::invalid_input_source(
                            format!(
                                "Sparse structural null list slot {slot} has {count} child slots"
                            )
                            .into(),
                        ));
                    }
                    if is_valid && count > 0 {
                        non_empty_positions.push(usize_to_u64(slot, "list position")?);
                        counts.push(u64::try_from(count).map_err(|_| {
                            Error::invalid_input_source(
                                format!("Sparse structural list count {count} exceeds u64::MAX")
                                    .into(),
                            )
                        })?);
                    }
                }

                let num_slots_u64 = usize_to_u64(num_slots, "list slot count")?;
                let num_non_empty = usize_to_u64(non_empty_positions.len(), "list position count")?;
                num_items = num_items
                    .checked_add(num_slots_u64 - num_non_empty)
                    .ok_or_else(|| {
                        Error::invalid_input_source(
                            "Sparse structural item count overflows u64".into(),
                        )
                    })?;
                let num_child_slots = offsets.last().copied().ok_or_else(|| {
                    Error::invalid_input_source("Sparse structural list has no offsets".into())
                })?;
                let num_child_slots = u64::try_from(num_child_slots).map_err(|_| {
                    Error::invalid_input_source(
                        format!(
                            "Sparse structural list child slot count {num_child_slots} is negative"
                        )
                        .into(),
                    )
                })?;
                layers.push(SparseStructuralLayerPlan::List {
                    num_slots: num_slots_u64,
                    num_child_slots,
                    non_empty_positions: SparsePositionSet::from_positions(
                        non_empty_positions,
                        num_slots_u64,
                        "list non-empty",
                    )?,
                    counts: SparseCountSet::from_counts(counts)?,
                    validity: validity_set(validity, num_slots, "list validity")?,
                });
            }
        }
    }

    let row_domain = match layers.first() {
        Some(SparseStructuralLayerPlan::Validity { num_slots, .. })
        | Some(SparseStructuralLayerPlan::List { num_slots, .. })
        | Some(SparseStructuralLayerPlan::FixedSizeList { num_slots, .. }) => *num_slots,
        None => {
            return Err(Error::invalid_input_source(
                "Sparse structural encoding requires at least one Arrow structural layer".into(),
            ));
        }
    };
    let plan = SparseStructuralPlan {
        layers,
        num_items,
        num_visible_items,
    };
    plan.validate(row_domain)?;
    Ok(plan)
}

/// ConstantLayout remains the canonical representation when no value payload exists.
pub(in crate::encodings::logical::primitive) fn uses_constant_layout(
    plan: &SparseStructuralPlan,
    field: &Field,
) -> bool {
    if plan.num_visible_items == 0 {
        return true;
    }
    if matches!(field.data_type(), arrow_schema::DataType::Struct(fields) if fields.is_empty()) {
        return true;
    }

    let Some(layer) = plan.layers.last() else {
        return false;
    };
    let (num_slots, validity) = match layer {
        SparseStructuralLayerPlan::Validity {
            num_slots,
            validity,
        }
        | SparseStructuralLayerPlan::List {
            num_slots,
            validity,
            ..
        }
        | SparseStructuralLayerPlan::FixedSizeList {
            num_slots,
            validity,
            ..
        } => (*num_slots, validity),
    };
    match validity.meaning {
        SparseValidityMeaning::NullPositions => validity.positions.len() == num_slots,
        SparseValidityMeaning::ValidPositions => validity.positions.is_empty(),
    }
}

fn supports_fixed_size_list_values(data: &DataBlock) -> bool {
    match data {
        DataBlock::FixedWidth(_) => true,
        DataBlock::FixedSizeList(list) => supports_fixed_size_list_values(list.child.as_ref()),
        DataBlock::Nullable(nullable) => supports_fixed_size_list_values(nullable.data.as_ref()),
        _ => false,
    }
}

/// Whether the sparse writer can encode this value block without changing its value path.
pub fn supports_value_block(data: &DataBlock) -> bool {
    match data {
        DataBlock::FixedWidth(_) | DataBlock::VariableWidth(_) => true,
        DataBlock::Struct(data) => !data.has_variable_width_child(),
        DataBlock::FixedSizeList(data) => supports_fixed_size_list_values(data.child.as_ref()),
        DataBlock::Empty()
        | DataBlock::Constant(_)
        | DataBlock::AllNull(_)
        | DataBlock::Nullable(_)
        | DataBlock::Opaque(_)
        | DataBlock::Dictionary(_) => false,
    }
}

struct SparseMiniBlockChunk {
    buffer_sizes: Vec<u32>,
    num_values: u32,
}

struct SparseMiniBlockCompressed {
    data: Vec<LanceBuffer>,
    chunks: Vec<SparseMiniBlockChunk>,
}

struct SerializedValuePage {
    num_buffers: u64,
    data: LanceBuffer,
    metadata: LanceBuffer,
}

pub struct PreparedSparseValues {
    num_values: u64,
    value_compression: CompressiveEncoding,
    values: SerializedValuePage,
}

pub enum SparseValueInput {
    Unprepared(DataBlock),
    Prepared(PreparedSparseValues),
}

struct EncodedStructuralPlan {
    layers: Vec<pb21::SparseStructuralLayer>,
    buffers: Vec<LanceBuffer>,
}

fn with_explicit_value_counts(
    compressed: MiniBlockCompressed,
) -> Result<SparseMiniBlockCompressed> {
    let mut values_in_previous_chunks = 0_u64;
    let mut chunks = Vec::with_capacity(compressed.chunks.len());
    for chunk in compressed.chunks {
        let num_values = chunk.num_values(values_in_previous_chunks, compressed.num_values);
        values_in_previous_chunks = values_in_previous_chunks
            .checked_add(num_values)
            .ok_or_else(|| Error::internal("Sparse value count overflows u64".to_string()))?;
        chunks.push(SparseMiniBlockChunk {
            buffer_sizes: chunk.buffer_sizes,
            num_values: u32::try_from(num_values).map_err(|_| {
                Error::invalid_input_source(
                    format!(
                        "Sparse value chunk has {num_values} visible values, which exceeds the u32 metadata limit"
                    )
                    .into(),
                )
            })?,
        });
    }
    if values_in_previous_chunks != compressed.num_values {
        return Err(Error::internal(format!(
            "Sparse value chunks describe {values_in_previous_chunks} values, expected {}",
            compressed.num_values
        )));
    }
    Ok(SparseMiniBlockCompressed {
        data: compressed.data,
        chunks,
    })
}

fn serialize_value_chunks(
    compressed: SparseMiniBlockCompressed,
    miniblock_chunk_size: MiniblockChunkSize,
) -> Result<SerializedValuePage> {
    let bytes_data = compressed.data.iter().map(LanceBuffer::len).sum::<usize>();
    let num_buffers = compressed.data.len();
    let mut data_buffer = Vec::with_capacity(bytes_data + 9 * num_buffers);
    let mut metadata = Vec::with_capacity(compressed.chunks.len() * 8);
    let mut buffer_offsets = vec![0_usize; num_buffers];

    for chunk in compressed.chunks {
        if chunk.buffer_sizes.len() != num_buffers {
            return Err(Error::internal(format!(
                "Sparse chunk has {} value buffer sizes, expected {num_buffers}",
                chunk.buffer_sizes.len()
            )));
        }

        let chunk_start = data_buffer.len();
        debug_assert_eq!(chunk_start % MINIBLOCK_ALIGNMENT, 0);
        data_buffer.extend_from_slice(&0_u16.to_le_bytes());
        if miniblock_chunk_size == MiniblockChunkSize::U32 {
            for buffer_size in &chunk.buffer_sizes {
                data_buffer.extend_from_slice(&buffer_size.to_le_bytes());
            }
        } else {
            for buffer_size in &chunk.buffer_sizes {
                let buffer_size = u16::try_from(*buffer_size).map_err(|_| {
                    Error::internal(format!(
                        "Sparse value buffer size ({buffer_size} bytes) exceeds 16-bit metadata"
                    ))
                })?;
                data_buffer.extend_from_slice(&buffer_size.to_le_bytes());
            }
        }
        let add_padding = |buffer: &mut Vec<u8>| {
            let padding = pad_bytes::<MINIBLOCK_ALIGNMENT>(buffer.len());
            buffer.extend(iter::repeat_n(FILL_BYTE, padding));
        };
        add_padding(&mut data_buffer);

        for (buffer_size, (buffer, buffer_offset)) in chunk
            .buffer_sizes
            .iter()
            .zip(compressed.data.iter().zip(buffer_offsets.iter_mut()))
        {
            let start = *buffer_offset;
            let end = start.checked_add(*buffer_size as usize).ok_or_else(|| {
                Error::internal("Sparse value buffer range overflows".to_string())
            })?;
            let bytes = buffer.as_ref().get(start..end).ok_or_else(|| {
                Error::internal(format!(
                    "Sparse value chunk requests bytes {start}..{end} from a {}-byte buffer",
                    buffer.len()
                ))
            })?;
            *buffer_offset = end;
            data_buffer.extend_from_slice(bytes);
            add_padding(&mut data_buffer);
        }

        let chunk_bytes = data_buffer.len() - chunk_start;
        if chunk_bytes == 0 || !chunk_bytes.is_multiple_of(MINIBLOCK_ALIGNMENT) {
            return Err(Error::internal(format!(
                "Sparse value chunk size {chunk_bytes} is not a positive multiple of {MINIBLOCK_ALIGNMENT}"
            )));
        }
        let words_minus_one = chunk_bytes / MINIBLOCK_ALIGNMENT - 1;
        metadata.extend_from_slice(
            &u32::try_from(words_minus_one)
                .map_err(|_| {
                    Error::internal(format!(
                        "Sparse value chunk size {chunk_bytes} exceeds the metadata limit"
                    ))
                })?
                .to_le_bytes(),
        );
        metadata.extend_from_slice(&chunk.num_values.to_le_bytes());
    }

    for (index, (consumed, buffer)) in buffer_offsets
        .iter()
        .zip(compressed.data.iter())
        .enumerate()
    {
        if *consumed != buffer.len() {
            return Err(Error::internal(format!(
                "Sparse value buffer {index} consumed {consumed} bytes, expected {}",
                buffer.len()
            )));
        }
    }

    Ok(SerializedValuePage {
        num_buffers: usize_to_u64(num_buffers, "value buffer count")?,
        data: LanceBuffer::from(data_buffer),
        metadata: LanceBuffer::from(metadata),
    })
}

pub fn prepare_values(
    field: &Field,
    compression_strategy: &dyn CompressionStrategy,
    data: DataBlock,
    miniblock_chunk_size: MiniblockChunkSize,
) -> Result<PreparedSparseValues> {
    match &data {
        DataBlock::AllNull(_) => {
            return Err(Error::internal(
                "All-null values must use ConstantLayout".to_string(),
            ));
        }
        DataBlock::Dictionary(_) => {
            return Err(Error::not_supported_source(
                "Sparse layout does not support dictionary data blocks".into(),
            ));
        }
        DataBlock::Struct(data) if data.has_variable_width_child() => {
            return Err(Error::not_supported_source(
                "Sparse layout does not support variable-width packed struct data blocks".into(),
            ));
        }
        _ => {}
    }

    let num_values = data.num_values();
    let compressor = compression_strategy.create_miniblock_compressor(field, &data)?;
    let support_large_chunk = miniblock_chunk_size == MiniblockChunkSize::U32;
    let compression_context = MiniBlockCompressionContext::new(0, support_large_chunk, false);
    let (compressed, value_compression) = compressor.compress(compression_context, data)?;
    let values = serialize_value_chunks(
        with_explicit_value_counts(compressed)?,
        miniblock_chunk_size,
    )?;
    Ok(PreparedSparseValues {
        num_values,
        value_compression,
        values,
    })
}

fn encode_u64_values(
    values: Vec<u64>,
    compression_strategy: &dyn CompressionStrategy,
) -> Result<(LanceBuffer, CompressiveEncoding)> {
    let num_values = usize_to_u64(values.len(), "u64 value count")?;
    let mut block = DataBlock::FixedWidth(FixedWidthDataBlock {
        data: LanceBuffer::reinterpret_vec(values),
        bits_per_value: 64,
        num_values,
        block_info: BlockInfo::new(),
    });
    block.compute_stat();
    let field = Field::new_arrow("", arrow_schema::DataType::UInt64, false)?;
    compress_required_block(compression_strategy, &field, block)
}

fn positions_to_deltas(positions: &[u64], label: &str) -> Result<Vec<u64>> {
    let mut previous = 0_u64;
    positions
        .iter()
        .copied()
        .enumerate()
        .map(|(index, position)| {
            if index > 0 && position <= previous {
                return Err(Error::invalid_input_source(
                    format!("Sparse structural {label} positions must be strictly increasing")
                        .into(),
                ));
            }
            let delta = if index == 0 {
                position
            } else {
                position - previous
            };
            previous = position;
            Ok(delta)
        })
        .collect()
}

fn encode_position_set(
    positions: &SparsePositionSet,
    compression_strategy: &dyn CompressionStrategy,
    label: &str,
) -> Result<(Option<LanceBuffer>, pb21::SparsePositionSet)> {
    let (buffer, positions_pb) = match positions {
        SparsePositionSet::Empty => (
            None,
            pb21::sparse_position_set::Positions::Empty(pb21::SparsePositionEmpty {}),
        ),
        SparsePositionSet::All { .. } => (
            None,
            pb21::sparse_position_set::Positions::All(pb21::SparsePositionAll {}),
        ),
        SparsePositionSet::Range { start, len } => (
            None,
            pb21::sparse_position_set::Positions::Range(pb21::SparsePositionRange {
                start: *start,
                length: *len,
            }),
        ),
        SparsePositionSet::Explicit(positions) => {
            if positions.is_empty() {
                return Err(Error::internal(format!(
                    "Sparse structural {label} explicit set is empty"
                )));
            }
            let (buffer, encoding) =
                encode_u64_values(positions_to_deltas(positions, label)?, compression_strategy)?;
            (
                Some(buffer),
                pb21::sparse_position_set::Positions::Explicit(encoding),
            )
        }
    };
    Ok((
        buffer,
        pb21::SparsePositionSet {
            positions: Some(positions_pb),
            num_positions: positions.len(),
        },
    ))
}

fn encode_count_set(
    counts: &SparseCountSet,
    compression_strategy: &dyn CompressionStrategy,
) -> Result<(Option<LanceBuffer>, pb21::SparseCountSet)> {
    let (buffer, counts_pb) = match counts {
        SparseCountSet::Empty => (
            None,
            pb21::sparse_count_set::Counts::Empty(pb21::SparseCountEmpty {}),
        ),
        SparseCountSet::Constant { value, .. } => (
            None,
            pb21::sparse_count_set::Counts::Constant(pb21::SparseCountConstant { value: *value }),
        ),
        SparseCountSet::Explicit { counts, .. } => {
            if counts.is_empty() {
                return Err(Error::internal(
                    "Sparse structural explicit count set is empty".to_string(),
                ));
            }
            let (buffer, encoding) = encode_u64_values(counts.to_vec(), compression_strategy)?;
            (
                Some(buffer),
                pb21::sparse_count_set::Counts::Explicit(encoding),
            )
        }
    };
    Ok((
        buffer,
        pb21::SparseCountSet {
            counts: Some(counts_pb),
        },
    ))
}

fn encode_validity_set(
    validity: &SparseValiditySet,
    compression_strategy: &dyn CompressionStrategy,
    label: &str,
) -> Result<(Option<LanceBuffer>, pb21::SparseValiditySet)> {
    let (buffer, positions) =
        encode_position_set(&validity.positions, compression_strategy, label)?;
    let meaning = match validity.meaning {
        SparseValidityMeaning::NullPositions => {
            pb21::sparse_validity_set::Meaning::SparseValidityNullPositions
        }
        SparseValidityMeaning::ValidPositions => {
            pb21::sparse_validity_set::Meaning::SparseValidityValidPositions
        }
    };
    Ok((
        buffer,
        pb21::SparseValiditySet {
            meaning: meaning as i32,
            positions: Some(positions),
        },
    ))
}

fn encode_structural_plan(
    plan: &SparseStructuralPlan,
    compression_strategy: &dyn CompressionStrategy,
) -> Result<EncodedStructuralPlan> {
    let mut layers = Vec::with_capacity(plan.layers.len());
    let mut buffers = Vec::new();

    for layer in &plan.layers {
        match layer {
            SparseStructuralLayerPlan::Validity {
                num_slots,
                validity,
            } => {
                let (validity_buffer, validity) =
                    encode_validity_set(validity, compression_strategy, "validity")?;
                buffers.extend(validity_buffer);
                layers.push(pb21::SparseStructuralLayer {
                    layer: Some(pb21::sparse_structural_layer::Layer::Validity(
                        pb21::SparseValidityLayer {
                            num_slots: *num_slots,
                            validity: Some(validity),
                        },
                    )),
                });
            }
            SparseStructuralLayerPlan::List {
                num_slots,
                num_child_slots,
                non_empty_positions,
                counts,
                validity,
            } => {
                if non_empty_positions.len() != counts.len() {
                    return Err(Error::invalid_input_source(
                        format!(
                            "Sparse structural list has {} non-empty positions but {} counts",
                            non_empty_positions.len(),
                            counts.len()
                        )
                        .into(),
                    ));
                }
                let (position_buffer, non_empty_positions) = encode_position_set(
                    non_empty_positions,
                    compression_strategy,
                    "list non-empty",
                )?;
                buffers.extend(position_buffer);
                let (count_buffer, counts) = encode_count_set(counts, compression_strategy)?;
                buffers.extend(count_buffer);
                let (validity_buffer, validity) =
                    encode_validity_set(validity, compression_strategy, "list validity")?;
                buffers.extend(validity_buffer);
                layers.push(pb21::SparseStructuralLayer {
                    layer: Some(pb21::sparse_structural_layer::Layer::List(
                        pb21::SparseListLayer {
                            num_slots: *num_slots,
                            num_child_slots: *num_child_slots,
                            non_empty_positions: Some(non_empty_positions),
                            counts: Some(counts),
                            validity: Some(validity),
                        },
                    )),
                });
            }
            SparseStructuralLayerPlan::FixedSizeList {
                num_slots,
                dimension,
                validity,
            } => {
                let (validity_buffer, validity) = encode_validity_set(
                    validity,
                    compression_strategy,
                    "fixed-size-list validity",
                )?;
                buffers.extend(validity_buffer);
                num_slots.checked_mul(*dimension).ok_or_else(|| {
                    Error::invalid_input_source(
                        format!(
                            "Sparse structural fixed-size-list child slot count overflows: slots={num_slots}, dimension={dimension}"
                        )
                        .into(),
                    )
                })?;
                layers.push(pb21::SparseStructuralLayer {
                    layer: Some(pb21::sparse_structural_layer::Layer::FixedSizeList(
                        pb21::SparseFixedSizeListLayer {
                            num_slots: *num_slots,
                            dimension: *dimension,
                            validity: Some(validity),
                        },
                    )),
                });
            }
        }
    }

    Ok(EncodedStructuralPlan { layers, buffers })
}

#[allow(clippy::too_many_arguments)]
pub(in crate::encodings::logical::primitive) fn encode_page(
    column_idx: u32,
    field: &Field,
    compression_strategy: &dyn CompressionStrategy,
    values: SparseValueInput,
    plan: SparseStructuralPlan,
    row_number: u64,
    num_rows: u64,
    miniblock_chunk_size: MiniblockChunkSize,
) -> Result<EncodedPage> {
    let PreparedSparseValues {
        num_values,
        value_compression,
        values,
    } = match values {
        SparseValueInput::Unprepared(data) => {
            prepare_values(field, compression_strategy, data, miniblock_chunk_size)?
        }
        SparseValueInput::Prepared(prepared) => prepared,
    };
    if plan.num_visible_items != num_values {
        return Err(Error::internal(format!(
            "Sparse structural plan has {} visible items but data has {} values",
            plan.num_visible_items, num_values
        )));
    }
    let structural = encode_structural_plan(&plan, compression_strategy)?;
    let description = pb21::PageLayout {
        layout: Some(pb21::page_layout::Layout::SparseLayout(
            pb21::SparseLayout {
                value_compression: Some(value_compression),
                num_buffers: values.num_buffers,
                num_items: plan.num_items,
                num_visible_items: plan.num_visible_items,
                has_large_chunk: miniblock_chunk_size == MiniblockChunkSize::U32,
                structural_layers: structural.layers,
            },
        )),
    };

    let mut page_data = Vec::with_capacity(2 + structural.buffers.len());
    page_data.push(values.metadata);
    page_data.push(values.data);
    page_data.extend(structural.buffers);
    Ok(EncodedPage {
        data: page_data,
        description: PageEncoding::Structural(description),
        num_rows,
        row_number,
        column_idx,
    })
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use arrow_array::{
        Array, ArrayRef, DictionaryArray, FixedSizeBinaryArray, FixedSizeListArray, Int8Array,
        Int32Array, LargeListArray, ListArray, StringArray, StructArray,
        builder::{Int32Builder, MapBuilder, StringBuilder},
        types::Int8Type,
    };
    use arrow_buffer::{BooleanBuffer, NullBuffer, OffsetBuffer, ScalarBuffer};
    use arrow_schema::{DataType, Field as ArrowField, Fields};

    use crate::{
        constants::{
            PACKED_STRUCT_META_KEY, STRUCTURAL_ENCODING_FULLZIP, STRUCTURAL_ENCODING_META_KEY,
            STRUCTURAL_ENCODING_MINIBLOCK, STRUCTURAL_ENCODING_SPARSE,
        },
        data::FixedSizeListBlock,
        encoder::{
            ColumnIndexSequence, EncodingOptions, FieldEncoder, MIN_PAGE_BUFFER_ALIGNMENT,
            OutOfLineBuffers,
        },
        testing::{
            TestCases, TestEncoding, check_round_trip_encoding_of_data, test_encoding_strategy,
        },
    };

    use super::*;

    fn sparse_metadata() -> HashMap<String, String> {
        HashMap::from([(
            STRUCTURAL_ENCODING_META_KEY.to_string(),
            STRUCTURAL_ENCODING_SPARSE.to_string(),
        )])
    }

    fn structural_metadata(value: &str) -> HashMap<String, String> {
        HashMap::from([(STRUCTURAL_ENCODING_META_KEY.to_string(), value.to_string())])
    }

    fn null_buffer(validity: impl IntoIterator<Item = bool>) -> NullBuffer {
        NullBuffer::new(BooleanBuffer::from_iter(validity))
    }

    fn list_i32(offsets: Vec<i32>, validity: Option<Vec<bool>>) -> ArrayRef {
        let num_values = offsets.last().copied().unwrap_or_default();
        let values = Arc::new(Int32Array::from_iter_values(0..num_values)) as ArrayRef;
        Arc::new(
            ListArray::try_new(
                Arc::new(ArrowField::new("item", DataType::Int32, true)),
                OffsetBuffer::new(ScalarBuffer::from(offsets)),
                values,
                validity.map(null_buffer),
            )
            .unwrap(),
        )
    }

    fn sparse_list_values(
        num_rows: usize,
        stride: usize,
        values: ArrayRef,
        item_field: Arc<ArrowField>,
    ) -> ArrayRef {
        let mut offsets = Vec::with_capacity(num_rows + 1);
        let mut num_values = 0_i32;
        offsets.push(num_values);
        for row in 0..num_rows {
            if (row + 1).is_multiple_of(stride) || row + 1 == num_rows {
                num_values += 1;
            }
            offsets.push(num_values);
        }
        assert_eq!(values.len(), num_values as usize);
        Arc::new(
            ListArray::try_new(
                item_field,
                OffsetBuffer::new(ScalarBuffer::from(offsets)),
                values,
                None,
            )
            .unwrap(),
        )
    }

    fn sparse_i32_list(num_rows: usize, stride: usize) -> ArrayRef {
        let num_values = num_rows.div_ceil(stride);
        sparse_list_values(
            num_rows,
            stride,
            Arc::new(Int32Array::from_iter_values(0..num_values as i32)),
            Arc::new(ArrowField::new("item", DataType::Int32, true)),
        )
    }

    fn unsplittable_nested_list(values: ArrayRef, item_field: Arc<ArrowField>) -> ArrayRef {
        const NUM_INNER_LISTS: usize = 70_000;

        let mut inner_offsets = vec![0_i32; NUM_INNER_LISTS + 1];
        assert!(!values.is_empty());
        assert!(values.len() <= NUM_INNER_LISTS);
        for value_index in 1..=values.len() {
            inner_offsets[NUM_INNER_LISTS - values.len() + value_index] = value_index as i32;
        }
        let inner = Arc::new(
            ListArray::try_new(
                item_field,
                OffsetBuffer::new(ScalarBuffer::from(inner_offsets)),
                values,
                None,
            )
            .unwrap(),
        ) as ArrayRef;
        Arc::new(
            ListArray::try_new(
                Arc::new(ArrowField::new("item", inner.data_type().clone(), true)),
                OffsetBuffer::new(ScalarBuffer::from(vec![0_i32, NUM_INNER_LISTS as i32])),
                inner,
                None,
            )
            .unwrap(),
        )
    }

    fn variable_packed_struct_values(num_values: usize) -> (ArrayRef, Arc<ArrowField>) {
        let fields = Fields::from(vec![ArrowField::new("value", DataType::Utf8, false)]);
        let values = Arc::new(StructArray::new(
            fields.clone(),
            vec![Arc::new(StringArray::from_iter_values(
                (0..num_values).map(|index| format!("value-{index}")),
            ))],
            None,
        )) as ArrayRef;
        let item_field = Arc::new(
            ArrowField::new("item", DataType::Struct(fields), true).with_metadata(HashMap::from([
                (PACKED_STRUCT_META_KEY.to_string(), "true".to_string()),
            ])),
        );
        (values, item_field)
    }

    fn dictionary_values(num_values: usize) -> (ArrayRef, Arc<ArrowField>) {
        let keys = Int8Array::from_iter_values((0..num_values).map(|index| (index % 2) as i8));
        let values = Arc::new(StringArray::from(vec!["value-0", "value-1"]));
        let dictionary = Arc::new(DictionaryArray::<Int8Type>::try_new(keys, values).unwrap());
        let item_field = Arc::new(ArrowField::new(
            "item",
            dictionary.data_type().clone(),
            true,
        ));
        (dictionary, item_field)
    }

    fn large_list_i32(offsets: Vec<i64>, validity: Option<Vec<bool>>) -> ArrayRef {
        let num_values = offsets.last().copied().unwrap_or_default();
        let values = Arc::new(Int32Array::from_iter_values(0..num_values as i32)) as ArrayRef;
        Arc::new(
            LargeListArray::try_new(
                Arc::new(ArrowField::new("item", DataType::Int32, true)),
                OffsetBuffer::new(ScalarBuffer::from(offsets)),
                values,
                validity.map(null_buffer),
            )
            .unwrap(),
        )
    }

    fn map_i32() -> ArrayRef {
        let mut builder = MapBuilder::new(None, StringBuilder::new(), Int32Builder::new());
        builder.keys().append_value("a");
        builder.values().append_value(1);
        builder.append(true).unwrap();
        builder.append(false).unwrap();
        builder.append(true).unwrap();
        builder.keys().append_value("b");
        builder.values().append_null();
        builder.keys().append_value("c");
        builder.values().append_value(3);
        builder.append(true).unwrap();
        Arc::new(builder.finish())
    }

    fn fixed_size_list_struct() -> ArrayRef {
        let fields = Fields::from(vec![ArrowField::new("value", DataType::Int32, true)]);
        let child = Arc::new(StructArray::new(
            fields.clone(),
            vec![Arc::new(Int32Array::from(vec![
                Some(0),
                Some(1),
                None,
                Some(3),
                Some(4),
                Some(5),
                None,
                Some(7),
                Some(8),
                Some(9),
                Some(10),
                None,
            ]))],
            Some(null_buffer([
                true, true, false, true, true, true, true, false, true, true, true, true,
            ])),
        )) as ArrayRef;
        Arc::new(
            FixedSizeListArray::try_new(
                Arc::new(ArrowField::new("item", DataType::Struct(fields), true)),
                2,
                child,
                Some(null_buffer([true, false, true, true, false, true])),
            )
            .unwrap(),
        )
    }

    fn list_fixed_size_list_struct() -> ArrayRef {
        let struct_fields = Fields::from(vec![ArrowField::new("value", DataType::Int32, true)]);
        let structs = Arc::new(StructArray::new(
            struct_fields.clone(),
            vec![Arc::new(Int32Array::from(vec![
                Some(0),
                None,
                Some(2),
                Some(3),
                None,
                Some(5),
            ]))],
            Some(null_buffer([true, true, false, true, true, true])),
        )) as ArrayRef;
        let fixed_size_list = Arc::new(
            FixedSizeListArray::try_new(
                Arc::new(ArrowField::new(
                    "item",
                    DataType::Struct(struct_fields),
                    true,
                )),
                2,
                structs,
                Some(null_buffer([true, false, true])),
            )
            .unwrap(),
        ) as ArrayRef;
        Arc::new(
            ListArray::try_new(
                Arc::new(ArrowField::new(
                    "item",
                    fixed_size_list.data_type().clone(),
                    true,
                )),
                OffsetBuffer::new(ScalarBuffer::from(vec![0_i32, 1, 1, 3, 3])),
                fixed_size_list,
                Some(null_buffer([true, false, true, true])),
            )
            .unwrap(),
        )
    }

    fn nullable_struct() -> ArrayRef {
        let fields = Fields::from(vec![ArrowField::new("value", DataType::Int32, true)]);
        Arc::new(StructArray::new(
            fields,
            vec![Arc::new(Int32Array::from(vec![
                Some(10),
                Some(20),
                None,
                Some(40),
                Some(50),
            ]))],
            Some(null_buffer([true, false, true, true, false])),
        ))
    }

    fn deeply_nested() -> ArrayRef {
        let leaf = Arc::new(Int32Array::from(vec![
            Some(0),
            None,
            Some(2),
            Some(3),
            None,
            Some(5),
            Some(6),
            Some(7),
        ])) as ArrayRef;
        let inner = Arc::new(
            LargeListArray::try_new(
                Arc::new(ArrowField::new("item", DataType::Int32, true)),
                OffsetBuffer::new(ScalarBuffer::from(vec![0_i64, 2, 2, 3, 5, 5, 8])),
                leaf,
                Some(null_buffer([true, false, true, true, true, true])),
            )
            .unwrap(),
        ) as ArrayRef;
        let struct_fields = Fields::from(vec![ArrowField::new(
            "inner",
            inner.data_type().clone(),
            true,
        )]);
        let structs = Arc::new(StructArray::new(
            struct_fields.clone(),
            vec![inner],
            Some(null_buffer([true, true, false, true, true, true])),
        )) as ArrayRef;
        Arc::new(
            ListArray::try_new(
                Arc::new(ArrowField::new(
                    "item",
                    DataType::Struct(struct_fields),
                    true,
                )),
                OffsetBuffer::new(ScalarBuffer::from(vec![0_i32, 2, 2, 2, 4, 6])),
                structs,
                Some(null_buffer([true, false, true, true, true])),
            )
            .unwrap(),
        )
    }

    fn page_layout(page: &EncodedPage) -> &pb21::page_layout::Layout {
        let PageEncoding::Structural(layout) = &page.description else {
            panic!("expected structural page encoding");
        };
        layout.layout.as_ref().expect("page layout must be present")
    }

    fn create_encoder(
        array: &ArrayRef,
        version: TestEncoding,
        metadata: HashMap<String, String>,
    ) -> Result<Box<dyn FieldEncoder>> {
        let arrow_field =
            ArrowField::new("values", array.data_type().clone(), true).with_metadata(metadata);
        let field = Field::try_from(&arrow_field)?;
        let strategy = test_encoding_strategy(version);
        let options = EncodingOptions {
            cache_bytes_per_column: 1,
            ..Default::default()
        };
        crate::testing::create_test_field_encoder(
            strategy.as_ref(),
            &field,
            &mut ColumnIndexSequence::default(),
            &options,
        )
    }

    async fn encode_pages(
        array: ArrayRef,
        version: TestEncoding,
        metadata: HashMap<String, String>,
    ) -> Result<Vec<EncodedPage>> {
        encode_chunks(vec![array], version, metadata).await
    }

    async fn encode_chunks(
        arrays: Vec<ArrayRef>,
        version: TestEncoding,
        metadata: HashMap<String, String>,
    ) -> Result<Vec<EncodedPage>> {
        let first = arrays
            .first()
            .ok_or_else(|| Error::internal("test input has no arrays".to_string()))?;
        let mut encoder = create_encoder(first, version, metadata)?;
        let mut external_buffers = OutOfLineBuffers::new(0, MIN_PAGE_BUFFER_ALIGNMENT);
        let mut pages = Vec::new();
        let mut row_number = 0_u64;
        for array in arrays {
            let num_rows = array.len() as u64;
            for task in encoder.maybe_encode(
                array,
                &mut external_buffers,
                crate::repdef::RepDefBuilder::default(),
                row_number,
                num_rows,
            )? {
                pages.push(task.await?);
            }
            row_number += num_rows;
        }
        for task in encoder.flush(&mut external_buffers)? {
            pages.push(task.await?);
        }
        for column in encoder.finish(&mut external_buffers).await? {
            pages.extend(column.final_pages);
        }
        Ok(pages)
    }

    fn sparse_layout(page: &EncodedPage) -> &pb21::SparseLayout {
        let pb21::page_layout::Layout::SparseLayout(sparse) = page_layout(page) else {
            panic!("expected SparseLayout, got {:?}", page_layout(page));
        };
        sparse
    }

    fn list_layer(sparse: &pb21::SparseLayout) -> &pb21::SparseListLayer {
        sparse
            .structural_layers
            .iter()
            .find_map(|layer| match layer.layer.as_ref() {
                Some(pb21::sparse_structural_layer::Layer::List(layer)) => Some(layer),
                _ => None,
            })
            .expect("expected sparse list layer")
    }

    fn validity_layer(layer: &pb21::SparseStructuralLayer) -> Option<&pb21::SparseValidityLayer> {
        match layer.layer.as_ref() {
            Some(pb21::sparse_structural_layer::Layer::Validity(layer)) => Some(layer),
            _ => None,
        }
    }

    fn layer_num_slots(layer: &pb21::SparseStructuralLayer) -> u64 {
        match layer.layer.as_ref().expect("expected sparse layer variant") {
            pb21::sparse_structural_layer::Layer::Validity(layer) => layer.num_slots,
            pb21::sparse_structural_layer::Layer::List(layer) => layer.num_slots,
            pb21::sparse_structural_layer::Layer::FixedSizeList(layer) => layer.num_slots,
        }
    }

    fn fixed_size_list_dimension(layer: &pb21::SparseStructuralLayer) -> Option<u64> {
        match layer.layer.as_ref() {
            Some(pb21::sparse_structural_layer::Layer::FixedSizeList(layer)) => {
                Some(layer.dimension)
            }
            _ => None,
        }
    }

    #[test]
    fn test_sparse_value_block_eligibility() {
        let fixed = DataBlock::from_array(Int32Array::from_iter_values(0..4));
        assert!(supports_value_block(&fixed));

        let variable = DataBlock::from_array(StringArray::from(vec!["a", "b"]));
        assert!(supports_value_block(&variable));

        let nullable = DataBlock::from_array(Int32Array::from(vec![Some(1), None]));
        assert!(!supports_value_block(&nullable));

        let all_null = DataBlock::from_array(Int32Array::from(vec![None, None]));
        assert!(!supports_value_block(&all_null));

        let (dictionary, _) = dictionary_values(4);
        assert!(!supports_value_block(&DataBlock::from_arrays(
            &[dictionary],
            4
        )));

        let fixed_fields = Fields::from(vec![ArrowField::new("value", DataType::Int32, false)]);
        let fixed_struct = StructArray::new(
            fixed_fields,
            vec![Arc::new(Int32Array::from_iter_values(0..4))],
            None,
        );
        assert!(supports_value_block(&DataBlock::from_array(fixed_struct)));

        let variable_fields = Fields::from(vec![ArrowField::new("value", DataType::Utf8, false)]);
        let variable_struct = StructArray::new(
            variable_fields,
            vec![Arc::new(StringArray::from(vec!["a", "b"]))],
            None,
        );
        assert!(!supports_value_block(&DataBlock::from_array(
            variable_struct
        )));

        let fixed_size_list = FixedSizeListArray::try_new(
            Arc::new(ArrowField::new("item", DataType::Int32, false)),
            2,
            Arc::new(Int32Array::from_iter_values(0..4)),
            None,
        )
        .unwrap();
        assert!(supports_value_block(&DataBlock::from_array(
            fixed_size_list
        )));

        let unsupported_fixed_size_list = DataBlock::FixedSizeList(FixedSizeListBlock {
            child: Box::new(DataBlock::from_array(StringArray::from(vec![
                "a", "b", "c", "d",
            ]))),
            dimension: 2,
        });
        assert!(!supports_value_block(&unsupported_fixed_size_list));
    }

    fn planned_list(
        offsets: Vec<i32>,
        list_validity: Option<Vec<bool>>,
        leaf_validity: Option<Vec<bool>>,
    ) -> SparseStructuralPlan {
        let num_values = u64::try_from(*offsets.last().unwrap()).unwrap();
        let mut builder = crate::repdef::RepDefBuilder::default();
        assert!(!builder.add_offsets(
            OffsetBuffer::new(ScalarBuffer::from(offsets)),
            list_validity.map(null_buffer),
        ));
        if let Some(leaf_validity) = leaf_validity {
            builder.add_validity_bitmap(null_buffer(leaf_validity));
        } else {
            builder.add_no_null(num_values as usize);
        }
        let normalized = crate::repdef::RepDefBuilder::normalize(vec![builder]);
        plan(&normalized, num_values).unwrap()
    }

    fn planned_list_layer(plan: &SparseStructuralPlan) -> &SparseStructuralLayerPlan {
        plan.layers
            .iter()
            .find(|layer| matches!(layer, SparseStructuralLayerPlan::List { .. }))
            .expect("expected planned list layer")
    }

    #[test]
    fn test_semantic_position_and_count_forms() {
        let empty = planned_list(vec![0, 0, 0, 0], None, None);
        assert_eq!(empty.num_items, 3);
        assert_eq!(empty.num_visible_items, 0);
        assert!(matches!(
            planned_list_layer(&empty),
            SparseStructuralLayerPlan::List {
                non_empty_positions: SparsePositionSet::Empty,
                counts: SparseCountSet::Empty,
                ..
            }
        ));

        let all = planned_list(vec![0, 2, 4, 6], None, None);
        assert_eq!(all.num_items, 6);
        assert!(matches!(
            planned_list_layer(&all),
            SparseStructuralLayerPlan::List {
                non_empty_positions: SparsePositionSet::All { len: 3 },
                counts: SparseCountSet::Constant { value: 2, len: 3 },
                ..
            }
        ));

        let range = planned_list(vec![0, 2, 4, 4, 4], None, None);
        assert_eq!(range.num_items, 6);
        assert!(matches!(
            planned_list_layer(&range),
            SparseStructuralLayerPlan::List {
                non_empty_positions: SparsePositionSet::Range { start: 0, len: 2 },
                counts: SparseCountSet::Constant { value: 2, len: 2 },
                ..
            }
        ));

        let explicit = planned_list(vec![0, 1, 1, 4, 4, 6], None, None);
        assert_eq!(explicit.num_items, 8);
        assert!(matches!(
            planned_list_layer(&explicit),
            SparseStructuralLayerPlan::List {
                non_empty_positions: SparsePositionSet::Explicit(positions),
                counts: SparseCountSet::Explicit { counts, .. },
                ..
            } if positions == &vec![0, 2, 4] && counts.as_ref() == [1, 3, 2]
        ));
    }

    #[test]
    fn test_validity_polarity_uses_semantic_encoded_cost() {
        let mostly_valid = BooleanBuffer::from_iter([true, false, true, true, false, true]);
        let validity = validity_set(Some(&mostly_valid), mostly_valid.len(), "test").unwrap();
        assert_eq!(validity.meaning, SparseValidityMeaning::NullPositions);
        assert!(matches!(validity.positions, SparsePositionSet::Explicit(ref p) if p == &[1, 4]));

        let mostly_null = BooleanBuffer::from_iter([false, true, false, false, true, false]);
        let validity = validity_set(Some(&mostly_null), mostly_null.len(), "test").unwrap();
        assert_eq!(validity.meaning, SparseValidityMeaning::ValidPositions);
        assert!(matches!(validity.positions, SparsePositionSet::Explicit(ref p) if p == &[1, 4]));

        let valid_island = BooleanBuffer::from_iter([false, false, true, true, false]);
        let validity = validity_set(Some(&valid_island), valid_island.len(), "test").unwrap();
        assert_eq!(validity.meaning, SparseValidityMeaning::ValidPositions);
        assert!(matches!(
            validity.positions,
            SparsePositionSet::Range { start: 2, len: 2 }
        ));

        let all_valid = BooleanBuffer::from_iter([true, true, true]);
        let validity = validity_set(Some(&all_valid), all_valid.len(), "test").unwrap();
        assert_eq!(validity.meaning, SparseValidityMeaning::NullPositions);
        assert!(matches!(validity.positions, SparsePositionSet::Empty));

        let all_null = BooleanBuffer::from_iter([false, false, false]);
        let validity = validity_set(Some(&all_null), all_null.len(), "test").unwrap();
        assert_eq!(validity.meaning, SparseValidityMeaning::ValidPositions);
        assert!(matches!(validity.positions, SparsePositionSet::Empty));
    }

    #[tokio::test]
    async fn test_explicit_sparse_nullable_primitive_roundtrip() {
        let array = Arc::new(Int32Array::from(vec![
            Some(10),
            None,
            Some(20),
            Some(30),
            None,
            Some(40),
        ])) as ArrayRef;
        let pages = encode_pages(
            array.clone(),
            TestEncoding::StructuralSparse,
            sparse_metadata(),
        )
        .await
        .unwrap();
        assert_eq!(pages.len(), 1);
        let sparse = sparse_layout(&pages[0]);
        assert_eq!(sparse.num_items, 6);
        assert_eq!(sparse.num_visible_items, 6);
        assert_eq!(sparse.structural_layers.len(), 1);
        let validity = validity_layer(&sparse.structural_layers[0])
            .unwrap()
            .validity
            .as_ref()
            .unwrap();
        assert_eq!(
            validity.meaning,
            pb21::sparse_validity_set::Meaning::SparseValidityNullPositions as i32
        );
        assert!(matches!(
            validity.positions.as_ref().unwrap().positions,
            Some(pb21::sparse_position_set::Positions::Explicit(_))
        ));

        let cases = TestCases::default()
            .with_encoding(TestEncoding::StructuralSparse)
            .with_page_sizes(vec![1])
            .with_range(1..5)
            .with_indices(vec![0, 2, 5]);
        check_round_trip_encoding_of_data(vec![array], &cases, sparse_metadata()).await;
    }

    #[tokio::test]
    async fn test_explicit_sparse_nullable_struct_roundtrip() {
        let array = nullable_struct();
        let pages = encode_pages(
            array.clone(),
            TestEncoding::StructuralSparse,
            sparse_metadata(),
        )
        .await
        .unwrap();
        assert_eq!(pages.len(), 1);
        let sparse = sparse_layout(&pages[0]);
        assert_eq!(sparse.structural_layers.len(), 2);
        assert!(
            sparse
                .structural_layers
                .iter()
                .all(|layer| validity_layer(layer).is_some())
        );
        let struct_validity = validity_layer(&sparse.structural_layers[0])
            .unwrap()
            .validity
            .as_ref()
            .unwrap();
        assert_eq!(
            struct_validity.meaning,
            pb21::sparse_validity_set::Meaning::SparseValidityNullPositions as i32
        );
        assert!(matches!(
            struct_validity.positions.as_ref().unwrap().positions,
            Some(pb21::sparse_position_set::Positions::Explicit(_))
        ));

        let cases = TestCases::default()
            .with_encoding(TestEncoding::StructuralSparse)
            .with_page_sizes(vec![1])
            .with_range(1..5)
            .with_indices(vec![0, 2, 4]);
        check_round_trip_encoding_of_data(vec![array], &cases, sparse_metadata()).await;
    }

    #[tokio::test]
    async fn test_explicit_sparse_struct_with_constant_and_sparse_children() {
        let fields = Fields::from(vec![
            ArrowField::new("constant", DataType::Int32, true),
            ArrowField::new("sparse", DataType::Int32, true),
        ]);
        let array = Arc::new(StructArray::new(
            fields,
            vec![
                Arc::new(Int32Array::from(vec![None::<i32>; 5])),
                Arc::new(Int32Array::from(vec![
                    Some(10),
                    Some(20),
                    None,
                    Some(40),
                    Some(50),
                ])),
            ],
            Some(null_buffer([true, false, true, true, false])),
        )) as ArrayRef;
        let pages = encode_pages(
            array.clone(),
            TestEncoding::StructuralSparse,
            sparse_metadata(),
        )
        .await
        .unwrap();
        assert!(pages.iter().any(|page| matches!(
            page_layout(page),
            pb21::page_layout::Layout::ConstantLayout(_)
        )));
        assert!(pages.iter().any(|page| matches!(
            page_layout(page),
            pb21::page_layout::Layout::SparseLayout(_)
        )));

        let cases = TestCases::default()
            .with_encoding(TestEncoding::StructuralSparse)
            .with_page_sizes(vec![1])
            .with_range(1..5)
            .with_indices(vec![0, 2, 4]);
        check_round_trip_encoding_of_data(vec![array], &cases, sparse_metadata()).await;
    }

    #[tokio::test]
    async fn test_explicit_sparse_emits_both_validity_polarities() {
        let mostly_valid = Arc::new(Int32Array::from(vec![
            Some(0),
            None,
            Some(2),
            Some(3),
            None,
            Some(5),
        ])) as ArrayRef;
        let mostly_null = Arc::new(Int32Array::from(vec![
            None,
            Some(1),
            None,
            None,
            Some(4),
            None,
        ])) as ArrayRef;

        let null_positions = encode_pages(
            mostly_valid.clone(),
            TestEncoding::StructuralSparse,
            sparse_metadata(),
        )
        .await
        .unwrap();
        let validity = validity_layer(&sparse_layout(&null_positions[0]).structural_layers[0])
            .unwrap()
            .validity
            .as_ref()
            .unwrap();
        assert_eq!(
            validity.meaning,
            pb21::sparse_validity_set::Meaning::SparseValidityNullPositions as i32
        );

        let valid_positions = encode_pages(
            mostly_null.clone(),
            TestEncoding::StructuralSparse,
            sparse_metadata(),
        )
        .await
        .unwrap();
        let validity = validity_layer(&sparse_layout(&valid_positions[0]).structural_layers[0])
            .unwrap()
            .validity
            .as_ref()
            .unwrap();
        assert_eq!(
            validity.meaning,
            pb21::sparse_validity_set::Meaning::SparseValidityValidPositions as i32
        );

        let cases = TestCases::default()
            .with_encoding(TestEncoding::StructuralSparse)
            .with_page_sizes(vec![1])
            .with_range(1..5)
            .with_indices(vec![0, 2, 5]);
        for array in [mostly_valid, mostly_null] {
            check_round_trip_encoding_of_data(vec![array], &cases, sparse_metadata()).await;
        }
    }

    #[tokio::test]
    async fn test_explicit_sparse_nested_page_boundaries_range_and_take() {
        let nested = deeply_nested();
        let chunks = vec![nested.slice(0, 2), nested.slice(2, 3)];
        let pages = encode_chunks(
            chunks.clone(),
            TestEncoding::StructuralSparse,
            sparse_metadata(),
        )
        .await
        .unwrap();
        assert!(pages.len() >= 2, "expected multiple sparse pages");
        let sparse = pages
            .iter()
            .map(sparse_layout)
            .collect::<Vec<&pb21::SparseLayout>>();
        assert!(sparse.iter().any(|layout| {
            layout
                .structural_layers
                .iter()
                .filter(|layer| {
                    matches!(
                        layer.layer.as_ref(),
                        Some(pb21::sparse_structural_layer::Layer::List(_))
                    )
                })
                .count()
                >= 2
        }));
        assert!(sparse.iter().any(|layout| {
            layout
                .structural_layers
                .iter()
                .any(|layer| validity_layer(layer).is_some())
        }));

        let cases = TestCases::default()
            .with_encoding(TestEncoding::StructuralSparse)
            .with_page_sizes(vec![1])
            .with_batch_size(2)
            .with_range(1..5)
            .with_range(2..4)
            .with_indices(vec![0, 2, 4])
            .with_indices(vec![1, 3]);
        check_round_trip_encoding_of_data(chunks, &cases, sparse_metadata()).await;
    }

    #[tokio::test]
    async fn test_explicit_sparse_list_and_large_list_null_empty_roundtrip() {
        let list = list_i32(
            vec![0, 0, 0, 2, 3, 3],
            Some(vec![false, true, true, true, true]),
        );
        let large_list = large_list_i32(
            vec![0, 0, 0, 2, 3, 3],
            Some(vec![false, true, true, true, true]),
        );
        let cases = TestCases::default()
            .with_encoding(TestEncoding::StructuralSparse)
            .with_page_sizes(vec![1])
            .with_range(0..5)
            .with_range(1..4)
            .with_indices(vec![0, 1, 4])
            .with_indices(vec![2, 3]);

        for array in [list, large_list] {
            let pages = encode_pages(
                array.clone(),
                TestEncoding::StructuralSparse,
                sparse_metadata(),
            )
            .await
            .unwrap();
            assert!(pages.iter().map(sparse_layout).all(|layout| {
                layout.structural_layers.iter().any(|layer| {
                    matches!(
                        layer.layer.as_ref(),
                        Some(pb21::sparse_structural_layer::Layer::List(_))
                    )
                })
            }));
            check_round_trip_encoding_of_data(vec![array], &cases, sparse_metadata()).await;
        }
    }

    #[tokio::test]
    async fn test_explicit_sparse_map_and_fixed_size_list_roundtrip() {
        let map = map_i32();
        let map_pages = encode_pages(
            map.clone(),
            TestEncoding::StructuralSparse,
            sparse_metadata(),
        )
        .await
        .unwrap();
        assert!(map_pages.iter().map(sparse_layout).any(|layout| {
            layout.structural_layers.iter().any(|layer| {
                matches!(
                    layer.layer.as_ref(),
                    Some(pb21::sparse_structural_layer::Layer::List(_))
                )
            })
        }));
        let map_cases = TestCases::default()
            .with_encoding(TestEncoding::StructuralSparse)
            .with_page_sizes(vec![1])
            .with_range(1..4)
            .with_indices(vec![0, 2, 3]);
        check_round_trip_encoding_of_data(vec![map], &map_cases, sparse_metadata()).await;

        let fsl = fixed_size_list_struct();
        let fsl_pages = encode_pages(
            fsl.clone(),
            TestEncoding::StructuralSparse,
            sparse_metadata(),
        )
        .await
        .unwrap();
        for page in &fsl_pages {
            let layout = sparse_layout(page);
            let outer_slots = layer_num_slots(layout.structural_layers.first().unwrap());
            let fixed_size_scale = layout
                .structural_layers
                .iter()
                .filter_map(fixed_size_list_dimension)
                .product::<u64>();
            assert_eq!(page.num_rows, outer_slots * fixed_size_scale);
        }
        assert!(fsl_pages.iter().map(sparse_layout).any(|layout| {
            layout
                .structural_layers
                .iter()
                .any(|layer| fixed_size_list_dimension(layer) == Some(2))
        }));
        let fsl_cases = TestCases::default()
            .with_encoding(TestEncoding::StructuralSparse)
            .with_page_sizes(vec![1])
            .with_range(1..6)
            .with_indices(vec![0, 3, 5]);
        check_round_trip_encoding_of_data(vec![fsl], &fsl_cases, sparse_metadata()).await;
    }

    #[tokio::test]
    async fn test_explicit_sparse_list_fixed_size_list_struct_roundtrip() {
        let array = list_fixed_size_list_struct();
        let pages = encode_pages(
            array.clone(),
            TestEncoding::StructuralSparse,
            sparse_metadata(),
        )
        .await
        .unwrap();
        assert!(pages.iter().map(sparse_layout).any(|layout| {
            let kinds = layout
                .structural_layers
                .iter()
                .map(|layer| match layer.layer.as_ref().unwrap() {
                    pb21::sparse_structural_layer::Layer::Validity(_) => "validity",
                    pb21::sparse_structural_layer::Layer::List(_) => "list",
                    pb21::sparse_structural_layer::Layer::FixedSizeList(_) => "fixed-size-list",
                })
                .collect::<Vec<_>>();
            kinds.starts_with(&["list", "fixed-size-list"])
        }));
        for page in &pages {
            let layout = sparse_layout(page);
            let outer_slots = layer_num_slots(layout.structural_layers.first().unwrap());
            assert_eq!(page.num_rows, outer_slots * 2);
        }

        let cases = TestCases::default()
            .with_encoding(TestEncoding::StructuralSparse)
            .with_page_sizes(vec![1])
            .with_range(1..4)
            .with_indices(vec![0, 2, 3])
            .with_indices(vec![1, 3]);
        check_round_trip_encoding_of_data(vec![array], &cases, sparse_metadata()).await;
    }

    #[tokio::test]
    async fn test_explicit_sparse_serializes_semantic_list_forms() {
        let all_array = list_i32(vec![0, 2, 4, 6], None);
        let all = encode_pages(
            all_array.clone(),
            TestEncoding::StructuralSparse,
            sparse_metadata(),
        )
        .await
        .unwrap();
        let all_layer = list_layer(sparse_layout(&all[0]));
        assert!(matches!(
            all_layer.non_empty_positions.as_ref().unwrap().positions,
            Some(pb21::sparse_position_set::Positions::All(_))
        ));
        assert!(matches!(
            all_layer.counts.as_ref().unwrap().counts,
            Some(pb21::sparse_count_set::Counts::Constant(
                pb21::SparseCountConstant { value: 2 }
            ))
        ));
        assert_eq!(all[0].data.len(), 2);

        let range_array = list_i32(vec![0, 2, 4, 4, 4], None);
        let range = encode_pages(
            range_array.clone(),
            TestEncoding::StructuralSparse,
            sparse_metadata(),
        )
        .await
        .unwrap();
        let range_layer = list_layer(sparse_layout(&range[0]));
        assert!(matches!(
            range_layer.non_empty_positions.as_ref().unwrap().positions,
            Some(pb21::sparse_position_set::Positions::Range(
                pb21::SparsePositionRange {
                    start: 0,
                    length: 2
                }
            ))
        ));
        assert!(matches!(
            range_layer.counts.as_ref().unwrap().counts,
            Some(pb21::sparse_count_set::Counts::Constant(
                pb21::SparseCountConstant { value: 2 }
            ))
        ));
        assert_eq!(range[0].data.len(), 2);

        let explicit_array = list_i32(vec![0, 1, 1, 4, 4, 6], None);
        let explicit = encode_pages(
            explicit_array.clone(),
            TestEncoding::StructuralSparse,
            sparse_metadata(),
        )
        .await
        .unwrap();
        let explicit_layer = list_layer(sparse_layout(&explicit[0]));
        assert!(matches!(
            explicit_layer
                .non_empty_positions
                .as_ref()
                .unwrap()
                .positions,
            Some(pb21::sparse_position_set::Positions::Explicit(_))
        ));
        assert!(matches!(
            explicit_layer.counts.as_ref().unwrap().counts,
            Some(pb21::sparse_count_set::Counts::Explicit(_))
        ));
        assert_eq!(explicit[0].data.len(), 4);

        let cases = TestCases::default()
            .with_encoding(TestEncoding::StructuralSparse)
            .with_page_sizes(vec![1])
            .with_range(1..3)
            .with_indices(vec![0, 2]);
        for array in [all_array, range_array, explicit_array] {
            check_round_trip_encoding_of_data(vec![array], &cases, sparse_metadata()).await;
        }
    }

    #[tokio::test]
    async fn test_constant_layout_boundary_is_explicit() {
        let structural_only = encode_pages(
            list_i32(vec![0, 0, 0, 0], None),
            TestEncoding::StructuralSparse,
            sparse_metadata(),
        )
        .await
        .unwrap();
        assert!(matches!(
            page_layout(&structural_only[0]),
            pb21::page_layout::Layout::ConstantLayout(_)
        ));

        let empty_struct = Arc::new(StructArray::new_empty_fields(3, None)) as ArrayRef;
        let empty_struct_pages = encode_pages(
            empty_struct,
            TestEncoding::StructuralSparse,
            sparse_metadata(),
        )
        .await
        .unwrap();
        assert!(matches!(
            page_layout(&empty_struct_pages[0]),
            pb21::page_layout::Layout::ConstantLayout(_)
        ));

        let all_null = Arc::new(Int32Array::from(vec![None, None, None])) as ArrayRef;
        let all_null_pages =
            encode_pages(all_null, TestEncoding::StructuralSparse, sparse_metadata())
                .await
                .unwrap();
        assert!(matches!(
            page_layout(&all_null_pages[0]),
            pb21::page_layout::Layout::ConstantLayout(_)
        ));

        let constant = Arc::new(Int32Array::from(vec![7, 7, 7])) as ArrayRef;
        let constant_pages =
            encode_pages(constant, TestEncoding::StructuralSparse, sparse_metadata())
                .await
                .unwrap();
        assert!(matches!(
            page_layout(&constant_pages[0]),
            pb21::page_layout::Layout::SparseLayout(_)
        ));
    }

    #[tokio::test]
    async fn test_default_and_explicit_dense_layouts_are_unchanged() {
        let array = Arc::new(Int32Array::from_iter_values(0..16)) as ArrayRef;
        let v2_2_default = encode_pages(array.clone(), TestEncoding::StructuralU32, HashMap::new())
            .await
            .unwrap();
        let v2_2_miniblock = encode_pages(
            array.clone(),
            TestEncoding::StructuralU32,
            structural_metadata(STRUCTURAL_ENCODING_MINIBLOCK),
        )
        .await
        .unwrap();
        assert_eq!(v2_2_default.len(), v2_2_miniblock.len());
        for (default, explicit) in v2_2_default.iter().zip(v2_2_miniblock.iter()) {
            assert!(matches!(
                page_layout(default),
                pb21::page_layout::Layout::MiniBlockLayout(_)
            ));
            assert_eq!(page_layout(default), page_layout(explicit));
            assert_eq!(default.data, explicit.data);
        }

        let v2_3_default = encode_pages(
            array.clone(),
            TestEncoding::StructuralSparse,
            HashMap::new(),
        )
        .await
        .unwrap();
        assert!(matches!(
            page_layout(&v2_3_default[0]),
            pb21::page_layout::Layout::MiniBlockLayout(_)
        ));

        let v2_3_miniblock = encode_pages(
            array.clone(),
            TestEncoding::StructuralSparse,
            structural_metadata(STRUCTURAL_ENCODING_MINIBLOCK),
        )
        .await
        .unwrap();
        assert!(matches!(
            page_layout(&v2_3_miniblock[0]),
            pb21::page_layout::Layout::MiniBlockLayout(_)
        ));

        let v2_3_fullzip = encode_pages(
            array.clone(),
            TestEncoding::StructuralSparse,
            structural_metadata(STRUCTURAL_ENCODING_FULLZIP),
        )
        .await
        .unwrap();
        assert!(matches!(
            page_layout(&v2_3_fullzip[0]),
            pb21::page_layout::Layout::FullZipLayout(_)
        ));

        let v2_3_sparse = encode_pages(array, TestEncoding::StructuralSparse, sparse_metadata())
            .await
            .unwrap();
        assert!(matches!(
            page_layout(&v2_3_sparse[0]),
            pb21::page_layout::Layout::SparseLayout(_)
        ));
    }

    #[tokio::test]
    async fn test_auto_sparse_for_split_required_page() {
        const NUM_ROWS: usize = 70_000;
        let array = sparse_i32_list(NUM_ROWS, 2_000);

        let within_budget = encode_pages(
            sparse_i32_list(4_096, 1_024),
            TestEncoding::StructuralSparse,
            HashMap::new(),
        )
        .await
        .unwrap();
        assert_eq!(within_budget.len(), 1);
        assert!(matches!(
            page_layout(&within_budget[0]),
            pb21::page_layout::Layout::MiniBlockLayout(_)
        ));

        let automatic = encode_pages(
            array.clone(),
            TestEncoding::StructuralSparse,
            HashMap::new(),
        )
        .await
        .unwrap();
        assert_eq!(automatic.len(), 1);
        assert!(matches!(
            page_layout(&automatic[0]),
            pb21::page_layout::Layout::SparseLayout(_)
        ));

        let miniblock = encode_pages(
            array.clone(),
            TestEncoding::StructuralSparse,
            structural_metadata(STRUCTURAL_ENCODING_MINIBLOCK),
        )
        .await
        .unwrap();
        assert!(miniblock.len() > 1);
        assert!(miniblock.iter().all(|page| matches!(
            page_layout(page),
            pb21::page_layout::Layout::MiniBlockLayout(_)
        )));

        let fullzip = encode_pages(
            array.clone(),
            TestEncoding::StructuralSparse,
            structural_metadata(STRUCTURAL_ENCODING_FULLZIP),
        )
        .await
        .unwrap();
        assert_eq!(fullzip.len(), miniblock.len());
        assert!(fullzip.iter().all(|page| matches!(
            page_layout(page),
            pb21::page_layout::Layout::FullZipLayout(_)
        )));

        let sparse = encode_pages(
            array.clone(),
            TestEncoding::StructuralSparse,
            sparse_metadata(),
        )
        .await
        .unwrap();
        assert_eq!(sparse.len(), 1);
        assert!(matches!(
            page_layout(&sparse[0]),
            pb21::page_layout::Layout::SparseLayout(_)
        ));

        let v2_2_default = encode_pages(array.clone(), TestEncoding::StructuralU32, HashMap::new())
            .await
            .unwrap();
        let v2_2_miniblock = encode_pages(
            array.clone(),
            TestEncoding::StructuralU32,
            structural_metadata(STRUCTURAL_ENCODING_MINIBLOCK),
        )
        .await
        .unwrap();
        assert_eq!(v2_2_default.len(), v2_2_miniblock.len());
        for (default, explicit) in v2_2_default.iter().zip(v2_2_miniblock.iter()) {
            assert_eq!(page_layout(default), page_layout(explicit));
            assert_eq!(default.data, explicit.data);
        }

        let cases = TestCases::default()
            .with_encoding(TestEncoding::StructuralSparse)
            .with_page_sizes(vec![1024 * 1024])
            .with_batch_size(NUM_ROWS as u32)
            .with_range(1_999..2_002)
            .with_indices(vec![0, 1_999, 2_000, NUM_ROWS as u64 - 1]);
        check_round_trip_encoding_of_data(vec![array], &cases, HashMap::new()).await;
    }

    #[tokio::test]
    async fn test_auto_sparse_for_single_row_over_budget() {
        let array = unsplittable_nested_list(
            Arc::new(Int32Array::from(vec![42, 43])),
            Arc::new(ArrowField::new("item", DataType::Int32, true)),
        );

        let automatic = encode_pages(
            array.clone(),
            TestEncoding::StructuralSparse,
            HashMap::new(),
        )
        .await
        .unwrap();
        assert_eq!(automatic.len(), 1);
        assert!(matches!(
            page_layout(&automatic[0]),
            pb21::page_layout::Layout::SparseLayout(_)
        ));

        let v2_2 = encode_pages(array.clone(), TestEncoding::StructuralU32, HashMap::new())
            .await
            .unwrap();
        assert_eq!(v2_2.len(), 1);
        assert!(matches!(
            page_layout(&v2_2[0]),
            pb21::page_layout::Layout::FullZipLayout(_)
        ));

        let miniblock_error = encode_pages(
            array.clone(),
            TestEncoding::StructuralSparse,
            structural_metadata(STRUCTURAL_ENCODING_MINIBLOCK),
        )
        .await
        .unwrap_err();
        assert!(
            miniblock_error
                .to_string()
                .contains("Mini-block cannot encode 70000 rep/def levels")
        );

        let fullzip = encode_pages(
            array.clone(),
            TestEncoding::StructuralSparse,
            structural_metadata(STRUCTURAL_ENCODING_FULLZIP),
        )
        .await
        .unwrap();
        assert!(matches!(
            page_layout(&fullzip[0]),
            pb21::page_layout::Layout::FullZipLayout(_)
        ));

        let explicit_sparse = encode_pages(
            array.clone(),
            TestEncoding::StructuralSparse,
            sparse_metadata(),
        )
        .await
        .unwrap();
        assert!(matches!(
            page_layout(&explicit_sparse[0]),
            pb21::page_layout::Layout::SparseLayout(_)
        ));

        let cases = TestCases::default()
            .with_encoding(TestEncoding::StructuralSparse)
            .with_range(0..1)
            .with_indices(vec![0]);
        check_round_trip_encoding_of_data(vec![array], &cases, HashMap::new()).await;
    }

    #[tokio::test]
    async fn test_auto_sparse_keeps_unsupported_values_dense() {
        const NUM_ROWS: usize = 70_000;
        let num_values = NUM_ROWS.div_ceil(2_000);

        let (dictionary, dictionary_field) = dictionary_values(num_values);
        let dictionary_array = sparse_list_values(NUM_ROWS, 2_000, dictionary, dictionary_field);
        let dictionary_pages = encode_pages(
            dictionary_array.clone(),
            TestEncoding::StructuralSparse,
            HashMap::new(),
        )
        .await
        .unwrap();
        assert!(dictionary_pages.len() > 1);
        assert!(dictionary_pages.iter().all(|page| matches!(
            page_layout(page),
            pb21::page_layout::Layout::MiniBlockLayout(_)
        )));

        let (packed_values, packed_field) = variable_packed_struct_values(num_values);
        let packed_array = sparse_list_values(NUM_ROWS, 2_000, packed_values, packed_field);
        let packed_pages = encode_pages(
            packed_array.clone(),
            TestEncoding::StructuralSparse,
            HashMap::new(),
        )
        .await
        .unwrap();
        assert!(packed_pages.len() > 1);
        assert!(packed_pages.iter().all(|page| matches!(
            page_layout(page),
            pb21::page_layout::Layout::FullZipLayout(_)
        )));

        let (packed_values, packed_field) = variable_packed_struct_values(2);
        let unsplittable_packed = unsplittable_nested_list(packed_values, packed_field);
        let packed_fallback = encode_pages(
            unsplittable_packed.clone(),
            TestEncoding::StructuralSparse,
            HashMap::new(),
        )
        .await
        .unwrap();
        assert_eq!(packed_fallback.len(), 1);
        assert!(matches!(
            page_layout(&packed_fallback[0]),
            pb21::page_layout::Layout::FullZipLayout(_)
        ));

        let (dictionary, dictionary_field) = dictionary_values(2);
        let unsplittable_dictionary = unsplittable_nested_list(dictionary, dictionary_field);
        let v2_2_error = encode_pages(
            unsplittable_dictionary.clone(),
            TestEncoding::StructuralU32,
            HashMap::new(),
        )
        .await
        .unwrap_err();
        let v2_3_error = encode_pages(
            unsplittable_dictionary,
            TestEncoding::StructuralSparse,
            HashMap::new(),
        )
        .await
        .unwrap_err();
        assert_eq!(v2_3_error.to_string(), v2_2_error.to_string());
        assert!(
            v2_3_error
                .to_string()
                .contains("Mini-block cannot encode 70000 rep/def levels")
        );

        let cases = TestCases::default()
            .with_encoding(TestEncoding::StructuralSparse)
            .with_page_sizes(vec![1024 * 1024])
            .with_batch_size(NUM_ROWS as u32)
            .with_range(1_999..2_002)
            .with_indices(vec![0, 2_000, NUM_ROWS as u64 - 1]);
        check_round_trip_encoding_of_data(vec![dictionary_array], &cases, HashMap::new()).await;
        let packed_cases = TestCases::default()
            .with_encoding(TestEncoding::StructuralSparse)
            .with_page_sizes(vec![1024 * 1024])
            .with_batch_size(NUM_ROWS as u32);
        check_round_trip_encoding_of_data(vec![packed_array], &packed_cases, HashMap::new()).await;

        let single_row_cases = TestCases::default()
            .with_encoding(TestEncoding::StructuralSparse)
            .with_page_sizes(vec![1024 * 1024])
            .with_range(0..1)
            .with_indices(vec![0]);
        check_round_trip_encoding_of_data(
            vec![unsplittable_packed],
            &single_row_cases,
            HashMap::new(),
        )
        .await;
    }

    #[tokio::test]
    async fn test_auto_sparse_wide_values_keep_dense_fallback() {
        let first = vec![0xAB_u8; 5_000];
        let second = vec![0xCD_u8; 5_000];
        let fixed_size_binary = Arc::new(
            FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                [Some(first.as_slice()), Some(second.as_slice())].into_iter(),
                5_000,
            )
            .unwrap(),
        ) as ArrayRef;

        const FSL_DIMENSION: i32 = 2_048;
        let fixed_size_list = Arc::new(
            FixedSizeListArray::try_new(
                Arc::new(ArrowField::new("item", DataType::Int32, true)),
                FSL_DIMENSION,
                Arc::new(Int32Array::from_iter_values(0..(FSL_DIMENSION * 2))),
                None,
            )
            .unwrap(),
        ) as ArrayRef;

        for (label, values) in [
            ("fixed-size binary", fixed_size_binary),
            ("fixed-size list", fixed_size_list),
        ] {
            let item_field = Arc::new(ArrowField::new("item", values.data_type().clone(), true));
            let array = unsplittable_nested_list(values, item_field);

            let v2_2 = encode_pages(array.clone(), TestEncoding::StructuralU32, HashMap::new())
                .await
                .unwrap();
            let v2_3 = encode_pages(
                array.clone(),
                TestEncoding::StructuralSparse,
                HashMap::new(),
            )
            .await
            .unwrap();
            for pages in [&v2_2, &v2_3] {
                assert_eq!(pages.len(), 1, "unexpected {label} page count");
                assert!(
                    matches!(
                        page_layout(&pages[0]),
                        pb21::page_layout::Layout::FullZipLayout(_)
                    ),
                    "{label} should retain the dense full-zip fallback"
                );
            }

            let explicit_error = encode_pages(
                array.clone(),
                TestEncoding::StructuralSparse,
                sparse_metadata(),
            )
            .await
            .unwrap_err();
            assert!(
                explicit_error.to_string().contains("too wide"),
                "explicit sparse should preserve the {label} value error: {explicit_error}"
            );

            let cases = TestCases::default()
                .with_u32_structural_encodings()
                .with_range(0..1)
                .with_indices(vec![0]);
            check_round_trip_encoding_of_data(vec![array], &cases, HashMap::new()).await;
        }
    }

    #[test]
    fn test_explicit_sparse_rejects_lance_2_2() {
        let array = Arc::new(Int32Array::from(vec![Some(1), None])) as ArrayRef;
        let Err(error) = create_encoder(&array, TestEncoding::StructuralU32, sparse_metadata())
        else {
            panic!("expected Lance 2.2 to reject explicit sparse encoding");
        };
        assert!(
            error
                .to_string()
                .contains("not enabled by the selected file format")
        );

        let structural_only = list_i32(vec![0, 0, 0], None);
        let Err(error) = create_encoder(
            &structural_only,
            TestEncoding::StructuralU32,
            sparse_metadata(),
        ) else {
            panic!("expected Lance 2.2 structural-only input to reject explicit sparse encoding");
        };
        assert!(
            error
                .to_string()
                .contains("not enabled by the selected file format")
        );
    }
}
