// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Packed encoding
//!
//! These encodings take struct data and compress it in a way that all fields are collected
//! together.
//!
//! This encoding can be transparent or opaque.  In order to be transparent we must use transparent
//! compression on all children.  Then we can zip together the compressed children.

use std::{convert::TryInto, sync::Arc};

use arrow_array::types::UInt64Type;

use lance_core::{Error, Result, datatypes::Field};

use crate::{
    buffer::LanceBuffer,
    compression::{
        CompressionStrategy, FixedPerValueDecompressor, MiniBlockDecompressor,
        VariablePerValueDecompressor,
    },
    data::{
        BlockInfo, DataBlock, DataBlockBuilder, FixedWidthDataBlock, StructDataBlock,
        VariableWidthBlock,
    },
    encodings::logical::primitive::{
        fullzip::{PerValueCompressor, PerValueDataBlock},
        miniblock::{MiniBlockCompressed, MiniBlockCompressionContext, MiniBlockCompressor},
    },
    format::{
        ProtobufUtils21,
        pb21::{CompressiveEncoding, PackedStruct, compressive_encoding::Compression},
    },
    statistics::{GetStat, Stat},
};

use super::value::{ValueDecompressor, ValueEncoder};

// Transforms a `StructDataBlock` into a row major `FixedWidthDataBlock`.
// Only fields with fixed-width fields are supported for now, and the
// assumption that all fields has `bits_per_value % 8 == 0` is made.
fn struct_data_block_to_fixed_width_data_block(
    struct_data_block: StructDataBlock,
    bits_per_values: &[u64],
) -> DataBlock {
    let data_size = struct_data_block.expect_single_stat::<UInt64Type>(Stat::DataSize);
    let mut output = Vec::with_capacity(data_size as usize);
    let num_values = struct_data_block.children[0].num_values();

    for i in 0..num_values as usize {
        for (j, child) in struct_data_block.children.iter().enumerate() {
            let bytes_per_value = (bits_per_values[j] / 8) as usize;
            let this_data = child
                .as_fixed_width_ref()
                .unwrap()
                .data
                .slice_with_length(bytes_per_value * i, bytes_per_value);
            output.extend_from_slice(&this_data);
        }
    }

    DataBlock::FixedWidth(FixedWidthDataBlock {
        bits_per_value: bits_per_values.iter().copied().sum(),
        data: LanceBuffer::from(output),
        num_values,
        block_info: BlockInfo::default(),
    })
}

#[derive(Debug, Default)]
pub struct PackedStructFixedWidthMiniBlockEncoder {}

impl MiniBlockCompressor for PackedStructFixedWidthMiniBlockEncoder {
    fn compress(
        &self,
        context: MiniBlockCompressionContext,
        data: DataBlock,
    ) -> Result<(MiniBlockCompressed, CompressiveEncoding)> {
        match data {
            DataBlock::Struct(struct_data_block) => {
                let bits_per_values = struct_data_block.children.iter().map(|data_block| data_block.as_fixed_width_ref().unwrap().bits_per_value).collect::<Vec<_>>();

                // transform struct datablock to fixed-width data block.
                let data_block = struct_data_block_to_fixed_width_data_block(struct_data_block, &bits_per_values);

                // store and transformed fixed-width data block.
                let value_miniblock_compressor = Box::new(ValueEncoder::default()) as Box<dyn MiniBlockCompressor>;
                let (value_miniblock_compressed, value_array_encoding) =
                value_miniblock_compressor.compress(context, data_block)?;

                Ok((
                    value_miniblock_compressed,
                    ProtobufUtils21::packed_struct(value_array_encoding, bits_per_values),
                ))
            }
            _ => Err(Error::invalid_input_source(format!(
                "Cannot compress a data block of type {} with PackedStructFixedWidthBlockEncoder",
                data.name()
            )
            .into())),
        }
    }
}

#[derive(Debug)]
pub struct PackedStructFixedWidthMiniBlockDecompressor {
    bits_per_values: Vec<u64>,
    array_encoding: Box<dyn MiniBlockDecompressor>,
}

impl PackedStructFixedWidthMiniBlockDecompressor {
    pub fn new(description: &PackedStruct) -> Self {
        let array_encoding: Box<dyn MiniBlockDecompressor> = match description
            .values
            .as_ref()
            .unwrap()
            .compression
            .as_ref()
            .unwrap()
        {
            Compression::Flat(flat) => Box::new(ValueDecompressor::from_flat(flat)),
            _ => panic!(
                "Currently only `ArrayEncoding::Flat` is supported in packed struct encoding in Lance 2.1."
            ),
        };
        Self {
            bits_per_values: description.bits_per_value.clone(),
            array_encoding,
        }
    }
}

impl MiniBlockDecompressor for PackedStructFixedWidthMiniBlockDecompressor {
    fn decompress(&self, data: Vec<LanceBuffer>, num_values: u64) -> Result<DataBlock> {
        assert_eq!(data.len(), 1);
        let encoded_data_block = self.array_encoding.decompress(data, num_values)?;
        let DataBlock::FixedWidth(encoded_data_block) = encoded_data_block else {
            panic!("ValueDecompressor should output FixedWidth DataBlock")
        };

        let bytes_per_values = self
            .bits_per_values
            .iter()
            .map(|bits_per_value| *bits_per_value as usize / 8)
            .collect::<Vec<_>>();

        assert!(encoded_data_block.bits_per_value % 8 == 0);
        let encoded_bytes_per_row = (encoded_data_block.bits_per_value / 8) as usize;

        // use a prefix_sum vector as a helper to reconstruct to `StructDataBlock`.
        let mut prefix_sum = vec![0; self.bits_per_values.len()];
        for i in 0..(self.bits_per_values.len() - 1) {
            prefix_sum[i + 1] = prefix_sum[i] + bytes_per_values[i];
        }

        let mut children_data_block = vec![];
        for i in 0..self.bits_per_values.len() {
            let child_buf_size = bytes_per_values[i] * num_values as usize;
            let mut child_buf: Vec<u8> = Vec::with_capacity(child_buf_size);

            for j in 0..num_values as usize {
                // the start of the data at this row is `j * encoded_bytes_per_row`, and the offset for this field is `prefix_sum[i]`, this field has length `bytes_per_values[i]`.
                let this_value = encoded_data_block.data.slice_with_length(
                    prefix_sum[i] + (j * encoded_bytes_per_row),
                    bytes_per_values[i],
                );

                child_buf.extend_from_slice(&this_value);
            }

            let child = DataBlock::FixedWidth(FixedWidthDataBlock {
                data: LanceBuffer::from(child_buf),
                bits_per_value: self.bits_per_values[i],
                num_values,
                block_info: BlockInfo::default(),
            });
            children_data_block.push(child);
        }
        Ok(DataBlock::Struct(StructDataBlock {
            children: children_data_block,
            block_info: BlockInfo::default(),
            validity: None,
        }))
    }
}

#[derive(Debug)]
enum VariablePackedFieldData {
    Fixed {
        block: FixedWidthDataBlock,
    },
    Variable {
        block: VariableWidthBlock,
        bits_per_length: u64,
    },
}

impl VariablePackedFieldData {
    fn append_row_bytes(&self, row_idx: usize, output: &mut Vec<u8>) -> Result<()> {
        match self {
            Self::Fixed { block } => {
                let bits_per_value = block.bits_per_value;
                if bits_per_value % 8 != 0 {
                    return Err(Error::invalid_input(
                        "Packed struct variable encoding requires byte-aligned fixed-width children",
                    ));
                }
                let bytes_per_value = (bits_per_value / 8) as usize;
                let start = row_idx
                    .checked_mul(bytes_per_value)
                    .ok_or_else(|| Error::invalid_input("Packed struct row size overflow"))?;
                let end = start + bytes_per_value;
                let data = block.data.as_ref();
                if end > data.len() {
                    return Err(Error::invalid_input(
                        "Packed struct fixed child out of bounds",
                    ));
                }
                output.extend_from_slice(&data[start..end]);
                Ok(())
            }
            Self::Variable {
                block,
                bits_per_length,
            } => {
                if bits_per_length % 8 != 0 {
                    return Err(Error::invalid_input(
                        "Packed struct variable children must have byte-aligned length prefixes",
                    ));
                }
                let prefix_bytes = (*bits_per_length / 8) as usize;
                if !(prefix_bytes == 4 || prefix_bytes == 8) {
                    return Err(Error::invalid_input(
                        "Packed struct variable children must use 32 or 64-bit length prefixes",
                    ));
                }
                match block.bits_per_offset {
                    32 => {
                        let offsets = block.offsets.borrow_to_typed_slice::<u32>();
                        let start = offsets[row_idx] as usize;
                        let end = offsets[row_idx + 1] as usize;
                        if end > block.data.len() {
                            return Err(Error::invalid_input(
                                "Packed struct variable child offsets out of bounds",
                            ));
                        }
                        let len = (end - start) as u32;
                        if prefix_bytes != std::mem::size_of::<u32>() {
                            return Err(Error::invalid_input(
                                "Packed struct variable child length prefix mismatch",
                            ));
                        }
                        output.extend_from_slice(&len.to_le_bytes());
                        output.extend_from_slice(&block.data[start..end]);
                        Ok(())
                    }
                    64 => {
                        let offsets = block.offsets.borrow_to_typed_slice::<u64>();
                        let start = offsets[row_idx] as usize;
                        let end = offsets[row_idx + 1] as usize;
                        if end > block.data.len() {
                            return Err(Error::invalid_input(
                                "Packed struct variable child offsets out of bounds",
                            ));
                        }
                        let len = (end - start) as u64;
                        if prefix_bytes != std::mem::size_of::<u64>() {
                            return Err(Error::invalid_input(
                                "Packed struct variable child length prefix mismatch",
                            ));
                        }
                        output.extend_from_slice(&len.to_le_bytes());
                        output.extend_from_slice(&block.data[start..end]);
                        Ok(())
                    }
                    _ => Err(Error::invalid_input(
                        "Packed struct variable child must use 32 or 64-bit offsets",
                    )),
                }
            }
        }
    }
}

#[derive(Debug)]
pub struct PackedStructVariablePerValueEncoder {
    strategy: Arc<dyn CompressionStrategy>,
    fields: Vec<Field>,
}

impl PackedStructVariablePerValueEncoder {
    pub fn new(strategy: Arc<dyn CompressionStrategy>, fields: Vec<Field>) -> Self {
        Self { strategy, fields }
    }
}

impl PerValueCompressor for PackedStructVariablePerValueEncoder {
    fn compress(&self, data: DataBlock) -> Result<(PerValueDataBlock, CompressiveEncoding)> {
        let DataBlock::Struct(struct_block) = data else {
            return Err(Error::invalid_input(
                "Packed struct encoder requires Struct data block",
            ));
        };

        if struct_block.children.is_empty() {
            return Err(Error::invalid_input(
                "Packed struct encoder requires at least one child field",
            ));
        }
        if struct_block.children.len() != self.fields.len() {
            return Err(Error::invalid_input(
                "Struct field metadata does not match number of children",
            ));
        }

        let num_values = struct_block.children[0].num_values();
        for child in struct_block.children.iter() {
            if child.num_values() != num_values {
                return Err(Error::invalid_input(
                    "Packed struct children must have matching value counts",
                ));
            }
        }

        let mut field_data = Vec::with_capacity(self.fields.len());
        let mut field_metadata = Vec::with_capacity(self.fields.len());

        for (field, child_block) in self.fields.iter().zip(struct_block.children) {
            let compressor = self.strategy.create_per_value(field, &child_block)?;
            let (compressed, encoding) = compressor.compress(child_block)?;
            match compressed {
                PerValueDataBlock::Fixed(block) => {
                    field_metadata.push(ProtobufUtils21::packed_struct_field_fixed(
                        encoding,
                        block.bits_per_value,
                    ));
                    field_data.push(VariablePackedFieldData::Fixed { block });
                }
                PerValueDataBlock::Variable(block) => {
                    let bits_per_length = block.bits_per_offset as u64;
                    field_metadata.push(ProtobufUtils21::packed_struct_field_variable(
                        encoding,
                        bits_per_length,
                    ));
                    field_data.push(VariablePackedFieldData::Variable {
                        block,
                        bits_per_length,
                    });
                }
            }
        }

        let mut row_data: Vec<u8> = Vec::new();
        let mut row_offsets: Vec<u64> = Vec::with_capacity(num_values as usize + 1);
        row_offsets.push(0);
        let mut total_bytes: usize = 0;
        let mut max_row_len: usize = 0;
        for row in 0..num_values as usize {
            let start = row_data.len();
            for field in &field_data {
                field.append_row_bytes(row, &mut row_data)?;
            }
            let end = row_data.len();
            let row_len = end - start;
            max_row_len = max_row_len.max(row_len);
            total_bytes = total_bytes
                .checked_add(row_len)
                .ok_or_else(|| Error::invalid_input("Packed struct row data size overflow"))?;
            row_offsets.push(end as u64);
        }
        debug_assert_eq!(total_bytes, row_data.len());

        let use_u32_offsets = total_bytes <= u32::MAX as usize && max_row_len <= u32::MAX as usize;
        let bits_per_offset = if use_u32_offsets { 32 } else { 64 };
        let offsets_buffer = if use_u32_offsets {
            let offsets_u32 = row_offsets
                .iter()
                .map(|&offset| offset as u32)
                .collect::<Vec<_>>();
            LanceBuffer::reinterpret_vec(offsets_u32)
        } else {
            LanceBuffer::reinterpret_vec(row_offsets)
        };

        let data_block = VariableWidthBlock {
            data: LanceBuffer::from(row_data),
            bits_per_offset,
            offsets: offsets_buffer,
            num_values,
            block_info: BlockInfo::new(),
        };

        Ok((
            PerValueDataBlock::Variable(data_block),
            ProtobufUtils21::packed_struct_variable(field_metadata),
        ))
    }
}

#[derive(Debug)]
pub(crate) enum VariablePackedStructFieldKind {
    Fixed {
        bits_per_value: u64,
        decompressor: Arc<dyn FixedPerValueDecompressor>,
    },
    Variable {
        bits_per_length: u64,
        decompressor: Arc<dyn VariablePerValueDecompressor>,
    },
}

#[derive(Debug)]
pub(crate) struct VariablePackedStructFieldDecoder {
    pub(crate) kind: VariablePackedStructFieldKind,
}

#[derive(Debug)]
pub struct PackedStructVariablePerValueDecompressor {
    fields: Vec<VariablePackedStructFieldDecoder>,
}

impl PackedStructVariablePerValueDecompressor {
    pub(crate) fn new(fields: Vec<VariablePackedStructFieldDecoder>) -> Self {
        Self { fields }
    }
}

enum FieldAccumulator {
    Fixed {
        builder: DataBlockBuilder,
        bits_per_value: u64,
        empty_value: DataBlock,
    },
    Variable32 {
        builder: DataBlockBuilder,
        empty_value: DataBlock,
    },
    Variable64 {
        builder: DataBlockBuilder,
        empty_value: DataBlock,
    },
}

impl FieldAccumulator {
    // In full-zip variable packed decoding, rep/def may produce a visible row
    // with an empty payload (e.g. null/invalid item). We still need to append
    // one placeholder per child so child row counts remain aligned.
    fn append_empty(&mut self) -> Result<()> {
        match self {
            Self::Fixed {
                builder,
                empty_value,
                ..
            } => builder.append(empty_value, 0..1),
            Self::Variable32 {
                builder,
                empty_value,
            } => builder.append(empty_value, 0..1),
            Self::Variable64 {
                builder,
                empty_value,
            } => builder.append(empty_value, 0..1),
        }
    }
}

impl VariablePerValueDecompressor for PackedStructVariablePerValueDecompressor {
    fn decompress(&self, data: VariableWidthBlock) -> Result<DataBlock> {
        let num_values = data.num_values;
        let offsets_u64 = match data.bits_per_offset {
            32 => data
                .offsets
                .borrow_to_typed_slice::<u32>()
                .iter()
                .map(|v| *v as u64)
                .collect::<Vec<_>>(),
            64 => data
                .offsets
                .borrow_to_typed_slice::<u64>()
                .as_ref()
                .to_vec(),
            _ => {
                return Err(Error::invalid_input(
                    "Packed struct row offsets must be 32 or 64 bits",
                ));
            }
        };

        if offsets_u64.len() != num_values as usize + 1 {
            return Err(Error::invalid_input(
                "Packed struct row offsets length mismatch",
            ));
        }

        let mut accumulators = Vec::with_capacity(self.fields.len());
        for field in &self.fields {
            match &field.kind {
                VariablePackedStructFieldKind::Fixed { bits_per_value, .. } => {
                    if bits_per_value % 8 != 0 {
                        return Err(Error::invalid_input(
                            "Packed struct fixed child must be byte-aligned",
                        ));
                    }
                    let bytes_per_value = bits_per_value.checked_div(8).ok_or_else(|| {
                        Error::invalid_input("Invalid bits per value for packed struct field")
                    })?;
                    let estimate = bytes_per_value.checked_mul(num_values).ok_or_else(|| {
                        Error::invalid_input("Packed struct fixed child allocation overflow")
                    })?;
                    let empty_value = DataBlock::FixedWidth(FixedWidthDataBlock {
                        data: LanceBuffer::from(vec![0_u8; bytes_per_value as usize]),
                        bits_per_value: *bits_per_value,
                        num_values: 1,
                        block_info: BlockInfo::new(),
                    });
                    accumulators.push(FieldAccumulator::Fixed {
                        builder: DataBlockBuilder::with_capacity_estimate(estimate),
                        bits_per_value: *bits_per_value,
                        empty_value,
                    });
                }
                VariablePackedStructFieldKind::Variable {
                    bits_per_length, ..
                } => match bits_per_length {
                    32 => accumulators.push(FieldAccumulator::Variable32 {
                        builder: DataBlockBuilder::with_capacity_estimate(data.data.len() as u64),
                        empty_value: DataBlock::VariableWidth(VariableWidthBlock {
                            data: LanceBuffer::empty(),
                            bits_per_offset: 32,
                            offsets: LanceBuffer::reinterpret_vec(vec![0_u32, 0_u32]),
                            num_values: 1,
                            block_info: BlockInfo::new(),
                        }),
                    }),
                    64 => accumulators.push(FieldAccumulator::Variable64 {
                        builder: DataBlockBuilder::with_capacity_estimate(data.data.len() as u64),
                        empty_value: DataBlock::VariableWidth(VariableWidthBlock {
                            data: LanceBuffer::empty(),
                            bits_per_offset: 64,
                            offsets: LanceBuffer::reinterpret_vec(vec![0_u64, 0_u64]),
                            num_values: 1,
                            block_info: BlockInfo::new(),
                        }),
                    }),
                    _ => {
                        return Err(Error::invalid_input(
                            "Packed struct variable child must use 32 or 64-bit length prefixes",
                        ));
                    }
                },
            }
        }

        for row_idx in 0..num_values as usize {
            let row_start = offsets_u64[row_idx] as usize;
            let row_end = offsets_u64[row_idx + 1] as usize;
            if row_end > data.data.len() || row_start > row_end {
                return Err(Error::invalid_input(
                    "Packed struct row bounds exceed buffer",
                ));
            }
            if row_start == row_end {
                for accumulator in accumulators.iter_mut() {
                    accumulator.append_empty()?;
                }
                continue;
            }
            let mut cursor = row_start;
            for (field, accumulator) in self.fields.iter().zip(accumulators.iter_mut()) {
                match (&field.kind, accumulator) {
                    (
                        VariablePackedStructFieldKind::Fixed { bits_per_value, .. },
                        FieldAccumulator::Fixed {
                            builder,
                            bits_per_value: acc_bits,
                            ..
                        },
                    ) => {
                        debug_assert_eq!(bits_per_value, acc_bits);
                        let bytes_per_value = (bits_per_value / 8) as usize;
                        let end = cursor + bytes_per_value;
                        if end > row_end {
                            return Err(Error::invalid_input(
                                "Packed struct fixed child exceeds row bounds",
                            ));
                        }
                        let value_block = DataBlock::FixedWidth(FixedWidthDataBlock {
                            data: LanceBuffer::from(data.data[cursor..end].to_vec()),
                            bits_per_value: *bits_per_value,
                            num_values: 1,
                            block_info: BlockInfo::new(),
                        });
                        builder.append(&value_block, 0..1)?;
                        cursor = end;
                    }
                    (
                        VariablePackedStructFieldKind::Variable {
                            bits_per_length, ..
                        },
                        FieldAccumulator::Variable32 { builder, .. },
                    ) => {
                        if *bits_per_length != 32 {
                            return Err(Error::invalid_input(
                                "Packed struct length prefix size mismatch",
                            ));
                        }
                        let end = cursor + std::mem::size_of::<u32>();
                        if end > row_end {
                            return Err(Error::invalid_input(
                                "Packed struct variable child length prefix out of bounds",
                            ));
                        }
                        let len = u32::from_le_bytes(
                            data.data[cursor..end]
                                .try_into()
                                .expect("slice has exact length"),
                        ) as usize;
                        cursor = end;
                        let value_end = cursor + len;
                        if value_end > row_end {
                            return Err(Error::invalid_input(
                                "Packed struct variable child exceeds row bounds",
                            ));
                        }
                        let value_block = DataBlock::VariableWidth(VariableWidthBlock {
                            data: LanceBuffer::from(data.data[cursor..value_end].to_vec()),
                            bits_per_offset: 32,
                            offsets: LanceBuffer::reinterpret_vec(vec![0_u32, len as u32]),
                            num_values: 1,
                            block_info: BlockInfo::new(),
                        });
                        builder.append(&value_block, 0..1)?;
                        cursor = value_end;
                    }
                    (
                        VariablePackedStructFieldKind::Variable {
                            bits_per_length, ..
                        },
                        FieldAccumulator::Variable64 { builder, .. },
                    ) => {
                        if *bits_per_length != 64 {
                            return Err(Error::invalid_input(
                                "Packed struct length prefix size mismatch",
                            ));
                        }
                        let end = cursor + std::mem::size_of::<u64>();
                        if end > row_end {
                            return Err(Error::invalid_input(
                                "Packed struct variable child length prefix out of bounds",
                            ));
                        }
                        let len = u64::from_le_bytes(
                            data.data[cursor..end]
                                .try_into()
                                .expect("slice has exact length"),
                        ) as usize;
                        cursor = end;
                        let value_end = cursor + len;
                        if value_end > row_end {
                            return Err(Error::invalid_input(
                                "Packed struct variable child exceeds row bounds",
                            ));
                        }
                        let value_block = DataBlock::VariableWidth(VariableWidthBlock {
                            data: LanceBuffer::from(data.data[cursor..value_end].to_vec()),
                            bits_per_offset: 64,
                            offsets: LanceBuffer::reinterpret_vec(vec![0_u64, len as u64]),
                            num_values: 1,
                            block_info: BlockInfo::new(),
                        });
                        builder.append(&value_block, 0..1)?;
                        cursor = value_end;
                    }
                    _ => {
                        return Err(Error::invalid_input(
                            "Packed struct accumulator kind mismatch",
                        ));
                    }
                }
            }
            if cursor != row_end {
                return Err(Error::invalid_input(
                    "Packed struct row parsing did not consume full row",
                ));
            }
        }

        let mut children = Vec::with_capacity(self.fields.len());
        for (field, accumulator) in self.fields.iter().zip(accumulators) {
            match (field, accumulator) {
                (
                    VariablePackedStructFieldDecoder {
                        kind: VariablePackedStructFieldKind::Fixed { decompressor, .. },
                    },
                    FieldAccumulator::Fixed { builder, .. },
                ) => {
                    let DataBlock::FixedWidth(block) = builder.finish() else {
                        panic!("Expected fixed-width datablock from builder");
                    };
                    let decoded = decompressor.decompress(block, num_values)?;
                    children.push(decoded);
                }
                (
                    VariablePackedStructFieldDecoder {
                        kind:
                            VariablePackedStructFieldKind::Variable {
                                bits_per_length,
                                decompressor,
                            },
                    },
                    FieldAccumulator::Variable32 { builder, .. },
                ) => {
                    let DataBlock::VariableWidth(mut block) = builder.finish() else {
                        panic!("Expected variable-width datablock from builder");
                    };
                    debug_assert_eq!(block.bits_per_offset, 32);
                    block.bits_per_offset = (*bits_per_length) as u8;
                    let decoded = decompressor.decompress(block)?;
                    children.push(decoded);
                }
                (
                    VariablePackedStructFieldDecoder {
                        kind:
                            VariablePackedStructFieldKind::Variable {
                                bits_per_length,
                                decompressor,
                            },
                    },
                    FieldAccumulator::Variable64 { builder, .. },
                ) => {
                    let DataBlock::VariableWidth(mut block) = builder.finish() else {
                        panic!("Expected variable-width datablock from builder");
                    };
                    debug_assert_eq!(block.bits_per_offset, 64);
                    block.bits_per_offset = (*bits_per_length) as u8;
                    let decoded = decompressor.decompress(block)?;
                    children.push(decoded);
                }
                _ => {
                    return Err(Error::invalid_input(
                        "Packed struct accumulator mismatch during finalize",
                    ));
                }
            }
        }

        Ok(DataBlock::Struct(StructDataBlock {
            children,
            block_info: BlockInfo::new(),
            validity: None,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        compression::DefaultDecompressionStrategy,
        compression_config::CompressionParams,
        constants::PACKED_STRUCT_META_KEY,
        statistics::ComputeStat,
        testing::{
            TestCases, TestEncoding, check_round_trip_encoding_of_data, test_compression_strategy,
        },
    };
    use arrow_array::{
        Array, ArrayRef, BinaryArray, Int32Array, Int64Array, LargeStringArray, StringArray,
        StructArray, UInt32Array,
    };
    use arrow_schema::{DataType, Field as ArrowField, Fields};
    use std::collections::HashMap;
    use std::sync::Arc;

    fn fixed_block_from_array(array: Int64Array) -> FixedWidthDataBlock {
        let num_values = array.len() as u64;
        let block = DataBlock::from_arrays(&[Arc::new(array) as ArrayRef], num_values);
        match block {
            DataBlock::FixedWidth(block) => block,
            _ => panic!("Expected fixed-width data block"),
        }
    }

    fn fixed_i32_block_from_array(array: Int32Array) -> FixedWidthDataBlock {
        let num_values = array.len() as u64;
        let block = DataBlock::from_arrays(&[Arc::new(array) as ArrayRef], num_values);
        match block {
            DataBlock::FixedWidth(block) => block,
            _ => panic!("Expected fixed-width data block"),
        }
    }

    fn variable_block_from_string_array(array: StringArray) -> VariableWidthBlock {
        let num_values = array.len() as u64;
        let block = DataBlock::from_arrays(&[Arc::new(array) as ArrayRef], num_values);
        match block {
            DataBlock::VariableWidth(block) => block,
            _ => panic!("Expected variable-width block"),
        }
    }

    fn variable_block_from_large_string_array(array: LargeStringArray) -> VariableWidthBlock {
        let num_values = array.len() as u64;
        let block = DataBlock::from_arrays(&[Arc::new(array) as ArrayRef], num_values);
        match block {
            DataBlock::VariableWidth(block) => block,
            _ => panic!("Expected variable-width block"),
        }
    }

    fn variable_block_from_binary_array(array: BinaryArray) -> VariableWidthBlock {
        let num_values = array.len() as u64;
        let block = DataBlock::from_arrays(&[Arc::new(array) as ArrayRef], num_values);
        match block {
            DataBlock::VariableWidth(block) => block,
            _ => panic!("Expected variable-width block"),
        }
    }

    #[test]
    fn variable_packed_struct_round_trip() -> Result<()> {
        let arrow_fields: Fields = vec![
            ArrowField::new("id", DataType::UInt32, false),
            ArrowField::new("name", DataType::Utf8, true),
        ]
        .into();
        let arrow_struct = ArrowField::new("item", DataType::Struct(arrow_fields), false);
        let struct_field = Field::try_from(&arrow_struct)?;

        let ids = vec![1_u32, 2, 42];
        let id_bytes = ids
            .iter()
            .flat_map(|value| value.to_le_bytes())
            .collect::<Vec<_>>();
        let mut id_block = FixedWidthDataBlock {
            data: LanceBuffer::reinterpret_vec(ids),
            bits_per_value: 32,
            num_values: 3,
            block_info: BlockInfo::new(),
        };
        id_block.compute_stat();
        let id_block = DataBlock::FixedWidth(id_block);

        let name_offsets = vec![0_i32, 1, 4, 4];
        let name_bytes = b"abcz".to_vec();
        let mut name_block = VariableWidthBlock {
            data: LanceBuffer::from(name_bytes.clone()),
            bits_per_offset: 32,
            offsets: LanceBuffer::reinterpret_vec(name_offsets.clone()),
            num_values: 3,
            block_info: BlockInfo::new(),
        };
        name_block.compute_stat();
        let name_block = DataBlock::VariableWidth(name_block);

        let struct_block = StructDataBlock {
            children: vec![id_block, name_block],
            block_info: BlockInfo::new(),
            validity: None,
        };

        let data_block = DataBlock::Struct(struct_block);

        let compression_strategy =
            test_compression_strategy(TestEncoding::StructuralU32, CompressionParams::default());
        let compressor = crate::compression::CompressionStrategy::create_per_value(
            compression_strategy.as_ref(),
            &struct_field,
            &data_block,
        )?;
        let (compressed, encoding) = compressor.compress(data_block)?;

        let PerValueDataBlock::Variable(zipped) = compressed else {
            panic!("expected variable-width packed struct output");
        };

        let decompression_strategy = DefaultDecompressionStrategy::default();
        let decompressor =
            crate::compression::DecompressionStrategy::create_variable_per_value_decompressor(
                &decompression_strategy,
                &encoding,
            )?;
        let decoded = decompressor.decompress(zipped)?;

        let DataBlock::Struct(decoded_struct) = decoded else {
            panic!("expected struct datablock after decode");
        };

        let decoded_id = decoded_struct.children[0].as_fixed_width_ref().unwrap();
        assert_eq!(decoded_id.bits_per_value, 32);
        assert_eq!(decoded_id.data.as_ref(), id_bytes.as_slice());

        let decoded_name = decoded_struct.children[1].as_variable_width_ref().unwrap();
        assert_eq!(decoded_name.bits_per_offset, 32);
        let decoded_offsets = decoded_name.offsets.borrow_to_typed_slice::<i32>();
        assert_eq!(decoded_offsets.as_ref(), name_offsets.as_slice());
        assert_eq!(decoded_name.data.as_ref(), name_bytes.as_slice());

        Ok(())
    }

    #[test]
    fn variable_packed_struct_large_utf8_round_trip() -> Result<()> {
        let arrow_fields: Fields = vec![
            ArrowField::new("value", DataType::Int64, false),
            ArrowField::new("text", DataType::LargeUtf8, false),
        ]
        .into();
        let arrow_struct = ArrowField::new("item", DataType::Struct(arrow_fields), false);
        let struct_field = Field::try_from(&arrow_struct)?;

        let id_block = fixed_block_from_array(Int64Array::from(vec![10, 20, 30, 40]));
        let payload_array = LargeStringArray::from(vec![
            "alpha",
            "a considerably longer payload for testing",
            "mid",
            "z",
        ]);
        let payload_block = variable_block_from_large_string_array(payload_array);

        let struct_block = StructDataBlock {
            children: vec![
                DataBlock::FixedWidth(id_block.clone()),
                DataBlock::VariableWidth(payload_block.clone()),
            ],
            block_info: BlockInfo::new(),
            validity: None,
        };

        let data_block = DataBlock::Struct(struct_block);

        let compression_strategy =
            test_compression_strategy(TestEncoding::StructuralU32, CompressionParams::default());
        let compressor = crate::compression::CompressionStrategy::create_per_value(
            compression_strategy.as_ref(),
            &struct_field,
            &data_block,
        )?;
        let (compressed, encoding) = compressor.compress(data_block)?;

        let PerValueDataBlock::Variable(zipped) = compressed else {
            panic!("expected variable-width packed struct output");
        };

        let decompression_strategy = DefaultDecompressionStrategy::default();
        let decompressor =
            crate::compression::DecompressionStrategy::create_variable_per_value_decompressor(
                &decompression_strategy,
                &encoding,
            )?;
        let decoded = decompressor.decompress(zipped)?;

        let DataBlock::Struct(decoded_struct) = decoded else {
            panic!("expected struct datablock after decode");
        };

        let decoded_id = decoded_struct.children[0].as_fixed_width_ref().unwrap();
        assert_eq!(decoded_id.bits_per_value, 64);
        assert_eq!(decoded_id.data.as_ref(), id_block.data.as_ref());

        let decoded_payload = decoded_struct.children[1].as_variable_width_ref().unwrap();
        assert_eq!(decoded_payload.bits_per_offset, 64);
        assert_eq!(
            decoded_payload
                .offsets
                .borrow_to_typed_slice::<i64>()
                .as_ref(),
            payload_block
                .offsets
                .borrow_to_typed_slice::<i64>()
                .as_ref()
        );
        assert_eq!(decoded_payload.data.as_ref(), payload_block.data.as_ref());

        Ok(())
    }

    #[tokio::test]
    async fn variable_packed_struct_utf8_round_trip() {
        // schema: Struct<id: UInt32, uri: Utf8, long_text: LargeUtf8>
        let fields = Fields::from(vec![
            Arc::new(ArrowField::new("id", DataType::UInt32, false)),
            Arc::new(ArrowField::new("uri", DataType::Utf8, false)),
            Arc::new(ArrowField::new("long_text", DataType::LargeUtf8, false)),
        ]);

        // mark struct as packed
        let mut meta = HashMap::new();
        meta.insert(PACKED_STRUCT_META_KEY.to_string(), "true".to_string());

        let array = Arc::new(StructArray::from(vec![
            (
                fields[0].clone(),
                Arc::new(UInt32Array::from(vec![1, 2, 3])) as ArrayRef,
            ),
            (
                fields[1].clone(),
                Arc::new(StringArray::from(vec![
                    Some("a"),
                    Some("b"),
                    Some("/tmp/x"),
                ])) as ArrayRef,
            ),
            (
                fields[2].clone(),
                Arc::new(LargeStringArray::from(vec![
                    Some("alpha"),
                    Some("a considerably longer payload for testing"),
                    Some("mid"),
                ])) as ArrayRef,
            ),
        ]));

        let test_cases = TestCases::default()
            .with_u32_structural_encodings()
            .with_expected_encoding("variable_packed_struct");

        check_round_trip_encoding_of_data(vec![array], &test_cases, meta).await;
    }

    #[test]
    fn variable_packed_struct_multi_variable_round_trip() -> Result<()> {
        let arrow_fields: Fields = vec![
            ArrowField::new("category", DataType::Utf8, false),
            ArrowField::new("payload", DataType::Binary, false),
            ArrowField::new("count", DataType::Int32, false),
        ]
        .into();
        let arrow_struct = ArrowField::new("item", DataType::Struct(arrow_fields), false);
        let struct_field = Field::try_from(&arrow_struct)?;

        let category_array = StringArray::from(vec!["red", "blue", "green", "red"]);
        let category_block = variable_block_from_string_array(category_array);
        let payload_values: Vec<Vec<u8>> =
            vec![vec![0x01, 0x02], vec![], vec![0x05, 0x06, 0x07], vec![0xff]];
        let payload_array =
            BinaryArray::from_iter_values(payload_values.iter().map(|v| v.as_slice()));
        let payload_block = variable_block_from_binary_array(payload_array);
        let count_block = fixed_i32_block_from_array(Int32Array::from(vec![1, 2, 3, 4]));

        let struct_block = StructDataBlock {
            children: vec![
                DataBlock::VariableWidth(category_block.clone()),
                DataBlock::VariableWidth(payload_block.clone()),
                DataBlock::FixedWidth(count_block.clone()),
            ],
            block_info: BlockInfo::new(),
            validity: None,
        };

        let data_block = DataBlock::Struct(struct_block);

        let compression_strategy =
            test_compression_strategy(TestEncoding::StructuralU32, CompressionParams::default());
        let compressor = crate::compression::CompressionStrategy::create_per_value(
            compression_strategy.as_ref(),
            &struct_field,
            &data_block,
        )?;
        let (compressed, encoding) = compressor.compress(data_block)?;

        let PerValueDataBlock::Variable(zipped) = compressed else {
            panic!("expected variable-width packed struct output");
        };

        let decompression_strategy = DefaultDecompressionStrategy::default();
        let decompressor =
            crate::compression::DecompressionStrategy::create_variable_per_value_decompressor(
                &decompression_strategy,
                &encoding,
            )?;
        let decoded = decompressor.decompress(zipped)?;

        let DataBlock::Struct(decoded_struct) = decoded else {
            panic!("expected struct datablock after decode");
        };

        let decoded_category = decoded_struct.children[0].as_variable_width_ref().unwrap();
        assert_eq!(decoded_category.bits_per_offset, 32);
        assert_eq!(
            decoded_category
                .offsets
                .borrow_to_typed_slice::<i32>()
                .as_ref(),
            category_block
                .offsets
                .borrow_to_typed_slice::<i32>()
                .as_ref()
        );
        assert_eq!(decoded_category.data.as_ref(), category_block.data.as_ref());

        let decoded_payload = decoded_struct.children[1].as_variable_width_ref().unwrap();
        assert_eq!(decoded_payload.bits_per_offset, 32);
        assert_eq!(
            decoded_payload
                .offsets
                .borrow_to_typed_slice::<i32>()
                .as_ref(),
            payload_block
                .offsets
                .borrow_to_typed_slice::<i32>()
                .as_ref()
        );
        assert_eq!(decoded_payload.data.as_ref(), payload_block.data.as_ref());

        let decoded_count = decoded_struct.children[2].as_fixed_width_ref().unwrap();
        assert_eq!(decoded_count.bits_per_value, 32);
        assert_eq!(decoded_count.data.as_ref(), count_block.data.as_ref());

        Ok(())
    }

    #[test]
    fn variable_packed_struct_requires_v22() {
        let arrow_fields: Fields = vec![
            ArrowField::new("value", DataType::Int64, false),
            ArrowField::new("text", DataType::Utf8, false),
        ]
        .into();
        let arrow_struct = ArrowField::new("item", DataType::Struct(arrow_fields), false);
        let struct_field = Field::try_from(&arrow_struct).unwrap();

        let value_block = fixed_block_from_array(Int64Array::from(vec![1, 2, 3]));
        let text_block =
            variable_block_from_string_array(StringArray::from(vec!["a", "bb", "ccc"]));

        let struct_block = StructDataBlock {
            children: vec![
                DataBlock::FixedWidth(value_block),
                DataBlock::VariableWidth(text_block),
            ],
            block_info: BlockInfo::new(),
            validity: None,
        };

        let compression_strategy =
            test_compression_strategy(TestEncoding::StructuralU16, CompressionParams::default());
        let result =
            compression_strategy.create_per_value(&struct_field, &DataBlock::Struct(struct_block));

        assert!(matches!(result, Err(Error::NotSupported { .. })));
    }

    #[test]
    fn variable_packed_struct_decompress_empty_row() -> Result<()> {
        let strategy = DefaultDecompressionStrategy::default();
        let fixed_decompressor = Arc::from(
            crate::compression::DecompressionStrategy::create_fixed_per_value_decompressor(
                &strategy,
                &ProtobufUtils21::flat(32, None),
            )?,
        );
        let variable_decompressor = Arc::from(
            crate::compression::DecompressionStrategy::create_variable_per_value_decompressor(
                &strategy,
                &ProtobufUtils21::variable(ProtobufUtils21::flat(32, None), None),
            )?,
        );

        let decompressor = PackedStructVariablePerValueDecompressor::new(vec![
            VariablePackedStructFieldDecoder {
                kind: VariablePackedStructFieldKind::Fixed {
                    bits_per_value: 32,
                    decompressor: fixed_decompressor,
                },
            },
            VariablePackedStructFieldDecoder {
                kind: VariablePackedStructFieldKind::Variable {
                    bits_per_length: 32,
                    decompressor: variable_decompressor,
                },
            },
        ]);

        let mut row_data = Vec::new();
        row_data.extend_from_slice(&1_u32.to_le_bytes());
        row_data.extend_from_slice(&1_u32.to_le_bytes());
        row_data.extend_from_slice(b"a");
        row_data.extend_from_slice(&2_u32.to_le_bytes());
        row_data.extend_from_slice(&0_u32.to_le_bytes());

        let input = VariableWidthBlock {
            data: LanceBuffer::from(row_data),
            bits_per_offset: 32,
            offsets: LanceBuffer::reinterpret_vec(vec![0_u32, 9_u32, 9_u32, 17_u32]),
            num_values: 3,
            block_info: BlockInfo::new(),
        };

        let decoded = decompressor.decompress(input)?;
        let DataBlock::Struct(decoded_struct) = decoded else {
            panic!("expected struct output");
        };

        let fixed = decoded_struct.children[0].as_fixed_width_ref().unwrap();
        assert_eq!(fixed.bits_per_value, 32);
        assert_eq!(
            fixed.data.borrow_to_typed_slice::<u32>().as_ref(),
            &[1, 0, 2]
        );

        let variable = decoded_struct.children[1].as_variable_width_ref().unwrap();
        assert_eq!(variable.bits_per_offset, 32);
        assert_eq!(
            variable.offsets.borrow_to_typed_slice::<u32>().as_ref(),
            &[0_u32, 1_u32, 1_u32, 1_u32]
        );
        assert_eq!(variable.data.as_ref(), b"a");

        Ok(())
    }
}
