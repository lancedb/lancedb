// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! FSST encoding
//!
//! FSST is a lightweight encoding for variable width data.  This module includes
//! adapters for both miniblock and per-value encoding.
//!
//! FSST encoding creates a small symbol table that is needed for decoding.  Currently
//! we create one symbol table per disk page and store it in the description.
//!
//! TODO: This seems to be potentially limiting.  Perhaps we should create one symbol
//! table per mini-block chunk?  In the per-value compression it may even make sense to
//! create multiple symbol tables for a single value!
//!
//! FSST encoding is transparent.

use lance_core::{Error, Result};

use crate::{
    buffer::LanceBuffer,
    compression::{MiniBlockDecompressor, VariablePerValueDecompressor},
    data::{BlockInfo, DataBlock, VariableWidthBlock},
    encodings::logical::primitive::{
        fullzip::{PerValueCompressor, PerValueDataBlock},
        miniblock::{MiniBlockCompressed, MiniBlockCompressionContext, MiniBlockCompressor},
    },
    format::{
        ProtobufUtils21,
        pb21::{self, CompressiveEncoding},
    },
};

use super::binary::BinaryMiniBlockEncoder;

struct FsstCompressed {
    data: VariableWidthBlock,
    symbol_table: Vec<u8>,
}

impl FsstCompressed {
    fn fsst_compress(data: DataBlock) -> Result<Self> {
        match data {
            DataBlock::VariableWidth(variable_width) => {
                match variable_width.bits_per_offset {
                    32 => {
                        let offsets = variable_width.offsets.borrow_to_typed_slice::<i32>();
                        let offsets_slice = offsets.as_ref();
                        let bytes_data = variable_width.data.into_buffer();

                        // prepare compression output buffer
                        let mut dest_offsets = vec![0_i32; offsets_slice.len() * 2];
                        let mut dest_values = vec![0_u8; bytes_data.len() * 2];
                        let mut symbol_table = vec![0_u8; fsst::fsst::FSST_SYMBOL_TABLE_SIZE];

                        // fsst compression
                        fsst::fsst::compress(
                            &mut symbol_table,
                            bytes_data.as_slice(),
                            offsets_slice,
                            &mut dest_values,
                            &mut dest_offsets,
                        )?;

                        // construct `DataBlock` for BinaryMiniBlockEncoder, we may want some `DataBlock` construct methods later
                        let compressed = VariableWidthBlock {
                            data: LanceBuffer::reinterpret_vec(dest_values),
                            bits_per_offset: 32,
                            offsets: LanceBuffer::reinterpret_vec(dest_offsets),
                            num_values: variable_width.num_values,
                            block_info: BlockInfo::new(),
                        };

                        Ok(Self {
                            data: compressed,
                            symbol_table,
                        })
                    }
                    64 => {
                        let offsets = variable_width.offsets.borrow_to_typed_slice::<i64>();
                        let offsets_slice = offsets.as_ref();
                        let bytes_data = variable_width.data.into_buffer();

                        // prepare compression output buffer
                        let mut dest_offsets = vec![0_i64; offsets_slice.len() * 2];
                        let mut dest_values = vec![0_u8; bytes_data.len() * 2];
                        let mut symbol_table = vec![0_u8; fsst::fsst::FSST_SYMBOL_TABLE_SIZE];

                        // fsst compression
                        fsst::fsst::compress(
                            &mut symbol_table,
                            bytes_data.as_slice(),
                            offsets_slice,
                            &mut dest_values,
                            &mut dest_offsets,
                        )?;

                        // construct `DataBlock` for BinaryMiniBlockEncoder, we may want some `DataBlock` construct methods later
                        let compressed = VariableWidthBlock {
                            data: LanceBuffer::reinterpret_vec(dest_values),
                            bits_per_offset: 64,
                            offsets: LanceBuffer::reinterpret_vec(dest_offsets),
                            num_values: variable_width.num_values,
                            block_info: BlockInfo::new(),
                        };

                        Ok(Self {
                            data: compressed,
                            symbol_table,
                        })
                    }
                    _ => panic!(
                        "Unsupported offsets type {}",
                        variable_width.bits_per_offset
                    ),
                }
            }
            _ => Err(Error::invalid_input_source(
                format!(
                    "Cannot compress a data block of type {} with FsstEncoder",
                    data.name()
                )
                .into(),
            )),
        }
    }
}

#[derive(Debug, Default)]
pub struct FsstMiniBlockEncoder {
    minichunk_size: Option<i64>,
}

impl FsstMiniBlockEncoder {
    pub fn new(minichunk_size: Option<i64>) -> Self {
        Self { minichunk_size }
    }
}

impl MiniBlockCompressor for FsstMiniBlockEncoder {
    fn compress(
        &self,
        context: MiniBlockCompressionContext,
        data: DataBlock,
    ) -> Result<(MiniBlockCompressed, CompressiveEncoding)> {
        let compressed = FsstCompressed::fsst_compress(data)?;

        let data_block = DataBlock::VariableWidth(compressed.data);

        // compress the fsst compressed data using `BinaryMiniBlockEncoder`
        let binary_compressor = Box::new(BinaryMiniBlockEncoder::new(self.minichunk_size))
            as Box<dyn MiniBlockCompressor>;

        let (binary_miniblock_compressed, binary_array_encoding) =
            binary_compressor.compress(context, data_block)?;

        Ok((
            binary_miniblock_compressed,
            ProtobufUtils21::fsst(binary_array_encoding, compressed.symbol_table),
        ))
    }
}

#[derive(Debug)]
pub struct FsstPerValueEncoder {
    inner: Box<dyn PerValueCompressor>,
}

impl FsstPerValueEncoder {
    pub fn new(inner: Box<dyn PerValueCompressor>) -> Self {
        Self { inner }
    }
}

impl PerValueCompressor for FsstPerValueEncoder {
    fn compress(&self, data: DataBlock) -> Result<(PerValueDataBlock, CompressiveEncoding)> {
        let compressed = FsstCompressed::fsst_compress(data)?;

        let data_block = DataBlock::VariableWidth(compressed.data);

        let (binary_compressed, binary_array_encoding) = self.inner.compress(data_block)?;

        Ok((
            binary_compressed,
            ProtobufUtils21::fsst(binary_array_encoding, compressed.symbol_table),
        ))
    }
}

#[derive(Debug)]
pub struct FsstPerValueDecompressor {
    symbol_table: LanceBuffer,
    inner_decompressor: Box<dyn VariablePerValueDecompressor>,
}

impl FsstPerValueDecompressor {
    pub fn new(
        symbol_table: LanceBuffer,
        inner_decompressor: Box<dyn VariablePerValueDecompressor>,
    ) -> Self {
        Self {
            symbol_table,
            inner_decompressor,
        }
    }
}

impl VariablePerValueDecompressor for FsstPerValueDecompressor {
    fn decompress(&self, data: VariableWidthBlock) -> Result<DataBlock> {
        // Step 1. Run inner decompressor
        let compressed_variable_data = self
            .inner_decompressor
            .decompress(data)?
            .as_variable_width()
            .unwrap();

        // Step 2. FSST decompress
        let bytes = compressed_variable_data.data.borrow_to_typed_slice::<u8>();
        let bytes = bytes.as_ref();

        match compressed_variable_data.bits_per_offset {
            32 => {
                let offsets = compressed_variable_data
                    .offsets
                    .borrow_to_typed_slice::<i32>();
                let offsets = offsets.as_ref();
                let num_values = compressed_variable_data.num_values;

                // The data will expand at most 8 times
                // The offsets will be the same size because we have the same # of strings
                let mut decompress_bytes_buf = vec![0u8; bytes.len() * 8];
                let mut decompress_offset_buf = vec![0i32; offsets.len()];
                fsst::fsst::decompress(
                    &self.symbol_table,
                    bytes,
                    offsets,
                    &mut decompress_bytes_buf,
                    &mut decompress_offset_buf,
                )?;

                // Ensure the offsets array is trimmed to exactly num_values + 1 elements
                decompress_offset_buf.truncate((num_values + 1) as usize);

                Ok(DataBlock::VariableWidth(VariableWidthBlock {
                    data: LanceBuffer::from(decompress_bytes_buf),
                    offsets: LanceBuffer::reinterpret_vec(decompress_offset_buf),
                    bits_per_offset: 32,
                    num_values,
                    block_info: BlockInfo::new(),
                }))
            }
            64 => {
                let offsets = compressed_variable_data
                    .offsets
                    .borrow_to_typed_slice::<i64>();
                let offsets = offsets.as_ref();
                let num_values = compressed_variable_data.num_values;

                // The data will expand at most 8 times
                // The offsets will be the same size because we have the same # of strings
                let mut decompress_bytes_buf = vec![0u8; bytes.len() * 8];
                let mut decompress_offset_buf = vec![0i64; offsets.len()];
                fsst::fsst::decompress(
                    &self.symbol_table,
                    bytes,
                    offsets,
                    &mut decompress_bytes_buf,
                    &mut decompress_offset_buf,
                )?;

                // Ensure the offsets array is trimmed to exactly num_values + 1 elements
                decompress_offset_buf.truncate((num_values + 1) as usize);

                Ok(DataBlock::VariableWidth(VariableWidthBlock {
                    data: LanceBuffer::from(decompress_bytes_buf),
                    offsets: LanceBuffer::reinterpret_vec(decompress_offset_buf),
                    bits_per_offset: 64,
                    num_values,
                    block_info: BlockInfo::new(),
                }))
            }
            _ => panic!(
                "Unsupported offset type {}",
                compressed_variable_data.bits_per_offset,
            ),
        }
    }
}

#[derive(Debug)]
pub struct FsstMiniBlockDecompressor {
    symbol_table: LanceBuffer,
    inner_decompressor: Box<dyn MiniBlockDecompressor>,
}

impl FsstMiniBlockDecompressor {
    pub fn new(
        description: &pb21::Fsst,
        inner_decompressor: Box<dyn MiniBlockDecompressor>,
    ) -> Self {
        Self {
            symbol_table: LanceBuffer::from_bytes(description.symbol_table.clone(), 1),
            inner_decompressor,
        }
    }
}

impl MiniBlockDecompressor for FsstMiniBlockDecompressor {
    fn decompress(&self, data: Vec<LanceBuffer>, num_values: u64) -> Result<DataBlock> {
        // Step 1. decompress data use `BinaryMiniBlockDecompressor`
        // Extract the bits_per_offset from the binary encoding
        let compressed_data_block = self.inner_decompressor.decompress(data, num_values)?;
        let DataBlock::VariableWidth(compressed_data_block) = compressed_data_block else {
            panic!("BinaryMiniBlockDecompressor should output VariableWidth DataBlock")
        };

        // Step 2. FSST decompress
        let bytes = &compressed_data_block.data;
        let (decompress_bytes_buf, decompress_offset_buf) =
            if compressed_data_block.bits_per_offset == 64 {
                let offsets = compressed_data_block.offsets.borrow_to_typed_slice::<i64>();
                let offsets = offsets.as_ref();

                // The data will expand at most 8 times
                // The offsets will be the same size because we have the same # of strings
                let mut decompress_bytes_buf = vec![0u8; bytes.len() * 8];
                let mut decompress_offset_buf = vec![0i64; offsets.len()];
                fsst::fsst::decompress(
                    &self.symbol_table,
                    bytes.as_ref(),
                    offsets,
                    &mut decompress_bytes_buf,
                    &mut decompress_offset_buf,
                )?;

                // Ensure the offsets array is trimmed to exactly num_values + 1 elements
                decompress_offset_buf.truncate((num_values + 1) as usize);

                (
                    decompress_bytes_buf,
                    LanceBuffer::reinterpret_vec(decompress_offset_buf),
                )
            } else {
                let offsets = compressed_data_block.offsets.borrow_to_typed_slice::<i32>();
                let offsets = offsets.as_ref();

                // The data will expand at most 8 times
                // The offsets will be the same size because we have the same # of strings
                let mut decompress_bytes_buf = vec![0u8; bytes.len() * 8];
                let mut decompress_offset_buf = vec![0i32; offsets.len()];
                fsst::fsst::decompress(
                    &self.symbol_table,
                    bytes.as_ref(),
                    offsets,
                    &mut decompress_bytes_buf,
                    &mut decompress_offset_buf,
                )?;

                // Ensure the offsets array is trimmed to exactly num_values + 1 elements
                decompress_offset_buf.truncate((num_values + 1) as usize);

                (
                    decompress_bytes_buf,
                    LanceBuffer::reinterpret_vec(decompress_offset_buf),
                )
            };

        Ok(DataBlock::VariableWidth(VariableWidthBlock {
            data: LanceBuffer::from(decompress_bytes_buf),
            offsets: decompress_offset_buf,
            bits_per_offset: compressed_data_block.bits_per_offset,
            num_values,
            block_info: BlockInfo::new(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use lance_datagen::{ByteCount, RowCount};

    use crate::testing::{TestCases, check_round_trip_encoding_of_data};

    #[test_log::test(tokio::test)]
    async fn test_fsst() {
        let test_cases = TestCases::default()
            .with_expected_encoding("fsst")
            .with_structural_encodings();

        // Generate data suitable for FSST (large strings, total size > 32KB)
        let arr = lance_datagen::gen_batch()
            .anon_col(lance_datagen::array::rand_utf8(ByteCount::from(100), false))
            .into_batch_rows(RowCount::from(5000))
            .unwrap()
            .column(0)
            .clone();

        // Test both explicit metadata and automatic selection
        // 1. Test with explicit FSST metadata
        let metadata_explicit =
            HashMap::from([("lance-encoding:compression".to_string(), "fsst".to_string())]);
        check_round_trip_encoding_of_data(vec![arr.clone()], &test_cases, metadata_explicit).await;

        // 2. Test automatic FSST selection based on data characteristics
        // FSST should be chosen automatically: max_len >= 5 and total_size >= 32KB
        check_round_trip_encoding_of_data(vec![arr], &test_cases, HashMap::new()).await;
    }
}
