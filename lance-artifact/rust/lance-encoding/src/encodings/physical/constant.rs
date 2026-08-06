// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Routines for compressing and decompressing constant-encoded data

use crate::{
    buffer::LanceBuffer,
    compression::{BlockDecompressor, FixedPerValueDecompressor},
    data::{AllNullDataBlock, ConstantDataBlock, DataBlock, FixedWidthDataBlock},
};

use lance_core::Result;

/// A decompressor for constant-encoded data
#[derive(Debug)]
pub struct ConstantDecompressor {
    scalar: Option<LanceBuffer>,
}

impl ConstantDecompressor {
    pub fn new(scalar: Option<LanceBuffer>) -> Self {
        Self { scalar }
    }
}

impl BlockDecompressor for ConstantDecompressor {
    fn decompress(&self, _data: LanceBuffer, num_values: u64) -> Result<DataBlock> {
        if let Some(scalar) = self.scalar.clone() {
            Ok(DataBlock::Constant(ConstantDataBlock {
                data: scalar,
                num_values,
            }))
        } else {
            Ok(DataBlock::AllNull(AllNullDataBlock { num_values }))
        }
    }
}

impl FixedPerValueDecompressor for ConstantDecompressor {
    fn decompress(&self, _data: FixedWidthDataBlock, num_values: u64) -> Result<DataBlock> {
        if let Some(scalar) = self.scalar.clone() {
            Ok(DataBlock::Constant(ConstantDataBlock {
                data: scalar,
                num_values,
            }))
        } else {
            Ok(DataBlock::AllNull(AllNullDataBlock { num_values }))
        }
    }

    fn bits_per_value(&self) -> u64 {
        self.scalar
            .as_ref()
            .map(|s| s.len() as u64 * 8)
            .unwrap_or(0)
    }
}
