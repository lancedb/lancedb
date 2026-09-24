// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Routines for compressing and decompressing constant-encoded data

use crate::{
    buffer::LanceBuffer,
    compression::{BlockDecompressor, FixedPerValueDecompressor, require_no_block_payload},
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
    fn decompress(&self, data: Option<LanceBuffer>, num_values: u64) -> Result<DataBlock> {
        require_no_block_payload(data, "Constant")?;
        if let Some(scalar) = self.scalar.clone() {
            Ok(DataBlock::Constant(ConstantDataBlock {
                data: scalar,
                num_values,
            }))
        } else {
            Ok(DataBlock::AllNull(AllNullDataBlock { num_values }))
        }
    }

    fn requires_payload(&self) -> bool {
        false
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn block_constant_requires_no_payload() {
        let decompressor = ConstantDecompressor::new(None);

        assert!(!decompressor.requires_payload());
        assert!(matches!(
            BlockDecompressor::decompress(&decompressor, None, 3).unwrap(),
            DataBlock::AllNull(AllNullDataBlock { num_values: 3 })
        ));
        assert!(
            BlockDecompressor::decompress(&decompressor, Some(LanceBuffer::empty()), 3).is_err()
        );
    }
}
