// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Routines for encoding and decoding full-zip data
//!
//! Full-zip is one of the two structural encodings in Lance 2.1.
//! In this approach the various compressed buffers are zipped
//! together so that all parts of a value are stored contiguously in memory.
//!
//! This requires transparent compression and is most suitable for
//! large data types.

use crate::{
    data::{DataBlock, FixedWidthDataBlock, VariableWidthBlock},
    format::pb21::CompressiveEncoding,
};

use lance_core::Result;

/// Per-value compression must either:
///
/// A single buffer of fixed-width values
/// A single buffer of value data and a buffer of offsets
///
/// TODO: In the future we may allow metadata buffers
#[derive(Debug)]
pub enum PerValueDataBlock {
    Fixed(FixedWidthDataBlock),
    Variable(VariableWidthBlock),
}

impl PerValueDataBlock {
    pub fn data_size(&self) -> u64 {
        match self {
            Self::Fixed(fixed) => fixed.data_size(),
            Self::Variable(variable) => variable.data_size(),
        }
    }
}

/// Trait for compression algorithms that are suitable for use in the zipped structural encoding
///
/// This compression must return either a FixedWidthDataBlock or a VariableWidthBlock.  This is because
/// we need to zip the data and those are the only two blocks we know how to zip today.
///
/// In addition, the compressed data must be able to be decompressed in a random-access fashion.
/// This means that the decompression algorithm must be able to decompress any value without
/// decompressing all values before it.
pub trait PerValueCompressor: std::fmt::Debug + Send + Sync {
    /// Compress the data into a single buffer
    ///
    /// Also returns a description of the compression that can be used to decompress when reading the data back
    fn compress(&self, data: DataBlock) -> Result<(PerValueDataBlock, CompressiveEncoding)>;
}
