// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Utilities for byte (not bit) packing for situations where saving a few
//! bits is less important than simplicity and speed.

use lance_core::{Error, Result};

pub struct U8BytePacker {
    data: Vec<u8>,
}

impl U8BytePacker {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            data: Vec::with_capacity(capacity),
        }
    }

    fn append(&mut self, value: u8) {
        self.data.push(value);
    }
}

pub struct U16BytePacker {
    data: Vec<u8>,
}

impl U16BytePacker {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            data: Vec::with_capacity(capacity * 2),
        }
    }

    fn append(&mut self, value: u16) {
        self.data.extend_from_slice(&value.to_le_bytes());
    }
}

pub struct U32BytePacker {
    data: Vec<u8>,
}

impl U32BytePacker {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            data: Vec::with_capacity(capacity * 4),
        }
    }

    fn append(&mut self, value: u32) {
        self.data.extend_from_slice(&value.to_le_bytes());
    }
}

pub struct U64BytePacker {
    data: Vec<u8>,
}

impl U64BytePacker {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            data: Vec::with_capacity(capacity * 8),
        }
    }

    fn append(&mut self, value: u64) {
        self.data.extend_from_slice(&value.to_le_bytes());
    }
}

/// A bytepacked integer encoder that automatically chooses the smallest
/// possible integer type to store the given values.
///
/// This is byte packing (not bit packing).  Not even that, we only fit things into
/// sizes of 1,2,4,8 bytes.  It's simple, fast, and easy but doesn't provide the
/// maximum possible compression.
///
/// Still, it's useful for things like offsets which are often small and fit into a
/// u16 or u32 but sometimes might need the full u64 range.
///
/// In the future we can investigate replacing this with something more sophisticated.
pub enum BytepackedIntegerEncoder {
    U8(U8BytePacker),
    U16(U16BytePacker),
    U32(U32BytePacker),
    U64(U64BytePacker),
    Zero,
}

impl BytepackedIntegerEncoder {
    /// Create a new encoder with the given capacity and maximum value.
    pub fn with_capacity(capacity: usize, max_value: u64) -> Self {
        if max_value == 0 {
            Self::Zero
        } else if max_value <= u8::MAX as u64 {
            Self::U8(U8BytePacker::with_capacity(capacity))
        } else if max_value <= u16::MAX as u64 {
            Self::U16(U16BytePacker::with_capacity(capacity))
        } else if max_value <= u32::MAX as u64 {
            Self::U32(U32BytePacker::with_capacity(capacity))
        } else {
            Self::U64(U64BytePacker::with_capacity(capacity))
        }
    }

    /// Append a value to the encoder.
    ///
    /// # Errors
    ///
    /// Returns an error if `value` does not fit in the width selected at
    /// construction time.
    pub fn append(&mut self, value: u64) -> Result<()> {
        match self {
            Self::U8(_) if value > u8::MAX as u64 => {
                return Err(Error::invalid_input(format!(
                    "value {value} does not fit in bytepacked u8"
                )));
            }
            Self::U16(_) if value > u16::MAX as u64 => {
                return Err(Error::invalid_input(format!(
                    "value {value} does not fit in bytepacked u16"
                )));
            }
            Self::U32(_) if value > u32::MAX as u64 => {
                return Err(Error::invalid_input(format!(
                    "value {value} does not fit in bytepacked u32"
                )));
            }
            _ => {}
        }
        self.append_trusted(value);
        Ok(())
    }

    /// Append a value whose range is guaranteed by the caller's construction.
    pub(crate) fn append_trusted(&mut self, value: u64) {
        match self {
            Self::U8(packer) => {
                debug_assert!(u8::try_from(value).is_ok());
                packer.append(value as u8);
            }
            Self::U16(packer) => {
                debug_assert!(u16::try_from(value).is_ok());
                packer.append(value as u16);
            }
            Self::U32(packer) => {
                debug_assert!(u32::try_from(value).is_ok());
                packer.append(value as u32);
            }
            Self::U64(packer) => packer.append(value),
            Self::Zero => {}
        }
    }

    /// Convert the encoder into a vector of bytes.
    pub fn into_data(self) -> Vec<u8> {
        match self {
            Self::U8(packer) => packer.data,
            Self::U16(packer) => packer.data,
            Self::U32(packer) => packer.data,
            Self::U64(packer) => packer.data,
            Self::Zero => Vec::new(),
        }
    }
}

/// An iterator that unpacks bytes into integers (currently only u64)
pub enum ByteUnpacker<I: Iterator<Item = u8>> {
    U8(I),
    U16(I),
    U32(I),
    U64(I),
}

impl<T: Iterator<Item = u8>> ByteUnpacker<T> {
    #[allow(clippy::new_ret_no_self)]
    pub fn new<I: IntoIterator<IntoIter = T>>(data: I, size: usize) -> impl Iterator<Item = u64> {
        match size {
            1 => Self::U8(data.into_iter()),
            2 => Self::U16(data.into_iter()),
            4 => Self::U32(data.into_iter()),
            8 => Self::U64(data.into_iter()),
            _ => panic!("Invalid size"),
        }
    }
}

impl<I: Iterator<Item = u8>> Iterator for ByteUnpacker<I> {
    type Item = u64;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::U8(iter) => iter.next().map(|v| v as u64),
            Self::U16(iter) => {
                let first_byte = iter.next()?;
                Some(u16::from_le_bytes([first_byte, iter.next().unwrap()]) as u64)
            }
            Self::U32(iter) => {
                let first_byte = iter.next()?;
                Some(u32::from_le_bytes([
                    first_byte,
                    iter.next().unwrap(),
                    iter.next().unwrap(),
                    iter.next().unwrap(),
                ]) as u64)
            }
            Self::U64(iter) => {
                let first_byte = iter.next()?;
                Some(u64::from_le_bytes([
                    first_byte,
                    iter.next().unwrap(),
                    iter.next().unwrap(),
                    iter.next().unwrap(),
                    iter.next().unwrap(),
                    iter.next().unwrap(),
                    iter.next().unwrap(),
                    iter.next().unwrap(),
                ]))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bytepacked_integer_encoder() {
        // Fits in u8
        let mut encoder = BytepackedIntegerEncoder::with_capacity(10, 100);
        encoder.append(50).unwrap();
        encoder.append(20).unwrap();
        encoder.append(30).unwrap();
        let data = encoder.into_data();
        assert_eq!(data, vec![50, 20, 30]);

        assert_eq!(
            ByteUnpacker::new(data, 1).collect::<Vec<_>>(),
            vec![50, 20, 30]
        );

        // Requires u16
        let mut encoder = BytepackedIntegerEncoder::with_capacity(10, 1000);
        encoder.append(500).unwrap();
        encoder.append(200).unwrap();
        encoder.append(300).unwrap();
        let data = encoder.into_data();
        assert_eq!(data, vec![244, 1, 200, 0, 44, 1]);

        assert_eq!(
            ByteUnpacker::new(data, 2).collect::<Vec<_>>(),
            vec![500, 200, 300]
        );

        // Requires u32
        let mut encoder = BytepackedIntegerEncoder::with_capacity(10, 1000000);
        encoder.append(500000).unwrap();
        encoder.append(200000).unwrap();
        encoder.append(300000).unwrap();
        let data = encoder.into_data();
        assert_eq!(data, vec![32, 161, 7, 0, 64, 13, 3, 0, 224, 147, 4, 0]);

        assert_eq!(
            ByteUnpacker::new(data, 4).collect::<Vec<_>>(),
            vec![500000, 200000, 300000]
        );

        // Requires u64
        let mut encoder = BytepackedIntegerEncoder::with_capacity(10, 0x10000000000);
        encoder.append(0x5000000000).unwrap();
        encoder.append(0x2000000000).unwrap();
        encoder.append(0x3000000000).unwrap();
        let data = encoder.into_data();
        assert_eq!(
            data,
            vec![
                0, 0, 0, 0, 80, 0, 0, 0, 0, 0, 0, 0, 32, 0, 0, 0, 0, 0, 0, 0, 48, 0, 0, 0
            ]
        );

        assert_eq!(
            ByteUnpacker::new(data, 8).collect::<Vec<_>>(),
            vec![0x5000000000, 0x2000000000, 0x3000000000]
        );
    }

    #[test]
    fn test_bytepacked_integer_encoder_rejects_overflow() {
        for (max_value, invalid_value, expected_width) in [
            (u8::MAX as u64, u8::MAX as u64 + 1, "u8"),
            (u16::MAX as u64, u16::MAX as u64 + 1, "u16"),
            (u32::MAX as u64, u32::MAX as u64 + 1, "u32"),
        ] {
            let mut encoder = BytepackedIntegerEncoder::with_capacity(1, max_value);
            let error = encoder.append(invalid_value).unwrap_err();
            assert!(error.to_string().contains(expected_width), "{error}");
        }

        let mut disabled = BytepackedIntegerEncoder::with_capacity(1, 0);
        disabled.append(u64::MAX).unwrap();
        assert!(disabled.into_data().is_empty());
    }
}
