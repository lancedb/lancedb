// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Utilities for byte (not bit) packing for situations where saving a few
//! bits is less important than simplicity and speed.

pub struct U8BytePacker {
    data: Vec<u8>,
}

impl U8BytePacker {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            data: Vec::with_capacity(capacity),
        }
    }

    fn append(&mut self, value: u64) {
        self.data.push(value as u8);
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

    fn append(&mut self, value: u64) {
        self.data.extend_from_slice(&(value as u16).to_le_bytes());
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

    fn append(&mut self, value: u64) {
        self.data.extend_from_slice(&(value as u32).to_le_bytes());
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
    /// # Safety
    ///
    /// This function is unsafe because it doesn't check for overflow.  If the
    /// value is too large to fit in the chosen integer type, it will be silently
    /// truncated.
    pub unsafe fn append(&mut self, value: u64) {
        match self {
            Self::U8(packer) => packer.append(value),
            Self::U16(packer) => packer.append(value),
            Self::U32(packer) => packer.append(value),
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
        unsafe {
            encoder.append(50);
            encoder.append(20);
            encoder.append(30);
        }
        let data = encoder.into_data();
        assert_eq!(data, vec![50, 20, 30]);

        assert_eq!(
            ByteUnpacker::new(data, 1).collect::<Vec<_>>(),
            vec![50, 20, 30]
        );

        // Requires u16
        let mut encoder = BytepackedIntegerEncoder::with_capacity(10, 1000);
        unsafe {
            encoder.append(500);
            encoder.append(200);
            encoder.append(300);
        }
        let data = encoder.into_data();
        assert_eq!(data, vec![244, 1, 200, 0, 44, 1]);

        assert_eq!(
            ByteUnpacker::new(data, 2).collect::<Vec<_>>(),
            vec![500, 200, 300]
        );

        // Requires u32
        let mut encoder = BytepackedIntegerEncoder::with_capacity(10, 1000000);
        unsafe {
            encoder.append(500000);
            encoder.append(200000);
            encoder.append(300000);
        }
        let data = encoder.into_data();
        assert_eq!(data, vec![32, 161, 7, 0, 64, 13, 3, 0, 224, 147, 4, 0]);

        assert_eq!(
            ByteUnpacker::new(data, 4).collect::<Vec<_>>(),
            vec![500000, 200000, 300000]
        );

        // Requires u64
        let mut encoder = BytepackedIntegerEncoder::with_capacity(10, 0x10000000000);
        unsafe {
            encoder.append(0x5000000000);
            encoder.append(0x2000000000);
            encoder.append(0x3000000000);
        }
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
}
