// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Local implementation of AsBytes trait
//!
//! This trait provides conversion from primitive types to byte slices,
//! similar to parquet::data_type::AsBytes but without the external dependency.

/// Trait to convert primitive types to byte slices
/// Reference: <https://arrow.apache.org/rust/src/parquet/data_type.rs.html>
pub trait AsBytes {
    /// Convert the value to a byte slice
    fn as_bytes(&self) -> impl AsRef<[u8]>;
}

impl AsBytes for i32 {
    fn as_bytes(&self) -> impl AsRef<[u8]> {
        self.to_le_bytes()
    }
}

impl AsBytes for i64 {
    fn as_bytes(&self) -> impl AsRef<[u8]> {
        self.to_le_bytes()
    }
}

impl AsBytes for f32 {
    fn as_bytes(&self) -> impl AsRef<[u8]> {
        self.to_le_bytes()
    }
}

impl AsBytes for f64 {
    fn as_bytes(&self) -> impl AsRef<[u8]> {
        self.to_le_bytes()
    }
}

impl AsBytes for u8 {
    fn as_bytes(&self) -> impl AsRef<[Self]> {
        [*self]
    }
}

impl AsBytes for u16 {
    fn as_bytes(&self) -> impl AsRef<[u8]> {
        self.to_le_bytes()
    }
}

impl AsBytes for u32 {
    fn as_bytes(&self) -> impl AsRef<[u8]> {
        self.to_le_bytes()
    }
}

impl AsBytes for u64 {
    fn as_bytes(&self) -> impl AsRef<[u8]> {
        self.to_le_bytes()
    }
}

impl AsBytes for i8 {
    fn as_bytes(&self) -> impl AsRef<[u8]> {
        [*self as u8]
    }
}

impl AsBytes for i16 {
    fn as_bytes(&self) -> impl AsRef<[u8]> {
        self.to_le_bytes()
    }
}

impl AsBytes for str {
    fn as_bytes(&self) -> impl AsRef<[u8]> {
        Self::as_bytes(self)
    }
}

impl AsBytes for [u8] {
    fn as_bytes(&self) -> impl AsRef<[u8]> {
        self
    }
}

impl AsBytes for bool {
    fn as_bytes(&self) -> impl AsRef<[u8]> {
        if *self { [1u8] } else { [0u8] }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_i32_as_bytes() {
        let val = 0x12345678i32;
        let bytes = val.as_bytes();
        assert_eq!(bytes.as_ref().len(), 4);
        // Check that we get the expected bytes in little-endian format
        assert_eq!(bytes.as_ref(), &[0x78, 0x56, 0x34, 0x12]);
    }

    #[test]
    fn test_i64_as_bytes() {
        let val = 0x123456789ABCDEF0i64;
        let bytes = val.as_bytes();
        assert_eq!(bytes.as_ref().len(), 8);
        // Check that we get the expected bytes in little-endian format
        assert_eq!(
            bytes.as_ref(),
            &[0xF0, 0xDE, 0xBC, 0x9A, 0x78, 0x56, 0x34, 0x12]
        );
    }

    #[test]
    fn test_f32_as_bytes() {
        let val = 1.0f32;
        let bytes = val.as_bytes();
        assert_eq!(bytes.as_ref().len(), 4);
        // f32 representation of 1.0 is [0x00, 0x00, 0x80, 0x3F] in little-endian
        assert_eq!(bytes.as_ref(), &[0x00, 0x00, 0x80, 0x3F]);
    }

    #[test]
    fn test_f64_as_bytes() {
        let val = 1.0f64;
        let bytes = val.as_bytes();
        assert_eq!(bytes.as_ref().len(), 8);
        // f64 representation of 1.0 is [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F] in little-endian
        assert_eq!(
            bytes.as_ref(),
            &[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F]
        );
    }

    #[test]
    fn test_str_as_bytes() {
        let val = "hello";
        let bytes = AsBytes::as_bytes(val);
        assert_eq!(bytes.as_ref(), b"hello");
    }

    #[test]
    fn test_slice_as_bytes() {
        let val: &[u8] = &[1, 2, 3, 4, 5];
        let bytes = val.as_bytes();
        assert_eq!(bytes.as_ref(), &[1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_bool_as_bytes() {
        let val_true = true;
        let bytes_true = val_true.as_bytes();
        assert_eq!(bytes_true.as_ref(), &[1u8]);

        let val_false = false;
        let bytes_false = val_false.as_bytes();
        assert_eq!(bytes_false.as_ref(), &[0u8]);
    }
}
