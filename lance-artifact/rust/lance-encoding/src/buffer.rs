// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Utilities for byte arrays

use std::{ops::Deref, panic::RefUnwindSafe, ptr::NonNull, sync::Arc};

use arrow_buffer::{ArrowNativeType, Buffer, MutableBuffer, ScalarBuffer};
use lance_core::{Error, Result, utils::bit::is_pwr_two};
use std::borrow::Cow;

/// A copy-on-write byte buffer.
///
/// It wraps arrow_buffer::Buffer which provides:
/// - Cheap cloning (reference counted)
/// - Zero-copy slicing
/// - Automatic memory alignment
///
/// LanceBuffer is designed to be used in situations where you might need to
/// pass around byte buffers efficiently without worrying about ownership.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LanceBuffer(Buffer);

impl LanceBuffer {
    /// Convert into an Arrow buffer. Never copies data.
    pub fn into_buffer(self) -> Buffer {
        self.0
    }

    /// Returns a buffer of the given size with all bits set to 0
    pub fn all_unset(len: usize) -> Self {
        Self(Buffer::from_vec(vec![0; len]))
    }

    /// Returns a buffer of the given size with all bits set to 1
    pub fn all_set(len: usize) -> Self {
        Self(Buffer::from_vec(vec![0xff; len]))
    }

    /// Creates an empty buffer
    pub fn empty() -> Self {
        Self(Buffer::from_vec(Vec::<u8>::new()))
    }

    /// Converts the buffer into a hex string
    pub fn as_hex(&self) -> String {
        hex::encode_upper(self)
    }

    /// Combine multiple buffers into a single buffer
    ///
    /// This does involve a data copy (and allocation of a new buffer)
    pub fn concat(buffers: &[Self]) -> Self {
        let total_len = buffers.iter().map(|b| b.len()).sum();
        let mut data = Vec::with_capacity(total_len);
        for buffer in buffers {
            data.extend_from_slice(buffer.as_ref());
        }
        Self(Buffer::from_vec(data))
    }

    /// Converts the buffer into a hex string, inserting a space
    /// between words
    pub fn as_spaced_hex(&self, bytes_per_word: u32) -> String {
        let hex = self.as_hex();
        let chars_per_word = bytes_per_word as usize * 2;
        let num_words = hex.len() / chars_per_word;
        let mut spaced_hex = String::with_capacity(hex.len() + num_words);
        for (i, c) in hex.chars().enumerate() {
            if i % chars_per_word == 0 && i != 0 {
                spaced_hex.push(' ');
            }
            spaced_hex.push(c);
        }
        spaced_hex
    }

    /// Create a LanceBuffer from a bytes::Bytes object
    ///
    /// The alignment must be specified (as `bytes_per_value`) since we want to make
    /// sure we can safely reinterpret the buffer.
    ///
    /// If the buffer is properly aligned this will be zero-copy.  If not, a copy
    /// will be made.
    ///
    /// If `bytes_per_value` is not a power of two, then we assume the buffer is
    /// never going to be reinterpret into another type and we can safely
    /// ignore the alignment.
    ///
    /// This is a zero-copy operation when the buffer is properly aligned.
    pub fn from_bytes(bytes: bytes::Bytes, bytes_per_value: u64) -> Self {
        if is_pwr_two(bytes_per_value) && bytes.as_ptr().align_offset(bytes_per_value as usize) != 0
        {
            // The original buffer is not aligned, cannot zero-copy
            let mut buf = Vec::with_capacity(bytes.len());
            buf.extend_from_slice(&bytes);
            Self(Buffer::from_vec(buf))
        } else {
            // The original buffer is aligned, can zero-copy
            // SAFETY: the alignment is correct we can make this conversion
            unsafe {
                Self(Buffer::from_custom_allocation(
                    NonNull::new(bytes.as_ptr() as _).expect("should be a valid pointer"),
                    bytes.len(),
                    Arc::new(bytes),
                ))
            }
        }
    }

    /// Make an owned copy of the buffer (always does a copy of the data)
    pub fn deep_copy(&self) -> Self {
        Self(Buffer::from_vec(self.0.to_vec()))
    }

    /// Reinterprets a `Vec<T>` as a LanceBuffer
    ///
    /// This is a zero-copy operation. We can safely reinterpret `Vec<T>` into `&[u8]` which is what happens here.
    /// However, we cannot safely reinterpret a `Vec<T>` into a `Vec<u8>` in rust due to alignment constraints
    /// from [`Vec::from_raw_parts`]:
    ///
    /// > `T` needs to have the same alignment as what `ptr` was allocated with.
    /// > (`T` having a less strict alignment is not sufficient, the alignment really
    /// > needs to be equal to satisfy the `dealloc` requirement that memory must be
    /// > allocated and deallocated with the same layout.)
    pub fn reinterpret_vec<T: ArrowNativeType>(vec: Vec<T>) -> Self {
        Self(Buffer::from_vec(vec))
    }

    /// Reinterprets `Arc<[T]>` as a LanceBuffer
    ///
    /// This is similar to [`Self::reinterpret_vec`] but for `Arc<[T]>` instead of `Vec<T>`
    ///
    /// The same alignment constraints apply
    pub fn reinterpret_slice<T: ArrowNativeType + RefUnwindSafe>(arc: Arc<[T]>) -> Self {
        let slice = arc.as_ref();
        let data = NonNull::new(slice.as_ptr() as _).unwrap_or(NonNull::dangling());
        let len = std::mem::size_of_val(slice);
        // SAFETY: the ptr will be valid for len items if the Arc<[T]> is valid
        let buffer = unsafe { Buffer::from_custom_allocation(data, len, Arc::new(arc)) };
        Self(buffer)
    }

    /// Reinterprets a LanceBuffer into a `Vec<T>`
    ///
    /// If the underlying buffer is not properly aligned, this will involve a copy of the data
    ///
    /// Note: doing this sort of re-interpretation generally makes assumptions about the endianness
    /// of the data.  Lance does not support big-endian machines so this is safe.  However, if we end
    /// up supporting big-endian machines in the future, then any use of this method will need to be
    /// carefully reviewed.
    pub fn borrow_to_typed_slice<T: ArrowNativeType>(&self) -> ScalarBuffer<T> {
        let align = std::mem::align_of::<T>();
        let is_aligned = self.as_ptr().align_offset(align) == 0;
        if !self.len().is_multiple_of(std::mem::size_of::<T>()) {
            panic!(
                "attempt to borrow_to_typed_slice to data type of size {} but we have {} bytes which isn't evenly divisible",
                std::mem::size_of::<T>(),
                self.len()
            );
        }

        if is_aligned {
            ScalarBuffer::<T>::from(self.clone().into_buffer())
        } else {
            let num_values = self.len() / std::mem::size_of::<T>();
            let vec = Vec::<T>::with_capacity(num_values);
            let mut bytes = MutableBuffer::from(vec);
            bytes.extend_from_slice(self);
            ScalarBuffer::<T>::from(Buffer::from(bytes))
        }
    }

    /// Reinterprets a LanceBuffer into a `&[T]`
    ///
    /// Unlike [`Self::borrow_to_typed_slice`], this function returns a `Cow<'_, [T]>` instead of an owned
    /// buffer. It saves the cost of Arc creation and destruction, which can be really helpful when
    /// we borrow data and just drop it without reusing it.
    ///
    /// Caller should decide which way to use based on their own needs.
    ///
    /// If the underlying buffer is not properly aligned, this will involve a copy of the data
    ///
    /// Note: doing this sort of re-interpretation generally makes assumptions about the endianness
    /// of the data.  Lance does not support big-endian machines so this is safe.  However, if we end
    /// up supporting big-endian machines in the future, then any use of this method will need to be
    /// carefully reviewed.
    pub fn borrow_to_typed_view<T: ArrowNativeType + bytemuck::Pod>(&self) -> Cow<'_, [T]> {
        let align = std::mem::align_of::<T>();
        if !self.len().is_multiple_of(std::mem::size_of::<T>()) {
            panic!(
                "attempt to view data type of size {} but we have {} bytes which isn't evenly divisible",
                std::mem::size_of::<T>(),
                self.len()
            );
        }

        if self.as_ptr().align_offset(align) == 0 {
            Cow::Borrowed(bytemuck::cast_slice(&self.0))
        } else {
            Cow::Owned(bytemuck::pod_collect_to_vec(self.0.as_slice()))
        }
    }

    /// Concatenates multiple buffers into a single buffer, consuming the input buffers
    ///
    /// If there is only one buffer, it will be returned as is
    pub fn concat_into_one(buffers: Vec<Self>) -> Self {
        if buffers.len() == 1 {
            return buffers.into_iter().next().unwrap();
        }

        let mut total_len = 0;
        for buffer in &buffers {
            total_len += buffer.len();
        }

        let mut data = Vec::with_capacity(total_len);
        for buffer in buffers {
            data.extend_from_slice(buffer.as_ref());
        }

        Self(Buffer::from_vec(data))
    }

    /// Zips multiple buffers into a single buffer, consuming the input buffers
    ///
    /// Unlike concat_into_one this "zips" the buffers, interleaving the values
    pub fn zip_into_one(buffers: Vec<(Self, u64)>, num_values: u64) -> Result<Self> {
        let bytes_per_value = buffers.iter().map(|(_, bits_per_value)| {
            if bits_per_value % 8 == 0 {
                Ok(bits_per_value / 8)
            } else {
                Err(Error::invalid_input_source(format!("LanceBuffer::zip_into_one only supports full-byte buffers currently and received a buffer with {} bits per value", bits_per_value).into()))
            }
        }).collect::<Result<Vec<_>>>()?;
        let total_bytes_per_value = bytes_per_value.iter().sum::<u64>();
        let total_bytes = (total_bytes_per_value * num_values) as usize;

        let mut zipped = vec![0_u8; total_bytes];
        let mut buffer_ptrs = buffers
            .iter()
            .zip(bytes_per_value)
            .map(|((buffer, _), bytes_per_value)| (buffer.as_ptr(), bytes_per_value as usize))
            .collect::<Vec<_>>();

        let mut zipped_ptr = zipped.as_mut_ptr();
        unsafe {
            let end = zipped_ptr.add(total_bytes);
            while zipped_ptr < end {
                for (buf, bytes_per_value) in buffer_ptrs.iter_mut() {
                    std::ptr::copy_nonoverlapping(*buf, zipped_ptr, *bytes_per_value);
                    zipped_ptr = zipped_ptr.add(*bytes_per_value);
                    *buf = buf.add(*bytes_per_value);
                }
            }
        }

        Ok(Self(Buffer::from_vec(zipped)))
    }

    /// Create a LanceBuffer from a slice
    ///
    /// This is NOT a zero-copy operation.  We can't create a borrowed buffer because
    /// we have no way of extending the lifetime of the slice.
    pub fn copy_slice(slice: &[u8]) -> Self {
        Self(Buffer::from_vec(slice.to_vec()))
    }

    /// Create a LanceBuffer from an array (fixed-size slice)
    ///
    /// This is NOT a zero-copy operation.  The slice memory could be on the stack and
    /// thus we can't forget it.
    pub fn copy_array<const N: usize>(array: [u8; N]) -> Self {
        Self(Buffer::from_vec(Vec::from(array)))
    }

    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns a new [LanceBuffer] that is a slice of this buffer starting at `offset`,
    /// with `length` bytes.
    /// Doing so allows the same memory region to be shared between lance buffers.
    /// # Panics
    /// Panics if `(offset + length)` is larger than the existing length.
    pub fn slice_with_length(&self, offset: usize, length: usize) -> Self {
        let original_buffer_len = self.len();
        assert!(
            offset.saturating_add(length) <= original_buffer_len,
            "the offset + length of the sliced Buffer cannot exceed the existing length"
        );
        Self(self.0.slice_with_length(offset, length))
    }

    /// Returns a new [LanceBuffer] that is a slice of this buffer starting at bit `offset`
    /// with `length` bits.
    ///
    /// Unlike `slice_with_length`, this method allows for slicing at a bit level but always
    /// requires a copy of the data (unless offset is byte-aligned)
    ///
    /// This method performs the bit slice using the Arrow convention of *bitwise* little-endian
    ///
    /// This means, given the bit buffer 0bABCDEFGH_HIJKLMNOP and the slice starting at bit 3 and
    /// with length 8, the result will be 0bNOPABCDE
    pub fn bit_slice_le_with_length(&self, offset: usize, length: usize) -> Self {
        let sliced = self.0.bit_slice(offset, length);
        Self(sliced)
    }

    /// Get a pointer to the underlying data
    pub fn as_ptr(&self) -> *const u8 {
        self.0.as_ptr()
    }
}

impl AsRef<[u8]> for LanceBuffer {
    fn as_ref(&self) -> &[u8] {
        self.0.as_slice()
    }
}

impl Deref for LanceBuffer {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

// All `From` implementations are zero-copy

impl From<Vec<u8>> for LanceBuffer {
    fn from(buffer: Vec<u8>) -> Self {
        Self(Buffer::from_vec(buffer))
    }
}

impl From<Buffer> for LanceBuffer {
    fn from(buffer: Buffer) -> Self {
        Self(buffer)
    }
}

// An iterator that keeps a clone of a borrowed LanceBuffer so we
// can have a 'static lifetime
pub struct LanceBufferIter {
    buffer: Buffer,
    index: usize,
}

impl Iterator for LanceBufferIter {
    type Item = u8;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.buffer.len() {
            None
        } else {
            // SAFETY: we just checked that index is in bounds
            let byte = unsafe { self.buffer.get_unchecked(self.index) };
            self.index += 1;
            Some(*byte)
        }
    }
}

impl IntoIterator for LanceBuffer {
    type Item = u8;
    type IntoIter = LanceBufferIter;

    fn into_iter(self) -> Self::IntoIter {
        LanceBufferIter {
            buffer: self.0,
            index: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use arrow_buffer::Buffer;

    use super::LanceBuffer;

    #[test]
    fn test_eq() {
        let buf = LanceBuffer::from(Buffer::from_vec(vec![1_u8, 2, 3]));
        let buf2 = LanceBuffer::from(vec![1, 2, 3]);
        assert_eq!(buf, buf2);
    }

    #[test]
    fn test_reinterpret_vec() {
        let vec = vec![1_u32, 2, 3];
        let buf = LanceBuffer::reinterpret_vec(vec);

        let mut expected = Vec::with_capacity(12);
        expected.extend_from_slice(&1_u32.to_ne_bytes());
        expected.extend_from_slice(&2_u32.to_ne_bytes());
        expected.extend_from_slice(&3_u32.to_ne_bytes());
        let expected = LanceBuffer::from(expected);

        assert_eq!(expected, buf);
        assert_eq!(buf.borrow_to_typed_slice::<u32>().as_ref(), vec![1, 2, 3]);
    }

    #[test]
    fn test_concat() {
        let buf1 = LanceBuffer::from(vec![1_u8, 2, 3]);
        let buf2 = LanceBuffer::from(vec![4_u8, 5, 6]);
        let buf3 = LanceBuffer::from(vec![7_u8, 8, 9]);

        let expected = LanceBuffer::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9]);
        assert_eq!(
            expected,
            LanceBuffer::concat_into_one(vec![buf1, buf2, buf3])
        );

        let empty = LanceBuffer::empty();
        assert_eq!(
            LanceBuffer::empty(),
            LanceBuffer::concat_into_one(vec![empty])
        );

        let expected = LanceBuffer::from(vec![1, 2, 3]);
        assert_eq!(
            expected,
            LanceBuffer::concat_into_one(vec![expected.deep_copy(), LanceBuffer::empty()])
        );
    }

    #[test]
    fn test_zip() {
        let buf1 = LanceBuffer::from(vec![1_u8, 2, 3]);
        let buf2 = LanceBuffer::reinterpret_vec(vec![1_u16, 2, 3]);
        let buf3 = LanceBuffer::reinterpret_vec(vec![1_u32, 2, 3]);

        let zipped = LanceBuffer::zip_into_one(vec![(buf1, 8), (buf2, 16), (buf3, 32)], 3).unwrap();

        assert_eq!(zipped.len(), 21);

        let mut expected = Vec::with_capacity(21);
        for i in 1..4 {
            expected.push(i as u8);
            expected.extend_from_slice(&(i as u16).to_ne_bytes());
            expected.extend_from_slice(&(i as u32).to_ne_bytes());
        }
        let expected = LanceBuffer::from(expected);

        assert_eq!(expected, zipped);
    }

    #[test]
    fn test_hex() {
        let buf = LanceBuffer::from(vec![1, 2, 15, 20]);
        assert_eq!("01020F14", buf.as_hex());
    }

    #[test]
    #[should_panic]
    fn test_to_typed_slice_invalid() {
        let buf = LanceBuffer::from(vec![0, 1, 2]);
        buf.borrow_to_typed_slice::<u16>();
    }

    #[test]
    fn test_to_typed_slice() {
        // Buffer is aligned, no copy will be made, both calls
        // should get same ptr
        let buf = LanceBuffer::from(vec![0, 1]);
        let borrow = buf.borrow_to_typed_slice::<u16>();
        let view_ptr = borrow.as_ref().as_ptr();
        let borrow2 = buf.borrow_to_typed_slice::<u16>();
        let view_ptr2 = borrow2.as_ref().as_ptr();

        assert_eq!(view_ptr, view_ptr2);

        let bytes = bytes::Bytes::from(vec![0, 1, 2]);
        let sliced = bytes.slice(1..3);
        // Intentionally LYING about alignment here to trigger test
        let buf = LanceBuffer::from_bytes(sliced, 1);
        let borrow = buf.borrow_to_typed_slice::<u16>();
        let view_ptr = borrow.as_ref().as_ptr();
        let borrow2 = buf.borrow_to_typed_slice::<u16>();
        let view_ptr2 = borrow2.as_ref().as_ptr();

        assert_ne!(view_ptr, view_ptr2);
    }

    #[test]
    fn test_bit_slice_le() {
        let buf = LanceBuffer::from(vec![0x0F, 0x0B]);

        // Keep in mind that validity buffers are *bitwise* little-endian
        assert_eq!(buf.bit_slice_le_with_length(0, 4).as_ref(), &[0x0F]);
        assert_eq!(buf.bit_slice_le_with_length(4, 4).as_ref(), &[0x00]);
        assert_eq!(buf.bit_slice_le_with_length(3, 8).as_ref(), &[0x61]);
        assert_eq!(buf.bit_slice_le_with_length(0, 8).as_ref(), &[0x0F]);
        assert_eq!(buf.bit_slice_le_with_length(4, 8).as_ref(), &[0xB0]);
        assert_eq!(buf.bit_slice_le_with_length(4, 12).as_ref(), &[0xB0, 0x00]);
    }
}
