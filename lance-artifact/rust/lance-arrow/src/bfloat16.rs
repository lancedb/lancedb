// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! bfloat16 support for Apache Arrow.

use std::fmt::Formatter;
use std::slice;

use arrow_array::{Array, FixedSizeBinaryArray, builder::BooleanBufferBuilder};
use arrow_buffer::{Buffer, MutableBuffer};
use arrow_data::ArrayData;
use arrow_schema::{ArrowError, DataType, Field as ArrowField};
use half::bf16;

use crate::{ARROW_EXT_NAME_KEY, FloatArray};

/// The name of the bfloat16 extension in Arrow metadata
pub const BFLOAT16_EXT_NAME: &str = "lance.bfloat16";

/// Check whether the given field is a bfloat16 field
///
/// A field is a bfloat16 field if it has a data type of `FixedSizeBinary(2)` and the metadata
/// contains the bfloat16 extension name.
pub fn is_bfloat16_field(field: &ArrowField) -> bool {
    field.data_type() == &DataType::FixedSizeBinary(2)
        && field
            .metadata()
            .get(ARROW_EXT_NAME_KEY)
            .map(|name| name == BFLOAT16_EXT_NAME)
            .unwrap_or_default()
}

/// The bfloat16 data type
///
/// This implements the [`ArrowFloatType`](crate::floats::ArrowFloatType) trait for bfloat16 values.
#[derive(Debug)]
pub struct BFloat16Type {}

/// An array of bfloat16 values
///
/// Note that bfloat16 is not the same thing as fp16 which is supported natively by arrow-rs.
#[derive(Clone)]
pub struct BFloat16Array {
    inner: FixedSizeBinaryArray,
}

impl std::fmt::Debug for BFloat16Array {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "BFloat16Array\n[\n")?;
        from_arrow::print_long_array(&self.inner, f, |array, i, f| {
            if array.is_null(i) {
                write!(f, "null")
            } else {
                let binary_values = array.value(i);
                let value =
                    bf16::from_bits(u16::from_le_bytes([binary_values[0], binary_values[1]]));
                write!(f, "{:?}", value)
            }
        })?;
        write!(f, "]")
    }
}

impl BFloat16Array {
    pub fn from_iter_values(iter: impl IntoIterator<Item = bf16>) -> Self {
        let values: Vec<bf16> = iter.into_iter().collect();
        values.into()
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn is_null(&self, i: usize) -> bool {
        self.inner.is_null(i)
    }

    pub fn null_count(&self) -> usize {
        self.inner.null_count()
    }

    pub fn iter(&self) -> BFloat16Iter<'_> {
        BFloat16Iter {
            array: self,
            index: 0,
        }
    }

    pub fn value(&self, i: usize) -> bf16 {
        assert!(
            i < self.len(),
            "Trying to access an element at index {} from a BFloat16Array of length {}",
            i,
            self.len()
        );
        // Safety:
        // `i < self.len()
        unsafe { self.value_unchecked(i) }
    }

    /// # Safety
    /// Caller must ensure that `i < self.len()`
    pub unsafe fn value_unchecked(&self, i: usize) -> bf16 {
        let binary_value = self.inner.value_unchecked(i);
        bf16::from_bits(u16::from_le_bytes([binary_value[0], binary_value[1]]))
    }

    pub fn into_inner(self) -> FixedSizeBinaryArray {
        self.inner
    }
}

impl FromIterator<Option<bf16>> for BFloat16Array {
    fn from_iter<I: IntoIterator<Item = Option<bf16>>>(iter: I) -> Self {
        let mut buffer = MutableBuffer::new(10);
        // No null buffer builder :(
        let mut nulls = BooleanBufferBuilder::new(10);
        let mut len = 0;

        for maybe_value in iter {
            if let Some(value) = maybe_value {
                let bytes = value.to_le_bytes();
                buffer.extend(bytes);
            } else {
                buffer.extend([0u8, 0u8]);
            }
            nulls.append(maybe_value.is_some());
            len += 1;
        }

        let null_buffer = nulls.finish();
        let num_valid = null_buffer.count_set_bits();
        let null_buffer = if num_valid == len {
            None
        } else {
            Some(null_buffer.into_inner())
        };

        let array_data = ArrayData::builder(DataType::FixedSizeBinary(2))
            .len(len)
            .add_buffer(buffer.into())
            .null_bit_buffer(null_buffer);
        // SAFETY: the value buffer contains exactly `2 * len` bytes (two bytes
        // pushed per iteration of the loop above, including the zero-fill for
        // null slots), which matches the `FixedSizeBinary(2)` storage layout.
        // The null bit buffer, when present, has `len` bits appended above, so
        // its length covers the array's logical range.
        let array_data = unsafe { array_data.build_unchecked() };
        Self {
            inner: FixedSizeBinaryArray::from(array_data),
        }
    }
}

impl FromIterator<bf16> for BFloat16Array {
    fn from_iter<I: IntoIterator<Item = bf16>>(iter: I) -> Self {
        Self::from_iter_values(iter)
    }
}

impl From<Vec<bf16>> for BFloat16Array {
    fn from(data: Vec<bf16>) -> Self {
        let len = data.len();
        // Zero-copy: `bf16` is `#[repr(transparent)]` over `u16` and derives
        // `bytemuck::Pod`, so `cast_vec` reinterprets the allocation in place —
        // no per-element copy or heap alloc. The crate-root `compile_error!`
        // pins `target_endian = "little"`, so the resulting bytes match the
        // `FixedSizeBinary(2)` on-disk order Lance writes elsewhere.
        let raw: Vec<u16> = bytemuck::cast_vec(data);
        let array_data = ArrayData::builder(DataType::FixedSizeBinary(2))
            .len(len)
            .add_buffer(Buffer::from_vec(raw));
        // SAFETY: the value buffer contains exactly `2 * len` bytes — one
        // `u16` per element after the layout-compatible cast — matching the
        // `FixedSizeBinary(2)` storage layout. No null buffer is attached, so
        // every element is logically valid.
        let array_data = unsafe { array_data.build_unchecked() };
        Self {
            inner: FixedSizeBinaryArray::from(array_data),
        }
    }
}

impl TryFrom<FixedSizeBinaryArray> for BFloat16Array {
    type Error = ArrowError;

    fn try_from(value: FixedSizeBinaryArray) -> Result<Self, Self::Error> {
        if value.value_length() == 2 {
            Ok(Self { inner: value })
        } else {
            Err(ArrowError::InvalidArgumentError(
                "FixedSizeBinaryArray must have a value length of 2".to_string(),
            ))
        }
    }
}

impl PartialEq<Self> for BFloat16Array {
    fn eq(&self, other: &Self) -> bool {
        self.inner.eq(&other.inner)
    }
}

pub struct BFloat16Iter<'a> {
    array: &'a BFloat16Array,
    index: usize,
}

impl<'a> Iterator for BFloat16Iter<'a> {
    type Item = Option<bf16>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.array.len() {
            return None;
        }
        let i = self.index;
        self.index += 1;
        if self.array.is_null(i) {
            Some(None)
        } else {
            Some(Some(self.array.value(i)))
        }
    }
}

/// Methods that are lifted from arrow-rs temporarily until they are made public.
mod from_arrow {
    use arrow_array::Array;

    /// Helper function for printing potentially long arrays.
    pub(super) fn print_long_array<A, F>(
        array: &A,
        f: &mut std::fmt::Formatter,
        print_item: F,
    ) -> std::fmt::Result
    where
        A: Array,
        F: Fn(&A, usize, &mut std::fmt::Formatter) -> std::fmt::Result,
    {
        let head = std::cmp::min(10, array.len());

        for i in 0..head {
            if array.is_null(i) {
                writeln!(f, "  null,")?;
            } else {
                write!(f, "  ")?;
                print_item(array, i, f)?;
                writeln!(f, ",")?;
            }
        }
        if array.len() > 10 {
            if array.len() > 20 {
                writeln!(f, "  ...{} elements...,", array.len() - 20)?;
            }

            let tail = std::cmp::max(head, array.len() - 10);

            for i in tail..array.len() {
                if array.is_null(i) {
                    writeln!(f, "  null,")?;
                } else {
                    write!(f, "  ")?;
                    print_item(array, i, f)?;
                    writeln!(f, ",")?;
                }
            }
        }
        Ok(())
    }
}

impl FloatArray<BFloat16Type> for FixedSizeBinaryArray {
    type FloatType = BFloat16Type;

    /// Returns the underlying `bf16` values as a borrowed slice.
    ///
    /// # Preconditions
    ///
    /// - `value_length()` must be 2 (the `FixedSizeBinary(2)` storage shape
    ///   used by [`BFloat16Array`]). Asserted at entry.
    /// - The value buffer must be at least 2-byte aligned. Lance's in-tree
    ///   constructors always satisfy this: value buffers are built either via
    ///   `MutableBuffer` (aligned to arrow-buffer's `ALIGNMENT` constant, ≥32
    ///   bytes) or via `Buffer::from_vec::<u16>` (aligned to `align_of::<u16>()`
    ///   == 2); both meet `bf16`'s 2-byte requirement. Externally-built
    ///   `FixedSizeBinaryArray`s arriving via FFI, IPC, or
    ///   `Buffer::from_custom_allocation` are not required by arrow-rs to be
    ///   aligned beyond a single byte; passing one to this method violates the
    ///   precondition. A `debug_assert` below catches such inputs in debug and
    ///   test builds.
    ///
    /// # Endianness
    ///
    /// `lance-arrow` is gated on `target_endian = "little"` at the crate root,
    /// so this method always returns values in the same byte order Lance writes
    /// (see [`BFloat16Array::value`] and the [`FromIterator`] impls).
    fn as_slice(&self) -> &[bf16] {
        assert_eq!(
            self.value_length(),
            2,
            "BFloat16 arrays must use FixedSizeBinary(2) storage"
        );
        debug_assert_eq!(
            (self.value_data().as_ptr() as usize) % std::mem::align_of::<bf16>(),
            0,
            "BFloat16 value buffer must be at least 2-byte aligned"
        );
        // SAFETY:
        // - The assert above pins `value_size == 2`, so `value_data().len() / 2`
        //   equals the array's logical element count.
        //   `FixedSizeBinaryArray::From<ArrayData>` constructs its value buffer
        //   as `buffers[0].slice_with_length(offset * 2, len * 2)` (arrow-array
        //   `fixed_size_binary_array.rs`), so `value_data()` already returns
        //   the offset-adjusted slice. Do not replace `value_data()` with an
        //   accessor that returns the un-sliced backing buffer.
        // - `bf16` is `#[repr(transparent)]` over `u16` (size 2, alignment 2);
        //   every `u16` bit pattern is a valid `bf16`, so any byte content
        //   yields a defined value — never UB.
        // - Alignment is the caller's responsibility per the precondition
        //   documented above. The `debug_assert_eq!` immediately preceding this
        //   block catches violations in debug and test builds only — release
        //   builds rely on callers honoring the precondition. arrow-rs
        //   declares `FixedSizeBinary(n)`'s
        //   `BufferSpec::FixedWidth { alignment: align_of::<u8>() == 1 }`
        //   (arrow-data `data.rs`), so arrow-rs alone does not guarantee
        //   2-byte alignment. Lance's in-tree construction paths build value
        //   buffers via `MutableBuffer` (arrow-buffer `ALIGNMENT` constant,
        //   ≥32 bytes) or `Buffer::from_vec::<u16>` (2-byte aligned), both of
        //   which satisfy `bf16`'s 2-byte requirement.
        // - The returned slice borrows from `self`; the underlying ref-counted,
        //   immutable Arrow buffer cannot be mutated or freed for the slice's
        //   lifetime.
        unsafe {
            slice::from_raw_parts(
                self.value_data().as_ptr() as *const bf16,
                self.value_data().len() / 2,
            )
        }
    }

    fn from_values(values: Vec<bf16>) -> Self {
        BFloat16Array::from(values).into_inner()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basics() {
        let values: Vec<f32> = vec![1.0, 2.0, 3.0];
        let values: Vec<bf16> = values.iter().map(|v| bf16::from_f32(*v)).collect();

        let array = BFloat16Array::from_iter_values(values.clone());
        let array2 = BFloat16Array::from(values.clone());
        assert_eq!(array, array2);
        assert_eq!(array.len(), 3);

        // Pin the raw little-endian bytes emitted by `From<Vec<bf16>>` (rewritten to
        // reinterpret the Vec via `bytemuck::cast_vec`), so a layout/byte-order
        // regression is caught directly rather than only through Debug formatting.
        // bf16 is the high 16 bits of the f32: 1.0->0x3F80, 2.0->0x4000, 3.0->0x4040.
        let inner = array2.clone().into_inner();
        let raw_bytes: Vec<u8> = (0..inner.len())
            .flat_map(|i| inner.value(i).to_vec())
            .collect();
        assert_eq!(raw_bytes, vec![0x80, 0x3F, 0x00, 0x40, 0x40, 0x40]);

        let expected_fmt = "BFloat16Array\n[\n  1.0,\n  2.0,\n  3.0,\n]";
        assert_eq!(expected_fmt, format!("{:?}", array));

        for (expected, value) in values.iter().zip(array.iter()) {
            assert_eq!(Some(*expected), value);
        }

        for (expected, value) in values.as_slice().iter().zip(array2.iter()) {
            assert_eq!(Some(*expected), value);
        }

        let arrow_array = array.into_inner();
        assert_eq!(arrow_array.as_slice(), values.as_slice());
    }

    #[test]
    fn test_nulls() {
        let values: Vec<Option<bf16>> =
            vec![Some(bf16::from_f32(1.0)), None, Some(bf16::from_f32(3.0))];
        let array = BFloat16Array::from_iter(values.clone());
        assert_eq!(array.len(), 3);
        assert_eq!(array.null_count(), 1);

        let expected_fmt = "BFloat16Array\n[\n  1.0,\n  null,\n  3.0,\n]";
        assert_eq!(expected_fmt, format!("{:?}", array));

        for (expected, value) in values.iter().zip(array.iter()) {
            assert_eq!(*expected, value);
        }
    }
}
