// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Flatten response batches once, releasing each source buffer as it is copied.

use std::collections::VecDeque;

use arrow_array::{Array, LargeBinaryArray};
use arrow_buffer::{BooleanBuffer, Buffer, NullBuffer, OffsetBuffer};
use arrow_schema::ArrowError;

fn allocation_error(error: impl std::fmt::Display) -> ArrowError {
    ArrowError::ComputeError(format!("cannot allocate blob result: {error}"))
}

pub(super) struct BlobAccumulator {
    offsets: Vec<i64>,
    validity: Vec<u8>,
    values: VecDeque<Buffer>,
    bytes: i64,
    has_nulls: bool,
}

impl BlobAccumulator {
    pub(super) fn new(rows: usize) -> Result<Self, ArrowError> {
        let capacity = rows
            .checked_add(1)
            .ok_or_else(|| allocation_error("row count overflow"))?;
        let mut offsets = Vec::new();
        offsets
            .try_reserve_exact(capacity)
            .map_err(allocation_error)?;
        offsets.push(0);
        let mut validity = Vec::new();
        let validity_bytes = rows / 8 + usize::from(rows % 8 != 0);
        validity
            .try_reserve_exact(validity_bytes)
            .map_err(allocation_error)?;
        Ok(Self {
            offsets,
            validity,
            values: VecDeque::new(),
            bytes: 0,
            has_nulls: false,
        })
    }

    pub(super) fn append(&mut self, array: &LargeBinaryArray) -> Result<(), ArrowError> {
        self.append_inner(array, false)
    }

    /// The first value continues the preceding response's final logical range.
    pub(super) fn append_continuation(
        &mut self,
        array: &LargeBinaryArray,
    ) -> Result<(), ArrowError> {
        self.append_inner(array, true)
    }

    fn append_inner(
        &mut self,
        array: &LargeBinaryArray,
        continuation: bool,
    ) -> Result<(), ArrowError> {
        let rows = self.offsets.len() - 1;
        if continuation
            && (rows == 0
                || array.is_empty()
                || (self.validity[(rows - 1) / 8] & (1 << ((rows - 1) % 8)) != 0)
                    != array.is_valid(0))
        {
            return Err(ArrowError::ComputeError(
                "inconsistent nulls in split blob range".into(),
            ));
        }
        let offsets = array.value_offsets();
        let start = offsets[0];
        let length = offsets[array.len()] - start;
        let total = self.bytes.checked_add(length).ok_or_else(|| {
            ArrowError::ComputeError("blob result exceeds i64 byte offsets".into())
        })?;
        let skip = usize::from(continuation);
        self.offsets
            .try_reserve(array.len() - skip)
            .map_err(allocation_error)?;
        let new_rows = rows
            .checked_add(array.len() - skip)
            .ok_or_else(|| allocation_error("row count overflow"))?;
        let validity_bytes = new_rows / 8 + usize::from(new_rows % 8 != 0);
        self.validity
            .try_reserve(validity_bytes - self.validity.len())
            .map_err(allocation_error)?;
        self.validity.resize(validity_bytes, 0);
        if length != 0 {
            self.values.try_reserve(1).map_err(allocation_error)?;
            self.values.push_back(
                array
                    .values()
                    .slice_with_length(start as usize, length as usize),
            );
        }
        if continuation {
            *self.offsets.last_mut().unwrap() = self.bytes + (offsets[1] - start);
        }
        self.offsets.extend(
            offsets[1 + skip..]
                .iter()
                .map(|offset| self.bytes + (*offset - start)),
        );
        for i in skip..array.len() {
            let row = rows + i - skip;
            if array.is_valid(i) {
                self.validity[row / 8] |= 1 << (row % 8);
            } else {
                self.has_nulls = true;
            }
        }
        self.bytes = total;
        Ok(())
    }

    pub(super) fn finish(mut self) -> Result<LargeBinaryArray, ArrowError> {
        let values = match self.values.len() {
            0 => Buffer::default(),
            1 => self.values.pop_front().unwrap(),
            _ => {
                let length = usize::try_from(self.bytes).map_err(allocation_error)?;
                let mut values = Vec::new();
                values.try_reserve_exact(length).map_err(allocation_error)?;
                // Drop each source allocation immediately after copying its bytes.
                while let Some(chunk) = self.values.pop_front() {
                    values.extend_from_slice(&chunk);
                }
                Buffer::from_vec(values)
            }
        };
        let rows = self.offsets.len() - 1;
        let nulls = self
            .has_nulls
            .then(|| NullBuffer::new(BooleanBuffer::new(Buffer::from_vec(self.validity), 0, rows)));
        Ok(LargeBinaryArray::new(
            OffsetBuffer::new(self.offsets.into()),
            values,
            nulls,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_buffer_is_not_copied() {
        let array = LargeBinaryArray::from(vec![Some(b"alpha".as_slice()), None, Some(b"")]);
        let mut result = BlobAccumulator::new(3).unwrap();
        result.append(&array).unwrap();
        let output = result.finish().unwrap();
        assert_eq!(output.values().as_ptr(), array.values().as_ptr());
        assert_eq!(output, array);
    }

    #[test]
    fn slices_batches_nulls_and_empty_values_keep_order() {
        let array = LargeBinaryArray::from(vec![
            Some(b"unused".as_slice()),
            Some(b"alpha"),
            None,
            Some(b""),
            Some(b"beta"),
        ]);
        let mut result = BlobAccumulator::new(6).unwrap();
        result.append(&array.slice(1, 3)).unwrap();
        result.append(&array.slice(4, 1)).unwrap();
        result.append(&array.slice(2, 2)).unwrap();
        assert_eq!(
            result.finish().unwrap(),
            LargeBinaryArray::from(vec![
                Some(b"alpha".as_slice()),
                None,
                Some(b""),
                Some(b"beta"),
                None,
                Some(b"")
            ])
        );
        assert_eq!(BlobAccumulator::new(0).unwrap().finish().unwrap().len(), 0);
    }

    #[test]
    fn impossible_capacity_is_an_error() {
        assert!(BlobAccumulator::new(usize::MAX).is_err());
        assert!(BlobAccumulator::new(usize::MAX / 2).is_err());
    }
}
