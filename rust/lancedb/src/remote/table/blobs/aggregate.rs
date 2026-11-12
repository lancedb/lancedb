// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Flatten response batches once, releasing each source buffer as it is copied.

use std::collections::VecDeque;

use arrow_array::{Array, LargeBinaryArray};
use arrow_buffer::{Buffer, MutableBuffer, NullBufferBuilder, OffsetBuffer};
use arrow_schema::ArrowError;

pub(super) struct BlobAccumulator {
    offsets: Vec<i64>,
    nulls: NullBufferBuilder,
    values: VecDeque<Buffer>,
    bytes: i64,
}

impl BlobAccumulator {
    pub(super) fn new(rows: usize) -> Self {
        let mut offsets = Vec::with_capacity(rows + 1);
        offsets.push(0);
        Self {
            offsets,
            nulls: NullBufferBuilder::new(rows),
            values: VecDeque::new(),
            bytes: 0,
        }
    }

    pub(super) fn append(&mut self, array: &LargeBinaryArray) -> Result<(), ArrowError> {
        let offsets = array.value_offsets();
        let start = offsets[0];
        let length = offsets[array.len()] - start;
        let total = self.bytes.checked_add(length).ok_or_else(|| {
            ArrowError::ComputeError("fetch_blobs result exceeds i64 byte offsets".into())
        })?;
        self.offsets.extend(
            offsets[1..]
                .iter()
                .map(|offset| self.bytes + (*offset - start)),
        );
        if let Some(nulls) = array.nulls() {
            self.nulls.append_buffer(nulls);
        } else {
            self.nulls.append_n_non_nulls(array.len());
        }
        if length != 0 {
            self.values.push_back(
                array
                    .values()
                    .slice_with_length(start as usize, length as usize),
            );
        }
        self.bytes = total;
        Ok(())
    }

    pub(super) fn finish(mut self) -> LargeBinaryArray {
        let values = match self.values.len() {
            0 => Buffer::default(),
            1 => self.values.pop_front().unwrap(),
            _ => {
                let mut values = MutableBuffer::new(self.bytes as usize);
                // Do not retain all decoded batches alongside the final allocation.
                // Drop each source allocation immediately after copying its bytes.
                while let Some(chunk) = self.values.pop_front() {
                    values.extend_from_slice(&chunk);
                }
                values.into()
            }
        };
        LargeBinaryArray::new(
            OffsetBuffer::new(self.offsets.into()),
            values,
            self.nulls.finish(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_buffer_is_not_copied() {
        let array = LargeBinaryArray::from(vec![Some(b"alpha".as_slice()), None, Some(b"")]);
        let mut result = BlobAccumulator::new(3);
        result.append(&array).unwrap();
        let output = result.finish();
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
        let mut result = BlobAccumulator::new(6);
        result.append(&array.slice(1, 3)).unwrap();
        result.append(&array.slice(4, 1)).unwrap();
        result.append(&array.slice(2, 2)).unwrap();
        assert_eq!(
            result.finish(),
            LargeBinaryArray::from(vec![
                Some(b"alpha".as_slice()),
                None,
                Some(b""),
                Some(b"beta"),
                None,
                Some(b""),
            ])
        );
        assert_eq!(BlobAccumulator::new(0).finish().len(), 0);
    }
}
