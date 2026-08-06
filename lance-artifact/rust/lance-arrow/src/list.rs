// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::sync::Arc;

use arrow_array::{Array, BooleanArray, GenericListArray, OffsetSizeTrait};
use arrow_buffer::{BooleanBufferBuilder, OffsetBuffer, ScalarBuffer};
use arrow_schema::Field;

pub trait ListArrayExt {
    /// Filters out masked null items from the list array
    ///
    /// It is legal for a list array to have a null entry with a non-zero length.  The
    /// values inside the entry are "garbage" and should be ignored.  This function
    /// filters the values array to remove the garbage values.
    ///
    /// The output list will always have zero-length nulls.
    fn filter_garbage_nulls(&self) -> Self;
    /// Returns a copy of the list's values array that has been sliced to size
    ///
    /// It is legal for a list array's offsets to not start with zero.  It's also legal
    /// for a list array's offsets to not extend to the entire values array.  This function
    /// behaves similarly to `values()` except it slices the array so that it starts at
    /// the first list offset and ends at the last list offset.
    fn trimmed_values(&self) -> Arc<dyn Array>;
    /// The offset type of the underlying list array.
    type Offset: OffsetSizeTrait;
    /// Returns offsets shifted so the first offset is zero, matching
    /// [`Self::trimmed_values`].
    ///
    /// Sliced list arrays (e.g. a filtered batch) keep offsets that reference the
    /// original values buffer, so combining them with trimmed values produces
    /// offsets that exceed the values length. Use this together with
    /// `trimmed_values` when constructing a new list array.
    fn trimmed_offsets(&self) -> OffsetBuffer<Self::Offset>;
}

impl<OffsetSize: OffsetSizeTrait> ListArrayExt for GenericListArray<OffsetSize> {
    fn filter_garbage_nulls(&self) -> Self {
        if self.is_empty() {
            return self.clone();
        }
        let Some(validity) = self.nulls().cloned() else {
            return self.clone();
        };

        let mut should_keep = BooleanBufferBuilder::new(self.values().len());

        // Handle case where offsets do not start at 0
        let preamble_len = self.offsets().first().unwrap().to_usize().unwrap();
        should_keep.append_n(preamble_len, false);

        let mut new_offsets: Vec<OffsetSize> = Vec::with_capacity(self.len() + 1);
        new_offsets.push(OffsetSize::zero());
        let mut cur_len = OffsetSize::zero();
        for (offset, is_valid) in self.offsets().windows(2).zip(validity.iter()) {
            let len = offset[1] - offset[0];
            if is_valid {
                cur_len += len;
                should_keep.append_n(len.to_usize().unwrap(), true);
                new_offsets.push(cur_len);
            } else {
                should_keep.append_n(len.to_usize().unwrap(), false);
                new_offsets.push(cur_len);
            }
        }

        // Offsets may not reference entire values buffer
        let trailer = self.values().len() - should_keep.len();
        should_keep.append_n(trailer, false);

        let should_keep = should_keep.finish();
        let should_keep = BooleanArray::new(should_keep, None);
        let new_values = arrow_select::filter::filter(self.values(), &should_keep).unwrap();
        let new_offsets = ScalarBuffer::from(new_offsets);
        let new_offsets = OffsetBuffer::new(new_offsets);

        Self::new(
            Arc::new(Field::new(
                "item",
                self.value_type(),
                self.values().is_nullable(),
            )),
            new_offsets,
            new_values,
            Some(validity),
        )
    }

    fn trimmed_values(&self) -> Arc<dyn Array> {
        let first_value = self
            .offsets()
            .first()
            .map(|v| v.to_usize().unwrap())
            .unwrap_or(0);
        let last_value = self
            .offsets()
            .last()
            .map(|v| v.to_usize().unwrap())
            .unwrap_or(0);
        self.values().slice(first_value, last_value - first_value)
    }

    type Offset = OffsetSize;

    fn trimmed_offsets(&self) -> OffsetBuffer<OffsetSize> {
        let offsets = self.offsets();
        let Some(&first) = offsets.first() else {
            return offsets.clone();
        };
        if first == OffsetSize::zero() {
            return offsets.clone();
        }
        let shifted: Vec<OffsetSize> = offsets.iter().map(|&o| o - first).collect();
        OffsetBuffer::new(ScalarBuffer::from(shifted))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{ListArray, UInt64Array};
    use arrow_buffer::{BooleanBuffer, NullBuffer, OffsetBuffer, ScalarBuffer};
    use arrow_schema::{DataType, Field};

    use super::ListArrayExt;

    #[test]
    fn test_filter_garbage_nulls() {
        let items = UInt64Array::from(vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
        let offsets = ScalarBuffer::<i32>::from(vec![2, 5, 8, 9]);
        let offsets = OffsetBuffer::new(offsets);
        let list_validity = NullBuffer::new(BooleanBuffer::from(vec![true, false, true]));
        let list_arr = ListArray::new(
            Arc::new(Field::new("item", DataType::UInt64, true)),
            offsets,
            Arc::new(items),
            Some(list_validity.clone()),
        );

        let filtered = list_arr.filter_garbage_nulls();

        let expected_items = UInt64Array::from(vec![2, 3, 4, 8]);
        let offsets = ScalarBuffer::<i32>::from(vec![0, 3, 3, 4]);
        let expected = ListArray::new(
            Arc::new(Field::new("item", DataType::UInt64, false)),
            OffsetBuffer::new(offsets),
            Arc::new(expected_items),
            Some(list_validity),
        );

        assert_eq!(filtered, expected);
    }

    #[test]
    fn test_trim_values() {
        let items = UInt64Array::from(vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
        let offsets = ScalarBuffer::<i32>::from(vec![2, 5, 6, 8, 9]);
        let offsets = OffsetBuffer::new(offsets);
        let list_arr = ListArray::new(
            Arc::new(Field::new("item", DataType::UInt64, true)),
            offsets,
            Arc::new(items),
            None,
        );
        let list_arr = list_arr.slice(1, 2);

        let trimmed = list_arr.trimmed_values();

        let expected_items = UInt64Array::from(vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
        let expected_items = expected_items.slice(5, 3);

        assert_eq!(trimmed.as_ref(), &expected_items);
    }
}
