// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Extension to arrow struct arrays

use arrow_array::{Array, StructArray, cast::AsArray, make_array};
use arrow_buffer::NullBuffer;
use arrow_data::{ArrayData, ArrayDataBuilder};
use arrow_schema::ArrowError;

pub trait StructArrayExt {
    /// Removes the offset / length of the struct array by pushing it into the children
    ///
    /// In arrow-rs when slice is called it recursively slices the children.
    /// In arrow-cpp when slice is called it just sets the offset/length of
    ///   the struct array and leaves the children as-is
    ///
    /// Both are legal approaches (╥﹏╥)
    ///
    /// This method helps reduce complexity by folding into the arrow-rs approach
    fn normalize_slicing(&self) -> Result<Self, ArrowError>
    where
        Self: Sized;

    /// Structs are allowed to mask valid items.  For example, a struct array might be:
    ///
    /// [ {"items": [1, 2, 3]}, NULL, {"items": NULL}]
    ///
    /// However, the underlying items array might be: [[1, 2, 3], [4, 5], NULL]
    ///
    /// The [4, 5] list is masked out because the struct array is null.
    ///
    /// The struct validity would be [true, false, true] and the list validity would be [true, true, false]
    ///
    /// This method pushes nulls down into all children.  In the above example the list validity would become
    /// [true, false, false].
    ///
    /// This method is not recursive.  If a child is a struct array it will not push that child's nulls down.
    ///
    /// This method does not remove garbage lists.  It only updates the validity so a future call to
    /// [crate::list::ListArrayExt::filter_garbage_nulls] will remove the garbage lists (without
    /// this pushdown it would not)
    fn pushdown_nulls(&self) -> Result<Self, ArrowError>
    where
        Self: Sized;
}

fn normalized_struct_array_data(data: ArrayData) -> Result<ArrayData, ArrowError> {
    let parent_offset = data.offset();
    let parent_len = data.len();
    let modified_children = data
        .child_data()
        .iter()
        .map(|d| {
            let d = normalized_struct_array_data(d.clone())?;
            let offset = d.offset();
            let len = d.len();
            if len < parent_len + parent_offset {
                return Err(ArrowError::InvalidArgumentError(format!(
                    "Child array {} has length {} which is less than the parent length {} plus the parent offset {}",
                    d.data_type(),
                    len,
                    parent_len,
                    parent_offset
                )));
            }
            let new_offset = offset + parent_offset;
            d.into_builder().offset(new_offset)
                .len(parent_len)
                .build()
        })
        .collect::<Result<Vec<_>, _>>()?;
    ArrayDataBuilder::new(data.data_type().clone())
        .len(parent_len)
        .offset(0)
        .buffers(data.buffers().to_vec())
        .child_data(modified_children)
        .build()
}

impl StructArrayExt for StructArray {
    fn normalize_slicing(&self) -> Result<Self, ArrowError>
    where
        Self: Sized,
    {
        if self.offset() == 0 && self.columns().iter().all(|c| c.len() == self.len()) {
            return Ok(self.clone());
        }

        let data = normalized_struct_array_data(self.to_data())?;
        Ok(Self::from(data))
    }

    fn pushdown_nulls(&self) -> Result<Self, ArrowError>
    where
        Self: Sized,
    {
        let Some(validity) = self.nulls() else {
            return Ok(self.clone());
        };
        let data = self.to_data();
        let children = data
            .child_data()
            .iter()
            .map(|c| {
                if let Some(child_validity) = c.nulls() {
                    let new_validity = child_validity.inner() & validity.inner();
                    c.clone()
                        .into_builder()
                        .nulls(Some(NullBuffer::from(new_validity)))
                        .build()
                } else {
                    Ok(c.clone()
                        .into_builder()
                        .nulls(Some(validity.clone()))
                        .build()?)
                }
            })
            .collect::<Result<Vec<_>, _>>()?;
        let arr = make_array(data.into_builder().child_data(children).build()?);
        Ok(arr.as_struct().clone())
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::{Array, Int32Array, StructArray, cast::AsArray, make_array};
    use arrow_schema::{DataType, Field, Fields};
    use std::sync::Arc;

    use crate::r#struct::StructArrayExt;

    #[test]
    fn test_normalize_slicing_no_offset() {
        let x = Int32Array::from(vec![1, 2, 3]);
        let y = Int32Array::from(vec![4, 5, 6]);
        let struct_array = StructArray::new(
            Fields::from(vec![
                Field::new("x", DataType::Int32, true),
                Field::new("y", DataType::Int32, true),
            ]),
            vec![Arc::new(x), Arc::new(y)],
            None,
        );

        let normalized = struct_array.normalize_slicing().unwrap();
        assert_eq!(normalized, struct_array);
    }

    #[test]
    fn test_arrow_rs_slicing() {
        let x = Int32Array::from(vec![1, 2, 3, 4]);
        let y = Int32Array::from(vec![5, 6, 7, 8]);
        let struct_array = StructArray::new(
            Fields::from(vec![
                Field::new("x", DataType::Int32, true),
                Field::new("y", DataType::Int32, true),
            ]),
            vec![Arc::new(x), Arc::new(y)],
            None,
        );

        // Slicing with arrow-rs propagates the slicing to the children so there should
        // be no change needed to the struct array
        let sliced = struct_array.slice(1, 2);
        let normalized = sliced.normalize_slicing().unwrap();

        assert_eq!(normalized, sliced);
    }

    #[test]
    fn test_arrow_cpp_slicing() {
        let x = Int32Array::from(vec![1, 2, 3, 4]);
        let y = Int32Array::from(vec![5, 6, 7, 8]);
        let struct_array = StructArray::new(
            Fields::from(vec![
                Field::new("x", DataType::Int32, true),
                Field::new("y", DataType::Int32, true),
            ]),
            vec![Arc::new(x), Arc::new(y)],
            None,
        );

        let data = struct_array.to_data();
        let sliced = data.into_builder().offset(1).len(2).build().unwrap();
        let sliced = make_array(sliced);
        let normalized = sliced.as_struct().clone().normalize_slicing().unwrap();

        assert_eq!(normalized, struct_array.slice(1, 2));
    }
}
