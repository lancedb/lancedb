// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::sync::Arc;

use arrow_array::{Array, RecordBatch, make_array};
use arrow_buffer::{BooleanBuffer, Buffer, NullBuffer};
use arrow_data::{ArrayData, ArrayDataBuilder, transform::MutableArrayData};
use arrow_schema::DataType;

pub fn deep_copy_buffer(buffer: &Buffer) -> Buffer {
    Buffer::from(buffer.as_slice())
}

pub fn deep_copy_nulls(nulls: Option<&NullBuffer>) -> Option<NullBuffer> {
    let nulls = nulls?;
    let bit_buffer = deep_copy_buffer(nulls.inner().inner());
    // SAFETY: `null_count` is taken from the source `NullBuffer`, which already
    // upheld `NullBuffer::new_unchecked`'s invariant — the unset-bit count over
    // the logical bit slice `[bit_offset, bit_offset + bit_len)`. `NullBuffer::slice`
    // adjusts only `BooleanBuffer::bit_offset` / `bit_len` and never byte-advances
    // the inner `Buffer`, so `deep_copy_buffer` (which copies the source `Buffer`'s
    // `as_slice()` view from byte 0) reproduces the exact bit pattern at the same
    // bit offsets; the unset-bit count is therefore preserved. `BooleanBuffer::new`
    // panics (does not UB) if `bit_offset + bit_len > 8 * buffer.len()`, and the
    // copy has the same length, so that check still passes.
    Some(unsafe {
        NullBuffer::new_unchecked(
            BooleanBuffer::new(bit_buffer, nulls.offset(), nulls.len()),
            nulls.null_count(),
        )
    })
}

pub fn deep_copy_array_data(data: &ArrayData) -> ArrayData {
    let data_type = data.data_type().clone();
    let len = data.len();
    let nulls = deep_copy_nulls(data.nulls());
    let offset = data.offset();
    let buffers = data
        .buffers()
        .iter()
        .map(deep_copy_buffer)
        .collect::<Vec<_>>();
    let child_data = data
        .child_data()
        .iter()
        .map(deep_copy_array_data)
        .collect::<Vec<_>>();
    // SAFETY: `build_unchecked` inherits `ArrayData::new_unchecked`'s contract —
    // `(data_type, len, offset, nulls, buffers, child_data)` must form a valid
    // Arrow array. This call reproduces `data` structurally: `data_type`, `len`,
    // and `offset` are forwarded unchanged; each buffer is replaced by a byte-
    // identical copy of its offset-applied `as_slice()` view (the output buffer
    // is `MutableBuffer`-allocated, at least as aligned as the source); `nulls`
    // is deep-copied with the same bit offset/length and unset-bit count (see
    // `deep_copy_nulls`); `child_data` is recursively cloned with the same
    // guarantee. Every value-level invariant the source upheld — UTF-8 validity,
    // monotonic offsets, in-bounds dictionary indices, run-end monotonicity,
    // struct child-length matching — therefore transfers to the copy. If the
    // source `ArrayData` was itself constructed via `new_unchecked` with an
    // invalid payload, this function faithfully reproduces that invalidity.
    unsafe {
        ArrayDataBuilder::new(data_type)
            .len(len)
            .nulls(nulls)
            .offset(offset)
            .buffers(buffers)
            .child_data(child_data)
            .build_unchecked()
    }
}

pub fn deep_copy_array(array: &dyn Array) -> Arc<dyn Array> {
    let data = array.to_data();
    let data = deep_copy_array_data(&data);
    make_array(data)
}

pub fn deep_copy_batch(batch: &RecordBatch) -> crate::Result<RecordBatch> {
    let arrays = batch
        .columns()
        .iter()
        .map(|array| deep_copy_array(array))
        .collect::<Vec<_>>();
    RecordBatch::try_new(batch.schema(), arrays)
}

/// Deep copy array data, extracting only the sliced portion using MutableArrayData
/// This is the most efficient and correct way to copy just the sliced data
pub fn deep_copy_array_data_sliced(data: &ArrayData) -> ArrayData {
    // Use MutableArrayData to efficiently copy just the slice
    let mut mutable = MutableArrayData::new(vec![data], false, data.len());

    // Which index this takes depends on the layout, because arrow's extenders
    // disagree about who applies `ArrayData::offset()`. The bit-packed one adds
    // it to the raw values buffer itself, so an offset-applied index there
    // reads from twice the offset and runs off the end of the buffer. Every
    // other layout either reads through an already-offset-applied view or
    // forwards the index to children carrying their own offsets, and takes the
    // absolute index it has always been given.
    let start = match data.data_type() {
        DataType::Boolean => 0,
        _ => data.offset(),
    };
    mutable.extend(0, start, start + data.len());

    // Freeze into immutable ArrayData
    mutable.freeze()
}

/// Deep copy an array, extracting only the sliced portion using MutableArrayData
pub fn deep_copy_array_sliced(array: &dyn Array) -> Arc<dyn Array> {
    let data = array.to_data();
    let data = deep_copy_array_data_sliced(&data);
    make_array(data)
}

/// Deep copy a RecordBatch, extracting only the sliced portion using MutableArrayData
pub fn deep_copy_batch_sliced(batch: &RecordBatch) -> crate::Result<RecordBatch> {
    let arrays = batch
        .columns()
        .iter()
        .map(|array| deep_copy_array_sliced(array))
        .collect::<Vec<_>>();
    RecordBatch::try_new(batch.schema(), arrays)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Array, BooleanArray, Int32Array, RecordBatch, StringArray};
    use arrow_data::ArrayDataBuilder;
    use arrow_schema::{DataType, Field, Schema};

    #[test]
    fn raw_sliced_fixed_size_list_data_keeps_its_child_offset() {
        // `ArrayData::slice` records the offset on the parent and leaves the
        // child whole -- a shape `FixedSizeListArray::slice` never produces,
        // but a legal one this public helper can be handed directly. Arrow's
        // extender forwards `start * size` to that unsliced child and adds
        // nothing, so the index it gets has to be the absolute one; this is
        // here to stop the boolean fix from being generalised over it.
        let child = Int32Array::from(vec![10, 11, 20, 21]).to_data();
        let field = Arc::new(Field::new_list_field(DataType::Int32, false));
        let data = ArrayDataBuilder::new(DataType::FixedSizeList(field, 2))
            .len(2)
            .add_child_data(child)
            .build()
            .unwrap();
        let sliced = data.slice(1, 1);

        let copied = super::deep_copy_array_data_sliced(&sliced);
        let copied_child = Int32Array::from(copied.child_data()[0].clone());
        assert_eq!(copied_child.values().as_ref(), &[20, 21]);
    }

    #[test]
    fn raw_sliced_struct_of_fixed_size_list_reaches_the_right_values() {
        // Slicing raw struct data pushes the window into the struct's children,
        // and a fixed-size-list child takes it as its own parent offset with
        // its values left whole. Two levels of offset, and the absolute index
        // has to remain right through both of them.
        let values = Int32Array::from(vec![10, 11, 20, 21, 30, 31]).to_data();
        let item = Arc::new(Field::new_list_field(DataType::Int32, false));
        let list = ArrayDataBuilder::new(DataType::FixedSizeList(item, 2))
            .len(3)
            .add_child_data(values)
            .build()
            .unwrap();
        let field = Arc::new(Field::new("l", list.data_type().clone(), false));
        let data = ArrayDataBuilder::new(DataType::Struct(vec![field].into()))
            .len(3)
            .add_child_data(list)
            .build()
            .unwrap();
        let sliced = data.slice(2, 1);

        let copied = super::deep_copy_array_data_sliced(&sliced);
        let copied_values = Int32Array::from(copied.child_data()[0].child_data()[0].clone());
        assert_eq!(copied_values.values().as_ref(), &[30, 31]);
    }

    #[test]
    fn raw_parent_offset_struct_keeps_the_selected_child_row() {
        // A struct whose children are whole and whose window is the parent
        // offset: arrow's struct extender forwards the index to those children
        // untouched, so it has to be the absolute one.
        let field = Arc::new(Field::new("a", DataType::Int32, false));
        let data = ArrayDataBuilder::new(DataType::Struct(vec![field].into()))
            .len(1)
            .offset(1)
            .add_child_data(Int32Array::from(vec![10, 20]).to_data())
            .build()
            .unwrap();

        let copied = super::deep_copy_array_data_sliced(&data);
        let copied_child = Int32Array::from(copied.child_data()[0].clone());
        assert_eq!(copied_child.values().as_ref(), &[20]);
    }

    #[test]
    fn sliced_boolean_deep_copy_reads_from_the_slice() {
        // A boolean slice keeps its `ArrayData::offset()` -- the buffer cannot
        // advance by a fraction of a byte -- so a copy that adds the offset on
        // top of what `MutableArrayData` already applies reads past the end of
        // the values buffer and panics.
        let array = BooleanArray::from(vec![true, false, true, false, true, false, true, false]);
        for (offset, len) in [(0usize, 8usize), (1, 7), (3, 5), (7, 1)] {
            let sliced = array.slice(offset, len);
            let copied = super::deep_copy_array_sliced(&sliced);
            let copied = copied.as_any().downcast_ref::<BooleanArray>().unwrap();
            let expected: Vec<bool> = (0..len).map(|i| sliced.value(i)).collect();
            let actual: Vec<bool> = (0..len).map(|i| copied.value(i)).collect();
            assert_eq!(actual, expected, "offset={offset} len={len}");
        }
    }

    #[test]
    fn sliced_boolean_deep_copy_keeps_its_nulls() {
        let array = BooleanArray::from(vec![
            Some(true),
            None,
            Some(false),
            Some(true),
            None,
            Some(false),
        ]);
        let sliced = array.slice(1, 4);
        let copied = super::deep_copy_array_sliced(&sliced);
        let copied = copied.as_any().downcast_ref::<BooleanArray>().unwrap();
        let expected: Vec<Option<bool>> = (0..sliced.len())
            .map(|i| (!sliced.is_null(i)).then(|| sliced.value(i)))
            .collect();
        let actual: Vec<Option<bool>> = (0..copied.len())
            .map(|i| (!copied.is_null(i)).then(|| copied.value(i)))
            .collect();
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_deep_copy_sliced_array_with_nulls() {
        let array = Arc::new(Int32Array::from(vec![
            Some(1),
            None,
            Some(3),
            None,
            Some(5),
        ]));
        let sliced_array = array.slice(1, 3);
        let copied_array = super::deep_copy_array(&sliced_array);
        assert_eq!(sliced_array.len(), copied_array.len());
        assert_eq!(sliced_array.nulls(), copied_array.nulls());
    }

    #[test]
    fn test_deep_copy_array_data_sliced() {
        let array = Int32Array::from((0..1000).collect::<Vec<i32>>());
        let sliced = array.slice(100, 10);

        let sliced_data = sliced.to_data();
        let copied_data = super::deep_copy_array_data_sliced(&sliced_data);

        assert_eq!(copied_data.len(), 10);
        assert_eq!(copied_data.offset(), 0);

        // Verify data correctness
        let copied_array = Int32Array::from(copied_data);
        for i in 0..10 {
            assert_eq!(copied_array.value(i), 100 + i as i32);
        }
    }

    #[test]
    fn test_deep_copy_array_sliced() {
        let array = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]));
        let sliced = array.slice(1, 3);

        let copied = super::deep_copy_array_sliced(&sliced);

        assert_eq!(copied.len(), 3);
        let copied_int = copied.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(copied_int.value(0), 2);
        assert_eq!(copied_int.value(1), 3);
        assert_eq!(copied_int.value(2), 4);
    }

    #[test]
    fn test_deep_copy_batch_sliced() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let id_array = Arc::new(Int32Array::from((0..100).collect::<Vec<i32>>()));
        let name_array = Arc::new(StringArray::from(
            (0..100)
                .map(|i| format!("name_{}", i))
                .collect::<Vec<String>>(),
        ));

        let batch = RecordBatch::try_new(
            schema,
            vec![id_array as Arc<dyn Array>, name_array as Arc<dyn Array>],
        )
        .unwrap();

        let sliced = batch.slice(10, 5);
        let copied = super::deep_copy_batch_sliced(&sliced).unwrap();

        assert_eq!(copied.num_rows(), 5);
        assert_eq!(copied.num_columns(), 2);

        // Verify data correctness
        let id_col = copied
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let name_col = copied
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        for i in 0..5 {
            assert_eq!(id_col.value(i), 10 + i as i32);
            assert_eq!(name_col.value(i), format!("name_{}", 10 + i));
        }
    }

    #[test]
    fn test_deep_copy_array_sliced_with_nulls() {
        let array = Arc::new(Int32Array::from(vec![
            Some(1),
            None,
            Some(3),
            None,
            Some(5),
        ]));
        let sliced = array.slice(1, 3); // [None, Some(3), None]

        let copied = super::deep_copy_array_sliced(&sliced);

        assert_eq!(copied.len(), 3);
        assert_eq!(copied.null_count(), 2); // Two nulls in the slice

        let copied_int = copied.as_any().downcast_ref::<Int32Array>().unwrap();
        assert!(!copied_int.is_valid(0)); // None
        assert!(copied_int.is_valid(1)); // Some(3)
        assert!(!copied_int.is_valid(2)); // None
        assert_eq!(copied_int.value(1), 3);
    }
}
