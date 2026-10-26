// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::sync::Arc;

use arrow_array::{Array, ArrayRef, BooleanArray, Int32Array, RecordBatch, StructArray};
use arrow_buffer::NullBuffer;
use arrow_schema::{DataType, Field, Schema};
use lance_arrow::RecordBatchExt;

#[test]
fn merge_nullable_struct_with_sliced_boolean_child() {
    let left_struct = StructArray::new(
        vec![Field::new("id", DataType::Int32, false)].into(),
        vec![Arc::new(Int32Array::from(vec![1, 2, 3, 4])) as ArrayRef],
        None,
    );

    // BooleanArray retains its offset when sliced. The merged child bitmap must
    // cover that offset as well as the four visible rows.
    let boolean_child: ArrayRef = Arc::new(BooleanArray::from_iter((0..1030).map(|index| {
        if index == 1027 {
            None
        } else {
            Some(index % 2 == 1)
        }
    })));
    let boolean_child = boolean_child.slice(1025, 4);
    assert_eq!(boolean_child.offset(), 1025);
    let right_struct = StructArray::new(
        vec![Field::new("flag", DataType::Boolean, true)].into(),
        vec![boolean_child],
        Some(NullBuffer::from(vec![true, false, true, true])),
    );

    let left_batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "nested",
            left_struct.data_type().clone(),
            true,
        )])),
        vec![Arc::new(left_struct)],
    )
    .unwrap();
    let right_batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "nested",
            right_struct.data_type().clone(),
            true,
        )])),
        vec![Arc::new(right_struct)],
    )
    .unwrap();

    let merged = left_batch.merge(&right_batch).unwrap();
    let nested = merged.column_by_name("nested").unwrap();
    let nested = nested.as_any().downcast_ref::<StructArray>().unwrap();
    let flag = nested.column_by_name("flag").unwrap();
    let flag = flag.as_any().downcast_ref::<BooleanArray>().unwrap();
    assert_eq!(
        flag.iter().collect::<Vec<_>>(),
        vec![Some(true), None, None, Some(false)]
    );
}
