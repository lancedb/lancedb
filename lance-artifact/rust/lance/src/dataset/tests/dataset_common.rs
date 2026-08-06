// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::sync::Arc;

use arrow::array::as_struct_array;
use arrow::compute::concat_batches;
use arrow_array::{
    ArrayRef, DictionaryArray, Int32Array, RecordBatch, RecordBatchIterator, StringArray,
    StructArray, UInt16Array,
};
use arrow_ord::sort::sort_to_indices;
use arrow_schema::{DataType, Field as ArrowField, Schema as ArrowSchema};
use arrow_select::take::take;
use futures::TryStreamExt;
use lance_file::version::LanceFileVersion;
use lance_table::format::WriterVersion;

use crate::Dataset;
use crate::dataset::WriteMode;
use crate::dataset::write::WriteParams;

// Used to validate that futures returned are Send.
pub(super) fn require_send<T: Send>(t: T) -> T {
    t
}

pub(super) async fn create_file(
    path: &std::path::Path,
    mode: WriteMode,
    data_storage_version: LanceFileVersion,
) {
    let fields = vec![
        ArrowField::new("i", DataType::Int32, false),
        ArrowField::new(
            "dict",
            DataType::Dictionary(Box::new(DataType::UInt16), Box::new(DataType::Utf8)),
            false,
        ),
    ];
    let schema = Arc::new(ArrowSchema::new(fields));
    let dict_values = StringArray::from_iter_values(["a", "b", "c", "d", "e"]);
    let batches: Vec<RecordBatch> = (0..20)
        .map(|i| {
            let mut arrays =
                vec![Arc::new(Int32Array::from_iter_values(i * 20..(i + 1) * 20)) as ArrayRef];
            arrays.push(Arc::new(
                DictionaryArray::try_new(
                    UInt16Array::from_iter_values((0_u16..20_u16).map(|v| v % 5)),
                    Arc::new(dict_values.clone()),
                )
                .unwrap(),
            ));
            RecordBatch::try_new(schema.clone(), arrays).unwrap()
        })
        .collect();
    let expected_batches = batches.clone();

    let test_uri = path.to_str().unwrap();
    let write_params = WriteParams {
        max_rows_per_file: 40,
        max_rows_per_group: 10,
        mode,
        data_storage_version: Some(data_storage_version),
        ..WriteParams::default()
    };
    let reader = RecordBatchIterator::new(batches.into_iter().map(Ok), schema.clone());
    Dataset::write(reader, test_uri, Some(write_params))
        .await
        .unwrap();

    let actual_ds = Dataset::open(test_uri).await.unwrap();
    assert_eq!(actual_ds.version().version, 1);
    assert_eq!(
        actual_ds.manifest.writer_version,
        Some(WriterVersion::default())
    );
    let actual_schema = ArrowSchema::from(actual_ds.schema());
    assert_eq!(&actual_schema, schema.as_ref());

    let actual_batches = actual_ds
        .scan()
        .try_into_stream()
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();

    // The batch size batches the group size.
    // (the v2 writer has no concept of group size)
    if data_storage_version == LanceFileVersion::Legacy {
        for batch in &actual_batches {
            assert_eq!(batch.num_rows(), 10);
        }
    }

    // sort
    let actual_batch = concat_batches(&schema, &actual_batches).unwrap();
    let idx_arr = actual_batch.column_by_name("i").unwrap();
    let sorted_indices = sort_to_indices(idx_arr, None, None).unwrap();
    let struct_arr: StructArray = actual_batch.into();
    let sorted_arr = take(&struct_arr, &sorted_indices, None).unwrap();

    let expected_struct_arr: StructArray =
        concat_batches(&schema, &expected_batches).unwrap().into();
    assert_eq!(&expected_struct_arr, as_struct_array(sorted_arr.as_ref()));

    // Each fragment has different fragment ID
    assert_eq!(
        actual_ds
            .fragments()
            .iter()
            .map(|f| f.id)
            .collect::<Vec<_>>(),
        (0..10).collect::<Vec<_>>()
    );
}
