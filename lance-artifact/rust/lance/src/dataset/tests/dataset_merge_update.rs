// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use std::vec;

use crate::dataset::ROW_ID;
use crate::dataset::WriteDestination;
use crate::dataset::optimize::{CompactionOptions, compact_files};
use crate::dataset::transaction::{DataReplacementGroup, Operation};
use crate::dataset::{AutoCleanupParams, MergeInsertBuilder, ProjectionRequest, UpdateBuilder};
use crate::index::DatasetIndexExt;
use crate::{Dataset, Error};
use lance_core::{ROW_ADDR, ROW_LAST_UPDATED_AT_VERSION};
use lance_index::IndexType;
use lance_index::optimize::OptimizeOptions;
use lance_index::scalar::FullTextSearchQuery;
use lance_index::scalar::ScalarIndexParams;
use lance_index::scalar::inverted::tokenizer::InvertedIndexParams;
use mock_instant::thread_local::MockClock;

use crate::dataset::write::{InsertBuilder, WriteMode, WriteParams};
use arrow::array::AsArray;
use arrow::compute::concat_batches;
use arrow_array::RecordBatch;
use arrow_array::{Array, LargeBinaryArray, StructArray};
use arrow_array::{
    ArrayRef, Float32Array, Int32Array, ListArray, RecordBatchIterator, StringArray,
    types::{Int32Type, UInt64Type},
};
use arrow_schema::{DataType, Field as ArrowField, Fields, Schema as ArrowSchema};
use lance_arrow::BLOB_META_KEY;
use lance_core::utils::tempfile::{TempDir, TempStrDir};
use lance_datafusion::utils::reader_to_stream;
use lance_datagen::{BatchCount, RowCount, array, gen_batch};
use lance_file::version::{ConcreteFileVersion, LanceFileVersion};
use lance_io::utils::CachedFileSize;
use lance_table::format::{BasePath, DataFile, Fragment};

use crate::dataset::write::merge_insert::{WhenMatched, WhenNotMatched};
use futures::TryStreamExt;
use lance_datafusion::datagen::DatafusionDatagenExt;
use object_store::path::Path;
use rand::seq::SliceRandom;
use rstest::rstest;

#[rstest]
#[tokio::test]
async fn test_merge(
    #[values(LanceFileVersion::Legacy, LanceFileVersion::Stable)]
    data_storage_version: LanceFileVersion,
    #[values(false, true)] use_stable_row_id: bool,
) {
    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("i", DataType::Int32, false),
        ArrowField::new("x", DataType::Float32, false),
    ]));
    let batch1 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(Float32Array::from(vec![1.0, 2.0])),
        ],
    )
    .unwrap();
    let batch2 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![3, 2])),
            Arc::new(Float32Array::from(vec![3.0, 4.0])),
        ],
    )
    .unwrap();

    let test_uri = TempStrDir::default();

    let write_params = WriteParams {
        mode: WriteMode::Append,
        data_storage_version: Some(data_storage_version),
        enable_stable_row_ids: use_stable_row_id,
        ..Default::default()
    };

    let batches = RecordBatchIterator::new(vec![batch1].into_iter().map(Ok), schema.clone());
    Dataset::write(batches, &test_uri, Some(write_params.clone()))
        .await
        .unwrap();

    let batches = RecordBatchIterator::new(vec![batch2].into_iter().map(Ok), schema.clone());
    Dataset::write(batches, &test_uri, Some(write_params.clone()))
        .await
        .unwrap();

    let dataset = Dataset::open(&test_uri).await.unwrap();
    assert_eq!(dataset.fragments().len(), 2);
    assert_eq!(dataset.manifest.max_fragment_id(), Some(1));

    let right_schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("i2", DataType::Int32, false),
        ArrowField::new("y", DataType::Utf8, true),
    ]));
    let right_batch1 = RecordBatch::try_new(
        right_schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["a", "b"])),
        ],
    )
    .unwrap();

    let batches =
        RecordBatchIterator::new(vec![right_batch1].into_iter().map(Ok), right_schema.clone());
    let mut dataset = Dataset::open(&test_uri).await.unwrap();
    dataset.merge(batches, "i", "i2").await.unwrap();
    dataset.validate().await.unwrap();

    assert_eq!(dataset.version().version, 3);
    assert_eq!(dataset.fragments().len(), 2);
    assert_eq!(dataset.fragments()[0].files.len(), 2);
    assert_eq!(dataset.fragments()[1].files.len(), 2);
    assert_eq!(dataset.manifest.max_fragment_id(), Some(1));

    let actual_batches = dataset
        .scan()
        .try_into_stream()
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let actual = concat_batches(&actual_batches[0].schema(), &actual_batches).unwrap();
    let expected = RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![
            ArrowField::new("i", DataType::Int32, false),
            ArrowField::new("x", DataType::Float32, false),
            ArrowField::new("y", DataType::Utf8, true),
        ])),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 2])),
            Arc::new(Float32Array::from(vec![1.0, 2.0, 3.0, 4.0])),
            Arc::new(StringArray::from(vec![
                Some("a"),
                Some("b"),
                None,
                Some("b"),
            ])),
        ],
    )
    .unwrap();

    assert_eq!(actual, expected);

    // Validate we can still read after re-instantiating dataset, which
    // clears the cache.
    let dataset = Dataset::open(&test_uri).await.unwrap();
    let actual_batches = dataset
        .scan()
        .try_into_stream()
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let actual = concat_batches(&actual_batches[0].schema(), &actual_batches).unwrap();
    assert_eq!(actual, expected);
}

#[rstest]
#[tokio::test]
async fn test_large_merge(
    #[values(LanceFileVersion::Legacy, LanceFileVersion::Stable)]
    data_storage_version: LanceFileVersion,
    #[values(false, true)] use_stable_row_id: bool,
) {
    // Tests a merge that spans multiple batches within files

    // This test also tests "null filling" when merging (e.g. when keys do not match
    // we need to insert nulls)

    let data = lance_datagen::gen_batch()
        .col("key", array::step::<Int32Type>())
        .col("value", array::fill_utf8("value".to_string()))
        .into_reader_rows(RowCount::from(1_000), BatchCount::from(10));

    let test_uri = TempStrDir::default();

    let write_params = WriteParams {
        mode: WriteMode::Append,
        data_storage_version: Some(data_storage_version),
        max_rows_per_file: 1024,
        max_rows_per_group: 150,
        enable_stable_row_ids: use_stable_row_id,
        ..Default::default()
    };
    Dataset::write(data, &test_uri, Some(write_params.clone()))
        .await
        .unwrap();

    let mut dataset = Dataset::open(&test_uri).await.unwrap();
    assert_eq!(dataset.fragments().len(), 10);
    assert_eq!(dataset.manifest.max_fragment_id(), Some(9));

    let new_data = lance_datagen::gen_batch()
        .col("key2", array::step_custom::<Int32Type>(500, 1))
        .col("new_value", array::fill_utf8("new_value".to_string()))
        .into_reader_rows(RowCount::from(1_000), BatchCount::from(10));

    dataset.merge(new_data, "key", "key2").await.unwrap();
    dataset.validate().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn test_merge_on_row_id(
    #[values(LanceFileVersion::Stable)] data_storage_version: LanceFileVersion,
    #[values(false, true)] use_stable_row_id: bool,
) {
    // Tests a merge on _rowid

    let data = lance_datagen::gen_batch()
        .col("key", array::step::<Int32Type>())
        .col("value", array::fill_utf8("value".to_string()))
        .into_reader_rows(RowCount::from(1_000), BatchCount::from(10));

    let write_params = WriteParams {
        mode: WriteMode::Append,
        data_storage_version: Some(data_storage_version),
        max_rows_per_file: 1024,
        max_rows_per_group: 150,
        enable_stable_row_ids: use_stable_row_id,
        ..Default::default()
    };
    let mut dataset = Dataset::write(data, "memory://", Some(write_params.clone()))
        .await
        .unwrap();
    assert_eq!(dataset.fragments().len(), 10);
    assert_eq!(dataset.manifest.max_fragment_id(), Some(9));

    let data = dataset.scan().with_row_id().try_into_batch().await.unwrap();
    let row_ids: Arc<dyn Array> = data[ROW_ID].clone();
    let key = data["key"].as_primitive::<Int32Type>();
    let new_schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("rowid", DataType::UInt64, false),
        ArrowField::new("new_value", DataType::Int32, false),
    ]));
    let new_value = Arc::new(
        key.into_iter()
            .map(|v| v.unwrap() + 1)
            .collect::<arrow_array::Int32Array>(),
    );
    let len = new_value.len() as u32;
    let new_batch = RecordBatch::try_new(new_schema.clone(), vec![row_ids, new_value]).unwrap();
    // shuffle new_batch
    let mut rng = rand::rng();
    let mut indices: Vec<u32> = (0..len).collect();
    indices.shuffle(&mut rng);
    let indices = arrow_array::UInt32Array::from_iter_values(indices);
    let new_batch = arrow::compute::take_record_batch(&new_batch, &indices).unwrap();
    let new_data = RecordBatchIterator::new(vec![Ok(new_batch)], new_schema.clone());
    dataset.merge(new_data, ROW_ID, "rowid").await.unwrap();
    dataset.validate().await.unwrap();
    assert_eq!(dataset.schema().fields.len(), 3);
    assert!(dataset.schema().field("key").is_some());
    assert!(dataset.schema().field("value").is_some());
    assert!(dataset.schema().field("new_value").is_some());
    let batch = dataset.scan().try_into_batch().await.unwrap();
    let key = batch["key"].as_primitive::<Int32Type>();
    let new_value = batch["new_value"].as_primitive::<Int32Type>();
    for i in 0..key.len() {
        assert_eq!(key.value(i) + 1, new_value.value(i));
    }
}

#[rstest]
#[tokio::test]
async fn test_merge_on_row_addr(
    #[values(LanceFileVersion::Stable)] data_storage_version: LanceFileVersion,
    #[values(false, true)] use_stable_row_id: bool,
) {
    // Tests a merge on _rowaddr

    let data = lance_datagen::gen_batch()
        .col("key", array::step::<Int32Type>())
        .col("value", array::fill_utf8("value".to_string()))
        .into_reader_rows(RowCount::from(1_000), BatchCount::from(10));

    let write_params = WriteParams {
        mode: WriteMode::Append,
        data_storage_version: Some(data_storage_version),
        max_rows_per_file: 1024,
        max_rows_per_group: 150,
        enable_stable_row_ids: use_stable_row_id,
        ..Default::default()
    };
    let mut dataset = Dataset::write(data, "memory://", Some(write_params.clone()))
        .await
        .unwrap();

    assert_eq!(dataset.fragments().len(), 10);
    assert_eq!(dataset.manifest.max_fragment_id(), Some(9));

    let data = dataset
        .scan()
        .with_row_address()
        .try_into_batch()
        .await
        .unwrap();
    let row_addrs = data[ROW_ADDR].clone();
    let key = data["key"].as_primitive::<Int32Type>();
    let new_schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("rowaddr", DataType::UInt64, false),
        ArrowField::new("new_value", DataType::Int32, false),
    ]));
    let new_value = Arc::new(
        key.into_iter()
            .map(|v| v.unwrap() + 1)
            .collect::<arrow_array::Int32Array>(),
    );
    let len = new_value.len() as u32;
    let new_batch = RecordBatch::try_new(new_schema.clone(), vec![row_addrs, new_value]).unwrap();
    // shuffle new_batch
    let mut rng = rand::rng();
    let mut indices: Vec<u32> = (0..len).collect();
    indices.shuffle(&mut rng);
    let indices = arrow_array::UInt32Array::from_iter_values(indices);
    let new_batch = arrow::compute::take_record_batch(&new_batch, &indices).unwrap();
    let new_data = RecordBatchIterator::new(vec![Ok(new_batch)], new_schema.clone());
    dataset.merge(new_data, ROW_ADDR, "rowaddr").await.unwrap();
    dataset.validate().await.unwrap();
    assert_eq!(dataset.schema().fields.len(), 3);
    assert!(dataset.schema().field("key").is_some());
    assert!(dataset.schema().field("value").is_some());
    assert!(dataset.schema().field("new_value").is_some());
    let batch = dataset.scan().try_into_batch().await.unwrap();
    let key = batch["key"].as_primitive::<Int32Type>();
    let new_value = batch["new_value"].as_primitive::<Int32Type>();
    for i in 0..key.len() {
        assert_eq!(key.value(i) + 1, new_value.value(i));
    }
}

#[tokio::test]
async fn test_insert_subschema() {
    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("a", DataType::Int32, false),
        ArrowField::new("b", DataType::Int32, true),
    ]));
    let empty_reader = RecordBatchIterator::new(vec![], schema.clone());
    let mut dataset = Dataset::write(empty_reader, "memory://", None)
        .await
        .unwrap();
    dataset.validate().await.unwrap();

    // If missing columns that aren't nullable, will return an error
    // TODO: provide alternative default than null.
    let just_b = Arc::new(schema.project(&[1]).unwrap());
    let batch =
        RecordBatch::try_new(just_b.clone(), vec![Arc::new(Int32Array::from(vec![1]))]).unwrap();
    let reader = RecordBatchIterator::new(vec![Ok(batch)], just_b.clone());
    let res = dataset.append(reader, None).await;
    assert!(
        matches!(res, Err(Error::SchemaMismatch { .. })),
        "Expected Error::SchemaMismatch, got {:?}",
        res
    );

    // If missing columns that are nullable, the write succeeds.
    let just_a = Arc::new(schema.project(&[0]).unwrap());
    let batch =
        RecordBatch::try_new(just_a.clone(), vec![Arc::new(Int32Array::from(vec![1]))]).unwrap();
    let reader = RecordBatchIterator::new(vec![Ok(batch)], just_a.clone());
    dataset.append(reader, None).await.unwrap();
    dataset.validate().await.unwrap();
    assert_eq!(dataset.count_rows(None).await.unwrap(), 1);

    // Looking at the fragments, there is no data file with the missing field
    let fragments = dataset.get_fragments();
    assert_eq!(fragments.len(), 1);
    assert_eq!(fragments[0].metadata.files.len(), 1);
    assert_eq!(fragments[0].metadata.files[0].fields.as_ref(), &[0]);

    // When reading back, columns that are missing are null
    let data = dataset.scan().try_into_batch().await.unwrap();
    let expected = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(Int32Array::from(vec![None])),
        ],
    )
    .unwrap();
    assert_eq!(data, expected);

    // Can still insert all columns
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![2])),
            Arc::new(Int32Array::from(vec![3])),
        ],
    )
    .unwrap();
    let reader = RecordBatchIterator::new(vec![Ok(batch.clone())], schema.clone());
    dataset.append(reader, None).await.unwrap();
    dataset.validate().await.unwrap();
    assert_eq!(dataset.count_rows(None).await.unwrap(), 2);

    // When reading back, only missing data is null, otherwise is filled in
    let data = dataset.scan().try_into_batch().await.unwrap();
    let expected = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(Int32Array::from(vec![None, Some(3)])),
        ],
    )
    .unwrap();
    assert_eq!(data, expected);

    // Can run compaction. All files should now have all fields.
    compact_files(&mut dataset, CompactionOptions::default(), None)
        .await
        .unwrap();
    dataset.validate().await.unwrap();
    let fragments = dataset.get_fragments();
    assert_eq!(fragments.len(), 1);
    assert_eq!(fragments[0].metadata.files.len(), 1);
    assert_eq!(fragments[0].metadata.files[0].fields.as_ref(), &[0, 1]);

    // Can scan and get expected data.
    let data = dataset.scan().try_into_batch().await.unwrap();
    assert_eq!(data, expected);
}

#[tokio::test]
async fn test_insert_nested_subschemas() {
    // Test subschemas at struct level
    // Test different orders
    // Test the Dataset::write() path
    // Test Take across fragments with different field id sets
    let test_uri = TempStrDir::default();

    let field_a = Arc::new(ArrowField::new("a", DataType::Int32, true));
    let field_b = Arc::new(ArrowField::new("b", DataType::Int32, false));
    let field_c = Arc::new(ArrowField::new("c", DataType::Int32, true));
    let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
        "s",
        DataType::Struct(vec![field_a.clone(), field_b.clone(), field_c.clone()].into()),
        true,
    )]));
    let empty_reader = RecordBatchIterator::new(vec![], schema.clone());
    let dataset = Dataset::write(empty_reader, &test_uri, None).await.unwrap();
    dataset.validate().await.unwrap();

    let append_options = WriteParams {
        mode: WriteMode::Append,
        ..Default::default()
    };
    // Can insert b, a
    let just_b_a = Arc::new(ArrowSchema::new(vec![ArrowField::new(
        "s",
        DataType::Struct(vec![field_b.clone(), field_a.clone()].into()),
        true,
    )]));
    let batch = RecordBatch::try_new(
        just_b_a.clone(),
        vec![Arc::new(StructArray::from(vec![
            (
                field_b.clone(),
                Arc::new(Int32Array::from(vec![1])) as ArrayRef,
            ),
            (field_a.clone(), Arc::new(Int32Array::from(vec![2]))),
        ]))],
    )
    .unwrap();
    let reader = RecordBatchIterator::new(vec![Ok(batch)], just_b_a.clone());
    let dataset = Dataset::write(reader, &test_uri, Some(append_options.clone()))
        .await
        .unwrap();
    dataset.validate().await.unwrap();
    let fragments = dataset.get_fragments();
    assert_eq!(fragments.len(), 1);
    assert_eq!(fragments[0].metadata.files.len(), 1);
    assert_eq!(fragments[0].metadata.files[0].fields.as_ref(), &[2, 1]);
    assert_eq!(
        fragments[0].metadata.files[0].column_indices.as_ref(),
        &[0, 1]
    );

    // Can insert c, b
    let just_c_b = Arc::new(ArrowSchema::new(vec![ArrowField::new(
        "s",
        DataType::Struct(vec![field_c.clone(), field_b.clone()].into()),
        true,
    )]));
    let batch = RecordBatch::try_new(
        just_c_b.clone(),
        vec![Arc::new(StructArray::from(vec![
            (
                field_c.clone(),
                Arc::new(Int32Array::from(vec![4])) as ArrayRef,
            ),
            (field_b.clone(), Arc::new(Int32Array::from(vec![3]))),
        ]))],
    )
    .unwrap();
    let reader = RecordBatchIterator::new(vec![Ok(batch)], just_c_b.clone());
    let dataset = Dataset::write(reader, &test_uri, Some(append_options.clone()))
        .await
        .unwrap();
    dataset.validate().await.unwrap();
    let fragments = dataset.get_fragments();
    assert_eq!(fragments.len(), 2);
    assert_eq!(fragments[1].metadata.files.len(), 1);
    assert_eq!(fragments[1].metadata.files[0].fields.as_ref(), &[3, 2]);
    assert_eq!(
        fragments[1].metadata.files[0].column_indices.as_ref(),
        &[0, 1]
    );

    // Can't insert a, c (b is non-nullable)
    let just_a_c = Arc::new(ArrowSchema::new(vec![ArrowField::new(
        "s",
        DataType::Struct(vec![field_a.clone(), field_c.clone()].into()),
        true,
    )]));
    let batch = RecordBatch::try_new(
        just_a_c.clone(),
        vec![Arc::new(StructArray::from(vec![
            (
                field_a.clone(),
                Arc::new(Int32Array::from(vec![5])) as ArrayRef,
            ),
            (field_c.clone(), Arc::new(Int32Array::from(vec![6]))),
        ]))],
    )
    .unwrap();
    let reader = RecordBatchIterator::new(vec![Ok(batch)], just_a_c.clone());
    let res = Dataset::write(reader, &test_uri, Some(append_options)).await;
    assert!(
        matches!(res, Err(Error::SchemaMismatch { .. })),
        "Expected Error::SchemaMismatch, got {:?}",
        res
    );

    // Can scan and get all data
    let data = dataset.scan().try_into_batch().await.unwrap();
    let expected = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StructArray::from(vec![
            (
                field_a.clone(),
                Arc::new(Int32Array::from(vec![Some(2), None])) as ArrayRef,
            ),
            (field_b.clone(), Arc::new(Int32Array::from(vec![1, 3]))),
            (
                field_c.clone(),
                Arc::new(Int32Array::from(vec![None, Some(4)])),
            ),
        ]))],
    )
    .unwrap();
    assert_eq!(data, expected);

    // Can call take and get rows from all three back in one batch
    let result = dataset
        .take(&[1, 0], Arc::new(dataset.schema().clone()))
        .await
        .unwrap();
    let expected = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StructArray::from(vec![
            (
                field_a.clone(),
                Arc::new(Int32Array::from(vec![None, Some(2)])) as ArrayRef,
            ),
            (field_b.clone(), Arc::new(Int32Array::from(vec![3, 1]))),
            (
                field_c.clone(),
                Arc::new(Int32Array::from(vec![Some(4), None])),
            ),
        ]))],
    )
    .unwrap();
    assert_eq!(result, expected);
}

#[tokio::test]
async fn test_insert_balanced_subschemas() {
    let test_uri = TempStrDir::default();

    let field_a = ArrowField::new("a", DataType::Int32, true);
    let field_b = ArrowField::new("b", DataType::LargeBinary, true);
    let schema = Arc::new(ArrowSchema::new(vec![
        field_a.clone(),
        field_b
            .clone()
            .with_metadata([(BLOB_META_KEY.to_string(), "true".to_string())].into()),
    ]));
    let empty_reader = RecordBatchIterator::new(vec![], schema.clone());
    let options = WriteParams {
        enable_stable_row_ids: true,
        enable_v2_manifest_paths: true,
        ..Default::default()
    };
    let mut dataset = Dataset::write(empty_reader, &test_uri, Some(options))
        .await
        .unwrap();
    dataset.validate().await.unwrap();

    // Insert left side
    let just_a = Arc::new(ArrowSchema::new(vec![field_a.clone()]));
    let batch =
        RecordBatch::try_new(just_a.clone(), vec![Arc::new(Int32Array::from(vec![1]))]).unwrap();
    let reader = RecordBatchIterator::new(vec![Ok(batch)], just_a.clone());
    dataset.append(reader, None).await.unwrap();
    dataset.validate().await.unwrap();

    let fragments = dataset.get_fragments();
    assert_eq!(fragments.len(), 1);
    assert_eq!(fragments[0].metadata.files.len(), 1);
    assert_eq!(fragments[0].metadata.files[0].fields.as_ref(), &[0]);

    // Insert right side
    let just_b = Arc::new(ArrowSchema::new(vec![field_b.clone()]));
    let batch = RecordBatch::try_new(
        just_b.clone(),
        vec![Arc::new(LargeBinaryArray::from_iter(vec![Some(vec![2u8])]))],
    )
    .unwrap();
    let reader = RecordBatchIterator::new(vec![Ok(batch)], just_b.clone());
    dataset.append(reader, None).await.unwrap();
    dataset.validate().await.unwrap();

    let fragments = dataset.get_fragments();
    assert_eq!(fragments.len(), 2);
    assert_eq!(fragments[1].metadata.files.len(), 1);
    assert_eq!(fragments[1].metadata.files[0].fields.as_ref(), &[1]);

    let data = dataset
        .take(
            &[0, 1],
            ProjectionRequest::from_columns(["a"], dataset.schema()),
        )
        .await
        .unwrap();
    assert_eq!(data.num_rows(), 2);
    let a_column = data.column(0).as_primitive::<Int32Type>();
    assert_eq!(a_column.value(0), 1);
    assert!(a_column.is_null(1));

    let blob_batch = dataset
        .take(
            &[0, 1],
            ProjectionRequest::from_columns(["b"], dataset.schema()),
        )
        .await
        .unwrap();
    let blob_descriptions = blob_batch.column(0).as_struct();
    assert!(blob_descriptions.is_null(0));
    assert!(blob_descriptions.is_valid(1));
}

#[tokio::test]
async fn test_datafile_replacement() {
    let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
        "a",
        DataType::Int32,
        true,
    )]));
    let empty_reader = RecordBatchIterator::new(vec![], schema.clone());
    let dataset = Arc::new(
        Dataset::write(empty_reader, "memory://", None)
            .await
            .unwrap(),
    );
    dataset.validate().await.unwrap();

    // Test empty replacement should commit a new manifest and do nothing
    let mut dataset = Dataset::commit(
        WriteDestination::Dataset(dataset.clone()),
        Operation::DataReplacement {
            replacements: vec![],
        },
        Some(1),
        None,
        None,
        Arc::new(Default::default()),
        false,
    )
    .await
    .unwrap();
    dataset.validate().await.unwrap();

    assert_eq!(dataset.version().version, 2);
    assert_eq!(dataset.get_fragments().len(), 0);

    // try the same thing on a non-empty dataset
    let vals: Int32Array = vec![1, 2, 3].into();
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(vals)]).unwrap();
    dataset
        .append(
            RecordBatchIterator::new(vec![Ok(batch)], schema.clone()),
            None,
        )
        .await
        .unwrap();

    let dataset = Dataset::commit(
        WriteDestination::Dataset(Arc::new(dataset)),
        Operation::DataReplacement {
            replacements: vec![],
        },
        Some(3),
        None,
        None,
        Arc::new(Default::default()),
        false,
    )
    .await
    .unwrap();
    dataset.validate().await.unwrap();

    assert_eq!(dataset.version().version, 4);
    assert_eq!(dataset.get_fragments().len(), 1);

    let batch = dataset.scan().try_into_batch().await.unwrap();
    assert_eq!(batch.num_rows(), 3);
    assert_eq!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .values(),
        &[1, 2, 3]
    );

    // write a new datafile
    let object_writer = dataset
        .object_store
        .create(&Path::from("data/test.lance"))
        .await
        .unwrap();
    let mut writer = lance_file::versions::v2_1::create_writer(
        object_writer,
        schema.as_ref().try_into().unwrap(),
        Default::default(),
    )
    .unwrap();

    let vals: Int32Array = vec![4, 5, 6].into();
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(vals)]).unwrap();
    writer.write_batch(&batch).await.unwrap();
    writer.finish().await.unwrap();

    // find the datafile we want to replace
    let frag = dataset.get_fragment(0).unwrap();
    let data_file = frag.data_file_for_field(0).unwrap();
    let mut new_data_file = data_file.clone();
    new_data_file.path = "test.lance".to_string();

    let dataset = Dataset::commit(
        WriteDestination::Dataset(Arc::new(dataset)),
        Operation::DataReplacement {
            replacements: vec![DataReplacementGroup(0, new_data_file)],
        },
        Some(4),
        None,
        None,
        Arc::new(Default::default()),
        false,
    )
    .await
    .unwrap();

    assert_eq!(dataset.version().version, 5);
    assert_eq!(dataset.get_fragments().len(), 1);
    assert_eq!(dataset.get_fragments()[0].metadata.files.len(), 1);

    let batch = dataset.scan().try_into_batch().await.unwrap();
    assert_eq!(batch.num_rows(), 3);
    assert_eq!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .values(),
        &[4, 5, 6]
    );
}

#[tokio::test]
async fn test_datafile_partial_replacement() {
    let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
        "a",
        DataType::Int32,
        true,
    )]));
    let empty_reader = RecordBatchIterator::new(vec![], schema.clone());
    let mut dataset = Dataset::write(empty_reader, "memory://", None)
        .await
        .unwrap();
    dataset.validate().await.unwrap();

    let vals: Int32Array = vec![1, 2, 3].into();
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(vals)]).unwrap();
    dataset
        .append(
            RecordBatchIterator::new(vec![Ok(batch)], schema.clone()),
            None,
        )
        .await
        .unwrap();

    let fragment = dataset.get_fragments().pop().unwrap().metadata;

    let extended_schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("a", DataType::Int32, true),
        ArrowField::new("b", DataType::Int32, true),
    ]));

    // add all null column
    let dataset = Dataset::commit(
        WriteDestination::Dataset(Arc::new(dataset)),
        Operation::Merge {
            fragments: vec![fragment],
            schema: extended_schema.as_ref().try_into().unwrap(),
        },
        Some(2),
        None,
        None,
        Arc::new(Default::default()),
        false,
    )
    .await
    .unwrap();

    let partial_schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
        "b",
        DataType::Int32,
        true,
    )]));

    // write a new datafile
    let object_writer = dataset
        .object_store
        .create(&Path::from("data/test.lance"))
        .await
        .unwrap();
    let mut writer = lance_file::versions::v2_1::create_writer(
        object_writer,
        partial_schema.as_ref().try_into().unwrap(),
        Default::default(),
    )
    .unwrap();

    let vals: Int32Array = vec![4, 5, 6].into();
    let batch = RecordBatch::try_new(partial_schema.clone(), vec![Arc::new(vals)]).unwrap();
    writer.write_batch(&batch).await.unwrap();
    writer.finish().await.unwrap();

    let (major, minor) = ConcreteFileVersion::from(LanceFileVersion::Stable).to_data_file_numbers();

    // find the datafile we want to replace
    let new_data_file = DataFile {
        path: "test.lance".to_string(),
        // the second column in the dataset
        fields: Arc::from([1]),
        // is located in the first column of this datafile
        column_indices: Arc::from([0]),
        file_major_version: major,
        file_minor_version: minor,
        file_size_bytes: CachedFileSize::unknown(),
        base_id: None,
    };

    let dataset = Dataset::commit(
        WriteDestination::Dataset(Arc::new(dataset)),
        Operation::DataReplacement {
            replacements: vec![DataReplacementGroup(0, new_data_file)],
        },
        Some(3),
        None,
        None,
        Arc::new(Default::default()),
        false,
    )
    .await
    .unwrap();

    assert_eq!(dataset.version().version, 4);
    assert_eq!(dataset.get_fragments().len(), 1);
    assert_eq!(dataset.get_fragments()[0].metadata.files.len(), 2);
    assert_eq!(
        dataset.get_fragments()[0].metadata.files[0].fields.as_ref(),
        &[0]
    );
    assert_eq!(
        dataset.get_fragments()[0].metadata.files[1].fields.as_ref(),
        &[1]
    );

    let batch = dataset.scan().try_into_batch().await.unwrap();
    assert_eq!(batch.num_rows(), 3);
    assert_eq!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .values(),
        &[1, 2, 3]
    );
    assert_eq!(
        batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .values(),
        &[4, 5, 6]
    );

    // do it again but on the first column
    // find the datafile we want to replace
    let new_data_file = DataFile {
        path: "test.lance".to_string(),
        // the first column in the dataset
        fields: Arc::from([0]),
        // is located in the first column of this datafile
        column_indices: Arc::from([0]),
        file_major_version: major,
        file_minor_version: minor,
        file_size_bytes: CachedFileSize::unknown(),
        base_id: None,
    };

    let dataset = Dataset::commit(
        WriteDestination::Dataset(Arc::new(dataset)),
        Operation::DataReplacement {
            replacements: vec![DataReplacementGroup(0, new_data_file)],
        },
        Some(4),
        None,
        None,
        Arc::new(Default::default()),
        false,
    )
    .await
    .unwrap();

    assert_eq!(dataset.version().version, 5);
    assert_eq!(dataset.get_fragments().len(), 1);
    assert_eq!(dataset.get_fragments()[0].metadata.files.len(), 2);

    let batch = dataset.scan().try_into_batch().await.unwrap();
    assert_eq!(batch.num_rows(), 3);
    assert_eq!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .values(),
        &[4, 5, 6]
    );
    assert_eq!(
        batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .values(),
        &[4, 5, 6]
    );
}

#[tokio::test]
async fn test_datafile_replacement_error() {
    let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
        "a",
        DataType::Int32,
        true,
    )]));
    let empty_reader = RecordBatchIterator::new(vec![], schema.clone());
    let mut dataset = Dataset::write(empty_reader, "memory://", None)
        .await
        .unwrap();
    dataset.validate().await.unwrap();

    let vals: Int32Array = vec![1, 2, 3].into();
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(vals)]).unwrap();
    dataset
        .append(
            RecordBatchIterator::new(vec![Ok(batch)], schema.clone()),
            None,
        )
        .await
        .unwrap();

    let fragment = dataset.get_fragments().pop().unwrap().metadata;

    let extended_schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("a", DataType::Int32, true),
        ArrowField::new("b", DataType::Int32, true),
    ]));

    // add all null column
    let dataset = Dataset::commit(
        WriteDestination::Dataset(Arc::new(dataset)),
        Operation::Merge {
            fragments: vec![fragment],
            schema: extended_schema.as_ref().try_into().unwrap(),
        },
        Some(2),
        None,
        None,
        Arc::new(Default::default()),
        false,
    )
    .await
    .unwrap();

    // find the datafile we want to replace
    let new_data_file = DataFile {
        path: "test.lance".to_string(),
        // the second column in the dataset
        fields: Arc::from([1]),
        // is located in the first column of this datafile
        column_indices: Arc::from([0]),
        file_major_version: 2,
        file_minor_version: 0,
        file_size_bytes: CachedFileSize::unknown(),
        base_id: None,
    };

    let new_data_file = DataFile {
        fields: Arc::from([0, 1]),
        ..new_data_file
    };

    let err = Dataset::commit(
        WriteDestination::Dataset(Arc::new(dataset.clone())),
        Operation::DataReplacement {
            replacements: vec![DataReplacementGroup(0, new_data_file)],
        },
        // read at the current version (after the Merge above)
        Some(dataset.manifest.version),
        None,
        None,
        Arc::new(Default::default()),
        false,
    )
    .await
    .unwrap_err();
    assert!(
        err.to_string()
            .contains("Expected to modify the fragment but no changes were made"),
        "Expected Error::DataFileReplacementError, got {:?}",
        err
    );
}

#[tokio::test]
async fn test_replace_dataset() {
    let test_dir = TempDir::default();
    let test_uri = test_dir.path_str();
    let test_path = test_dir.obj_path();

    let data = gen_batch()
        .col("int", array::step::<Int32Type>())
        .into_batch_rows(RowCount::from(20))
        .unwrap();
    let data1 = data.slice(0, 10);
    let data2 = data.slice(10, 10);
    let mut ds = InsertBuilder::new(&test_uri)
        .execute(vec![data1])
        .await
        .unwrap();

    ds.object_store
        .as_ref()
        .remove_dir_all(test_path)
        .await
        .unwrap();

    let ds2 = InsertBuilder::new(&test_uri)
        .execute(vec![data2.clone()])
        .await
        .unwrap();

    ds.checkout_latest().await.unwrap();
    let roundtripped = ds.scan().try_into_batch().await.unwrap();
    assert_eq!(roundtripped, data2);

    ds.validate().await.unwrap();
    ds2.validate().await.unwrap();
    assert_eq!(ds.manifest.version, 1);
    assert_eq!(ds2.manifest.version, 1);
}

#[tokio::test]
async fn test_insert_skip_auto_cleanup() {
    let test_uri = TempStrDir::default();

    // Create initial dataset with aggressive auto cleanup (interval=1, older_than=1ms)
    let data = gen_batch()
        .col("id", array::step::<Int32Type>())
        .into_reader_rows(RowCount::from(100), BatchCount::from(1));

    let write_params = WriteParams {
        mode: WriteMode::Create,
        auto_cleanup: Some(AutoCleanupParams {
            interval: 1,
            older_than: chrono::TimeDelta::try_milliseconds(0).unwrap(), // Cleanup versions older than 0ms
        }),
        ..Default::default()
    };

    // Start at 1 second after epoch
    MockClock::set_system_time(std::time::Duration::from_secs(1));

    let dataset = Dataset::write(data, &test_uri, Some(write_params))
        .await
        .unwrap();
    assert_eq!(dataset.version().version, 1);

    // Advance time by 1 second
    MockClock::set_system_time(std::time::Duration::from_secs(2));

    // First append WITHOUT skip_auto_cleanup - should trigger cleanup
    let data1 = gen_batch()
        .col("id", array::step::<Int32Type>())
        .into_df_stream(RowCount::from(50), BatchCount::from(1));

    let write_params1 = WriteParams {
        mode: WriteMode::Append,
        skip_auto_cleanup: false,
        ..Default::default()
    };

    let dataset2 = InsertBuilder::new(WriteDestination::Dataset(Arc::new(dataset)))
        .with_params(&write_params1)
        .execute_stream(data1)
        .await
        .unwrap();

    assert_eq!(dataset2.version().version, 2);

    // Advance time
    MockClock::set_system_time(std::time::Duration::from_secs(3));

    // Need to do another commit for cleanup to take effect since cleanup runs on the old dataset
    let data1_extra = gen_batch()
        .col("id", array::step::<Int32Type>())
        .into_df_stream(RowCount::from(10), BatchCount::from(1));

    let dataset2_extra = InsertBuilder::new(WriteDestination::Dataset(Arc::new(dataset2)))
        .with_params(&write_params1)
        .execute_stream(data1_extra)
        .await
        .unwrap();

    assert_eq!(dataset2_extra.version().version, 3);

    // Version 1 should be cleaned up due to auto cleanup (cleanup runs every version)
    assert!(
        dataset2_extra.checkout_version(1).await.is_err(),
        "Version 1 should have been cleaned up"
    );
    // Version 2 should still exist
    assert!(
        dataset2_extra.checkout_version(2).await.is_ok(),
        "Version 2 should still exist"
    );

    // Advance time
    MockClock::set_system_time(std::time::Duration::from_secs(4));

    // Second append WITH skip_auto_cleanup - should NOT trigger cleanup
    let data2 = gen_batch()
        .col("id", array::step::<Int32Type>())
        .into_df_stream(RowCount::from(30), BatchCount::from(1));

    let write_params2 = WriteParams {
        mode: WriteMode::Append,
        skip_auto_cleanup: true, // Skip auto cleanup
        ..Default::default()
    };

    let dataset3 = InsertBuilder::new(WriteDestination::Dataset(Arc::new(dataset2_extra)))
        .with_params(&write_params2)
        .execute_stream(data2)
        .await
        .unwrap();

    assert_eq!(dataset3.version().version, 4);

    // Version 2 should still exist because skip_auto_cleanup was enabled
    assert!(
        dataset3.checkout_version(2).await.is_ok(),
        "Version 2 should still exist because skip_auto_cleanup was enabled"
    );
    // Version 3 should also still exist
    assert!(
        dataset3.checkout_version(3).await.is_ok(),
        "Version 3 should still exist"
    );
}

#[tokio::test]
async fn test_nullable_struct_v2_1_issue_4385() {
    // Test for issue #4385: nullable struct should preserve null values in v2.1 format
    use arrow_array::cast::AsArray;
    use arrow_schema::Fields;

    // Create a struct field with nullable float field
    let struct_fields = Fields::from(vec![ArrowField::new("x", DataType::Float32, true)]);

    // Create outer struct with the nullable struct as a field (not root)
    let outer_fields = Fields::from(vec![
        ArrowField::new("id", DataType::Int32, false),
        ArrowField::new("data", DataType::Struct(struct_fields.clone()), true),
    ]);
    let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
        "record",
        DataType::Struct(outer_fields.clone()),
        false,
    )]));

    // Create data with null struct
    let id_values = Int32Array::from(vec![1, 2, 3]);
    let x_values = Float32Array::from(vec![Some(1.0), Some(2.0), Some(3.0)]);
    let inner_struct_array = StructArray::new(
        struct_fields,
        vec![Arc::new(x_values) as ArrayRef],
        Some(vec![true, false, true].into()), // Second struct is null
    );

    let outer_struct_array = StructArray::new(
        outer_fields,
        vec![
            Arc::new(id_values) as ArrayRef,
            Arc::new(inner_struct_array.clone()) as ArrayRef,
        ],
        None, // Outer struct is not nullable
    );

    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(outer_struct_array)]).unwrap();

    // Write dataset with v2.1 format
    let test_uri = TempStrDir::default();

    let write_params = WriteParams {
        mode: WriteMode::Create,
        data_storage_version: Some(LanceFileVersion::V2_1),
        ..Default::default()
    };

    let batches = vec![batch.clone()];
    let batch_reader = RecordBatchIterator::new(batches.into_iter().map(Ok), schema.clone());

    Dataset::write(batch_reader, &test_uri, Some(write_params))
        .await
        .unwrap();

    // Read back the dataset
    let dataset = Dataset::open(&test_uri).await.unwrap();
    let scanner = dataset.scan();
    let result_batches = scanner
        .try_into_stream()
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();

    assert_eq!(result_batches.len(), 1);
    let result_batch = &result_batches[0];
    let read_outer_struct = result_batch.column(0).as_struct();
    let read_inner_struct = read_outer_struct.column(1).as_struct(); // "data" field

    // The bug: null struct is not preserved
    assert!(
        read_inner_struct.is_null(1),
        "Second struct should be null but it's not. Read value: {:?}",
        read_inner_struct
    );

    // Verify the null count is preserved
    assert_eq!(
        inner_struct_array.null_count(),
        read_inner_struct.null_count(),
        "Null count should be preserved"
    );
}

#[tokio::test]
async fn test_issue_4902_packed_struct_v2_1_read_error() {
    use std::collections::HashMap;

    use arrow_array::{ArrayRef, Int32Array, RecordBatchIterator, StructArray, UInt32Array};
    use arrow_schema::{Field as ArrowField, Fields, Schema as ArrowSchema};

    let struct_fields = Fields::from(vec![
        ArrowField::new("x", DataType::UInt32, false),
        ArrowField::new("y", DataType::UInt32, false),
    ]);
    let mut packed_metadata = HashMap::new();
    packed_metadata.insert("packed".to_string(), "true".to_string());

    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("int_col", DataType::Int32, false),
        ArrowField::new("struct_col", DataType::Struct(struct_fields.clone()), false)
            .with_metadata(packed_metadata),
    ]));

    let int_values = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8]));
    let x_values = Arc::new(UInt32Array::from(vec![1, 4, 7, 10, 13, 16, 19, 22]));
    let y_values = Arc::new(UInt32Array::from(vec![2, 5, 8, 11, 14, 17, 20, 23]));
    let struct_array = Arc::new(StructArray::new(
        struct_fields,
        vec![x_values.clone() as ArrayRef, y_values.clone() as ArrayRef],
        None,
    ));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            int_values.clone() as ArrayRef,
            struct_array.clone() as ArrayRef,
        ],
    )
    .unwrap();

    let test_uri = TempStrDir::default();
    let write_params = WriteParams {
        mode: WriteMode::Create,
        data_storage_version: Some(LanceFileVersion::V2_1),
        ..Default::default()
    };
    let reader = RecordBatchIterator::new(vec![Ok(batch.clone())], schema.clone());
    Dataset::write(reader, &test_uri, Some(write_params))
        .await
        .unwrap();

    let dataset = Dataset::open(&test_uri).await.unwrap();

    let result_batches = dataset
        .scan()
        .try_into_stream()
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(result_batches, vec![batch.clone()]);

    let struct_batches = dataset
        .scan()
        .project(&["struct_col"])
        .unwrap()
        .try_into_stream()
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(struct_batches.len(), 1);
    let read_struct = struct_batches[0].column(0).as_struct();
    assert_eq!(read_struct, struct_array.as_ref());
}

#[tokio::test]
async fn test_issue_4429_nested_struct_encoding_v2_1_with_over_65k_structs() {
    // Regression test for miniblock 16KB limit with nested struct patterns
    // Tests encoding behavior when a nested struct<list<struct>> contains
    // large amounts of data that exceeds miniblock encoding limits

    // Create a struct with multiple fields that will trigger miniblock encoding
    // Each field is 4 bytes, making the struct narrow enough for miniblock
    let measurement_fields = vec![
        ArrowField::new("val_a", DataType::Float32, true),
        ArrowField::new("val_b", DataType::Float32, true),
        ArrowField::new("val_c", DataType::Float32, true),
        ArrowField::new("val_d", DataType::Float32, true),
        ArrowField::new("seq_high", DataType::Int32, true),
        ArrowField::new("seq_low", DataType::Int32, true),
    ];
    let measurement_type = DataType::Struct(measurement_fields.clone().into());

    // Create nested schema: struct<measurements: list<struct>>
    // This pattern can trigger encoding issues with large data volumes
    let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
        "data",
        DataType::Struct(
            vec![ArrowField::new(
                "measurements",
                DataType::List(Arc::new(ArrowField::new(
                    "item",
                    measurement_type.clone(),
                    true,
                ))),
                true,
            )]
            .into(),
        ),
        true,
    )]));

    // Create large number of measurements that will exceed encoding limits
    // Using 70,520 to match the exact problematic size
    const NUM_MEASUREMENTS: usize = 70_520;

    // Generate data for two full sets (rows 0 and 2 will have data, row 1 empty)
    const TOTAL_MEASUREMENTS: usize = NUM_MEASUREMENTS * 2;

    // Create arrays with realistic values
    let val_a_array =
        Float32Array::from_iter((0..TOTAL_MEASUREMENTS).map(|i| Some(16.66 + (i as f32 * 0.0001))));
    let val_b_array =
        Float32Array::from_iter((0..TOTAL_MEASUREMENTS).map(|i| Some(-3.54 + (i as f32 * 0.0002))));
    let val_c_array =
        Float32Array::from_iter((0..TOTAL_MEASUREMENTS).map(|i| Some(2.94 + (i as f32 * 0.0001))));
    let val_d_array =
        Float32Array::from_iter((0..TOTAL_MEASUREMENTS).map(|i| Some(((i % 50) + 10) as f32)));
    let seq_high_array = Int32Array::from_iter((0..TOTAL_MEASUREMENTS).map(|_| Some(1736962329)));
    let seq_low_array =
        Int32Array::from_iter((0..TOTAL_MEASUREMENTS).map(|i| Some(304403000 + (i * 1000) as i32)));

    // Create the struct array with all measurements
    let struct_array = StructArray::from(vec![
        (
            Arc::new(ArrowField::new("val_a", DataType::Float32, true)),
            Arc::new(val_a_array) as ArrayRef,
        ),
        (
            Arc::new(ArrowField::new("val_b", DataType::Float32, true)),
            Arc::new(val_b_array) as ArrayRef,
        ),
        (
            Arc::new(ArrowField::new("val_c", DataType::Float32, true)),
            Arc::new(val_c_array) as ArrayRef,
        ),
        (
            Arc::new(ArrowField::new("val_d", DataType::Float32, true)),
            Arc::new(val_d_array) as ArrayRef,
        ),
        (
            Arc::new(ArrowField::new("seq_high", DataType::Int32, true)),
            Arc::new(seq_high_array) as ArrayRef,
        ),
        (
            Arc::new(ArrowField::new("seq_low", DataType::Int32, true)),
            Arc::new(seq_low_array) as ArrayRef,
        ),
    ]);

    // Create list array with pattern: [70520 items, 0 items, 70520 items]
    // This pattern triggers the issue with V2.1 encoding
    let offsets = vec![
        0i32,
        NUM_MEASUREMENTS as i32,       // End of row 0
        NUM_MEASUREMENTS as i32,       // End of row 1 (empty)
        (NUM_MEASUREMENTS * 2) as i32, // End of row 2
    ];
    let list_array = ListArray::try_new(
        Arc::new(ArrowField::new("item", measurement_type, true)),
        arrow_buffer::OffsetBuffer::new(arrow_buffer::ScalarBuffer::from(offsets)),
        Arc::new(struct_array) as ArrayRef,
        None,
    )
    .unwrap();

    // Create the outer struct wrapping the list
    let data_struct = StructArray::from(vec![(
        Arc::new(ArrowField::new(
            "measurements",
            DataType::List(Arc::new(ArrowField::new(
                "item",
                DataType::Struct(measurement_fields.into()),
                true,
            ))),
            true,
        )),
        Arc::new(list_array) as ArrayRef,
    )]);

    // Create the final record batch with 3 rows
    let batch =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(data_struct) as ArrayRef]).unwrap();

    assert_eq!(batch.num_rows(), 3, "Should have exactly 3 rows");

    let test_uri = TempStrDir::default();

    // Test with V2.1 format which has different encoding behavior
    let batches = vec![batch];
    let reader = RecordBatchIterator::new(batches.into_iter().map(Ok), schema.clone());

    // V2.1 format triggers miniblock encoding for narrow structs
    let write_params = WriteParams {
        data_storage_version: Some(lance_file::version::LanceFileVersion::V2_1),
        ..Default::default()
    };

    // Write dataset - this will panic with miniblock 16KB assertion
    let dataset = Dataset::write(reader, &test_uri, Some(write_params))
        .await
        .unwrap();

    dataset.validate().await.unwrap();
    assert_eq!(dataset.count_rows(None).await.unwrap(), 3);
}

/// Regression test for https://github.com/lancedb/lance/issues/5321
///
/// merge_insert with reordered columns triggers the RewriteColumns path,
/// which prunes the index bitmap. After compact + optimize_indices, the old
/// stale B-tree data was being merged back in, causing "non-existent fragment"
/// errors on subsequent queries.
#[tokio::test]
async fn test_merge_insert_with_reordered_columns_and_index() {
    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", DataType::Int32, false),
        ArrowField::new("value", DataType::Utf8, true),
    ]));

    // Step 1: Create dataset with one row {id: 1, value: "a"}
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![0, 1])),
            Arc::new(StringArray::from(vec!["x", "a"])),
        ],
    )
    .unwrap();
    let reader = RecordBatchIterator::new(vec![Ok(batch)], schema.clone());
    let mut dataset = Dataset::write(
        reader,
        "memory://test_5321",
        Some(WriteParams {
            max_rows_per_file: 1, // Force multiple fragments for testing
            ..Default::default()
        }),
    )
    .await
    .unwrap();

    // Step 2: Create BTree index on 'id'
    dataset
        .create_index(
            &["id"],
            IndexType::BTree,
            None,
            &ScalarIndexParams::default(),
            false,
        )
        .await
        .unwrap();

    // Step 3: merge_insert with reversed column order (value, id)
    // This triggers the RewriteColumns path, which prunes the index bitmap
    let reversed_schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("value", DataType::Utf8, true),
        ArrowField::new("id", DataType::Int32, false),
    ]));
    let source_batch = RecordBatch::try_new(
        reversed_schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["b", "c"])),
            Arc::new(Int32Array::from(vec![1, 2])),
        ],
    )
    .unwrap();

    let merge_job = MergeInsertBuilder::try_new(Arc::new(dataset.clone()), vec!["id".to_string()])
        .unwrap()
        .when_matched(WhenMatched::UpdateAll)
        .when_not_matched(WhenNotMatched::InsertAll)
        .try_build()
        .unwrap();

    let reader = Box::new(RecordBatchIterator::new(
        vec![Ok(source_batch)],
        reversed_schema.clone(),
    ));
    let (dataset, _stats) = merge_job.execute(reader_to_stream(reader)).await.unwrap();
    let mut dataset = dataset.as_ref().clone();

    // Step 4: compact_files
    compact_files(&mut dataset, CompactionOptions::default(), None)
        .await
        .unwrap();

    // Step 5: optimize_indices
    dataset
        .optimize_indices(&OptimizeOptions::default())
        .await
        .unwrap();

    // Step 6: Another merge_insert should NOT error
    let source_batch2 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(StringArray::from(vec!["d"])),
        ],
    )
    .unwrap();

    let merge_job2 = MergeInsertBuilder::try_new(Arc::new(dataset.clone()), vec!["id".to_string()])
        .unwrap()
        .when_matched(WhenMatched::UpdateAll)
        .when_not_matched(WhenNotMatched::InsertAll)
        .try_build()
        .unwrap();

    let reader2 = Box::new(RecordBatchIterator::new(
        vec![Ok(source_batch2)],
        schema.clone(),
    ));
    let (final_dataset, _) = merge_job2.execute(reader_to_stream(reader2)).await.unwrap();
    final_dataset.validate().await.unwrap();
}

/// With stable row ids, updating a top-level struct column keeps a scalar index on a
/// nested child field correct. The update API rejects nested column references, so a
/// nested field can only be changed by setting its whole struct column; that update must
/// not wrongly extend the child-field index over the rewritten fragment (which would
/// leave the updated value unscanned and silently dropped).
#[tokio::test]
async fn test_update_struct_column_keeps_nested_index() {
    let struct_fields = Fields::from(vec![ArrowField::new("x", DataType::Int32, true)]);
    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", DataType::Int32, false),
        ArrowField::new("s", DataType::Struct(struct_fields.clone()), true),
    ]));
    let s_arr = StructArray::new(
        struct_fields.clone(),
        vec![Arc::new(Int32Array::from(vec![10, 20, 30])) as ArrayRef],
        None,
    );
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
            Arc::new(s_arr) as ArrayRef,
        ],
    )
    .unwrap();
    let reader = RecordBatchIterator::new(vec![Ok(batch)], schema.clone());
    let mut dataset = Dataset::write(
        reader,
        "memory://test_update_nested_index",
        Some(WriteParams {
            enable_stable_row_ids: true,
            ..Default::default()
        }),
    )
    .await
    .unwrap();

    // BTree index on the NESTED field `s.x`.
    dataset
        .create_index(
            &["s.x"],
            IndexType::BTree,
            None,
            &ScalarIndexParams::default(),
            false,
        )
        .await
        .unwrap();

    let pre = dataset
        .scan()
        .filter("s.x = 20")
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    assert_eq!(pre.num_rows(), 1, "precondition: s.x=20 should match id=2");

    // Nested column references are rejected by `set`, so update the whole struct column
    // `s` for id=2, changing s.x 20 -> 999.
    let update_result = UpdateBuilder::new(Arc::new(dataset.clone()))
        .update_where("id = 2")
        .unwrap()
        .set("s", "named_struct('x', cast(999 as int))")
        .unwrap()
        .build()
        .unwrap()
        .execute()
        .await
        .unwrap();
    let dataset = update_result.new_dataset;

    // The nested `s.x` index must NOT be extended to the rewritten fragment: its
    // effective coverage stays {0}, so the rewritten fragment is left unindexed and
    // fully scanned.
    let sx_idx = dataset
        .load_indices()
        .await
        .unwrap()
        .iter()
        .find(|i| i.fields.len() == 1)
        .expect("nested s.x index")
        .clone();
    let effective = sx_idx
        .effective_fragment_bitmap(&dataset.fragment_bitmap)
        .expect("index has a fragment bitmap");
    assert_eq!(
        effective.iter().collect::<Vec<_>>(),
        vec![0],
        "nested-field index must not be extended to the rewritten fragment"
    );

    // The updated value must be found, and the stale value gone.
    let new = dataset
        .scan()
        .filter("s.x = 999")
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    assert_eq!(
        new.num_rows(),
        1,
        "updated value s.x=999 must be found after the struct-column update"
    );
    let old = dataset
        .scan()
        .filter("s.x = 20")
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    assert_eq!(old.num_rows(), 0, "s.x=20 should no longer match any row");
}

/// Sum a named execution metric (e.g. `fragments_scanned`) across every node of a
/// physical plan. Used to observe FilteredReadExec data-scan behavior.
fn sum_scan_metric(plan: &Arc<dyn datafusion::physical_plan::ExecutionPlan>, name: &str) -> usize {
    let mut total = plan
        .metrics()
        .and_then(|m| m.sum_by_name(name))
        .map(|v| v.as_usize())
        .unwrap_or(0);
    for child in plan.children() {
        total += sum_scan_metric(child, name);
    }
    total
}

/// Control: with stable row ids, a merge_insert full-row update of a *flat*
/// indexed column is handled correctly. The flat field's id is in
/// `fields_for_preserving_frag_bitmap`, so the RewriteRows index-maintenance path
/// does NOT extend the index bitmap to the rewritten fragment; that fragment stays
/// unindexed and is fully scanned, so the new value is found and the old is not.
/// Contrast with `test_merge_insert_nested_index_stable_row_id`.
#[tokio::test]
async fn test_merge_insert_flat_index_stable_row_id() {
    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", DataType::Int32, false),
        ArrowField::new("val", DataType::Int32, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(Int32Array::from(vec![10, 20, 30])),
        ],
    )
    .unwrap();
    let reader = RecordBatchIterator::new(vec![Ok(batch)], schema.clone());
    let mut dataset = Dataset::write(
        reader,
        "memory://test_mi_flat_index",
        Some(WriteParams {
            enable_stable_row_ids: true,
            ..Default::default()
        }),
    )
    .await
    .unwrap();

    // BTree index on the flat indexed column `val`.
    dataset
        .create_index(
            &["val"],
            IndexType::BTree,
            None,
            &ScalarIndexParams::default(),
            false,
        )
        .await
        .unwrap();

    // Sanity: index returns id=2 for val=20 before the update.
    let pre = dataset
        .scan()
        .filter("val = 20")
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    assert_eq!(pre.num_rows(), 1, "precondition: val=20 should match id=2");

    // Full-row update of id=2 changing val 20 -> 999. Only id=2 is in the source
    // (all matched, no inserts) so the new fragment is a pure rewrite-rows fragment.
    let source = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![2])),
            Arc::new(Int32Array::from(vec![999])),
        ],
    )
    .unwrap();
    let merge_job = MergeInsertBuilder::try_new(Arc::new(dataset.clone()), vec!["id".to_string()])
        .unwrap()
        .when_matched(WhenMatched::UpdateAll)
        .when_not_matched(WhenNotMatched::DoNothing)
        .try_build()
        .unwrap();
    let reader = Box::new(RecordBatchIterator::new(vec![Ok(source)], schema.clone()));
    let (dataset, _stats) = merge_job.execute(reader_to_stream(reader)).await.unwrap();

    // Index bitmap coverage: the guard did NOT extend the bitmap, so the index
    // still covers only the original fragment 0, not the rewritten fragment.
    let val_idx = dataset
        .load_indices()
        .await
        .unwrap()
        .iter()
        .find(|i| i.name == "val_idx")
        .unwrap()
        .clone();
    assert_eq!(
        val_idx
            .fragment_bitmap
            .as_ref()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![0],
        "flat index must not be extended to the rewritten fragment"
    );

    // The scan is PARTIALLY indexed, not fully indexed: the index's effective
    // coverage ({0}) is a strict subset of the live fragments ({0, 1}), so the
    // rewritten fragment is left to a full scan.
    assert_eq!(
        dataset.fragment_bitmap.iter().collect::<Vec<_>>(),
        vec![0, 1],
        "the update should have produced a second fragment"
    );
    let effective = val_idx
        .effective_fragment_bitmap(&dataset.fragment_bitmap)
        .expect("index has a fragment bitmap");
    assert_eq!(
        effective.iter().collect::<Vec<_>>(),
        vec![0],
        "index effectively covers only fragment 0"
    );
    assert!(
        effective.len() < dataset.fragment_bitmap.len(),
        "scan must be partially indexed: the rewritten fragment is not covered by the index"
    );

    // Project only _rowid. The scan is PARTIALLY indexed: because the rewritten
    // fragment is not covered by the index, FilteredReadExec must DATA-SCAN it to
    // surface the moved value, which shows up in its `fragments_scanned` metric.
    let mut scanner = dataset.scan();
    scanner
        .empty_project()
        .unwrap()
        .with_row_id()
        .filter("val = 999")
        .unwrap();
    let plan = scanner.create_plan().await.unwrap();
    let batches = datafusion::physical_plan::collect(
        plan.clone(),
        Arc::new(datafusion::execution::TaskContext::default()),
    )
    .await
    .unwrap();
    // Exactly one fragment — the uncovered (rewritten) one — is data-scanned.
    let fragments_scanned = sum_scan_metric(&plan, "fragments_scanned");
    assert_eq!(
        fragments_scanned, 1,
        "partially-indexed scan must data-scan exactly the one uncovered fragment, but fragments_scanned={fragments_scanned}"
    );
    let val999_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        val999_rows, 1,
        "val=999 lives only in the unindexed fragment and must be found by the full scan"
    );

    // The stale old value is gone (the index-covered fragment's row was moved out).
    let val20_rows = dataset
        .scan()
        .empty_project()
        .unwrap()
        .with_row_id()
        .filter("val = 20")
        .unwrap()
        .try_into_batch()
        .await
        .unwrap()
        .num_rows();
    assert_eq!(val20_rows, 0, "val=20 should no longer match any row");
}

/// With stable row ids, a full-row merge_insert update of a column covered by a
/// NESTED-field scalar index keeps that index correct.
///
/// The updated row keeps its row id and moves to a new fragment.
/// `register_pure_rewrite_rows_update_frags_in_indices` decides whether to extend each
/// index's bitmap to that new fragment by testing `index.fields` against
/// `fields_for_preserving_frag_bitmap`, which merge_insert builds via `fields_pre_order()`
/// so that nested leaf ids are included. The nested `s.x` index must therefore NOT be
/// extended over the rewritten fragment: that fragment stays unindexed and is fully
/// scanned, so the updated value is found.
///
/// Regression guard: before the fix the set was built from top-level `schema().fields`,
/// omitting the nested leaf id, so the index was wrongly extended over the rewritten
/// fragment and the updated value was silently dropped.
#[tokio::test]
async fn test_merge_insert_nested_index_stable_row_id() {
    let struct_fields = Fields::from(vec![ArrowField::new("x", DataType::Int32, false)]);
    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", DataType::Int32, false),
        ArrowField::new("s", DataType::Struct(struct_fields.clone()), false),
    ]));
    let make_batch = |ids: Vec<i32>, xs: Vec<i32>| {
        let s = StructArray::new(
            struct_fields.clone(),
            vec![Arc::new(Int32Array::from(xs)) as ArrayRef],
            None,
        );
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(ids)) as ArrayRef,
                Arc::new(s) as ArrayRef,
            ],
        )
        .unwrap()
    };

    let reader = RecordBatchIterator::new(
        vec![Ok(make_batch(vec![1, 2, 3], vec![10, 20, 30]))],
        schema.clone(),
    );
    let mut dataset = Dataset::write(
        reader,
        "memory://test_mi_nested_index",
        Some(WriteParams {
            enable_stable_row_ids: true,
            ..Default::default()
        }),
    )
    .await
    .unwrap();

    // BTree index on the NESTED field `s.x`.
    dataset
        .create_index(
            &["s.x"],
            IndexType::BTree,
            None,
            &ScalarIndexParams::default(),
            false,
        )
        .await
        .unwrap();

    // Sanity: index finds id=2 for s.x = 20.
    let pre = dataset
        .scan()
        .filter("s.x = 20")
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    assert_eq!(pre.num_rows(), 1, "precondition: s.x=20 should match id=2");

    // Full-row merge_insert update of id=2 changing s.x 20 -> 999 (pure rewrite-rows fragment).
    let merge_job = MergeInsertBuilder::try_new(Arc::new(dataset.clone()), vec!["id".to_string()])
        .unwrap()
        .when_matched(WhenMatched::UpdateAll)
        .when_not_matched(WhenNotMatched::DoNothing)
        .try_build()
        .unwrap();
    let reader = Box::new(RecordBatchIterator::new(
        vec![Ok(make_batch(vec![2], vec![999]))],
        schema.clone(),
    ));
    let (dataset, _stats) = merge_job.execute(reader_to_stream(reader)).await.unwrap();

    // The rewritten fragment must NOT be covered by the nested `s.x` index, so
    // FilteredReadExec data-scans exactly that one fragment and finds the updated value.
    // (Before the fix the index was wrongly extended over the rewritten fragment, so it
    // was never data-scanned — fragments_scanned == 0 — and the value was dropped.)
    let mut scanner = dataset.scan();
    scanner
        .empty_project()
        .unwrap()
        .with_row_id()
        .filter("s.x = 999")
        .unwrap();
    let plan = scanner.create_plan().await.unwrap();
    let batches = datafusion::physical_plan::collect(
        plan.clone(),
        Arc::new(datafusion::execution::TaskContext::default()),
    )
    .await
    .unwrap();
    let fragments_scanned = sum_scan_metric(&plan, "fragments_scanned");
    assert_eq!(
        fragments_scanned, 1,
        "the rewritten fragment must be data-scanned exactly once, but fragments_scanned={fragments_scanned}"
    );
    let sx999_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        sx999_rows, 1,
        "updated value s.x=999 must be found after the nested-field merge_insert update"
    );
}

/// With stable row ids, a merge_insert full-row update invalidates EVERY column index
/// for the rewritten rows — not only indices on columns that actually changed — because
/// merge_insert treats the whole schema as modified (it does not detect which columns
/// changed). Here `col1` is updated and `col2` is left unchanged, yet both `col1_idx`
/// and `col2_idx` drop the rewritten fragment from their coverage.
#[tokio::test]
async fn test_merge_insert_flat_index_stable_row_id_multiple_indexes() {
    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", DataType::Int32, false),
        ArrowField::new("col1", DataType::Int32, false),
        ArrowField::new("col2", DataType::Int32, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(Int32Array::from(vec![10, 20])),
            Arc::new(Int32Array::from(vec![100, 200])),
        ],
    )
    .unwrap();
    let reader = RecordBatchIterator::new(vec![Ok(batch)], schema.clone());
    let mut dataset = Dataset::write(
        reader,
        "memory://test_mi_flat_multi_index",
        Some(WriteParams {
            enable_stable_row_ids: true,
            ..Default::default()
        }),
    )
    .await
    .unwrap();

    for col in ["col1", "col2"] {
        dataset
            .create_index(
                &[col],
                IndexType::BTree,
                None,
                &ScalarIndexParams::default(),
                false,
            )
            .await
            .unwrap();
    }

    // Full-row update of id=1: col1 10 -> 999, col2 left unchanged (100).
    let source = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(Int32Array::from(vec![999])),
            Arc::new(Int32Array::from(vec![100])),
        ],
    )
    .unwrap();
    let merge_job = MergeInsertBuilder::try_new(Arc::new(dataset.clone()), vec!["id".to_string()])
        .unwrap()
        .when_matched(WhenMatched::UpdateAll)
        .when_not_matched(WhenNotMatched::DoNothing)
        .try_build()
        .unwrap();
    let reader = Box::new(RecordBatchIterator::new(vec![Ok(source)], schema.clone()));
    let (dataset, _stats) = merge_job.execute(reader_to_stream(reader)).await.unwrap();

    // The update produced a second fragment holding the rewritten row id=1.
    assert_eq!(
        dataset.fragment_bitmap.iter().collect::<Vec<_>>(),
        vec![0, 1],
        "update should have produced a second fragment"
    );

    let indices = dataset.load_indices().await.unwrap();
    let covered = |name: &str| {
        indices
            .iter()
            .find(|i| i.name == name)
            .unwrap()
            .fragment_bitmap
            .as_ref()
            .unwrap()
            .iter()
            .collect::<Vec<_>>()
    };

    // The index on the CHANGED column is invalidated (the rewritten fragment is not
    // covered) -- expected.
    assert_eq!(
        covered("col1_idx"),
        vec![0],
        "col1 index must drop the rewritten fragment (col1 changed)"
    );

    // ALL column indexes are invalidated, not only the changed ones: the index on the
    // UNCHANGED column `col2` also drops the rewritten fragment.
    //
    // TODO(stable-row-id optimization): merge_insert treats the whole schema as modified
    // because it does not detect which columns actually changed, so every index is
    // invalidated for the rewritten rows. With stable row ids the moved rows keep their
    // row ids and col2's values are unchanged, so `col2_idx` could instead be EXTENDED to
    // the rewritten fragment (preserving its coverage) rather than invalidated, avoiding
    // an unnecessary reindex. See `register_pure_rewrite_rows_update_frags_in_indices`.
    assert_eq!(
        covered("col2_idx"),
        vec![0],
        "col2 index is also invalidated even though col2 was not changed (see TODO)"
    );
}

#[tokio::test]
async fn test_data_replacement_advances_row_lineage() {
    let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
        "value",
        DataType::Int32,
        true,
    )]));
    let batch =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![1, 2]))]).unwrap();
    let reader = RecordBatchIterator::new(vec![Ok(batch)], schema.clone());
    let dataset = Dataset::write(
        reader,
        "memory://",
        Some(WriteParams {
            enable_stable_row_ids: true,
            ..Default::default()
        }),
    )
    .await
    .unwrap();

    let replacement = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int32Array::from(vec![10, 20]))],
    )
    .unwrap();
    let object_writer = dataset
        .object_store
        .create(&Path::from("data/lineage_replacement.lance"))
        .await
        .unwrap();
    let mut writer = lance_file::versions::v2_1::create_writer(
        object_writer,
        schema.as_ref().try_into().unwrap(),
        Default::default(),
    )
    .unwrap();
    writer.write_batch(&replacement).await.unwrap();
    writer.finish().await.unwrap();

    let frag = dataset.get_fragment(0).unwrap();
    let mut new_data_file = frag.data_file_for_field(0).unwrap().clone();
    new_data_file.path = "lineage_replacement.lance".to_string();

    let read_version = dataset.version().version;
    let dataset = Dataset::commit(
        WriteDestination::Dataset(Arc::new(dataset)),
        Operation::DataReplacement {
            replacements: vec![DataReplacementGroup(0, new_data_file)],
        },
        Some(read_version),
        None,
        None,
        Arc::new(Default::default()),
        false,
    )
    .await
    .unwrap();
    assert_eq!(dataset.version().version, 2);

    // The rows read differently now, so their last-updated stamp has to name
    // the version that changed them or get_updated_rows will never see them.
    let batch = dataset
        .scan()
        .project(&["value", ROW_LAST_UPDATED_AT_VERSION])
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    assert_eq!(
        batch["value"].as_primitive::<Int32Type>().values(),
        &[10, 20]
    );
    assert_eq!(
        batch[ROW_LAST_UPDATED_AT_VERSION]
            .as_primitive::<UInt64Type>()
            .values(),
        &[2, 2]
    );
}

/// DataReplacement should invalidate index fragment bitmaps for replaced fields.
#[tokio::test]
async fn test_data_replacement_invalidates_index_bitmap() {
    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("a", DataType::Int32, true),
        ArrowField::new("b", DataType::Int32, true),
    ]));

    // Create dataset with 2 columns
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(Int32Array::from(vec![10, 20, 30])),
        ],
    )
    .unwrap();
    let reader = RecordBatchIterator::new(vec![Ok(batch)], schema.clone());
    let mut dataset = Dataset::write(reader, "memory://test_replacement_idx", None)
        .await
        .unwrap();

    // Create scalar index on column 'a'
    dataset
        .create_index(
            &["a"],
            IndexType::BTree,
            None,
            &ScalarIndexParams::default(),
            false,
        )
        .await
        .unwrap();

    // Verify fragment 0 is in the index bitmap
    let indices = dataset.load_indices().await.unwrap();
    let a_index = indices.iter().find(|idx| idx.name == "a_idx").unwrap();
    assert!(a_index.fragment_bitmap.as_ref().unwrap().contains(0));

    // Write a replacement data file for column 'a'
    let single_col_schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
        "a",
        DataType::Int32,
        true,
    )]));
    let replacement_batch = RecordBatch::try_new(
        single_col_schema.clone(),
        vec![Arc::new(Int32Array::from(vec![4, 5, 6]))],
    )
    .unwrap();

    let object_writer = dataset
        .object_store
        .create(&Path::from("data/replacement.lance"))
        .await
        .unwrap();
    let mut writer = lance_file::versions::v2_1::create_writer(
        object_writer,
        single_col_schema.as_ref().try_into().unwrap(),
        Default::default(),
    )
    .unwrap();
    writer.write_batch(&replacement_batch).await.unwrap();
    writer.finish().await.unwrap();

    // Build replacement DataFile matching the existing data file for column 'a'
    let frag = dataset.get_fragment(0).unwrap();
    let data_file = frag.data_file_for_field(0).unwrap();
    let mut new_data_file = data_file.clone();
    new_data_file.path = "replacement.lance".to_string();

    // Commit DataReplacement
    let read_version = dataset.version().version;
    let dataset = Dataset::commit(
        WriteDestination::Dataset(Arc::new(dataset)),
        Operation::DataReplacement {
            replacements: vec![DataReplacementGroup(0, new_data_file)],
        },
        Some(read_version),
        None,
        None,
        Arc::new(Default::default()),
        false,
    )
    .await
    .unwrap();

    // The index bitmap for 'a' should no longer contain fragment 0
    let indices = dataset.load_indices().await.unwrap();
    let a_index = indices.iter().find(|idx| idx.name == "a_idx").unwrap();
    let effective = a_index
        .effective_fragment_bitmap(&dataset.fragment_bitmap)
        .unwrap();
    assert!(
        !effective.contains(0),
        "Fragment 0 should be removed from index bitmap after DataReplacement on indexed column"
    );
}

/// Run a predicate over `col` twice -- once index-served, once via a forced flat scan
/// (`use_scalar_index(false)`) -- assert the two agree, and return the matching `col`
/// values sorted. Equality is the index-consistency invariant: a divergence means the
/// index served rows that disagree with the underlying data.
async fn index_consistent_values(dataset: &Dataset, col: &str, predicate: &str) -> Vec<i32> {
    let sorted = |batch: &RecordBatch| -> Vec<i32> {
        let mut v: Vec<i32> = batch
            .column_by_name(col)
            .unwrap()
            .as_primitive::<Int32Type>()
            .iter()
            .flatten()
            .collect();
        v.sort();
        v
    };

    let indexed = dataset
        .scan()
        .filter(predicate)
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    let flat = dataset
        .scan()
        .use_scalar_index(false)
        .filter(predicate)
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();

    let indexed_vals = sorted(&indexed);
    let flat_vals = sorted(&flat);
    assert_eq!(
        indexed_vals, flat_vals,
        "index-served `{predicate}` disagrees with a flat scan"
    );
    indexed_vals
}

/// Build a Merge overlay fragment that rewrites a single `field_id` in place: tombstone
/// (-2) the field in `prev`'s existing data files and back it with `new_file` (a new
/// single-column file) instead. A file left with no live field is dropped. This is the
/// manifest shape an in-place column rewrite produces when it falls back from a
/// DataReplacement to a Merge.
fn build_overlay_frag(prev: &Fragment, field_id: i32, new_file: &str) -> Fragment {
    let mut overlay = prev.clone();
    overlay.files = prev
        .files
        .iter()
        .filter_map(|df| {
            let masked: Vec<i32> = df
                .fields
                .iter()
                .map(|&f| if f == field_id { -2 } else { f })
                .collect();
            if masked.iter().all(|&f| f == -2) {
                return None; // file holds only the tombstoned field
            }
            let mut m = df.clone();
            m.fields = masked.into();
            Some(m)
        })
        .collect();
    overlay.add_file(
        new_file,
        vec![field_id],
        vec![0],
        ConcreteFileVersion::from(LanceFileVersion::default()),
        None,
    );
    overlay
}

/// A `Merge` that rewrites an indexed column's data in place must keep that column's
/// index consistent: a query the index serves must return the same rows as a flat scan
/// of the rewritten data. The overlay fragment tombstones (-2) the column's field id in
/// the existing data file and appends a new file for it, so the field stays in the
/// schema and its index is retained -- the rewritten fragment must be pruned from that
/// index. This is the shape produced when an in-place column rewrite cannot be expressed
/// as a DataReplacement (e.g. an `update` has merged the fragment's column files) and
/// falls back to a Merge overlay.
#[tokio::test]
async fn test_merge_rewriting_indexed_column_keeps_index_consistent() {
    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("a", DataType::Int32, true),
        ArrowField::new("b", DataType::Int32, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
            Arc::new(Int32Array::from(vec![10, 20, 30, 40])),
        ],
    )
    .unwrap();
    let reader = RecordBatchIterator::new(vec![Ok(batch)], schema.clone());
    let mut dataset = Dataset::write(reader, "memory://merge_index_rewrite", None)
        .await
        .unwrap();

    dataset
        .create_index(
            &["a"],
            IndexType::BTree,
            None,
            &ScalarIndexParams::default(),
            false,
        )
        .await
        .unwrap();

    // Baseline: the index serves correct results before the rewrite.
    assert_eq!(
        index_consistent_values(&dataset, "a", "a >= 3").await,
        vec![3, 4]
    );

    let a_field_id = 0i32;

    // Write a new single-column file holding `a`'s replacement values.
    let a_only = Arc::new(ArrowSchema::new(vec![ArrowField::new(
        "a",
        DataType::Int32,
        true,
    )]));
    let new_a = RecordBatch::try_new(
        a_only.clone(),
        vec![Arc::new(Int32Array::from(vec![91, 92, 93, 94]))],
    )
    .unwrap();
    let new_a_path = dataset.data_dir().join("merge_new_a.lance");
    let object_writer = dataset.object_store.create(&new_a_path).await.unwrap();
    let mut writer = lance_file::versions::v2_1::create_writer(
        object_writer,
        a_only.as_ref().try_into().unwrap(),
        Default::default(),
    )
    .unwrap();
    writer.write_batch(&new_a).await.unwrap();
    writer.finish().await.unwrap();

    // Overlay that file onto fragment 0, rewriting `a` in place.
    let prev = dataset.get_fragment(0).unwrap().metadata().clone();
    let overlay = build_overlay_frag(&prev, a_field_id, "merge_new_a.lance");

    let read_version = dataset.version().version;
    let dataset = Dataset::commit(
        WriteDestination::Dataset(Arc::new(dataset)),
        Operation::Merge {
            fragments: vec![overlay],
            schema: schema.as_ref().try_into().unwrap(),
        },
        Some(read_version),
        None,
        None,
        Arc::new(Default::default()),
        false,
    )
    .await
    .unwrap();

    // `a` now holds [91, 92, 93, 94]; an index-served query must reflect that.
    assert_eq!(
        index_consistent_values(&dataset, "a", "a >= 90").await,
        vec![91, 92, 93, 94],
        "index-served `a >= 90` must return the rewritten values"
    );
    assert!(
        index_consistent_values(&dataset, "a", "a < 90")
            .await
            .is_empty(),
        "no row satisfies `a < 90` after the rewrite"
    );
}

/// DataReplacement on an indexed column should remove the fragment from
/// fragment_bitmap AND add it to invalidated_fragment_bitmap so that
/// stale index entries are blocked at query time.
#[tokio::test]
async fn test_data_replacement_populates_invalidated_bitmap() {
    use object_store::path::Path;

    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", DataType::Int32, false),
        ArrowField::new("value", DataType::Int32, true),
    ]));

    // Create dataset with one fragment
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(Int32Array::from(vec![10, 20, 30])),
        ],
    )
    .unwrap();
    let reader = RecordBatchIterator::new(vec![Ok(batch)], schema.clone());
    let mut dataset = Dataset::write(reader, "memory://test_replacement_invalidated", None)
        .await
        .unwrap();

    // Create BTree index on 'value'
    dataset
        .create_index(
            &["value"],
            IndexType::BTree,
            None,
            &ScalarIndexParams::default(),
            false,
        )
        .await
        .unwrap();

    // Verify initial state: fragment 0 in bitmap, no invalidated fragments
    let indices = dataset.load_indices().await.unwrap();
    let idx = indices.iter().find(|i| i.name == "value_idx").unwrap();
    assert!(idx.fragment_bitmap.as_ref().unwrap().contains(0));

    // Write a replacement data file for column 'value'
    let value_schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
        "value",
        DataType::Int32,
        true,
    )]));
    let replacement_batch = RecordBatch::try_new(
        value_schema.clone(),
        vec![Arc::new(Int32Array::from(vec![40, 50, 60]))],
    )
    .unwrap();

    let object_writer = dataset
        .object_store
        .create(&Path::from("data/replacement_inv.lance"))
        .await
        .unwrap();
    let mut writer = lance_file::versions::v2_1::create_writer(
        object_writer,
        value_schema.as_ref().try_into().unwrap(),
        Default::default(),
    )
    .unwrap();
    writer.write_batch(&replacement_batch).await.unwrap();
    writer.finish().await.unwrap();

    // Build replacement DataFile
    let frag = dataset.get_fragment(0).unwrap();
    let lance_schema: lance_core::datatypes::Schema = schema.as_ref().try_into().unwrap();
    let value_field_id = lance_schema.field("value").unwrap().id;
    let data_file = frag.data_file_for_field(value_field_id as u32).unwrap();
    let mut new_data_file = data_file.clone();
    new_data_file.path = "replacement_inv.lance".to_string();

    // Commit DataReplacement
    let read_version = dataset.version().version;
    let dataset = Dataset::commit(
        WriteDestination::Dataset(Arc::new(dataset)),
        Operation::DataReplacement {
            replacements: vec![DataReplacementGroup(0, new_data_file)],
        },
        Some(read_version),
        None,
        None,
        Arc::new(Default::default()),
        false,
    )
    .await
    .unwrap();

    // Check: fragment 0 removed from fragment_bitmap
    let indices = dataset.load_indices().await.unwrap();
    let idx = indices.iter().find(|i| i.name == "value_idx").unwrap();
    assert!(
        !idx.fragment_bitmap.as_ref().unwrap().contains(0),
        "Fragment 0 should be removed from fragment_bitmap"
    );
}

/// Regression test (lance-format/lance#6283): after in-place update via
/// DataReplacement, stale FTS index entries for the replaced fragment must
/// be blocked at query time so searches reflect the new data.
#[tokio::test]
async fn test_fts_stale_entries_after_data_replacement() {
    use lance_index::scalar::{FullTextSearchQuery, inverted::InvertedIndexParams};

    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", DataType::Int32, false),
        ArrowField::new("text", DataType::Utf8, true),
    ]));

    // Step 1: Create dataset with 2 rows in separate fragments
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![0, 1])),
            Arc::new(StringArray::from(vec![
                "the quick brown fox",
                "the lazy dog",
            ])),
        ],
    )
    .unwrap();
    let reader = RecordBatchIterator::new(vec![Ok(batch)], schema.clone());
    let mut dataset = Dataset::write(
        reader,
        "memory://test_fts_incremental_reindex",
        Some(WriteParams {
            max_rows_per_file: 1, // Force 2 fragments
            ..Default::default()
        }),
    )
    .await
    .unwrap();

    // Step 2: Create FTS inverted index on 'text'
    let params = InvertedIndexParams::default();
    dataset
        .create_index(&["text"], IndexType::Inverted, None, &params, true)
        .await
        .unwrap();

    // Sanity check: "quick" and "lazy" should each return 1 result
    let results = dataset
        .scan()
        .full_text_search(FullTextSearchQuery::new("quick".to_owned()))
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    assert_eq!(results.num_rows(), 1);
    let results = dataset
        .scan()
        .full_text_search(FullTextSearchQuery::new("lazy".to_owned()))
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    assert_eq!(results.num_rows(), 1);

    // Step 3: Replace fragment 1's data file via DataReplacement.
    // The fragment ID stays the same, but the text changes from
    // "the lazy dog" to "a speedy cat". This prunes fragment 1
    // from the FTS index's fragment_bitmap.
    let frag1 = dataset.get_fragment(1).unwrap();
    let old_data_file = frag1.metadata().files[0].clone();

    // Write replacement data file with updated text
    let replacement_batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(StringArray::from(vec!["a speedy cat"])),
        ],
    )
    .unwrap();
    let replacement_path = dataset.data_dir().join("replacement.lance");
    let object_writer = dataset
        .object_store
        .create(&replacement_path)
        .await
        .unwrap();
    let mut writer = lance_file::versions::v2_1::create_writer(
        object_writer,
        schema.as_ref().try_into().unwrap(),
        Default::default(),
    )
    .unwrap();
    writer.write_batch(&replacement_batch).await.unwrap();
    writer.finish().await.unwrap();

    let mut new_data_file = old_data_file.clone();
    new_data_file.path = "replacement.lance".to_string();

    let read_version = dataset.manifest.version;
    let dataset = Dataset::commit(
        WriteDestination::Dataset(Arc::new(dataset)),
        Operation::DataReplacement {
            replacements: vec![DataReplacementGroup(1, new_data_file)],
        },
        Some(read_version),
        None,
        None,
        Arc::new(Default::default()),
        false,
    )
    .await
    .unwrap();

    // Verify the replacement worked — fragment 1 now has the new text
    let batch = dataset.scan().try_into_batch().await.unwrap();
    assert_eq!(batch.num_rows(), 2);

    // Step 4: FTS search should reflect the new data, not the old.
    // Fragment 1 is unindexed (pruned from fragment_bitmap), so the
    // scanner does a flat FTS scan on it and uses the index for fragment 0.

    // "speedy" is in the new text for fragment 1 — found via flat scan.
    let results = dataset
        .scan()
        .full_text_search(FullTextSearchQuery::new("speedy".to_owned()))
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    assert_eq!(results.num_rows(), 1);

    // "lazy" was in the OLD text for fragment 1. The index has stale
    // posting entries for it, but fragment 1 was pruned from the
    // fragment_bitmap. Flat scan of fragment 1 sees "a speedy cat"
    // which doesn't match. So 0 results.
    let results = dataset
        .scan()
        .full_text_search(FullTextSearchQuery::new("lazy".to_owned()))
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    assert_eq!(
        results.num_rows(),
        0,
        "Expected 0 results for 'lazy' (stale data, fragment 1 pruned from index)"
    );

    // "quick" is in fragment 0 which is still indexed — should still work.
    let results = dataset
        .scan()
        .full_text_search(FullTextSearchQuery::new("quick".to_owned()))
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    assert_eq!(results.num_rows(), 1);
}

/// Same scenario as test_fts_index_incremental_reindex_after_in_place_update
/// but with a vector (IVF_PQ) index instead of FTS.
#[tokio::test]
async fn test_vector_index_after_data_replacement() {
    use arrow_array::FixedSizeListArray;
    use lance_arrow::FixedSizeListArrayExt;
    use lance_index::vector::{ivf::IvfBuildParams, pq::PQBuildParams};
    use lance_testing::datagen::generate_random_array;

    const DIM: usize = 32;
    const ROWS_PER_FRAG: usize = 256;
    const TOTAL: usize = ROWS_PER_FRAG * 2;

    let fsl_field = ArrowField::new(
        "vector",
        DataType::FixedSizeList(
            Arc::new(ArrowField::new("item", DataType::Float32, true)),
            DIM as i32,
        ),
        true,
    );
    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", DataType::Int32, false),
        fsl_field,
    ]));

    // Step 1: Create dataset with TOTAL rows in 2 fragments.
    let vectors = generate_random_array(TOTAL * DIM);
    let vector_array =
        Arc::new(FixedSizeListArray::try_new_from_values(vectors, DIM as i32).unwrap());
    let ids: Vec<i32> = (0..TOTAL as i32).collect();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int32Array::from(ids)), vector_array.clone()],
    )
    .unwrap();
    let reader = RecordBatchIterator::new(vec![Ok(batch)], schema.clone());
    let mut dataset = Dataset::write(
        reader,
        "memory://test_vec_data_replacement",
        Some(WriteParams {
            max_rows_per_file: ROWS_PER_FRAG,
            ..Default::default()
        }),
    )
    .await
    .unwrap();
    assert_eq!(dataset.get_fragments().len(), 2);

    // Step 2: Create IVF_PQ vector index
    let ivf_params = IvfBuildParams::new(2);
    let pq_params = PQBuildParams {
        num_sub_vectors: 1,
        ..Default::default()
    };
    let params = crate::index::vector::VectorIndexParams::with_ivf_pq_params(
        lance_linalg::distance::MetricType::L2,
        ivf_params,
        pq_params,
    );
    dataset
        .create_index(&["vector"], IndexType::Vector, None, &params, true)
        .await
        .unwrap();

    // Sanity: nearest to all-zeros query with refine should return
    // results from both fragments.
    let query_zeros = Float32Array::from(vec![0.0_f32; DIM]);
    let results = dataset
        .scan()
        .nearest("vector", &query_zeros, 10)
        .unwrap()
        .refine(10)
        .try_into_batch()
        .await
        .unwrap();
    assert_eq!(results.num_rows(), 10);

    // Step 3: DataReplacement — replace fragment 1's data.
    // Write all-999.0 vectors so they are very far from origin.
    let frag1 = dataset.get_fragment(1).unwrap();
    let frag1_id = frag1.id() as u64;
    let old_data_file = frag1.metadata().files[0].clone();

    let far_values: Vec<f32> = vec![999.0_f32; ROWS_PER_FRAG * DIM];
    let far_vectors = Float32Array::from(far_values);
    let far_vector_array =
        FixedSizeListArray::try_new_from_values(far_vectors, DIM as i32).unwrap();
    let replacement_ids: Vec<i32> = (ROWS_PER_FRAG as i32..(TOTAL as i32)).collect();
    let replacement_batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(replacement_ids)),
            Arc::new(far_vector_array),
        ],
    )
    .unwrap();

    let replacement_path = dataset.data_dir().join("replacement.lance");
    let object_writer = dataset
        .object_store
        .create(&replacement_path)
        .await
        .unwrap();
    let mut writer = lance_file::versions::v2_1::create_writer(
        object_writer,
        schema.as_ref().try_into().unwrap(),
        Default::default(),
    )
    .unwrap();
    writer.write_batch(&replacement_batch).await.unwrap();
    writer.finish().await.unwrap();

    let mut new_data_file = old_data_file.clone();
    new_data_file.path = "replacement.lance".to_string();

    let read_version = dataset.manifest.version;
    let dataset = Dataset::commit(
        WriteDestination::Dataset(Arc::new(dataset)),
        Operation::DataReplacement {
            replacements: vec![DataReplacementGroup(frag1_id, new_data_file)],
        },
        Some(read_version),
        None,
        None,
        Arc::new(Default::default()),
        false,
    )
    .await
    .unwrap();

    // Step 4: Search — nearest to all-zeros WITHOUT refine.
    // Fragment 1's vectors are now all-999.0 (very far from origin).
    // The top-10 nearest should all come from fragment 0.
    // If stale index entries leak through, results from fragment 1
    // would appear with their old (closer) PQ-approximated distances.
    let results = dataset
        .scan()
        .nearest("vector", &query_zeros, 10)
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    let ids = results["id"].as_any().downcast_ref::<Int32Array>().unwrap();
    for i in 0..results.num_rows() {
        assert!(
            (ids.value(i) as usize) < ROWS_PER_FRAG,
            "Result {} has id={} which is from fragment 1 (stale index entry leaked through)",
            i,
            ids.value(i)
        );
    }
}

/// Regression test: inverted (FTS) index should not carry stale data after
/// merge_insert + compact + optimize_indices.
///
/// This is the FTS equivalent of test_merge_insert_with_reordered_columns_and_index.
/// The inverted index's update() ignores the valid_old_fragments filter, so stale
/// posting list entries from pruned fragments survive the merge and cause errors
/// when queries try to resolve the old row addresses.
#[tokio::test]
async fn test_fts_index_stale_data_after_merge_insert_compact_optimize() {
    use lance_index::scalar::{FullTextSearchQuery, inverted::InvertedIndexParams};

    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", DataType::Int32, false),
        ArrowField::new("text", DataType::Utf8, true),
    ]));

    // Step 1: Create dataset with 2 rows in separate fragments
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![0, 1])),
            Arc::new(StringArray::from(vec![
                "the quick brown fox",
                "the lazy dog",
            ])),
        ],
    )
    .unwrap();
    let reader = RecordBatchIterator::new(vec![Ok(batch)], schema.clone());
    let mut dataset = Dataset::write(
        reader,
        "memory://test_fts_stale",
        Some(WriteParams {
            max_rows_per_file: 1, // Force 2 fragments
            ..Default::default()
        }),
    )
    .await
    .unwrap();

    // Step 2: Create FTS inverted index on 'text'
    let params = InvertedIndexParams::default();
    dataset
        .create_index(&["text"], IndexType::Inverted, None, &params, true)
        .await
        .unwrap();

    // Sanity check: searching "quick" should return 1 result
    let results = dataset
        .scan()
        .full_text_search(FullTextSearchQuery::new("quick".to_owned()))
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    assert_eq!(results.num_rows(), 1);

    // Step 3: merge_insert with reversed column order (text, id)
    // This triggers the RewriteColumns/DataReplacement path, which prunes the
    // index fragment bitmap for the 'text' column.
    let reversed_schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("text", DataType::Utf8, true),
        ArrowField::new("id", DataType::Int32, false),
    ]));
    let source_batch = RecordBatch::try_new(
        reversed_schema.clone(),
        vec![
            Arc::new(StringArray::from(vec![
                "updated fox text",
                "new entry here",
            ])),
            Arc::new(Int32Array::from(vec![1, 2])),
        ],
    )
    .unwrap();

    let merge_job = MergeInsertBuilder::try_new(Arc::new(dataset.clone()), vec!["id".to_string()])
        .unwrap()
        .when_matched(WhenMatched::UpdateAll)
        .when_not_matched(WhenNotMatched::InsertAll)
        .try_build()
        .unwrap();

    let reader = Box::new(RecordBatchIterator::new(
        vec![Ok(source_batch)],
        reversed_schema.clone(),
    ));
    let (dataset, _stats) = merge_job.execute(reader_to_stream(reader)).await.unwrap();
    let mut dataset = dataset.as_ref().clone();

    // Step 4: compact_files — moves rows to new fragment(s)
    compact_files(&mut dataset, CompactionOptions::default(), None)
        .await
        .unwrap();

    // Step 5: optimize_indices — should rebuild the FTS index without stale data.
    // With the current bug, the inverted index ignores valid_old_fragments and
    // merges stale posting list entries pointing at now-deleted fragments.
    dataset
        .optimize_indices(&OptimizeOptions::default())
        .await
        .unwrap();

    // Step 6: FTS search should not error and should return correct results.
    // "quick" appeared in the original data for id=0 (never updated), so it
    // should still be found.
    let results = dataset
        .scan()
        .full_text_search(FullTextSearchQuery::new("quick".to_owned()))
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    assert_eq!(
        results.num_rows(),
        1,
        "Expected 1 result for 'quick' after optimize, got {}",
        results.num_rows()
    );

    // "lazy" was in the original text for id=1, but id=1 was updated to
    // "updated fox text". The old posting for "lazy" should have been filtered
    // out during the index update.
    let results = dataset
        .scan()
        .full_text_search(FullTextSearchQuery::new("lazy".to_owned()))
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    assert_eq!(
        results.num_rows(),
        0,
        "Expected 0 results for 'lazy' (stale data should be filtered), got {}",
        results.num_rows()
    );

    // "updated" should be found (new text for id=1)
    let results = dataset
        .scan()
        .full_text_search(FullTextSearchQuery::new("updated".to_owned()))
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    assert_eq!(results.num_rows(), 1);

    // "entry" should be found (new row id=2)
    let results = dataset
        .scan()
        .full_text_search(FullTextSearchQuery::new("entry".to_owned()))
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    assert_eq!(results.num_rows(), 1);

    // Step 7: Another merge_insert should NOT error
    let source_batch2 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(StringArray::from(vec!["final text"])),
        ],
    )
    .unwrap();

    let merge_job2 = MergeInsertBuilder::try_new(Arc::new(dataset.clone()), vec!["id".to_string()])
        .unwrap()
        .when_matched(WhenMatched::UpdateAll)
        .when_not_matched(WhenNotMatched::InsertAll)
        .try_build()
        .unwrap();

    let reader2 = Box::new(RecordBatchIterator::new(
        vec![Ok(source_batch2)],
        schema.clone(),
    ));
    let (final_dataset, _) = merge_job2.execute(reader_to_stream(reader2)).await.unwrap();
    final_dataset.validate().await.unwrap();
}

/// Regression test: when rows are updated in-place, the FTS index must
/// invalidate old entries and allow re-indexing incrementally.
///
/// Sequence:
/// 1. Write fragments 1 and 2.
/// 2. Create FTS index covering fragments 1 and 2.
/// 3. Update fragment 1 in-place via merge_insert (DataReplacement path).
///    This removes fragment 1 from the index's fragment_bitmap.
/// 4. Call optimize_indices (append) to create a new index segment covering
///    the updated fragment 1.
/// 5. Call optimize_indices (merge) to merge both segments. The first segment
///    contains the old, invalidated values for fragment 1; the second segment
///    contains the new, valid values. We must keep only the new values.
#[tokio::test]
async fn test_fts_index_incremental_reindex_after_in_place_update() {
    use lance_index::scalar::{FullTextSearchQuery, inverted::InvertedIndexParams};

    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", DataType::Int32, false),
        ArrowField::new("text", DataType::Utf8, true),
    ]));

    // Step 1: Create dataset with 2 rows in separate fragments
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![0, 1])),
            Arc::new(StringArray::from(vec![
                "the quick brown fox",
                "the lazy dog",
            ])),
        ],
    )
    .unwrap();
    let reader = RecordBatchIterator::new(vec![Ok(batch)], schema.clone());
    let mut dataset = Dataset::write(
        reader,
        "memory://test_fts_incremental_reindex",
        Some(WriteParams {
            max_rows_per_file: 1, // Force 2 fragments
            ..Default::default()
        }),
    )
    .await
    .unwrap();

    // Step 2: Create FTS inverted index on 'text'
    let params = InvertedIndexParams::default();
    dataset
        .create_index(&["text"], IndexType::Inverted, None, &params, true)
        .await
        .unwrap();

    // Sanity check: "quick" and "lazy" should each return 1 result
    let results = dataset
        .scan()
        .full_text_search(FullTextSearchQuery::new("quick".to_owned()))
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    assert_eq!(results.num_rows(), 1);
    let results = dataset
        .scan()
        .full_text_search(FullTextSearchQuery::new("lazy".to_owned()))
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    assert_eq!(results.num_rows(), 1);

    // Step 3: merge_insert with reversed column order to trigger
    // RewriteColumns/DataReplacement path, which prunes the index
    // fragment bitmap for the updated fragment.
    // Update id=1 ("the lazy dog" -> "a speedy cat")
    let reversed_schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("text", DataType::Utf8, true),
        ArrowField::new("id", DataType::Int32, false),
    ]));
    let source_batch = RecordBatch::try_new(
        reversed_schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["a speedy cat"])),
            Arc::new(Int32Array::from(vec![1])),
        ],
    )
    .unwrap();

    let merge_job = MergeInsertBuilder::try_new(Arc::new(dataset.clone()), vec!["id".to_string()])
        .unwrap()
        .when_matched(WhenMatched::UpdateAll)
        .when_not_matched(WhenNotMatched::DoNothing)
        .try_build()
        .unwrap();

    let reader = Box::new(RecordBatchIterator::new(
        vec![Ok(source_batch)],
        reversed_schema.clone(),
    ));
    let (dataset, _stats) = merge_job.execute(reader_to_stream(reader)).await.unwrap();
    let mut dataset = dataset.as_ref().clone();

    // Step 4: First optimize_indices (append) — creates a new index segment
    // covering the updated (previously unindexed) fragment.
    dataset
        .optimize_indices(&OptimizeOptions::append())
        .await
        .unwrap();

    // At this point we have two index segments:
    //  - Segment 1: original index (has old data for fragment with id=1)
    //  - Segment 2: new delta index (has new data for the updated fragment)

    // Step 5: Second optimize_indices (merge all) — merges both segments.
    // The merge must discard old invalidated entries from segment 1 for
    // the updated fragment and keep only the new entries from segment 2.
    dataset
        .optimize_indices(&OptimizeOptions::default())
        .await
        .unwrap();

    // Step 6: Verify search correctness after merge.

    // "quick" was in the original data for id=0 (not updated), should still be found.
    let results = dataset
        .scan()
        .full_text_search(FullTextSearchQuery::new("quick".to_owned()))
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    assert_eq!(
        results.num_rows(),
        1,
        "Expected 1 result for 'quick' (id=0 was not updated), got {}",
        results.num_rows()
    );

    // "lazy" was in the old text for id=1 which was updated to "a speedy cat".
    // The old posting for "lazy" must have been filtered out during the merge.
    let results = dataset
        .scan()
        .full_text_search(FullTextSearchQuery::new("lazy".to_owned()))
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    assert_eq!(
        results.num_rows(),
        0,
        "Expected 0 results for 'lazy' (stale data should be filtered), got {}",
        results.num_rows()
    );

    // "speedy" is in the new text for id=1, should be found.
    let results = dataset
        .scan()
        .full_text_search(FullTextSearchQuery::new("speedy".to_owned()))
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    assert_eq!(
        results.num_rows(),
        1,
        "Expected 1 result for 'speedy' (new text for id=1), got {}",
        results.num_rows()
    );

    // "cat" is in the new text for id=1, should be found.
    let results = dataset
        .scan()
        .full_text_search(FullTextSearchQuery::new("cat".to_owned()))
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    assert_eq!(
        results.num_rows(),
        1,
        "Expected 1 result for 'cat' (new text for id=1), got {}",
        results.num_rows()
    );
}

/// Regression test for https://github.com/lance-format/lance/issues/6338
/// Sub-schema merge_insert with binary columns on v2.2 causes data corruption
/// when the binary values are >= 256 bytes.
#[tokio::test]
async fn test_sub_schema_merge_insert_binary_v2_2() {
    use crate::dataset::write::merge_insert::WhenMatched;
    use arrow_array::BinaryArray;

    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", DataType::Int64, false),
        ArrowField::new("a", DataType::Binary, true),
        ArrowField::new("b", DataType::Utf8, true),
    ]));

    let test_uri = TempStrDir::default();

    // Initial write: 2 rows with null binary values
    let initial_batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(arrow_array::Int64Array::from(vec![0, 1])),
            Arc::new(BinaryArray::from(vec![None::<&[u8]>, None])),
            Arc::new(StringArray::from(vec![None::<&str>, None])),
        ],
    )
    .unwrap();

    let write_params = WriteParams {
        data_storage_version: Some(LanceFileVersion::V2_2),
        ..Default::default()
    };
    let batches = RecordBatchIterator::new(vec![initial_batch].into_iter().map(Ok), schema.clone());
    Dataset::write(batches, &test_uri, Some(write_params))
        .await
        .unwrap();

    let sub_schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", DataType::Int64, false),
        ArrowField::new("a", DataType::Binary, true),
    ]));

    // Sub-schema merge_insert for row 0 (binary value >= 256 bytes)
    let data_a: Vec<u8> = (0..256).map(|i| (i % 251) as u8).collect();
    {
        let update_batch = RecordBatch::try_new(
            sub_schema.clone(),
            vec![
                Arc::new(arrow_array::Int64Array::from(vec![0])),
                Arc::new(BinaryArray::from(vec![Some(data_a.as_slice())])),
            ],
        )
        .unwrap();
        let dataset = Dataset::open(&test_uri).await.unwrap();
        let source = Box::new(RecordBatchIterator::new(
            vec![update_batch].into_iter().map(Ok),
            sub_schema.clone(),
        ));
        MergeInsertBuilder::try_new(dataset.into(), vec!["id".into()])
            .unwrap()
            .when_matched(WhenMatched::UpdateAll)
            .try_build()
            .unwrap()
            .execute_reader(source)
            .await
            .unwrap();
    }

    // Read back and verify first merge worked
    let dataset = Dataset::open(&test_uri).await.unwrap();
    let table = dataset
        .scan()
        .project(&["id", "a"])
        .unwrap()
        .try_into_stream()
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let table = concat_batches(&table[0].schema(), &table).unwrap();
    assert_eq!(table.num_rows(), 2);

    // Sub-schema merge_insert for row 1 (binary value >= 256 bytes)
    let data_b: Vec<u8> = (0..256).map(|i| ((i + 100) % 251) as u8).collect();
    {
        let update_batch = RecordBatch::try_new(
            sub_schema.clone(),
            vec![
                Arc::new(arrow_array::Int64Array::from(vec![1])),
                Arc::new(BinaryArray::from(vec![Some(data_b.as_slice())])),
            ],
        )
        .unwrap();
        let dataset = Dataset::open(&test_uri).await.unwrap();
        let source = Box::new(RecordBatchIterator::new(
            vec![update_batch].into_iter().map(Ok),
            sub_schema.clone(),
        ));
        MergeInsertBuilder::try_new(dataset.into(), vec!["id".into()])
            .unwrap()
            .when_matched(WhenMatched::UpdateAll)
            .try_build()
            .unwrap()
            .execute_reader(source)
            .await
            .unwrap();
    }

    // Read back and verify - this is where the bug manifests
    let dataset = Dataset::open(&test_uri).await.unwrap();
    let table = dataset
        .scan()
        .project(&["id", "a"])
        .unwrap()
        .try_into_stream()
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let table = concat_batches(&table[0].schema(), &table).unwrap();
    assert_eq!(table.num_rows(), 2);

    let a_col = table.column_by_name("a").unwrap();
    let binary_arr = a_col.as_any().downcast_ref::<BinaryArray>().unwrap();
    assert_eq!(binary_arr.value(0), data_a.as_slice());
    assert_eq!(binary_arr.value(1), data_b.as_slice());
}

#[tokio::test]
async fn test_fts_unfiltered_after_compaction_returns_remapped_row_ids() {
    // After `compact_files` with `defer_index_remap = true`, queries
    // read the old FTS index but must apply the dataset's
    // FragReuseIndex remap. Otherwise the deferred-row_id path
    // returns pre-compaction row_ids that no longer exist.
    use arrow::datatypes::UInt64Type;

    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", DataType::Int32, false),
        ArrowField::new("text", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![0, 1, 2, 3])),
            Arc::new(StringArray::from(vec![
                "alpha first",
                "alpha second",
                "alpha third",
                "alpha fourth",
            ])),
        ],
    )
    .unwrap();
    let mut dataset = Dataset::write(
        RecordBatchIterator::new(vec![Ok(batch)], schema),
        "memory://test_fts_frag_reuse",
        Some(WriteParams {
            max_rows_per_file: 1, // 4 fragments -> 4 partitions
            ..Default::default()
        }),
    )
    .await
    .unwrap();
    dataset
        .create_index(
            &["text"],
            IndexType::Inverted,
            None,
            &InvertedIndexParams::default(),
            true,
        )
        .await
        .unwrap();
    compact_files(
        &mut dataset,
        CompactionOptions {
            target_rows_per_fragment: 1000,
            defer_index_remap: true,
            ..Default::default()
        },
        None,
    )
    .await
    .unwrap();

    let after = dataset
        .scan()
        .with_row_id()
        .full_text_search(FullTextSearchQuery::new("alpha".to_owned()))
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    assert_eq!(after.num_rows(), 4);
    let returned: Vec<u64> = after[ROW_ID].as_primitive::<UInt64Type>().values().to_vec();
    let live: std::collections::HashSet<u64> =
        dataset.scan().with_row_id().try_into_batch().await.unwrap()[ROW_ID]
            .as_primitive::<UInt64Type>()
            .values()
            .iter()
            .copied()
            .collect();
    for id in &returned {
        assert!(live.contains(id), "stale row_id {id}");
    }
}

// ---------------------------------------------------------------------------
// Multi-base tests: merge insert on datasets whose data lives across multiple
// registered base paths, with and without routing new fragments to bases.
// ---------------------------------------------------------------------------

/// Fixture: primary storage plus two external bases. base1 is a dataset-root
/// style base (files under `{base1}/data/`), base2 is a plain data directory
/// (files directly under `{base2}/`). Initial data: ids 0..6 in two fragments
/// in base1, ids 6..9 in one fragment in primary storage.
struct MultiBaseFixture {
    _tmp: TempDir,
    dataset: Dataset,
    base1_dir: std::path::PathBuf,
    base2_dir: std::path::PathBuf,
}

fn multi_base_schema() -> Arc<ArrowSchema> {
    Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", DataType::Int32, false),
        ArrowField::new("a", DataType::Int32, false),
        ArrowField::new("b", DataType::Utf8, true),
    ]))
}

fn multi_base_batch(ids: &[i32], a_offset: i32, b_prefix: &str) -> RecordBatch {
    RecordBatch::try_new(
        multi_base_schema(),
        vec![
            Arc::new(Int32Array::from(ids.to_vec())),
            Arc::new(Int32Array::from(
                ids.iter().map(|id| id + a_offset).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                ids.iter()
                    .map(|id| format!("{}{}", b_prefix, id))
                    .collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

async fn multi_base_fixture(indexed: bool) -> MultiBaseFixture {
    let tmp = TempDir::default();
    let primary_dir = tmp.std_path().join("primary");
    let base1_dir = tmp.std_path().join("base1");
    let base2_dir = tmp.std_path().join("base2");
    std::fs::create_dir_all(&base1_dir).unwrap();
    std::fs::create_dir_all(&base2_dir).unwrap();
    let primary_uri = format!("file://{}", primary_dir.display());

    let reader = RecordBatchIterator::new(
        vec![Ok(multi_base_batch(&[0, 1, 2, 3, 4, 5], 100, "orig"))],
        multi_base_schema(),
    );
    let dataset = Dataset::write(
        reader,
        &primary_uri,
        Some(WriteParams {
            mode: WriteMode::Create,
            max_rows_per_file: 3,
            initial_bases: Some(vec![
                BasePath {
                    id: 1,
                    name: Some("base1".to_string()),
                    is_dataset_root: true,
                    path: format!("file://{}", base1_dir.display()),
                },
                BasePath {
                    id: 2,
                    name: Some("base2".to_string()),
                    is_dataset_root: false,
                    path: format!("file://{}", base2_dir.display()),
                },
            ]),
            target_bases: Some(vec![1]),
            ..Default::default()
        }),
    )
    .await
    .unwrap();

    let reader = RecordBatchIterator::new(
        vec![Ok(multi_base_batch(&[6, 7, 8], 100, "orig"))],
        multi_base_schema(),
    );
    let mut dataset = Dataset::write(
        reader,
        Arc::new(dataset),
        Some(WriteParams {
            mode: WriteMode::Append,
            ..Default::default()
        }),
    )
    .await
    .unwrap();

    if indexed {
        dataset
            .create_index(
                &["id"],
                IndexType::BTree,
                None,
                &ScalarIndexParams::default(),
                false,
            )
            .await
            .unwrap();
    }

    let fragments = dataset.get_fragments();
    assert_eq!(fragments.len(), 3);
    for fragment in &fragments[..2] {
        for file in &fragment.metadata.files {
            assert_eq!(file.base_id, Some(1));
        }
    }
    for file in &fragments[2].metadata.files {
        assert_eq!(file.base_id, None);
    }

    MultiBaseFixture {
        _tmp: tmp,
        dataset,
        base1_dir,
        base2_dir,
    }
}

/// Collect (id, a, b) rows sorted by id.
async fn collect_multi_base_rows(dataset: &Dataset) -> Vec<(i32, i32, Option<String>)> {
    let mut scan = dataset.scan();
    scan.project(&["id", "a", "b"]).unwrap();
    let batches = scan
        .try_into_stream()
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let mut rows = vec![];
    for batch in batches {
        let ids = batch.column(0).as_primitive::<Int32Type>();
        let a = batch.column(1).as_primitive::<Int32Type>();
        let b = batch.column(2).as_string::<i32>();
        for i in 0..batch.num_rows() {
            let b_val = if b.is_null(i) {
                None
            } else {
                Some(b.value(i).to_string())
            };
            rows.push((ids.value(i), a.value(i), b_val));
        }
    }
    rows.sort_unstable();
    rows
}

fn expected_row(id: i32, a_offset: i32, b_prefix: &str) -> (i32, i32, Option<String>) {
    (id, id + a_offset, Some(format!("{}{}", b_prefix, id)))
}

/// Merge insert against a multi-base table without routing: every path must
/// read fragments from external bases correctly and write all new files to
/// primary storage with no base id. `indexed` toggles the v2 plan path
/// (false) vs the legacy indexed-scan path (true).
#[rstest]
#[tokio::test]
async fn test_merge_insert_on_multi_base_table(#[values(false, true)] indexed: bool) {
    let fixture = multi_base_fixture(indexed).await;
    let dataset = Arc::new(fixture.dataset);

    // Update one row in each existing fragment (two in base1, one in
    // primary), insert two new rows.
    let source = multi_base_batch(&[1, 4, 7, 10, 11], 1000, "new");
    let job = MergeInsertBuilder::try_new(dataset.clone(), vec!["id".to_string()])
        .unwrap()
        .when_matched(WhenMatched::UpdateAll)
        .when_not_matched(WhenNotMatched::InsertAll)
        .try_build()
        .unwrap();
    let reader = Box::new(RecordBatchIterator::new(
        vec![Ok(source)],
        multi_base_schema(),
    ));
    let (dataset, stats) = job.execute(reader_to_stream(reader)).await.unwrap();

    assert_eq!(stats.num_updated_rows, 3);
    assert_eq!(stats.num_inserted_rows, 2);
    assert_eq!(dataset.count_rows(None).await.unwrap(), 11);

    for fragment in dataset.get_fragments() {
        let metadata = &fragment.metadata;
        if metadata.id >= 3 {
            // Fragments written by the merge live in primary storage.
            for file in &metadata.files {
                assert_eq!(file.base_id, None);
            }
        } else {
            // Pre-existing fragments keep their base and get local deletion
            // files for the rewritten rows.
            let expected_base = if metadata.id < 2 { Some(1) } else { None };
            for file in &metadata.files {
                assert_eq!(file.base_id, expected_base);
            }
            let deletion = metadata.deletion_file.as_ref().unwrap();
            assert_eq!(deletion.base_id, None);
        }
    }

    let mut expected = vec![];
    for id in [0, 2, 3, 5, 6, 8] {
        expected.push(expected_row(id, 100, "orig"));
    }
    for id in [1, 4, 7, 10, 11] {
        expected.push(expected_row(id, 1000, "new"));
    }
    expected.sort_unstable();
    assert_eq!(collect_multi_base_rows(&dataset).await, expected);

    // Re-open from scratch to make sure the result is readable without any
    // cached state.
    let dataset = Dataset::open(dataset.uri()).await.unwrap();
    assert_eq!(collect_multi_base_rows(&dataset).await, expected);
}

/// Merge insert routing new fragments to target bases, by id and by name,
/// covering both base layouts (dataset-root and plain data directory).
#[rstest]
#[tokio::test]
async fn test_merge_insert_route_to_target_bases(#[values(false, true)] indexed: bool) {
    let fixture = multi_base_fixture(indexed).await;
    let dataset = Arc::new(fixture.dataset);

    let source = multi_base_batch(&[1, 4, 10, 11], 1000, "new");
    let job = MergeInsertBuilder::try_new(dataset.clone(), vec!["id".to_string()])
        .unwrap()
        .when_matched(WhenMatched::UpdateAll)
        .when_not_matched(WhenNotMatched::InsertAll)
        .target_bases(vec![2])
        .try_build()
        .unwrap();
    let reader = Box::new(RecordBatchIterator::new(
        vec![Ok(source)],
        multi_base_schema(),
    ));
    let (dataset, stats) = job.execute(reader_to_stream(reader)).await.unwrap();
    assert_eq!(stats.num_updated_rows, 2);
    assert_eq!(stats.num_inserted_rows, 2);

    // New fragments land in base2, which is a plain data directory, so the
    // files sit directly under it.
    let mut merge_files = 0;
    for fragment in dataset.get_fragments() {
        let metadata = &fragment.metadata;
        if metadata.id >= 3 {
            for file in &metadata.files {
                assert_eq!(file.base_id, Some(2));
                let on_disk = fixture.base2_dir.join(file.path.as_str());
                assert!(on_disk.exists(), "missing data file {:?}", on_disk);
                merge_files += 1;
            }
        }
    }
    assert!(merge_files > 0);

    let max_fragment_id = dataset.manifest.max_fragment_id().unwrap();

    // A second merge referencing a base by name: base1 is a dataset-root
    // base, so files go under `{base1}/data/`.
    let source = multi_base_batch(&[12, 13], 1000, "new");
    let job = MergeInsertBuilder::try_new(dataset.clone(), vec!["id".to_string()])
        .unwrap()
        .when_matched(WhenMatched::UpdateAll)
        .when_not_matched(WhenNotMatched::InsertAll)
        .target_base_names_or_paths(vec!["base1".to_string()])
        .try_build()
        .unwrap();
    let reader = Box::new(RecordBatchIterator::new(
        vec![Ok(source)],
        multi_base_schema(),
    ));
    let (dataset, stats) = job.execute(reader_to_stream(reader)).await.unwrap();
    assert_eq!(stats.num_inserted_rows, 2);

    let mut merge_files = 0;
    for fragment in dataset.get_fragments() {
        let metadata = &fragment.metadata;
        if metadata.id > max_fragment_id {
            for file in &metadata.files {
                assert_eq!(file.base_id, Some(1));
                let on_disk = fixture.base1_dir.join("data").join(file.path.as_str());
                assert!(on_disk.exists(), "missing data file {:?}", on_disk);
                merge_files += 1;
            }
        }
    }
    assert!(merge_files > 0);

    let mut expected = vec![];
    for id in [0, 2, 3, 5, 6, 7, 8] {
        expected.push(expected_row(id, 100, "orig"));
    }
    for id in [1, 4, 10, 11, 12, 13] {
        expected.push(expected_row(id, 1000, "new"));
    }
    expected.sort_unstable();
    assert_eq!(collect_multi_base_rows(&dataset).await, expected);

    let dataset = Dataset::open(dataset.uri()).await.unwrap();
    assert_eq!(collect_multi_base_rows(&dataset).await, expected);
}

/// Partial-schema merge insert on a multi-base table: column patches for
/// existing fragments stay in primary storage (mixing with data files in
/// external bases within the same fragment) while inserted rows route to the
/// requested base. Requires an index on the join key to reach the in-place
/// update path.
#[tokio::test]
async fn test_merge_insert_partial_schema_multi_base() {
    let fixture = multi_base_fixture(true).await;
    let dataset = Arc::new(fixture.dataset);

    // Update column `a` for all rows of fragment 0 (full column rewrite) and
    // one row of fragment 1 (incremental update), insert ids 10 and 11.
    let partial_schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", DataType::Int32, false),
        ArrowField::new("a", DataType::Int32, false),
    ]));
    let ids = vec![0, 1, 2, 3, 10, 11];
    let source = RecordBatch::try_new(
        partial_schema.clone(),
        vec![
            Arc::new(Int32Array::from(ids.clone())),
            Arc::new(Int32Array::from(
                ids.iter().map(|id| id + 1000).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap();

    let job = MergeInsertBuilder::try_new(dataset.clone(), vec!["id".to_string()])
        .unwrap()
        .when_matched(WhenMatched::UpdateAll)
        .when_not_matched(WhenNotMatched::InsertAll)
        .target_bases(vec![2])
        .try_build()
        .unwrap();
    let reader = Box::new(RecordBatchIterator::new(vec![Ok(source)], partial_schema));
    let (dataset, stats) = job.execute(reader_to_stream(reader)).await.unwrap();
    assert_eq!(stats.num_updated_rows, 4);
    assert_eq!(stats.num_inserted_rows, 2);

    for fragment in dataset.get_fragments() {
        let metadata = &fragment.metadata;
        match metadata.id {
            0 | 1 => {
                // Patched fragments: the original file in base1 plus a column
                // patch written to primary storage.
                assert_eq!(metadata.files.len(), 2);
                assert_eq!(metadata.files[0].base_id, Some(1));
                assert_eq!(metadata.files[1].base_id, None);
            }
            2 => {
                assert_eq!(metadata.files.len(), 1);
                assert_eq!(metadata.files[0].base_id, None);
            }
            _ => {
                // Inserted rows route to base2.
                for file in &metadata.files {
                    assert_eq!(file.base_id, Some(2));
                    let on_disk = fixture.base2_dir.join(file.path.as_str());
                    assert!(on_disk.exists(), "missing data file {:?}", on_disk);
                }
            }
        }
    }

    // Updated rows keep their `b` values, inserted rows have no `b`.
    let mut expected = vec![];
    for id in [4, 5, 6, 7, 8] {
        expected.push(expected_row(id, 100, "orig"));
    }
    for id in [0, 1, 2, 3] {
        expected.push((id, id + 1000, Some(format!("orig{}", id))));
    }
    for id in [10, 11] {
        expected.push((id, id + 1000, None));
    }
    expected.sort_unstable();
    assert_eq!(collect_multi_base_rows(&dataset).await, expected);

    let dataset = Dataset::open(dataset.uri()).await.unwrap();
    assert_eq!(collect_multi_base_rows(&dataset).await, expected);
}

/// Round-robin distribution across multiple target bases within a single
/// merge insert. New data files are cut at `max_rows_per_file` (the write
/// default of 1Mi rows), so inserting more rows than that produces multiple
/// files, which must alternate between the target bases.
#[tokio::test]
async fn test_merge_insert_round_robin_target_bases() {
    let tmp = TempDir::default();
    let primary_dir = tmp.std_path().join("primary");
    let base1_dir = tmp.std_path().join("base1");
    let base2_dir = tmp.std_path().join("base2");
    std::fs::create_dir_all(&base1_dir).unwrap();
    std::fs::create_dir_all(&base2_dir).unwrap();
    let primary_uri = format!("file://{}", primary_dir.display());

    let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
        "id",
        DataType::Int32,
        false,
    )]));
    let reader = RecordBatchIterator::new(
        vec![Ok(RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from_iter_values(0..10))],
        )
        .unwrap())],
        schema.clone(),
    );
    let dataset = Dataset::write(
        reader,
        &primary_uri,
        Some(WriteParams {
            mode: WriteMode::Create,
            initial_bases: Some(vec![
                BasePath {
                    id: 1,
                    name: Some("base1".to_string()),
                    is_dataset_root: true,
                    path: format!("file://{}", base1_dir.display()),
                },
                BasePath {
                    id: 2,
                    name: Some("base2".to_string()),
                    is_dataset_root: false,
                    path: format!("file://{}", base2_dir.display()),
                },
            ]),
            ..Default::default()
        }),
    )
    .await
    .unwrap();

    // 1.2Mi new rows -> two data files -> one per base.
    const BATCH_ROWS: i32 = 100_000;
    let batches: Vec<_> = (0..12)
        .map(|i| {
            let start = 1000 + i * BATCH_ROWS;
            Ok(RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int32Array::from_iter_values(
                    start..start + BATCH_ROWS,
                ))],
            )
            .unwrap())
        })
        .collect();
    let job = MergeInsertBuilder::try_new(Arc::new(dataset), vec!["id".to_string()])
        .unwrap()
        .target_bases(vec![1, 2])
        .try_build()
        .unwrap();
    let reader = Box::new(RecordBatchIterator::new(batches, schema.clone()));
    let (dataset, stats) = job.execute(reader_to_stream(reader)).await.unwrap();
    assert_eq!(stats.num_inserted_rows, 1_200_000);
    assert_eq!(dataset.count_rows(None).await.unwrap(), 1_200_010);

    let merge_file_bases: Vec<_> = dataset
        .get_fragments()
        .iter()
        .filter(|fragment| fragment.metadata.id >= 1)
        .flat_map(|fragment| fragment.metadata.files.iter().map(|file| file.base_id))
        .collect();
    assert_eq!(merge_file_bases, vec![Some(1), Some(2)]);
}

/// Target base validation across build and execution paths.
#[tokio::test]
async fn test_merge_insert_target_bases_validation() {
    let fixture = multi_base_fixture(false).await;
    let dataset = Arc::new(fixture.dataset);

    // Both selectors set fails at build time.
    let err = MergeInsertBuilder::try_new(dataset.clone(), vec!["id".to_string()])
        .unwrap()
        .when_matched(WhenMatched::UpdateAll)
        .when_not_matched(WhenNotMatched::InsertAll)
        .target_bases(vec![1])
        .target_base_names_or_paths(vec!["base2".to_string()])
        .try_build()
        .err()
        .unwrap();
    assert!(
        err.to_string()
            .contains("Cannot specify both target_base_names_or_paths and target_bases"),
        "unexpected error: {}",
        err
    );

    // Unknown base id fails at execution.
    let job = MergeInsertBuilder::try_new(dataset.clone(), vec!["id".to_string()])
        .unwrap()
        .when_matched(WhenMatched::UpdateAll)
        .when_not_matched(WhenNotMatched::InsertAll)
        .target_bases(vec![99])
        .try_build()
        .unwrap();
    let reader = Box::new(RecordBatchIterator::new(
        vec![Ok(multi_base_batch(&[1], 1000, "new"))],
        multi_base_schema(),
    ));
    let err = job.execute(reader_to_stream(reader)).await.unwrap_err();
    assert!(
        err.to_string()
            .contains("Target base ID 99 not found in available bases"),
        "unexpected error: {}",
        err
    );

    // An empty target base list is rejected rather than silently ignored.
    let job = MergeInsertBuilder::try_new(dataset.clone(), vec!["id".to_string()])
        .unwrap()
        .when_matched(WhenMatched::UpdateAll)
        .when_not_matched(WhenNotMatched::InsertAll)
        .target_bases(vec![])
        .try_build()
        .unwrap();
    let reader = Box::new(RecordBatchIterator::new(
        vec![Ok(multi_base_batch(&[1], 1000, "new"))],
        multi_base_schema(),
    ));
    let err = job.execute(reader_to_stream(reader)).await.unwrap_err();
    assert!(
        err.to_string().contains("target_bases cannot be empty"),
        "unexpected error: {}",
        err
    );

    // Unknown base name fails at execution.
    let job = MergeInsertBuilder::try_new(dataset.clone(), vec!["id".to_string()])
        .unwrap()
        .when_matched(WhenMatched::UpdateAll)
        .when_not_matched(WhenNotMatched::InsertAll)
        .target_base_names_or_paths(vec!["nonexistent".to_string()])
        .try_build()
        .unwrap();
    let reader = Box::new(RecordBatchIterator::new(
        vec![Ok(multi_base_batch(&[1], 1000, "new"))],
        multi_base_schema(),
    ));
    let err = job.execute(reader_to_stream(reader)).await.unwrap_err();
    assert!(
        err.to_string()
            .contains("Base reference 'nonexistent' not found in available bases"),
        "unexpected error: {}",
        err
    );

    // Delete-only merges write no data files but still validate target bases.
    let job = MergeInsertBuilder::try_new(dataset.clone(), vec!["id".to_string()])
        .unwrap()
        .when_matched(WhenMatched::Delete)
        .when_not_matched(WhenNotMatched::DoNothing)
        .target_bases(vec![99])
        .try_build()
        .unwrap();
    let reader = Box::new(RecordBatchIterator::new(
        vec![Ok(multi_base_batch(&[1], 1000, "new"))],
        multi_base_schema(),
    ));
    let err = job.execute(reader_to_stream(reader)).await.unwrap_err();
    assert!(
        err.to_string()
            .contains("Target base ID 99 not found in available bases"),
        "unexpected error: {}",
        err
    );

    // Valid target bases on a delete-only merge are a no-op.
    let job = MergeInsertBuilder::try_new(dataset.clone(), vec!["id".to_string()])
        .unwrap()
        .when_matched(WhenMatched::Delete)
        .when_not_matched(WhenNotMatched::DoNothing)
        .target_bases(vec![2])
        .try_build()
        .unwrap();
    let reader = Box::new(RecordBatchIterator::new(
        vec![Ok(multi_base_batch(&[5], 1000, "new"))],
        multi_base_schema(),
    ));
    let (dataset, stats) = job.execute(reader_to_stream(reader)).await.unwrap();
    assert_eq!(stats.num_deleted_rows, 1);
    assert_eq!(dataset.count_rows(None).await.unwrap(), 8);

    // Datasets with no registered bases reject target bases.
    let plain_dir = TempStrDir::default();
    let reader = RecordBatchIterator::new(
        vec![Ok(multi_base_batch(&[0, 1], 100, "orig"))],
        multi_base_schema(),
    );
    let plain_dataset = Dataset::write(reader, plain_dir.as_str(), None)
        .await
        .unwrap();
    let job = MergeInsertBuilder::try_new(Arc::new(plain_dataset), vec!["id".to_string()])
        .unwrap()
        .when_matched(WhenMatched::UpdateAll)
        .when_not_matched(WhenNotMatched::InsertAll)
        .target_bases(vec![1])
        .try_build()
        .unwrap();
    let reader = Box::new(RecordBatchIterator::new(
        vec![Ok(multi_base_batch(&[1], 1000, "new"))],
        multi_base_schema(),
    ));
    let err = job.execute(reader_to_stream(reader)).await.unwrap_err();
    assert!(
        err.to_string()
            .contains("Target base ID 1 not found in available bases"),
        "unexpected error: {}",
        err
    );
}

/// Base id 0 and the dataset URI include primary storage in the merge insert
/// target rotation.
#[tokio::test]
async fn test_merge_insert_target_bases_include_primary() {
    let fixture = multi_base_fixture(false).await;
    let dataset = Arc::new(fixture.dataset);
    let primary_uri = dataset.uri().to_string();

    // Single new file: the first slot (primary) receives it.
    let source = multi_base_batch(&[1, 10], 1000, "new");
    let job = MergeInsertBuilder::try_new(dataset.clone(), vec!["id".to_string()])
        .unwrap()
        .when_matched(WhenMatched::UpdateAll)
        .when_not_matched(WhenNotMatched::InsertAll)
        .target_bases(vec![0, 2])
        .try_build()
        .unwrap();
    let reader = Box::new(RecordBatchIterator::new(
        vec![Ok(source)],
        multi_base_schema(),
    ));
    let (dataset, _) = job.execute(reader_to_stream(reader)).await.unwrap();
    let new_files: Vec<_> = dataset
        .get_fragments()
        .iter()
        .filter(|f| f.metadata.id >= 3)
        .flat_map(|f| f.metadata.files.iter().map(|file| file.base_id))
        .collect();
    assert_eq!(new_files, vec![None]);

    // Flipped order: the first slot is base 2.
    let max_id = dataset.manifest.max_fragment_id().unwrap();
    let source = multi_base_batch(&[11], 1000, "new");
    let job = MergeInsertBuilder::try_new(dataset.clone(), vec!["id".to_string()])
        .unwrap()
        .when_matched(WhenMatched::UpdateAll)
        .when_not_matched(WhenNotMatched::InsertAll)
        .target_bases(vec![2, 0])
        .try_build()
        .unwrap();
    let reader = Box::new(RecordBatchIterator::new(
        vec![Ok(source)],
        multi_base_schema(),
    ));
    let (dataset, _) = job.execute(reader_to_stream(reader)).await.unwrap();
    let new_files: Vec<_> = dataset
        .get_fragments()
        .iter()
        .filter(|f| f.metadata.id > max_id)
        .flat_map(|f| f.metadata.files.iter().map(|file| file.base_id))
        .collect();
    assert_eq!(new_files, vec![Some(2)]);

    // Names variant: the dataset's URI selects primary storage.
    let max_id = dataset.manifest.max_fragment_id().unwrap();
    let source = multi_base_batch(&[12], 1000, "new");
    let job = MergeInsertBuilder::try_new(dataset.clone(), vec!["id".to_string()])
        .unwrap()
        .when_matched(WhenMatched::UpdateAll)
        .when_not_matched(WhenNotMatched::InsertAll)
        .target_base_names_or_paths(vec![primary_uri])
        .try_build()
        .unwrap();
    let reader = Box::new(RecordBatchIterator::new(
        vec![Ok(source)],
        multi_base_schema(),
    ));
    let (dataset, _) = job.execute(reader_to_stream(reader)).await.unwrap();
    let new_files: Vec<_> = dataset
        .get_fragments()
        .iter()
        .filter(|f| f.metadata.id > max_id)
        .flat_map(|f| f.metadata.files.iter().map(|file| file.base_id))
        .collect();
    assert_eq!(new_files, vec![None]);

    let mut expected = vec![];
    for id in [0, 2, 3, 4, 5, 6, 7, 8] {
        expected.push(expected_row(id, 100, "orig"));
    }
    for id in [1, 10, 11, 12] {
        expected.push(expected_row(id, 1000, "new"));
    }
    expected.sort_unstable();
    assert_eq!(collect_multi_base_rows(&dataset).await, expected);

    let dataset = Dataset::open(dataset.uri()).await.unwrap();
    assert_eq!(collect_multi_base_rows(&dataset).await, expected);
}

/// Merge insert attempts discarded by a retryable commit conflict must clean
/// up the data files they routed to target bases; after concurrent merges the
/// bases must contain only files referenced by the final manifest.
#[tokio::test]
async fn test_merge_insert_conflict_retry_cleans_routed_files() {
    let fixture = multi_base_fixture(false).await;
    let dataset = Arc::new(fixture.dataset);
    let concurrency: u32 = 5;

    let barrier = Arc::new(tokio::sync::Barrier::new(concurrency as usize));
    let mut handles = Vec::new();
    for i in 0..concurrency {
        // Every task starts from the same dataset version and updates the same
        // row, so all but one attempt per round hit a retryable conflict.
        let dataset = dataset.clone();
        let barrier = barrier.clone();
        handles.push(tokio::spawn(async move {
            barrier.wait().await;
            let source = multi_base_batch(&[1, 100 + i as i32], 1000 + i as i32, "new");
            let job = MergeInsertBuilder::try_new(dataset, vec!["id".to_string()])
                .unwrap()
                .when_matched(WhenMatched::UpdateAll)
                .when_not_matched(WhenNotMatched::InsertAll)
                .conflict_retries(20)
                .retry_timeout(Duration::from_secs(60))
                .target_bases(vec![1, 2])
                .try_build()
                .unwrap();
            let reader = Box::new(RecordBatchIterator::new(
                vec![Ok(source)],
                multi_base_schema(),
            ));
            job.execute(reader_to_stream(reader)).await.unwrap()
        }));
    }
    let mut total_attempts = 0;
    for handle in handles {
        let (_dataset, stats) = handle.await.unwrap();
        total_attempts += stats.num_attempts;
    }
    assert!(
        total_attempts > concurrency,
        "expected at least one conflicted attempt, got {} attempts",
        total_attempts
    );

    let dataset = Dataset::open(dataset.uri()).await.unwrap();
    assert_eq!(
        dataset.count_rows(None).await.unwrap(),
        9 + concurrency as usize
    );

    let mut referenced: HashSet<(Option<u32>, String)> = HashSet::new();
    for fragment in dataset.get_fragments() {
        for file in &fragment.metadata.files {
            referenced.insert((file.base_id, file.path.to_string()));
        }
    }
    let list_files = |dir: &std::path::Path| -> Vec<String> {
        if !dir.exists() {
            return vec![];
        }
        std::fs::read_dir(dir)
            .unwrap()
            .filter_map(|entry| {
                let path = entry.unwrap().path();
                if path.extension().is_some_and(|ext| ext == "lance") {
                    Some(path.file_name().unwrap().to_string_lossy().to_string())
                } else {
                    None
                }
            })
            .collect()
    };
    for name in list_files(&fixture.base1_dir.join("data")) {
        assert!(
            referenced.contains(&(Some(1), name.clone())),
            "orphaned file in base1: {}",
            name
        );
    }
    for name in list_files(&fixture.base2_dir) {
        assert!(
            referenced.contains(&(Some(2), name.clone())),
            "orphaned file in base2: {}",
            name
        );
    }
}

/// `target_all_bases` on merge insert resolves to every registered base at
/// execution time, with primary storage first when included.
#[tokio::test]
async fn test_merge_insert_target_all_bases() {
    let fixture = multi_base_fixture(false).await;
    let dataset = Arc::new(fixture.dataset);

    // Single new file: with primary included it takes the first slot.
    let source = multi_base_batch(&[20], 1000, "new");
    let job = MergeInsertBuilder::try_new(dataset.clone(), vec!["id".to_string()])
        .unwrap()
        .when_matched(WhenMatched::UpdateAll)
        .when_not_matched(WhenNotMatched::InsertAll)
        .target_all_bases(true)
        .try_build()
        .unwrap();
    let reader = Box::new(RecordBatchIterator::new(
        vec![Ok(source)],
        multi_base_schema(),
    ));
    let (dataset, _) = job.execute(reader_to_stream(reader)).await.unwrap();
    let new_files: Vec<_> = dataset
        .get_fragments()
        .iter()
        .filter(|f| f.metadata.id >= 3)
        .flat_map(|f| f.metadata.files.iter().map(|file| file.base_id))
        .collect();
    assert_eq!(new_files, vec![None]);

    // Without primary the first slot is the lowest registered base id.
    let max_id = dataset.manifest.max_fragment_id().unwrap();
    let source = multi_base_batch(&[21], 1000, "new");
    let job = MergeInsertBuilder::try_new(dataset.clone(), vec!["id".to_string()])
        .unwrap()
        .when_matched(WhenMatched::UpdateAll)
        .when_not_matched(WhenNotMatched::InsertAll)
        .target_all_bases(false)
        .try_build()
        .unwrap();
    let reader = Box::new(RecordBatchIterator::new(
        vec![Ok(source)],
        multi_base_schema(),
    ));
    let (dataset, _) = job.execute(reader_to_stream(reader)).await.unwrap();
    let new_files: Vec<_> = dataset
        .get_fragments()
        .iter()
        .filter(|f| f.metadata.id > max_id)
        .flat_map(|f| f.metadata.files.iter().map(|file| file.base_id))
        .collect();
    assert_eq!(new_files, vec![Some(1)]);

    // Cannot be combined with explicit target bases.
    let err = MergeInsertBuilder::try_new(dataset.clone(), vec!["id".to_string()])
        .unwrap()
        .when_matched(WhenMatched::UpdateAll)
        .when_not_matched(WhenNotMatched::InsertAll)
        .target_bases(vec![1])
        .target_all_bases(true)
        .try_build()
        .err()
        .unwrap();
    assert!(
        err.to_string()
            .contains("Cannot specify target_all_bases together with"),
        "unexpected error: {}",
        err
    );

    let expected_new: Vec<_> = [20, 21]
        .iter()
        .map(|id| expected_row(*id, 1000, "new"))
        .collect();
    let all_rows = collect_multi_base_rows(&dataset).await;
    for row in expected_new {
        assert!(all_rows.contains(&row), "missing row {:?}", row);
    }
}
