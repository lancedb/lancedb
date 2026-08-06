// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::sync::Arc;
use std::vec;

use crate::dataset::InsertBuilder;
use crate::dataset::optimize::{CompactionOptions, compact_files};
use crate::utils::test::copy_test_data_to_tmp;
use crate::{Dataset, Result};
use lance_table::format::IndexMetadata;

use crate::dataset::write::{WriteMode, WriteParams};
use crate::index::DatasetIndexExt;
use arrow::compute::concat_batches;
use arrow_array::RecordBatch;
use arrow_array::{Float32Array, Int64Array, RecordBatchIterator};
use arrow_schema::Schema as ArrowSchema;
use lance_file::version::LanceFileVersion;

use futures::{StreamExt, TryStreamExt};
use rstest::rstest;

pub(super) async fn scan_dataset(uri: &str) -> Result<Vec<RecordBatch>> {
    let results = Dataset::open(uri)
        .await?
        .scan()
        .try_into_stream()
        .await?
        .try_collect::<Vec<_>>()
        .await?;
    Ok(results)
}

#[rstest]
#[tokio::test]
async fn test_v0_7_5_migration() {
    // We migrate to add Fragment.physical_rows and DeletionFile.num_deletions
    // after this version.

    // Copy over table
    let test_dir = copy_test_data_to_tmp("v0.7.5/with_deletions").unwrap();
    let test_uri = test_dir.path_str();

    // Assert num rows, deletions, and physical rows are all correct.
    let dataset = Dataset::open(&test_uri).await.unwrap();
    assert_eq!(dataset.count_rows(None).await.unwrap(), 90);
    assert_eq!(dataset.count_deleted_rows().await.unwrap(), 10);
    let total_physical_rows = futures::stream::iter(dataset.get_fragments())
        .then(|f| async move { f.physical_rows().await })
        .try_fold(0, |acc, x| async move { Ok(acc + x) })
        .await
        .unwrap();
    assert_eq!(total_physical_rows, 100);

    // Append 5 rows
    let schema = Arc::new(ArrowSchema::from(dataset.schema()));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from_iter_values(100..105))],
    )
    .unwrap();
    let batches = RecordBatchIterator::new(vec![Ok(batch)], schema.clone());
    let write_params = WriteParams {
        mode: WriteMode::Append,
        ..Default::default()
    };
    let dataset = Dataset::write(batches, &test_uri, Some(write_params))
        .await
        .unwrap();

    // Assert num rows, deletions, and physical rows are all correct.
    assert_eq!(dataset.count_rows(None).await.unwrap(), 95);
    assert_eq!(dataset.count_deleted_rows().await.unwrap(), 10);
    let total_physical_rows = futures::stream::iter(dataset.get_fragments())
        .then(|f| async move { f.physical_rows().await })
        .try_fold(0, |acc, x| async move { Ok(acc + x) })
        .await
        .unwrap();
    assert_eq!(total_physical_rows, 105);

    dataset.validate().await.unwrap();

    // Scan data and assert it is as expected.
    let expected = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from_iter_values(
            (0..10).chain(20..105),
        ))],
    )
    .unwrap();
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
async fn test_fix_v0_8_0_broken_migration() {
    // The migration from v0.7.5 was broken in 0.8.0. This validates we can
    // automatically fix tables that have this problem.

    // Copy over table
    let test_dir = copy_test_data_to_tmp("v0.8.0/migrated_from_v0.7.5").unwrap();
    let test_uri = test_dir.path_str();
    let test_uri = &test_uri;

    // Assert num rows, deletions, and physical rows are all correct, even
    // though stats are bad.
    let dataset = Dataset::open(test_uri).await.unwrap();
    assert_eq!(dataset.count_rows(None).await.unwrap(), 92);
    assert_eq!(dataset.count_deleted_rows().await.unwrap(), 10);
    let total_physical_rows = futures::stream::iter(dataset.get_fragments())
        .then(|f| async move { f.physical_rows().await })
        .try_fold(0, |acc, x| async move { Ok(acc + x) })
        .await
        .unwrap();
    assert_eq!(total_physical_rows, 102);

    // Append 5 rows to table.
    let schema = Arc::new(ArrowSchema::from(dataset.schema()));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from_iter_values(100..105))],
    )
    .unwrap();
    let batches = RecordBatchIterator::new(vec![Ok(batch)], schema.clone());
    let write_params = WriteParams {
        mode: WriteMode::Append,
        data_storage_version: Some(LanceFileVersion::Legacy),
        ..Default::default()
    };
    let dataset = Dataset::write(batches, test_uri, Some(write_params))
        .await
        .unwrap();

    // Assert statistics are all now correct.
    let physical_rows: Vec<_> = dataset
        .get_fragments()
        .iter()
        .map(|f| f.metadata.physical_rows)
        .collect();
    assert_eq!(physical_rows, vec![Some(100), Some(2), Some(5)]);
    let num_deletions: Vec<_> = dataset
        .get_fragments()
        .iter()
        .map(|f| {
            f.metadata
                .deletion_file
                .as_ref()
                .and_then(|df| df.num_deleted_rows)
        })
        .collect();
    assert_eq!(num_deletions, vec![Some(10), None, None]);
    assert_eq!(dataset.count_rows(None).await.unwrap(), 97);

    // Scan data and assert it is as expected.
    let expected = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from_iter_values(
            (0..10).chain(20..100).chain(0..2).chain(100..105),
        ))],
    )
    .unwrap();
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
async fn test_v0_8_14_invalid_index_fragment_bitmap(
    #[values(LanceFileVersion::Legacy, LanceFileVersion::Stable)]
    data_storage_version: LanceFileVersion,
) {
    // Old versions of lance could create an index whose fragment bitmap was
    // invalid because it did not include fragments that were part of the index
    //
    // We need to make sure we do not rely on the fragment bitmap in these older
    // versions and instead fall back to a slower legacy behavior
    let test_dir = copy_test_data_to_tmp("v0.8.14/corrupt_index").unwrap();
    let test_uri = test_dir.path_str();
    let test_uri = &test_uri;

    let mut dataset = Dataset::open(test_uri).await.unwrap();

    // Uncomment to reproduce the issue.  The below query will panic
    // let mut scan = dataset.scan();
    // let query_vec = Float32Array::from(vec![0_f32; 128]);
    // let scan_fut = scan
    //     .nearest("vector", &query_vec, 2000)
    //     .unwrap()
    //     .nprobes(4)
    //     .prefilter(true)
    //     .try_into_stream()
    //     .await
    //     .unwrap()
    //     .try_collect::<Vec<_>>()
    //     .await
    //     .unwrap();

    // Add some data and recalculate the index, forcing a migration
    let mut scan = dataset.scan();
    let data = scan
        .limit(Some(10), None)
        .unwrap()
        .try_into_stream()
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let schema = data[0].schema();
    let data = RecordBatchIterator::new(data.into_iter().map(arrow::error::Result::Ok), schema);

    let broken_version = dataset.version().version;

    // Any transaction, no matter how simple, should trigger the fragment bitmap to be recalculated
    dataset
        .append(
            data,
            Some(WriteParams {
                data_storage_version: Some(data_storage_version),
                ..Default::default()
            }),
        )
        .await
        .unwrap();

    for idx in dataset.load_indices().await.unwrap().iter() {
        // The corrupt fragment_bitmap does not contain 0 but the
        // restored one should
        assert!(idx.fragment_bitmap.as_ref().unwrap().contains(0));
    }

    let mut dataset = dataset.checkout_version(broken_version).await.unwrap();
    dataset.restore().await.unwrap();

    // Running compaction right away should work (this is verifying compaction
    // is not broken by the potentially malformed fragment bitmaps)
    compact_files(&mut dataset, CompactionOptions::default(), None)
        .await
        .unwrap();

    for idx in dataset.load_indices().await.unwrap().iter() {
        assert!(idx.fragment_bitmap.as_ref().unwrap().contains(0));
    }

    let mut scan = dataset.scan();
    let query_vec = Float32Array::from(vec![0_f32; 128]);
    let batches = scan
        .nearest("vector", &query_vec, 2000)
        .unwrap()
        .nprobes(4)
        .prefilter(true)
        .try_into_stream()
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();

    let row_count = batches.iter().map(|batch| batch.num_rows()).sum::<usize>();
    assert_eq!(row_count, 1900);
}

#[tokio::test]
async fn test_fix_v0_10_5_corrupt_schema() {
    // Schemas could be corrupted by successive calls to `add_columns` and
    // `drop_columns`. We should be able to detect this by checking for
    // duplicate field ids. We should be able to fix this in new commits
    // by dropping unused data files and re-writing the schema.

    // Copy over table
    let test_dir = copy_test_data_to_tmp("v0.10.5/corrupt_schema").unwrap();
    let test_uri = test_dir.path_str();
    let test_uri = &test_uri;

    let mut dataset = Dataset::open(test_uri).await.unwrap();

    let validate_res = dataset.validate().await;
    assert!(validate_res.is_err());

    // Force a migration.
    dataset.delete("false").await.unwrap();
    dataset.validate().await.unwrap();

    let data = dataset.scan().try_into_batch().await.unwrap();
    assert_eq!(
        data["b"]
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values(),
        &[0, 4, 8, 12]
    );
    assert_eq!(
        data["c"]
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values(),
        &[0, 5, 10, 15]
    );
}

#[tokio::test]
async fn test_fix_v0_21_0_corrupt_fragment_bitmap() {
    // In v0.21.0 and earlier, delta indices had a bug where the fragment bitmap
    // could contain fragments that are part of other index deltas.

    // Copy over table
    let test_dir = copy_test_data_to_tmp("v0.21.0/bad_index_fragment_bitmap").unwrap();
    let test_uri = test_dir.path_str();
    let test_uri = &test_uri;

    let mut dataset = Dataset::open(test_uri).await.unwrap();

    let validate_res = dataset.validate().await;
    assert!(validate_res.is_err());
    assert_eq!(dataset.load_indices().await.unwrap()[0].name, "vector_idx");

    // Calling index statistics will force a migration
    let stats = dataset.index_statistics("vector_idx").await.unwrap();
    let stats: serde_json::Value = serde_json::from_str(&stats).unwrap();
    assert_eq!(stats["num_indexed_fragments"], 2);

    dataset.checkout_latest().await.unwrap();
    dataset.validate().await.unwrap();

    let indices = dataset.load_indices().await.unwrap();
    assert_eq!(indices.len(), 2);
    fn get_bitmap(meta: &IndexMetadata) -> Vec<u32> {
        meta.fragment_bitmap.as_ref().unwrap().iter().collect()
    }
    assert_eq!(get_bitmap(&indices[0]), vec![0]);
    assert_eq!(get_bitmap(&indices[1]), vec![1]);
}

#[tokio::test]
async fn test_max_fragment_id_migration() {
    // v0.5.9 and earlier did not store the max fragment id in the manifest.
    // This test ensures that we can read such datasets and migrate them to
    // the latest version, which requires the max fragment id to be present.
    {
        let test_dir = copy_test_data_to_tmp("v0.5.9/no_fragments").unwrap();
        let test_uri = test_dir.path_str();
        let test_uri = &test_uri;
        let dataset = Dataset::open(test_uri).await.unwrap();

        assert_eq!(dataset.manifest.max_fragment_id, None);
        assert_eq!(dataset.manifest.max_fragment_id(), None);
    }

    {
        let test_dir = copy_test_data_to_tmp("v0.5.9/dataset_with_fragments").unwrap();
        let test_uri = test_dir.path_str();
        let test_uri = &test_uri;
        let dataset = Dataset::open(test_uri).await.unwrap();

        assert_eq!(dataset.manifest.max_fragment_id, None);
        assert_eq!(dataset.manifest.max_fragment_id(), Some(2));
    }
}

#[tokio::test]
async fn test_index_without_file_sizes() {
    // Test that we can open indices created before the `files` field was added
    // to IndexMetadata. The index should still work correctly, falling back to
    // HEAD calls for file sizes.

    let test_dir = copy_test_data_to_tmp("pre_file_sizes/index_without_file_sizes").unwrap();
    let test_uri = test_dir.path_str();

    // Open the dataset
    let dataset = Dataset::open(&test_uri).await.unwrap();

    // Verify the index exists and has no file size info
    let indices = dataset.load_indices().await.unwrap();
    assert_eq!(indices.len(), 1);
    let index = &indices[0];
    assert_eq!(index.name, "values_idx");
    assert!(
        index.files.is_none() || index.files.as_ref().unwrap().is_empty(),
        "Index should not have file size info (created with old version)"
    );

    // Verify the index still works - scan with a filter that uses the index
    let batch = dataset
        .scan()
        .filter("values = 'value_42'")
        .unwrap()
        .try_into_batch()
        .await
        .unwrap();
    assert_eq!(batch.num_rows(), 1);

    // Verify describe_indices returns None for total_size_bytes for old indices
    let descriptions = dataset.describe_indices(None).await.unwrap();
    assert_eq!(descriptions.len(), 1);
    assert!(
        descriptions[0].total_size_bytes().is_none(),
        "Old index without file sizes should return None for total_size_bytes"
    );
}

#[tokio::test]
async fn test_index_file_size_migration() {
    // Test that file sizes are migrated when a write operation is performed
    // on a dataset with an index missing file sizes.

    let test_dir = copy_test_data_to_tmp("pre_file_sizes/index_without_file_sizes").unwrap();
    let test_uri = test_dir.path_str();

    // Open the dataset and verify the index has no file sizes
    let dataset = Dataset::open(&test_uri).await.unwrap();
    let indices = dataset.load_indices().await.unwrap();
    assert!(
        indices[0].files.is_none() || indices[0].files.as_ref().unwrap().is_empty(),
        "Index should not have file size info before migration"
    );

    // Perform a write operation (append) to trigger migration
    let batch = arrow_array::record_batch!(
        ("id", Int64, [100, 101]),
        ("values", Utf8, ["value_100", "value_101"])
    )
    .unwrap();
    let dataset = InsertBuilder::new(Arc::new(dataset))
        .with_params(&WriteParams {
            mode: WriteMode::Append,
            ..Default::default()
        })
        .execute(vec![batch])
        .await
        .unwrap();

    // Verify the index now has file sizes after migration
    let indices = dataset.load_indices().await.unwrap();
    let index = &indices[0];
    assert!(
        index.files.is_some() && !index.files.as_ref().unwrap().is_empty(),
        "Index should have file size info after migration"
    );

    // Verify each file has a positive size
    for file in index.files.as_ref().unwrap() {
        assert!(
            file.size_bytes > 0,
            "File {} should have positive size after migration",
            file.path
        );
    }

    // Verify describe_indices now returns total_size_bytes
    let descriptions = dataset.describe_indices(None).await.unwrap();
    assert!(
        descriptions[0].total_size_bytes().is_some(),
        "Index should have total_size_bytes after migration"
    );
    assert!(
        descriptions[0].total_size_bytes().unwrap() > 0,
        "Total size should be positive after migration"
    );
}

/// Regression test for issue #5702: project_by_schema should reorder fields inside List<Struct>.
///
/// This test reads a dataset with:
/// - Fragment 0: List<Struct<a, b, c>> with all fields + "extra" column
/// - Fragment 1: List<Struct<c, b>> with reordered/missing inner struct fields
///
/// Before the fix, reading would fail with:
/// "Incorrect datatype for StructArray field expected List(Struct(...)) got List(Struct(...))"
#[tokio::test]
async fn test_list_struct_field_reorder_issue_5702() {
    let test_dir = copy_test_data_to_tmp("v1.0.1/list_struct_reorder.lance")
        .expect("Failed to copy test data");
    let test_uri = test_dir.path_str();

    let dataset = Dataset::open(&test_uri)
        .await
        .expect("Failed to open dataset");

    // Verify we have 2 fragments
    assert_eq!(dataset.get_fragments().len(), 2);

    // This read would fail before the fix for #5702
    let batches = scan_dataset(&test_uri)
        .await
        .expect("Failed to scan dataset");
    let batch = concat_batches(&batches[0].schema(), batches.iter()).expect("Failed to concat");

    // Verify we got all 4 rows
    assert_eq!(batch.num_rows(), 4);

    // Verify schema has expected columns
    assert_eq!(batch.schema().fields().len(), 3); // id, data, extra
}
