// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::collections::HashMap;
use std::sync::Arc;
use std::vec;

use crate::Dataset;
use crate::dataset::UpdateBuilder;
use crate::dataset::builder::DatasetBuilder;
use crate::dataset::transaction::{Operation, Transaction};
use crate::datatypes::Schema;
use lance_table::io::commit::ManifestNamingScheme;

use crate::dataset::write::{CommitBuilder, WriteMode, WriteParams};
use arrow_array::RecordBatch;
use arrow_array::RecordBatchReader;
use arrow_array::{RecordBatchIterator, UInt32Array, types::Int32Type};
use arrow_schema::{DataType, Field as ArrowField, Schema as ArrowSchema};
use lance_core::utils::tempfile::{TempDir, TempStdDir, TempStrDir};
use lance_datagen::{BatchCount, RowCount, array, gen_batch};
use lance_file::version::LanceFileVersion;
use mock_instant::thread_local::MockClock;

use crate::dataset::refs::branch_contents_path;
use crate::utils::test::copy_test_data_to_tmp;
use futures::TryStreamExt;
use lance_core::Error;
use object_store::path::Path;
use rstest::rstest;
use std::cmp::Ordering;

fn assert_all_manifests_use_scheme(test_dir: &TempStdDir, scheme: ManifestNamingScheme) {
    let entries_names = test_dir
        .join("_versions")
        .read_dir()
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        // Ignore the version hint file, which is not a manifest.
        .filter(|name| !name.starts_with("latest_version_hint"))
        .collect::<Vec<_>>();
    assert!(
        entries_names
            .iter()
            .all(|name| ManifestNamingScheme::detect_scheme(name) == Some(scheme)),
        "Entries: {:?}",
        entries_names
    );
}

#[tokio::test]
async fn test_v2_manifest_path_create() {
    // Can create a dataset, using V2 paths
    let data = lance_datagen::gen_batch()
        .col("key", array::step::<Int32Type>())
        .into_batch_rows(RowCount::from(10))
        .unwrap();
    let test_dir = TempStdDir::default();
    let test_uri = test_dir.to_str().unwrap();
    Dataset::write(
        RecordBatchIterator::new([Ok(data.clone())], data.schema().clone()),
        test_uri,
        Some(WriteParams {
            enable_v2_manifest_paths: true,
            ..Default::default()
        }),
    )
    .await
    .unwrap();

    assert_all_manifests_use_scheme(&test_dir, ManifestNamingScheme::V2);

    // Appending to it will continue to use those paths
    let dataset = Dataset::write(
        RecordBatchIterator::new([Ok(data.clone())], data.schema().clone()),
        test_uri,
        Some(WriteParams {
            mode: WriteMode::Append,
            ..Default::default()
        }),
    )
    .await
    .unwrap();

    assert_all_manifests_use_scheme(&test_dir, ManifestNamingScheme::V2);

    UpdateBuilder::new(Arc::new(dataset))
        .update_where("key = 5")
        .unwrap()
        .set("key", "200")
        .unwrap()
        .build()
        .unwrap()
        .execute()
        .await
        .unwrap();

    assert_all_manifests_use_scheme(&test_dir, ManifestNamingScheme::V2);
}

#[tokio::test]
async fn test_v2_manifest_path_commit() {
    let schema = Schema::try_from(&ArrowSchema::new(vec![ArrowField::new(
        "x",
        DataType::Int32,
        false,
    )]))
    .unwrap();
    let operation = Operation::Overwrite {
        fragments: vec![],
        schema,
        config_upsert_values: None,
        initial_bases: None,
    };
    let test_dir = TempStdDir::default();
    let test_uri = test_dir.to_str().unwrap();
    let dataset = Dataset::commit(
        test_uri,
        operation,
        None,
        None,
        None,
        Default::default(),
        true, // enable_v2_manifest_paths
    )
    .await
    .unwrap();

    assert!(dataset.manifest_location.naming_scheme == ManifestNamingScheme::V2);

    assert_all_manifests_use_scheme(&test_dir, ManifestNamingScheme::V2);
}

#[tokio::test]
async fn test_strict_overwrite() {
    let schema = Schema::try_from(&ArrowSchema::new(vec![ArrowField::new(
        "x",
        DataType::Int32,
        false,
    )]))
    .unwrap();
    let operation = Operation::Overwrite {
        fragments: vec![],
        schema,
        config_upsert_values: None,
        initial_bases: None,
    };
    let test_uri = TempStrDir::default();
    let read_version_0_transaction = Transaction::new(0, operation, None);
    let strict_builder = CommitBuilder::new(&test_uri).with_max_retries(0);
    let unstrict_builder = CommitBuilder::new(&test_uri).with_max_retries(1);
    strict_builder
        .clone()
        .execute(read_version_0_transaction.clone())
        .await
        .expect("Strict overwrite should succeed when writing a new dataset");
    strict_builder
        .clone()
        .execute(read_version_0_transaction.clone())
        .await
        .expect_err("Strict overwrite should fail when committing to a stale version");
    unstrict_builder
        .clone()
        .execute(read_version_0_transaction.clone())
        .await
        .expect("Unstrict overwrite should succeed when committing to a stale version");
}

#[tokio::test]
async fn test_version_id_fast_path() {
    let test_uri = TempStrDir::default();
    let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
        "i",
        DataType::UInt32,
        false,
    )]));

    let data = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(UInt32Array::from_iter_values(0..5))],
    )
    .unwrap();
    let reader = RecordBatchIterator::new(vec![data].into_iter().map(Ok), schema.clone());

    let original = Dataset::write(reader, &test_uri, None).await.unwrap();
    assert_eq!(original.version_id(), 1);
    assert_eq!(original.version_id(), original.version().version);
    assert_eq!(original.latest_version_id().await.unwrap(), 1);

    let data = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(UInt32Array::from_iter_values(5..10))],
    )
    .unwrap();
    let reader = RecordBatchIterator::new(vec![data].into_iter().map(Ok), schema);
    let updated = Dataset::write(
        reader,
        &test_uri,
        Some(WriteParams {
            mode: WriteMode::Append,
            ..Default::default()
        }),
    )
    .await
    .unwrap();
    assert_eq!(updated.version_id(), 2);
    assert_eq!(updated.version_id(), updated.version().version);
    assert_eq!(updated.latest_version_id().await.unwrap(), 2);

    let historical = updated.checkout_version(1).await.unwrap();
    assert_eq!(historical.version_id(), 1);
    assert_eq!(historical.version_id(), historical.version().version);
    assert_eq!(historical.latest_version_id().await.unwrap(), 2);
}

#[rstest]
#[tokio::test]
async fn test_stale_checks_cover_fast_successor_and_latest_version(
    #[values(false, true)] enable_v2_manifest_paths: bool,
) {
    let expected_scheme = if enable_v2_manifest_paths {
        ManifestNamingScheme::V2
    } else {
        ManifestNamingScheme::V1
    };
    let test_uri = TempStrDir::default();
    let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
        "i",
        DataType::UInt32,
        false,
    )]));

    let data = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(UInt32Array::from_iter_values(0..5))],
    )
    .unwrap();
    let reader = RecordBatchIterator::new(vec![data].into_iter().map(Ok), schema.clone());

    let original = Dataset::write(
        reader,
        &test_uri,
        Some(WriteParams {
            enable_v2_manifest_paths,
            ..Default::default()
        }),
    )
    .await
    .unwrap();
    assert_eq!(original.manifest_location().naming_scheme, expected_scheme);
    assert!(!original.is_stale().await.unwrap());
    assert!(!original.has_successor_version().await.unwrap());

    let data = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(UInt32Array::from_iter_values(5..10))],
    )
    .unwrap();
    let reader = RecordBatchIterator::new(vec![data].into_iter().map(Ok), schema);
    let updated = Dataset::write(
        reader,
        &test_uri,
        Some(WriteParams {
            mode: WriteMode::Append,
            enable_v2_manifest_paths,
            ..Default::default()
        }),
    )
    .await
    .unwrap();

    assert!(original.is_stale().await.unwrap());
    assert!(original.has_successor_version().await.unwrap());
    assert_eq!(updated.manifest_location().naming_scheme, expected_scheme);
    assert!(!updated.is_stale().await.unwrap());
    assert!(!updated.has_successor_version().await.unwrap());

    let historical = updated.checkout_version(1).await.unwrap();
    assert_eq!(
        historical.manifest_location().naming_scheme,
        expected_scheme
    );
    assert!(historical.is_stale().await.unwrap());
    assert!(historical.has_successor_version().await.unwrap());
}

#[rstest]
#[tokio::test]
async fn test_restore(
    #[values(LanceFileVersion::Legacy, LanceFileVersion::Stable)]
    data_storage_version: LanceFileVersion,
) {
    // Create a table
    let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
        "i",
        DataType::UInt32,
        false,
    )]));

    let test_uri = TempStrDir::default();

    let data = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(UInt32Array::from_iter_values(0..100))],
    );
    let reader = RecordBatchIterator::new(vec![data.unwrap()].into_iter().map(Ok), schema);
    let mut dataset = Dataset::write(
        reader,
        &test_uri,
        Some(WriteParams {
            data_storage_version: Some(data_storage_version),
            ..Default::default()
        }),
    )
    .await
    .unwrap();
    assert_eq!(dataset.manifest.version, 1);
    let original_manifest = dataset.manifest.clone();

    // Delete some rows
    dataset.delete("i > 50").await.unwrap();
    assert_eq!(dataset.manifest.version, 2);

    // Checkout a previous version
    let mut dataset = dataset.checkout_version(1).await.unwrap();
    assert_eq!(dataset.manifest.version, 1);
    let fragments = dataset.get_fragments();
    assert_eq!(fragments.len(), 1);
    assert_eq!(dataset.count_fragments(), 1);
    assert_eq!(fragments[0].metadata.deletion_file, None);
    assert_eq!(dataset.manifest, original_manifest);

    // Checkout latest and then go back.
    dataset.checkout_latest().await.unwrap();
    assert_eq!(dataset.manifest.version, 2);
    let mut dataset = dataset.checkout_version(1).await.unwrap();

    // Restore to a previous version
    dataset.restore().await.unwrap();
    assert_eq!(dataset.manifest.version, 3);
    assert_eq!(dataset.manifest.fragments, original_manifest.fragments);
    assert_eq!(dataset.manifest.schema, original_manifest.schema);

    // Delete some rows again (make sure we can still write as usual)
    dataset.delete("i > 30").await.unwrap();
    assert_eq!(dataset.manifest.version, 4);
    let fragments = dataset.get_fragments();
    assert_eq!(fragments.len(), 1);
    assert_eq!(dataset.count_fragments(), 1);
    assert!(fragments[0].metadata.deletion_file.is_some());
}

#[rstest]
#[tokio::test]
async fn test_tag(
    #[values(LanceFileVersion::Legacy, LanceFileVersion::Stable)]
    data_storage_version: LanceFileVersion,
) {
    // Create a table
    let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
        "i",
        DataType::UInt32,
        false,
    )]));

    let test_uri = TempStrDir::default();

    let data = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(UInt32Array::from_iter_values(0..100))],
    );
    let reader = RecordBatchIterator::new(vec![data.unwrap()].into_iter().map(Ok), schema);
    let mut dataset = Dataset::write(
        reader,
        &test_uri,
        Some(WriteParams {
            data_storage_version: Some(data_storage_version),
            ..Default::default()
        }),
    )
    .await
    .unwrap();
    assert_eq!(dataset.manifest.version, 1);

    // delete some rows
    dataset.delete("i > 50").await.unwrap();
    assert_eq!(dataset.manifest.version, 2);

    assert_eq!(dataset.tags().list().await.unwrap().len(), 0);

    let bad_tag_creation = dataset.tags().create("tag1", 3).await;
    assert_eq!(
        bad_tag_creation.err().unwrap().to_string(),
        "Version not found error: version main:3 does not exist"
    );

    let bad_tag_deletion = dataset.tags().delete("tag1").await;
    assert_eq!(
        bad_tag_deletion.err().unwrap().to_string(),
        "Ref not found error: tag tag1 does not exist"
    );

    MockClock::set_system_time(std::time::Duration::from_secs(1));
    dataset.tags().create("tag1", 1).await.unwrap();
    let mut tag_map = dataset.tags().list().await.unwrap();
    let tag1_meta = tag_map.remove("tag1").unwrap();
    assert_eq!(tag1_meta.created_at, tag1_meta.updated_at);
    assert!(tag1_meta.created_at.is_some());

    assert_eq!(dataset.tags().list().await.unwrap().len(), 1);

    let another_bad_tag_creation = dataset.tags().create("tag1", 1).await;
    assert_eq!(
        another_bad_tag_creation.err().unwrap().to_string(),
        "Ref conflict error: tag tag1 already exists"
    );

    dataset.tags().delete("tag1").await.unwrap();

    assert_eq!(dataset.tags().list().await.unwrap().len(), 0);

    dataset.tags().create("tag1", 1).await.unwrap();
    dataset.tags().create("tag2", 1).await.unwrap();
    dataset.tags().create("v1.0.0-rc1", 2).await.unwrap();

    let default_order = dataset.tags().list_tags_ordered(None).await.unwrap();
    let default_names: Vec<_> = default_order.iter().map(|t| &t.0).collect();
    assert_eq!(
        default_names,
        ["v1.0.0-rc1", "tag1", "tag2"],
        "Default ordering mismatch"
    );

    let asc_order = dataset
        .tags()
        .list_tags_ordered(Some(Ordering::Less))
        .await
        .unwrap();
    let asc_names: Vec<_> = asc_order.iter().map(|t| &t.0).collect();
    assert_eq!(
        asc_names,
        ["tag1", "tag2", "v1.0.0-rc1"],
        "Ascending ordering mismatch"
    );

    let desc_order = dataset
        .tags()
        .list_tags_ordered(Some(Ordering::Greater))
        .await
        .unwrap();
    let desc_names: Vec<_> = desc_order.iter().map(|t| &t.0).collect();
    assert_eq!(
        desc_names,
        ["v1.0.0-rc1", "tag1", "tag2"],
        "Descending ordering mismatch"
    );

    assert_eq!(dataset.tags().list().await.unwrap().len(), 3);

    let bad_checkout = dataset.checkout_version("tag3").await;
    assert_eq!(
        bad_checkout.err().unwrap().to_string(),
        "Ref not found error: tag tag3 does not exist"
    );

    dataset = dataset.checkout_version("tag1").await.unwrap();
    assert_eq!(dataset.manifest.version, 1);

    let first_ver = DatasetBuilder::from_uri(&test_uri)
        .with_tag("tag1")
        .load()
        .await
        .unwrap();
    assert_eq!(first_ver.version().version, 1);

    // test update tag
    let bad_tag_update = dataset.tags().update("tag3", 1).await;
    assert_eq!(
        bad_tag_update.err().unwrap().to_string(),
        "Ref not found error: tag tag3 does not exist"
    );

    let another_bad_tag_update = dataset.tags().update("tag1", 3).await;
    assert_eq!(
        another_bad_tag_update.err().unwrap().to_string(),
        "Version not found error: version main:3 does not exist"
    );

    let tag1_before_update = dataset.tags().get("tag1").await.unwrap();
    MockClock::set_system_time(std::time::Duration::from_secs(2));
    dataset.tags().update("tag1", 2).await.unwrap();
    let tag1_after_update = dataset.tags().get("tag1").await.unwrap();
    assert_eq!(tag1_after_update.created_at, tag1_before_update.created_at);
    assert!(tag1_after_update.updated_at > tag1_before_update.updated_at);
    dataset = dataset.checkout_version("tag1").await.unwrap();
    assert_eq!(dataset.manifest.version, 2);

    let tag1_before_second_update = dataset.tags().get("tag1").await.unwrap();
    MockClock::set_system_time(std::time::Duration::from_secs(3));
    dataset.tags().update("tag1", 1).await.unwrap();
    let tag1_after_second_update = dataset.tags().get("tag1").await.unwrap();
    assert_eq!(
        tag1_after_second_update.created_at,
        tag1_before_second_update.created_at
    );
    assert!(tag1_after_second_update.updated_at > tag1_before_second_update.updated_at);
    dataset = dataset.checkout_version("tag1").await.unwrap();
    assert_eq!(dataset.manifest.version, 1);
}

#[rstest]
#[tokio::test]
async fn test_fragment_id_zero_not_reused() {
    // Test case 1: Fragment id zero isn't re-used
    // 1. Create a dataset with 1 fragment
    // 2. Delete all rows
    // 3. Append another fragment
    // 4. Assert new fragment has id 1 not 0

    let test_uri = TempStrDir::default();

    let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
        "i",
        DataType::UInt32,
        false,
    )]));

    // Create dataset with 1 fragment
    let data = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(UInt32Array::from_iter_values(0..10))],
    )
    .unwrap();
    let batches = RecordBatchIterator::new(vec![data].into_iter().map(Ok), schema.clone());
    let mut dataset = Dataset::write(batches, &test_uri, None).await.unwrap();

    // Verify we have 1 fragment with id 0
    assert_eq!(dataset.get_fragments().len(), 1);
    assert_eq!(dataset.get_fragments()[0].id(), 0);
    assert_eq!(dataset.manifest.max_fragment_id(), Some(0));

    // Delete all rows
    dataset.delete("true").await.unwrap();

    // After deletion, dataset should be empty but max_fragment_id preserved
    assert_eq!(dataset.get_fragments().len(), 0);
    assert_eq!(dataset.count_rows(None).await.unwrap(), 0);
    assert_eq!(dataset.manifest.max_fragment_id(), Some(0));

    // Append another fragment
    let data = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(UInt32Array::from_iter_values(20..30))],
    )
    .unwrap();
    let batches = RecordBatchIterator::new(vec![data].into_iter().map(Ok), schema.clone());
    let write_params = WriteParams {
        mode: WriteMode::Append,
        ..Default::default()
    };
    let dataset = Dataset::write(batches, &test_uri, Some(write_params))
        .await
        .unwrap();

    // Assert new fragment has id 1, not 0
    assert_eq!(dataset.get_fragments().len(), 1);
    assert_eq!(dataset.get_fragments()[0].id(), 1);
    assert_eq!(dataset.manifest.max_fragment_id(), Some(1));
}

#[rstest]
#[tokio::test]
async fn test_fragment_id_never_reset() {
    // Test case 2: Fragment id is never reset, even if all rows are deleted
    // 1. Create dataset with N fragments
    // 2. Delete all rows
    // 3. Append more fragments
    // 4. Assert new fragments have ids >= N

    let test_uri = TempStrDir::default();

    let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
        "i",
        DataType::UInt32,
        false,
    )]));

    // Create dataset with 3 fragments (N=3)
    let data = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(UInt32Array::from_iter_values(0..30))],
    )
    .unwrap();
    let batches = RecordBatchIterator::new(vec![Ok(data)], schema.clone());
    let write_params = WriteParams {
        max_rows_per_file: 10, // Force multiple fragments
        ..Default::default()
    };
    let mut dataset = Dataset::write(batches, &test_uri, Some(write_params))
        .await
        .unwrap();

    // Verify we have 3 fragments with ids 0, 1, 2
    assert_eq!(dataset.get_fragments().len(), 3);
    assert_eq!(dataset.get_fragments()[0].id(), 0);
    assert_eq!(dataset.get_fragments()[1].id(), 1);
    assert_eq!(dataset.get_fragments()[2].id(), 2);
    assert_eq!(dataset.manifest.max_fragment_id(), Some(2));

    // Delete all rows
    dataset.delete("true").await.unwrap();

    // After deletion, dataset should be empty but max_fragment_id preserved
    assert_eq!(dataset.get_fragments().len(), 0);
    assert_eq!(dataset.count_rows(None).await.unwrap(), 0);
    assert_eq!(dataset.manifest.max_fragment_id(), Some(2));

    // Append more fragments (2 new fragments)
    let data = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(UInt32Array::from_iter_values(100..120))],
    )
    .unwrap();
    let batches = RecordBatchIterator::new(vec![Ok(data)], schema.clone());
    let write_params = WriteParams {
        mode: WriteMode::Append,
        max_rows_per_file: 10, // Force multiple fragments
        ..Default::default()
    };
    let dataset = Dataset::write(batches, &test_uri, Some(write_params))
        .await
        .unwrap();

    // Assert new fragments have ids >= N (3, 4)
    assert_eq!(dataset.get_fragments().len(), 2);
    assert_eq!(dataset.get_fragments()[0].id(), 3);
    assert_eq!(dataset.get_fragments()[1].id(), 4);
    assert_eq!(dataset.manifest.max_fragment_id(), Some(4));
}

#[tokio::test]
async fn test_overwrite_does_not_reuse_fragment_ids() {
    // An overwrite replaces every fragment, but the ids it hands out must still
    // continue from the dataset's high water mark: an id that named one set of
    // rows must never name another.
    let test_dir = TempStrDir::default();
    let write = |mode: WriteMode, rows: u64| {
        let reader = gen_batch()
            .col("i", array::step::<Int32Type>())
            .into_reader_rows(RowCount::from(rows), BatchCount::from(1));
        let params = WriteParams {
            mode,
            max_rows_per_file: 10,
            ..Default::default()
        };
        Dataset::write(reader, test_dir.as_str(), Some(params))
    };
    let fragment_ids = |dataset: &Dataset| {
        dataset
            .get_fragments()
            .iter()
            .map(|f| f.id())
            .collect::<Vec<_>>()
    };

    let dataset = write(WriteMode::Create, 30).await.unwrap();
    assert_eq!(fragment_ids(&dataset), vec![0, 1, 2]);

    let dataset = write(WriteMode::Overwrite, 20).await.unwrap();
    assert_eq!(fragment_ids(&dataset), vec![3, 4]);
    assert_eq!(dataset.manifest.max_fragment_id(), Some(4));

    let dataset = write(WriteMode::Append, 10).await.unwrap();
    assert_eq!(fragment_ids(&dataset), vec![3, 4, 5]);
}

#[rstest]
#[tokio::test]
async fn test_overwrite_rejects_fragment_with_deletion_file(
    // Every form of the operation must be rejected: upserting config alongside
    // the overwrite does not make renumbering the fragment any safer.
    #[values(None, Some(HashMap::from([("key".to_string(), "value".to_string())])))]
    config_upsert_values: Option<HashMap<String, String>>,
) {
    // A deletion file's path embeds the fragment id, so it cannot follow its
    // fragment to the fresh id an overwrite assigns. Such a fragment belongs to
    // the dataset being replaced, so the operation is rejected rather than
    // silently losing the deletions.
    let test_dir = TempStrDir::default();
    let reader = gen_batch()
        .col("i", array::step::<Int32Type>())
        .into_reader_rows(RowCount::from(10), BatchCount::from(1));
    let mut dataset = Dataset::write(reader, test_dir.as_str(), None)
        .await
        .unwrap();
    dataset.delete("i < 3").await.unwrap();

    let fragment = dataset.manifest.fragments[0].clone();
    assert!(fragment.deletion_file.is_some());
    let schema = dataset.schema().clone();
    let read_version = dataset.manifest.version;

    let err = CommitBuilder::new(Arc::new(dataset))
        .execute(Transaction::new(
            read_version,
            Operation::Overwrite {
                fragments: vec![fragment],
                schema,
                config_upsert_values,
                initial_bases: None,
            },
            None,
        ))
        .await
        .unwrap_err();
    assert!(
        matches!(err, Error::InvalidInput { .. }),
        "expected invalid input, got {err:?}"
    );
    assert!(
        err.to_string().contains("must be newly written"),
        "unexpected message: {err}"
    );
}

#[tokio::test]
async fn test_commit_rejects_duplicate_fragment_ids() {
    // Append honors an id a fragment arrives with, so a caller can still hand in
    // one that an existing fragment already uses. Committing that would leave two
    // fragments sharing everything keyed by the id.
    let test_dir = TempStrDir::default();
    let write = |mode: WriteMode| {
        let reader = gen_batch()
            .col("i", array::step::<Int32Type>())
            .into_reader_rows(RowCount::from(10), BatchCount::from(1));
        let params = WriteParams {
            mode,
            max_rows_per_file: 5,
            ..Default::default()
        };
        Dataset::write(reader, test_dir.as_str(), Some(params))
    };
    let dataset = write(WriteMode::Create).await.unwrap();
    let existing = dataset.manifest.fragments[1].clone();
    assert_eq!(existing.id, 1);

    let err = CommitBuilder::new(Arc::new(dataset))
        .execute(Transaction::new(
            1,
            Operation::Append {
                fragments: vec![existing],
            },
            None,
        ))
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("two fragments with id 1"),
        "unexpected message: {err}"
    );
}

#[tokio::test]
async fn test_commit_on_dataset_with_mixed_file_versions() {
    // A v0.16 dataset that has both v1 and v2 files also has two fragments with
    // id 1, because the id allocation of that era could hand out an id a caller
    // had already supplied. The mixture is the more actionable diagnosis, so the
    // duplicate check must not preempt it.
    let test_dir = copy_test_data_to_tmp("v0.16.0/wrong_data_version_no_fix.lance").unwrap();
    let mut dataset = Dataset::open(&test_dir.path_str()).await.unwrap();
    let ids = dataset
        .manifest
        .fragments
        .iter()
        .map(|f| f.id)
        .collect::<Vec<_>>();
    assert_eq!(ids, vec![0, 1, 1, 2]);

    let err = dataset.delete("false").await.unwrap_err();
    assert!(
        err.to_string()
            .contains("The dataset contains a mixture of file versions"),
        "unexpected message: {err}"
    );
}

/// create_branch and shallow_clone must read the SOURCE ref's chain, not the
/// receiver's. Both chains get a version 2 with diverged row counts so a clone
/// that wrongly resolves the version under the receiver succeeds silently with
/// the wrong data.
#[tokio::test]
async fn test_create_branch_and_shallow_clone_from_other_branch() {
    let tempdir = TempDir::default();
    let test_uri = tempdir.path_str();

    let gen_rows = |start: i32, rows: u64| {
        gen_batch()
            .col("id", array::step_custom::<Int32Type>(start, 1))
            .into_reader_rows(RowCount::from(rows), BatchCount::from(1))
    };
    let write = |uri: String, start: i32, rows: u64, mode: WriteMode| async move {
        Dataset::write(
            gen_rows(start, rows),
            uri.as_str(),
            Some(WriteParams {
                mode,
                ..Default::default()
            }),
        )
        .await
        .unwrap()
    };

    // main v1: 50 rows.
    let mut main_ds = write(test_uri.clone(), 0, 50, WriteMode::Create).await;
    // dev: forked at v1, appended 30 rows -> dev v2 has 80 rows.
    let dev_ds = main_ds.create_branch("dev", 1, None).await.unwrap();
    write(dev_ds.uri().to_string(), 1000, 30, WriteMode::Append).await;
    // Diverge main to the same version number with a different row count.
    let mut main_ds = write(test_uri.clone(), 5000, 10, WriteMode::Append).await; // main v2: 60 rows

    // Cross-source create_branch: receiver is main, source is dev.
    let child_ds = main_ds
        .create_branch("child", ("dev", 2), None)
        .await
        .unwrap();
    assert_eq!(
        child_ds.count_rows(None).await.unwrap(),
        80,
        "child must clone dev@2, not main@2"
    );

    // Cross-source shallow_clone: same rule.
    let clone_uri = format!("{}_clone", test_uri);
    let cloned_ds = main_ds
        .shallow_clone(&clone_uri, ("dev", 2), None)
        .await
        .unwrap();
    assert_eq!(
        cloned_ds.count_rows(None).await.unwrap(),
        80,
        "shallow clone must read dev@2, not main@2"
    );
}

#[tokio::test]
async fn test_branch() {
    let tempdir = TempDir::default();
    let test_uri = tempdir.path_str();
    let data_storage_version = LanceFileVersion::Stable;

    // Generate consistent test data batches
    let generate_data = |prefix: &str, start_id: i32, row_count: u64| {
        gen_batch()
            .col("id", array::step_custom::<Int32Type>(start_id, 1))
            .col("value", array::fill_utf8(format!("{prefix}_data")))
            .into_reader_rows(RowCount::from(row_count), BatchCount::from(1))
    };

    // Reusable dataset writer with configurable mode
    async fn write_dataset(
        uri: &str,
        data_reader: impl RecordBatchReader + Send + 'static,
        mode: WriteMode,
        version: LanceFileVersion,
    ) -> Dataset {
        let params = WriteParams {
            max_rows_per_file: 100,
            max_rows_per_group: 20,
            data_storage_version: Some(version),
            mode,
            ..Default::default()
        };
        Dataset::write(data_reader, uri, Some(params))
            .await
            .unwrap()
    }

    // Unified dataset scanning and row counting
    async fn collect_rows(dataset: &Dataset) -> (usize, Vec<RecordBatch>) {
        let batches = dataset
            .scan()
            .try_into_stream()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        (batches.iter().map(|b| b.num_rows()).sum(), batches)
    }

    // Phase 1: Create empty dataset, write data batch 1, create branch1 based on version_number, write data batch 2
    let mut dataset = write_dataset(
        &test_uri,
        generate_data("batch1", 0, 50),
        WriteMode::Create,
        data_storage_version,
    )
    .await;

    let original_version = dataset.version().version;
    assert_eq!(original_version, 1);

    // Create branch1 on the latest version and write data batch 2
    let mut branch1_dataset = dataset
        .create_branch("branch1", original_version, None)
        .await
        .unwrap();
    assert_eq!(branch1_dataset.uri, format!("{}/tree/branch1", test_uri));

    branch1_dataset = write_dataset(
        branch1_dataset.uri(),
        generate_data("batch2", 50, 30),
        WriteMode::Append,
        data_storage_version,
    )
    .await;

    // Phase 2: Create branch2 based on branch1's latest version_number, write data batch 3
    let mut branch2_dataset = branch1_dataset
        .create_branch(
            "dev/branch2",
            ("branch1", branch1_dataset.version().version),
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        branch2_dataset.uri,
        format!("{}/tree/dev/branch2", test_uri)
    );

    branch2_dataset = write_dataset(
        branch2_dataset.uri(),
        generate_data("batch3", 80, 20),
        WriteMode::Append,
        data_storage_version,
    )
    .await;

    // Phase 3: Create a tag on branch2, the actual tag content is under root dataset
    // create branch3 based on that tag, write data batch 4
    branch2_dataset
        .tags()
        .create("tag1", ("dev/branch2", branch2_dataset.version().version))
        .await
        .unwrap();

    let mut branch3_dataset = branch2_dataset
        .create_branch("feature/nathan/branch3", "tag1", None)
        .await
        .unwrap();
    assert_eq!(
        branch3_dataset.uri,
        format!("{}/tree/feature/nathan/branch3", test_uri)
    );

    branch3_dataset = write_dataset(
        branch3_dataset.uri(),
        generate_data("batch4", 100, 25),
        WriteMode::Append,
        data_storage_version,
    )
    .await;

    // Verify data correctness and independence of each branch
    // Main branch only has data 1 (50 rows)
    let main_dataset = Dataset::open(&test_uri).await.unwrap();
    let (main_rows, _) = collect_rows(&main_dataset).await;
    assert_eq!(main_rows, 50); // only batch1
    assert_eq!(main_dataset.version().version, 1);

    // branch1 has data 1 + 2 (80 rows)
    let updated_branch1 = Dataset::open(branch1_dataset.uri()).await.unwrap();
    let (branch1_rows, _) = collect_rows(&updated_branch1).await;
    assert_eq!(branch1_rows, 80); // batch1+batch2
    assert_eq!(updated_branch1.version().version, 2);

    // branch2 has data 1 + 2 + 3 (100 rows)
    let updated_branch2 = Dataset::open(branch2_dataset.uri()).await.unwrap();
    let (branch2_rows, _) = collect_rows(&updated_branch2).await;
    assert_eq!(branch2_rows, 100); // batch1+batch2+batch3
    assert_eq!(updated_branch2.version().version, 3);

    // branch3 has data 1 + 2 + 3 + 4 (125 rows)
    let updated_branch3 = Dataset::open(branch3_dataset.uri()).await.unwrap();
    let (branch3_rows, _) = collect_rows(&updated_branch3).await;
    assert_eq!(branch3_rows, 125); // batch1+batch2+batch3+batch4
    assert_eq!(updated_branch3.version().version, 4);

    // Use list_branches to get branch list and verify each field of branch_content
    let branches = dataset.list_branches().await.unwrap();
    assert_eq!(branches.len(), 3);
    assert!(branches.contains_key("branch1"));
    assert!(branches.contains_key("dev/branch2"));
    assert!(branches.contains_key("feature/nathan/branch3"));

    // Verify branch1 content
    let branch1_content = branches.get("branch1").unwrap();
    assert_eq!(branch1_content.parent_branch, None); // Created based on main branch
    assert_eq!(branch1_content.parent_version, 1);
    assert!(branch1_content.create_at > 0);
    assert!(branch1_content.manifest_size > 0);

    // Verify branch2 content
    let branch2_content = branches.get("dev/branch2").unwrap();
    assert_eq!(branch2_content.parent_branch.as_deref().unwrap(), "branch1");
    assert_eq!(branch2_content.parent_version, 2);
    assert!(branch2_content.create_at > 0);
    assert!(branch2_content.manifest_size > 0);
    assert!(branch2_content.create_at >= branch1_content.create_at);

    // Verify branch3 content
    let branch3_content = branches.get("feature/nathan/branch3").unwrap();
    // Created based on tag pointed to branch2
    assert_eq!(
        branch3_content.parent_branch.as_deref().unwrap(),
        "dev/branch2"
    );
    assert_eq!(branch3_content.parent_version, 3);
    assert!(branch3_content.create_at > 0);
    assert!(branch3_content.manifest_size > 0);
    assert!(branch3_content.create_at >= branch2_content.create_at);

    // Verify checkout_branch
    let checkout_branch1 = main_dataset.checkout_branch("branch1").await.unwrap();
    let checkout_branch2 = checkout_branch1
        .checkout_branch("dev/branch2")
        .await
        .unwrap();
    let checkout_branch2_tag = checkout_branch1.checkout_version("tag1").await.unwrap();
    let checkout_branch3 = checkout_branch2_tag
        .checkout_branch("feature/nathan/branch3")
        .await
        .unwrap();
    let checkout_branch3_at_version3 = checkout_branch2
        .checkout_version(("feature/nathan/branch3", 3))
        .await
        .unwrap();
    assert_eq!(checkout_branch3.version().version, 4);
    assert_eq!(checkout_branch3_at_version3.version().version, 3);
    assert_eq!(checkout_branch2.version().version, 3);
    assert_eq!(checkout_branch2_tag.version().version, 3);
    assert_eq!(checkout_branch1.version().version, 2);
    assert_eq!(checkout_branch3.count_rows(None).await.unwrap(), 125);
    assert_eq!(
        checkout_branch3_at_version3.count_rows(None).await.unwrap(),
        100
    );
    assert_eq!(checkout_branch2.count_rows(None).await.unwrap(), 100);
    assert_eq!(checkout_branch2_tag.count_rows(None).await.unwrap(), 100);
    assert_eq!(checkout_branch1.count_rows(None).await.unwrap(), 80);
    assert_eq!(
        checkout_branch3.manifest.branch.as_deref().unwrap(),
        "feature/nathan/branch3"
    );
    assert_eq!(
        checkout_branch3_at_version3
            .manifest
            .branch
            .as_deref()
            .unwrap(),
        "feature/nathan/branch3"
    );
    assert_eq!(
        checkout_branch2.manifest.branch.as_deref().unwrap(),
        "dev/branch2"
    );
    assert_eq!(
        checkout_branch2_tag.manifest.branch.as_deref().unwrap(),
        "dev/branch2"
    );
    assert_eq!(
        checkout_branch1.manifest.branch.as_deref().unwrap(),
        "branch1"
    );

    // Opening at a branch-pointing tag through the builder must check out the
    // tag's branch chain, not main's chain at the tag's version number.
    let tag_open = DatasetBuilder::from_uri(&test_uri)
        .with_tag("tag1")
        .load()
        .await
        .unwrap();
    assert_eq!(tag_open.manifest.branch.as_deref(), Some("dev/branch2"));
    assert_eq!(tag_open.version().version, 3);
    assert_eq!(tag_open.count_rows(None).await.unwrap(), 100);

    // Malformed branch names are rejected at the boundary
    for bad_name in ["", "branch1/"] {
        let err = main_dataset
            .checkout_version((Some(bad_name), None::<u64>))
            .await
            .unwrap_err();
        assert!(
            matches!(err, Error::InvalidRef { .. }),
            "checkout of {:?} must be rejected as InvalidRef, got: {}",
            bad_name,
            err
        );
        let err = DatasetBuilder::from_uri(&test_uri)
            .with_branch(bad_name, None)
            .load()
            .await
            .unwrap_err();
        assert!(
            matches!(err, Error::InvalidRef { .. }),
            "open of {:?} must be rejected as InvalidRef, got: {}",
            bad_name,
            err
        );
    }

    // "main" stays a valid spelling of the main branch on checkout; the JNI
    // bindings construct Ref::Version(Some("main"), _) directly.
    let main_by_name = checkout_branch1.checkout_branch("main").await.unwrap();
    assert_eq!(main_by_name.manifest.branch, None);
    assert_eq!(main_by_name.version().version, 1);
    let main_by_ref = checkout_branch1
        .checkout_version(crate::dataset::refs::Ref::Version(
            Some("main".to_string()),
            None,
        ))
        .await
        .unwrap();
    assert_eq!(main_by_ref.manifest.branch, None);

    // A checkout whose resolved manifest is not on the requested branch must
    // error loudly instead of handing back another branch's data: stage main's
    // manifest under a branch path that was never created, so resolution finds
    // a manifest belonging to main.
    use object_store::ObjectStoreExt as _;
    let staged_manifest = main_dataset.manifest_location().path.clone();
    let staged_copy = Path::parse(format!(
        "{}/tree/ghost/_versions/{}",
        test_uri,
        staged_manifest.filename().unwrap()
    ))
    .unwrap();
    main_dataset
        .object_store
        .inner
        .copy(&staged_manifest, &staged_copy)
        .await
        .unwrap();
    let err = main_dataset.checkout_branch("ghost").await.unwrap_err();
    assert!(
        err.to_string().contains("resolved a manifest belonging to"),
        "expected the branch-mismatch guardrail, got: {}",
        err
    );
    main_dataset
        .object_store
        .remove_dir_all(Path::parse(format!("{}/tree/ghost", test_uri)).unwrap())
        .await
        .unwrap();

    let mut dataset = main_dataset;
    // Finally delete all branches
    assert!(matches!(
        dataset.delete_branch("branch1").await,
        Err(Error::RefConflict { message: _ })
    ));
    // Test deleting zombie branch
    let root_location = dataset.refs.root().unwrap();
    let branch_file = branch_contents_path(&root_location.path, "feature/nathan/branch3");
    dataset.object_store.delete(&branch_file).await.unwrap();
    // Now "feature/nathan/branch3" is a zombie branch
    // Use delete_branch to verify if the directory is cleaned up
    dataset
        .force_delete_branch("feature/nathan/branch3")
        .await
        .unwrap();
    let cleaned_path = Path::parse(format!("{}/tree/feature", test_uri)).unwrap();
    assert!(!dataset.object_store.exists(&cleaned_path).await.unwrap());

    dataset.delete_branch("dev/branch2").await.unwrap();
    dataset.delete_branch("branch1").await.unwrap();

    // Verify list_branches is empty
    let branches_after_delete = dataset.list_branches().await.unwrap();
    assert!(branches_after_delete.is_empty());

    // Verify branch directories are all deleted cleanly
    let test_path = tempdir.obj_path();
    let branches = dataset
        .object_store
        .read_dir(test_path.clone().join("tree"))
        .await
        .unwrap();
    assert!(branches.is_empty());
}
