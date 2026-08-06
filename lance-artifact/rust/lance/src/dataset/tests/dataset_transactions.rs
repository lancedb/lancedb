use std::collections::HashMap;
// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::sync::Arc;
use std::vec;

use crate::dataset::builder::DatasetBuilder;
use crate::dataset::transaction::{Operation, Transaction};
use crate::dataset::{ManifestWriteConfig, TRANSACTIONS_DIR, write_manifest_file};
use crate::io::ObjectStoreParams;
use crate::session::Session;
use crate::{Dataset, Result};
use lance_table::io::commit::ManifestNamingScheme;

use crate::dataset::write::{CommitBuilder, InsertBuilder, WriteMode, WriteParams};
use crate::index::DatasetIndexExt;
use arrow_array::Array;
use arrow_array::RecordBatch;
use arrow_array::{Int32Array, RecordBatchIterator, StringArray, types::Int32Type};
use arrow_schema::{DataType, Field as ArrowField, Schema as ArrowSchema};
use lance_core::utils::tempfile::{TempDir, TempStrDir};
use lance_datagen::{BatchCount, RowCount, array};

use crate::datafusion::LanceTableProvider;
use datafusion::prelude::SessionContext;
use futures::TryStreamExt;
use lance_datafusion::udf::register_functions;
use object_store::ObjectStoreExt;

#[tokio::test]
async fn test_read_transaction_properties() {
    const LANCE_COMMIT_MESSAGE_KEY: &str = "__lance_commit_message";
    // Create a test dataset
    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", DataType::Int32, false),
        ArrowField::new("value", DataType::Utf8, false),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
        ],
    )
    .unwrap();

    let test_uri = TempStrDir::default();

    // Create WriteParams with properties
    let mut properties1 = HashMap::new();
    properties1.insert(
        LANCE_COMMIT_MESSAGE_KEY.to_string(),
        "First commit".to_string(),
    );
    properties1.insert("custom_prop".to_string(), "custom_value".to_string());

    let write_params = WriteParams {
        transaction_properties: Some(Arc::new(properties1)),
        ..Default::default()
    };

    let dataset = Dataset::write(
        RecordBatchIterator::new([Ok(batch.clone())], schema.clone()),
        &test_uri,
        Some(write_params),
    )
    .await
    .unwrap();

    let transaction = dataset.read_transaction_by_version(1).await.unwrap();
    assert!(transaction.is_some());
    let props = transaction.unwrap().transaction_properties.unwrap();
    assert_eq!(props.len(), 2);
    assert_eq!(
        props.get(LANCE_COMMIT_MESSAGE_KEY),
        Some(&"First commit".to_string())
    );
    assert_eq!(props.get("custom_prop"), Some(&"custom_value".to_string()));

    let mut properties2 = HashMap::new();
    properties2.insert(
        LANCE_COMMIT_MESSAGE_KEY.to_string(),
        "Second commit".to_string(),
    );
    properties2.insert("another_prop".to_string(), "another_value".to_string());

    let write_params = WriteParams {
        transaction_properties: Some(Arc::new(properties2)),
        mode: WriteMode::Append,
        ..Default::default()
    };

    let batch2 = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![4, 5])),
            Arc::new(StringArray::from(vec!["d", "e"])),
        ],
    )
    .unwrap();

    let mut dataset = dataset;
    dataset
        .append(
            RecordBatchIterator::new([Ok(batch2)], schema.clone()),
            Some(write_params),
        )
        .await
        .unwrap();

    let transaction = dataset.read_transaction_by_version(2).await.unwrap();
    assert!(transaction.is_some());
    let props = transaction.unwrap().transaction_properties.unwrap();
    assert_eq!(props.len(), 2);
    assert_eq!(
        props.get(LANCE_COMMIT_MESSAGE_KEY),
        Some(&"Second commit".to_string())
    );
    assert_eq!(
        props.get("another_prop"),
        Some(&"another_value".to_string())
    );

    let transaction = dataset.read_transaction_by_version(1).await.unwrap();
    assert!(transaction.is_some());
    let props = transaction.unwrap().transaction_properties.unwrap();
    assert_eq!(props.len(), 2);
    assert_eq!(
        props.get(LANCE_COMMIT_MESSAGE_KEY),
        Some(&"First commit".to_string())
    );
    assert_eq!(props.get("custom_prop"), Some(&"custom_value".to_string()));

    let result = dataset.read_transaction_by_version(999).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_session_store_registry() {
    // Create a session
    let session = Arc::new(Session::default());
    let registry = session.store_registry();
    assert!(registry.active_stores().is_empty());

    // Create a dataset with memory store
    let write_params = WriteParams {
        session: Some(session.clone()),
        ..Default::default()
    };
    let batch = RecordBatch::try_new(
        Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "a",
            DataType::Int32,
            false,
        )])),
        vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
    )
    .unwrap();
    let dataset = InsertBuilder::new("memory://test")
        .with_params(&write_params)
        .execute(vec![batch.clone()])
        .await
        .unwrap();

    // Assert there is one active store.
    assert_eq!(registry.active_stores().len(), 1);

    // If we create another dataset also in memory, it should re-use the
    // existing store.
    let dataset2 = InsertBuilder::new("memory://test2")
        .with_params(&write_params)
        .execute(vec![batch.clone()])
        .await
        .unwrap();
    assert_eq!(registry.active_stores().len(), 1);
    assert_eq!(
        Arc::as_ptr(&dataset.object_store.as_ref().inner),
        Arc::as_ptr(&dataset2.object_store.as_ref().inner)
    );

    // If we create another with **different parameters**, it should create a new store.
    let write_params2 = WriteParams {
        session: Some(session.clone()),
        store_params: Some(ObjectStoreParams {
            block_size: Some(10_000),
            ..Default::default()
        }),
        ..Default::default()
    };
    let dataset3 = InsertBuilder::new("memory://test3")
        .with_params(&write_params2)
        .execute(vec![batch.clone()])
        .await
        .unwrap();
    assert_eq!(registry.active_stores().len(), 2);
    assert_ne!(
        Arc::as_ptr(&dataset.object_store.as_ref().inner),
        Arc::as_ptr(&dataset3.object_store.as_ref().inner)
    );

    // Remove both datasets
    drop(dataset3);
    assert_eq!(registry.active_stores().len(), 1);
    drop(dataset2);
    drop(dataset);
    assert_eq!(registry.active_stores().len(), 0);
}

#[tokio::test]
async fn test_migrate_v2_manifest_paths() {
    let test_uri = TempStrDir::default();

    let data = lance_datagen::gen_batch()
        .col("key", array::step::<Int32Type>())
        .into_reader_rows(RowCount::from(10), BatchCount::from(1));
    let mut dataset = Dataset::write(
        data,
        &test_uri,
        Some(WriteParams {
            enable_v2_manifest_paths: false,
            ..Default::default()
        }),
    )
    .await
    .unwrap();
    assert_eq!(
        dataset.manifest_location().naming_scheme,
        ManifestNamingScheme::V1
    );

    dataset.migrate_manifest_paths_v2().await.unwrap();
    assert_eq!(
        dataset.manifest_location().naming_scheme,
        ManifestNamingScheme::V2
    );
}

pub(super) async fn execute_sql(
    sql: &str,
    table: String,
    dataset: Arc<Dataset>,
) -> Result<Vec<RecordBatch>> {
    let ctx = SessionContext::new();
    ctx.register_table(
        table,
        Arc::new(LanceTableProvider::new(dataset, false, false)),
    )?;
    register_functions(&ctx);

    let df = ctx.sql(sql).await?;
    Ok(df
        .execute_stream()
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await?)
}

pub(super) fn assert_results<T: Array + PartialEq + 'static>(
    results: Vec<RecordBatch>,
    values: &T,
) {
    assert_eq!(results.len(), 1);
    let results = results.into_iter().next().unwrap();
    assert_eq!(results.num_columns(), 1);

    assert_eq!(
        results.column(0).as_any().downcast_ref::<T>().unwrap(),
        values
    )
}

fn gen_rows() -> impl arrow_array::RecordBatchReader + Send + 'static {
    lance_datagen::gen_batch()
        .col("key", array::step::<Int32Type>())
        .into_reader_rows(RowCount::from(10), BatchCount::from(1))
}

/// Write a dataset with `versions` versions of 10 rows each.
async fn write_versions(uri: &str, versions: usize, enable_v2_manifest_paths: bool) -> Dataset {
    let mut ds = Dataset::write(
        gen_rows(),
        uri,
        Some(WriteParams {
            enable_v2_manifest_paths,
            ..Default::default()
        }),
    )
    .await
    .unwrap();
    for _ in 1..versions {
        ds.append(
            gen_rows(),
            Some(WriteParams {
                mode: WriteMode::Append,
                enable_v2_manifest_paths,
                ..Default::default()
            }),
        )
        .await
        .unwrap();
    }
    ds
}

#[tokio::test]
async fn test_inline_transaction() {
    use arrow_array::{Int32Array, RecordBatch, RecordBatchIterator};
    use arrow_schema::{DataType, Field as ArrowField, Schema as ArrowSchema};
    use std::sync::Arc;

    async fn create_dataset(rows: i32) -> Arc<Dataset> {
        let dir = TempDir::default();
        let uri = dir.path_str();
        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "i",
            DataType::Int32,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from_iter_values(0..rows))],
        )
        .unwrap();
        let ds = Dataset::write(
            RecordBatchIterator::new(vec![Ok(batch)], schema),
            uri.as_str(),
            None,
        )
        .await
        .unwrap();
        Arc::new(ds)
    }

    fn make_tx(read_version: u64) -> Transaction {
        Transaction::new(read_version, Operation::Append { fragments: vec![] }, None)
    }

    async fn delete_external_tx_file(ds: &Dataset) {
        if let Some(tx_file) = ds.manifest.transaction_file.as_ref() {
            let tx_path = ds
                .base
                .clone()
                .join(TRANSACTIONS_DIR)
                .join(tx_file.as_str());
            let _ = ds.object_store.inner.delete(&tx_path).await; // ignore errors
        }
    }

    let session = Arc::new(Session::default());

    // Case 1: Default write_flag=true, delete external transaction file, read should use inline transaction
    let ds = create_dataset(5).await;
    let read_version = ds.manifest().version;
    let tx = make_tx(read_version);
    let ds2 = CommitBuilder::new(ds.clone())
        .execute(tx.clone())
        .await
        .unwrap();
    delete_external_tx_file(&ds2).await;
    let read_tx = ds2.read_transaction().await.unwrap().unwrap();
    assert_eq!(read_tx, tx.clone());

    // Case 2: reading small manifest caches transaction data, eliminating transaction reading IO.
    let read_ds2 = DatasetBuilder::from_uri(ds2.uri.clone())
        .with_session(session.clone())
        .load()
        .await
        .unwrap();
    let stats = read_ds2.object_store.as_ref().io_stats_incremental(); // Reset
    assert!(stats.read_bytes < 64 * 1024);
    // Because the manifest is so small, we should have opportunistically
    // cached the transaction in memory already.
    let inline_tx = read_ds2.read_transaction().await.unwrap().unwrap();
    let stats = read_ds2.object_store.as_ref().io_stats_incremental();
    assert_eq!(stats.read_iops, 0);
    assert_eq!(stats.read_bytes, 0);
    assert_eq!(inline_tx, tx);

    // Case 3: manifest does not contain inline transaction, read should fall back to external transaction file
    let ds = create_dataset(2).await;
    let tx = make_tx(ds.manifest().version);
    let tx_file = crate::io::commit::write_transaction_file(
        ds.object_store.as_ref(),
        &ds.base,
        &lance_table::format::pb::Transaction::from(&tx),
    )
    .await
    .unwrap();
    let (mut manifest, indices) = tx
        .build_manifest(
            Some(ds.manifest.as_ref()),
            ds.load_indices().await.unwrap().as_ref().clone(),
            &tx_file,
            &ManifestWriteConfig::default(),
        )
        .unwrap();
    let location = write_manifest_file(
        ds.object_store.as_ref(),
        ds.commit_handler.as_ref(),
        &ds.base,
        &mut manifest,
        if indices.is_empty() {
            None
        } else {
            Some(indices.clone())
        },
        &ManifestWriteConfig::default(),
        ds.manifest_location.naming_scheme,
        None,
    )
    .await
    .unwrap();
    let ds_new = ds.checkout_version(location.version).await.unwrap();
    assert!(ds_new.manifest.transaction_section.is_none());
    assert!(ds_new.manifest.transaction_file.is_some());
    let read_tx = ds_new.read_transaction().await.unwrap().unwrap();
    assert_eq!(read_tx, tx);

    // The direct read takes the same external-file fallback.
    let version_transaction = ds_new
        .read_version_transaction(location.version)
        .await
        .unwrap();
    assert_eq!(version_transaction.transaction, Some(tx));
}

#[tokio::test]
async fn test_read_version_transaction_does_not_populate_caches() {
    use lance_index::IndexType;
    use lance_index::scalar::ScalarIndexParams;

    let test_uri = TempStrDir::default();
    let mut dataset = write_versions(&test_uri, 1, true).await;
    // Index the table so historical manifests carry an IndexSection that a
    // caching read path would decode.
    dataset
        .create_index(
            &["key"],
            IndexType::BTree,
            None,
            &ScalarIndexParams::default(),
            true,
        )
        .await
        .unwrap(); // version 2
    for _ in 0..18 {
        dataset
            .append(
                gen_rows(),
                Some(WriteParams {
                    mode: WriteMode::Append,
                    ..Default::default()
                }),
            )
            .await
            .unwrap();
    }
    let latest_version = dataset.version().version;
    assert_eq!(latest_version, 20);

    // Fresh session so any cache insertion by the API under test shows as growth.
    let session = Arc::new(Session::default());
    let dataset = DatasetBuilder::from_uri(&test_uri)
        .with_session(session.clone())
        .load()
        .await
        .unwrap();

    let metadata_stats_before = session.metadata_cache_stats().await;
    let index_stats_before = session.index_cache_stats().await;

    let mut actual = Vec::with_capacity(latest_version as usize);
    for version in 1..=latest_version {
        let version_transaction = dataset.read_version_transaction(version).await.unwrap();
        assert_eq!(version_transaction.version, version);
        actual.push(version_transaction);
    }

    let metadata_stats_after = session.metadata_cache_stats().await;
    let index_stats_after = session.index_cache_stats().await;
    assert_eq!(
        metadata_stats_after.num_entries,
        metadata_stats_before.num_entries
    );
    assert_eq!(
        metadata_stats_after.size_bytes,
        metadata_stats_before.size_bytes
    );
    assert_eq!(
        index_stats_after.num_entries,
        index_stats_before.num_entries
    );
    assert_eq!(index_stats_after.size_bytes, index_stats_before.size_bytes);

    // Results match a full checkout.
    for version_transaction in &actual {
        let checked_out = dataset
            .checkout_version(version_transaction.version)
            .await
            .unwrap();
        assert_eq!(
            version_transaction.transaction,
            checked_out.read_transaction().await.unwrap()
        );
        assert_eq!(
            version_transaction.timestamp,
            checked_out.version().timestamp
        );
        assert!(version_transaction.transaction.is_some());
    }

    // A missing (e.g. cleaned up) version errors as DatasetNotFound, matching
    // the historical checkout_version-based contract of the public API.
    let err = dataset.read_version_transaction(9999).await.unwrap_err();
    assert!(
        matches!(err, crate::Error::DatasetNotFound { .. }),
        "expected DatasetNotFound for a missing version, got {err:?}"
    );
}

#[tokio::test]
async fn test_read_transaction_recovers_from_stale_manifest_size() {
    let test_uri = TempStrDir::default();
    let ds = write_versions(&test_uri, 1, true).await;
    let manifest = ds.manifest().clone();
    // Only meaningful for the inline path; a plain write inlines the transaction.
    assert!(manifest.transaction_section.is_some());

    // A size at/under the transaction offset makes the first read_message fail
    // "file size is too small"; only the retry at the true size can recover.
    let mut stale = ds.manifest_location().clone();
    stale.size = Some(1);
    let recovered = ds
        .read_transaction_from_storage(&manifest, &stale)
        .await
        .unwrap();
    assert_eq!(recovered, ds.read_transaction().await.unwrap());
    assert!(recovered.is_some());
}

#[tokio::test]
async fn test_read_version_transaction_v1_manifest_naming() {
    let test_uri = TempStrDir::default();
    let ds = write_versions(&test_uri, 3, false).await;
    assert_eq!(
        ds.manifest_location().naming_scheme,
        ManifestNamingScheme::V1
    );

    for version in 1..=3 {
        let version_transaction = ds.read_version_transaction(version).await.unwrap();
        let checked_out = ds.checkout_version(version).await.unwrap();
        assert_eq!(
            version_transaction.transaction,
            checked_out.read_transaction().await.unwrap()
        );
        assert_eq!(
            version_transaction.timestamp,
            checked_out.version().timestamp
        );
    }
}

#[tokio::test]
async fn test_read_version_transaction_on_branch() {
    let test_uri = TempStrDir::default();
    let mut main_ds = write_versions(&test_uri, 1, true).await;
    let branch_ds = main_ds.create_branch("dev", 1, None).await.unwrap();

    // Commit on the branch.
    let branch_ds = Dataset::write(
        gen_rows(),
        branch_ds.uri(),
        Some(WriteParams {
            mode: WriteMode::Append,
            ..Default::default()
        }),
    )
    .await
    .unwrap();
    assert_eq!(branch_ds.manifest().branch.as_deref(), Some("dev"));

    // Versions resolve against the branch chain and match a full checkout.
    for version in branch_ds.versions().await.unwrap() {
        let version_transaction = branch_ds
            .read_version_transaction(version.version)
            .await
            .unwrap();
        assert_eq!(version_transaction.version, version.version);
        assert_eq!(version_transaction.timestamp, version.timestamp);
        let checked_out = branch_ds.checkout_version(version.version).await.unwrap();
        assert_eq!(checked_out.manifest().branch.as_deref(), Some("dev"));
        assert_eq!(
            version_transaction.transaction,
            checked_out.read_transaction().await.unwrap()
        );
    }

    // The append on the branch is the branch's own transaction.
    let latest = branch_ds.version().version;
    let version_transaction = branch_ds.read_version_transaction(latest).await.unwrap();
    assert!(matches!(
        version_transaction.transaction,
        Some(Transaction {
            operation: Operation::Append { .. },
            ..
        })
    ));
}

#[tokio::test]
async fn test_list_detached_manifests() {
    let test_uri = TempStrDir::default();

    // Create initial dataset
    let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
        "id",
        DataType::Int32,
        false,
    )]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
    )
    .unwrap();

    let dataset = Arc::new(
        Dataset::write(
            RecordBatchIterator::new([Ok(batch.clone())], schema.clone()),
            &test_uri,
            None,
        )
        .await
        .unwrap(),
    );

    // Initially there should be no detached manifests
    let detached = dataset.list_detached_manifests().await.unwrap();
    assert!(detached.is_empty());

    // Create a detached transaction with properties
    let mut properties = HashMap::new();
    properties.insert("detached_key".to_string(), "detached_value".to_string());

    let batch2 = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int32Array::from(vec![4, 5, 6]))],
    )
    .unwrap();

    // Use execute_uncommitted + CommitBuilder with_detached(true)
    let transaction = InsertBuilder::new(dataset.clone())
        .with_params(&WriteParams {
            mode: WriteMode::Append,
            transaction_properties: Some(Arc::new(properties.clone())),
            ..Default::default()
        })
        .execute_uncommitted(vec![batch2])
        .await
        .unwrap();

    CommitBuilder::new(dataset.clone())
        .with_detached(true)
        .execute(transaction)
        .await
        .unwrap();

    // Now there should be one detached manifest
    let detached = dataset.list_detached_manifests().await.unwrap();
    assert_eq!(detached.len(), 1);

    // The detached version should have the high bit set
    let detached_version = detached[0].version;
    assert!(lance_table::format::is_detached_version(detached_version));

    // We should be able to checkout the detached version and read transaction properties
    let checked_out = dataset.checkout_version(detached_version).await.unwrap();
    let tx = checked_out.read_transaction().await.unwrap().unwrap();
    let tx_props = tx.transaction_properties.unwrap();
    assert_eq!(
        tx_props.get("detached_key"),
        Some(&"detached_value".to_string())
    );

    // The detached dataset should have more rows
    assert_eq!(checked_out.count_rows(None).await.unwrap(), 6);

    // Create another detached transaction
    let batch3 = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int32Array::from(vec![7, 8, 9]))],
    )
    .unwrap();

    let mut properties2 = HashMap::new();
    properties2.insert("second_key".to_string(), "second_value".to_string());

    let transaction2 = InsertBuilder::new(dataset.clone())
        .with_params(&WriteParams {
            mode: WriteMode::Append,
            transaction_properties: Some(Arc::new(properties2)),
            ..Default::default()
        })
        .execute_uncommitted(vec![batch3])
        .await
        .unwrap();

    CommitBuilder::new(dataset.clone())
        .with_detached(true)
        .execute(transaction2)
        .await
        .unwrap();

    // Now there should be two detached manifests
    let detached = dataset.list_detached_manifests().await.unwrap();
    assert_eq!(detached.len(), 2);

    // Both should be detached versions
    for loc in &detached {
        assert!(lance_table::format::is_detached_version(loc.version));
    }

    // Regular versions() should not include detached manifests
    let versions = dataset.versions().await.unwrap();
    assert_eq!(versions.len(), 1);
    assert_eq!(versions[0].version, 1);
}

/// Transaction properties large enough to push the transaction over the
/// inline threshold.
fn large_props(key: &str) -> Option<Arc<HashMap<String, String>>> {
    use crate::io::commit::MAX_INLINE_TRANSACTION_BYTES;
    let mut props = HashMap::new();
    props.insert(
        key.to_string(),
        "x".repeat(2 * MAX_INLINE_TRANSACTION_BYTES),
    );
    Some(Arc::new(props))
}

fn spill_test_batch() -> (Arc<ArrowSchema>, RecordBatch) {
    let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
        "i",
        DataType::Int32,
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int32Array::from_iter_values(0..10))],
    )
    .unwrap();
    (schema, batch)
}

/// Load the dataset with a fresh session so assertions hit storage, not caches.
async fn reopen(uri: &str) -> Dataset {
    DatasetBuilder::from_uri(uri)
        .with_session(Arc::new(Session::default()))
        .load()
        .await
        .unwrap()
}

#[tokio::test]
async fn test_large_transaction_spills_to_external_file() {
    use crate::io::commit::MAX_INLINE_TRANSACTION_BYTES;

    let (schema, batch) = spill_test_batch();
    let test_uri = TempStrDir::default();

    // New-dataset commit path: a transaction too large to inline is written
    // only to the external transaction file.
    Dataset::write(
        RecordBatchIterator::new([Ok(batch.clone())], schema.clone()),
        &test_uri,
        Some(WriteParams {
            transaction_properties: large_props("payload"),
            ..Default::default()
        }),
    )
    .await
    .unwrap();
    let ds = reopen(&test_uri).await;
    assert!(ds.manifest.transaction_section.is_none());
    assert!(matches!(ds.manifest.transaction_file.as_deref(), Some(f) if !f.is_empty()));
    let tx = ds.read_transaction().await.unwrap().unwrap();
    assert_eq!(
        tx.transaction_properties
            .unwrap()
            .get("payload")
            .unwrap()
            .len(),
        2 * MAX_INLINE_TRANSACTION_BYTES
    );

    // Normal commit path: a small append is still inlined.
    Dataset::write(
        RecordBatchIterator::new([Ok(batch)], schema),
        &test_uri,
        Some(WriteParams {
            mode: WriteMode::Append,
            ..Default::default()
        }),
    )
    .await
    .unwrap();
    let ds = reopen(&test_uri).await;
    assert!(ds.manifest.transaction_section.is_some());
}

#[tokio::test]
async fn test_spilled_restore_and_deep_clone_read_own_transaction() {
    // Restore and deep clone both rebuild the new manifest from an existing
    // manifest file, inheriting its inline transaction offset and external
    // transaction file name. When the new transaction is too large to inline,
    // readers fall back to exactly those fields, so the stale inherited values
    // must not leak into the new manifest.
    use crate::dataset::transaction::TransactionBuilder;

    let (schema, batch) = spill_test_batch();
    let source_uri = TempStrDir::default();
    Dataset::write(
        RecordBatchIterator::new([Ok(batch.clone())], schema.clone()),
        &source_uri,
        None,
    )
    .await
    .unwrap();
    let ds = Dataset::write(
        RecordBatchIterator::new([Ok(batch)], schema),
        &source_uri,
        Some(WriteParams {
            mode: WriteMode::Append,
            ..Default::default()
        }),
    )
    .await
    .unwrap();
    // The manifests restored from / cloned from below carry an inline
    // transaction offset and their own transaction file.
    assert!(ds.manifest.transaction_section.is_some());
    let source_version = ds.manifest().version;
    let source_ref_path = ds.uri().to_string();
    let source_tx_file = ds.manifest.transaction_file.clone();
    assert!(matches!(source_tx_file.as_deref(), Some(f) if !f.is_empty()));

    // Restore with a spilled transaction: the stale inline offset must be
    // cleared and the transaction read back from the external file.
    let restore_tx = TransactionBuilder::new(source_version, Operation::Restore { version: 1 })
        .transaction_properties(large_props("payload"))
        .build();
    CommitBuilder::new(Arc::new(ds))
        .execute(restore_tx.clone())
        .await
        .unwrap();
    let restored = reopen(&source_uri).await;
    assert!(restored.manifest.transaction_section.is_none());
    assert_eq!(
        restored.read_transaction().await.unwrap().unwrap(),
        restore_tx
    );

    // Deep clone with a spilled transaction: the manifest must reference the
    // clone's own transaction file, not the source's.
    let clone_tx = TransactionBuilder::new(
        source_version,
        Operation::Clone {
            is_shallow: false,
            ref_name: None,
            ref_version: source_version,
            ref_path: source_ref_path,
            branch_name: None,
        },
    )
    .transaction_properties(large_props("payload"))
    .build();
    let clone_uri = TempStrDir::default();
    CommitBuilder::new(&clone_uri)
        .execute(clone_tx.clone())
        .await
        .unwrap();
    let cloned = reopen(&clone_uri).await;
    assert!(cloned.manifest.transaction_section.is_none());
    assert_ne!(cloned.manifest.transaction_file, source_tx_file);
    assert_eq!(cloned.read_transaction().await.unwrap().unwrap(), clone_tx);
}
