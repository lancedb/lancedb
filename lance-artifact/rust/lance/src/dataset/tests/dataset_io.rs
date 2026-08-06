// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::collections::HashMap;
use std::sync::Arc;
use std::vec;

use super::dataset_common::{create_file, require_send};

use crate::dataset::WriteDestination;
use crate::dataset::WriteMode::Overwrite;
use crate::dataset::builder::DatasetBuilder;
use crate::dataset::{ManifestWriteConfig, write_manifest_file};
use crate::session::Session;
use crate::session::caches::ManifestKey;
use crate::{Dataset, Error, Result};
use lance_table::format::DataStorageFormat;

use crate::dataset::write::{CommitBuilder, InsertBuilder, WriteMode, WriteParams};
use arrow::array::as_struct_array;
use arrow::compute::concat_batches;
use arrow_array::RecordBatch;
use arrow_array::RecordBatchReader;
use arrow_array::{Array, FixedSizeListArray, Int16Array, Int16DictionaryArray, StructArray};
use arrow_array::{
    ArrayRef, BooleanArray, Int8Array, Int8DictionaryArray, Int32Array, Int64Array,
    RecordBatchIterator, StringArray,
    cast::as_string_array,
    types::{Float32Type, Int32Type},
};
use arrow_ord::sort::sort_to_indices;
use arrow_schema::{DataType, Field as ArrowField, Schema as ArrowSchema};
use lance_arrow::bfloat16::{self, BFLOAT16_EXT_NAME};
use lance_arrow::{ARROW_EXT_META_KEY, ARROW_EXT_NAME_KEY};
use lance_core::utils::tempfile::{TempStdDir, TempStrDir};
use lance_datagen::{BatchCount, RowCount, array, gen_batch};
use lance_file::{
    version::{ConcreteFileVersion, LanceFileVersion},
    writer::FileWriterOptions,
};
use lance_io::assert_io_eq;
use lance_table::feature_flags;
use lance_table::format::BasePath;
use object_store::ObjectStoreExt;

use crate::index::DatasetIndexExt;
use futures::TryStreamExt;
use lance_index::IndexType;
use lance_index::scalar::ScalarIndexParams;
use lance_io::object_store::{
    ObjectStore, ObjectStoreParams, StorageOptionsAccessor, WrappingObjectStore,
};
use lance_io::utils::tracking_store::IOTracker;
use lance_table::io::manifest::read_manifest;
use object_store::path::Path;
use rstest::rstest;

fn file_object_store_uri(path: &std::path::Path) -> String {
    let path = path.to_str().unwrap().replace('\\', "/");
    let path_prefix = if path.starts_with('/') { "" } else { "/" };
    format!("file-object-store://{path_prefix}{path}")
}

#[tokio::test]
async fn test_truncate_table() {
    let tmpdir = tempfile::tempdir().unwrap();
    let path = tmpdir.path();
    create_file(path, WriteMode::Create, LanceFileVersion::V2_2).await;

    let uri = path.to_str().unwrap();
    let mut ds = Dataset::open(uri).await.unwrap();
    let rows_before = ds.count_rows(None).await.unwrap();
    assert!(rows_before > 0);

    ds.truncate_table().await.unwrap();

    let rows_after = ds.count_rows(None).await.unwrap();
    assert_eq!(rows_after, 0);
    assert_eq!(ds.count_fragments(), 0);

    let expected_schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("i", DataType::Int32, false),
        ArrowField::new(
            "dict",
            DataType::Dictionary(Box::new(DataType::UInt16), Box::new(DataType::Utf8)),
            false,
        ),
    ]));
    let actual_schema = ArrowSchema::from(ds.schema());
    assert_eq!(&actual_schema, expected_schema.as_ref());
}

async fn drain_scan(dataset: &Dataset) {
    dataset
        .scan()
        .try_into_stream()
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
}

#[tokio::test]
async fn test_with_object_store_clone_preserves_shared_state_and_overrides_store_binding() {
    let test_dir = TempStdDir::default();
    create_file(&test_dir, WriteMode::Create, LanceFileVersion::Stable).await;
    let uri = test_dir.to_str().unwrap();
    let dataset = Dataset::open(uri).await.unwrap();

    let io_tracker = Arc::new(IOTracker::default());
    let store_params = ObjectStoreParams {
        object_store_wrapper: Some(io_tracker),
        ..Default::default()
    };
    let (wrapped_store, _) = ObjectStore::from_uri_and_params(
        dataset.session().store_registry(),
        dataset.uri(),
        &store_params,
    )
    .await
    .unwrap();
    let wrapped_dataset = dataset.with_object_store(wrapped_store, Some(store_params));
    assert!(Arc::ptr_eq(&dataset.session(), &wrapped_dataset.session()));
    assert!(!Arc::ptr_eq(
        &dataset.object_store.as_ref().inner,
        &wrapped_dataset.object_store.as_ref().inner
    ));
}

#[tokio::test]
async fn test_with_object_store_enables_isolated_per_request_io_tracking() {
    let test_dir = TempStdDir::default();
    create_file(&test_dir, WriteMode::Create, LanceFileVersion::Stable).await;
    let uri = test_dir.to_str().unwrap();
    let dataset = Dataset::open(uri).await.unwrap();

    let tracker_a = Arc::new(IOTracker::default());
    let store_params_a = ObjectStoreParams {
        object_store_wrapper: Some(tracker_a.clone()),
        ..Default::default()
    };
    let (wrapped_store_a, _) = ObjectStore::from_uri_and_params(
        dataset.session().store_registry(),
        dataset.uri(),
        &store_params_a,
    )
    .await
    .unwrap();
    let wrapped_a = dataset.with_object_store(wrapped_store_a, Some(store_params_a));

    let tracker_b = Arc::new(IOTracker::default());
    let store_params_b = ObjectStoreParams {
        object_store_wrapper: Some(tracker_b.clone()),
        ..Default::default()
    };
    let (wrapped_store_b, _) = ObjectStore::from_uri_and_params(
        dataset.session().store_registry(),
        dataset.uri(),
        &store_params_b,
    )
    .await
    .unwrap();
    let wrapped_b = dataset.with_object_store(wrapped_store_b, Some(store_params_b));

    let _ = tracker_a.incremental_stats(); // reset
    let _ = tracker_b.incremental_stats(); // reset

    // Request A uses only wrapper A.
    drain_scan(&wrapped_a).await;
    assert!(tracker_a.incremental_stats().read_iops > 0);
    assert_eq!(tracker_b.incremental_stats().read_iops, 0);

    // Request B uses only wrapper B.
    drain_scan(&wrapped_b).await;
    assert_eq!(tracker_a.incremental_stats().read_iops, 0);
    assert!(tracker_b.incremental_stats().read_iops > 0);

    // Base dataset does not use request-specific wrappers.
    drain_scan(&dataset).await;
    assert_eq!(tracker_a.incremental_stats().read_iops, 0);
    assert_eq!(tracker_b.incremental_stats().read_iops, 0);
}

#[tokio::test]
async fn test_with_object_store_wrappers_wraps_primary_store() {
    let test_dir = TempStdDir::default();
    create_file(&test_dir, WriteMode::Create, LanceFileVersion::Stable).await;
    let uri = test_dir.to_str().unwrap();
    let dataset = Dataset::open(uri).await.unwrap();

    let tracker = Arc::new(IOTracker::default());
    let wrapped =
        dataset.with_object_store_wrappers(vec![tracker.clone() as Arc<dyn WrappingObjectStore>]);

    let _ = tracker.incremental_stats();
    drain_scan(&wrapped).await;
    assert!(tracker.incremental_stats().read_iops > 0);
}

#[tokio::test]
async fn test_with_object_store_wrappers_wraps_base_store_params() {
    let test_dir = TempStdDir::default();
    create_file(&test_dir, WriteMode::Create, LanceFileVersion::Stable).await;
    let uri = test_dir.to_str().unwrap();
    let dataset = Arc::new(Dataset::open(uri).await.unwrap());

    let base_dir = tempfile::tempdir().unwrap();
    let base_uri = file_object_store_uri(base_dir.path());
    let base = BasePath::new(1, base_uri, Some("base".to_string()), true);
    dataset.add_bases(vec![base.clone()], None).await.unwrap();

    let existing_tracker = Arc::new(IOTracker::default());
    let base_store_params = ObjectStoreParams {
        object_store_wrapper: Some(existing_tracker.clone()),
        ..Default::default()
    };
    let dataset = DatasetBuilder::from_uri(uri)
        .with_base_store_params(&base.path, base_store_params)
        .load()
        .await
        .unwrap();

    let request_tracker = Arc::new(IOTracker::default());
    let wrapped = dataset
        .with_object_store_wrappers(vec![request_tracker.clone() as Arc<dyn WrappingObjectStore>]);
    let base_store = wrapped.object_store(Some(1)).await.unwrap();
    let base_location = base
        .extract_path(wrapped.session().store_registry())
        .unwrap()
        .join("data")
        .join("probe.lance");

    base_store.put(&base_location, b"hello").await.unwrap();
    let _ = existing_tracker.incremental_stats();
    let _ = request_tracker.incremental_stats();

    base_store
        .inner
        .get_range(&base_location, 0..1)
        .await
        .unwrap();

    assert!(existing_tracker.incremental_stats().read_iops > 0);
    assert!(request_tracker.incremental_stats().read_iops > 0);
}

#[tokio::test]
async fn test_store_params_for_base_resolves_base_scoped_options() {
    let test_dir = TempStdDir::default();
    create_file(&test_dir, WriteMode::Create, LanceFileVersion::Stable).await;
    let uri = test_dir.to_str().unwrap();
    let dataset = Arc::new(Dataset::open(uri).await.unwrap());

    let base_dir = tempfile::tempdir().unwrap();
    let base_uri = file_object_store_uri(base_dir.path());
    let base = BasePath::new(1, base_uri, Some("base".to_string()), true);
    dataset.add_bases(vec![base.clone()], None).await.unwrap();

    // Reopen with a single flat storage options map carrying base-scoped
    // entries (`base_<id>.<key>`) next to shared defaults.
    let dataset = DatasetBuilder::from_uri(uri)
        .with_storage_options(HashMap::from([
            ("shared_option".to_string(), "shared".to_string()),
            (
                "base_1.scoped_option".to_string(),
                "base1-value".to_string(),
            ),
        ]))
        .load()
        .await
        .unwrap();

    // The registered base resolves the scoped entry on top of shared defaults.
    let base_path = dataset.manifest.base_paths.get(&1).unwrap().clone();
    let params = dataset.store_params_for_base(Some(&base_path));
    assert_eq!(
        params.storage_options().unwrap(),
        &HashMap::from([
            ("shared_option".to_string(), "shared".to_string()),
            ("scoped_option".to_string(), "base1-value".to_string()),
        ])
    );

    // The default scope keeps only the shared defaults.
    let params = dataset.store_params_for_base(None);
    assert_eq!(
        params.storage_options().unwrap(),
        &HashMap::from([("shared_option".to_string(), "shared".to_string())])
    );

    // The base store resolved from scoped options is usable end to end.
    let base_store = dataset.object_store(Some(1)).await.unwrap();
    let probe = base
        .extract_path(dataset.session().store_registry())
        .unwrap()
        .join("data")
        .join("probe.lance");
    base_store.put(&probe, b"hello").await.unwrap();
    let read = base_store.inner.get_range(&probe, 0..5).await.unwrap();
    assert_eq!(read.as_ref(), b"hello");
}

#[tokio::test]
async fn test_with_object_store_wrappers_wraps_refs_store() {
    let test_dir = tempfile::tempdir().unwrap();
    let uri = file_object_store_uri(test_dir.path());
    let batch = gen_batch()
        .col("i", array::step::<Int32Type>())
        .into_batch_rows(RowCount::from(2))
        .unwrap();
    let dataset = Dataset::write(
        RecordBatchIterator::new(vec![Ok(batch.clone())], batch.schema()),
        &uri,
        None,
    )
    .await
    .unwrap();
    dataset
        .tags()
        .create("v1", dataset.manifest().version)
        .await
        .unwrap();

    let tracker = Arc::new(IOTracker::default());
    let wrapped =
        dataset.with_object_store_wrappers(vec![tracker.clone() as Arc<dyn WrappingObjectStore>]);

    let _ = tracker.incremental_stats();
    wrapped.tags().get("v1").await.unwrap();
    assert!(tracker.incremental_stats().read_iops > 0);
}

#[tokio::test]
async fn test_create_data_file_uses_base_object_store() {
    let primary_dir = tempfile::tempdir().unwrap();
    let source_dir = tempfile::tempdir().unwrap();
    let primary_uri = file_object_store_uri(primary_dir.path());
    let source_uri = file_object_store_uri(source_dir.path());

    let source_batch = gen_batch()
        .col("id", array::step::<Int32Type>())
        .into_batch_rows(RowCount::from(8))
        .unwrap();
    let source = Dataset::write(
        RecordBatchIterator::new(vec![Ok(source_batch.clone())], source_batch.schema()),
        &source_uri,
        None,
    )
    .await
    .unwrap();
    let primary = Arc::new(
        Dataset::write(
            RecordBatchIterator::new(vec![Ok(source_batch.clone())], source_batch.schema()),
            &primary_uri,
            None,
        )
        .await
        .unwrap(),
    );

    let base = BasePath::new(1, source_uri.clone(), Some("source".to_string()), true);
    primary.add_bases(vec![base.clone()], None).await.unwrap();

    let tracker = Arc::new(IOTracker::default());
    let dataset = DatasetBuilder::from_uri(&primary_uri)
        .with_base_store_params(
            &base.path,
            ObjectStoreParams {
                object_store_wrapper: Some(tracker.clone()),
                ..Default::default()
            },
        )
        .with_session(Arc::new(Session::default()))
        .load()
        .await
        .unwrap();
    let source_file = &source.manifest().fragments[0].files[0];

    let _ = tracker.incremental_stats();
    let data_file = dataset
        .create_data_file(&source_file.path, Some(base.id))
        .await
        .unwrap();

    assert_eq!(data_file.base_id, Some(base.id));
    assert!(tracker.incremental_stats().read_iops > 0);
}

#[tokio::test]
async fn test_create_data_file_rejects_nested_schema_mismatch() {
    let dataset_uri = format!(
        "memory://test_create_data_file_rejects_nested_schema_mismatch/{}",
        uuid::Uuid::new_v4()
    );

    let dataset_struct_fields = vec![
        ArrowField::new("a", DataType::Int32, true),
        ArrowField::new("b", DataType::Int32, true),
    ];
    let dataset_schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
        "s",
        DataType::Struct(dataset_struct_fields.clone().into()),
        true,
    )]));
    let dataset_struct = StructArray::try_new(
        dataset_struct_fields.clone().into(),
        vec![
            Arc::new(Int32Array::from(vec![1])) as ArrayRef,
            Arc::new(Int32Array::from(vec![2])) as ArrayRef,
        ],
        None,
    )
    .unwrap();
    let dataset_batch =
        RecordBatch::try_new(dataset_schema.clone(), vec![Arc::new(dataset_struct)]).unwrap();
    let dataset = Dataset::write(
        RecordBatchIterator::new(vec![Ok(dataset_batch)], dataset_schema),
        &dataset_uri,
        Some(WriteParams {
            data_storage_version: Some(LanceFileVersion::V2_2),
            ..Default::default()
        }),
    )
    .await
    .unwrap();

    async fn write_replacement_file(
        dataset: &Dataset,
        file_name: &str,
        struct_fields: Vec<ArrowField>,
    ) {
        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "s",
            DataType::Struct(struct_fields.clone().into()),
            true,
        )]));
        let values = struct_fields
            .iter()
            .enumerate()
            .map(|(idx, _)| Arc::new(Int32Array::from(vec![idx as i32])) as ArrayRef)
            .collect::<Vec<_>>();
        let struct_array = StructArray::try_new(struct_fields.into(), values, None).unwrap();
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(struct_array) as ArrayRef]).unwrap();

        let object_writer = dataset
            .object_store
            .create(&dataset.data_dir().join(file_name))
            .await
            .unwrap();
        let mut writer = lance_file::versions::v2_2::create_writer(
            object_writer,
            crate::datatypes::Schema::try_from(schema.as_ref()).unwrap(),
            FileWriterOptions::default(),
        )
        .unwrap();
        writer.write_batch(&batch).await.unwrap();
        writer.finish().await.unwrap();
    }

    write_replacement_file(
        &dataset,
        "nested_reordered.lance",
        vec![
            ArrowField::new("b", DataType::Int32, true),
            ArrowField::new("a", DataType::Int32, true),
        ],
    )
    .await;
    let err = dataset
        .create_data_file("nested_reordered.lance", None)
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("Schema mismatch"),
        "unexpected error: {err}"
    );

    write_replacement_file(
        &dataset,
        "nested_unknown.lance",
        vec![
            ArrowField::new("x", DataType::Int32, true),
            ArrowField::new("y", DataType::Int32, true),
        ],
    )
    .await;
    let err = dataset
        .create_data_file("nested_unknown.lance", None)
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("Schema mismatch"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn test_shallow_clone_base_artifacts_use_base_object_store() {
    let source_dir = tempfile::tempdir().unwrap();
    let clone_dir = tempfile::tempdir().unwrap();
    let source_uri = file_object_store_uri(source_dir.path());
    let clone_uri = file_object_store_uri(clone_dir.path());

    let batch = gen_batch()
        .col("id", array::step::<Int32Type>())
        .into_batch_rows(RowCount::from(64))
        .unwrap();
    let mut source = Dataset::write(
        RecordBatchIterator::new(vec![Ok(batch.clone())], batch.schema()),
        &source_uri,
        None,
    )
    .await
    .unwrap();
    source
        .create_index(
            &["id"],
            IndexType::Scalar,
            Some("id_idx".to_string()),
            &ScalarIndexParams::default(),
            false,
        )
        .await
        .unwrap();
    source.delete("id < 4").await.unwrap();
    source
        .tags()
        .create("with_artifacts", source.version().version)
        .await
        .unwrap();

    let cloned = source
        .shallow_clone(&clone_uri, "with_artifacts", None)
        .await
        .unwrap();
    let base = cloned
        .manifest()
        .base_paths
        .values()
        .next()
        .unwrap()
        .clone();
    let tracker = Arc::new(IOTracker::default());
    let cloned = DatasetBuilder::from_uri(&clone_uri)
        .with_base_store_params(
            &base.path,
            ObjectStoreParams {
                object_store_wrapper: Some(tracker.clone()),
                ..Default::default()
            },
        )
        .with_session(Arc::new(Session::default()))
        .load()
        .await
        .unwrap();

    let fragment = cloned
        .get_fragments()
        .into_iter()
        .find(|fragment| fragment.metadata.deletion_file.is_some())
        .unwrap();
    assert_eq!(
        fragment.metadata.deletion_file.as_ref().unwrap().base_id,
        Some(base.id)
    );

    let _ = tracker.incremental_stats();
    fragment.get_deletion_vector().await.unwrap().unwrap();
    assert!(tracker.incremental_stats().read_iops > 0);

    let indices = cloned.load_indices().await.unwrap();
    assert_eq!(indices[0].base_id, Some(base.id));

    let _ = tracker.incremental_stats();
    cloned.index_statistics("id_idx").await.unwrap();
    assert!(tracker.incremental_stats().read_iops > 0);
}

#[cfg(feature = "azure")]
#[tokio::test]
async fn test_object_store_uses_runtime_base_store_params() {
    let test_dir = TempStdDir::default();
    create_file(&test_dir, WriteMode::Create, LanceFileVersion::Stable).await;
    let uri = test_dir.to_str().unwrap();
    let dataset = Arc::new(Dataset::open(uri).await.unwrap());

    let base_a = BasePath::new(
        1,
        "az://container/path-a".to_string(),
        Some("base-a".to_string()),
        true,
    );
    let base_b = BasePath::new(
        2,
        "az://container/path-b".to_string(),
        Some("base-b".to_string()),
        true,
    );
    dataset
        .add_bases(vec![base_a.clone(), base_b.clone()], None)
        .await
        .unwrap();

    let base_a_store_params = ObjectStoreParams {
        storage_options_accessor: Some(Arc::new(StorageOptionsAccessor::with_static_options(
            HashMap::from([
                ("account_name".to_string(), "account-a".to_string()),
                ("account_key".to_string(), "dGVzdA==".to_string()),
            ]),
        ))),
        ..Default::default()
    };
    let default_store_params = ObjectStoreParams {
        storage_options_accessor: Some(Arc::new(StorageOptionsAccessor::with_static_options(
            HashMap::from([
                ("account_name".to_string(), "account-b".to_string()),
                ("account_key".to_string(), "dGVzdA==".to_string()),
            ]),
        ))),
        ..Default::default()
    };

    let dataset = DatasetBuilder::from_uri(uri)
        .with_store_params(default_store_params)
        .with_base_store_params(&base_a.path, base_a_store_params)
        .load()
        .await
        .unwrap();

    let store_a = dataset.object_store(Some(1)).await.unwrap();
    let store_a_again = dataset.object_store(Some(1)).await.unwrap();
    let store_b = dataset.object_store(Some(2)).await.unwrap();

    assert!(Arc::ptr_eq(&store_a, &store_a_again));
    assert!(!Arc::ptr_eq(&store_a, &store_b));
    assert_eq!(store_a.store_prefix, "az$container@account-a");
    assert_eq!(store_b.store_prefix, "az$container@account-b");
}

#[rstest]
#[lance_test_macros::test(tokio::test)]
async fn test_create_dataset(
    #[values(LanceFileVersion::Legacy, LanceFileVersion::Stable)]
    data_storage_version: LanceFileVersion,
) {
    // Appending / Overwriting a dataset that does not exist is treated as Create
    for mode in [WriteMode::Create, WriteMode::Append, Overwrite] {
        let test_dir = TempStdDir::default();
        create_file(&test_dir, mode, data_storage_version).await
    }
}

#[rstest]
#[lance_test_macros::test(tokio::test)]
async fn test_create_and_fill_empty_dataset(
    #[values(LanceFileVersion::Legacy, LanceFileVersion::Stable)]
    data_storage_version: LanceFileVersion,
) {
    let test_uri = TempStrDir::default();
    let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
        "i",
        DataType::Int32,
        false,
    )]));
    let i32_array: ArrayRef = Arc::new(Int32Array::new(vec![].into(), None));
    let batch = RecordBatch::try_from_iter(vec![("i", i32_array)]).unwrap();
    let reader = RecordBatchIterator::new(vec![batch].into_iter().map(Ok), schema.clone());
    // check schema of reader and original is same
    assert_eq!(schema.as_ref(), reader.schema().as_ref());
    let result = Dataset::write(
        reader,
        &test_uri,
        Some(WriteParams {
            data_storage_version: Some(data_storage_version),
            ..Default::default()
        }),
    )
    .await
    .unwrap();

    // check dataset empty
    assert_eq!(result.count_rows(None).await.unwrap(), 0);
    // Since the dataset is empty, will return None.
    assert_eq!(result.manifest.max_fragment_id(), None);

    // append rows to dataset
    let mut write_params = WriteParams {
        max_rows_per_file: 40,
        max_rows_per_group: 10,
        data_storage_version: Some(data_storage_version),
        ..Default::default()
    };
    // We should be able to append even if the metadata doesn't exactly match.
    let schema_with_meta = Arc::new(
        schema
            .as_ref()
            .clone()
            .with_metadata([("key".to_string(), "value".to_string())].into()),
    );
    let batches = vec![
        RecordBatch::try_new(
            schema_with_meta,
            vec![Arc::new(Int32Array::from_iter_values(0..10))],
        )
        .unwrap(),
    ];
    write_params.mode = WriteMode::Append;
    let batches = RecordBatchIterator::new(batches.into_iter().map(Ok), schema.clone());
    Dataset::write(batches, &test_uri, Some(write_params))
        .await
        .unwrap();

    let expected_batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int32Array::from_iter_values(0..10))],
    )
    .unwrap();

    // get actual dataset
    let actual_ds = Dataset::open(&test_uri).await.unwrap();
    // confirm schema is same
    let actual_schema = ArrowSchema::from(actual_ds.schema());
    assert_eq!(&actual_schema, schema.as_ref());
    // check num rows is 10
    assert_eq!(actual_ds.count_rows(None).await.unwrap(), 10);
    // Max fragment id is still 0 since we only have 1 fragment.
    assert_eq!(actual_ds.manifest.max_fragment_id(), Some(0));
    // check expected batch is correct
    let actual_batches = actual_ds
        .scan()
        .try_into_stream()
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    // sort
    let actual_batch = concat_batches(&schema, &actual_batches).unwrap();
    let idx_arr = actual_batch.column_by_name("i").unwrap();
    let sorted_indices = sort_to_indices(idx_arr, None, None).unwrap();
    let struct_arr: StructArray = actual_batch.into();
    let sorted_arr = arrow_select::take::take(&struct_arr, &sorted_indices, None).unwrap();
    let expected_struct_arr: StructArray = expected_batch.into();
    assert_eq!(&expected_struct_arr, as_struct_array(sorted_arr.as_ref()));
}

#[tokio::test]
async fn test_scan_constant_boolean_inline_value_v2_2() {
    let test_uri = TempStrDir::default();
    let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
        "flag",
        DataType::Boolean,
        false,
    )]));

    let rows = 1024usize;
    let flags: ArrayRef = Arc::new(BooleanArray::from_iter(std::iter::repeat_n(true, rows)));
    let batch = RecordBatch::try_new(schema.clone(), vec![flags]).unwrap();
    let reader = RecordBatchIterator::new(vec![Ok(batch)].into_iter(), schema.clone());

    Dataset::write(
        reader,
        &test_uri,
        Some(WriteParams {
            data_storage_version: Some(LanceFileVersion::V2_2),
            ..Default::default()
        }),
    )
    .await
    .unwrap();

    let ds = Dataset::open(&test_uri).await.unwrap();
    let batches = ds
        .scan()
        .project(&["flag"])
        .unwrap()
        .try_into_stream()
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, rows);
    for batch in batches {
        let flags = batch
            .column_by_name("flag")
            .unwrap()
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        for i in 0..flags.len() {
            assert!(flags.value(i));
        }
    }
}

#[rstest]
#[lance_test_macros::test(tokio::test)]
async fn test_create_with_empty_iter(
    #[values(LanceFileVersion::Legacy, LanceFileVersion::Stable)]
    data_storage_version: LanceFileVersion,
) {
    let test_uri = TempStrDir::default();
    let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
        "i",
        DataType::Int32,
        false,
    )]));
    let reader = RecordBatchIterator::new(vec![].into_iter().map(Ok), schema.clone());
    // check schema of reader and original is same
    assert_eq!(schema.as_ref(), reader.schema().as_ref());
    let write_params = Some(WriteParams {
        data_storage_version: Some(data_storage_version),
        ..Default::default()
    });
    let result = Dataset::write(reader, &test_uri, write_params)
        .await
        .unwrap();

    // check dataset empty
    assert_eq!(result.count_rows(None).await.unwrap(), 0);
    // Since the dataset is empty, will return None.
    assert_eq!(result.manifest.max_fragment_id(), None);
}

#[tokio::test]
async fn test_load_manifest_iops() {
    // Use consistent session so memory store can be reused.
    let session = Arc::new(Session::default());
    let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
        "i",
        DataType::Int32,
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int32Array::from_iter_values(0..10_i32))],
    )
    .unwrap();
    let batches = RecordBatchIterator::new(vec![Ok(batch)], schema.clone());
    let _original_ds = Dataset::write(
        batches,
        "memory://test",
        Some(WriteParams {
            session: Some(session.clone()),
            ..Default::default()
        }),
    )
    .await
    .unwrap();

    let _ = _original_ds.object_store.as_ref().io_stats_incremental(); //reset

    let _dataset = DatasetBuilder::from_uri("memory://test")
        .with_session(session)
        .load()
        .await
        .unwrap();

    // The write above committed on this same Session, so the manifest is already
    // in the metadata cache. Opening therefore issues a single IOP:
    // 1. List _versions directory to resolve the latest manifest location.
    // The manifest body is served from the cache instead of being read from storage.
    let io_stats = _dataset.object_store.as_ref().io_stats_incremental();
    assert_io_eq!(io_stats, read_iops, 1);
}

#[tokio::test]
async fn test_checkout_removed_version_not_served_from_cache() {
    let test_uri = TempStrDir::default();
    let session = Arc::new(Session::default());
    let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
        "i",
        DataType::Int32,
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int32Array::from_iter_values(0..10_i32))],
    )
    .unwrap();
    let dataset = Dataset::write(
        RecordBatchIterator::new(vec![Ok(batch)], schema.clone()),
        &test_uri,
        Some(WriteParams {
            session: Some(session.clone()),
            ..Default::default()
        }),
    )
    .await
    .unwrap();

    let version = dataset.manifest().version;
    let location = dataset.manifest_location().clone();
    let cache = session.metadata_cache.for_dataset(&dataset.uri);

    assert!(
        cache
            .get_with_key(&ManifestKey {
                version,
                e_tag: location.e_tag.as_deref(),
            })
            .await
            .is_some(),
        "manifest should be cached after the write"
    );
    dataset.checkout_version(version).await.unwrap();

    // Remove the version from storage, as cleanup (or a manual delete) would.
    dataset.object_store.delete(&location.path).await.unwrap();

    let resolved = dataset
        .commit_handler
        .resolve_version_location(&dataset.base, version, &dataset.object_store.inner)
        .await
        .unwrap();
    assert!(
        resolved.size.is_none(),
        "resolving a removed version must fall back to a size-less location, got {:?}",
        resolved.size
    );

    cache
        .insert_with_key(
            &ManifestKey {
                version,
                e_tag: None,
            },
            Arc::new(dataset.manifest().clone()),
        )
        .await;
    assert!(
        dataset.checkout_version(version).await.is_err(),
        "checkout of a version removed from storage must not be served from cache"
    );
}

#[rstest]
#[tokio::test]
async fn test_write_params(
    #[values(LanceFileVersion::Legacy, LanceFileVersion::Stable)]
    data_storage_version: LanceFileVersion,
) {
    use crate::dataset::fragment::FragReadConfig;

    let test_uri = TempStrDir::default();

    let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
        "i",
        DataType::Int32,
        false,
    )]));
    let num_rows: usize = 1_000;
    let batches = vec![
        RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from_iter_values(0..num_rows as i32))],
        )
        .unwrap(),
    ];

    let batches = RecordBatchIterator::new(batches.into_iter().map(Ok), schema.clone());

    let write_params = WriteParams {
        max_rows_per_file: 100,
        max_rows_per_group: 10,
        data_storage_version: Some(data_storage_version),
        ..Default::default()
    };
    let dataset = Dataset::write(batches, &test_uri, Some(write_params))
        .await
        .unwrap();

    assert_eq!(dataset.count_rows(None).await.unwrap(), num_rows);

    let fragments = dataset.get_fragments();
    assert_eq!(fragments.len(), 10);
    assert_eq!(dataset.count_fragments(), 10);
    for fragment in &fragments {
        assert_eq!(fragment.count_rows(None).await.unwrap(), 100);
        let reader = fragment
            .open(dataset.schema(), FragReadConfig::default())
            .await
            .unwrap();
        // No group / batch concept in v2
        if data_storage_version == LanceFileVersion::Legacy {
            assert_eq!(reader.legacy_num_batches(), 10);
            for i in 0..reader.legacy_num_batches() as u32 {
                assert_eq!(reader.legacy_num_rows_in_batch(i).unwrap(), 10);
            }
        }
    }
}

#[rstest]
#[tokio::test]
async fn test_write_manifest(
    #[values(
        LanceFileVersion::Legacy,
        LanceFileVersion::Stable,
        LanceFileVersion::Next
    )]
    data_storage_version: LanceFileVersion,
) {
    use lance_table::feature_flags::FLAG_UNKNOWN;

    let test_uri = TempStrDir::default();

    let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
        "i",
        DataType::Int32,
        false,
    )]));
    let batches = vec![
        RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from_iter_values(0..20))],
        )
        .unwrap(),
    ];

    let batches = RecordBatchIterator::new(batches.into_iter().map(Ok), schema.clone());
    let write_fut = Dataset::write(
        batches,
        &test_uri,
        Some(WriteParams {
            data_storage_version: Some(data_storage_version),
            auto_cleanup: None,
            ..Default::default()
        }),
    );
    let write_fut = require_send(write_fut);
    let mut dataset = write_fut.await.unwrap();

    // Check it has no flags
    let manifest = read_manifest(
        dataset.object_store.as_ref(),
        &dataset
            .commit_handler
            .resolve_latest_location(&dataset.base, dataset.object_store.as_ref())
            .await
            .unwrap()
            .path,
        None,
    )
    .await
    .unwrap();

    assert_eq!(
        manifest.data_storage_format,
        DataStorageFormat::new(ConcreteFileVersion::from(data_storage_version))
    );
    assert!(!matches!(
        manifest.data_storage_format.version.to_manifest_string(),
        "stable" | "next"
    ));
    assert_eq!(manifest.reader_feature_flags, 0);

    // Create one with deletions
    dataset.delete("i < 10").await.unwrap();
    dataset.validate().await.unwrap();

    // Check it set the flag
    let mut manifest = read_manifest(
        dataset.object_store.as_ref(),
        &dataset
            .commit_handler
            .resolve_latest_location(&dataset.base, dataset.object_store.as_ref())
            .await
            .unwrap()
            .path,
        None,
    )
    .await
    .unwrap();
    assert_eq!(
        manifest.writer_feature_flags,
        feature_flags::FLAG_DELETION_FILES
    );
    assert_eq!(
        manifest.reader_feature_flags,
        feature_flags::FLAG_DELETION_FILES
    );

    // Write with custom manifest
    manifest.writer_feature_flags |= FLAG_UNKNOWN; // Set another flag
    manifest.reader_feature_flags |= FLAG_UNKNOWN;
    manifest.version += 1;
    write_manifest_file(
        dataset.object_store.as_ref(),
        dataset.commit_handler.as_ref(),
        &dataset.base,
        &mut manifest,
        None,
        &ManifestWriteConfig {
            auto_set_feature_flags: false,
            timestamp: None,
            use_stable_row_ids: false,
            use_legacy_format: None,
            storage_format: None,
            disable_transaction_file: false,
        },
        dataset.manifest_location.naming_scheme,
        None,
    )
    .await
    .unwrap();

    // Check it rejects reading it
    let read_result = Dataset::open(&test_uri).await;
    assert!(matches!(read_result, Err(Error::NotSupported { .. })));

    // Check it rejects writing to it.
    let batches = vec![
        RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from_iter_values(0..20))],
        )
        .unwrap(),
    ];
    let batches = RecordBatchIterator::new(batches.into_iter().map(Ok), schema.clone());
    let write_result = Dataset::write(
        batches,
        &test_uri,
        Some(WriteParams {
            mode: WriteMode::Append,
            data_storage_version: Some(data_storage_version),
            ..Default::default()
        }),
    )
    .await;

    assert!(matches!(write_result, Err(Error::NotSupported { .. })));
}

#[tokio::test]
async fn test_rle_v2_v23_write_and_append() {
    let test_uri = TempStrDir::default();
    let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
        "i",
        DataType::Int32,
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int32Array::from(vec![7; 1000]))],
    )
    .unwrap();

    let batches = RecordBatchIterator::new(vec![Ok(batch)].into_iter(), schema.clone());
    let mut dataset = Dataset::write(
        batches,
        &test_uri,
        Some(WriteParams {
            data_storage_version: Some(LanceFileVersion::V2_3),
            ..Default::default()
        }),
    )
    .await
    .unwrap();

    let manifest = read_manifest(
        dataset.object_store.as_ref(),
        &dataset
            .commit_handler
            .resolve_latest_location(&dataset.base, dataset.object_store.as_ref())
            .await
            .unwrap()
            .path,
        None,
    )
    .await
    .unwrap();
    assert_eq!(
        manifest.data_storage_format.lance_file_version().unwrap(),
        LanceFileVersion::V2_3
    );

    let append_batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int32Array::from(vec![9; 1000]))],
    )
    .unwrap();
    let append_batches =
        RecordBatchIterator::new(vec![Ok(append_batch)].into_iter(), schema.clone());
    dataset = Dataset::write(
        append_batches,
        &test_uri,
        Some(WriteParams {
            mode: WriteMode::Append,
            ..Default::default()
        }),
    )
    .await
    .unwrap();

    assert_eq!(
        dataset
            .manifest
            .data_storage_format
            .lance_file_version()
            .unwrap(),
        LanceFileVersion::V2_3
    );

    let actual = dataset.scan().try_into_batch().await.unwrap();
    let expected = RecordBatch::try_new(
        schema,
        vec![Arc::new(Int32Array::from(
            [vec![7; 1000], vec![9; 1000]].concat(),
        ))],
    )
    .unwrap();
    assert_eq!(actual, expected);
}

#[tokio::test]
async fn test_rle_v2_uncommitted_create_commits_v23_storage() {
    let test_uri = TempStrDir::default();
    let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
        "i",
        DataType::Int32,
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int32Array::from(vec![7; 1000]))],
    )
    .unwrap();

    let transaction = InsertBuilder::new(test_uri.as_str())
        .with_params(&WriteParams {
            data_storage_version: Some(LanceFileVersion::V2_3),
            ..Default::default()
        })
        .execute_uncommitted(vec![batch])
        .await
        .unwrap();

    let dataset = CommitBuilder::new(test_uri.as_str())
        .execute(transaction)
        .await
        .unwrap();
    assert_eq!(
        dataset
            .manifest
            .data_storage_format
            .lance_file_version()
            .unwrap(),
        LanceFileVersion::V2_3
    );
}

#[tokio::test]
async fn test_rle_v2_shallow_clone_preserves_v23_storage() {
    let test_uri = TempStrDir::default();
    let clone_uri = TempStrDir::default();
    let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
        "i",
        DataType::Int32,
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int32Array::from(vec![7; 1000]))],
    )
    .unwrap();

    let mut dataset = Dataset::write(
        RecordBatchIterator::new(vec![Ok(batch)].into_iter(), schema),
        &test_uri,
        Some(WriteParams {
            data_storage_version: Some(LanceFileVersion::V2_3),
            ..Default::default()
        }),
    )
    .await
    .unwrap();

    let clone = dataset
        .shallow_clone(clone_uri.as_str(), dataset.version().version, None)
        .await
        .unwrap();
    assert_eq!(
        clone
            .manifest
            .data_storage_format
            .lance_file_version()
            .unwrap(),
        LanceFileVersion::V2_3
    );
}

#[rstest]
#[tokio::test]
async fn append_dataset(
    #[values(LanceFileVersion::Legacy, LanceFileVersion::Stable)]
    data_storage_version: LanceFileVersion,
) {
    let test_uri = TempStrDir::default();

    let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
        "i",
        DataType::Int32,
        false,
    )]));
    let batches = vec![
        RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from_iter_values(0..20))],
        )
        .unwrap(),
    ];

    let mut write_params = WriteParams {
        max_rows_per_file: 40,
        max_rows_per_group: 10,
        data_storage_version: Some(data_storage_version),
        ..Default::default()
    };
    let batches = RecordBatchIterator::new(batches.into_iter().map(Ok), schema.clone());
    Dataset::write(batches, &test_uri, Some(write_params.clone()))
        .await
        .unwrap();

    let batches = vec![
        RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from_iter_values(20..40))],
        )
        .unwrap(),
    ];
    write_params.mode = WriteMode::Append;
    let batches = RecordBatchIterator::new(batches.into_iter().map(Ok), schema.clone());
    Dataset::write(batches, &test_uri, Some(write_params.clone()))
        .await
        .unwrap();

    let expected_batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int32Array::from_iter_values(0..40))],
    )
    .unwrap();

    let actual_ds = Dataset::open(&test_uri).await.unwrap();
    assert_eq!(actual_ds.version().version, 2);
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
    // sort
    let actual_batch = concat_batches(&schema, &actual_batches).unwrap();
    let idx_arr = actual_batch.column_by_name("i").unwrap();
    let sorted_indices = sort_to_indices(idx_arr, None, None).unwrap();
    let struct_arr: StructArray = actual_batch.into();
    let sorted_arr = arrow_select::take::take(&struct_arr, &sorted_indices, None).unwrap();

    let expected_struct_arr: StructArray = expected_batch.into();
    assert_eq!(&expected_struct_arr, as_struct_array(sorted_arr.as_ref()));

    // Each fragments has different fragment ID
    assert_eq!(
        actual_ds
            .fragments()
            .iter()
            .map(|f| f.id)
            .collect::<Vec<_>>(),
        (0..2).collect::<Vec<_>>()
    )
}

#[rstest]
#[tokio::test]
async fn test_deep_clone(
    #[values(LanceFileVersion::Legacy, LanceFileVersion::Stable)]
    data_storage_version: LanceFileVersion,
) {
    // Setup source and target dirs
    let test_dir = TempStdDir::default();
    let base_dir = test_dir.join("base_ds");
    let test_uri = base_dir.to_str().unwrap();
    let clone_dir = test_dir.join("clone_ds");
    let cloned_uri = clone_dir.to_str().unwrap();

    // Generate test data
    let data_reader = gen_batch()
        .col("id", array::step::<Int32Type>())
        .col("val", array::fill_utf8("deep".to_string()))
        .into_reader_rows(RowCount::from(64), BatchCount::from(1));

    // Create source dataset
    let mut dataset = Dataset::write(
        data_reader,
        test_uri,
        Some(WriteParams {
            max_rows_per_file: 64,
            max_rows_per_group: 16,
            data_storage_version: Some(data_storage_version),
            ..Default::default()
        }),
    )
    .await
    .unwrap();

    let mut branch = dataset
        .create_branch("branch", dataset.version().version, None)
        .await
        .unwrap();

    // Create a scalar index to validate index copy
    branch
        .create_index(
            &["id"],
            IndexType::Scalar,
            Some("id_idx".to_string()),
            &ScalarIndexParams::default(),
            false,
        )
        .await
        .unwrap();

    // Create a deletion file by deleting some rows
    branch.delete("id < 10").await.unwrap();

    let original_version = branch.version().version;
    branch
        .tags()
        .create("tag", ("branch", original_version))
        .await
        .unwrap();

    // Perform deep clone
    let cloned_dataset = branch.deep_clone(cloned_uri, "tag", None).await.unwrap();

    // Validate target dataset rows
    let batches = cloned_dataset
        .scan()
        .try_into_stream()
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 54); // 64 rows - 10 deletions
    assert_eq!(cloned_dataset.version().version, original_version);
    assert!(cloned_dataset.manifest().base_paths.is_empty());

    // Validate internal file counts are equal between source and cloned datasets
    let store = branch.object_store.as_ref();
    let src_root = dataset.base.clone();
    let branch_root = branch.base.clone();
    let dst_root = cloned_dataset.base.clone();

    let src_data = count_files(store, &src_root, "data").await;
    let dst_data = count_files(store, &dst_root, "data").await;
    assert_eq!(src_data, dst_data);

    let src_idx = count_files(store, &branch_root, "_indices").await;
    let dst_idx = count_files(store, &dst_root, "_indices").await;
    assert_eq!(src_idx, dst_idx);

    let src_del = count_files(store, &branch_root, "_deletions").await;
    let dst_del = count_files(store, &dst_root, "_deletions").await;
    assert_eq!(src_del, dst_del);

    // Validate index exists in cloned dataset
    let cloned_indices = cloned_dataset.load_indices().await.unwrap();
    assert!(!cloned_indices.is_empty());
    assert_eq!(cloned_indices.first().unwrap().name, "id_idx");

    // Verify base_id cleared in cloned manifest and indices
    for frag in cloned_dataset.manifest().fragments.iter() {
        for df in &frag.files {
            assert!(df.base_id.is_none());
        }
        if let Some(del) = &frag.deletion_file {
            assert!(del.base_id.is_none());
        }
    }
    for idx in cloned_indices.iter() {
        assert!(idx.base_id.is_none());
    }

    // Attempt cloning again to the same target should error
    let res = dataset.deep_clone(cloned_uri, "tag", None).await;
    assert!(matches!(res, Err(Error::DatasetAlreadyExists { .. })));

    // Invalid tag should error
    let res_invalid = dataset
        .deep_clone(&format!("{}/clone_invalid", test_uri), "no_such_tag", None)
        .await;
    assert!(matches!(res_invalid, Err(Error::RefNotFound { .. })));

    // deep_clone version before the deletion
    let clone_dir = test_dir.join("clone_ds_old_ver");
    let cloned_ds = clone_dir.to_str().unwrap();
    let cloned_dataset = branch
        .deep_clone(cloned_ds, ("branch", original_version - 1), None)
        .await
        .unwrap();
    let store = branch.object_store.as_ref();
    let dst_root = cloned_dataset.base.clone();

    // Validate target dataset rows
    let batches = cloned_dataset
        .scan()
        .try_into_stream()
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 64);
    assert_eq!(cloned_dataset.version().version, original_version - 1);
    assert!(cloned_dataset.manifest().base_paths.is_empty());
    assert_eq!(count_files(store, &dst_root, "_deletions").await, 0);
}

#[tokio::test]
async fn test_deep_clone_recognizes_ambiguous_commit_as_own() {
    use crate::utils::test::{AmbiguousCommitHandler, AmbiguousFailure};

    let test_dir = TempStdDir::default();
    let source_dir = test_dir.join("source");
    let source_uri = source_dir.to_str().unwrap();
    let target_dir = test_dir.join("target");
    let target_uri = target_dir.to_str().unwrap();
    let handler = Arc::new(AmbiguousCommitHandler::default());
    let data_reader = gen_batch()
        .col("id", array::step::<Int32Type>())
        .into_reader_rows(RowCount::from(32), BatchCount::from(1));
    let mut source = Dataset::write(
        data_reader,
        source_uri,
        Some(WriteParams {
            commit_handler: Some(handler.clone()),
            ..Default::default()
        }),
    )
    .await
    .unwrap();
    let source_transaction_file = source.manifest().transaction_file.clone();

    handler.fail_next(AmbiguousFailure::LandAndConflict);
    let cloned = source
        .deep_clone(target_uri, source.version().version, None)
        .await
        .expect("readback must identify the deep-clone transaction that landed");

    assert_eq!(cloned.count_rows(None).await.unwrap(), 32);
    assert_ne!(cloned.manifest().transaction_file, source_transaction_file);
    assert!(cloned.manifest().transaction_section.is_some());
}

// Uses an in-memory source store to force a cross-store copy. The in-memory store has
// known platform-specific quirks on Windows (it reads back empty there; see the note in
// tests/resource_tests.rs), so this test is gated to non-Windows. The local write side is
// covered on Windows by `test_deep_clone` (same-store), and the cross-store streaming path
// against real cloud stores is platform-agnostic std/tokio I/O.
#[cfg(not(windows))]
#[rstest]
#[tokio::test]
async fn test_deep_clone_cross_store(
    #[values(LanceFileVersion::Legacy, LanceFileVersion::Stable)]
    data_storage_version: LanceFileVersion,
) {
    // Source lives in an in-memory store while the target is a local directory, so the
    // two stores have different `store_prefix`es and `deep_clone` must stream files from
    // the source store to the target store (the cross-account code path).
    let session = Arc::new(Session::default());
    let test_dir = TempStdDir::default();
    let clone_dir = test_dir.join("clone_ds");
    let cloned_uri = clone_dir.to_str().unwrap();

    // 64 rows across 4 files exercises the multi-fragment copy path.
    let data_reader = gen_batch()
        .col("id", array::step::<Int32Type>())
        .col("val", array::fill_utf8("deep".to_string()))
        .into_reader_rows(RowCount::from(64), BatchCount::from(1));

    let mut dataset = Dataset::write(
        data_reader,
        "memory://cross_store_src",
        Some(WriteParams {
            max_rows_per_file: 16,
            max_rows_per_group: 16,
            data_storage_version: Some(data_storage_version),
            session: Some(session.clone()),
            ..Default::default()
        }),
    )
    .await
    .unwrap();
    assert_ne!(dataset.object_store.store_prefix, "");

    // Create a scalar index so the index files and the manifest index section are also
    // copied across stores (the index section is read through the source store at commit).
    dataset
        .create_index(
            &["id"],
            IndexType::Scalar,
            Some("id_idx".to_string()),
            &ScalarIndexParams::default(),
            false,
        )
        .await
        .unwrap();

    // Delete some rows so a deletion file is also streamed across stores.
    dataset.delete("id < 10").await.unwrap();
    let cloned_dataset = dataset
        .deep_clone(cloned_uri, dataset.version().version, None)
        .await
        .unwrap();

    // The clone targets a local store, distinct from the in-memory source.
    assert_ne!(
        cloned_dataset.object_store.store_prefix,
        dataset.object_store.store_prefix
    );
    assert!(cloned_dataset.manifest().base_paths.is_empty());

    // Re-open the clone from a fresh session to prove the files were physically copied
    // into the target store and the clone is fully independent of the source store.
    let reopened = DatasetBuilder::from_uri(cloned_uri).load().await.unwrap();
    let batches = reopened
        .scan()
        .try_into_stream()
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 54); // 64 rows - 10 deletions

    // The scalar index must have been copied and resolve against the target store, with its
    // base reference normalized to local (no external base_paths).
    let cloned_indices = reopened.load_indices().await.unwrap();
    assert_eq!(cloned_indices.len(), 1);
    assert_eq!(cloned_indices.first().unwrap().name, "id_idx");
    assert!(cloned_indices.iter().all(|idx| idx.base_id.is_none()));
}

// Helper: count files under a dataset directory (data/_indices/_deletions)
async fn count_files(store: &ObjectStore, root: &Path, prefix: &str) -> usize {
    use futures::StreamExt;
    let dir = root.clone().join(prefix);
    let mut stream = store.read_dir_all(&dir, None);
    let mut count: usize = 0;
    while stream.next().await.transpose().unwrap().is_some() {
        count += 1;
    }
    count
}

#[rstest]
#[tokio::test]
async fn test_shallow_clone_with_hybrid_paths(
    #[values(LanceFileVersion::Legacy, LanceFileVersion::Stable)]
    data_storage_version: LanceFileVersion,
) {
    let test_dir = TempStdDir::default();
    let base_dir = test_dir.join("base");
    let test_uri = base_dir.to_str().unwrap();
    let clone_dir = test_dir.join("clone");
    let cloned_uri = clone_dir.to_str().unwrap();

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

    // Create initial dataset
    let mut dataset = write_dataset(
        test_uri,
        generate_data("initial", 0, 50),
        WriteMode::Create,
        data_storage_version,
    )
    .await;

    // Store original state for comparison
    let original_version = dataset.version().version;
    let original_fragment_count = dataset.fragments().len();

    // Create tag and shallow clone
    dataset
        .tags()
        .create("test_tag", original_version)
        .await
        .unwrap();
    let cloned_dataset = dataset
        .shallow_clone(cloned_uri, "test_tag", None)
        .await
        .unwrap();

    // Verify cloned dataset state
    let (cloned_rows, _) = collect_rows(&cloned_dataset).await;
    assert_eq!(cloned_rows, 50);
    assert_eq!(cloned_dataset.version().version, original_version);

    // Append data to cloned dataset
    let updated_cloned = write_dataset(
        cloned_uri,
        generate_data("cloned_new", 50, 30),
        WriteMode::Append,
        data_storage_version,
    )
    .await;

    // Verify updated cloned dataset
    let (updated_cloned_rows, updated_batches) = collect_rows(&updated_cloned).await;
    assert_eq!(updated_cloned_rows, 80);
    assert_eq!(updated_cloned.version().version, original_version + 1);

    // Append data to original dataset
    let updated_original = write_dataset(
        test_uri,
        generate_data("original_new", 50, 25),
        WriteMode::Append,
        data_storage_version,
    )
    .await;

    // Verify updated original dataset
    let (original_rows, _) = collect_rows(&updated_original).await;
    assert_eq!(original_rows, 75);
    assert_eq!(updated_original.version().version, original_version + 1);

    // Final validations
    // Verify cloned dataset isolation
    let final_cloned = Dataset::open(cloned_uri).await.unwrap();
    let (final_cloned_rows, _) = collect_rows(&final_cloned).await;

    // Data integrity check
    let combined_batch = concat_batches(&updated_batches[0].schema(), &updated_batches).unwrap();
    assert_eq!(combined_batch.column_by_name("id").unwrap().len(), 80);
    assert_eq!(combined_batch.column_by_name("value").unwrap().len(), 80);

    // Fragment count validation
    assert_eq!(
        updated_original.fragments().len(),
        original_fragment_count + 1
    );
    assert_eq!(final_cloned.fragments().len(), original_fragment_count + 1);

    // Final assertions
    assert_eq!(final_cloned_rows, 80);
    assert_eq!(final_cloned.version().version, original_version + 1);
}

#[rstest]
#[tokio::test]
async fn test_shallow_clone_multiple_times(
    #[values(LanceFileVersion::Legacy, LanceFileVersion::Stable)]
    data_storage_version: LanceFileVersion,
) {
    let test_uri = TempStrDir::default();
    let append_row_count = 36;

    // Async dataset writer function
    async fn write_dataset(
        dest: impl Into<WriteDestination<'_>>,
        row_count: u64,
        mode: WriteMode,
        version: LanceFileVersion,
    ) -> Dataset {
        let data = gen_batch()
            .col("index", array::step::<Int32Type>())
            .col("category", array::fill_utf8("base".to_string()))
            .col("score", array::step_custom::<Float32Type>(1.0, 0.5));
        Dataset::write(
            data.into_reader_rows(RowCount::from(row_count), BatchCount::from(1)),
            dest,
            Some(WriteParams {
                max_rows_per_file: 60,
                max_rows_per_group: 12,
                mode,
                data_storage_version: Some(version),
                ..Default::default()
            }),
        )
        .await
        .unwrap()
    }

    let mut current_dataset = write_dataset(
        &test_uri,
        append_row_count,
        WriteMode::Create,
        data_storage_version,
    )
    .await;

    let test_round = 3;
    // Generate clone paths
    let clone_paths = (1..=test_round)
        .map(|i| format!("{}/clone{}", test_uri, i))
        .collect::<Vec<_>>();
    let mut cloned_datasets = Vec::with_capacity(test_round);

    // Unified cloning procedure, write a fragment to each cloned dataset.
    for path in clone_paths.iter() {
        current_dataset
            .tags()
            .create("v1", current_dataset.latest_version_id().await.unwrap())
            .await
            .unwrap();

        current_dataset = current_dataset
            .shallow_clone(path, "v1", None)
            .await
            .unwrap();
        current_dataset = write_dataset(
            Arc::new(current_dataset),
            append_row_count,
            WriteMode::Append,
            data_storage_version,
        )
        .await;
        cloned_datasets.push(current_dataset.clone());
    }

    // Validation function
    async fn validate_dataset(
        dataset: &Dataset,
        expected_rows: usize,
        expected_fragments_count: usize,
        expected_base_paths_count: usize,
    ) {
        let batches = dataset
            .scan()
            .try_into_stream()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, expected_rows);
        assert_eq!(dataset.fragments().len(), expected_fragments_count);
        assert_eq!(
            dataset.manifest().base_paths.len(),
            expected_base_paths_count
        );
    }

    // Verify cloned datasets row count, fragment count, base_path count
    for (i, ds) in cloned_datasets.iter().enumerate() {
        validate_dataset(ds, 36 * (i + 2), i + 2, i + 1).await;
    }

    // Verify original dataset row count, fragment count, base_path count
    let original = Dataset::open(&test_uri).await.unwrap();
    validate_dataset(&original, 36, 1, 0).await;
}

#[rstest]
#[tokio::test]
async fn test_self_dataset_append(
    #[values(LanceFileVersion::Legacy, LanceFileVersion::Stable)]
    data_storage_version: LanceFileVersion,
) {
    let test_uri = TempStrDir::default();

    let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
        "i",
        DataType::Int32,
        false,
    )]));
    let batches = vec![
        RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from_iter_values(0..20))],
        )
        .unwrap(),
    ];

    let mut write_params = WriteParams {
        max_rows_per_file: 40,
        max_rows_per_group: 10,
        data_storage_version: Some(data_storage_version),
        ..Default::default()
    };
    let batches = RecordBatchIterator::new(batches.into_iter().map(Ok), schema.clone());
    let mut ds = Dataset::write(batches, &test_uri, Some(write_params.clone()))
        .await
        .unwrap();

    let batches = vec![
        RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from_iter_values(20..40))],
        )
        .unwrap(),
    ];
    write_params.mode = WriteMode::Append;
    let batches = RecordBatchIterator::new(batches.into_iter().map(Ok), schema.clone());

    ds.append(batches, Some(write_params.clone()))
        .await
        .unwrap();

    let expected_batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int32Array::from_iter_values(0..40))],
    )
    .unwrap();

    let actual_ds = Dataset::open(&test_uri).await.unwrap();
    assert_eq!(actual_ds.version().version, 2);
    // validate fragment ids
    assert_eq!(actual_ds.fragments().len(), 2);
    assert_eq!(
        actual_ds
            .fragments()
            .iter()
            .map(|f| f.id)
            .collect::<Vec<_>>(),
        (0..2).collect::<Vec<_>>()
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
    // sort
    let actual_batch = concat_batches(&schema, &actual_batches).unwrap();
    let idx_arr = actual_batch.column_by_name("i").unwrap();
    let sorted_indices = sort_to_indices(idx_arr, None, None).unwrap();
    let struct_arr: StructArray = actual_batch.into();
    let sorted_arr = arrow_select::take::take(&struct_arr, &sorted_indices, None).unwrap();

    let expected_struct_arr: StructArray = expected_batch.into();
    assert_eq!(&expected_struct_arr, as_struct_array(sorted_arr.as_ref()));

    actual_ds.validate().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn test_self_dataset_append_schema_different(
    #[values(LanceFileVersion::Legacy, LanceFileVersion::Stable)]
    data_storage_version: LanceFileVersion,
) {
    let test_uri = TempStrDir::default();

    let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
        "i",
        DataType::Int32,
        false,
    )]));
    let batches = vec![
        RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from_iter_values(0..20))],
        )
        .unwrap(),
    ];

    let other_schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
        "i",
        DataType::Int64,
        false,
    )]));
    let other_batches = vec![
        RecordBatch::try_new(
            other_schema.clone(),
            vec![Arc::new(Int64Array::from_iter_values(0..20))],
        )
        .unwrap(),
    ];

    let mut write_params = WriteParams {
        max_rows_per_file: 40,
        max_rows_per_group: 10,
        data_storage_version: Some(data_storage_version),
        ..Default::default()
    };
    let batches = RecordBatchIterator::new(batches.into_iter().map(Ok), schema.clone());
    let mut ds = Dataset::write(batches, &test_uri, Some(write_params.clone()))
        .await
        .unwrap();

    write_params.mode = WriteMode::Append;
    let other_batches =
        RecordBatchIterator::new(other_batches.into_iter().map(Ok), other_schema.clone());

    let result = ds.append(other_batches, Some(write_params.clone())).await;
    // Error because schema is different
    assert!(matches!(result, Err(Error::SchemaMismatch { .. })))
}

#[rstest]
#[tokio::test]
async fn append_dictionary(
    #[values(LanceFileVersion::Legacy, LanceFileVersion::Stable)]
    data_storage_version: LanceFileVersion,
) {
    // We store the dictionary as part of the schema, so we check that the
    // dictionary is consistent between appends.

    let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
        "x",
        DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
        false,
    )]));
    let dictionary = Arc::new(StringArray::from(vec!["a", "b"]));
    let indices = Int8Array::from(vec![0, 1, 0]);
    let batches = vec![
        RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(
                Int8DictionaryArray::try_new(indices, dictionary.clone()).unwrap(),
            )],
        )
        .unwrap(),
    ];

    let test_uri = TempStrDir::default();
    let mut write_params = WriteParams {
        max_rows_per_file: 40,
        max_rows_per_group: 10,
        data_storage_version: Some(data_storage_version),
        ..Default::default()
    };
    let batches = RecordBatchIterator::new(batches.into_iter().map(Ok), schema.clone());
    Dataset::write(batches, &test_uri, Some(write_params.clone()))
        .await
        .unwrap();

    // create a new one with same dictionary
    let indices = Int8Array::from(vec![1, 0, 1]);
    let batches = vec![
        RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(
                Int8DictionaryArray::try_new(indices, dictionary).unwrap(),
            )],
        )
        .unwrap(),
    ];

    // Write to dataset (successful)
    write_params.mode = WriteMode::Append;
    let batches = RecordBatchIterator::new(batches.into_iter().map(Ok), schema.clone());
    Dataset::write(batches, &test_uri, Some(write_params.clone()))
        .await
        .unwrap();

    // Create a new one with *different* dictionary
    let dictionary = Arc::new(StringArray::from(vec!["d", "c"]));
    let indices = Int8Array::from(vec![1, 0, 1]);
    let batches = vec![
        RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(
                Int8DictionaryArray::try_new(indices, dictionary).unwrap(),
            )],
        )
        .unwrap(),
    ];

    // Try write to dataset (fails with legacy format)
    let batches = RecordBatchIterator::new(batches.into_iter().map(Ok), schema.clone());
    let result = Dataset::write(batches, &test_uri, Some(write_params)).await;
    if data_storage_version == LanceFileVersion::Legacy {
        assert!(result.is_err());
    } else {
        assert!(result.is_ok());
    }
}

#[rstest]
#[tokio::test]
async fn overwrite_dataset(
    #[values(LanceFileVersion::Legacy, LanceFileVersion::Stable)]
    data_storage_version: LanceFileVersion,
) {
    let test_uri = TempStrDir::default();

    let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
        "i",
        DataType::Int32,
        false,
    )]));
    let batches = vec![
        RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from_iter_values(0..20))],
        )
        .unwrap(),
    ];

    let mut write_params = WriteParams {
        max_rows_per_file: 40,
        max_rows_per_group: 10,
        data_storage_version: Some(data_storage_version),
        ..Default::default()
    };
    let batches = RecordBatchIterator::new(batches.into_iter().map(Ok), schema.clone());
    let dataset = Dataset::write(batches, &test_uri, Some(write_params.clone()))
        .await
        .unwrap();

    let fragments = dataset.get_fragments();
    assert_eq!(fragments.len(), 1);
    assert_eq!(dataset.manifest.max_fragment_id(), Some(0));

    let new_schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
        "s",
        DataType::Utf8,
        false,
    )]));
    let new_batches = vec![
        RecordBatch::try_new(
            new_schema.clone(),
            vec![Arc::new(StringArray::from_iter_values(
                (20..40).map(|v| v.to_string()),
            ))],
        )
        .unwrap(),
    ];
    write_params.mode = Overwrite;
    let new_batch_reader =
        RecordBatchIterator::new(new_batches.into_iter().map(Ok), new_schema.clone());
    let dataset = Dataset::write(new_batch_reader, &test_uri, Some(write_params.clone()))
        .await
        .unwrap();

    let fragments = dataset.get_fragments();
    assert_eq!(fragments.len(), 1);
    // Fragment ids continue from the dataset's high water mark after an
    // overwrite; they are never reused.
    assert_eq!(fragments[0].id(), 1);
    assert_eq!(dataset.manifest.max_fragment_id(), Some(1));

    let actual_ds = Dataset::open(&test_uri).await.unwrap();
    assert_eq!(actual_ds.version().version, 2);
    let actual_schema = ArrowSchema::from(actual_ds.schema());
    assert_eq!(&actual_schema, new_schema.as_ref());

    let actual_batches = actual_ds
        .scan()
        .try_into_stream()
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let actual_batch = concat_batches(&new_schema, &actual_batches).unwrap();

    assert_eq!(new_schema.clone(), actual_batch.schema());
    let arr = actual_batch.column_by_name("s").unwrap();
    assert_eq!(
        &StringArray::from_iter_values((20..40).map(|v| v.to_string())),
        as_string_array(arr)
    );
    assert_eq!(actual_ds.version().version, 2);

    // But we can still check out the first version
    let first_ver = DatasetBuilder::from_uri(&test_uri)
        .with_version(1)
        .load()
        .await
        .unwrap();
    assert_eq!(first_ver.version().version, 1);
    assert_eq!(&ArrowSchema::from(first_ver.schema()), schema.as_ref());
}

#[rstest]
#[tokio::test]
async fn test_fast_count_rows(
    #[values(LanceFileVersion::Legacy, LanceFileVersion::Stable)]
    data_storage_version: LanceFileVersion,
) {
    let test_uri = TempStrDir::default();

    let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
        "i",
        DataType::Int32,
        false,
    )]));

    let batches: Vec<RecordBatch> = (0..20)
        .map(|i| {
            RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int32Array::from_iter_values(i * 20..(i + 1) * 20))],
            )
            .unwrap()
        })
        .collect();

    let write_params = WriteParams {
        max_rows_per_file: 40,
        max_rows_per_group: 10,
        data_storage_version: Some(data_storage_version),
        ..Default::default()
    };
    let batches = RecordBatchIterator::new(batches.into_iter().map(Ok), schema.clone());
    Dataset::write(batches, &test_uri, Some(write_params))
        .await
        .unwrap();

    let dataset = Dataset::open(&test_uri).await.unwrap();
    dataset.validate().await.unwrap();
    assert_eq!(10, dataset.fragments().len());
    assert_eq!(400, dataset.count_rows(None).await.unwrap());
    assert_eq!(
        200,
        dataset
            .count_rows(Some("i < 200".to_string()))
            .await
            .unwrap()
    );
}

#[rstest]
#[tokio::test]
async fn test_sample_with_fragment_ids(
    #[values(LanceFileVersion::Legacy, LanceFileVersion::Stable)]
    data_storage_version: LanceFileVersion,
) {
    let test_uri = TempStrDir::default();
    let data = gen_batch()
        .col("i", array::step::<Int32Type>())
        .into_reader_rows(RowCount::from(12), BatchCount::from(1));
    let mut dataset = Dataset::write(
        data,
        &test_uri,
        Some(WriteParams {
            max_rows_per_file: 4,
            max_rows_per_group: 2,
            data_storage_version: Some(data_storage_version),
            ..Default::default()
        }),
    )
    .await
    .unwrap();

    dataset.delete("i IN (1, 9)").await.unwrap();

    let projection = dataset.schema().project(&["i"]).unwrap();
    let sampled = dataset
        .sample(8, &projection, Some(&[0, 0, 2]))
        .await
        .unwrap();
    let sampled_values = sampled
        .column_by_name("i")
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap()
        .values()
        .to_vec();

    assert_eq!(sampled_values, vec![0, 2, 3, 8, 10, 11]);
}

#[rstest]
#[tokio::test]
async fn test_sample_with_empty_fragment_ids_rejected(
    #[values(LanceFileVersion::Legacy, LanceFileVersion::Stable)]
    data_storage_version: LanceFileVersion,
) {
    let test_uri = TempStrDir::default();
    let data = gen_batch()
        .col("i", array::step::<Int32Type>())
        .into_reader_rows(RowCount::from(8), BatchCount::from(1));
    let dataset = Dataset::write(
        data,
        &test_uri,
        Some(WriteParams {
            max_rows_per_file: 4,
            max_rows_per_group: 2,
            data_storage_version: Some(data_storage_version),
            ..Default::default()
        }),
    )
    .await
    .unwrap();

    let projection = dataset.schema().project(&["i"]).unwrap();
    let err = dataset.sample(1, &projection, Some(&[])).await.unwrap_err();

    assert!(matches!(err, Error::InvalidInput { .. }));
    assert!(
        err.to_string()
            .contains("does not accept an empty fragment_ids list")
    );
}

#[rstest]
#[tokio::test]
async fn test_sample_with_unknown_fragment_ids_rejected(
    #[values(LanceFileVersion::Legacy, LanceFileVersion::Stable)]
    data_storage_version: LanceFileVersion,
) {
    let test_uri = TempStrDir::default();
    let data = gen_batch()
        .col("i", array::step::<Int32Type>())
        .into_reader_rows(RowCount::from(8), BatchCount::from(1));
    let dataset = Dataset::write(
        data,
        &test_uri,
        Some(WriteParams {
            max_rows_per_file: 4,
            max_rows_per_group: 2,
            data_storage_version: Some(data_storage_version),
            ..Default::default()
        }),
    )
    .await
    .unwrap();

    let projection = dataset.schema().project(&["i"]).unwrap();
    let err = dataset
        .sample(1, &projection, Some(&[0, 999]))
        .await
        .unwrap_err();

    assert!(matches!(err, Error::InvalidInput { .. }));
    assert!(
        err.to_string()
            .contains("not part of the current dataset version")
    );
    assert!(err.to_string().contains("999"));
}

#[rstest]
#[tokio::test]
async fn test_bfloat16_roundtrip(
    #[values(LanceFileVersion::Legacy, LanceFileVersion::Stable)]
    data_storage_version: LanceFileVersion,
) -> Result<()> {
    let inner_field = Arc::new(
        ArrowField::new("item", DataType::FixedSizeBinary(2), true).with_metadata(
            [
                (ARROW_EXT_NAME_KEY.into(), BFLOAT16_EXT_NAME.into()),
                (ARROW_EXT_META_KEY.into(), "".into()),
            ]
            .into(),
        ),
    );
    let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
        "fsl",
        DataType::FixedSizeList(inner_field.clone(), 2),
        false,
    )]));

    let values = bfloat16::BFloat16Array::from_iter_values(
        (0..6).map(|i| i as f32).map(half::bf16::from_f32),
    );
    let vectors = FixedSizeListArray::new(inner_field, 2, Arc::new(values.into_inner()), None);

    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(vectors)]).unwrap();

    let test_uri = TempStrDir::default();

    let dataset = Dataset::write(
        RecordBatchIterator::new(vec![Ok(batch.clone())], schema.clone()),
        &test_uri,
        Some(WriteParams {
            data_storage_version: Some(data_storage_version),
            ..Default::default()
        }),
    )
    .await?;

    let data = dataset.scan().try_into_batch().await?;
    assert_eq!(batch, data);

    Ok(())
}

#[tokio::test]
async fn test_overwrite_mixed_version() {
    let test_uri = TempStrDir::default();

    let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
        "a",
        DataType::Int32,
        false,
    )]));
    let arr = Arc::new(Int32Array::from(vec![1, 2, 3]));

    let data = RecordBatch::try_new(schema.clone(), vec![arr]).unwrap();
    let reader = RecordBatchIterator::new(vec![data.clone()].into_iter().map(Ok), schema.clone());

    let dataset = Dataset::write(
        reader,
        &test_uri,
        Some(WriteParams {
            data_storage_version: Some(LanceFileVersion::Legacy),
            ..Default::default()
        }),
    )
    .await
    .unwrap();

    assert_eq!(
        dataset
            .manifest
            .data_storage_format
            .lance_file_version()
            .unwrap(),
        LanceFileVersion::Legacy
    );

    let reader = RecordBatchIterator::new(vec![data].into_iter().map(Ok), schema);
    let dataset = Dataset::write(
        reader,
        &test_uri,
        Some(WriteParams {
            mode: WriteMode::Overwrite,
            ..Default::default()
        }),
    )
    .await
    .unwrap();

    assert_eq!(
        dataset
            .manifest
            .data_storage_format
            .lance_file_version()
            .unwrap(),
        LanceFileVersion::Legacy
    );
}

#[tokio::test]
async fn test_open_nonexisting_dataset() {
    let temp_dir = TempStdDir::default();
    let dataset_dir = temp_dir.join("non_existing");
    let dataset_uri = dataset_dir.to_str().unwrap();

    let res = Dataset::open(dataset_uri).await;
    assert!(res.is_err());

    assert!(!dataset_dir.exists());
}

#[tokio::test]
async fn test_manifest_partially_fits() {
    // This regresses a bug that occurred when the manifest file was over 4KiB but the manifest
    // itself was less than 4KiB (due to a dictionary).  4KiB is important here because that's the
    // block size we use when reading the "last block"

    let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
        "x",
        DataType::Dictionary(Box::new(DataType::Int16), Box::new(DataType::Utf8)),
        false,
    )]));
    let dictionary = Arc::new(StringArray::from_iter_values(
        (0..1000).map(|i| i.to_string()),
    ));
    let indices = Int16Array::from_iter_values(0..1000);
    let batches = vec![
        RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(
                Int16DictionaryArray::try_new(indices, dictionary.clone()).unwrap(),
            )],
        )
        .unwrap(),
    ];

    let test_uri = TempStrDir::default();
    let batches = RecordBatchIterator::new(batches.into_iter().map(Ok), schema.clone());
    Dataset::write(batches, &test_uri, None).await.unwrap();

    let dataset = Dataset::open(&test_uri).await.unwrap();
    assert_eq!(1000, dataset.count_rows(None).await.unwrap());
}

#[tokio::test]
async fn test_dataset_uri_roundtrips() {
    let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
        "a",
        DataType::Int32,
        false,
    )]));

    let test_uri = TempStrDir::default();
    let vectors = Arc::new(Int32Array::from_iter_values(vec![]));

    let data = RecordBatch::try_new(schema.clone(), vec![vectors]);
    let reader = RecordBatchIterator::new(vec![data.unwrap()].into_iter().map(Ok), schema);
    let dataset = Dataset::write(
        reader,
        &test_uri,
        Some(WriteParams {
            ..Default::default()
        }),
    )
    .await
    .unwrap();

    let uri = dataset.uri();
    assert_eq!(uri, test_uri.as_str());

    let ds2 = Dataset::open(uri).await.unwrap();
    assert_eq!(
        ds2.latest_version_id().await.unwrap(),
        dataset.latest_version_id().await.unwrap()
    );
}

/// A commit handler whose resolve_latest_location always returns an IO error.
/// Used to verify that non-NotFound errors from resolve_latest_location are
/// propagated as-is rather than being wrapped as DatasetNotFound.
#[derive(Debug)]
struct ErroringCommitHandler;

#[async_trait::async_trait]
impl lance_table::io::commit::CommitHandler for ErroringCommitHandler {
    async fn resolve_latest_location(
        &self,
        _base_path: &Path,
        _object_store: &ObjectStore,
    ) -> Result<lance_table::io::commit::ManifestLocation> {
        Err(Error::io("simulated I/O error".to_string()))
    }

    async fn commit(
        &self,
        _manifest: &mut lance_table::format::Manifest,
        _indices: Option<Vec<lance_table::format::IndexMetadata>>,
        _base_path: &Path,
        _object_store: &ObjectStore,
        _manifest_writer: lance_table::io::commit::ManifestWriter,
        _naming_scheme: lance_table::io::commit::ManifestNamingScheme,
        _transaction: Option<lance_table::format::Transaction>,
    ) -> std::result::Result<
        lance_table::io::commit::ManifestLocation,
        lance_table::io::commit::CommitError,
    > {
        unimplemented!()
    }
}

#[tokio::test]
async fn test_open_dataset_non_not_found_error_is_not_masked() {
    // When resolve_latest_location returns an IO error, it should propagate
    // as an IO error, not be wrapped as DatasetNotFound.
    let store = Arc::new(object_store::memory::InMemory::new());
    let location = url::Url::parse("memory://test").unwrap();

    #[allow(deprecated)]
    let result = DatasetBuilder::from_uri("memory://test")
        .with_object_store(store, location, Arc::new(ErroringCommitHandler))
        .load()
        .await;

    let err = result.unwrap_err();
    assert!(
        matches!(err, Error::IO { .. }),
        "Expected IO error but got: {:?}",
        err,
    );
}
