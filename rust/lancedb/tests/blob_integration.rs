// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, BinaryArray, Int64Array, LargeBinaryArray, RecordBatch, StringArray,
    StructArray, UInt64Array,
};
use arrow_schema::{DataType, Field, Fields, Schema};
use futures::TryStreamExt;
use lance::Dataset;
use lance_file::version::LanceFileVersion;
use lancedb::{
    Connection, Error, Result, Table,
    blob::{BlobRangeRequest, blob},
    connect, connect_namespace,
    database::listing::{
        ListingDatabaseOptions, NewTableConfig, OPT_NEW_TABLE_ENABLE_STABLE_ROW_IDS,
    },
    query::{ExecutableQuery, QueryBase},
    table::{AddDataMode, CompactionOptions, OptimizeAction, OptimizeStats},
};
use tempfile::tempdir;

fn blob_table_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        blob("image", true),
    ]))
}

fn binary_input_batch(ids: &[i64], payloads: &[Option<&[u8]>]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("image", DataType::LargeBinary, true),
        ])),
        vec![
            Arc::new(Int64Array::from(ids.to_vec())),
            Arc::new(LargeBinaryArray::from_iter(payloads.iter().copied())),
        ],
    )
    .unwrap()
}

async fn create_inline_blob_table(
    db: &Connection,
    name: &str,
    ids: &[i64],
    payloads: &[Option<&[u8]>],
) -> Result<Table> {
    let table = db
        .create_empty_table(name, blob_table_schema())
        .execute()
        .await?;
    table
        .add(binary_input_batch(ids, payloads))
        .execute()
        .await?;
    Ok(table)
}

async fn storage_format_version(table: &Table) -> LanceFileVersion {
    table
        .as_native()
        .unwrap()
        .manifest()
        .await
        .unwrap()
        .data_storage_format
        .lance_file_version()
        .unwrap()
        .resolve()
}

async fn uses_stable_row_ids(table: &Table) -> bool {
    table
        .as_native()
        .unwrap()
        .manifest()
        .await
        .unwrap()
        .uses_stable_row_ids()
}

async fn query_image_struct(table: &Table) -> StructArray {
    let batches = table
        .query()
        .execute()
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let batch = arrow_select::concat::concat_batches(&batches[0].schema(), &batches).unwrap();
    batch
        .column_by_name("image")
        .expect("image column present")
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("image column is a descriptor struct")
        .clone()
}

#[tokio::test]
async fn declaring_blob_column_bumps_format_and_enables_stable_row_ids() -> Result<()> {
    let tmp = tempdir().unwrap();
    let db = connect(tmp.path().to_str().unwrap()).execute().await?;
    let table = db
        .create_empty_table("t", blob_table_schema())
        .execute()
        .await?;

    assert!(storage_format_version(&table).await >= LanceFileVersion::V2_2);
    assert!(uses_stable_row_ids(&table).await);
    Ok(())
}

#[tokio::test]
async fn explicit_stable_row_id_setting_wins_over_blob_default() -> Result<()> {
    let tmp = tempdir().unwrap();
    let db = connect(tmp.path().to_str().unwrap()).execute().await?;
    let table = db
        .create_empty_table("t", blob_table_schema())
        .storage_option(OPT_NEW_TABLE_ENABLE_STABLE_ROW_IDS, "false")
        .execute()
        .await?;

    assert!(storage_format_version(&table).await >= LanceFileVersion::V2_2);
    assert!(!uses_stable_row_ids(&table).await);
    Ok(())
}

#[tokio::test]
async fn non_blob_table_keeps_default_format_and_row_id_setting() -> Result<()> {
    let tmp = tempdir().unwrap();
    let db = connect(tmp.path().to_str().unwrap()).execute().await?;
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let table = db.create_empty_table("t", schema).execute().await?;

    assert!(storage_format_version(&table).await < LanceFileVersion::V2_2);
    assert!(!uses_stable_row_ids(&table).await);
    Ok(())
}

#[tokio::test]
async fn creating_with_blob_data_bumps_format() -> Result<()> {
    let tmp = tempdir().unwrap();
    let db = connect(tmp.path().to_str().unwrap()).execute().await?;

    let blob_field = blob("image", true);
    let DataType::Struct(children) = blob_field.data_type().clone() else {
        unreachable!("blob field is a struct")
    };
    let image = StructArray::new(
        children,
        vec![
            Arc::new(LargeBinaryArray::from_iter_values([b"payload".as_slice()])),
            Arc::new(StringArray::from(vec![None::<&str>])),
        ],
        None,
    );
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            blob_field,
        ])),
        vec![Arc::new(Int64Array::from(vec![1])), Arc::new(image)],
    )
    .unwrap();
    let table = db.create_table("t", batch).execute().await?;

    assert!(storage_format_version(&table).await >= LanceFileVersion::V2_2);
    assert!(uses_stable_row_ids(&table).await);
    assert_eq!(table.count_rows(None).await?, 1);
    Ok(())
}

#[tokio::test]
async fn add_coerces_large_binary_into_blob_column() -> Result<()> {
    let tmp = tempdir().unwrap();
    let db = connect(tmp.path().to_str().unwrap()).execute().await?;
    let table =
        create_inline_blob_table(&db, "t", &[1, 2], &[Some(b"cat".as_slice()), Some(b"dog")])
            .await?;

    assert_eq!(table.count_rows(None).await?, 2);
    let image = query_image_struct(&table).await;
    assert_eq!(image.len(), 2);
    let schema = table.schema().await?;
    let field = schema.field_with_name("image").unwrap();
    assert_eq!(
        field
            .metadata()
            .get("ARROW:extension:name")
            .map(String::as_str),
        Some("lance.blob.v2")
    );
    Ok(())
}

#[tokio::test]
async fn add_coerces_binary_into_blob_column() -> Result<()> {
    let tmp = tempdir().unwrap();
    let db = connect(tmp.path().to_str().unwrap()).execute().await?;
    let table = db
        .create_empty_table("t", blob_table_schema())
        .execute()
        .await?;

    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("image", DataType::Binary, true),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(BinaryArray::from_iter_values([b"small".as_slice()])),
        ],
    )
    .unwrap();
    table.add(batch).execute().await?;

    assert_eq!(table.count_rows(None).await?, 1);
    Ok(())
}

#[tokio::test]
async fn add_accepts_null_blob_rows() -> Result<()> {
    let tmp = tempdir().unwrap();
    let db = connect(tmp.path().to_str().unwrap()).execute().await?;
    let table = create_inline_blob_table(
        &db,
        "t",
        &[1, 2, 3],
        &[Some(b"first".as_slice()), None, Some(b"third")],
    )
    .await?;

    assert_eq!(table.count_rows(None).await?, 3);
    let image = query_image_struct(&table).await;
    assert_eq!(image.len(), 3);
    Ok(())
}

#[tokio::test]
async fn add_rejects_uncoercible_blob_input() -> Result<()> {
    let tmp = tempdir().unwrap();
    let db = connect(tmp.path().to_str().unwrap()).execute().await?;
    let table = db
        .create_empty_table("t", blob_table_schema())
        .execute()
        .await?;

    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("image", DataType::Utf8, true),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(StringArray::from(vec!["not bytes"])),
        ],
    )
    .unwrap();
    let err = table.add(batch).execute().await.unwrap_err();
    assert!(err.to_string().contains("image"));
    Ok(())
}

#[tokio::test]
async fn connection_level_stable_row_id_setting_wins_over_blob_default() -> Result<()> {
    let tmp = tempdir().unwrap();
    let db = connect(tmp.path().to_str().unwrap())
        .storage_option(OPT_NEW_TABLE_ENABLE_STABLE_ROW_IDS, "false")
        .execute()
        .await?;
    let table = db
        .create_empty_table("t", blob_table_schema())
        .execute()
        .await?;

    assert!(storage_format_version(&table).await >= LanceFileVersion::V2_2);
    assert!(!uses_stable_row_ids(&table).await);
    Ok(())
}

#[tokio::test]
async fn namespace_create_applies_blob_defaults() -> Result<()> {
    let tmp = tempdir().unwrap();
    let mut properties = std::collections::HashMap::new();
    properties.insert("root".to_string(), tmp.path().to_str().unwrap().to_string());
    let db = connect_namespace("dir", properties).execute().await?;
    let table = db
        .create_empty_table("t", blob_table_schema())
        .execute()
        .await?;

    assert!(storage_format_version(&table).await >= LanceFileVersion::V2_2);
    assert!(uses_stable_row_ids(&table).await);
    Ok(())
}

// Overwrite takes the input schema as-is. A raw-binary overwrite drops the blob
// marker; re-declaring blob v2 in the input restores it.
#[tokio::test]
async fn overwrite_replaces_blob_schema_with_input_schema() -> Result<()> {
    let tmp = tempdir().unwrap();
    let db = connect(tmp.path().to_str().unwrap()).execute().await?;
    let table = create_inline_blob_table(&db, "t", &[1], &[Some(b"blob".as_slice())]).await?;

    let raw_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("image", DataType::LargeBinary, true),
    ]));
    let raw_batch = RecordBatch::try_new(
        raw_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![2])),
            Arc::new(LargeBinaryArray::from_iter_values([b"plain".as_slice()])),
        ],
    )
    .unwrap();
    table
        .add(raw_batch)
        .mode(AddDataMode::Overwrite)
        .execute()
        .await?;
    let schema = table.schema().await?;
    assert_eq!(schema, raw_schema);
    assert!(
        !schema
            .field_with_name("image")
            .unwrap()
            .metadata()
            .contains_key("ARROW:extension:name")
    );

    let blob_field = blob("image", true);
    let DataType::Struct(children) = blob_field.data_type().clone() else {
        unreachable!("blob field is a struct")
    };
    let image = StructArray::new(
        children,
        vec![
            Arc::new(LargeBinaryArray::from_iter_values([b"declared".as_slice()])),
            Arc::new(StringArray::from(vec![None::<&str>])),
        ],
        None,
    );
    let declared_batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            blob_field,
        ])),
        vec![Arc::new(Int64Array::from(vec![3])), Arc::new(image)],
    )
    .unwrap();
    table
        .add(declared_batch)
        .mode(AddDataMode::Overwrite)
        .execute()
        .await?;
    let schema = table.schema().await?;
    assert_eq!(
        schema
            .field_with_name("image")
            .unwrap()
            .metadata()
            .get("ARROW:extension:name")
            .map(String::as_str),
        Some("lance.blob.v2")
    );
    Ok(())
}

async fn collect_row_ids(table: &Table) -> Result<Vec<u64>> {
    let batches = table
        .query()
        .with_row_id()
        .execute()
        .await?
        .try_collect::<Vec<_>>()
        .await?;
    let batch = arrow_select::concat::concat_batches(&batches[0].schema(), &batches).unwrap();
    Ok(batch
        .column_by_name("_rowid")
        .unwrap()
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap()
        .values()
        .to_vec())
}

async fn collect_id_rowid(table: &Table) -> Result<Vec<(i64, u64)>> {
    let batches = table
        .query()
        .with_row_id()
        .execute()
        .await?
        .try_collect::<Vec<_>>()
        .await?;
    let batch = arrow_select::concat::concat_batches(&batches[0].schema(), &batches).unwrap();
    let ids = batch
        .column_by_name("id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let row_ids = batch
        .column_by_name("_rowid")
        .unwrap()
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    Ok(ids
        .values()
        .iter()
        .copied()
        .zip(row_ids.values().iter().copied())
        .collect())
}

#[tokio::test]
async fn fetch_blobs_round_trips_bytes() -> Result<()> {
    let tmp = tempdir().unwrap();
    let db = connect(tmp.path().to_str().unwrap()).execute().await?;
    let payload: &[u8] = b"blob-round-trip-payload";
    let table = create_inline_blob_table(&db, "t", &[1], &[Some(payload)]).await?;

    let ids = collect_row_ids(&table).await?;
    let bytes = table.fetch_blobs("image", &ids).await?;
    assert_eq!(bytes.len(), 1);
    assert_eq!(bytes.value(0), payload);
    Ok(())
}

#[tokio::test]
async fn fetch_blobs_round_trips_nested_blob_column() -> Result<()> {
    let tmp = tempdir().unwrap();
    let db = connect(tmp.path().to_str().unwrap()).execute().await?;

    let blob_field = blob("blob", true);
    let DataType::Struct(blob_children) = blob_field.data_type().clone() else {
        unreachable!("blob field is a struct")
    };
    let blob_array = StructArray::new(
        blob_children,
        vec![
            Arc::new(LargeBinaryArray::from_iter_values([
                b"hello".as_slice(),
                b"world".as_slice(),
            ])) as ArrayRef,
            Arc::new(StringArray::from(vec![None::<&str>, None::<&str>])) as ArrayRef,
        ],
        None,
    );
    let info_fields: Fields = vec![Field::new("name", DataType::Utf8, false), blob_field].into();
    let info_array = StructArray::new(
        info_fields.clone(),
        vec![
            Arc::new(StringArray::from(vec!["a", "b"])) as ArrayRef,
            Arc::new(blob_array) as ArrayRef,
        ],
        None,
    );
    let schema = Arc::new(Schema::new(vec![Field::new(
        "info",
        DataType::Struct(info_fields),
        true,
    )]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(info_array) as ArrayRef]).unwrap();
    let table = db.create_table("t", batch).execute().await?;

    assert!(storage_format_version(&table).await >= LanceFileVersion::V2_2);
    assert!(uses_stable_row_ids(&table).await);

    let ids = collect_row_ids(&table).await?;
    let bytes = table.fetch_blobs("info.blob", &ids).await?;
    assert_eq!(bytes.len(), 2);
    let values: std::collections::HashSet<&[u8]> =
        (0..bytes.len()).map(|i| bytes.value(i)).collect();
    assert!(values.contains(b"hello".as_slice()));
    assert!(values.contains(b"world".as_slice()));
    Ok(())
}

#[tokio::test]
async fn blob_columns_lists_nested_dotted_paths() -> Result<()> {
    let tmp = tempdir().unwrap();
    let db = connect(tmp.path().to_str().unwrap()).execute().await?;
    let blob_field = blob("blob", true);
    let info = Field::new(
        "info",
        DataType::Struct(vec![Field::new("name", DataType::Utf8, false), blob_field].into()),
        true,
    );
    let schema = Arc::new(Schema::new(vec![
        blob("thumbnail", true),
        Field::new("id", DataType::Int64, false),
        info,
    ]));
    let table = db.create_empty_table("t", schema).execute().await?;
    assert_eq!(table.blob_columns().await?, vec!["thumbnail", "info.blob"]);
    Ok(())
}

#[tokio::test]
async fn blob_columns_lists_blob_fields_in_order() -> Result<()> {
    let tmp = tempdir().unwrap();
    let db = connect(tmp.path().to_str().unwrap()).execute().await?;
    let schema = Arc::new(Schema::new(vec![
        blob("thumbnail", true),
        Field::new("id", DataType::Int64, false),
        blob("image", true),
    ]));
    let table = db.create_empty_table("t", schema).execute().await?;
    assert_eq!(table.blob_columns().await?, vec!["thumbnail", "image"]);

    let plain = db
        .create_empty_table(
            "plain",
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
        )
        .execute()
        .await?;
    assert!(plain.blob_columns().await?.is_empty());
    Ok(())
}

#[tokio::test]
async fn fetch_blobs_preserves_null_alignment() -> Result<()> {
    let tmp = tempdir().unwrap();
    let db = connect(tmp.path().to_str().unwrap()).execute().await?;
    let table = create_inline_blob_table(
        &db,
        "t",
        &[1, 2, 3, 4],
        &[Some(b"a".as_slice()), None, Some(b"c"), None],
    )
    .await?;

    let pairs = collect_id_rowid(&table).await?;
    let ids: Vec<u64> = pairs.iter().map(|(_, rowid)| *rowid).collect();
    let bytes = table.fetch_blobs("image", &ids).await?;
    assert_eq!(bytes.len(), ids.len());
    for (i, (id, _)) in pairs.iter().enumerate() {
        match id {
            1 => assert_eq!(bytes.value(i), b"a"),
            2 | 4 => assert!(bytes.is_null(i)),
            3 => assert_eq!(bytes.value(i), b"c"),
            _ => unreachable!(),
        }
    }
    Ok(())
}

#[tokio::test]
async fn fetch_blobs_all_null_column_returns_all_nulls() -> Result<()> {
    let tmp = tempdir().unwrap();
    let db = connect(tmp.path().to_str().unwrap()).execute().await?;
    let table = create_inline_blob_table(&db, "t", &[1, 2], &[None, None]).await?;

    let ids = collect_row_ids(&table).await?;
    let bytes = table.fetch_blobs("image", &ids).await?;
    assert_eq!(bytes.len(), 2);
    assert_eq!(bytes.null_count(), 2);

    let files = table.fetch_blob_files("image", &ids).await?;
    assert_eq!(files.len(), 2);
    assert!(files.iter().all(Option::is_none));
    Ok(())
}

#[tokio::test]
async fn fetch_blobs_aligns_with_reordered_and_duplicate_ids() -> Result<()> {
    let tmp = tempdir().unwrap();
    let db = connect(tmp.path().to_str().unwrap()).execute().await?;
    let table = create_inline_blob_table(
        &db,
        "t",
        &[1, 2, 3],
        &[Some(b"one".as_slice()), Some(b"two"), Some(b"three")],
    )
    .await?;

    let pairs = collect_id_rowid(&table).await?;
    let by_id = |want: i64| pairs.iter().find(|(id, _)| *id == want).unwrap().1;
    let request = vec![by_id(3), by_id(1), by_id(3), by_id(2)];
    let bytes = table.fetch_blobs("image", &request).await?;
    assert_eq!(bytes.len(), 4);
    assert_eq!(bytes.value(0), b"three");
    assert_eq!(bytes.value(1), b"one");
    assert_eq!(bytes.value(2), b"three");
    assert_eq!(bytes.value(3), b"two");
    Ok(())
}

#[tokio::test]
async fn fetch_blob_ranges_aligns_repeated_ranges_and_nulls() -> Result<()> {
    let tmp = tempdir().unwrap();
    let db = connect(tmp.path().to_str().unwrap()).execute().await?;
    let table =
        create_inline_blob_table(&db, "t", &[1, 2], &[Some(b"abcdefghij".as_slice()), None])
            .await?;

    let pairs = collect_id_rowid(&table).await?;
    let by_id = |want: i64| pairs.iter().find(|(id, _)| *id == want).unwrap().1;
    let requests = [
        BlobRangeRequest::new(by_id(1), 2, 3),
        BlobRangeRequest::new(by_id(2), 0, 0),
        BlobRangeRequest::new(by_id(1), 0, 2),
        BlobRangeRequest::new(by_id(1), 2, 3),
        BlobRangeRequest::new(by_id(1), 10, 0),
    ];
    let bytes = table.fetch_blob_ranges("image", requests).await?;

    assert_eq!(bytes.len(), requests.len());
    assert_eq!(bytes.value(0), b"cde");
    assert!(bytes.is_null(1));
    assert_eq!(bytes.value(2), b"ab");
    assert_eq!(bytes.value(3), b"cde");
    assert_eq!(bytes.value(4), b"");
    Ok(())
}

#[tokio::test]
async fn fetch_blob_ranges_validates_requests() -> Result<()> {
    let tmp = tempdir().unwrap();
    let db = connect(tmp.path().to_str().unwrap()).execute().await?;
    let table = create_inline_blob_table(&db, "t", &[1], &[Some(b"abc".as_slice())]).await?;
    let row_id = collect_row_ids(&table).await?[0];

    let err = table
        .fetch_blob_ranges("image", [BlobRangeRequest::new(row_id, 2, 2)])
        .await
        .unwrap_err();
    assert!(err.to_string().contains("exceeds blob size"));

    let err = table
        .fetch_blob_ranges("image", [BlobRangeRequest::new(row_id, u64::MAX, 1)])
        .await
        .unwrap_err();
    assert!(err.to_string().contains("offset + length overflowed"));

    let err = table
        .fetch_blob_ranges("image", [BlobRangeRequest::new(u64::MAX, 0, 1)])
        .await
        .unwrap_err();
    assert!(matches!(&err, Error::InvalidInput { .. }), "got {err:?}");
    assert!(err.to_string().contains("row IDs"));
    Ok(())
}

#[tokio::test]
async fn fetch_blob_ranges_empty_requests_returns_empty_array() -> Result<()> {
    let tmp = tempdir().unwrap();
    let db = connect(tmp.path().to_str().unwrap()).execute().await?;
    let table = create_inline_blob_table(&db, "t", &[1], &[Some(b"x".as_slice())]).await?;

    let bytes = table.fetch_blob_ranges("image", std::iter::empty()).await?;
    assert!(bytes.is_empty());
    Ok(())
}

#[tokio::test]
async fn fetch_blobs_empty_ids_returns_empty() -> Result<()> {
    let tmp = tempdir().unwrap();
    let db = connect(tmp.path().to_str().unwrap()).execute().await?;
    let table = create_inline_blob_table(&db, "t", &[1], &[Some(b"x".as_slice())]).await?;

    assert_eq!(table.fetch_blobs("image", &[]).await?.len(), 0);
    assert!(table.fetch_blob_files("image", &[]).await?.is_empty());
    Ok(())
}

#[tokio::test]
async fn fetch_blobs_out_of_range_id_errors_without_panic() -> Result<()> {
    let tmp = tempdir().unwrap();
    let db = connect(tmp.path().to_str().unwrap()).execute().await?;
    let table = create_inline_blob_table(&db, "t", &[1], &[Some(b"x".as_slice())]).await?;

    let err = table.fetch_blobs("image", &[u64::MAX]).await.unwrap_err();
    assert!(err.to_string().contains("row IDs"));
    Ok(())
}

#[tokio::test]
async fn fetch_blob_apis_reject_mixed_valid_and_missing_row_ids() -> Result<()> {
    let tmp = tempdir().unwrap();
    let db = connect(tmp.path().to_str().unwrap()).execute().await?;
    let table = create_inline_blob_table(&db, "t", &[1], &[Some(b"x".as_slice())]).await?;
    let row_id = collect_row_ids(&table).await?[0];
    let row_ids = [u64::MAX, row_id];

    let err = table.fetch_blobs("image", &row_ids).await.unwrap_err();
    assert!(matches!(&err, Error::InvalidInput { .. }), "got {err:?}");
    assert!(err.to_string().contains("row IDs"));

    let err = table.fetch_blob_files("image", &row_ids).await.unwrap_err();
    assert!(matches!(&err, Error::InvalidInput { .. }), "got {err:?}");
    assert!(err.to_string().contains("row IDs"));

    let requests = row_ids.map(|row_id| BlobRangeRequest::new(row_id, 0, 1));
    let err = table
        .fetch_blob_ranges("image", requests)
        .await
        .unwrap_err();
    assert!(matches!(&err, Error::InvalidInput { .. }), "got {err:?}");
    assert!(err.to_string().contains("row IDs"));
    Ok(())
}

#[tokio::test]
async fn fetch_blobs_rejects_non_blob_column() -> Result<()> {
    let tmp = tempdir().unwrap();
    let db = connect(tmp.path().to_str().unwrap()).execute().await?;
    let table = create_inline_blob_table(&db, "t", &[1], &[Some(b"x".as_slice())]).await?;

    let err = table.fetch_blobs("id", &[0]).await.unwrap_err();
    assert!(matches!(err, Error::InvalidInput { .. }));
    assert!(err.to_string().contains("'id' is not a blob column"));

    let err = table.fetch_blob_files("id", &[0]).await.unwrap_err();
    assert!(err.to_string().contains("'id' is not a blob column"));
    Ok(())
}

#[tokio::test]
async fn fetch_blobs_rejects_unknown_column() -> Result<()> {
    let tmp = tempdir().unwrap();
    let db = connect(tmp.path().to_str().unwrap()).execute().await?;
    let table = create_inline_blob_table(&db, "t", &[1], &[Some(b"x".as_slice())]).await?;

    let err = table.fetch_blobs("missing", &[0]).await.unwrap_err();
    assert!(err.to_string().contains("no column named 'missing'"));
    Ok(())
}

#[tokio::test]
async fn fetch_blobs_rejects_legacy_v1_blob_column() -> Result<()> {
    let tmp = tempdir().unwrap();
    let db = connect(tmp.path().to_str().unwrap()).execute().await?;
    let legacy = Field::new("image", DataType::LargeBinary, true).with_metadata(
        std::collections::HashMap::from([("lance-encoding:blob".to_string(), "true".to_string())]),
    );
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        legacy,
    ]));
    let table = db.create_empty_table("t", schema).execute().await?;

    let err = table.fetch_blobs("image", &[0]).await.unwrap_err();
    assert!(err.to_string().contains("legacy blob column"));
    Ok(())
}

#[tokio::test]
async fn fetch_blob_files_reads_lazily_and_aligns_nulls() -> Result<()> {
    let tmp = tempdir().unwrap();
    let db = connect(tmp.path().to_str().unwrap()).execute().await?;
    let table =
        create_inline_blob_table(&db, "t", &[1, 2], &[Some(b"lazy-bytes".as_slice()), None])
            .await?;

    let pairs = collect_id_rowid(&table).await?;
    let ids: Vec<u64> = pairs.iter().map(|(_, rowid)| *rowid).collect();
    let files = table.fetch_blob_files("image", &ids).await?;
    assert_eq!(files.len(), 2);
    for ((id, _), file) in pairs.iter().zip(&files) {
        match id {
            1 => {
                let handle = file.as_ref().unwrap();
                assert_eq!(handle.read().await.unwrap().as_ref(), b"lazy-bytes");
            }
            2 => assert!(file.is_none()),
            _ => unreachable!(),
        }
    }
    Ok(())
}

#[tokio::test]
async fn fetch_blobs_reads_multiple_blob_columns_independently() -> Result<()> {
    let tmp = tempdir().unwrap();
    let db = connect(tmp.path().to_str().unwrap()).execute().await?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        blob("image", true),
        blob("thumbnail", true),
    ]));
    let table = db.create_empty_table("t", schema).execute().await?;
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("image", DataType::LargeBinary, true),
            Field::new("thumbnail", DataType::LargeBinary, true),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(LargeBinaryArray::from_iter(vec![
                Some(b"image-1".as_slice()),
                None,
            ])),
            Arc::new(LargeBinaryArray::from_iter(vec![
                None,
                Some(b"thumb-2".as_slice()),
            ])),
        ],
    )
    .unwrap();
    table.add(batch).execute().await?;

    let pairs = collect_id_rowid(&table).await?;
    let ids: Vec<u64> = pairs.iter().map(|(_, rowid)| *rowid).collect();
    let images = table.fetch_blobs("image", &ids).await?;
    let thumbs = table.fetch_blobs("thumbnail", &ids).await?;
    for (i, (id, _)) in pairs.iter().enumerate() {
        match id {
            1 => {
                assert_eq!(images.value(i), b"image-1");
                assert!(thumbs.is_null(i));
            }
            2 => {
                assert!(images.is_null(i));
                assert_eq!(thumbs.value(i), b"thumb-2");
            }
            _ => unreachable!(),
        }
    }
    Ok(())
}

#[tokio::test]
async fn fetch_blobs_spans_fragments() -> Result<()> {
    let tmp = tempdir().unwrap();
    let db = connect(tmp.path().to_str().unwrap()).execute().await?;
    let table = create_inline_blob_table(&db, "t", &[1], &[Some(b"frag-one".as_slice())]).await?;
    table
        .add(binary_input_batch(&[2], &[Some(b"frag-two".as_slice())]))
        .execute()
        .await?;

    let pairs = collect_id_rowid(&table).await?;
    let ids: Vec<u64> = pairs.iter().map(|(_, rowid)| *rowid).collect();
    let bytes = table.fetch_blobs("image", &ids).await?;
    for (i, (id, _)) in pairs.iter().enumerate() {
        match id {
            1 => assert_eq!(bytes.value(i), b"frag-one"),
            2 => assert_eq!(bytes.value(i), b"frag-two"),
            _ => unreachable!(),
        }
    }
    Ok(())
}

#[tokio::test]
async fn fetch_blobs_packed_payload_round_trip() -> Result<()> {
    let tmp = tempdir().unwrap();
    let db = connect(tmp.path().to_str().unwrap()).execute().await?;
    let big = vec![0xAB_u8; 100 * 1024];
    let small = b"small".to_vec();
    let table = create_inline_blob_table(
        &db,
        "t",
        &[1, 2],
        &[Some(big.as_slice()), Some(small.as_slice())],
    )
    .await?;

    let pairs = collect_id_rowid(&table).await?;
    let ids: Vec<u64> = pairs.iter().map(|(_, rowid)| *rowid).collect();
    let bytes = table.fetch_blobs("image", &ids).await?;
    for (i, (id, _)) in pairs.iter().enumerate() {
        match id {
            1 => assert_eq!(bytes.value(i), big.as_slice()),
            2 => assert_eq!(bytes.value(i), small.as_slice()),
            _ => unreachable!(),
        }
    }
    Ok(())
}

#[tokio::test]
async fn fetch_blobs_after_delete() -> Result<()> {
    let tmp = tempdir().unwrap();
    let db = connect(tmp.path().to_str().unwrap()).execute().await?;
    let table = create_inline_blob_table(
        &db,
        "t",
        &[1, 2, 3],
        &[Some(b"one".as_slice()), Some(b"two"), Some(b"three")],
    )
    .await?;

    table.delete("id = 2").await?;
    let pairs = collect_id_rowid(&table).await?;
    assert_eq!(pairs.len(), 2);
    let ids: Vec<u64> = pairs.iter().map(|(_, rowid)| *rowid).collect();
    let bytes = table.fetch_blobs("image", &ids).await?;
    for (i, (id, _)) in pairs.iter().enumerate() {
        match id {
            1 => assert_eq!(bytes.value(i), b"one"),
            3 => assert_eq!(bytes.value(i), b"three"),
            _ => unreachable!(),
        }
    }
    Ok(())
}

#[tokio::test]
async fn fetch_blobs_with_precompaction_row_ids_survives_compaction() -> Result<()> {
    let tmp = tempdir().unwrap();
    let db = connect(tmp.path().to_str().unwrap()).execute().await?;
    let table = create_inline_blob_table(&db, "t", &[1], &[Some(b"frag-one".as_slice())]).await?;
    table
        .add(binary_input_batch(&[2], &[Some(b"frag-two".as_slice())]))
        .execute()
        .await?;

    let pairs_before = collect_id_rowid(&table).await?;
    let ids_before: Vec<u64> = pairs_before.iter().map(|(_, rowid)| *rowid).collect();

    table
        .optimize(OptimizeAction::Compact {
            options: CompactionOptions::default(),
            remap_options: None,
        })
        .await?;

    let bytes_after = table.fetch_blobs("image", &ids_before).await?;
    assert_eq!(bytes_after.len(), 2);
    for (i, (id, _)) in pairs_before.iter().enumerate() {
        match id {
            1 => assert_eq!(bytes_after.value(i), b"frag-one"),
            2 => assert_eq!(bytes_after.value(i), b"frag-two"),
            _ => unreachable!(),
        }
    }

    let ranges = ids_before
        .iter()
        .map(|row_id| BlobRangeRequest::new(*row_id, 5, 3));
    let ranges_after = table.fetch_blob_ranges("image", ranges).await?;
    assert_eq!(ranges_after.len(), 2);
    for (i, (id, _)) in pairs_before.iter().enumerate() {
        match id {
            1 => assert_eq!(ranges_after.value(i), b"one"),
            2 => assert_eq!(ranges_after.value(i), b"two"),
            _ => unreachable!(),
        }
    }
    Ok(())
}

#[tokio::test]
async fn empty_blob_reads_back_as_empty_bytes() -> Result<()> {
    let tmp = tempdir().unwrap();
    let db = connect(tmp.path().to_str().unwrap()).execute().await?;
    let table = create_inline_blob_table(&db, "t", &[1], &[Some(b"".as_slice())]).await?;

    let ids = collect_row_ids(&table).await?;
    let bytes = table.fetch_blobs("image", &ids).await?;
    assert_eq!(bytes.len(), 1);
    assert!(!bytes.is_null(0));
    assert!(bytes.value(0).is_empty());
    Ok(())
}

const DEDICATED_BLOB_LEN: usize = 64 * 1024;
const SCRAMBLED_LOGICAL_IDS: [i64; 7] = [6, 3, 1, 4, 6, 2, 5];

fn dedicated_blob_bytes(tag: u8) -> Vec<u8> {
    vec![tag; DEDICATED_BLOB_LEN]
}

async fn multi_fragment_dedicated_blob_table(db: &Connection) -> Result<Table> {
    let rows: [(i64, Option<u8>); 6] = [
        (1, Some(1)),
        (2, Some(2)),
        (3, None),
        (4, Some(4)),
        (5, None),
        (6, Some(6)),
    ];
    let mut table: Option<Table> = None;
    for (logical_id, blob_tag) in rows {
        let bytes = blob_tag.map(dedicated_blob_bytes);
        let image = [bytes.as_deref()];
        table = Some(match table {
            None => create_inline_blob_table(db, "t", &[logical_id], &image).await?,
            Some(t) => {
                t.add(binary_input_batch(&[logical_id], &image))
                    .execute()
                    .await?;
                t
            }
        });
    }
    Ok(table.unwrap())
}

async fn row_ids_for_logical(table: &Table, logical_ids: &[i64]) -> Result<Vec<u64>> {
    let id_rowid = collect_id_rowid(table).await?;
    Ok(logical_ids
        .iter()
        .map(|logical_id| {
            id_rowid
                .iter()
                .find(|(id, _)| id == logical_id)
                .map(|(_, row_id)| *row_id)
                .unwrap()
        })
        .collect())
}

#[tokio::test]
async fn fetch_blobs_aligns_across_fragments_with_nulls_and_dups() -> Result<()> {
    let tmp = tempdir().unwrap();
    let db = connect(tmp.path().to_str().unwrap()).execute().await?;
    let table = multi_fragment_dedicated_blob_table(&db).await?;
    let row_ids = row_ids_for_logical(&table, &SCRAMBLED_LOGICAL_IDS).await?;

    let bytes = table.fetch_blobs("image", &row_ids).await?;
    assert_eq!(bytes.len(), SCRAMBLED_LOGICAL_IDS.len());
    for (slot, logical_id) in SCRAMBLED_LOGICAL_IDS.iter().enumerate() {
        match logical_id {
            3 | 5 => assert!(bytes.is_null(slot)),
            id => assert_eq!(
                bytes.value(slot),
                dedicated_blob_bytes(*id as u8).as_slice()
            ),
        }
    }
    Ok(())
}

#[tokio::test]
async fn fetch_blob_ranges_aligns_across_fragments_with_nulls_and_dups() -> Result<()> {
    let tmp = tempdir().unwrap();
    let db = connect(tmp.path().to_str().unwrap()).execute().await?;
    let table = multi_fragment_dedicated_blob_table(&db).await?;
    let row_ids = row_ids_for_logical(&table, &SCRAMBLED_LOGICAL_IDS).await?;
    let requests = row_ids
        .iter()
        .map(|row_id| BlobRangeRequest::new(*row_id, 123, 8));

    let bytes = table.fetch_blob_ranges("image", requests).await?;
    assert_eq!(bytes.len(), SCRAMBLED_LOGICAL_IDS.len());
    for (slot, logical_id) in SCRAMBLED_LOGICAL_IDS.iter().enumerate() {
        match logical_id {
            3 | 5 => assert!(bytes.is_null(slot)),
            id => assert_eq!(bytes.value(slot), [*id as u8; 8]),
        }
    }
    Ok(())
}

#[tokio::test]
async fn fetch_blob_files_aligns_across_fragments_with_nulls_and_dups() -> Result<()> {
    let tmp = tempdir().unwrap();
    let db = connect(tmp.path().to_str().unwrap()).execute().await?;
    let table = multi_fragment_dedicated_blob_table(&db).await?;
    let row_ids = row_ids_for_logical(&table, &SCRAMBLED_LOGICAL_IDS).await?;

    let files = table.fetch_blob_files("image", &row_ids).await?;
    assert_eq!(files.len(), SCRAMBLED_LOGICAL_IDS.len());
    for (slot, logical_id) in SCRAMBLED_LOGICAL_IDS.iter().enumerate() {
        match logical_id {
            3 | 5 => assert!(files[slot].is_none()),
            id => {
                let payload = files[slot].as_ref().unwrap().read().await?;
                assert_eq!(payload.as_ref(), dedicated_blob_bytes(*id as u8).as_slice());
            }
        }
    }
    Ok(())
}

/// Rows exercising the null/empty interleavings from
/// <https://github.com/lancedb/lancedb/issues/3744>: a payload, a null, a valid
/// empty value, then payloads whose descriptors a fragment rewrite used to zero.
fn null_empty_input_batch() -> RecordBatch {
    let owned = [
        Some(dedicated_blob_bytes(1)),
        None,
        Some(Vec::new()),
        Some(dedicated_blob_bytes(4)),
        Some(dedicated_blob_bytes(5)),
        Some(dedicated_blob_bytes(6)),
    ];
    let payloads: Vec<Option<&[u8]>> = owned.iter().map(|payload| payload.as_deref()).collect();
    binary_input_batch(&[1, 2, 3, 4, 5, 6], &payloads)
}

/// One `(id, Some((payload length, first byte)))` per live row, or `(id, None)`
/// for a null blob.  Comparing lengths and first bytes keeps failure output
/// readable where comparing whole payloads would not.
type BlobSummary = Vec<(i64, Option<(usize, Option<u8>)>)>;

/// The rows [`null_empty_input_batch`] leaves behind after `id IN (1, 4)` is
/// deleted: a null, a valid empty value, and the two payloads that follow them.
fn expected_null_empty_survivors() -> BlobSummary {
    vec![
        (2, None),
        (3, Some((0, None))),
        (5, Some((DEDICATED_BLOB_LEN, Some(5)))),
        (6, Some((DEDICATED_BLOB_LEN, Some(6)))),
    ]
}

/// `optimize()` only rewrites a fragment when lance's compaction planner selects
/// it — here because the delete pushes the fragment past
/// `materialize_deletions_threshold` (0.1 by default; these tests delete 2 of 6
/// rows). Without this check, a planner or threshold change upstream would leave
/// both regression tests green while no rewrite happened at all.
fn assert_compacted(stats: &OptimizeStats) {
    let metrics = stats
        .compaction
        .as_ref()
        .expect("OptimizeAction::All runs compaction");
    assert!(
        metrics.fragments_removed >= 1,
        "optimize() rewrote no fragment, so this test proves nothing: {metrics:?}"
    );
}

fn summarize(rows: &[(i64, Option<Vec<u8>>)]) -> BlobSummary {
    rows.iter()
        .map(|(id, payload)| {
            (
                *id,
                payload
                    .as_ref()
                    .map(|bytes| (bytes.len(), bytes.first().copied())),
            )
        })
        .collect()
}

async fn sorted_id_rowid(table: &Table) -> Result<Vec<(i64, u64)>> {
    let mut pairs = collect_id_rowid(table).await?;
    pairs.sort_by_key(|(id, _)| *id);
    Ok(pairs)
}

/// `{position, size}` descriptors of a legacy v1 blob column, keyed by `id`.
async fn v1_blob_descriptors(table: &Table) -> Result<Vec<(i64, Option<(u64, u64)>)>> {
    let batches = table
        .query()
        .execute()
        .await?
        .try_collect::<Vec<_>>()
        .await?;
    let batch = arrow_select::concat::concat_batches(&batches[0].schema(), &batches).unwrap();
    let ids = batch
        .column_by_name("id")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let descriptors = batch
        .column_by_name("image")
        .unwrap()
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("v1 blob column reads back as a descriptor struct");
    let position = descriptors
        .column_by_name("position")
        .unwrap()
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    let size = descriptors
        .column_by_name("size")
        .unwrap()
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap();
    let mut rows: Vec<(i64, Option<(u64, u64)>)> = (0..batch.num_rows())
        .map(|row| {
            let descriptor =
                (!descriptors.is_null(row)).then(|| (position.value(row), size.value(row)));
            (ids.value(row), descriptor)
        })
        .collect();
    rows.sort_by_key(|(id, _)| *id);
    Ok(rows)
}

/// Payload bytes of every live row of a legacy v1 blob column, keyed by `id`.
/// [`Table::fetch_blobs`] rejects v1 columns, so read them through lance.
async fn v1_blob_payloads(dataset_uri: &str, table: &Table) -> Result<Vec<(i64, Option<Vec<u8>>)>> {
    let pairs = sorted_id_rowid(table).await?;
    let row_ids: Vec<u64> = pairs.iter().map(|(_, row_id)| *row_id).collect();
    let dataset = Arc::new(Dataset::open(dataset_uri).await?);
    let files = dataset.take_blobs(&row_ids, "image").await?;
    assert_eq!(
        files.len(),
        pairs.len(),
        "take_blobs returned {} handles for {} live rows",
        files.len(),
        pairs.len()
    );
    let mut rows = Vec::with_capacity(pairs.len());
    for ((id, _), file) in pairs.iter().zip(files) {
        let payload = match file {
            Some(file) => Some(file.read().await?.to_vec()),
            None => None,
        };
        rows.push((*id, payload));
    }
    Ok(rows)
}

/// Length and first byte of every live blob v2 value, keyed by `id`.
async fn blob_v2_values(table: &Table) -> Result<BlobSummary> {
    let pairs = sorted_id_rowid(table).await?;
    let row_ids: Vec<u64> = pairs.iter().map(|(_, row_id)| *row_id).collect();
    let bytes = table.fetch_blobs("image", &row_ids).await?;
    Ok(pairs
        .iter()
        .enumerate()
        .map(|(slot, (id, _))| {
            let value = (!bytes.is_null(slot))
                .then(|| (bytes.value(slot).len(), bytes.value(slot).first().copied()));
            (*id, value)
        })
        .collect())
}

/// Regression test for [#3744]: on storage 2.0 (legacy v1 descriptors),
/// compaction rewrote every payload following a null or empty value in the same
/// fragment as `{position: 0, size: 0}`, so the payload bytes read back as `b""`
/// and the new fragment no longer referenced them at all.
///
/// [#3744]: https://github.com/lancedb/lancedb/issues/3744
#[tokio::test]
async fn optimize_preserves_v1_blob_payloads_with_null_and_empty() -> Result<()> {
    let tmp = tempdir().unwrap();
    let db_uri = tmp.path().to_str().unwrap().to_string();
    let db = connect(&db_uri)
        .database_options(&ListingDatabaseOptions {
            new_table_config: NewTableConfig {
                data_storage_version: Some(LanceFileVersion::V2_0),
                ..Default::default()
            },
            ..Default::default()
        })
        .execute()
        .await?;
    let legacy = Field::new("image", DataType::LargeBinary, true).with_metadata(
        std::collections::HashMap::from([("lance-encoding:blob".to_string(), "true".to_string())]),
    );
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        legacy,
    ]));
    let table = db.create_empty_table("t", schema).execute().await?;
    table.add(null_empty_input_batch()).execute().await?;
    assert_eq!(
        storage_format_version(&table).await,
        LanceFileVersion::V2_0.resolve(),
        "v1 blob descriptors only exist below storage 2.2"
    );
    let dataset_uri = table.uri().await?;

    // Any rewrite triggers it; deleting rows is the shape from the issue.
    table.delete("id IN (1, 4)").await?;
    let descriptors_before = v1_blob_descriptors(&table).await?;
    let before = v1_blob_payloads(&dataset_uri, &table).await?;
    assert_eq!(
        summarize(&before),
        expected_null_empty_survivors(),
        "test setup no longer produces the null/empty/payload mix"
    );

    let stats = table.optimize(OptimizeAction::All).await?;
    assert_compacted(&stats);

    let descriptors_after = v1_blob_descriptors(&table).await?;
    let after = v1_blob_payloads(&dataset_uri, &table).await?;
    assert_eq!(
        summarize(&after),
        summarize(&before),
        "optimize() lost blob payloads; descriptors before={descriptors_before:?} after={descriptors_after:?}"
    );
    assert!(after == before, "optimize() changed blob payload bytes");
    Ok(())
}

/// Regression test for the blob v2 half of [#3744]: compaction rewrote a valid
/// empty value as null, destroying the null-vs-empty distinction.
///
/// [#3744]: https://github.com/lancedb/lancedb/issues/3744
#[tokio::test]
async fn optimize_preserves_blob_v2_null_and_empty_distinction() -> Result<()> {
    let tmp = tempdir().unwrap();
    let db = connect(tmp.path().to_str().unwrap()).execute().await?;
    let table = db
        .create_empty_table("t", blob_table_schema())
        .execute()
        .await?;
    table.add(null_empty_input_batch()).execute().await?;
    assert!(
        storage_format_version(&table).await >= LanceFileVersion::V2_2,
        "blob v2 columns require storage >= 2.2"
    );

    table.delete("id IN (1, 4)").await?;
    let before = blob_v2_values(&table).await?;
    assert_eq!(
        before,
        expected_null_empty_survivors(),
        "test setup no longer produces the null/empty/payload mix"
    );

    let stats = table.optimize(OptimizeAction::All).await?;
    assert_compacted(&stats);

    assert_eq!(
        blob_v2_values(&table).await?,
        before,
        "optimize() changed blob v2 values"
    );
    Ok(())
}
