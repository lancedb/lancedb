// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Integration tests for blob v2 columns.

use std::sync::Arc;

use arrow_array::{Array, BinaryArray, Int64Array, LargeBinaryArray, RecordBatch, StructArray};
use arrow_schema::{DataType, Field, Schema};
use futures::TryStreamExt;
use lance_encoding::version::LanceFileVersion;
use lancedb::{
    Connection, Result, Table, blob::blob, connect,
    database::listing::OPT_NEW_TABLE_ENABLE_STABLE_ROW_IDS, query::ExecutableQuery,
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
        .expect("blob column reads back as a descriptor struct")
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

    assert!(
        storage_format_version(&table).await >= LanceFileVersion::V2_2,
        "format bump still applies; the schema cannot be written below 2.2"
    );
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

    // Batch already declares the blob field (pre-built struct).
    let blob_field = blob("image", true);
    let DataType::Struct(children) = blob_field.data_type().clone() else {
        unreachable!("blob field is a struct")
    };
    let image = StructArray::new(
        children,
        vec![
            Arc::new(LargeBinaryArray::from_iter_values([b"payload".as_slice()])),
            Arc::new(arrow_array::StringArray::from(vec![None::<&str>])),
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
    // Table schema still has the blob marker after append.
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
            Arc::new(arrow_array::StringArray::from(vec!["not bytes"])),
        ],
    )
    .unwrap();
    let err = table.add(batch).execute().await.unwrap_err();
    assert!(err.to_string().contains("image"), "got: {err}");
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
    let db = lancedb::connect_namespace("dir", properties)
        .execute()
        .await?;
    let table = db
        .create_empty_table("t", blob_table_schema())
        .execute()
        .await?;

    assert!(storage_format_version(&table).await >= LanceFileVersion::V2_2);
    assert!(uses_stable_row_ids(&table).await);
    Ok(())
}

// Overwrite takes the input schema as-is (same as cast skip). Raw binary
// overwrite drops the blob marker unless the input declares blob v2.
#[tokio::test]
async fn overwrite_replaces_blob_schema_with_input_schema() -> Result<()> {
    use lancedb::table::AddDataMode;

    let tmp = tempdir().unwrap();
    let db = connect(tmp.path().to_str().unwrap()).execute().await?;
    let table = create_inline_blob_table(&db, "t", &[1], &[Some(b"blob".as_slice())]).await?;

    // Raw binary overwrite. Plain LargeBinary replaces the blob declaration.
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
            .contains_key("ARROW:extension:name"),
        "raw binary overwrite leaves a plain binary column"
    );

    // Overwrite with a declared blob struct keeps the blob column.
    let blob_field = blob("image", true);
    let DataType::Struct(children) = blob_field.data_type().clone() else {
        unreachable!("blob field is a struct")
    };
    let image = StructArray::new(
        children,
        vec![
            Arc::new(LargeBinaryArray::from_iter_values([b"declared".as_slice()])),
            Arc::new(arrow_array::StringArray::from(vec![None::<&str>])),
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
