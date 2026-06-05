// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Integration tests for Lance blob v2 columns through the public LanceDB API.

use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, Int64Array, LargeBinaryArray, RecordBatch, StringArray, StructArray,
};
use arrow_schema::{DataType, Field, Schema};
use futures::TryStreamExt;
use lancedb::{
    Error, Result, Table,
    blob::blob,
    connect,
    query::{ExecutableQuery, QueryBase},
    table::{CompactionOptions, OptimizeAction},
};
use tempfile::tempdir;

async fn create_inline_blob_table(
    db: &lancedb::Connection,
    name: &str,
    ids: &[i64],
    payloads: &[Option<&[u8]>],
) -> Result<Table> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        blob("image", true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("image", DataType::LargeBinary, true),
        ])),
        vec![
            Arc::new(Int64Array::from(ids.to_vec())),
            Arc::new(LargeBinaryArray::from_iter(payloads.iter().copied())),
        ],
    )
    .unwrap();
    let table = db.create_empty_table(name, schema).execute().await?;
    table.add(batch).execute().await?;
    Ok(table)
}

async fn collect_row_ids(table: &Table) -> Result<Vec<u64>> {
    let rows = table
        .query()
        .with_row_id()
        .execute()
        .await?
        .try_collect::<Vec<_>>()
        .await?;
    let batch = arrow_select::concat::concat_batches(&rows[0].schema(), &rows).unwrap();
    Ok(batch
        .column_by_name("_rowid")
        .expect("_rowid")
        .as_any()
        .downcast_ref::<arrow_array::UInt64Array>()
        .unwrap()
        .values()
        .to_vec())
}

#[tokio::test]
async fn inline_blob_round_trip_bumps_format_to_v2_2() -> Result<()> {
    let tmp = tempdir().unwrap();
    let db = connect(tmp.path().to_str().unwrap()).execute().await?;
    let payload: &[u8] = b"blob-round-trip-payload";
    let table = create_inline_blob_table(&db, "t", &[1], &[Some(payload)]).await?;

    let ids = collect_row_ids(&table).await?;
    let bytes = table.take_blobs("image", &ids).await?;
    assert_eq!(bytes.value(0), payload);

    let manifest = table.as_native().unwrap().manifest().await?;
    let version = manifest
        .data_storage_format
        .lance_file_version()
        .unwrap()
        .resolve();
    assert!(version >= lance_encoding::version::LanceFileVersion::V2_2);
    Ok(())
}

#[tokio::test]
async fn take_blobs_rejects_non_blob_column() -> Result<()> {
    let tmp = tempdir().unwrap();
    let db = connect(tmp.path().to_str().unwrap()).execute().await?;
    let table = create_inline_blob_table(&db, "t", &[1], &[Some(b"x".as_slice())]).await?;
    let err = table.take_blobs("id", &[0]).await.unwrap_err();
    assert!(
        matches!(err, Error::InvalidInput { .. }),
        "expected InvalidInput, got {err:?}"
    );
    assert!(err.to_string().contains("not a blob column"));
    Ok(())
}

#[tokio::test]
async fn take_blobs_rejects_unknown_column() -> Result<()> {
    let tmp = tempdir().unwrap();
    let db = connect(tmp.path().to_str().unwrap()).execute().await?;
    let table = create_inline_blob_table(&db, "t", &[1], &[Some(b"x".as_slice())]).await?;
    let err = table.take_blobs("missing", &[0]).await.unwrap_err();
    assert!(
        matches!(err, Error::InvalidInput { .. }),
        "expected InvalidInput, got {err:?}"
    );
    assert!(err.to_string().contains("no column named"));
    Ok(())
}

#[tokio::test]
async fn take_blob_files_returns_readable_handles() -> Result<()> {
    let tmp = tempdir().unwrap();
    let db = connect(tmp.path().to_str().unwrap()).execute().await?;
    let payload = b"handle-bytes".as_slice();
    let table = create_inline_blob_table(&db, "t", &[1], &[Some(payload)]).await?;
    let ids = collect_row_ids(&table).await?;
    let files = table.take_blob_files("image", &ids).await?;
    assert_eq!(files.len(), 1);
    let bytes = files[0].read().await.unwrap();
    assert_eq!(bytes.to_vec(), payload);
    Ok(())
}

// `take_blobs` keeps the output aligned 1:1 with the input row ids by
// stitching nulls back in for rows whose blob value is null. Callers can
// safely zip the result with the row ids they passed in.
#[tokio::test]
async fn take_blobs_preserves_null_alignment() -> Result<()> {
    let tmp = tempdir().unwrap();
    let db = connect(tmp.path().to_str().unwrap()).execute().await?;
    let table =
        create_inline_blob_table(&db, "t", &[1, 2], &[Some(b"present".as_slice()), None]).await?;
    let ids = collect_row_ids(&table).await?;
    assert_eq!(ids.len(), 2);
    let bytes = table.take_blobs("image", &ids).await?;
    assert_eq!(bytes.len(), ids.len(), "output aligned 1:1 with row ids");
    let nulls = (0..bytes.len()).filter(|&i| bytes.is_null(i)).count();
    assert_eq!(nulls, 1, "the one null blob row stays null in the result");
    let non_null_idx = (0..bytes.len())
        .find(|&i| !bytes.is_null(i))
        .expect("at least one non-null");
    assert_eq!(bytes.value(non_null_idx), b"present");
    Ok(())
}

#[tokio::test]
async fn blob_columns_lists_blob_v2_fields() -> Result<()> {
    let tmp = tempdir().unwrap();
    let db = connect(tmp.path().to_str().unwrap()).execute().await?;
    let table = create_inline_blob_table(&db, "t", &[1], &[Some(b"x".as_slice())]).await?;
    assert_eq!(table.blob_columns().await?, vec!["image".to_string()]);
    Ok(())
}

#[tokio::test]
async fn blob_columns_empty_when_no_blob_fields() -> Result<()> {
    let tmp = tempdir().unwrap();
    let db = connect(tmp.path().to_str().unwrap()).execute().await?;
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![1_i64]))],
    )
    .unwrap();
    let table = db.create_empty_table("t", schema).execute().await?;
    table.add(batch).execute().await?;
    assert!(table.blob_columns().await?.is_empty());
    Ok(())
}

#[tokio::test]
async fn compacting_blob_table_preserves_blob_round_trip() -> Result<()> {
    let tmp = tempdir().unwrap();
    let db = connect(tmp.path().to_str().unwrap()).execute().await?;
    let table = create_inline_blob_table(&db, "t", &[1], &[Some(b"frag-one".as_slice())]).await?;

    let batch2 = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("image", DataType::LargeBinary, true),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![2_i64])),
            Arc::new(LargeBinaryArray::from_iter_values([b"frag-two".as_slice()])),
        ],
    )
    .unwrap();
    table.add(batch2).execute().await?;

    let ids_before = collect_row_ids(&table).await?;
    assert_eq!(ids_before.len(), 2);
    assert_eq!(table.take_blobs("image", &ids_before).await?.len(), 2);

    table
        .optimize(OptimizeAction::Compact {
            options: CompactionOptions::default(),
            remap_options: None,
        })
        .await?;

    let pairs_after = collect_id_rowid(&table).await?;
    let ids_after: Vec<u64> = pairs_after.iter().map(|(_, rowid)| *rowid).collect();
    let bytes_after = table.take_blobs("image", &ids_after).await?;
    assert_eq!(bytes_after.len(), 2);
    for (i, (id, _)) in pairs_after.iter().enumerate() {
        match id {
            1 => assert_eq!(bytes_after.value(i), b"frag-one"),
            2 => assert_eq!(bytes_after.value(i), b"frag-two"),
            _ => unreachable!("unexpected id {id} after compaction"),
        }
    }
    Ok(())
}
async fn create_two_blob_table(db: &lancedb::Connection, name: &str) -> Result<Table> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        blob("image", true),
        blob("thumb", true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("image", DataType::LargeBinary, true),
            Field::new("thumb", DataType::LargeBinary, true),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![1_i64])),
            Arc::new(LargeBinaryArray::from_iter_values([
                b"full-image".as_slice()
            ])),
            Arc::new(LargeBinaryArray::from_iter_values(
                [b"thumbnail".as_slice()],
            )),
        ],
    )
    .unwrap();
    let table = db.create_empty_table(name, schema).execute().await?;
    table.add(batch).execute().await?;
    Ok(table)
}

#[tokio::test]
async fn take_blobs_aligns_with_reordered_ids() -> Result<()> {
    let tmp = tempdir().unwrap();
    let db = connect(tmp.path().to_str().unwrap()).execute().await?;
    let table = create_inline_blob_table(
        &db,
        "t",
        &[1, 2, 3],
        &[
            Some(b"a".as_slice()),
            Some(b"b".as_slice()),
            Some(b"c".as_slice()),
        ],
    )
    .await?;
    let mut ids = collect_row_ids(&table).await?;
    ids.reverse();
    let bytes = table.take_blobs("image", &ids).await?;
    assert_eq!(bytes.len(), 3, "output aligns 1:1 with the input ids");
    assert_eq!(
        bytes.value(0),
        b"c",
        "result follows input id order, not scan order"
    );
    assert_eq!(bytes.value(1), b"b");
    assert_eq!(bytes.value(2), b"a");
    Ok(())
}

#[tokio::test]
async fn take_blobs_empty_ids_returns_empty() -> Result<()> {
    let tmp = tempdir().unwrap();
    let db = connect(tmp.path().to_str().unwrap()).execute().await?;
    let table = create_inline_blob_table(&db, "t", &[1], &[Some(b"x".as_slice())]).await?;
    let bytes = table.take_blobs("image", &[]).await?;
    assert_eq!(bytes.len(), 0);
    Ok(())
}

#[tokio::test]
async fn take_blob_files_empty_ids_returns_empty() -> Result<()> {
    let tmp = tempdir().unwrap();
    let db = connect(tmp.path().to_str().unwrap()).execute().await?;
    let table = create_inline_blob_table(&db, "t", &[1], &[Some(b"x".as_slice())]).await?;
    let files = table.take_blob_files("image", &[]).await?;
    assert!(files.is_empty());
    Ok(())
}

#[tokio::test]
async fn take_blobs_handles_interleaved_nulls() -> Result<()> {
    let tmp = tempdir().unwrap();
    let db = connect(tmp.path().to_str().unwrap()).execute().await?;
    let table = create_inline_blob_table(
        &db,
        "t",
        &[1, 2, 3, 4, 5],
        &[
            Some(b"a".as_slice()),
            None,
            Some(b"c".as_slice()),
            None,
            Some(b"e".as_slice()),
        ],
    )
    .await?;
    let ids = collect_row_ids(&table).await?;
    let bytes = table.take_blobs("image", &ids).await?;
    assert_eq!(bytes.len(), 5);
    assert_eq!(bytes.null_count(), 2);
    assert_eq!(bytes.value(0), b"a");
    assert!(bytes.is_null(1));
    assert_eq!(bytes.value(2), b"c");
    assert!(bytes.is_null(3));
    assert_eq!(bytes.value(4), b"e");
    Ok(())
}

#[tokio::test]
async fn take_blobs_reads_multiple_blob_columns_independently() -> Result<()> {
    let tmp = tempdir().unwrap();
    let db = connect(tmp.path().to_str().unwrap()).execute().await?;
    let table = create_two_blob_table(&db, "t").await?;
    let ids = collect_row_ids(&table).await?;
    let image = table.take_blobs("image", &ids).await?;
    let thumb = table.take_blobs("thumb", &ids).await?;
    assert_eq!(image.value(0), b"full-image");
    assert_eq!(thumb.value(0), b"thumbnail");
    Ok(())
}

#[tokio::test]
async fn add_accepts_prebuilt_blob_struct_without_recoercion() -> Result<()> {
    let tmp = tempdir().unwrap();
    let db = connect(tmp.path().to_str().unwrap()).execute().await?;
    let table = db
        .create_empty_table(
            "t",
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                blob("image", true),
            ])),
        )
        .execute()
        .await?;

    let image_field = blob("image", true);
    let DataType::Struct(child_fields) = image_field.data_type().clone() else {
        unreachable!("blob field is a struct");
    };
    let data: ArrayRef = Arc::new(LargeBinaryArray::from_iter_values([b"prebuilt".as_slice()]));
    let uri: ArrayRef = Arc::new(StringArray::from(vec![None::<&str>]));
    let prebuilt = StructArray::new(child_fields, vec![data, uri], None);

    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            image_field,
        ])),
        vec![Arc::new(Int64Array::from(vec![1_i64])), Arc::new(prebuilt)],
    )
    .unwrap();
    table.add(batch).execute().await?;

    let ids = collect_row_ids(&table).await?;
    let bytes = table.take_blobs("image", &ids).await?;
    assert_eq!(bytes.value(0), b"prebuilt");
    Ok(())
}

async fn append_inline_blobs(table: &Table, ids: &[i64], payloads: &[Option<&[u8]>]) -> Result<()> {
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("image", DataType::LargeBinary, true),
        ])),
        vec![
            Arc::new(Int64Array::from(ids.to_vec())),
            Arc::new(LargeBinaryArray::from_iter(payloads.iter().copied())),
        ],
    )
    .unwrap();
    table.add(batch).execute().await?;
    Ok(())
}

async fn collect_id_rowid(table: &Table) -> Result<Vec<(i64, u64)>> {
    let rows = table
        .query()
        .with_row_id()
        .execute()
        .await?
        .try_collect::<Vec<_>>()
        .await?;
    let batch = arrow_select::concat::concat_batches(&rows[0].schema(), &rows).unwrap();
    let ids = batch
        .column_by_name("id")
        .expect("id")
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let rowids = batch
        .column_by_name("_rowid")
        .expect("_rowid")
        .as_any()
        .downcast_ref::<arrow_array::UInt64Array>()
        .unwrap();
    Ok((0..batch.num_rows())
        .map(|i| (ids.value(i), rowids.value(i)))
        .collect())
}

async fn create_two_blob_table_rows(
    db: &lancedb::Connection,
    name: &str,
    ids: &[i64],
    images: &[Option<&[u8]>],
    thumbs: &[Option<&[u8]>],
) -> Result<Table> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        blob("image", true),
        blob("thumb", true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("image", DataType::LargeBinary, true),
            Field::new("thumb", DataType::LargeBinary, true),
        ])),
        vec![
            Arc::new(Int64Array::from(ids.to_vec())),
            Arc::new(LargeBinaryArray::from_iter(images.iter().copied())),
            Arc::new(LargeBinaryArray::from_iter(thumbs.iter().copied())),
        ],
    )
    .unwrap();
    let table = db.create_empty_table(name, schema).execute().await?;
    table.add(batch).execute().await?;
    Ok(table)
}

// PIN: a zero-length non-null blob (b"") encodes identically to a null at the v2
// descriptor level (kind=Inline, position=0, size=0), so it currently reads back
// as null. This is the documented null/empty conflation. When Lance distinguishes
// them, flip this to assert a non-null empty value.
#[tokio::test]
async fn zero_length_blob_reads_back_as_null() -> Result<()> {
    let tmp = tempdir().unwrap();
    let db = connect(tmp.path().to_str().unwrap()).execute().await?;
    let table = create_inline_blob_table(
        &db,
        "t",
        &[1, 2],
        &[Some(b"".as_slice()), Some(b"data".as_slice())],
    )
    .await?;
    let pairs = collect_id_rowid(&table).await?;
    let row_ids: Vec<u64> = pairs.iter().map(|(_, r)| *r).collect();
    let bytes = table.take_blobs("image", &row_ids).await?;
    assert_eq!(bytes.len(), 2);
    for (i, (id, _)) in pairs.iter().enumerate() {
        match id {
            1 => assert!(
                bytes.is_null(i),
                "zero-length blob currently reads back as null (documented conflation)"
            ),
            2 => assert_eq!(bytes.value(i), b"data"),
            _ => unreachable!(),
        }
    }
    Ok(())
}

#[tokio::test]
async fn take_blobs_across_fragments() -> Result<()> {
    let tmp = tempdir().unwrap();
    let db = connect(tmp.path().to_str().unwrap()).execute().await?;
    let table = create_inline_blob_table(
        &db,
        "t",
        &[1, 2],
        &[Some(b"one".as_slice()), Some(b"two".as_slice())],
    )
    .await?;
    append_inline_blobs(
        &table,
        &[3, 4],
        &[Some(b"three".as_slice()), Some(b"four".as_slice())],
    )
    .await?;

    let pairs = collect_id_rowid(&table).await?;
    assert_eq!(
        pairs.len(),
        4,
        "two appends produce four rows across fragments"
    );
    let row_ids: Vec<u64> = pairs.iter().map(|(_, r)| *r).collect();
    let bytes = table.take_blobs("image", &row_ids).await?;
    assert_eq!(bytes.len(), 4);
    let expected = |id: i64| -> &'static [u8] {
        match id {
            1 => b"one",
            2 => b"two",
            3 => b"three",
            4 => b"four",
            _ => unreachable!(),
        }
    };
    for (i, (id, _)) in pairs.iter().enumerate() {
        assert_eq!(
            bytes.value(i),
            expected(*id),
            "row {i} (id {id}) bytes resolve correctly across fragment boundaries"
        );
    }
    Ok(())
}

#[tokio::test]
async fn take_blobs_with_duplicate_ids() -> Result<()> {
    let tmp = tempdir().unwrap();
    let db = connect(tmp.path().to_str().unwrap()).execute().await?;
    let table = create_inline_blob_table(
        &db,
        "t",
        &[1, 2],
        &[Some(b"first".as_slice()), Some(b"second".as_slice())],
    )
    .await?;
    let pairs = collect_id_rowid(&table).await?;
    let by_id = |id: i64| pairs.iter().find(|(i, _)| *i == id).unwrap().1;
    let dup = vec![by_id(2), by_id(1), by_id(2)];
    let bytes = table.take_blobs("image", &dup).await?;
    assert_eq!(
        bytes.len(),
        3,
        "duplicate ids each get their own output slot"
    );
    assert_eq!(bytes.value(0), b"second");
    assert_eq!(bytes.value(1), b"first");
    assert_eq!(bytes.value(2), b"second");
    Ok(())
}

// An out-of-range row id must be handled gracefully (clean error or null slot),
// never a panic or silent corruption.
#[tokio::test]
async fn take_blobs_out_of_range_id_does_not_panic() -> Result<()> {
    let tmp = tempdir().unwrap();
    let db = connect(tmp.path().to_str().unwrap()).execute().await?;
    let table = create_inline_blob_table(&db, "t", &[1], &[Some(b"only".as_slice())]).await?;
    // A clean error or an aligned slot are both fine; the invariant under test
    // is that an out-of-range row id never panics or corrupts.
    if let Ok(arr) = table.take_blobs("image", &[u64::MAX]).await {
        assert_eq!(arr.len(), 1, "out-of-range id yields a single aligned slot");
    }
    Ok(())
}

#[tokio::test]
async fn take_blobs_all_null_column() -> Result<()> {
    let tmp = tempdir().unwrap();
    let db = connect(tmp.path().to_str().unwrap()).execute().await?;
    let table = create_inline_blob_table(&db, "t", &[1, 2, 3], &[None, None, None]).await?;
    let ids = collect_row_ids(&table).await?;
    let bytes = table.take_blobs("image", &ids).await?;
    assert_eq!(bytes.len(), 3);
    assert_eq!(
        bytes.null_count(),
        3,
        "every row null exercises the empty non-null subset path"
    );
    Ok(())
}

#[tokio::test]
async fn take_blobs_mixed_nulls_across_columns() -> Result<()> {
    let tmp = tempdir().unwrap();
    let db = connect(tmp.path().to_str().unwrap()).execute().await?;
    let table = create_two_blob_table_rows(
        &db,
        "t",
        &[1, 2],
        &[Some(b"img1".as_slice()), None],
        &[None, Some(b"thumb2".as_slice())],
    )
    .await?;
    let pairs = collect_id_rowid(&table).await?;
    let row_ids: Vec<u64> = pairs.iter().map(|(_, r)| *r).collect();
    let image = table.take_blobs("image", &row_ids).await?;
    let thumb = table.take_blobs("thumb", &row_ids).await?;
    for (i, (id, _)) in pairs.iter().enumerate() {
        match id {
            1 => {
                assert_eq!(image.value(i), b"img1");
                assert!(thumb.is_null(i), "thumb null mask is independent of image");
            }
            2 => {
                assert!(image.is_null(i), "image null mask is independent of thumb");
                assert_eq!(thumb.value(i), b"thumb2");
            }
            _ => unreachable!(),
        }
    }
    Ok(())
}

#[tokio::test]
async fn take_blobs_after_delete() -> Result<()> {
    let tmp = tempdir().unwrap();
    let db = connect(tmp.path().to_str().unwrap()).execute().await?;
    let table = create_inline_blob_table(
        &db,
        "t",
        &[1, 2, 3],
        &[
            Some(b"a".as_slice()),
            Some(b"b".as_slice()),
            Some(b"c".as_slice()),
        ],
    )
    .await?;
    table.delete("id = 2").await?;
    let pairs = collect_id_rowid(&table).await?;
    assert_eq!(pairs.len(), 2, "one row deleted");
    let row_ids: Vec<u64> = pairs.iter().map(|(_, r)| *r).collect();
    let bytes = table.take_blobs("image", &row_ids).await?;
    assert_eq!(bytes.len(), 2);
    let expected = |id: i64| -> &'static [u8] {
        match id {
            1 => b"a",
            3 => b"c",
            _ => unreachable!("id {id} should have been deleted"),
        }
    };
    for (i, (id, _)) in pairs.iter().enumerate() {
        assert_eq!(
            bytes.value(i),
            expected(*id),
            "surviving rows resolve after delete"
        );
    }
    Ok(())
}

async fn create_many_row_blob_table(
    db: &lancedb::Connection,
    name: &str,
    n: usize,
) -> Result<(Table, Vec<Vec<u8>>)> {
    let payloads: Vec<Vec<u8>> = (0..n).map(|i| format!("row-{i}").into_bytes()).collect();
    let ids: Vec<i64> = (0..n as i64).collect();
    let images: Vec<Option<&[u8]>> = payloads.iter().map(|p| Some(p.as_slice())).collect();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        blob("image", true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("image", DataType::LargeBinary, true),
        ])),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(LargeBinaryArray::from_iter(images)),
        ],
    )
    .unwrap();
    let table = db.create_empty_table(name, schema).execute().await?;
    table.add(batch).execute().await?;
    Ok((table, payloads))
}

#[tokio::test]
async fn take_blobs_many_rows() -> Result<()> {
    let tmp = tempdir().unwrap();
    let db = connect(tmp.path().to_str().unwrap()).execute().await?;
    let (table, payloads) = create_many_row_blob_table(&db, "t", 1000).await?;
    let pairs = collect_id_rowid(&table).await?;
    assert_eq!(pairs.len(), 1000);
    let row_ids: Vec<u64> = pairs.iter().map(|(_, r)| *r).collect();
    let bytes = table.take_blobs("image", &row_ids).await?;
    assert_eq!(bytes.len(), 1000);
    for (i, (id, _)) in pairs.iter().enumerate() {
        assert_eq!(bytes.value(i), payloads[*id as usize].as_slice());
    }
    Ok(())
}

#[tokio::test]
async fn take_blobs_large_payload_round_trip() -> Result<()> {
    let tmp = tempdir().unwrap();
    let db = connect(tmp.path().to_str().unwrap()).execute().await?;
    let payload = vec![0xABu8; 1_000_000];
    let table = create_inline_blob_table(&db, "t", &[1], &[Some(payload.as_slice())]).await?;
    let ids = collect_row_ids(&table).await?;
    let bytes = table.take_blobs("image", &ids).await?;
    assert_eq!(bytes.value(0).len(), 1_000_000);
    assert_eq!(bytes.value(0), payload.as_slice());
    Ok(())
}

#[tokio::test]
async fn take_blobs_large_payloads_across_fragments() -> Result<()> {
    let tmp = tempdir().unwrap();
    let db = connect(tmp.path().to_str().unwrap()).execute().await?;
    let big_a = vec![0x11u8; 300_000];
    let big_b = vec![0x22u8; 300_000];
    let big_c = vec![0x33u8; 300_000];
    let table = create_inline_blob_table(&db, "t", &[1], &[Some(big_a.as_slice())]).await?;
    append_inline_blobs(
        &table,
        &[2, 3],
        &[Some(big_b.as_slice()), Some(big_c.as_slice())],
    )
    .await?;
    let pairs = collect_id_rowid(&table).await?;
    let row_ids: Vec<u64> = pairs.iter().map(|(_, r)| *r).collect();
    let bytes = table.take_blobs("image", &row_ids).await?;
    for (i, (id, _)) in pairs.iter().enumerate() {
        let expected = match id {
            1 => &big_a,
            2 => &big_b,
            3 => &big_c,
            _ => unreachable!(),
        };
        assert_eq!(
            bytes.value(i),
            expected.as_slice(),
            "large blob id {id} intact across fragments"
        );
    }
    Ok(())
}

#[tokio::test]
async fn blob_columns_returns_only_blob_fields_in_order() -> Result<()> {
    let tmp = tempdir().unwrap();
    let db = connect(tmp.path().to_str().unwrap()).execute().await?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        blob("alpha", true),
        Field::new("plain", DataType::LargeBinary, true),
        blob("beta", true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("alpha", DataType::LargeBinary, true),
            Field::new("plain", DataType::LargeBinary, true),
            Field::new("beta", DataType::LargeBinary, true),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![1_i64])),
            Arc::new(LargeBinaryArray::from_iter_values([b"a".as_slice()])),
            Arc::new(LargeBinaryArray::from_iter_values([b"p".as_slice()])),
            Arc::new(LargeBinaryArray::from_iter_values([b"b".as_slice()])),
        ],
    )
    .unwrap();
    let table = db.create_empty_table("t", schema).execute().await?;
    table.add(batch).execute().await?;
    assert_eq!(
        table.blob_columns().await?,
        vec!["alpha".to_string(), "beta".to_string()],
        "only blob v2 columns, in declaration order, excluding the plain binary column"
    );
    Ok(())
}

#[tokio::test]
async fn take_blob_files_rejects_non_blob_and_unknown_columns() -> Result<()> {
    let tmp = tempdir().unwrap();
    let db = connect(tmp.path().to_str().unwrap()).execute().await?;
    let table = create_inline_blob_table(&db, "t", &[1], &[Some(b"x".as_slice())]).await?;
    let err = table.take_blob_files("id", &[0]).await.unwrap_err();
    assert!(matches!(err, Error::InvalidInput { .. }), "got {err:?}");
    assert!(err.to_string().contains("not a blob column"));
    let err = table.take_blob_files("missing", &[0]).await.unwrap_err();
    assert!(matches!(err, Error::InvalidInput { .. }), "got {err:?}");
    assert!(err.to_string().contains("no column named"));
    Ok(())
}
