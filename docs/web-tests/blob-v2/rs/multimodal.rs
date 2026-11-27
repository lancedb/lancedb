// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

// --8<-- [start:blob_api_imports]
use std::sync::Arc;

use arrow_array::{
    Array, Int64Array, LargeBinaryArray, RecordBatch, RecordBatchIterator, StringArray, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema};
use futures_util::TryStreamExt;
use lancedb::blob::{BlobRangeRequest, blob};
use lancedb::connect;
use lancedb::database::CreateTableMode;
use lancedb::query::{ExecutableQuery, QueryBase};
// --8<-- [end:blob_api_imports]

use arrow_array::{StructArray, UInt8Array};

const INLINE_MAX: usize = 64 * 1024;
const DEDICATED_MIN: usize = 4 * 1024 * 1024;

#[tokio::main]
async fn main() {
    tutorial().await;
    deferred_fetch().await;
    storage_tiers().await;
}

async fn tutorial() {
    let temp_dir = tempfile::tempdir().unwrap();
    let original_dir = std::env::current_dir().unwrap();
    std::env::set_current_dir(temp_dir.path()).unwrap();

    // --8<-- [start:blob_api_setup]
    let db = connect("./blob_db").execute().await.unwrap();
    // --8<-- [end:blob_api_setup]

    // --8<-- [start:blob_api_schema]
    let blob_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("label", DataType::Utf8, true),
        blob("image", true),
    ]));
    // --8<-- [end:blob_api_schema]

    // --8<-- [start:blob_api_ingest]
    let blob_table = db
        .create_empty_table("photos", blob_schema)
        .mode(CreateTableMode::Overwrite)
        .execute()
        .await
        .unwrap();

    let input_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("label", DataType::Utf8, true),
        Field::new("image", DataType::LargeBinary, true),
    ]));
    let input_batch = RecordBatch::try_new(
        input_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1_i64, 2_i64])),
            Arc::new(StringArray::from(vec![Some("door"), Some("window")])),
            Arc::new(LargeBinaryArray::from_iter_values([
                b"inline_image_bytes".as_slice(),
                b"second_image_bytes".as_slice(),
            ])),
        ],
    )
    .unwrap();
    let input_reader: Box<dyn arrow_array::RecordBatchReader + Send> = Box::new(
        RecordBatchIterator::new(vec![Ok(input_batch)].into_iter(), input_schema),
    );
    blob_table.add(input_reader).execute().await.unwrap();
    // --8<-- [end:blob_api_ingest]
    assert_eq!(blob_table.count_rows(None).await.unwrap(), 2);

    // --8<-- [start:blob_api_files]
    let batches = blob_table
        .query()
        .with_row_id()
        .execute()
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let row_ids = batches[0]
        .column_by_name("_rowid")
        .unwrap()
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap()
        .values()
        .to_vec();

    let handles = blob_table
        .fetch_blob_files("image", &row_ids[..1])
        .await
        .unwrap();
    let header = handles[0].as_ref().unwrap().read_range(0..6).await.unwrap();
    handles[0].as_ref().unwrap().close().await.unwrap();
    // --8<-- [end:blob_api_files]
    assert_eq!(header.as_ref(), b"inline");

    // --8<-- [start:blob_api_fetch]
    let payloads = blob_table.fetch_blobs("image", &row_ids).await.unwrap();
    // --8<-- [end:blob_api_fetch]
    assert_eq!(payloads.len(), 2);

    // --8<-- [start:blob_api_ranges]
    let slices = blob_table
        .fetch_blob_ranges(
            "image",
            [
                BlobRangeRequest::new(row_ids[0], 0, 6),
                BlobRangeRequest::new(row_ids[1], 0, 6),
                BlobRangeRequest::new(row_ids[1], 7, 5),
            ],
        )
        .await
        .unwrap();

    let second_handles = blob_table
        .fetch_blob_files("image", &row_ids[1..2])
        .await
        .unwrap();
    let second_image = second_handles[0].as_ref().unwrap();
    let parts = second_image.read_ranges(&[0..6, 7..12]).await.unwrap();
    second_image.close().await.unwrap();
    // --8<-- [end:blob_api_ranges]
    assert_eq!(slices.value(0), b"inline");
    assert_eq!(slices.value(1), b"second");
    assert_eq!(slices.value(2), b"image");
    assert_eq!(parts[0].as_ref(), b"second");
    assert_eq!(parts[1].as_ref(), b"image");

    std::env::set_current_dir(original_dir).unwrap();
}

async fn deferred_fetch() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db = connect(temp_dir.path().to_str().unwrap())
        .execute()
        .await
        .unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        blob("image", true),
    ]));
    let table = db
        .create_empty_table("photos", schema)
        .execute()
        .await
        .unwrap();
    let input_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("image", DataType::LargeBinary, true),
    ]));
    let batch = RecordBatch::try_new(
        input_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1_i64])),
            Arc::new(LargeBinaryArray::from_iter_values([
                b"deferred-bytes".as_slice()
            ])),
        ],
    )
    .unwrap();
    let reader: Box<dyn arrow_array::RecordBatchReader + Send> = Box::new(
        RecordBatchIterator::new(vec![Ok(batch)].into_iter(), input_schema),
    );
    table.add(reader).execute().await.unwrap();

    let described = table
        .query()
        .execute()
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(descriptor_kind(&described[0], 0), 0);
    assert!(described[0].column_by_name("_rowid").is_none());

    let located = table
        .query()
        .with_row_id()
        .execute()
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let row_id = located[0]
        .column_by_name("_rowid")
        .unwrap()
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap()
        .value(0);
    let payloads = table.fetch_blobs("image", &[row_id]).await.unwrap();
    let handles = table.fetch_blob_files("image", &[row_id]).await.unwrap();
    let streamed = handles[0]
        .as_ref()
        .unwrap()
        .read_range(0..payloads.value(0).len() as u64)
        .await
        .unwrap();
    handles[0].as_ref().unwrap().close().await.unwrap();

    assert_eq!(payloads.value(0), b"deferred-bytes");
    assert_eq!(streamed.as_ref(), b"deferred-bytes");
}

async fn storage_tiers() {
    let cases = [
        (1_i64, marked(INLINE_MAX, 11), 0_u8),
        (2_i64, marked(INLINE_MAX + 1, 22), 1_u8),
        (3_i64, marked(DEDICATED_MIN, 33), 1_u8),
        (4_i64, marked(DEDICATED_MIN + 1, 44), 2_u8),
    ];
    let temp_dir = tempfile::tempdir().unwrap();
    let db = connect(temp_dir.path().to_str().unwrap())
        .execute()
        .await
        .unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        blob("image", true),
    ]));
    let table = db
        .create_empty_table("tiers", schema)
        .execute()
        .await
        .unwrap();
    let input_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("image", DataType::LargeBinary, true),
    ]));
    let batch = RecordBatch::try_new(
        input_schema.clone(),
        vec![
            Arc::new(Int64Array::from_iter_values(cases.iter().map(|row| row.0))),
            Arc::new(LargeBinaryArray::from_iter_values(
                cases.iter().map(|row| row.1.as_slice()),
            )),
        ],
    )
    .unwrap();
    let reader: Box<dyn arrow_array::RecordBatchReader + Send> = Box::new(
        RecordBatchIterator::new(vec![Ok(batch)].into_iter(), input_schema),
    );
    table.add(reader).execute().await.unwrap();

    let described = table
        .query()
        .execute()
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let located = table
        .query()
        .with_row_id()
        .execute()
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let mut kind_by_id = std::collections::HashMap::new();
    for batch in &described {
        let ids = batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            kind_by_id.insert(ids.value(row), descriptor_kind(batch, row));
        }
    }
    let mut row_id_by_id = std::collections::HashMap::new();
    for batch in &located {
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
        for row in 0..batch.num_rows() {
            row_id_by_id.insert(ids.value(row), row_ids.value(row));
        }
    }

    for (id, payload, kind) in &cases {
        assert_eq!(kind_by_id[id], *kind);
        let row_id = row_id_by_id[id];
        let fetched = table.fetch_blobs("image", &[row_id]).await.unwrap();
        assert_eq!(fetched.value(0), payload.as_slice());
        let handles = table.fetch_blob_files("image", &[row_id]).await.unwrap();
        let handle = handles[0].as_ref().unwrap();
        let head = handle.read_range(0..1).await.unwrap();
        let tail = handle
            .read_range((payload.len() - 1) as u64..payload.len() as u64)
            .await
            .unwrap();
        handle.close().await.unwrap();
        assert_eq!(head.as_ref(), &payload[..1]);
        assert_eq!(tail.as_ref(), &payload[payload.len() - 1..]);
    }
}

fn marked(size: usize, marker: u8) -> Vec<u8> {
    let mut payload = vec![0_u8; size];
    payload[0] = marker;
    payload[size - 1] = marker;
    payload
}

fn descriptor_kind(batch: &RecordBatch, row: usize) -> u8 {
    let image = batch
        .column_by_name("image")
        .unwrap()
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap_or_else(|| panic!("image column was {}", batch.schema()));
    image
        .column_by_name("kind")
        .unwrap()
        .as_any()
        .downcast_ref::<UInt8Array>()
        .unwrap()
        .value(row)
}
