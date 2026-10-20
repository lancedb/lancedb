// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

// --8<-- [start:multimodal_imports]
use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::Arc;

use arrow_array::types::Float32Type;
use arrow_array::{
    Array, BinaryArray, FixedSizeListArray, Int32Array, Int64Array, LargeBinaryArray, RecordBatch,
    RecordBatchIterator, StringArray, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema};
use futures_util::TryStreamExt;
use lancedb::blob::{BlobOptions, BlobRangeRequest, blob_with_options};
use lancedb::connect;
use lancedb::database::CreateTableMode;
use lancedb::query::{ExecutableQuery, QueryBase};
// --8<-- [end:multimodal_imports]

#[tokio::main]
async fn main() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db_uri = temp_dir.path().to_str().unwrap().to_string();
    let db = connect(&db_uri).execute().await.unwrap();

    // --8<-- [start:create_dummy_data]
    let create_dummy_image = |color: u8| -> Vec<u8> {
        let mut png_like = vec![137, 80, 78, 71, 13, 10, 26, 10];
        png_like.push(color);
        png_like
    };

    let data = vec![
        (
            1_i32,
            "red_square.png",
            vec![0.1_f32; 128],
            create_dummy_image(1),
            "red",
        ),
        (
            2_i32,
            "blue_square.png",
            vec![0.2_f32; 128],
            create_dummy_image(2),
            "blue",
        ),
    ];
    // --8<-- [end:create_dummy_data]

    // --8<-- [start:define_schema]
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("filename", DataType::Utf8, false),
        Field::new(
            "vector",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 128),
            false,
        ),
        Field::new("image_blob", DataType::Binary, false),
        Field::new("label", DataType::Utf8, false),
    ]);
    // --8<-- [end:define_schema]

    // --8<-- [start:ingest_data]
    let schema = Arc::new(schema);
    let image_batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from_iter_values(data.iter().map(|row| row.0))),
            Arc::new(StringArray::from_iter_values(data.iter().map(|row| row.1))),
            Arc::new(
                FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
                    data.iter()
                        .map(|row| Some(row.2.iter().copied().map(Some).collect::<Vec<_>>())),
                    128,
                ),
            ),
            Arc::new(BinaryArray::from_iter_values(
                data.iter().map(|row| row.3.as_slice()),
            )),
            Arc::new(StringArray::from_iter_values(data.iter().map(|row| row.4))),
        ],
    )
    .unwrap();
    let image_reader: Box<dyn arrow_array::RecordBatchReader + Send> = Box::new(
        RecordBatchIterator::new(vec![Ok(image_batch)].into_iter(), schema.clone()),
    );
    let table = db
        .create_table("images", image_reader)
        .mode(CreateTableMode::Overwrite)
        .execute()
        .await
        .unwrap();
    // --8<-- [end:ingest_data]
    assert_eq!(table.count_rows(None).await.unwrap(), 2);

    // --8<-- [start:search_data]
    let query_vector = vec![0.1_f32; 128];
    let results = table
        .query()
        .nearest_to(query_vector)
        .unwrap()
        .limit(1)
        .execute()
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    // --8<-- [end:search_data]

    // --8<-- [start:process_results]
    for batch in &results {
        let filenames = batch
            .column_by_name("filename")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let images = batch
            .column_by_name("image_blob")
            .unwrap()
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();

        for row in 0..batch.num_rows() {
            let image_bytes = images.value(row);
            println!(
                "Retrieved image: {}, Byte length: {}",
                filenames.value(row),
                image_bytes.len()
            );
        }
    }
    // --8<-- [end:process_results]
    let search_rows: usize = results.iter().map(|batch| batch.num_rows()).sum();
    assert_eq!(search_rows, 1);

    // --8<-- [start:blob_api_schema]
    let blob_options = BlobOptions {
        inline_size_threshold: Some(64 * 1024),
        dedicated_size_threshold: NonZeroUsize::new(4 * 1024 * 1024),
    };
    let blob_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        blob_with_options("video", true, blob_options),
    ]));
    // --8<-- [end:blob_api_schema]

    // --8<-- [start:blob_api_ingest]
    let blob_table = db
        .create_empty_table("videos", blob_schema)
        .mode(CreateTableMode::Overwrite)
        .execute()
        .await
        .unwrap();

    let input_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("video", DataType::LargeBinary, true),
    ]));
    let input_batch = RecordBatch::try_new(
        input_schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1_i64, 2_i64])),
            Arc::new(LargeBinaryArray::from_iter_values([
                b"inline_video_bytes".as_slice(),
                b"second_video_bytes".as_slice(),
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

    // --8<-- [start:blob_api_fetch]
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

    let payloads = blob_table.fetch_blobs("video", &row_ids).await.unwrap();

    let handles = blob_table
        .fetch_blob_files("video", &row_ids[..1])
        .await
        .unwrap();
    let header = handles[0].as_ref().unwrap().read_range(0..6).await.unwrap();
    handles[0].as_ref().unwrap().close().await.unwrap();
    // --8<-- [end:blob_api_fetch]
    assert_eq!(payloads.len(), 2);
    assert_eq!(header.as_ref(), b"inline");

    // --8<-- [start:blob_api_ranges]
    let slices = blob_table
        .fetch_blob_ranges(
            "video",
            [
                BlobRangeRequest::new(row_ids[0], 0, 6),
                BlobRangeRequest::new(row_ids[1], 0, 6),
                BlobRangeRequest::new(row_ids[1], 7, 5),
            ],
        )
        .await
        .unwrap();

    let second_handles = blob_table
        .fetch_blob_files("video", &row_ids[1..2])
        .await
        .unwrap();
    let second_clip = second_handles[0].as_ref().unwrap();
    let parts = second_clip.read_ranges(&[0..6, 7..12]).await.unwrap();
    second_clip.close().await.unwrap();
    // --8<-- [end:blob_api_ranges]
    assert_eq!(slices.value(0), b"inline");
    assert_eq!(slices.value(1), b"second");
    assert_eq!(slices.value(2), b"video");
    assert_eq!(parts[0].as_ref(), b"second");
    assert_eq!(parts[1].as_ref(), b"video");

    // --8<-- [start:legacy_blob_api_schema]
    let legacy_blob_metadata = HashMap::from([(
        "lance-encoding:blob".to_string(),
        "true".to_string(),
    )]);
    let legacy_blob_schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("video", DataType::LargeBinary, true).with_metadata(legacy_blob_metadata),
    ]);
    // --8<-- [end:legacy_blob_api_schema]

    // --8<-- [start:legacy_blob_api_ingest]
    let legacy_blob_rows = vec![
        (1_i64, b"fake_video_bytes_1".to_vec()),
        (2_i64, b"fake_video_bytes_2".to_vec()),
    ];

    let legacy_blob_schema = Arc::new(legacy_blob_schema);
    let legacy_blob_batch = RecordBatch::try_new(
        legacy_blob_schema.clone(),
        vec![
            Arc::new(Int64Array::from_iter_values(
                legacy_blob_rows.iter().map(|row| row.0),
            )),
            Arc::new(LargeBinaryArray::from_iter_values(
                legacy_blob_rows.iter().map(|row| row.1.as_slice()),
            )),
        ],
    )
    .unwrap();
    let legacy_blob_reader: Box<dyn arrow_array::RecordBatchReader + Send> = Box::new(
        RecordBatchIterator::new(vec![Ok(legacy_blob_batch)].into_iter(), legacy_blob_schema),
    );
    let legacy_blob_table = db
        .create_table("legacy_videos", legacy_blob_reader)
        .mode(CreateTableMode::Overwrite)
        .execute()
        .await
        .unwrap();
    // --8<-- [end:legacy_blob_api_ingest]
    assert_eq!(legacy_blob_table.count_rows(None).await.unwrap(), 2);
}
