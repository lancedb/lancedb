// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

// --8<-- [start:multimodal_imports]
use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::types::Float32Type;
use arrow_array::{
    BinaryArray, FixedSizeListArray, Int32Array, Int64Array, LargeBinaryArray, RecordBatch,
    RecordBatchIterator, StringArray,
};
use arrow_schema::{DataType, Field, Schema};
use futures_util::TryStreamExt;
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
    let image_reader: Box<dyn arrow_array::RecordBatchReader + Send> =
        Box::new(RecordBatchIterator::new(vec![Ok(image_batch)].into_iter(), schema.clone()));
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
    let blob_metadata = HashMap::from([(
        "lance-encoding:blob".to_string(),
        "true".to_string(),
    )]);
    let blob_schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("video", DataType::LargeBinary, true).with_metadata(blob_metadata),
    ]);
    // --8<-- [end:blob_api_schema]

    // --8<-- [start:blob_api_ingest]
    let blob_rows = vec![
        (1_i64, b"fake_video_bytes_1".to_vec()),
        (2_i64, b"fake_video_bytes_2".to_vec()),
    ];

    let blob_schema = Arc::new(blob_schema);
    let blob_batch = RecordBatch::try_new(
        blob_schema.clone(),
        vec![
            Arc::new(Int64Array::from_iter_values(blob_rows.iter().map(|row| row.0))),
            Arc::new(LargeBinaryArray::from_iter_values(
                blob_rows.iter().map(|row| row.1.as_slice()),
            )),
        ],
    )
    .unwrap();
    let blob_reader: Box<dyn arrow_array::RecordBatchReader + Send> =
        Box::new(RecordBatchIterator::new(vec![Ok(blob_batch)].into_iter(), blob_schema));
    let blob_table = db
        .create_table("videos", blob_reader)
        .mode(CreateTableMode::Overwrite)
        .execute()
        .await
        .unwrap();
    // --8<-- [end:blob_api_ingest]
    assert_eq!(blob_table.count_rows(None).await.unwrap(), 2);
}
