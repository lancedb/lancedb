// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use lancedb::{connect, table::NewColumnTransform};

// --8<-- [start:quickstart_define_struct]
use std::sync::Arc;

use lancedb::arrow::arrow_array::types::Float32Type;
use lancedb::arrow::arrow_array::{
    FixedSizeListArray, Int8Array, LargeStringArray, RecordBatch, RecordBatchIterator,
    RecordBatchReader, StructArray,
};
use lancedb::arrow::arrow_schema::{DataType, Field, FieldRef, Fields, Schema};

// Define structs representing the data schema
#[derive(Debug, Clone)]
struct Stats {
    motion: i8,
    sharpness: i8,
    exposure: i8,
    framing: i8,
}

#[derive(Debug, Clone)]
struct Frame {
    frame_id: String,
    video_id: String,
    caption: String,
    stats: Stats,
    embedding: [f32; 4],
}

fn frames_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("frame_id", DataType::LargeUtf8, false),
        Field::new("video_id", DataType::LargeUtf8, false),
        Field::new("caption", DataType::LargeUtf8, false),
        Field::new(
            "stats",
            DataType::Struct(Fields::from(vec![
                Arc::new(Field::new("motion", DataType::Int8, false)),
                Arc::new(Field::new("sharpness", DataType::Int8, false)),
                Arc::new(Field::new("exposure", DataType::Int8, false)),
                Arc::new(Field::new("framing", DataType::Int8, false)),
            ])),
            false,
        ),
        Field::new(
            "embedding",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 4),
            false,
        ),
    ]))
}

fn frames_to_reader(
    schema: Arc<Schema>,
    rows: &[Frame],
) -> Box<dyn RecordBatchReader + Send> {
    let frame_ids = LargeStringArray::from_iter_values(rows.iter().map(|row| row.frame_id.as_str()));
    let video_ids = LargeStringArray::from_iter_values(rows.iter().map(|row| row.video_id.as_str()));
    let captions =
        LargeStringArray::from_iter_values(rows.iter().map(|row| row.caption.as_str()));

    let motion = Int8Array::from_iter_values(rows.iter().map(|row| row.stats.motion));
    let sharpness = Int8Array::from_iter_values(rows.iter().map(|row| row.stats.sharpness));
    let exposure = Int8Array::from_iter_values(rows.iter().map(|row| row.stats.exposure));
    let framing = Int8Array::from_iter_values(rows.iter().map(|row| row.stats.framing));
    let stats_fields: Vec<FieldRef> = vec![
        Arc::new(Field::new("motion", DataType::Int8, false)),
        Arc::new(Field::new("sharpness", DataType::Int8, false)),
        Arc::new(Field::new("exposure", DataType::Int8, false)),
        Arc::new(Field::new("framing", DataType::Int8, false)),
    ];
    let stats = StructArray::new(
        stats_fields.into(),
        vec![
            Arc::new(motion),
            Arc::new(sharpness),
            Arc::new(exposure),
            Arc::new(framing),
        ],
        None,
    );

    let embeddings = FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
        rows.iter()
            .map(|row| Some(row.embedding.iter().copied().map(Some).collect::<Vec<_>>())),
        4,
    );

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(frame_ids),
            Arc::new(video_ids),
            Arc::new(captions),
            Arc::new(stats),
            Arc::new(embeddings),
        ],
    )
    .unwrap();

    Box::new(RecordBatchIterator::new(
        vec![Ok(batch)].into_iter(),
        schema,
    ))
}
// --8<-- [end:quickstart_define_struct]

#[tokio::main]
async fn main() {
    let temp_dir = tempfile::tempdir().unwrap();
    let uri = temp_dir.path().to_str().unwrap();
    let db = connect(uri).execute().await.unwrap();

    // --8<-- [start:quickstart_data]
    let data = vec![
        Frame {
            frame_id: "1".to_string(),
            video_id: "clip-1".to_string(),
            caption: "a robot arm reaching toward a cup".to_string(),
            stats: Stats {
                motion: 3,
                sharpness: 4,
                exposure: 4,
                framing: 3,
            },
            embedding: [0.7, 0.1, 0.9, 0.7],
        },
        Frame {
            frame_id: "2".to_string(),
            video_id: "clip-1".to_string(),
            caption: "a two-finger gripper closing on a cup".to_string(),
            stats: Stats {
                motion: 5,
                sharpness: 4,
                exposure: 5,
                framing: 2,
            },
            embedding: [0.2, 0.9, 0.4, 0.9],
        },
        Frame {
            frame_id: "3".to_string(),
            video_id: "clip-2".to_string(),
            caption: "a robot arm holding a cup steady".to_string(),
            stats: Stats {
                motion: 2,
                sharpness: 3,
                exposure: 3,
                framing: 4,
            },
            embedding: [0.9, 0.1, 0.5, 0.4],
        },
    ];
    // --8<-- [end:quickstart_data]

    // --8<-- [start:quickstart_create_table]
    use lancedb::database::CreateTableMode;

    let schema = frames_schema();
    let table = db
        .create_table("frames", frames_to_reader(schema.clone(), &data))
        .mode(CreateTableMode::Overwrite)
        .execute()
        .await
        .unwrap();
    // --8<-- [end:quickstart_create_table]
    assert_eq!(table.count_rows(None).await.unwrap(), 3);

    // --8<-- [start:quickstart_create_indexes]
    use lancedb::index::Index;
    use lancedb::index::scalar::BTreeIndexBuilder;
    use lancedb::index::vector::IvfPqIndexBuilder;

    table
        .create_index(&["embedding"], Index::IvfPq(IvfPqIndexBuilder::default()))
        .execute()
        .await
        .unwrap();
    table
        .create_index(
            &["stats.motion"],
            Index::BTree(BTreeIndexBuilder::default()),
        )
        .execute()
        .await
        .unwrap();
    // --8<-- [end:quickstart_create_indexes]
    let mut index_names: Vec<_> = table
        .list_indices()
        .await
        .unwrap()
        .into_iter()
        .map(|index| index.name)
        .collect();
    index_names.sort();
    assert_eq!(index_names, ["embedding_idx", "stats.motion_idx"]);

    // --8<-- [start:quickstart_add_data]
    let more_data = vec![Frame {
        frame_id: "4".to_string(),
        video_id: "clip-2".to_string(),
        caption: "a gripper lowering a cup onto the table".to_string(),
        stats: Stats {
            motion: 5,
            sharpness: 4,
            exposure: 4,
            framing: 2,
        },
        embedding: [0.3, 0.9, 0.6, 0.8],
    }];
    table
        .add(frames_to_reader(schema.clone(), &more_data))
        .execute()
        .await
        .unwrap();
    // --8<-- [end:quickstart_add_data]
    assert_eq!(table.count_rows(None).await.unwrap(), 4);

    // --8<-- [start:quickstart_vector_search_1]
    use lancedb::arrow::IntoPolars;
    use lancedb::query::{ExecutableQuery, QueryBase, Select};

    // Search for frames similar to "a gripper closing on an object"
    let query_vector = [0.2, 0.8, 0.4, 0.9];

    let result = table
        .query()
        .nearest_to(&query_vector)
        .unwrap()
        .select(Select::Columns(vec![
            "frame_id".to_string(),
            "video_id".to_string(),
            "caption".to_string(),
            "_distance".to_string(),
        ]))
        .limit(2)
        .execute()
        .await
        .unwrap()
        .into_polars()
        .await
        .unwrap();
    println!("{result:?}");
    // --8<-- [end:quickstart_vector_search_1]
    let frame_id_col = result.column("frame_id").unwrap().str().unwrap();
    assert_eq!(frame_id_col.get(0).unwrap(), "2");
    assert_eq!(frame_id_col.get(1).unwrap(), "4");

    // --8<-- [start:quickstart_curate_with_metadata]
    let curated = table
        .query()
        .nearest_to(&query_vector)
        .unwrap()
        .only_if("stats.motion >= 4")
        .select(Select::Columns(vec![
            "frame_id".to_string(),
            "video_id".to_string(),
            "caption".to_string(),
            "_distance".to_string(),
        ]))
        .limit(2)
        .execute()
        .await
        .unwrap()
        .into_polars()
        .await
        .unwrap();
    println!("{curated:?}");
    // --8<-- [end:quickstart_curate_with_metadata]
    let curated_frame_id_col = curated.column("frame_id").unwrap().str().unwrap();
    assert_eq!(curated_frame_id_col.get(0).unwrap(), "2");
    assert_eq!(curated_frame_id_col.get(1).unwrap(), "4");

    // --8<-- [start:quickstart_output_array]
    let result = table
        .query()
        .nearest_to(&query_vector)
        .unwrap()
        .select(Select::Columns(vec![
            "frame_id".to_string(),
            "video_id".to_string(),
            "caption".to_string(),
            "_distance".to_string(),
        ]))
        .limit(2)
        .execute()
        .await
        .unwrap()
        .into_polars()
        .await
        .unwrap();
    println!("{result:?}");
    // --8<-- [end:quickstart_output_array]
    let frame_id_col = result.column("frame_id").unwrap().str().unwrap();
    assert_eq!(frame_id_col.get(0).unwrap(), "2");

    // --8<-- [start:quickstart_add_feature]
    table
        .add_columns(
            NewColumnTransform::SqlExpressions(vec![(
                "quality_score".to_string(),
                "cast(((stats.motion + stats.sharpness + stats.exposure + stats.framing) / 4.0) as float)"
                    .to_string(),
            )]),
            None,
        )
        .await
        .unwrap();
    // --8<-- [end:quickstart_add_feature]

    // --8<-- [start:quickstart_query_feature]
    let features = table
        .query()
        .select(Select::Columns(vec![
            "frame_id".to_string(),
            "video_id".to_string(),
            "quality_score".to_string(),
        ]))
        .execute()
        .await
        .unwrap()
        .into_polars()
        .await
        .unwrap();
    println!("{features:?}");
    // --8<-- [end:quickstart_query_feature]
    assert_eq!(features.column("quality_score").unwrap().len(), 4);

    // What the quickstart's download step leaves in an application's directory.
    let app = tempfile::tempdir().unwrap();
    std::fs::create_dir(app.path().join("data")).unwrap();
    std::fs::copy(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../web/static/assets/images/quickstart/sir-lancelot.jpg"),
        app.path().join("data/sample-frame.jpg"),
    )
    .unwrap();
    std::env::set_current_dir(app.path()).unwrap();

    // --8<-- [start:quickstart_multimodal_bytes]
    use lancedb::arrow::arrow_array::BinaryArray;

    let image_bytes = std::fs::read("data/sample-frame.jpg").unwrap();

    let image_schema = Arc::new(Schema::new(vec![
        Field::new("frame_id", DataType::LargeUtf8, false),
        Field::new("caption", DataType::LargeUtf8, false),
        Field::new("image", DataType::Binary, false),
        Field::new(
            "embedding",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 4),
            false,
        ),
    ]));
    let image_embeddings = [[0.9_f32, 0.1, 0.5, 0.4]];
    let image_batch = RecordBatch::try_new(
        image_schema.clone(),
        vec![
            Arc::new(LargeStringArray::from_iter_values(["thumb-1"])),
            Arc::new(LargeStringArray::from_iter_values([
                "Sample frame thumbnail",
            ])),
            Arc::new(BinaryArray::from_iter_values([image_bytes.as_slice()])),
            Arc::new(
                FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
                    image_embeddings
                        .iter()
                        .map(|vector| Some(vector.iter().copied().map(Some).collect::<Vec<_>>())),
                    4,
                ),
            ),
        ],
    )
    .unwrap();
    let image_reader: Box<dyn RecordBatchReader + Send> = Box::new(RecordBatchIterator::new(
        vec![Ok(image_batch)].into_iter(),
        image_schema,
    ));
    let multimodal_table = db
        .create_table("frame_images", image_reader)
        .mode(CreateTableMode::Overwrite)
        .execute()
        .await
        .unwrap();
    // --8<-- [end:quickstart_multimodal_bytes]
    assert_eq!(multimodal_table.count_rows(None).await.unwrap(), 1);
}
