// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::sync::Arc;

use arrow_array::types::Float32Type;
use arrow_array::{
    FixedSizeListArray, Int8Array, LargeStringArray, RecordBatch, RecordBatchIterator, StructArray,
};
use arrow_schema::{DataType, Field, FieldRef, Schema};
use lancedb::arrow::IntoPolars;
use lancedb::database::CreateTableMode;
use lancedb::query::{ExecutableQuery, QueryBase, Select};
use lancedb::{connect, table::NewColumnTransform};
use polars::prelude::DataFrame;
use serde::{Deserialize, Serialize};

// --8<-- [start:quickstart_define_struct]
// Define structs representing the data schema
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Stats {
    strength: i8,
    magic: i8,
    leadership: i8,
    wisdom: i8,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Character {
    id: String,
    name: String,
    role: String,
    description: String,
    stats: Stats,
    vector: [f32; 4],
}

fn characters_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::LargeUtf8, false),
        Field::new("name", DataType::LargeUtf8, false),
        Field::new("role", DataType::LargeUtf8, false),
        Field::new("description", DataType::LargeUtf8, false),
        Field::new(
            "stats",
            DataType::Struct(arrow_schema::Fields::from(vec![
                Arc::new(Field::new("strength", DataType::Int8, false)),
                Arc::new(Field::new("magic", DataType::Int8, false)),
                Arc::new(Field::new("leadership", DataType::Int8, false)),
                Arc::new(Field::new("wisdom", DataType::Int8, false)),
            ])),
            false,
        ),
        Field::new(
            "vector",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 4),
            false,
        ),
    ]))
}
// --8<-- [end:quickstart_define_struct]

type BatchIter = Box<dyn arrow_array::RecordBatchReader + Send>;

fn characters_to_reader(schema: Arc<Schema>, rows: &[Character]) -> BatchIter {
    let ids = LargeStringArray::from_iter_values(rows.iter().map(|row| row.id.as_str()));
    let names = LargeStringArray::from_iter_values(rows.iter().map(|row| row.name.as_str()));
    let roles = LargeStringArray::from_iter_values(rows.iter().map(|row| row.role.as_str()));
    let descriptions =
        LargeStringArray::from_iter_values(rows.iter().map(|row| row.description.as_str()));

    let strength = Int8Array::from_iter_values(rows.iter().map(|row| row.stats.strength));
    let magic = Int8Array::from_iter_values(rows.iter().map(|row| row.stats.magic));
    let leadership = Int8Array::from_iter_values(rows.iter().map(|row| row.stats.leadership));
    let wisdom = Int8Array::from_iter_values(rows.iter().map(|row| row.stats.wisdom));
    let stats_fields: Vec<FieldRef> = vec![
        Arc::new(Field::new("strength", DataType::Int8, false)),
        Arc::new(Field::new("magic", DataType::Int8, false)),
        Arc::new(Field::new("leadership", DataType::Int8, false)),
        Arc::new(Field::new("wisdom", DataType::Int8, false)),
    ];
    let stats = StructArray::new(
        stats_fields.into(),
        vec![
            Arc::new(strength),
            Arc::new(magic),
            Arc::new(leadership),
            Arc::new(wisdom),
        ],
        None,
    );

    let vectors = FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
        rows.iter()
            .map(|row| Some(row.vector.iter().copied().map(Some).collect::<Vec<_>>())),
        4,
    );

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(ids),
            Arc::new(names),
            Arc::new(roles),
            Arc::new(descriptions),
            Arc::new(stats),
            Arc::new(vectors),
        ],
    )
    .unwrap();

    Box::new(RecordBatchIterator::new(
        vec![Ok(batch)].into_iter(),
        schema,
    ))
}

#[tokio::main]
async fn main() {
    let temp_dir = tempfile::tempdir().unwrap();
    let uri = temp_dir.path().to_str().unwrap();
    let db = connect(uri).execute().await.unwrap();

    // --8<-- [start:quickstart_data]
    let data = vec![
        Character {
            id: "1".to_string(),
            name: "King Arthur".to_string(),
            role: "King".to_string(),
            description: "Leader of Camelot and wielder of Excalibur.".to_string(),
            stats: Stats {
                strength: 4,
                magic: 1,
                leadership: 5,
                wisdom: 4,
            },
            vector: [0.7, 0.1, 0.9, 0.7],
        },
        Character {
            id: "2".to_string(),
            name: "Merlin".to_string(),
            role: "Wizard".to_string(),
            description: "Advisor and prophet with deep magical knowledge.".to_string(),
            stats: Stats {
                strength: 2,
                magic: 5,
                leadership: 4,
                wisdom: 5,
            },
            vector: [0.2, 0.9, 0.4, 0.9],
        },
        Character {
            id: "3".to_string(),
            name: "Sir Lancelot".to_string(),
            role: "Knight".to_string(),
            description: "Legendary knight known for courage and combat skill.".to_string(),
            stats: Stats {
                strength: 5,
                magic: 1,
                leadership: 3,
                wisdom: 3,
            },
            vector: [0.9, 0.1, 0.5, 0.4],
        },
    ];
    // --8<-- [end:quickstart_data]

    // --8<-- [start:quickstart_create_table]
    let schema = characters_schema();
    let table = db
        .create_table("characters", characters_to_reader(schema.clone(), &data))
        .mode(CreateTableMode::Overwrite)
        .execute()
        .await
        .unwrap();
    // --8<-- [end:quickstart_create_table]
    assert_eq!(table.count_rows(None).await.unwrap(), 3);
    db.drop_table("characters", &[]).await.unwrap();

    // --8<-- [start:quickstart_create_table_no_overwrite]
    let table = db
        .create_table("characters", characters_to_reader(schema.clone(), &data))
        .execute()
        .await
        .unwrap();
    // --8<-- [end:quickstart_create_table_no_overwrite]
    assert_eq!(table.count_rows(None).await.unwrap(), 3);

    // --8<-- [start:quickstart_vector_search_1]
    // Search for examples similar to a "wise magical advisor"
    let query_vector = [0.2, 0.8, 0.4, 0.9];

    let result: DataFrame = table
        .query()
        .nearest_to(&query_vector)
        .unwrap()
        .select(Select::Columns(vec![
            "name".to_string(),
            "role".to_string(),
            "description".to_string(),
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
    let name_col = result.column("name").unwrap().str().unwrap();
    assert_eq!(name_col.get(0).unwrap(), "Merlin");

    // --8<-- [start:quickstart_curate_with_metadata]
    let curated: DataFrame = table
        .query()
        .nearest_to(&query_vector)
        .unwrap()
        .only_if("stats.magic >= 4")
        .select(Select::Columns(vec![
            "name".to_string(),
            "role".to_string(),
            "description".to_string(),
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
    let curated_name_col = curated.column("name").unwrap().str().unwrap();
    assert_eq!(curated_name_col.get(0).unwrap(), "Merlin");

    // --8<-- [start:quickstart_output_array]
    let result: DataFrame = table
        .query()
        .nearest_to(&query_vector)
        .unwrap()
        .select(Select::Columns(vec![
            "name".to_string(),
            "role".to_string(),
            "description".to_string(),
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
    let name_col = result.column("name").unwrap().str().unwrap();
    assert_eq!(name_col.get(0).unwrap(), "Merlin");

    // --8<-- [start:quickstart_add_feature]
    table
        .add_columns(
            NewColumnTransform::SqlExpressions(vec![(
                "power_score".to_string(),
                "cast(((stats.strength + stats.magic + stats.leadership + stats.wisdom) / 4.0) as float)"
                    .to_string(),
            )]),
            None,
        )
        .await
        .unwrap();
    // --8<-- [end:quickstart_add_feature]

    // --8<-- [start:quickstart_query_feature]
    let features: DataFrame = table
        .query()
        .select(Select::Columns(vec![
            "name".to_string(),
            "role".to_string(),
            "power_score".to_string(),
        ]))
        .execute()
        .await
        .unwrap()
        .into_polars()
        .await
        .unwrap();
    println!("{features:?}");
    // --8<-- [end:quickstart_query_feature]
    assert!(features.column("power_score").is_ok());

    // --8<-- [start:quickstart_multimodal_bytes]
    use std::sync::Arc;

    use arrow_array::{
        BinaryArray, FixedSizeListArray, LargeStringArray, RecordBatch, RecordBatchIterator,
    };
    use arrow_schema::{DataType, Field, Schema};

    let image_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../docs/static/assets/images/quickstart/sir-lancelot.jpg");
    let image_bytes = std::fs::read(image_path).unwrap();

    let image_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::LargeUtf8, false),
        Field::new("description", DataType::LargeUtf8, false),
        Field::new("image", DataType::Binary, false),
        Field::new(
            "vector",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 4),
            false,
        ),
    ]));
    let image_vectors = [[0.9_f32, 0.1, 0.5, 0.4]];
    let image_batch = RecordBatch::try_new(
        image_schema.clone(),
        vec![
            Arc::new(LargeStringArray::from_iter_values(["lancelot"])),
            Arc::new(LargeStringArray::from_iter_values([
                "Portrait of Sir Lancelot",
            ])),
            Arc::new(BinaryArray::from_iter_values([image_bytes.as_slice()])),
            Arc::new(
                FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
                    image_vectors
                        .iter()
                        .map(|vector| Some(vector.iter().copied().map(Some).collect::<Vec<_>>())),
                    4,
                ),
            ),
        ],
    )
    .unwrap();
    let image_reader: Box<dyn arrow_array::RecordBatchReader + Send> = Box::new(
        RecordBatchIterator::new(vec![Ok(image_batch)].into_iter(), image_schema),
    );
    let multimodal_table = db
        .create_table("character_images", image_reader)
        .mode(CreateTableMode::Overwrite)
        .execute()
        .await
        .unwrap();
    // --8<-- [end:quickstart_multimodal_bytes]
    assert_eq!(multimodal_table.count_rows(None).await.unwrap(), 1);
}
