// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

use serde::Deserialize;

type BatchIter = Box<dyn arrow_array::RecordBatchReader + Send>;

// --8<-- [start:basic_imports]
use arrow_array::types::Float32Type;
use arrow_array::{
    FixedSizeListArray, Int8Array, Int16Array, RecordBatch, RecordBatchIterator, StringArray,
    StructArray,
};
use arrow_schema::{DataType, Field, FieldRef, Schema};
use futures_util::TryStreamExt;
use lancedb::database::CreateTableMode;
use lancedb::query::{ExecutableQuery, QueryBase, Select};
use lancedb::{connect, table::NewColumnTransform};
// --8<-- [end:basic_imports]

#[derive(Debug, Clone, Deserialize)]
struct Stats {
    strength: i8,
    courage: i8,
    magic: i8,
    wisdom: i8,
}

#[derive(Debug, Clone, Deserialize)]
struct Character {
    id: i16,
    name: String,
    role: String,
    description: String,
    vector: [f32; 4],
    stats: Stats,
}

fn camelot_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int16, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("role", DataType::Utf8, false),
        Field::new("description", DataType::Utf8, false),
        Field::new(
            "vector",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 4),
            false,
        ),
        Field::new(
            "stats",
            DataType::Struct(arrow_schema::Fields::from(vec![
                Arc::new(Field::new("strength", DataType::Int8, false)),
                Arc::new(Field::new("courage", DataType::Int8, false)),
                Arc::new(Field::new("magic", DataType::Int8, false)),
                Arc::new(Field::new("wisdom", DataType::Int8, false)),
            ])),
            false,
        ),
    ]))
}

fn characters_to_record_batch(schema: Arc<Schema>, characters: &[Character]) -> RecordBatch {
    let ids = Int16Array::from_iter_values(characters.iter().map(|c| c.id));
    let names = StringArray::from_iter_values(characters.iter().map(|c| c.name.as_str()));
    let roles = StringArray::from_iter_values(characters.iter().map(|c| c.role.as_str()));
    let descriptions =
        StringArray::from_iter_values(characters.iter().map(|c| c.description.as_str()));

    let vectors = FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
        characters
            .iter()
            .map(|c| Some(c.vector.iter().copied().map(Some).collect::<Vec<_>>())),
        4,
    );

    let strength = Int8Array::from_iter_values(characters.iter().map(|c| c.stats.strength));
    let courage = Int8Array::from_iter_values(characters.iter().map(|c| c.stats.courage));
    let magic = Int8Array::from_iter_values(characters.iter().map(|c| c.stats.magic));
    let wisdom = Int8Array::from_iter_values(characters.iter().map(|c| c.stats.wisdom));

    let stats_fields: Vec<FieldRef> = vec![
        Arc::new(Field::new("strength", DataType::Int8, false)),
        Arc::new(Field::new("courage", DataType::Int8, false)),
        Arc::new(Field::new("magic", DataType::Int8, false)),
        Arc::new(Field::new("wisdom", DataType::Int8, false)),
    ];
    let stats = StructArray::new(
        stats_fields.into(),
        vec![
            Arc::new(strength),
            Arc::new(courage),
            Arc::new(magic),
            Arc::new(wisdom),
        ],
        None,
    );

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(ids),
            Arc::new(names),
            Arc::new(roles),
            Arc::new(descriptions),
            Arc::new(vectors),
            Arc::new(stats),
        ],
    )
    .unwrap()
}

fn characters_to_reader(schema: Arc<Schema>, characters: &[Character]) -> BatchIter {
    let batch = characters_to_record_batch(schema.clone(), characters);
    Box::new(RecordBatchIterator::new(vec![Ok(batch)].into_iter(), schema))
}

fn camelot_json_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("camelot.json")
}

#[tokio::main]
async fn main() {
    let temp_dir = tempfile::tempdir().unwrap();
    let uri = temp_dir.path().to_str().unwrap();
    let db = connect(uri).execute().await.unwrap();

    // --8<-- [start:data_load]
    let data: Vec<Character> =
        serde_json::from_str(&fs::read_to_string(camelot_json_path()).unwrap()).unwrap();
    // --8<-- [end:data_load]

    let schema = camelot_schema();

    // --8<-- [start:basic_create_table]
    let mut table = db
        .create_table("camelot", characters_to_reader(schema.clone(), &data))
        .mode(CreateTableMode::Overwrite)
        .execute()
        .await
        .unwrap();
    // --8<-- [end:basic_create_table]
    assert_eq!(table.count_rows(None).await.unwrap(), 8);

    // --8<-- [start:basic_open_table]
    table = db.open_table("camelot").execute().await.unwrap();
    // --8<-- [end:basic_open_table]

    // --8<-- [start:basic_create_empty_table]
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int16, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("role", DataType::Utf8, false),
        Field::new("description", DataType::Utf8, false),
        Field::new(
            "vector",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 4),
            false,
        ),
        Field::new(
            "stats",
            DataType::Struct(arrow_schema::Fields::from(vec![
                Arc::new(Field::new("strength", DataType::Int8, false)),
                Arc::new(Field::new("courage", DataType::Int8, false)),
                Arc::new(Field::new("magic", DataType::Int8, false)),
                Arc::new(Field::new("wisdom", DataType::Int8, false)),
            ])),
            false,
        ),
    ]));
    db.create_empty_table("camelot_empty", schema)
        .mode(CreateTableMode::Overwrite)
        .execute()
        .await
        .unwrap();
    // --8<-- [end:basic_create_empty_table]
    db.drop_table("camelot_empty", &[]).await.unwrap();

    // --8<-- [start:basic_add_data]
    let magical_characters = vec![
        Character {
            id: 9,
            name: "Morgan le Fay".to_string(),
            role: "Sorceress".to_string(),
            description: "A powerful enchantress, Arthur's half-sister, and a complex figure who oscillates between aiding and opposing Camelot.".to_string(),
            vector: [0.10, 0.84, 0.25, 0.70],
            stats: Stats {
                strength: 2,
                courage: 3,
                magic: 5,
                wisdom: 4,
            },
        },
        Character {
            id: 10,
            name: "The Lady of the Lake".to_string(),
            role: "Mystical Guardian".to_string(),
            description: "A mysterious supernatural figure associated with Avalon, known for giving Arthur the sword Excalibur.".to_string(),
            vector: [0.00, 0.90, 0.58, 0.88],
            stats: Stats {
                strength: 2,
                courage: 3,
                magic: 5,
                wisdom: 5,
            },
        },
    ];
    table
        .add(characters_to_reader(camelot_schema(), &magical_characters))
        .execute()
        .await
        .unwrap();
    // --8<-- [end:basic_add_data]

    // --8<-- [start:basic_vector_search]
    let query_vector = [0.03, 0.85, 0.61, 0.90];
    let result = table
        .query()
        .nearest_to(&query_vector)
        .unwrap()
        .limit(5)
        .execute()
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    println!("{result:?}");
    // --8<-- [end:basic_vector_search]

    // --8<-- [start:basic_add_columns]
    table
        .add_columns(
            NewColumnTransform::SqlExpressions(vec![(
                "power".to_string(),
                "cast(((stats.strength + stats.courage + stats.magic + stats.wisdom) / 4.0) as float)"
                    .to_string(),
            )]),
            None,
        )
        .await
        .unwrap();
    // --8<-- [end:basic_add_columns]

    // --8<-- [start:basic_vector_search_q1]
    // Who are the characters similar to  "wizard"?
    let query_vector_1 = [0.03, 0.85, 0.61, 0.90];
    let r1 = table
        .query()
        .nearest_to(&query_vector_1)
        .unwrap()
        .limit(5)
        .select(Select::Columns(vec![
            "name".to_string(),
            "role".to_string(),
            "description".to_string(),
        ]))
        .execute()
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    println!("{r1:?}");
    // --8<-- [end:basic_vector_search_q1]

    // --8<-- [start:basic_vector_search_q2]
    // Who are the characters similar to "wizard" with high magic stats?
    let query_vector_2 = [0.03, 0.85, 0.61, 0.90];
    let r2 = table
        .query()
        .nearest_to(&query_vector_2)
        .unwrap()
        .only_if("stats.magic > 3")
        .select(Select::Columns(vec![
            "name".to_string(),
            "role".to_string(),
            "description".to_string(),
        ]))
        .limit(5)
        .execute()
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    println!("{r2:?}");
    // --8<-- [end:basic_vector_search_q2]

    // --8<-- [start:basic_vector_search_q3]
    // Who are the strongest characters?
    let r3 = table
        .query()
        .only_if("stats.strength > 3")
        .select(Select::Columns(vec![
            "name".to_string(),
            "role".to_string(),
            "description".to_string(),
        ]))
        .limit(5)
        .execute()
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    println!("{r3:?}");
    // --8<-- [end:basic_vector_search_q3]

    // --8<-- [start:basic_vector_search_q4]
    // Who are the strongest characters?
    let r4 = table
        .query()
        .select(Select::Columns(vec![
            "name".to_string(),
            "role".to_string(),
            "description".to_string(),
            "power".to_string(),
        ]))
        .execute()
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    println!("{r4:?}");
    // --8<-- [end:basic_vector_search_q4]

    // --8<-- [start:basic_drop_columns]
    table.drop_columns(&["power"]).await.unwrap();
    // --8<-- [end:basic_drop_columns]

    // --8<-- [start:basic_delete_rows]
    table.delete("role = 'Traitor Knight'").await.unwrap();
    // --8<-- [end:basic_delete_rows]

    // --8<-- [start:basic_drop_table]
    db.drop_table("camelot", &[]).await.unwrap();
    // --8<-- [end:basic_drop_table]
}
