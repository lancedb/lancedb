// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! This example demonstrates writing Rust structs into LanceDB and reading
//! query results back as structs, using the `serde` feature.

use std::sync::Arc;

use lancedb::arrow::arrow_schema::{DataType, Field, Schema};
use lancedb::arrow::{IntoStructs, from_structs, from_structs_with_schema};
use lancedb::query::{ExecutableQuery, QueryBase};
use lancedb::{Result, connect};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct Doc {
    id: i64,
    text: String,
    vector: Vec<f32>,
}

#[derive(Deserialize)]
struct Hit {
    id: i64,
    text: String,
    _distance: f32,
}

#[tokio::main]
async fn main() -> Result<()> {
    let db = connect("memory://").execute().await?;

    // Declare the vector column as a FixedSizeList so the table can be searched.
    // `from_structs` alone would store `Vec<f32>` as a variable-length list.
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("text", DataType::Utf8, false),
        Field::new(
            "vector",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 3),
            false,
        ),
    ]));
    let docs = vec![
        Doc {
            id: 1,
            text: "apple".into(),
            vector: vec![1.0, 0.0, 0.0],
        },
        Doc {
            id: 2,
            text: "banana".into(),
            vector: vec![0.0, 1.0, 0.0],
        },
    ];
    let table = db
        .create_table("docs", from_structs_with_schema(&docs, schema)?)
        .execute()
        .await?;

    // Appending can use the schema traced from the struct; the data is cast to
    // the table's schema on write.
    let more = vec![Doc {
        id: 3,
        text: "cherry".into(),
        vector: vec![0.0, 0.0, 1.0],
    }];
    table.add(from_structs(&more)?).execute().await?;

    let hits: Vec<Hit> = table
        .query()
        .nearest_to(&[0.0f32, 0.9, 0.1])?
        .limit(2)
        .execute()
        .await?
        .into_structs()
        .await?;
    for hit in hits {
        println!("{} {} (distance {:.3})", hit.id, hit.text, hit._distance);
    }
    Ok(())
}
