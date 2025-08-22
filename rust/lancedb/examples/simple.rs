// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! This example demonstrates basic usage of LanceDb.
//!
//! Snippets from this example are used in the quickstart documentation.

use std::sync::Arc;

use arrow_array::types::Float32Type;
use arrow_array::{FixedSizeListArray, Int32Array, RecordBatch, RecordBatchIterator};
use arrow_schema::{DataType, Field, Schema};
use futures::TryStreamExt;

use lancedb::arrow::IntoArrow;
use lancedb::connection::Connection;
use lancedb::index::Index;
use lancedb::query::{ExecutableQuery, QueryBase};
use lancedb::{connect, Result, Table as LanceDbTable};

#[tokio::main]
async fn main() -> Result<()> {
    if std::path::Path::new("data").exists() {
        std::fs::remove_dir_all("data").unwrap();
    }
    // --8<-- [start:connect]
    let uri = "data/sample-lancedb";
    let db = connect(uri).execute().await?;
    // --8<-- [end:connect]

    // --8<-- [start:list_names]
    println!("{:?}", db.table_names().execute().await?);
    // --8<-- [end:list_names]
    let tbl = create_table(&db).await?;
    create_index(&tbl).await?;
    let batches = search(&tbl).await?;
    println!("{:?}", batches);

    create_empty_table(&db).await.unwrap();

    // --8<-- [start:delete]
    tbl.delete("id > 24").await.unwrap();
    // --8<-- [end:delete]

    // --8<-- [start:drop_table]
    db.drop_table("my_table", &[]).await.unwrap();
    // --8<-- [end:drop_table]
    Ok(())
}

#[allow(dead_code)]
async fn open_with_existing_tbl() -> Result<()> {
    let uri = "data/sample-lancedb";
    let db = connect(uri).execute().await?;
    #[allow(unused_variables)]
    // --8<-- [start:open_existing_tbl]
    let table = db.open_table("my_table").execute().await.unwrap();
    // --8<-- [end:open_existing_tbl]
    Ok(())
}

fn create_some_records() -> Result<impl IntoArrow> {
    const TOTAL: usize = 1000;
    const DIM: usize = 128;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new(
            "vector",
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, true)),
                DIM as i32,
            ),
            true,
        ),
    ]));

    // Create a RecordBatch stream.
    let batches = RecordBatchIterator::new(
        vec![RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from_iter_values(0..TOTAL as i32)),
                Arc::new(
                    FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
                        (0..TOTAL).map(|_| Some(vec![Some(1.0); DIM])),
                        DIM as i32,
                    ),
                ),
            ],
        )
        .unwrap()]
        .into_iter()
        .map(Ok),
        schema.clone(),
    );
    Ok(Box::new(batches))
}

async fn create_table(db: &Connection) -> Result<LanceDbTable> {
    // --8<-- [start:create_table]
    let initial_data = create_some_records()?;
    let tbl = db
        .create_table("my_table", initial_data)
        .execute()
        .await
        .unwrap();
    // --8<-- [end:create_table]

    // --8<-- [start:add]
    let new_data = create_some_records()?;
    tbl.add(new_data).execute().await.unwrap();
    // --8<-- [end:add]

    Ok(tbl)
}

async fn create_empty_table(db: &Connection) -> Result<LanceDbTable> {
    // --8<-- [start:create_empty_table]
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("item", DataType::Utf8, true),
    ]));
    db.create_empty_table("empty_table", schema).execute().await
    // --8<-- [end:create_empty_table]
}

async fn create_index(table: &LanceDbTable) -> Result<()> {
    // --8<-- [start:create_index]
    table.create_index(&["vector"], Index::Auto).execute().await
    // --8<-- [end:create_index]
}

async fn search(table: &LanceDbTable) -> Result<Vec<RecordBatch>> {
    // --8<-- [start:search]
    table
        .query()
        .limit(2)
        .nearest_to(&[1.0; 128])?
        .execute()
        .await?
        .try_collect::<Vec<_>>()
        .await
    // --8<-- [end:search]
}
