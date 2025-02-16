// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! This example demonstrates setting advanced parameters when building an IVF PQ index
//!
//! Snippets from this example are used in the documentation on ANN indices.

use std::sync::Arc;

use arrow_array::types::Float32Type;
use arrow_array::{
    FixedSizeListArray, Int32Array, RecordBatch, RecordBatchIterator, RecordBatchReader,
};
use arrow_schema::{DataType, Field, Schema};

use futures::TryStreamExt;
use lancedb::connection::Connection;
use lancedb::index::vector::IvfPqIndexBuilder;
use lancedb::index::Index;
use lancedb::query::{ExecutableQuery, QueryBase};
use lancedb::{connect, DistanceType, Result, Table};

#[tokio::main]
async fn main() -> Result<()> {
    if std::path::Path::new("data").exists() {
        std::fs::remove_dir_all("data").unwrap();
    }
    let uri = "data/sample-lancedb";
    let db = connect(uri).execute().await?;
    let tbl = create_table(&db).await?;

    create_index(&tbl).await?;
    search_index(&tbl).await?;
    Ok(())
}

fn create_some_records() -> Result<Box<dyn RecordBatchReader + Send>> {
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

async fn create_table(db: &Connection) -> Result<Table> {
    let initial_data: Box<dyn RecordBatchReader + Send> = create_some_records()?;
    let tbl = db
        .create_table("my_table", Box::new(initial_data))
        .execute()
        .await
        .unwrap();
    Ok(tbl)
}

async fn create_index(table: &Table) -> Result<()> {
    // --8<-- [start:create_index]
    // For this example, `table` is a lancedb::Table with a column named
    // "vector" that is a vector column with dimension 128.

    // By default, if the column "vector" appears to be a vector column,
    // then an IVF_PQ index with reasonable defaults is created.
    table
        .create_index(&["vector"], Index::Auto)
        .execute()
        .await?;
    // For advanced cases, it is also possible to specifically request an
    // IVF_PQ index and provide custom parameters.
    table
        .create_index(
            &["vector"],
            Index::IvfPq(
                // Here we specify advanced indexing parameters.  In this case
                // we are creating an index that my have better recall than the
                // default but is also larger and slower.
                IvfPqIndexBuilder::default()
                    // This overrides the default distance type of l2
                    .distance_type(DistanceType::Cosine)
                    // With 1000 rows this have been ~31 by default
                    .num_partitions(50)
                    // With dimension 128 this would have been 8 by default
                    .num_sub_vectors(16),
            ),
        )
        .execute()
        .await?;
    // --8<-- [end:create_index]
    Ok(())
}

async fn search_index(table: &Table) -> Result<()> {
    // --8<-- [start:search1]
    let query_vector = [1.0; 128];
    // By default the index will find the 10 closest results using default
    // search parameters that give a reasonable tradeoff between accuracy
    // and search latency
    let mut results = table
        .vector_search(&query_vector)?
        // Note: you should always set the distance_type to match the value used
        // to train the index
        .distance_type(DistanceType::Cosine)
        .execute()
        .await?;
    while let Some(batch) = results.try_next().await? {
        println!("{:?}", batch);
    }
    // We can also provide custom search parameters.  Here we perform a
    // slower but more accurate search
    let mut results = table
        .vector_search(&query_vector)?
        .distance_type(DistanceType::Cosine)
        // Override the default of 10 to get more rows
        .limit(15)
        // Override the default of 20 to search more partitions
        .nprobes(30)
        // Override the default of None to apply a refine step
        .refine_factor(1)
        .execute()
        .await?;
    while let Some(batch) = results.try_next().await? {
        println!("{:?}", batch);
    }
    Ok(())
    // --8<-- [end:search1]
}
