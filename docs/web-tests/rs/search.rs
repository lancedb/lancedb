// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! This example demonstrates vector search query options.
//!
//! Snippets from this example are used in the documentation on vector search.

use std::sync::Arc;

use arrow_array::types::Float32Type;
use arrow_array::{
    Array, FixedSizeListArray, Int32Array, Int64Array, RecordBatch, RecordBatchIterator,
    RecordBatchReader, UInt8Array,
};
use arrow_schema::{DataType, Field, Schema};

use futures::TryStreamExt;
use lancedb::connection::Connection;
use lancedb::index::vector::IvfFlatIndexBuilder;
use lancedb::index::Index;
use lancedb::query::{ExecutableQuery, QueryBase, Select};
use lancedb::{connect, DistanceType, Result, Table};

const DIM: usize = 128;

#[tokio::main]
async fn main() -> Result<()> {
    if std::path::Path::new("data").exists() {
        std::fs::remove_dir_all("data").unwrap();
    }
    let uri = "data/sample-lancedb";
    let db = connect(uri).execute().await?;
    let tbl = create_table(&db).await?;
    tbl.create_index(&["vector"], Index::Auto).execute().await?;

    configure_distance_metric(&tbl).await?;
    exact_vs_approximate(&tbl).await?;
    search_distance_range(&tbl).await?;
    vector_search_prefilter(&tbl).await?;
    vector_search_postfilter(&tbl).await?;
    fast_search(&tbl).await?;
    brute_force_search(&tbl).await?;
    bypass_vector_index(&tbl).await?;
    batch_search(&tbl).await?;
    binary_search(&db).await?;
    Ok(())
}

fn create_some_records() -> Result<Box<dyn RecordBatchReader + Send>> {
    const TOTAL: usize = 1000;

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
        .create_table("my_vectors", initial_data)
        .execute()
        .await
        .unwrap();
    Ok(tbl)
}

async fn configure_distance_metric(table: &Table) -> Result<()> {
    let query_vector = [1.0; DIM];
    // --8<-- [start:configure_distance_metric]
    // Use the same distance metric the index was trained with.
    let mut results = table
        .vector_search(&query_vector)?
        .distance_type(DistanceType::Cosine)
        .limit(10)
        .execute()
        .await?;
    while let Some(batch) = results.try_next().await? {
        println!("{:?}", batch);
    }
    // --8<-- [end:configure_distance_metric]
    Ok(())
}

async fn exact_vs_approximate(table: &Table) -> Result<()> {
    let query_vector = [1.0; DIM];
    // --8<-- [start:exact_vs_approximate]
    // Approximate ANN search (fast, distances may come from the index representation)
    let mut fast_results = table.vector_search(&query_vector)?.limit(10).execute().await?;
    while let Some(batch) = fast_results.try_next().await? {
        println!("{:?}", batch);
    }

    // Rerank a larger candidate set on full vectors for better recall
    let mut refined_results = table
        .vector_search(&query_vector)?
        .limit(10)
        .refine_factor(20)
        .execute()
        .await?;
    while let Some(batch) = refined_results.try_next().await? {
        println!("{:?}", batch);
    }
    // --8<-- [end:exact_vs_approximate]
    Ok(())
}

async fn search_distance_range(table: &Table) -> Result<()> {
    let query_vector = [1.0; DIM];
    // --8<-- [start:search_distance_range]
    // Only return rows whose distance falls within [0.1, 0.5).
    let mut results = table
        .vector_search(&query_vector)?
        .distance_range(Some(0.1), Some(0.5))
        .execute()
        .await?;
    while let Some(batch) = results.try_next().await? {
        println!("{:?}", batch);
    }
    // --8<-- [end:search_distance_range]
    Ok(())
}

async fn vector_search_prefilter(table: &Table) -> Result<()> {
    let query_vector = [1.0; DIM];
    // --8<-- [start:vector_search_prefilter]
    // Prefiltering is the default: the filter is applied before vector search.
    let mut results = table
        .vector_search(&query_vector)?
        .only_if("id > 100")
        .select(Select::columns(&["id"]))
        .limit(5)
        .execute()
        .await?;
    while let Some(batch) = results.try_next().await? {
        println!("{:?}", batch);
    }
    // --8<-- [end:vector_search_prefilter]
    Ok(())
}

async fn vector_search_postfilter(table: &Table) -> Result<()> {
    let query_vector = [1.0; DIM];
    // --8<-- [start:vector_search_postfilter]
    // Apply the filter after vector search by calling postfilter().
    let mut results = table
        .vector_search(&query_vector)?
        .only_if("id > 100")
        .postfilter()
        .select(Select::columns(&["id"]))
        .limit(5)
        .execute()
        .await?;
    while let Some(batch) = results.try_next().await? {
        println!("{:?}", batch);
    }
    // --8<-- [end:vector_search_postfilter]
    Ok(())
}

async fn fast_search(table: &Table) -> Result<()> {
    let query_vector = [1.0; DIM];
    // --8<-- [start:fast_search]
    // Skip unindexed data for lower latency.
    let mut results = table
        .vector_search(&query_vector)?
        .fast_search()
        .limit(5)
        .execute()
        .await?;
    while let Some(batch) = results.try_next().await? {
        println!("{:?}", batch);
    }
    // --8<-- [end:fast_search]
    Ok(())
}

async fn brute_force_search(table: &Table) -> Result<()> {
    let query_vector = [1.0; DIM];
    // --8<-- [start:brute_force_search]
    // A plain vector search returns the top-k closest rows.
    let mut results = table.vector_search(&query_vector)?.limit(3).execute().await?;
    while let Some(batch) = results.try_next().await? {
        println!("{:?}", batch);
    }
    // --8<-- [end:brute_force_search]
    Ok(())
}

async fn bypass_vector_index(table: &Table) -> Result<()> {
    let query_vector = [1.0; DIM];
    // --8<-- [start:bypass_vector_index]
    // Force an exhaustive (flat) scan for exact, ground-truth results.
    let mut results = table
        .vector_search(&query_vector)?
        .bypass_vector_index()
        .limit(5)
        .execute()
        .await?;
    while let Some(batch) = results.try_next().await? {
        println!("{:?}", batch);
    }
    // --8<-- [end:bypass_vector_index]
    Ok(())
}

async fn batch_search(table: &Table) -> Result<()> {
    let query_1 = [1.0; DIM];
    let query_2 = [0.5; DIM];
    // --8<-- [start:batch_search]
    // Search multiple query vectors in one call. Each result row carries a
    // `query_index` mapping it back to the query it matched.
    let mut results = table
        .vector_search(&query_1)?
        .add_query_vector(&query_2)?
        .limit(5)
        .execute()
        .await?;
    while let Some(batch) = results.try_next().await? {
        println!("{:?}", batch);
    }
    // --8<-- [end:batch_search]
    Ok(())
}

async fn binary_search(db: &Connection) -> Result<()> {
    // A 256-bit binary vector is stored as 256 / 8 = 32 packed uint8 bytes.
    const NUM_BYTES: i32 = 32;
    const TOTAL: usize = 1024;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(
            "vector",
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::UInt8, true)),
                NUM_BYTES,
            ),
            true,
        ),
    ]));

    let values =
        UInt8Array::from_iter_values((0..TOTAL * NUM_BYTES as usize).map(|i| (i % 256) as u8));
    let vectors = FixedSizeListArray::try_new(
        Arc::new(Field::new("item", DataType::UInt8, true)),
        NUM_BYTES,
        Arc::new(values),
        None,
    )
    .unwrap();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from_iter_values(0..TOTAL as i64)),
            Arc::new(vectors),
        ],
    )
    .unwrap();
    let reader: Box<dyn RecordBatchReader + Send> =
        Box::new(RecordBatchIterator::new(vec![Ok(batch)].into_iter(), schema.clone()));
    let tbl = db.create_table("binary_vectors", reader).execute().await?;
    tbl.create_index(
        &["vector"],
        Index::IvfFlat(IvfFlatIndexBuilder::default().distance_type(DistanceType::Hamming)),
    )
    .execute()
    .await?;

    // --8<-- [start:binary_search]
    // Binary vectors use `hamming` distance over the packed uint8 bytes.
    let query: Arc<dyn Array> = Arc::new(UInt8Array::from(vec![1u8; NUM_BYTES as usize]));
    let mut results = tbl
        .vector_search(query)?
        .distance_type(DistanceType::Hamming)
        .limit(10)
        .execute()
        .await?;
    while let Some(batch) = results.try_next().await? {
        println!("{:?}", batch);
    }
    // --8<-- [end:binary_search]
    Ok(())
}
