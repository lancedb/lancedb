// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Example: LanceDB Arrow Flight SQL Server
//!
//! This example demonstrates how to:
//! 1. Create a LanceDB database with sample data
//! 2. Start an Arrow Flight SQL server
//! 3. Connect with a Flight SQL client
//! 4. Run SQL queries including vector_search and fts table functions
//!
//! Run with: `cargo run --features flight --example flight_sql`

use std::sync::Arc;

use arrow_array::{FixedSizeListArray, Float32Array, Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use lance_arrow::FixedSizeListArrayExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging if env_logger is available
    let _ = std::env::var("RUST_LOG").ok();

    // 1. Create an in-memory LanceDB database
    let db = lancedb::connect("memory://flight_sql_demo")
        .execute()
        .await?;

    // 2. Create a table with text and vector data
    let dim = 4i32;
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("text", DataType::Utf8, false),
        Field::new(
            "vector",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), dim),
            true,
        ),
    ]));

    let ids = Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8]);
    let texts = StringArray::from(vec![
        "the quick brown fox jumps over the lazy dog",
        "a fast red fox leaps across the sleeping hound",
        "machine learning models process natural language",
        "neural networks learn from training data",
        "the brown dog chases the red fox through the forest",
        "deep learning algorithms improve with more data",
        "a lazy cat sleeps on the warm windowsill",
        "vector databases enable fast similarity search",
    ]);
    let flat_values = Float32Array::from(vec![
        1.0, 0.0, 0.0, 0.0, // fox-like
        0.9, 0.1, 0.0, 0.0, // similar to fox
        0.0, 1.0, 0.0, 0.0, // ML-like
        0.0, 0.9, 0.1, 0.0, // similar to ML
        0.7, 0.3, 0.0, 0.0, // fox+dog mix
        0.0, 0.8, 0.2, 0.0, // ML-like
        0.1, 0.0, 0.0, 0.9, // cat-like
        0.0, 0.5, 0.5, 0.0, // tech-like
    ]);
    let vector_array = FixedSizeListArray::try_new_from_values(flat_values, dim)?;

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(ids), Arc::new(texts), Arc::new(vector_array)],
    )?;

    let table = db.create_table("documents", batch).execute().await?;

    // 3. Create indices
    println!("Creating FTS index on 'text' column...");
    table
        .create_index(&["text"], lancedb::index::Index::FTS(Default::default()))
        .execute()
        .await?;

    println!("Database ready with {} rows", table.count_rows(None).await?);

    // 4. Start Flight SQL server
    let addr = "0.0.0.0:50051".parse()?;
    println!("Starting Arrow Flight SQL server on {}...", addr);
    println!();
    println!("Connect with any ADBC Flight SQL client:");
    println!("  URI: grpc://localhost:50051");
    println!();
    println!("Example SQL queries:");
    println!("  SELECT * FROM documents LIMIT 5;");
    println!("  SELECT * FROM vector_search('documents', '[1.0, 0.0, 0.0, 0.0]', 3);");
    println!(
        "  SELECT * FROM fts('documents', '{{\"match\": {{\"column\": \"text\", \"terms\": \"fox\"}}}}');"
    );
    println!();

    lancedb::flight::serve(db, addr).await?;

    Ok(())
}
