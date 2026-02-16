// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Example demonstrating the expression builder API for type-safe filters.
//!
//! Run with: cargo run --example expr_api

use std::sync::Arc;

use arrow_array::types::Float32Type;
use arrow_array::{FixedSizeListArray, Int32Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use futures::TryStreamExt;
use lancedb::expr::{col, contains, expr_to_sql_string, lit, lower};
use lancedb::query::{ExecutableQuery, QueryBase};
use lancedb::{connect, Result, Table};

#[tokio::main]
async fn main() -> Result<()> {
    let uri = "data/expr-api-demo";
    if std::path::Path::new(uri).exists() {
        std::fs::remove_dir_all(uri).unwrap();
    }

    let db = connect(uri).execute().await?;
    let table = create_table(&db).await?;

    println!("=== Expression API Demo ===\n");

    println!("1. Basic filter: id > 5");
    let expr = col("id").gt(lit(5));
    println!("   SQL: {}", expr_to_sql_string(&expr)?);
    let batches = table
        .query()
        .only_if_expr(expr)
        .limit(3)
        .execute()
        .await?
        .try_collect::<Vec<_>>()
        .await?;
    println!("   Rows returned: {}\n", total_rows(&batches));

    println!("2. Compound filter: id > 2 AND id < 8");
    let expr = col("id").gt(lit(2)).and(col("id").lt(lit(8)));
    println!("   SQL: {}", expr_to_sql_string(&expr)?);
    let batches = table
        .query()
        .only_if_expr(expr)
        .limit(5)
        .execute()
        .await?
        .try_collect::<Vec<_>>()
        .await?;
    println!("   Rows returned: {}\n", total_rows(&batches));

    println!("3. String filter: name = 'Alice'");
    let expr = col("name").eq(lit("Alice"));
    println!("   SQL: {}", expr_to_sql_string(&expr)?);
    let batches = table
        .query()
        .only_if_expr(expr)
        .execute()
        .await?
        .try_collect::<Vec<_>>()
        .await?;
    println!("   Rows returned: {}\n", total_rows(&batches));

    println!("4. String function: LOWER(name) = 'bob'");
    let expr = lower(col("name")).eq(lit("bob"));
    println!("   SQL: {}", expr_to_sql_string(&expr)?);
    let batches = table
        .query()
        .only_if_expr(expr)
        .execute()
        .await?
        .try_collect::<Vec<_>>()
        .await?;
    println!("   Rows returned: {}\n", total_rows(&batches));

    println!("5. Contains: name CONTAINS 'li'");
    let expr = contains(col("name"), lit("li"));
    println!("   SQL: {}", expr_to_sql_string(&expr)?);
    let batches = table
        .query()
        .only_if_expr(expr)
        .execute()
        .await?
        .try_collect::<Vec<_>>()
        .await?;
    println!("   Rows returned: {}\n", total_rows(&batches));

    println!("6. Equivalent SQL filter (for comparison)");
    let batches_sql = table
        .query()
        .only_if("id > 5")
        .limit(3)
        .execute()
        .await?
        .try_collect::<Vec<_>>()
        .await?;
    let batches_expr = table
        .query()
        .only_if_expr(col("id").gt(lit(5)))
        .limit(3)
        .execute()
        .await?
        .try_collect::<Vec<_>>()
        .await?;
    println!(
        "   SQL filter rows: {}, Expr filter rows: {} (should match)\n",
        total_rows(&batches_sql),
        total_rows(&batches_expr)
    );

    std::fs::remove_dir_all(uri).ok();
    println!("=== Demo complete ===");
    Ok(())
}

async fn create_table(db: &lancedb::connection::Connection) -> Result<Table> {
    const DIM: usize = 8;
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new(
            "vector",
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, true)),
                DIM as i32,
            ),
            true,
        ),
    ]));

    let names = [
        "Alice", "Bob", "Charlie", "alice", "bob", "David", "Eve", "Frank",
    ];
    let data = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from_iter_values(0..8)),
            Arc::new(StringArray::from_iter_values(names.iter().map(|s| *s))),
            Arc::new(
                FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
                    (0..8).map(|_| Some(vec![Some(0.1); DIM])),
                    DIM as i32,
                ),
            ),
        ],
    )?;

    db.create_table("expr_demo", data).execute().await
}

fn total_rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}
