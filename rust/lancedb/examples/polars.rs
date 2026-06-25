// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! This example demonstrates ingesting a Polars DataFrame into LanceDB and
//! reading it back out as a Polars DataFrame.

use lancedb::arrow::IntoPolars;
use lancedb::query::ExecutableQuery;
use lancedb::{Result, connect};
use polars::prelude::{DataFrame, NamedFrom, Series};

fn make_dataframe() -> DataFrame {
    let ids = Series::new("id", &[1i32, 2, 3, 4, 5]);
    let names = Series::new("name", &["Alice", "Bob", "Carol", "Dave", "Eve"]);
    let scores = Series::new("score", &[9.5f64, 8.1, 7.3, 9.0, 6.5]);
    DataFrame::new(vec![ids, names, scores]).unwrap()
}

#[tokio::main]
async fn main() -> Result<()> {
    let tmp = tempfile::tempdir().unwrap();
    let db = connect(tmp.path().to_str().unwrap()).execute().await?;

    // Ingest a Polars DataFrame directly — DataFrame now implements Scannable.
    let df = make_dataframe();
    println!("Input DataFrame:\n{df}");

    let table = db.create_table("people", df).execute().await?;

    // Append more rows.
    let more = DataFrame::new(vec![
        Series::new("id", &[6i32, 7]),
        Series::new("name", &["Frank", "Grace"]),
        Series::new("score", &[7.8f64, 8.9]),
    ])
    .unwrap();
    table.add(more).execute().await?;

    // Read back as a Polars DataFrame.
    let result_df = table
        .query()
        .execute()
        .await?
        .into_polars()
        .await?;

    println!("\nRound-tripped DataFrame ({} rows):\n{result_df}", result_df.height());
    Ok(())
}
