// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

// --8<-- [start:example]
use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, RecordBatchIterator, RecordBatchReader, StringArray};
use arrow_schema::{DataType, Field, Schema};
use futures::TryStreamExt;
use lancedb::query::ExecutableQuery;

fn make_users_reader(rows: &[(i64, &str)]) -> Box<dyn RecordBatchReader + Send> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.0))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.1))),
        ],
    )
    .unwrap();
    Box::new(RecordBatchIterator::new(
        vec![Ok(batch)].into_iter(),
        schema,
    ))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let db = lancedb::connect(directory.path().to_str().unwrap())
        .execute()
        .await?;
    let table = db
        .create_table("users", make_users_reader(&[(1, "Alice")]))
        .execute()
        .await?;
    table
        .add(make_users_reader(&[(2, "Bob"), (3, "Charlie")]))
        .progress(|progress| println!("Rows written: {}", progress.output_rows()))
        .execute()
        .await?;
    let rows: Vec<RecordBatch> = table.query().execute().await?.try_collect().await?;
    println!("After insertion: {rows:?}");
    assert_eq!(table.count_rows(None).await?, 3);
    assert_eq!(
        table
            .count_rows(Some("id = 1 AND name = 'Alice'".into()))
            .await?,
        1
    );
    assert_eq!(
        table
            .count_rows(Some("id = 2 AND name = 'Bob'".into()))
            .await?,
        1
    );
    assert_eq!(table.count_rows(Some("id = 3".into())).await?, 1);
    Ok(())
}
// --8<-- [end:example]
