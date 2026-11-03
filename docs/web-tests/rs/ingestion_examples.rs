// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

// --8<-- [start:write_workflow]
use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, RecordBatchIterator, RecordBatchReader, StringArray};
use arrow_schema::{DataType, Field, Schema};
use futures::TryStreamExt;
use lancedb::query::{ExecutableQuery, QueryBase};

fn make_users_reader(rows: &[(i64, &str, i64)]) -> Box<dyn RecordBatchReader + Send> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("login_count", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.0))),
            Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.1))),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.2))),
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
        .create_table(
            "users",
            make_users_reader(&[(1, "Alice", 10), (2, "Bob", 20)]),
        )
        .execute()
        .await?;
    table
        .add(make_users_reader(&[(3, "Charlie", 5)]))
        .execute()
        .await?;
    table
        .update()
        .only_if("id = 2")
        .column("name", "'Bobby'")
        .execute()
        .await?;
    table
        .update()
        .only_if("id = 2")
        .column("login_count", "login_count + 1")
        .execute()
        .await?;
    let mut merge = table.merge_insert(&["id"]);
    merge
        .when_matched_update_all(None)
        .when_not_matched_insert_all();
    merge
        .execute(make_users_reader(&[(3, "Charlie", 6), (4, "Dana", 1)]))
        .await?;
    let selected: Vec<RecordBatch> = table
        .query()
        .only_if("id = 4")
        .execute()
        .await?
        .try_collect()
        .await?;
    println!("Rows selected for deletion: {selected:?}");
    table.delete("id = 4").await?;
    let rows: Vec<RecordBatch> = table.query().execute().await?.try_collect().await?;
    println!("Remaining rows: {rows:?}");
    assert_eq!(table.count_rows(None).await?, 3);
    assert_eq!(
        table
            .count_rows(Some(
                "id = 2 AND name = 'Bobby' AND login_count = 21".into()
            ))
            .await?,
        1
    );
    assert_eq!(
        table
            .count_rows(Some("id = 3 AND login_count = 6".into()))
            .await?,
        1
    );
    Ok(())
}
// --8<-- [end:write_workflow]
