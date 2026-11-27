// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{RecordBatch, RecordBatchIterator, RecordBatchReader, StringArray};
use arrow_schema::{DataType, Field, Schema};
fn make_quotes_reader(rows: Vec<(i64, &str, &str)>) -> Box<dyn RecordBatchReader + Send> {
    let ids: Vec<i64> = rows.iter().map(|(id, _, _)| *id).collect();
    let authors: Vec<&str> = rows.iter().map(|(_, author, _)| *author).collect();
    let quotes: Vec<&str> = rows.iter().map(|(_, _, quote)| *quote).collect();

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("author", DataType::Utf8, false),
        Field::new("quote", DataType::Utf8, false),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(arrow_array::Int64Array::from(ids)),
            Arc::new(StringArray::from(authors)),
            Arc::new(StringArray::from(quotes)),
        ],
    )
    .unwrap();
    Box::new(RecordBatchIterator::new(
        vec![Ok(batch)].into_iter(),
        schema,
    ))
}

#[tokio::test]
async fn checkpoint_delta_docs() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let db = lancedb::connect(directory.path().to_str().unwrap())
        .execute()
        .await?;

    // --8<-- [start:versioning_checkpoint_delta]
    use futures::TryStreamExt;

    let table = db
        .create_table(
            "checkpoint_delta_example",
            make_quotes_reader(vec![
                (1, "Richard", "Wubba Lubba Dub Dub!"),
                (2, "Morty", "Rick, what's going on?"),
            ]),
        )
        .storage_option("new_table_enable_stable_row_ids", "true")
        .execute()
        .await?;

    let mut tags = table.tags().await?;
    tags.create("baseline", table.version().await?).await?;
    table
        .add(make_quotes_reader(vec![(3, "Summer", "Whatever, Grandpa")]))
        .execute()
        .await?;
    table
        .update()
        .only_if("id = 2")
        .column("quote", "'Aww geez, Rick!'")
        .execute()
        .await?;
    table.delete("id = 1").await?;

    let delta = table.delta("baseline", None).await?;
    let mut inserted = delta.inserted_rows().await?;
    while let Some(batch) = inserted.try_next().await? {
        println!("inserted: {:?}", batch.column_by_name("id"));
    }
    let mut deleted = delta.deleted_row_ids().await?;
    while let Some(batch) = deleted.try_next().await? {
        println!("deleted row ids: {batch:?}");
    }
    for transaction in delta.list_transactions().await? {
        println!("{}", transaction.operation.name());
    }
    // --8<-- [end:versioning_checkpoint_delta]

    assert_eq!(table.version().await?, 4);

    let inserted = delta.inserted_rows().await?.try_collect::<Vec<_>>().await?;
    let inserted_ids: Vec<i64> = inserted
        .iter()
        .flat_map(|batch| {
            batch
                .column_by_name("id")
                .unwrap()
                .as_primitive::<Int64Type>()
                .values()
                .to_vec()
        })
        .collect();
    assert_eq!(inserted_ids, vec![3]);

    let updated = delta.updated_rows().await?.try_collect::<Vec<_>>().await?;
    let updated_ids: Vec<i64> = updated
        .iter()
        .flat_map(|batch| {
            batch
                .column_by_name("id")
                .unwrap()
                .as_primitive::<Int64Type>()
                .values()
                .to_vec()
        })
        .collect();
    assert_eq!(updated_ids, vec![2]);
    assert_eq!(
        delta
            .deleted_row_ids()
            .await?
            .try_collect::<Vec<_>>()
            .await?
            .len(),
        1
    );
    assert_eq!(
        delta
            .list_transactions()
            .await?
            .iter()
            .map(|transaction| transaction.operation.name())
            .collect::<Vec<_>>(),
        vec!["Append", "Update", "Delete"]
    );

    Ok(())
}
