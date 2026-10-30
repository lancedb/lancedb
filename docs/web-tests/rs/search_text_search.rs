// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

// --8<-- [start:example_body]
use arrow_array::{Int64Array, RecordBatch, RecordBatchIterator, StringArray};
use arrow_schema::{DataType, Field, Schema};
use futures::TryStreamExt;
use lance_index::scalar::FullTextSearchQuery;
use lancedb::index::{Index, scalar::FtsIndexBuilder};
use lancedb::query::{ExecutableQuery, QueryBase, Select};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let db = lancedb::connect(directory.path().to_str().unwrap())
        .execute()
        .await?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("text", DataType::Utf8, false),
        Field::new("category", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec![
                "A puppy playing in the park",
                "A dog resting at home",
                "A guide to growing tomatoes",
            ])),
            Arc::new(StringArray::from(vec!["animals", "animals", "gardening"])),
        ],
    )?;
    let reader = RecordBatchIterator::new(vec![Ok(batch)].into_iter(), schema);
    let table = db
        .create_table(
            "documents",
            Box::new(reader) as Box<dyn arrow_array::RecordBatchReader + Send>,
        )
        .execute()
        .await?;
    table
        .create_index(&["text"], Index::FTS(FtsIndexBuilder::default()))
        .execute()
        .await?;
    let query = table
        .query()
        .full_text_search(FullTextSearchQuery::new("puppy".into()).with_column("text".into())?)
        .select(Select::columns(&["id", "text", "_score"]))
        .limit(2);
    let rows: Vec<RecordBatch> = query.execute().await?.try_collect().await?;
    println!("{rows:?}");
    // --8<-- [end:example_body]
    let ids: Vec<i64> = rows
        .iter()
        .flat_map(|b| {
            b.column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect();
    assert_eq!(ids, vec![1]);
    let scores: Vec<f32> = rows
        .iter()
        .flat_map(|b| {
            b.column_by_name("_score")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow_array::Float32Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect();
    assert!(scores[0] > 0.0);
    // --8<-- [start:example_tail]
    Ok(())
}
// --8<-- [end:example_tail]
