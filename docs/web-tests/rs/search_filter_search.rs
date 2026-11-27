// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

// --8<-- [start:example_body]
use arrow_array::types::Float32Type;
use arrow_array::{FixedSizeListArray, Int64Array, RecordBatch, RecordBatchIterator, StringArray};
use arrow_schema::{DataType, Field, Schema};
use futures::TryStreamExt;
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
        Field::new(
            "vector",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 2),
            true,
        ),
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
            Arc::new(
                FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
                    [[1.0, 0.0], [0.9, 0.1], [0.0, 1.0]]
                        .into_iter()
                        .map(|v| Some(v.into_iter().map(Some).collect::<Vec<_>>())),
                    2,
                ),
            ),
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
    let query = table
        .vector_search(&[0.0_f32, 1.0])?
        .only_if("category = 'animals'")
        .select(Select::columns(&["id", "text", "category", "_distance"]))
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
    assert_eq!(
        ids.into_iter().collect::<std::collections::BTreeSet<_>>(),
        std::collections::BTreeSet::from([1, 2])
    );
    assert!(rows.iter().all(|b| {
        b.column_by_name("category")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .all(|v| v == Some("animals"))
    }));
    // --8<-- [start:example_tail]
    Ok(())
}
// --8<-- [end:example_tail]
