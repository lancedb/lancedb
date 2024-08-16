// Copyright 2024 Lance Developers.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use arrow_array::{Int32Array, RecordBatch, RecordBatchIterator, RecordBatchReader, StringArray};
use arrow_schema::{DataType, Field, Schema};

use futures::TryStreamExt;
use lance_index::scalar::FullTextSearchQuery;
use lancedb::connection::Connection;
use lancedb::index::scalar::FtsIndexBuilder;
use lancedb::index::Index;
use lancedb::query::{ExecutableQuery, QueryBase};
use lancedb::{connect, Result, Table};
use rand::random;

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

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("doc", DataType::Utf8, true),
    ]));

    let words = random_word::all(random_word::Lang::En)
        .iter()
        .step_by(1024)
        .take(500)
        .map(|w| *w)
        .collect::<Vec<_>>();
    let n_terms = 3;
    let batches = RecordBatchIterator::new(
        vec![RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from_iter_values(0..TOTAL as i32)),
                Arc::new(StringArray::from_iter_values((0..TOTAL).map(|_| {
                    (0..n_terms)
                        .map(|_| words[random::<usize>() % words.len()])
                        .collect::<Vec<_>>()
                        .join(" ")
                }))),
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
    let tbl = db.create_table("my_table", initial_data).execute().await?;
    Ok(tbl)
}

async fn create_index(table: &Table) -> Result<()> {
    table
        .create_index(&["doc"], Index::FTS(FtsIndexBuilder::default()))
        .execute()
        .await?;
    Ok(())
}

async fn search_index(table: &Table) -> Result<()> {
    let words = random_word::all(random_word::Lang::En)
        .iter()
        .step_by(1024)
        .take(500)
        .map(|w| *w)
        .collect::<Vec<_>>();
    let query = words[0].to_owned();
    println!("Searching for: {}", query);

    let mut results = table
        .query()
        .full_text_search(FullTextSearchQuery::new(words[0].to_owned()))
        .select(lancedb::query::Select::Columns(vec!["doc".to_owned()]))
        .limit(10)
        .execute()
        .await?;
    while let Some(batch) = results.try_next().await? {
        println!("{:?}", batch);
    }
    Ok(())
}
