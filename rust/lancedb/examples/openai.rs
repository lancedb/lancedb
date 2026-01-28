// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

// --8<-- [start:imports]

use std::{iter::once, sync::Arc};

use arrow_array::{RecordBatch, StringArray};
use futures::StreamExt;
use lancedb::{
    connect,
    embeddings::{openai::OpenAIEmbeddingFunction, EmbeddingDefinition, EmbeddingFunction},
    query::{ExecutableQuery, QueryBase},
    Result,
};

// --8<-- [end:imports]

// --8<-- [start:openai_embeddings]
#[tokio::main]
async fn main() -> Result<()> {
    let tempdir = tempfile::tempdir().unwrap();
    let tempdir = tempdir.path().to_str().unwrap();
    let api_key = std::env::var("OPENAI_API_KEY").expect("OPENAI_API_KEY is not set");
    let embedding = Arc::new(OpenAIEmbeddingFunction::new_with_model(
        api_key,
        "text-embedding-3-large",
    )?);

    let db = connect(tempdir).execute().await?;
    db.embedding_registry()
        .register("openai", embedding.clone())?;

    let table = db
        .create_table("vectors", make_data())
        .add_embedding(EmbeddingDefinition::new(
            "text",
            "openai",
            Some("embeddings"),
        ))?
        .execute()
        .await?;

    let query = Arc::new(StringArray::from_iter_values(once("something warm")));
    let query_vector = embedding.compute_query_embeddings(query)?;
    let mut results = table
        .vector_search(query_vector)?
        .limit(1)
        .execute()
        .await?;

    let rb = results.next().await.unwrap()?;
    let out = rb
        .column_by_name("text")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    let text = out.iter().next().unwrap().unwrap();
    println!("Closest match: {}", text);
    Ok(())
}
// --8<-- [end:openai_embeddings]

fn make_data() -> RecordBatch {
    arrow_array::record_batch!(
        ("id", Int32, [1, 2, 3, 4]),
        (
            "text",
            Utf8,
            [
                "Black T-Shirt",
                "Leather Jacket",
                "Winter Parka",
                "Hooded Sweatshirt"
            ]
        ),
        ("price", Float64, [10.0, 50.0, 100.0, 30.0])
    ).unwrap()
}
