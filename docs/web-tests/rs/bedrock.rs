// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::{iter::once, sync::Arc};

use arrow_array::{Float64Array, Int32Array, RecordBatch, RecordBatchIterator, StringArray};
use arrow_schema::{DataType, Field, Schema};
use aws_config::Region;
use aws_sdk_bedrockruntime::Client;
use futures::StreamExt;
use lancedb::{
    arrow::IntoArrow,
    connect,
    embeddings::{bedrock::BedrockEmbeddingFunction, EmbeddingDefinition, EmbeddingFunction},
    query::{ExecutableQuery, QueryBase},
    Result,
};

#[tokio::main]
async fn main() -> Result<()> {
    let tempdir = tempfile::tempdir().unwrap();
    let tempdir = tempdir.path().to_str().unwrap();

    // create Bedrock embedding function
    let region: String = "us-east-1".to_string();
    let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(Region::new(region))
        .load()
        .await;

    let embedding = Arc::new(BedrockEmbeddingFunction::new(
        Client::new(&config), // AWS Region
    ));

    let db = connect(tempdir).execute().await?;
    db.embedding_registry()
        .register("bedrock", embedding.clone())?;

    let table = db
        .create_table("vectors", make_data())
        .add_embedding(EmbeddingDefinition::new(
            "text",
            "bedrock",
            Some("embeddings"),
        ))?
        .execute()
        .await?;

    // execute vector search
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

fn make_data() -> impl IntoArrow {
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, true),
        Field::new("text", DataType::Utf8, false),
        Field::new("price", DataType::Float64, false),
    ]);

    let id = Int32Array::from(vec![1, 2, 3, 4]);
    let text = StringArray::from_iter_values(vec![
        "Black T-Shirt",
        "Leather Jacket",
        "Winter Parka",
        "Hooded Sweatshirt",
    ]);
    let price = Float64Array::from(vec![10.0, 50.0, 100.0, 30.0]);
    let schema = Arc::new(schema);
    let rb = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(id), Arc::new(text), Arc::new(price)],
    )
    .unwrap();
    Box::new(RecordBatchIterator::new(vec![Ok(rb)], schema))
}
