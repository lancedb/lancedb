// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use arrow_array::{RecordBatch, RecordBatchIterator, StringArray};
use arrow_schema::{DataType, Field, Schema};
use futures::TryStreamExt;
use lance_index::scalar::FullTextSearchQuery;
use lancedb::index::scalar::FtsIndexBuilder;
use lancedb::index::Index;
use lancedb::{
    arrow::IntoArrow,
    connect,
    embeddings::{
        sentence_transformers::SentenceTransformersEmbeddings, EmbeddingDefinition,
        EmbeddingFunction,
    },
    query::{QueryBase, QueryExecutionOptions},
    Result, Table,
};
use std::{iter::once, sync::Arc};

#[tokio::main]
async fn main() -> Result<()> {
    let tempdir = tempfile::tempdir().unwrap();
    let tempdir = tempdir.path().to_str().unwrap();
    let embedding = SentenceTransformersEmbeddings::builder().build()?;
    let embedding = Arc::new(embedding);
    let db = connect(tempdir).execute().await?;
    db.embedding_registry()
        .register("sentence-transformers", embedding.clone())?;

    // Create the table with embeddings
    let table = db
        .create_table("vectors", make_data())
        .add_embedding(EmbeddingDefinition::new(
            "facts",
            "sentence-transformers",
            Some("embeddings"),
        ))?
        .execute()
        .await?;

    // Create the FTS index
    create_index(&table).await?;

    // Perform a hybrid search using the FTS index and embeddings
    let query_str = "world records";
    let query = Arc::new(StringArray::from_iter_values(once(query_str)));
    let query_vector = embedding.compute_query_embeddings(query)?;
    let mut results = table
        .query()
        .full_text_search(FullTextSearchQuery::new(query_str.to_owned()))
        .nearest_to(query_vector)?
        .limit(5)
        .execute_hybrid(QueryExecutionOptions::default())
        .await?;

    while let Some(batch) = results.try_next().await? {
        let out = batch
            .column_by_name("facts")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for text in out.iter().flatten() {
            println!("Result: {}", text);
        }
    }

    Ok(())
}

fn make_data() -> impl IntoArrow {
    let schema = Schema::new(vec![Field::new("facts", DataType::Utf8, false)]);

    let facts = StringArray::from_iter_values(vec![
        "Albert Einstein was a theoretical physicist.",
        "The capital of France is Paris.",
        "The Great Wall of China is one of the Seven Wonders of the World.",
        "Python is a popular programming language.",
        "Mount Everest is the highest mountain in the world.",
        "Leonardo da Vinci painted the Mona Lisa.",
        "Shakespeare wrote Hamlet.",
        "The human body has 206 bones.",
        "The speed of light is approximately 299,792 kilometers per second.",
        "Water boils at 100 degrees Celsius.",
        "The Earth orbits the Sun.",
        "The Pyramids of Giza are located in Egypt.",
        "Coffee is one of the most popular beverages in the world.",
        "Tokyo is the capital city of Japan.",
        "Photosynthesis is the process by which plants make their food.",
        "The Pacific Ocean is the largest ocean on Earth.",
        "Mozart was a prolific composer of classical music.",
        "The Internet is a global network of computers.",
        "Basketball is a sport played with a ball and a hoop.",
        "The first computer virus was created in 1983.",
        "Artificial neural networks are inspired by the human brain.",
        "Deep learning is a subset of machine learning.",
        "IBM's Watson won Jeopardy! in 2011.",
        "The first computer programmer was Ada Lovelace.",
        "The first chatbot was ELIZA, created in the 1960s.",
    ]);
    let schema = Arc::new(schema);
    let rb = RecordBatch::try_new(schema.clone(), vec![Arc::new(facts)]).unwrap();
    Box::new(RecordBatchIterator::new(vec![Ok(rb)], schema))
}

async fn create_index(table: &Table) -> Result<()> {
    table
        .create_index(&["facts"], Index::FTS(FtsIndexBuilder::default()))
        .execute()
        .await?;
    Ok(())
}
