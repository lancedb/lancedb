use std::sync::Arc;

use arrow_array::{Float64Array, GenericStringArray, Int32Array, RecordBatch, RecordBatchIterator};
use arrow_schema::{DataType, Field, Schema};
use futures::StreamExt;
use lancedb::{
    arrow::IntoArrow,
    connect,
    embeddings::{openai::OpenAIEmbeddingFunction, EmbeddingDefinition},
    query::ExecutableQuery,
    Result,
};

#[tokio::main]
async fn main() -> Result<()> {
    let tempdir = tempfile::tempdir().unwrap();
    let tempdir = tempdir.path().to_str().unwrap();
    let api_key = std::env::var("OPENAI_API_KEY").expect("OPENAI_API_KEY is not set");
    let embedding = OpenAIEmbeddingFunction::new("text", api_key);

    let db = connect(tempdir).execute().await?;
    db.embedding_registry()
        .register("openai", Arc::new(embedding))?;

    let table = db
        .create_table("vectors", make_data())
        .add_embedding(EmbeddingDefinition::new(
            "text",
            "openai",
            Some("embeddings"),
        ))?
        .execute()
        .await?;

    // there is no equivalent to '.search(<query>)' yet
    let mut results = table.query().execute().await?;
    while let Some(Ok(batch)) = results.next().await {
        let embeddings = batch.column_by_name("embeddings");
        assert!(embeddings.is_some());
        let embeddings = embeddings.unwrap();
        println!("{:?}", embeddings);
    }

    Ok(())
}

fn make_data() -> impl IntoArrow {
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, true),
        Field::new("text", DataType::Utf8, false),
        Field::new("price", DataType::Float64, false),
    ]);

    let id = Int32Array::from(vec![1, 2]);
    let text = GenericStringArray::<i32>::from(vec![Some("Black T-Shirt"), Some("Leather Jacket")]);
    let price = Float64Array::from(vec![10.0, 50.0]);
    let schema = Arc::new(schema);
    let rb = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(id), Arc::new(text), Arc::new(price)],
    )
    .unwrap();
    Box::new(RecordBatchIterator::new(vec![Ok(rb)], schema))
}
