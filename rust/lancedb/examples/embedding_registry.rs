use std::{borrow::Cow, sync::Arc};

use arrow::datatypes::Float32Type;
use arrow_array::{
    Array, FixedSizeListArray, GenericStringArray, Int32Array, RecordBatch, RecordBatchIterator,
};
use arrow_schema::{DataType, Field, Schema};
use futures::StreamExt;
use lancedb::{
    arrow::IntoArrow,
    connect,
    embeddings::{EmbeddingDefinition, EmbeddingFunction},
    query::ExecutableQuery,
    Result,
};

#[derive(Debug)]
struct MockEmbed {
    source_type: DataType,
    dest_type: DataType,
}
const DIM: usize = 1;

impl MockEmbed {
    pub fn new() -> Self {
        Self {
            source_type: DataType::Utf8,
            dest_type: DataType::new_fixed_size_list(DataType::Float32, DIM as i32, true),
        }
    }
}

impl EmbeddingFunction for MockEmbed {
    fn name(&self) -> &str {
        "mock_embed"
    }
    fn source_type(&self) -> Cow<DataType> {
        Cow::Borrowed(&self.source_type)
    }
    fn dest_type(&self) -> Cow<DataType> {
        Cow::Borrowed(&self.dest_type)
    }
    fn embed(&self, source: Arc<dyn Array>) -> Result<Arc<dyn Array>> {
        let len = source.len();
        Ok(Arc::new(FixedSizeListArray::from_iter_primitive::<
            Float32Type,
            _,
            _,
        >(
            (0..len).map(|_| Some(vec![Some(1.0); DIM])),
            DIM as i32,
        )))
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let tempdir = tempfile::tempdir().unwrap();
    let tempdir = tempdir.path().to_str().unwrap();
    let db = connect(tempdir).execute().await?;
    let embed_fun = MockEmbed::new();
    db.embedding_registry()
        .register("embed_fun", Arc::new(embed_fun));
    let tbl = db
        .create_table("test", create_some_records()?)
        .add_embedding(EmbeddingDefinition::new(
            "text",
            "embed_fun",
            Some("embeddings"),
        ))?
        .execute()
        .await?;
    let mut res = tbl.query().execute().await?;
    while let Some(batch) = res.next().await {
        // just drain the batches
        batch?;
    }

    // now make sure the embeddings are applied when
    // we add new records too
    tbl.add(create_some_records()?).execute().await?;
    let mut res = tbl.query().execute().await?;
    while let Some(batch) = res.next().await {
        println!("batch = {:?}", batch);
    }
    Ok(())
}

fn create_some_records() -> Result<impl IntoArrow> {
    const TOTAL: usize = 2;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("text", DataType::Utf8, true),
    ]));

    // Create a RecordBatch stream.
    let batches = RecordBatchIterator::new(
        vec![RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from_iter_values(0..TOTAL as i32)),
                Arc::new(GenericStringArray::<i32>::from_iter(
                    std::iter::repeat(Some("hello world".to_string())).take(TOTAL),
                )),
            ],
        )
        .unwrap()]
        .into_iter()
        .map(Ok),
        schema.clone(),
    );
    Ok(Box::new(batches))
}
