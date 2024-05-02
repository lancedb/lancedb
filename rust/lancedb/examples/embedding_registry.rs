use std::{borrow::Cow, collections::HashSet, sync::Arc};

use arrow::datatypes::Float32Type;
use arrow_array::{
    Array, FixedSizeListArray, GenericStringArray, Int32Array, RecordBatch, RecordBatchIterator,
};
use arrow_schema::{DataType, Field, Schema};
use futures::StreamExt;
use lancedb::{
    arrow::IntoArrow,
    connect,
    embeddings::{EmbeddingDefinition, EmbeddingFunction, EmbeddingRegistry},
    query::ExecutableQuery,
    Error, Result,
};

#[tokio::main]
async fn main() -> Result<()> {
    custom_func().await?;
    custom_registry().await?;
    Ok(())
}

async fn custom_func() -> Result<()> {
    let tempdir = tempfile::tempdir().unwrap();
    let tempdir = tempdir.path().to_str().unwrap();
    let db = connect(tempdir).execute().await?;
    let embed_fun = MockEmbed::new();
    db.embedding_registry()
        .register("embed_fun", Arc::new(embed_fun))?;

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
    while let Some(Ok(batch)) = res.next().await {
        let embeddings = batch.column_by_name("embeddings");
        assert!(embeddings.is_some());
        let embeddings = embeddings.unwrap();
        assert_eq!(
            embeddings.data_type(),
            MockEmbed::new().dest_type().as_ref()
        );
    }
    // now make sure the embeddings are applied when
    // we add new records too
    tbl.add(create_some_records()?).execute().await?;
    let mut res = tbl.query().execute().await?;
    while let Some(Ok(batch)) = res.next().await {
        let embeddings = batch.column_by_name("embeddings");
        assert!(embeddings.is_some());
        let embeddings = embeddings.unwrap();
        assert_eq!(
            embeddings.data_type(),
            MockEmbed::new().dest_type().as_ref()
        );
    }
    Ok(())
}

async fn custom_registry() -> Result<()> {
    #[derive(Debug)]
    struct MyRegistry {}

    /// a mock registry that only has one function called `embed_fun`
    impl EmbeddingRegistry for MyRegistry {
        fn functions(&self) -> HashSet<String> {
            std::iter::once("embed_fun".to_string()).collect()
        }

        fn register(&self, _name: &str, _function: Arc<dyn EmbeddingFunction>) -> Result<()> {
            Err(Error::Other {
                message: "MyRegistry is read-only".to_string(),
                source: None,
            })
        }

        fn get(&self, name: &str) -> Option<Arc<dyn EmbeddingFunction>> {
            if name == "embed_fun" {
                Some(Arc::new(MockEmbed::new()))
            } else {
                None
            }
        }
    }

    let tempdir = tempfile::tempdir().unwrap();
    let tempdir = tempdir.path().to_str().unwrap();

    let db = connect(tempdir)
        .embedding_registry(Arc::new(MyRegistry {}))
        .execute()
        .await?;

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
    while let Some(Ok(batch)) = res.next().await {
        let embeddings = batch.column_by_name("embeddings");
        assert!(embeddings.is_some());
        let embeddings = embeddings.unwrap();
        assert_eq!(
            embeddings.data_type(),
            MockEmbed::new().dest_type().as_ref()
        );
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
