// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
    iter::repeat,
    sync::Arc,
};

use arrow::buffer::NullBuffer;
use arrow_array::{
    Array, FixedSizeListArray, Float32Array, Int32Array, RecordBatch, RecordBatchIterator,
    StringArray,
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

#[tokio::test]
async fn test_custom_func() -> Result<()> {
    let tempdir = tempfile::tempdir().unwrap();
    let tempdir = tempdir.path().to_str().unwrap();
    let db = connect(tempdir).execute().await?;
    let embed_fun = MockEmbed::new("embed_fun".to_string(), 1);
    db.embedding_registry()
        .register("embed_fun", Arc::new(embed_fun.clone()))?;

    let tbl = db
        .create_table("test", create_some_records()?)
        .add_embedding(EmbeddingDefinition::new(
            "text",
            &embed_fun.name,
            Some("embeddings"),
        ))?
        .execute()
        .await?;
    let mut res = tbl.query().execute().await?;
    while let Some(Ok(batch)) = res.next().await {
        let embeddings = batch.column_by_name("embeddings");
        assert!(embeddings.is_some());
        let embeddings = embeddings.unwrap();
        assert_eq!(embeddings.data_type(), embed_fun.dest_type()?.as_ref());
    }
    // now make sure the embeddings are applied when
    // we add new records too
    tbl.add(create_some_records()?).execute().await?;
    let mut res = tbl.query().execute().await?;
    while let Some(Ok(batch)) = res.next().await {
        let embeddings = batch.column_by_name("embeddings");
        assert!(embeddings.is_some());
        let embeddings = embeddings.unwrap();
        assert_eq!(embeddings.data_type(), embed_fun.dest_type()?.as_ref());
    }
    Ok(())
}

#[tokio::test]
async fn test_custom_registry() -> Result<()> {
    let tempdir = tempfile::tempdir().unwrap();
    let tempdir = tempdir.path().to_str().unwrap();

    let db = connect(tempdir)
        .embedding_registry(Arc::new(MyRegistry::default()))
        .execute()
        .await?;

    let tbl = db
        .create_table("test", create_some_records()?)
        .add_embedding(EmbeddingDefinition::new(
            "text",
            "func_1",
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
            MockEmbed::new("func_1".to_string(), 1)
                .dest_type()?
                .as_ref()
        );
    }
    Ok(())
}

#[tokio::test]
async fn test_multiple_embeddings() -> Result<()> {
    let tempdir = tempfile::tempdir().unwrap();
    let tempdir = tempdir.path().to_str().unwrap();

    let db = connect(tempdir).execute().await?;
    let func_1 = MockEmbed::new("func_1".to_string(), 1);
    let func_2 = MockEmbed::new("func_2".to_string(), 10);
    db.embedding_registry()
        .register(&func_1.name, Arc::new(func_1.clone()))?;
    db.embedding_registry()
        .register(&func_2.name, Arc::new(func_2.clone()))?;

    let tbl = db
        .create_table("test", create_some_records()?)
        .add_embedding(EmbeddingDefinition::new(
            "text",
            &func_1.name,
            Some("first_embeddings"),
        ))?
        .add_embedding(EmbeddingDefinition::new(
            "text",
            &func_2.name,
            Some("second_embeddings"),
        ))?
        .execute()
        .await?;
    let mut res = tbl.query().execute().await?;
    while let Some(Ok(batch)) = res.next().await {
        let embeddings = batch.column_by_name("first_embeddings");
        assert!(embeddings.is_some());
        let second_embeddings = batch.column_by_name("second_embeddings");
        assert!(second_embeddings.is_some());

        let embeddings = embeddings.unwrap();
        assert_eq!(embeddings.data_type(), func_1.dest_type()?.as_ref());

        let second_embeddings = second_embeddings.unwrap();
        assert_eq!(second_embeddings.data_type(), func_2.dest_type()?.as_ref());
    }

    // now make sure the embeddings are applied when
    // we add new records too
    tbl.add(create_some_records()?).execute().await?;
    let mut res = tbl.query().execute().await?;
    while let Some(Ok(batch)) = res.next().await {
        let embeddings = batch.column_by_name("first_embeddings");
        assert!(embeddings.is_some());
        let second_embeddings = batch.column_by_name("second_embeddings");
        assert!(second_embeddings.is_some());

        let embeddings = embeddings.unwrap();
        assert_eq!(embeddings.data_type(), func_1.dest_type()?.as_ref());

        let second_embeddings = second_embeddings.unwrap();
        assert_eq!(second_embeddings.data_type(), func_2.dest_type()?.as_ref());
    }
    Ok(())
}

#[tokio::test]
async fn test_open_table_embeddings() -> Result<()> {
    let tempdir = tempfile::tempdir().unwrap();
    let tempdir = tempdir.path().to_str().unwrap();

    let db = connect(tempdir).execute().await?;
    let embed_fun = MockEmbed::new("embed_fun".to_string(), 1);
    db.embedding_registry()
        .register("embed_fun", Arc::new(embed_fun.clone()))?;

    db.create_table("test", create_some_records()?)
        .add_embedding(EmbeddingDefinition::new(
            "text",
            &embed_fun.name,
            Some("embeddings"),
        ))?
        .execute()
        .await?;

    // now open the table and check the embeddings
    let tbl = db.open_table("test").execute().await?;

    let mut res = tbl.query().execute().await?;
    while let Some(Ok(batch)) = res.next().await {
        let embeddings = batch.column_by_name("embeddings");
        assert!(embeddings.is_some());
        let embeddings = embeddings.unwrap();
        assert_eq!(embeddings.data_type(), embed_fun.dest_type()?.as_ref());
    }
    // now make sure the embeddings are applied when
    // we add new records too
    tbl.add(create_some_records()?).execute().await?;
    let mut res = tbl.query().execute().await?;
    while let Some(Ok(batch)) = res.next().await {
        let embeddings = batch.column_by_name("embeddings");
        assert!(embeddings.is_some());
        let embeddings = embeddings.unwrap();
        assert_eq!(embeddings.data_type(), embed_fun.dest_type()?.as_ref());
    }
    Ok(())
}

#[tokio::test]
async fn test_no_func_in_registry() -> Result<()> {
    let tempdir = tempfile::tempdir().unwrap();
    let tempdir = tempdir.path().to_str().unwrap();

    let db = connect(tempdir).execute().await?;

    let res = db
        .create_table("test", create_some_records()?)
        .add_embedding(EmbeddingDefinition::new(
            "text",
            "some_func",
            Some("first_embeddings"),
        ));
    assert!(res.is_err());
    assert!(matches!(
        res.err().unwrap(),
        Error::EmbeddingFunctionNotFound { .. }
    ));

    Ok(())
}

#[tokio::test]
async fn test_no_func_in_registry_on_add() -> Result<()> {
    let tempdir = tempfile::tempdir().unwrap();
    let tempdir = tempdir.path().to_str().unwrap();

    let db = connect(tempdir).execute().await?;
    db.embedding_registry().register(
        "some_func",
        Arc::new(MockEmbed::new("some_func".to_string(), 1)),
    )?;

    db.create_table("test", create_some_records()?)
        .add_embedding(EmbeddingDefinition::new(
            "text",
            "some_func",
            Some("first_embeddings"),
        ))?
        .execute()
        .await?;

    let db = connect(tempdir).execute().await?;

    let tbl = db.open_table("test").execute().await?;
    // This should fail because 'tbl' is expecting "some_func" to be in the registry
    let res = tbl.add(create_some_records()?).execute().await;
    assert!(res.is_err());
    assert!(matches!(
        res.unwrap_err(),
        crate::Error::EmbeddingFunctionNotFound { .. }
    ));

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
                Arc::new(StringArray::from_iter(
                    repeat(Some("hello world".to_string())).take(TOTAL),
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
struct MyRegistry {
    functions: HashMap<String, Arc<dyn EmbeddingFunction>>,
}
impl Default for MyRegistry {
    fn default() -> Self {
        let funcs: Vec<Arc<dyn EmbeddingFunction>> = vec![
            Arc::new(MockEmbed::new("func_1".to_string(), 1)),
            Arc::new(MockEmbed::new("func_2".to_string(), 10)),
        ];
        Self {
            functions: funcs
                .into_iter()
                .map(|f| (f.name().to_string(), f))
                .collect(),
        }
    }
}

/// a mock registry that only has one function called `embed_fun`
impl EmbeddingRegistry for MyRegistry {
    fn functions(&self) -> HashSet<String> {
        self.functions.keys().cloned().collect()
    }

    fn register(&self, _name: &str, _function: Arc<dyn EmbeddingFunction>) -> Result<()> {
        Err(Error::Other {
            message: "MyRegistry is read-only".to_string(),
            source: None,
        })
    }

    fn get(&self, name: &str) -> Option<Arc<dyn EmbeddingFunction>> {
        self.functions.get(name).cloned()
    }
}

#[derive(Debug, Clone)]
struct MockEmbed {
    source_type: DataType,
    dest_type: DataType,
    name: String,
    dim: usize,
}

impl MockEmbed {
    pub fn new(name: String, dim: usize) -> Self {
        Self {
            source_type: DataType::Utf8,
            dest_type: DataType::new_fixed_size_list(DataType::Float32, dim as _, true),
            name,
            dim,
        }
    }
}

impl EmbeddingFunction for MockEmbed {
    fn name(&self) -> &str {
        &self.name
    }
    fn source_type(&self) -> Result<Cow<'_, DataType>> {
        Ok(Cow::Borrowed(&self.source_type))
    }
    fn dest_type(&self) -> Result<Cow<'_, DataType>> {
        Ok(Cow::Borrowed(&self.dest_type))
    }
    fn compute_source_embeddings(&self, source: Arc<dyn Array>) -> Result<Arc<dyn Array>> {
        // We can't use the FixedSizeListBuilder here because it always adds a null bitmap
        // and we want to explicitly work with non-nullable arrays.
        let len = source.len();
        let inner = Arc::new(Float32Array::from(vec![Some(1.0); len * self.dim]));
        let field = Field::new("item", inner.data_type().clone(), false);
        let arr = FixedSizeListArray::new(
            Arc::new(field),
            self.dim as _,
            inner,
            Some(NullBuffer::new_valid(len)),
        );

        Ok(Arc::new(arr))
    }

    #[allow(unused_variables)]
    fn compute_query_embeddings(&self, input: Arc<dyn Array>) -> Result<Arc<dyn Array>> {
        unimplemented!()
    }
}
