// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::{
    borrow::Cow,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use arrow::buffer::NullBuffer;
use arrow_array::{
    Array, FixedSizeListArray, Float32Array, RecordBatch, RecordBatchIterator, StringArray,
};
use arrow_schema::{DataType, Field, Schema};
use lancedb::{
    embeddings::{EmbeddingDefinition, EmbeddingFunction, MaybeEmbedded, WithEmbeddings},
    Error, Result,
};

#[derive(Debug)]
struct SlowMockEmbed {
    name: String,
    dim: usize,
    delay_ms: u64,
    call_count: Arc<AtomicUsize>,
}

impl SlowMockEmbed {
    pub fn new(name: String, dim: usize, delay_ms: u64) -> Self {
        Self {
            name,
            dim,
            delay_ms,
            call_count: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn get_call_count(&self) -> usize {
        self.call_count.load(Ordering::SeqCst)
    }
}

impl EmbeddingFunction for SlowMockEmbed {
    fn name(&self) -> &str {
        &self.name
    }

    fn source_type(&self) -> Result<Cow<'_, DataType>> {
        Ok(Cow::Owned(DataType::Utf8))
    }

    fn dest_type(&self) -> Result<Cow<'_, DataType>> {
        Ok(Cow::Owned(DataType::new_fixed_size_list(
            DataType::Float32,
            self.dim as _,
            true,
        )))
    }

    fn compute_source_embeddings(&self, source: Arc<dyn Array>) -> Result<Arc<dyn Array>> {
        // Simulate slow embedding computation
        std::thread::sleep(Duration::from_millis(self.delay_ms));
        self.call_count.fetch_add(1, Ordering::SeqCst);

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

    fn compute_query_embeddings(&self, _input: Arc<dyn Array>) -> Result<Arc<dyn Array>> {
        unimplemented!()
    }
}

fn create_test_batch() -> Result<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![Field::new("text", DataType::Utf8, false)]));
    let text = StringArray::from(vec!["hello", "world"]);
    RecordBatch::try_new(schema, vec![Arc::new(text)]).map_err(|e| Error::Runtime {
        message: format!("Failed to create test batch: {}", e),
    })
}

#[test]
fn test_single_embedding_fast_path() {
    // Single embedding should execute without spawning threads
    let batch = create_test_batch().unwrap();
    let schema = batch.schema();

    let embed = Arc::new(SlowMockEmbed::new("test".to_string(), 2, 10));
    let embedding_def = EmbeddingDefinition::new("text", "test", Some("embedding"));

    let reader = RecordBatchIterator::new(vec![Ok(batch)], schema);
    let embeddings = vec![(embedding_def, embed.clone() as Arc<dyn EmbeddingFunction>)];
    let mut with_embeddings = WithEmbeddings::new(reader, embeddings);

    let result = with_embeddings.next().unwrap().unwrap();
    assert!(result.column_by_name("embedding").is_some());
    assert_eq!(embed.get_call_count(), 1);
}

#[test]
fn test_multiple_embeddings_parallel() {
    // Multiple embeddings should execute in parallel
    let batch = create_test_batch().unwrap();
    let schema = batch.schema();

    let embed1 = Arc::new(SlowMockEmbed::new("embed1".to_string(), 2, 100));
    let embed2 = Arc::new(SlowMockEmbed::new("embed2".to_string(), 3, 100));
    let embed3 = Arc::new(SlowMockEmbed::new("embed3".to_string(), 4, 100));

    let def1 = EmbeddingDefinition::new("text", "embed1", Some("emb1"));
    let def2 = EmbeddingDefinition::new("text", "embed2", Some("emb2"));
    let def3 = EmbeddingDefinition::new("text", "embed3", Some("emb3"));

    let reader = RecordBatchIterator::new(vec![Ok(batch)], schema);
    let embeddings = vec![
        (def1, embed1.clone() as Arc<dyn EmbeddingFunction>),
        (def2, embed2.clone() as Arc<dyn EmbeddingFunction>),
        (def3, embed3.clone() as Arc<dyn EmbeddingFunction>),
    ];
    let mut with_embeddings = WithEmbeddings::new(reader, embeddings);

    let result = with_embeddings.next().unwrap().unwrap();

    // Verify all embedding columns are present
    assert!(result.column_by_name("emb1").is_some());
    assert!(result.column_by_name("emb2").is_some());
    assert!(result.column_by_name("emb3").is_some());

    // Verify all embeddings were computed
    assert_eq!(embed1.get_call_count(), 1);
    assert_eq!(embed2.get_call_count(), 1);
    assert_eq!(embed3.get_call_count(), 1);
}

#[test]
fn test_embedding_column_order_preserved() {
    // Verify that embedding columns are added in the same order as definitions
    let batch = create_test_batch().unwrap();
    let schema = batch.schema();

    let embed1 = Arc::new(SlowMockEmbed::new("embed1".to_string(), 2, 10));
    let embed2 = Arc::new(SlowMockEmbed::new("embed2".to_string(), 3, 10));
    let embed3 = Arc::new(SlowMockEmbed::new("embed3".to_string(), 4, 10));

    let def1 = EmbeddingDefinition::new("text", "embed1", Some("first"));
    let def2 = EmbeddingDefinition::new("text", "embed2", Some("second"));
    let def3 = EmbeddingDefinition::new("text", "embed3", Some("third"));

    let reader = RecordBatchIterator::new(vec![Ok(batch)], schema);
    let embeddings = vec![
        (def1, embed1 as Arc<dyn EmbeddingFunction>),
        (def2, embed2 as Arc<dyn EmbeddingFunction>),
        (def3, embed3 as Arc<dyn EmbeddingFunction>),
    ];
    let mut with_embeddings = WithEmbeddings::new(reader, embeddings);

    let result = with_embeddings.next().unwrap().unwrap();
    let result_schema = result.schema();

    // Original column is first
    assert_eq!(result_schema.field(0).name(), "text");
    // Embedding columns follow in order
    assert_eq!(result_schema.field(1).name(), "first");
    assert_eq!(result_schema.field(2).name(), "second");
    assert_eq!(result_schema.field(3).name(), "third");
}

#[test]
fn test_embedding_error_propagation() {
    // Test that errors from embedding computation are properly propagated
    #[derive(Debug)]
    struct FailingEmbed {
        name: String,
    }

    impl EmbeddingFunction for FailingEmbed {
        fn name(&self) -> &str {
            &self.name
        }

        fn source_type(&self) -> Result<Cow<'_, DataType>> {
            Ok(Cow::Owned(DataType::Utf8))
        }

        fn dest_type(&self) -> Result<Cow<'_, DataType>> {
            Ok(Cow::Owned(DataType::new_fixed_size_list(
                DataType::Float32,
                2,
                true,
            )))
        }

        fn compute_source_embeddings(&self, _source: Arc<dyn Array>) -> Result<Arc<dyn Array>> {
            Err(Error::Runtime {
                message: "Intentional failure".to_string(),
            })
        }

        fn compute_query_embeddings(&self, _input: Arc<dyn Array>) -> Result<Arc<dyn Array>> {
            unimplemented!()
        }
    }

    let batch = create_test_batch().unwrap();
    let schema = batch.schema();

    let embed = Arc::new(FailingEmbed {
        name: "failing".to_string(),
    });
    let def = EmbeddingDefinition::new("text", "failing", Some("emb"));

    let reader = RecordBatchIterator::new(vec![Ok(batch)], schema);
    let embeddings = vec![(def, embed as Arc<dyn EmbeddingFunction>)];
    let mut with_embeddings = WithEmbeddings::new(reader, embeddings);

    let result = with_embeddings.next().unwrap();
    assert!(result.is_err());
    let err_msg = format!("{}", result.err().unwrap());
    assert!(err_msg.contains("Intentional failure"));
}

#[test]
fn test_maybe_embedded_with_no_embeddings() {
    // Test that MaybeEmbedded::No variant works correctly
    let batch = create_test_batch().unwrap();
    let schema = batch.schema();

    let reader = RecordBatchIterator::new(vec![Ok(batch.clone())], schema.clone());
    let table_def = lancedb::table::TableDefinition {
        schema: schema.clone(),
        column_definitions: vec![lancedb::table::ColumnDefinition {
            kind: lancedb::table::ColumnKind::Physical,
        }],
    };

    let mut maybe_embedded = MaybeEmbedded::try_new(reader, table_def, None).unwrap();

    let result = maybe_embedded.next().unwrap().unwrap();
    assert_eq!(result.num_columns(), 1);
    assert_eq!(result.column(0).as_ref(), batch.column(0).as_ref());
}
